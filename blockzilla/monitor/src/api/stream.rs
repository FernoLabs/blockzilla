use std::{
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};

use futures_core::Stream;
use futures_util::{StreamExt, stream as futures_stream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast};
use topcoat::{
    Result,
    context::Cx,
    datastar::{PatchElements, PatchSignals},
    router::{
        StatusCode,
        content::sse::{Event, KeepAlive, Sse},
        query_params,
        response::{IntoResponse, Response},
        route,
    },
};

use crate::{
    app::{DashboardPage, render_dashboard_frame},
    state::{DashboardState, StreamEvent, snapshot, subscribe},
};

static STREAM_LIMIT: OnceLock<Arc<Semaphore>> = OnceLock::new();

/// Installs the process-wide stream limit before the router starts. A
/// `NonZeroUsize` makes zero an invalid CLI/configuration value instead of
/// producing a monitor that can never establish its own update stream.
pub(crate) fn configure_stream_limit(max: NonZeroUsize) -> std::result::Result<(), &'static str> {
    STREAM_LIMIT
        .set(Arc::new(Semaphore::new(max.get())))
        .map_err(|_| "stream connection limit already configured")
}

#[topcoat::router::query_params(error = bad_request)]
struct StreamQuery {
    view: Option<DashboardPage>,
}

/// The permit is deliberately a field of the stream that becomes the SSE
/// response body. Dropping the route-handler future is not enough: an open
/// tab must consume capacity until its body is dropped on disconnect.
struct PermitStream {
    inner: Pin<Box<dyn Stream<Item = Result<Event>> + Send>>,
    _permit: OwnedSemaphorePermit,
}

impl Stream for PermitStream {
    type Item = Result<Event>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

fn full_resync_events(state: &DashboardState) -> [StreamEvent; 2] {
    [
        StreamEvent::Signals(state.to_signals()),
        StreamEvent::Structure,
    ]
}

async fn next_update_batch(rx: &mut broadcast::Receiver<StreamEvent>) -> Option<Vec<StreamEvent>> {
    match rx.recv().await {
        Ok(event) => Some(vec![event]),
        Err(broadcast::error::RecvError::Closed) => None,
        Err(broadcast::error::RecvError::Lagged(_)) => {
            // Drop the receiver's retained-but-stale tail before taking the
            // replacement snapshot. Anything published after resubscribe
            // is queued and replayed (possibly redundantly); replaying old
            // deltas after a full resync could otherwise regress the tab.
            *rx = rx.resubscribe();
            let state = snapshot().await;
            Some(full_resync_events(&state).into())
        }
    }
}

async fn encode_event(page: DashboardPage, event: StreamEvent) -> Result<Event> {
    match event {
        StreamEvent::Signals(signals) => PatchSignals::json(&signals).map(Into::into),
        StreamEvent::Structure => {
            let state = snapshot().await;
            let view = render_dashboard_frame(page, &state).await?;
            let html = view.render(&Cx::default());
            Ok(PatchElements::new(html).selector("#dashboard-frame").into())
        }
    }
}

async fn stream_response(cx: &Cx, page: DashboardPage, limit: Arc<Semaphore>) -> Result<Response> {
    let permit = match limit.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "dashboard stream capacity reached; retry shortly",
            )
                .into_response(cx);
        }
    };

    // Subscribe before taking the initial snapshot. If a publish races
    // this gap, it is queued in `rx`; replaying a harmless duplicate after
    // the full resync is preferable to missing that update permanently.
    let rx = subscribe();
    let initial_state = snapshot().await;
    let initial = futures_stream::iter(full_resync_events(&initial_state));
    let updates = futures_stream::unfold(rx, |mut rx| async move {
        next_update_batch(&mut rx).await.map(|events| (events, rx))
    })
    .flat_map(futures_stream::iter);
    let events = initial
        .chain(updates)
        .then(move |event| encode_event(page, event));
    let events = PermitStream {
        inner: Box::pin(events),
        _permit: permit,
    };

    Sse::new(events)
        .keep_alive(KeepAlive::new())
        .into_response(cx)
}

/// One long-lived connection per open dashboard tab. Every initial
/// connection and lag recovery sends both a complete signal map and a
/// freshly rendered route-specific `#dashboard-frame`; a slow client can
/// therefore repair missing/stale rows as well as scalar values.
#[route(GET "/api/stream")]
async fn stream(cx: &Cx) -> Result<Response> {
    let page = query_params::<StreamQuery>(cx)?
        .view
        .unwrap_or(DashboardPage::Overview);
    let limit = STREAM_LIMIT
        .get()
        .expect("stream connection limit must be configured before serving")
        .clone();
    stream_response(cx, page, limit).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_resync_includes_signals_and_structure() {
        let events = full_resync_events(&DashboardState::default());
        assert!(matches!(events[0], StreamEvent::Signals(_)));
        assert!(matches!(events[1], StreamEvent::Structure));
    }

    #[tokio::test]
    async fn lag_resync_discards_retained_stale_deltas() {
        let (tx, mut rx) = broadcast::channel(2);
        for value in 1..=3 {
            tx.send(StreamEvent::Signals(serde_json::json!({ "queued": value })))
                .unwrap();
        }

        let batch = next_update_batch(&mut rx).await.unwrap();
        assert!(matches!(batch[0], StreamEvent::Signals(_)));
        assert!(matches!(batch[1], StreamEvent::Structure));
        assert!(
            matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
            "retained pre-resync deltas must not replay after the full state"
        );
    }

    #[tokio::test]
    async fn stream_permit_is_held_for_the_response_body_lifetime() {
        let cx = &Cx::default();
        let limit = Arc::new(Semaphore::new(1));

        let first = stream_response(cx, DashboardPage::Overview, limit.clone())
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(limit.available_permits(), 0);
        assert_eq!(first.headers()["content-type"], "text/event-stream");
        let mut body = first.into_body().into_data_stream();
        for expected in [
            "event: datastar-patch-signals",
            "event: datastar-patch-elements",
        ] {
            let bytes = tokio::time::timeout(std::time::Duration::from_secs(5), body.next())
                .await
                .expect("initial SSE event must be ready")
                .expect("stream must remain open")
                .expect("initial event must render");
            let event = std::str::from_utf8(&bytes).unwrap();
            assert!(event.contains(expected), "{event}");
            if expected.ends_with("elements") {
                assert!(event.contains("data: selector #dashboard-frame"));
                assert!(event.contains("id=\"dashboard-frame\""));
            }
        }

        let rejected = stream_response(cx, DashboardPage::Overview, limit.clone())
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(limit.available_permits(), 0);

        drop(body);
        assert_eq!(limit.available_permits(), 1);

        let replacement = stream_response(cx, DashboardPage::Overview, limit.clone())
            .await
            .unwrap();
        assert_eq!(replacement.status(), StatusCode::OK);
        assert_eq!(limit.available_permits(), 0);
    }
}
