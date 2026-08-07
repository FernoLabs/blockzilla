use futures_core::Stream;
use futures_util::{stream, StreamExt};
use tokio_stream::wrappers::BroadcastStream;
use topcoat::{
    datastar::PatchSignals,
    router::{
        content::sse::{Event, KeepAlive, Sse},
        route,
    },
    Result,
};

use crate::state::{full_signals, subscribe};

/// One long-lived connection per open dashboard tab. `data-on:load` on
/// `#dashboard` opens this with `@get('/api/stream')`. Every event after
/// the first carries only the signals that changed since the previous
/// publish (`state::publish`'s diff, not a full re-serialization), with
/// removed signals set to `null` so Datastar deletes them from the
/// client's store.
///
/// The very first event is always a full signal map, sent before this
/// subscribes to the broadcast: `data-signals` on the page already seeded
/// the state as of the *server render*, but a value can change in the gap
/// between that render and this connection opening, and a delta-only
/// stream would never repeat a change that already happened. Diff-less
/// full sends can't have that gap (the next full tick always corrects it);
/// diff-based ones need this instead. The same fix covers a client that
/// falls behind the broadcast buffer (`Err(Lagged)` below): resend the
/// full map rather than silently skip, so a slow tab self-heals instead of
/// permanently missing whatever changed while it was behind.
#[route(GET "/api/stream")]
async fn stream() -> Result<Sse<impl Stream<Item = Result<Event>> + use<>>> {
    let rx = subscribe();

    let initial = stream::once(async { full_signals() });
    let updates = BroadcastStream::new(rx).map(|msg| match msg {
        Ok(delta) => delta,
        Err(_lagged) => full_signals(),
    });

    let events = initial
        .chain(updates)
        .map(|signals| PatchSignals::json(&signals).map(Into::into));

    Ok(Sse::new(events).keep_alive(KeepAlive::new()))
}
