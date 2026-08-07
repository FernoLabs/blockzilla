use futures_core::Stream;
use futures_util::{StreamExt, stream};
use tokio_stream::wrappers::BroadcastStream;
use topcoat::{
    Result,
    datastar::{PatchElements, PatchSignals},
    router::{
        content::sse::{Event, KeepAlive, Sse},
        route,
    },
};

use crate::state::{StreamEvent, full_signals, subscribe};

/// One long-lived connection per open dashboard tab. `data-on:load` on
/// `#dashboard` opens this with `@get('/api/stream')`. Most events are a
/// `PatchSignals` carrying only the values that changed since the previous
/// publish (`state::publish`'s diff, not a full re-serialization), with
/// removed signals set to `null` so Datastar deletes them from the
/// client's store. When a list's *membership* changes -- a row appearing
/// or disappearing, not just an existing row's fields ticking -- a
/// `PatchElements` morphs the affected container's freshly rendered HTML
/// into the DOM instead, since a signal patch has nothing to bind a new
/// row to and never removes a stale one; see `StreamEvent`.
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
/// permanently missing whatever changed while it was behind -- membership
/// is self-healing too, since a full signal map implies a full page load's
/// worth of state and the list containers were already rendered fresh as
/// of that same server render.
#[route(GET "/api/stream")]
async fn stream() -> Result<Sse<impl Stream<Item = Result<Event>> + use<>>> {
    let rx = subscribe();

    let initial = stream::once(async { StreamEvent::Signals(full_signals()) });
    let updates = BroadcastStream::new(rx).map(|msg| match msg {
        Ok(event) => event,
        Err(_lagged) => StreamEvent::Signals(full_signals()),
    });

    let events = initial.chain(updates).map(|event| match event {
        StreamEvent::Signals(signals) => PatchSignals::json(&signals).map(Into::into),
        StreamEvent::Elements { selector, html } => {
            Ok(PatchElements::new(html).selector(selector).into())
        }
    });

    Ok(Sse::new(events).keep_alive(KeepAlive::new()))
}
