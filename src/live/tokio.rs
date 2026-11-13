use super::LiveSubscription;
use crate::entry::LiveEntry;
use crate::error::{Result, SdJournalError};
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

const TOKIO_CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(100);

impl LiveSubscription {
    /// Convert this blocking subscription into a bounded Tokio receiver.
    ///
    /// A worker thread forwards entries using this subscription's configured channel capacity.
    /// Receive entries with `recv().await`; dropping the receiver releases the subscription.
    ///
    /// Returns [`SdJournalError::Io`] if the forwarding thread cannot be created.
    pub fn into_tokio(self) -> Result<tokio::sync::mpsc::Receiver<Result<LiveEntry>>> {
        let (tx, rx) = tokio::sync::mpsc::channel(self.channel_capacity);
        thread::Builder::new()
            .spawn(move || {
                loop {
                    if tx.is_closed() {
                        break;
                    }
                    match self.recv_timeout(TOKIO_CANCELLATION_POLL_INTERVAL) {
                        Ok(item) => {
                            if tx.blocking_send(item).is_err() {
                                break;
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .map_err(|error| SdJournalError::io("spawn_tokio_bridge", None, error))?;
        Ok(rx)
    }
}
