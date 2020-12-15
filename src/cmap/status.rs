use crate::error::Error;

/// Struct used to track the latest status of the pool.
#[derive(Clone, Debug)]
struct PoolStatus {
    /// The current generation of the pool.
    generation: u32,
}

impl Default for PoolStatus {
    fn default() -> Self {
        PoolStatus { generation: 0 }
    }
}

/// Create a channel for publishing and receiving updates to the pool's generation.
pub(super) fn channel() -> (PoolGenerationPublisher, PoolGenerationSubscriber) {
    let (sender, receiver) = tokio::sync::watch::channel(Default::default());
    (
        PoolGenerationPublisher { sender },
        PoolGenerationSubscriber { receiver },
    )
}

/// Struct used to publish updates to the pool's generation.
#[derive(Debug)]
pub(super) struct PoolGenerationPublisher {
    sender: tokio::sync::watch::Sender<PoolStatus>,
}

impl PoolGenerationPublisher {
    /// Publish a new generation.
    /// If the clear was caused by a connection establishment error, provide the error.
    pub(super) fn publish(&self, new_generation: u32) {
        let new_status = PoolStatus {
            generation: new_generation,
        };

        // if nobody is listening, this will return an error, which we don't mind.
        let _: std::result::Result<_, _> = self.sender.broadcast(new_status);
    }
}

/// Subscriber used to get the latest generation of the pool.
///
/// This can also be used to listen for when the pool encounters an error during connection
/// establishment.
#[derive(Clone, Debug)]
pub(crate) struct PoolGenerationSubscriber {
    receiver: tokio::sync::watch::Receiver<PoolStatus>,
}

impl PoolGenerationSubscriber {
    /// Get a copy of the latest generation.
    pub(crate) fn generation(&self) -> u32 {
        self.receiver.borrow().generation
    }

    // /// Listen for a connection establishment failure.
    // pub(crate) async fn listen_for_establishment_failure(&mut self) -> Option<Error> {
    //     while let Some(status) = self.receiver.recv().await {
    //         if let Some(error) = status.establishment_error {
    //             return Some(error);
    //         }
    //     }
    //     None
    // }

    #[cfg(test)]
    pub(crate) async fn wait_for_generation_change(
        &mut self,
        timeout: std::time::Duration,
    ) -> Option<u32> {
        // watch receivers return the latest vlaue immediately, need to compare against it to
        // determine if that has happened.
        let initial_generation = self.receiver.borrow().generation;
        crate::RUNTIME
            .timeout(timeout, async {
                while let Some(status) = self.receiver.recv().await {
                    if status.generation != initial_generation {
                        return Some(status.generation);
                    }
                }
                None
            })
            .await
            .ok()
            .flatten()
    }
}
