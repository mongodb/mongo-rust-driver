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
    let (sender, mut receiver) = tokio::sync::watch::channel(Default::default());
    // The first call to recv on a watch channel returns immediately with the initial value.
    // We use futures::executor::block_on because this is not a truly blocking task, so
    // the runtimes don't need to shift things around to ensure scheduling continues normally.
    futures::executor::block_on(receiver.recv());
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
    pub(super) fn publish(&self, new_generation: u32) {
        let new_status = PoolStatus {
            generation: new_generation,
        };

        // if nobody is listening, this will return an error, which we don't mind.
        let _: std::result::Result<_, _> = self.sender.broadcast(new_status);
    }
}

/// Subscriber used to get the latest generation of the pool.
#[derive(Clone, Debug)]
pub(crate) struct PoolGenerationSubscriber {
    receiver: tokio::sync::watch::Receiver<PoolStatus>,
}

impl PoolGenerationSubscriber {
    /// Get a copy of the latest generation.
    pub(crate) fn generation(&self) -> u32 {
        self.receiver.borrow().generation
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_generation_change(
        &mut self,
        timeout: std::time::Duration,
    ) -> Option<u32> {
        crate::RUNTIME
            .timeout(timeout, self.receiver.recv())
            .await
            .ok()
            .flatten()
            .map(|status| status.generation)
    }
}
