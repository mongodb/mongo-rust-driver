/// A message type that includes an acknowledgement mechanism.
/// When this is dropped or `acknowledge` is called, the sender will be notified.
#[derive(Debug)]
pub(crate) struct AcknowledgedMessage<M, R = ()> {
    notifier: tokio::sync::oneshot::Sender<R>,
    message: M,
}

impl<M, R> AcknowledgedMessage<M, R> {
    /// Create a new message and return it along with the AcknowledgmentReceiver that will
    /// be notified when the message is received or when it is dropped.
    pub(crate) fn package(message: M) -> (Self, AcknowledgmentReceiver<R>) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        (
            Self {
                message,
                notifier: sender,
            },
            AcknowledgmentReceiver { receiver },
        )
    }

    /// Get the message.
    pub(crate) fn into_message(self) -> M {
        self.message
    }

    /// Send acknowledgement to the receiver.
    #[allow(dead_code)]
    pub(crate) fn acknowledge(self, result: impl Into<R>) {
        // returns an error when the other end hangs up e.g. due to a timeout.
        let _: std::result::Result<_, _> = self.notifier.send(result.into());
    }
}

/// Receiver for the acknowledgement that the message was received or dropped.
pub(crate) struct AcknowledgmentReceiver<R> {
    receiver: tokio::sync::oneshot::Receiver<R>,
}

impl<R> AcknowledgmentReceiver<R> {
    /// Wait for the message to be acknowledged. If this returns None, that means the message
    /// was dropped without the receiving end explicitly sending anything back.
    pub(crate) async fn wait_for_acknowledgment(self) -> Option<R> {
        self.receiver.await.ok()
    }
}
