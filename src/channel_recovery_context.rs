use crate::{
    Error,
    frames::{ExpectedReply, Frames},
    notifier::Notifier,
    topology::ChannelDefinition,
};

use std::collections::VecDeque;

pub(crate) struct ChannelRecoveryContext {
    cause: Error,
    topology: ChannelDefinition,
    expected_replies: Option<VecDeque<ExpectedReply>>,
    notifier: Notifier,
}

impl ChannelRecoveryContext {
    pub(crate) fn new(cause: Error, topology: ChannelDefinition) -> Self {
        let notifier = Notifier::default();
        Self {
            cause: cause.with_notifier(Some(notifier.clone())),
            topology,
            expected_replies: None,
            notifier,
        }
    }

    pub(crate) fn cause(&self) -> Error {
        self.cause.clone()
    }

    pub(crate) fn notifier(&self) -> Notifier {
        self.notifier.clone()
    }

    pub(crate) fn topology(&self) -> ChannelDefinition {
        self.topology.clone()
    }

    pub(crate) fn set_expected_replies(
        &mut self,
        expected_replies: Option<VecDeque<ExpectedReply>>,
    ) {
        self.expected_replies = expected_replies;
    }

    pub(crate) fn finalize_recovery(self) {
        self.notifier.notify_all();
        if let Some(replies) = self.expected_replies {
            Frames::cancel_expected_replies(replies, self.cause);
        }
    }

    /// Ends a recovery that will not complete: wakes its waiters and fails the replies it kept.
    pub(crate) fn abandon(self, error: Error) {
        self.notifier.notify_all();
        if let Some(replies) = self.expected_replies {
            Frames::cancel_expected_replies(replies, error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectionState, ErrorKind, Promise, channel::Reply};
    use futures_lite::future;

    #[test]
    fn an_abandoned_recovery_wakes_its_waiters_and_fails_the_replies_it_kept() {
        let mut ctx = ChannelRecoveryContext::new(
            ErrorKind::MissingHeartbeatError.into(),
            ChannelDefinition::default(),
        );
        let (reply, resolver) = Promise::<()>::new("tx.select-ok");
        ctx.set_expected_replies(Some(VecDeque::from([ExpectedReply(
            Reply::TxSelectOk(resolver.clone()),
            Box::new(resolver),
        )])));
        let notifier = ctx.notifier();

        ctx.abandon(ErrorKind::InvalidConnectionState(ConnectionState::Error).into());

        assert!(future::block_on(future::poll_once(notifier)).is_some());
        assert!(matches!(reply.try_wait(), Some(Err(_))));
    }
}
