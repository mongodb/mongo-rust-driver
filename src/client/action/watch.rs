use std::future::IntoFuture;

use bson::Document;
use futures_util::FutureExt;

use crate::{Client, change_stream::{options::ChangeStreamOptions, ChangeStream, event::ChangeStreamEvent, session::SessionChangeStream}, ClientSession, error::{Result, ErrorKind}, client::BoxFuture, operation::AggregateTarget};

impl Client {
    pub fn watch_2(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
    ) -> Watch {
        Watch {
            client: &self,
            pipeline: pipeline.into_iter().collect(),
            options: None,
            session: Implicit,
        }
    }
}

mod private {
    pub trait Sealed {}
    impl Sealed for super::Implicit {}
    impl<'a> Sealed for super::Explicit<'a> {}
}

pub trait Session: private::Sealed {}
impl Session for Implicit {}
impl<'a> Session for Explicit<'a> {}

pub struct Implicit;
pub struct Explicit<'a>(&'a mut ClientSession);

#[must_use]
pub struct Watch<'a, S: Session = Implicit> {
    client: &'a Client,
    pipeline: Vec<Document>,
    options: Option<ChangeStreamOptions>,
    session: S,
}

impl<'a, S: Session> Watch<'a, S> {
    fn options(&mut self) -> &mut ChangeStreamOptions {
        self.options.get_or_insert_with(ChangeStreamOptions::default)
    }
}

impl<'a> Watch<'a, Implicit> {
    pub fn with_session<'s>(self, session: &'s mut ClientSession) -> Watch<'a, Explicit<'s>> {
        Watch {
            client: self.client,
            pipeline: self.pipeline,
            options: self.options,
            session: Explicit(session),
        }
    }
}

impl<'a> IntoFuture for Watch<'a, Implicit> {
    type Output = Result<ChangeStream<ChangeStreamEvent<Document>>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        async {
            resolve_options!(self.client, self.options, [read_concern, selection_criteria]);
            self.options
                .get_or_insert_with(Default::default)
                .all_changes_for_cluster = Some(true);
            let target = AggregateTarget::Database("admin".to_string());
            self.client.execute_watch(self.pipeline, self.options, target, None).await
        }.boxed()
    }
}

impl<'a> IntoFuture for Watch<'a, Explicit<'a>> {
    type Output = Result<SessionChangeStream<ChangeStreamEvent<Document>>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        async {
            resolve_read_concern_with_session!(self.client, self.options, Some(&mut *self.session.0))?;
            resolve_selection_criteria_with_session!(self.client, self.options, Some(&mut *self.session.0))?;
            self.options
                .get_or_insert_with(Default::default)
                .all_changes_for_cluster = Some(true);
            let target = AggregateTarget::Database("admin".to_string());
            self.client.execute_watch_with_session(self.pipeline, self.options, target, None, self.session.0)
                .await
        }.boxed()
    }
}