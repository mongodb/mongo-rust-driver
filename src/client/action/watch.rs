use std::future::IntoFuture;

use bson::Document;
use futures_util::FutureExt;

use crate::{Client, change_stream::{options::{ChangeStreamOptions, FullDocumentType, FullDocumentBeforeChangeType}, ChangeStream, event::ChangeStreamEvent, session::SessionChangeStream}, ClientSession, error::{Result, ErrorKind}, client::BoxFuture, operation::AggregateTarget};

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

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl crate::sync::Client {
    pub fn watch_2(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
    ) -> Watch {
        self.async_client.watch_2(pipeline)
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

macro_rules! option_setters {
    (
        $opt_field:ident: $opt_field_ty:ty;
        $(
            $(#[$($attrss:tt)*])*
            $opt_name:ident: $opt_ty:ty,
        )+
    ) => {
        fn options(&mut self) -> &mut $opt_field_ty {
            self.$opt_field.get_or_insert_with(<$opt_field_ty>::default)
        }

        $(
            $(#[$($attrss)*])*
            pub fn $opt_name(mut self, value: $opt_ty) -> Self {
                self.options().$opt_name = Some(value);
                self
            }
        )+
    };
}

impl<'a, S: Session> Watch<'a, S> {
    option_setters!(options: ChangeStreamOptions;
        /// Configures how the
        /// [`ChangeStreamEvent::full_document`](crate::change_stream::event::ChangeStreamEvent::full_document)
        /// field will be populated. By default, the field will be empty for updates.
        full_document: FullDocumentType,
        /// Configures how the
        /// [`ChangeStreamEvent::full_document_before_change`](
        /// crate::change_stream::event::ChangeStreamEvent::full_document_before_change) field will be
        /// populated.  By default, the field will be empty for updates.
        full_document_before_change: FullDocumentBeforeChangeType,
    );
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

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<'a> Watch<'a, Implicit> {
    pub fn run(self) -> Result<crate::sync::ChangeStream<ChangeStreamEvent<Document>>> {
        crate::runtime::block_on(self.into_future()).map(crate::sync::ChangeStream::new)
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<'a> Watch<'a, Explicit<'a>> {
    pub fn run(self) -> Result<crate::sync::SessionChangeStream<ChangeStreamEvent<Document>>> {
        crate::runtime::block_on(self.into_future()).map(crate::sync::SessionChangeStream::new)
    }
}