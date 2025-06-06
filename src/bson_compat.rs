pub(crate) trait RawDocumentBufExt {
    fn append_ref<'a>(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>>,
    );
}

impl RawDocumentBufExt for crate::bson::RawDocumentBuf {
    fn append_ref<'a>(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>>,
    ) {
        self.append(key, value)
    }
}
