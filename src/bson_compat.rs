#[cfg(feature = "bson-3")]
pub(crate) trait RawDocumentBufExt {
    fn append_ref<'a>(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>>,
    );
}

#[cfg(feature = "bson-3")]
impl RawDocumentBufExt for crate::bson::RawDocumentBuf {
    fn append_ref<'a>(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>>,
    ) {
        self.append(key, value)
    }
}

#[cfg(feature = "bson-3")]
pub(crate) use crate::bson::error::Result as RawResult;

#[cfg(not(feature = "bson-3"))]
pub(crate) use crate::bson::raw::Result as RawResult;

#[cfg(feature = "bson-3")]
pub(crate) use crate::bson::error::Error as RawError;

#[cfg(not(feature = "bson-3"))]
pub(crate) use crate::bson::raw::Error as RawError;
