pub(crate) trait RawDocumentBufExt {
    fn append_ref_err<'a>(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>>,
    ) -> crate::error::Result<()>;

    #[cfg(not(feature = "bson-3"))]
    fn decode_from_bytes(data: Vec<u8>) -> Self;
}

#[cfg(feature = "bson-3")]
impl RawDocumentBufExt for crate::bson::RawDocumentBuf {
    fn append_ref_err<'a>(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>>,
    ) -> crate::error::Result<()> {
        Ok(self.append(key, value)?)
    }

    #[cfg(not(feature = "bson-3"))]
    fn decode_from_bytes(data: Vec<u8>) -> Self {
        Self::from_bytes(data)
    }
}

#[cfg(not(feature = "bson-3"))]
impl RawDocumentBufExt for crate::bson::RawDocumentBuf {
    fn append_ref_err<'a>(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>>,
    ) -> crate::error::Result<()> {
        self.append(key, value);
        Ok(())
    }
}

#[cfg(not(feature = "bson-3"))]
pub(crate) trait RawDocumentExt: ?Sized {
    fn decode_from_bytes<D: AsRef<[u8]> + ?Sized>(data: &D) -> RawResult<&Self>;
}

#[cfg(not(feature = "bson-3"))]
impl RawDocumentExt for RawDocument {
    fn decode_from_bytes<D: AsRef<[u8]> + ?Sized>(data: &D) -> RawResult<&Self> {
        Self::from_bytes(data)
    }
}

#[cfg(not(feature = "bson-3"))]
pub(crate) trait DocumentExt {
    fn encode_to_vec(&self) -> RawResult<Vec<u8>>;
}

#[cfg(not(feature = "bson-3"))]
impl DocumentExt for Document {
    fn encode_to_vec(&self) -> RawResult<Vec<u8>> {
        let mut out = vec![];
        self.to_writer(&mut out)?;
        Ok(out)
    }
}

macro_rules! use_either {
    ($($name:ident => $path3:path | $path2:path);+;) => {
        $(
            #[cfg(feature = "bson-3")]
            pub(crate) use $path3 as $name;

            #[cfg(not(feature = "bson-3"))]
            pub(crate) use $path2 as $name;
        )+
    };
}

// Exported name => bson3 import | bson2 import
use_either! {
    RawResult => crate::bson::error::Result | crate::bson::raw::Result;
    RawError => crate::bson::error::Error | crate::bson::raw::Error;
    serialize_to_raw_document_buf => crate::bson::serialize_to_raw_document_buf | crate::bson::to_raw_document_buf;
    serialize_to_document => crate::bson::serialize_to_document | crate::bson::to_document;
    serialize_to_bson => crate::bson::serialize_to_bson | crate::bson::to_bson;
    deserialize_from_slice => crate::bson::deserialize_from_slice | crate::bson::from_slice;
    deserialize_from_document => crate::bson::deserialize_from_document | crate::bson::from_document;
    deserialize_from_bson => crate::bson::deserialize_from_bson | crate::bson::from_bson;
}
