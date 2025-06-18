use crate::bson::RawBson;

pub(crate) trait RawDocumentBufExt {
    fn append_err(&mut self, key: impl AsRef<str>, value: impl Into<RawBson>) -> RawResult<()>;

    fn append_ref_err<'a>(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>>,
    ) -> RawResult<()>;

    #[cfg(not(feature = "bson-3"))]
    fn decode_from_bytes(data: Vec<u8>) -> Self;
}

#[cfg(feature = "bson-3")]
impl RawDocumentBufExt for crate::bson::RawDocumentBuf {
    fn append_err(&mut self, key: impl AsRef<str>, value: impl Into<RawBson>) -> RawResult<()> {
        self.append(key, value.into())
    }

    fn append_ref_err<'a>(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>>,
    ) -> RawResult<()> {
        self.append(key, value)
    }
}

#[cfg(not(feature = "bson-3"))]
impl RawDocumentBufExt for crate::bson::RawDocumentBuf {
    fn append_err(&mut self, key: impl AsRef<str>, value: impl Into<RawBson>) -> RawResult<()> {
        self.append(key, value);
        Ok(())
    }

    fn append_ref_err<'a>(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>>,
    ) -> crate::error::Result<()> {
        self.append(key, value);
        Ok(())
    }

    fn decode_from_bytes(data: Vec<u8>) -> Self {
        Self::from_bytes(data)
    }
}

pub(crate) trait RawArrayBufExt: Sized {
    fn from_iter_err<V: Into<RawBson>, I: IntoIterator<Item = V>>(iter: I) -> RawResult<Self>;
}

#[cfg(feature = "bson-3")]
impl RawArrayBufExt for crate::bson::RawArrayBuf {
    fn from_iter_err<V: Into<RawBson>, I: IntoIterator<Item = V>>(iter: I) -> RawResult<Self> {
        Self::from_iter(iter.into_iter().map(|v| v.into()))
    }
}

#[cfg(not(feature = "bson-3"))]
impl RawArrayBufExt for crate::bson::RawArrayBuf {
    fn from_iter_err<V: Into<RawBson>, I: IntoIterator<Item = V>>(iter: I) -> RawResult<Self> {
        Ok(Self::from_iter(iter))
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
            pub(crate) use crate::bson::{$path3 as $name};

            #[cfg(not(feature = "bson-3"))]
            pub(crate) use crate::bson::{$path2 as $name};
        )+
    };
}

// Exported name => bson3 import | bson2 import
use_either! {
    RawResult                       => error::Result                    | raw::Result;
    RawError                        => error::Error                     | raw::Error;
    serialize_to_raw_document_buf   => serialize_to_raw_document_buf    | to_raw_document_buf;
    serialize_to_document           => serialize_to_document            | to_document;
    serialize_to_bson               => serialize_to_bson                | to_bson;
    deserialize_from_slice          => deserialize_from_slice           | from_slice;
    deserialize_from_document       => deserialize_from_document        | from_document;
    deserialize_from_bson           => deserialize_from_bson            | from_bson;
}
