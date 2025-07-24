#[cfg(feature = "bson-3")]
pub(crate) type CStr = crate::bson::raw::CStr;
#[cfg(feature = "bson-3")]
pub(crate) type CString = crate::bson::raw::CString;
#[cfg(feature = "bson-3")]
pub(crate) use crate::bson::raw::cstr;

#[cfg(not(feature = "bson-3"))]
pub(crate) type CStr = str;
#[cfg(not(feature = "bson-3"))]
pub(crate) type CString = String;
#[cfg(not(feature = "bson-3"))]
macro_rules! cstr {
    ($text:literal) => {
        $text
    };
}
#[cfg(not(feature = "bson-3"))]
pub(crate) use cstr;

pub(crate) fn cstr_to_str(cs: &CStr) -> &str {
    #[cfg(feature = "bson-3")]
    {
        cs.as_str()
    }
    #[cfg(not(feature = "bson-3"))]
    {
        cs
    }
}

pub(crate) trait RawDocumentBufExt: Sized {
    fn append_ref_compat<'a>(
        &mut self,
        key: impl AsRef<CStr>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>> + 'a,
    );

    #[cfg(not(feature = "bson-3"))]
    fn decode_from_bytes(data: Vec<u8>) -> RawResult<Self>;
}

#[cfg(feature = "bson-3")]
impl RawDocumentBufExt for crate::bson::RawDocumentBuf {
    fn append_ref_compat<'a>(
        &mut self,
        key: impl AsRef<CStr>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>> + 'a,
    ) {
        self.append(key, value);
    }
}

#[cfg(not(feature = "bson-3"))]
impl RawDocumentBufExt for crate::bson::RawDocumentBuf {
    fn append_ref_compat<'a>(
        &mut self,
        key: impl AsRef<CStr>,
        value: impl Into<crate::bson::raw::RawBsonRef<'a>>,
    ) {
        self.append_ref(key, value)
    }

    fn decode_from_bytes(data: Vec<u8>) -> RawResult<Self> {
        Self::from_bytes(data)
    }
}

#[cfg(not(feature = "bson-3"))]
pub(crate) trait RawDocumentExt {
    fn decode_from_bytes<D: AsRef<[u8]> + ?Sized>(data: &D) -> RawResult<&Self>;
}

#[cfg(not(feature = "bson-3"))]
impl RawDocumentExt for crate::bson::RawDocument {
    fn decode_from_bytes<D: AsRef<[u8]> + ?Sized>(data: &D) -> RawResult<&Self> {
        Self::from_bytes(data)
    }
}

#[cfg(not(feature = "bson-3"))]
#[allow(dead_code)]
pub(crate) trait DocumentExt {
    fn encode_to_vec(&self) -> crate::bson::ser::Result<Vec<u8>>;
}

#[cfg(not(feature = "bson-3"))]
impl DocumentExt for crate::bson::Document {
    fn encode_to_vec(&self) -> crate::bson::ser::Result<Vec<u8>> {
        let mut out = vec![];
        self.to_writer(&mut out)?;
        Ok(out)
    }
}

macro_rules! use_either {
    ($($name:ident => $path3:path | $path2:path);+;) => {
        $(
            #[cfg(feature = "bson-3")]
            #[allow(unused_imports)]
            pub(crate) use crate::bson::{$path3 as $name};

            #[cfg(not(feature = "bson-3"))]
            #[allow(unused_imports)]
            pub(crate) use crate::bson::{$path2 as $name};
        )+
    };
}

// Exported name => bson3 import | bson2 import
use_either! {
    RawResult                       => error::Result                    | raw::Result;
    RawError                        => error::Error                     | raw::Error;
    DeError                         => error::Error                     | de::Error;
    SerError                        => error::Error                     | ser::Error;
    serialize_to_raw_document_buf   => serialize_to_raw_document_buf    | to_raw_document_buf;
    serialize_to_document           => serialize_to_document            | to_document;
    serialize_to_bson               => serialize_to_bson                | to_bson;
    deserialize_from_slice          => deserialize_from_slice           | from_slice;
    deserialize_from_document       => deserialize_from_document        | from_document;
    deserialize_from_bson           => deserialize_from_bson            | from_bson;
}
