use arbitrary::Arbitrary;
use bson::RawDocumentBuf;

#[derive(Debug, Arbitrary)]
pub struct FuzzRawDocumentImpl {
    bytes: Vec<u8>,
}

impl From<FuzzRawDocumentImpl> for RawDocumentBuf {
    fn from(doc: FuzzRawDocumentImpl) -> Self {
        RawDocumentBuf::from_bytes(doc.bytes).unwrap_or_else(|_| {
            RawDocumentBuf::from_bytes(vec![5, 0, 0, 0, 0]).unwrap()
        })
    }
}

#[derive(Debug, Arbitrary)]
pub struct FuzzDocumentSequenceImpl(pub Vec<FuzzRawDocumentImpl>);

impl From<FuzzDocumentSequenceImpl> for Vec<RawDocumentBuf> {
    fn from(seq: FuzzDocumentSequenceImpl) -> Self {
        seq.0.into_iter()
            .map(Into::into)
            .collect()
    }
}
