pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    MongoDB(mongodb::error::Error),
    Io(std::io::Error),
    Decoder(bson::DecoderError),
}

impl From<mongodb::error::Error> for Error {
    fn from(error: mongodb::error::Error) -> Self {
        Error::MongoDB(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io(error)
    }
}

impl From<bson::DecoderError> for Error {
    fn from(error: bson::DecoderError) -> Self {
        Error::Decoder(error)
    }
}
