use crate::error::{ErrorKind, Result};

/// Decompresses the given message with the decompression algorithm indicated by the given
/// ID. Returns an error if decompression fails, if the ID is unsupported, or if the message
/// decompresses to more than `uncompressed_size` bytes.
pub(crate) fn decompress_message(
    message: &[u8],
    compressor_id: u8,
    uncompressed_size: usize,
) -> Result<Vec<u8>> {
    if compressor_id == super::NOOP_COMPRESSOR_ID {
        return Ok(message.into());
    }

    #[cfg(feature = "zstd-compression")]
    if compressor_id == super::ZSTD_COMPRESSOR_ID {
        return decompress_zstd(message, uncompressed_size);
    }

    #[cfg(feature = "zlib-compression")]
    if compressor_id == super::ZLIB_COMPRESSOR_ID {
        return decompress_zlib(message, uncompressed_size);
    }

    #[cfg(feature = "snappy-compression")]
    if compressor_id == super::SNAPPY_COMPRESSOR_ID {
        return decompress_snappy(message);
    }

    let _ = uncompressed_size;
    Err(ErrorKind::InvalidResponse {
        message: format!("Unsupported compressor ID returned from the server: {compressor_id}"),
    }
    .into())
}

#[cfg(feature = "zstd-compression")]
fn decompress_zstd(message: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
    use std::cell::RefCell;

    use zstd::bulk::Decompressor;

    thread_local! {
        static CONTEXT: RefCell<Option<Decompressor<'static>>> = const { RefCell::new(None) };
    }

    CONTEXT
        .with_borrow_mut(|cached| {
            let mut decompressor = match cached.take() {
                Some(decompressor) => decompressor,
                None => Decompressor::new()?,
            };
            let decompressed = decompressor.decompress(message, uncompressed_size)?;
            *cached = Some(decompressor);
            Ok(decompressed)
        })
        .map_err(|error: std::io::Error| {
            ErrorKind::Internal {
                message: format!("Could not decompress message with zstd: {error}"),
            }
            .into()
        })
}

#[cfg(feature = "zlib-compression")]
fn decompress_zlib(message: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
    use std::cell::RefCell;

    use flate2::{Decompress, FlushDecompress, Status};

    thread_local! {
        static CONTEXT: RefCell<Option<Decompress>> = const { RefCell::new(None) };
    }

    CONTEXT.with_borrow_mut(|cached| {
        let mut decompress = match cached.take() {
            Some(mut decompress) => {
                decompress.reset(true);
                decompress
            }
            None => Decompress::new(true),
        };
        let mut decompressed = Vec::with_capacity(uncompressed_size);
        let status = decompress
            .decompress_vec(message, &mut decompressed, FlushDecompress::Finish)
            .map_err(|error| ErrorKind::Internal {
                message: format!("Could not decompress message with zlib: {error}"),
            })?;
        if status != Status::StreamEnd {
            return Err(ErrorKind::InvalidResponse {
                message: format!(
                    "The server's zlib message does not end within its uncompressed length of \
                     {uncompressed_size} bytes"
                ),
            }
            .into());
        }
        *cached = Some(decompress);
        Ok(decompressed)
    })
}

#[cfg(feature = "snappy-compression")]
fn decompress_snappy(message: &[u8]) -> Result<Vec<u8>> {
    use snap::raw::Decoder;

    let mut decoder = Decoder::new();
    decoder.decompress_vec(message).map_err(|error| {
        ErrorKind::Internal {
            message: format!("Could not decompress message with snappy: {error}"),
        }
        .into()
    })
}
