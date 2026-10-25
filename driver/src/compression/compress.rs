use crate::{
    error::{ErrorKind, Result},
    options::Compressor,
};

impl Compressor {
    pub(crate) fn compress(&self, flag_bytes: &[u8], section_bytes: &[u8]) -> Result<Vec<u8>> {
        let result = match *self {
            #[cfg(feature = "zstd-compression")]
            Self::Zstd { level } => compress_zstd(level, flag_bytes, section_bytes),
            #[cfg(feature = "zlib-compression")]
            Self::Zlib { level } => compress_zlib(level, flag_bytes, section_bytes),
            #[cfg(feature = "snappy-compression")]
            Self::Snappy => compress_snappy(flag_bytes, section_bytes),
        };

        result.map_err(|error| {
            ErrorKind::Internal {
                message: format!(
                    "Failed to compress message with {} compression: {}",
                    self.name(),
                    error
                ),
            }
            .into()
        })
    }
}

#[cfg(feature = "zstd-compression")]
fn compress_zstd(
    level: Option<i32>,
    flag_bytes: &[u8],
    section_bytes: &[u8],
) -> std::io::Result<Vec<u8>> {
    use std::cell::RefCell;

    use zstd::{
        zstd_safe::{
            zstd_sys::ZSTD_EndDirective,
            CCtx,
            CParameter,
            InBuffer,
            OutBuffer,
            ResetDirective,
        },
        DEFAULT_COMPRESSION_LEVEL,
    };

    // A context allocates and initializes several hundred KiB of state, so one is kept per thread
    // instead of being created for every message.
    thread_local! {
        static CONTEXT: RefCell<Option<CCtx<'static>>> = const { RefCell::new(None) };
    }

    let to_io_error = |code| std::io::Error::other(zstd::zstd_safe::get_error_name(code));
    let total_len = flag_bytes.len() + section_bytes.len();

    CONTEXT.with_borrow_mut(|cached| {
        let mut cctx = match cached.take() {
            Some(cctx) => cctx,
            None => CCtx::try_create()
                .ok_or_else(|| std::io::Error::other("failed to create a zstd context"))?,
        };
        cctx.reset(ResetDirective::SessionOnly)
            .map_err(to_io_error)?;
        cctx.set_parameter(CParameter::CompressionLevel(
            level.unwrap_or(DEFAULT_COMPRESSION_LEVEL),
        ))
        .map_err(to_io_error)?;
        cctx.set_pledged_src_size(Some(total_len as u64))
            .map_err(to_io_error)?;

        let mut compressed = Vec::with_capacity(zstd::zstd_safe::compress_bound(total_len));
        let mut output = OutBuffer::around(&mut compressed);
        let mut input = InBuffer::around(flag_bytes);
        while input.pos() < flag_bytes.len() {
            cctx.compress_stream2(&mut output, &mut input, ZSTD_EndDirective::ZSTD_e_continue)
                .map_err(to_io_error)?;
        }
        let mut input = InBuffer::around(section_bytes);
        while cctx
            .compress_stream2(&mut output, &mut input, ZSTD_EndDirective::ZSTD_e_end)
            .map_err(to_io_error)?
            != 0
        {
            if output.pos() == output.capacity() {
                return Err(std::io::Error::other(
                    "zstd output exceeded the compression bound",
                ));
            }
        }

        *cached = Some(cctx);
        Ok(compressed)
    })
}

#[cfg(feature = "zlib-compression")]
fn compress_zlib(
    level: Option<u32>,
    flag_bytes: &[u8],
    section_bytes: &[u8],
) -> std::io::Result<Vec<u8>> {
    use std::cell::RefCell;

    use flate2::{Compress, Compression, FlushCompress, Status};

    // See compress_zstd.
    thread_local! {
        static CONTEXT: RefCell<Option<(Compression, Compress)>> = const { RefCell::new(None) };
    }

    let level = match level {
        Some(level) => Compression::new(level),
        None => Compression::default(),
    };
    let total_len = flag_bytes.len() + section_bytes.len();

    CONTEXT.with_borrow_mut(|cached| {
        let mut compress = match cached.take() {
            Some((cached_level, mut compress)) if cached_level == level => {
                compress.reset();
                compress
            }
            _ => Compress::new(level, true),
        };

        let mut compressed = Vec::with_capacity(total_len + total_len / 1000 + 64);
        loop {
            if compressed.len() == compressed.capacity() {
                compressed.reserve(compressed.capacity());
            }
            let consumed = usize::try_from(compress.total_in()).map_err(std::io::Error::other)?;
            let status = match consumed.checked_sub(flag_bytes.len()) {
                None => compress.compress_vec(
                    &flag_bytes[consumed..],
                    &mut compressed,
                    FlushCompress::None,
                )?,
                Some(section_consumed) => compress.compress_vec(
                    &section_bytes[section_consumed..],
                    &mut compressed,
                    FlushCompress::Finish,
                )?,
            };
            if status == Status::StreamEnd {
                break;
            }
        }

        *cached = Some((level, compress));
        Ok(compressed)
    })
}

#[cfg(feature = "snappy-compression")]
fn compress_snappy(flag_bytes: &[u8], section_bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    use snap::raw::Encoder;

    let mut uncompressed = flag_bytes.to_vec();
    uncompressed.extend_from_slice(section_bytes);

    let mut encoder = Encoder::new();
    Ok(encoder.compress_vec(&uncompressed)?)
}
