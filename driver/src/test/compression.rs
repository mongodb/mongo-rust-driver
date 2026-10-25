use crate::{options::Compressor, test::get_client_options};

// Verifies that a compressor is properly set when a compression feature flag is enabled. Actual
// compression behavior is tested by running the driver test suite with a compressor configured;
// this test just makes sure our setup is correct.
#[tokio::test]
async fn test_compression_enabled() {
    let options = get_client_options().await;
    let compressors = options
        .compressors
        .as_ref()
        .expect("compressors client option should be set when compression is enabled");

    #[cfg(feature = "zstd-compression")]
    assert!(compressors
        .iter()
        .any(|compressor| matches!(compressor, Compressor::Zstd { .. })));

    #[cfg(feature = "zlib-compression")]
    assert!(compressors
        .iter()
        .any(|compressor| matches!(compressor, Compressor::Zlib { .. })));

    #[cfg(feature = "snappy-compression")]
    assert!(compressors
        .iter()
        .any(|compressor| matches!(compressor, Compressor::Snappy)));
}

mod contexts {
    use crate::{compression::decompress::decompress_message, options::Compressor};

    fn compressors() -> Vec<Compressor> {
        vec![
            #[cfg(feature = "zstd-compression")]
            Compressor::Zstd { level: None },
            #[cfg(feature = "zstd-compression")]
            Compressor::Zstd { level: Some(1) },
            #[cfg(feature = "zstd-compression")]
            Compressor::Zstd { level: Some(19) },
            #[cfg(feature = "zlib-compression")]
            Compressor::Zlib { level: None },
            #[cfg(feature = "zlib-compression")]
            Compressor::Zlib { level: Some(1) },
            #[cfg(feature = "zlib-compression")]
            Compressor::Zlib { level: Some(0) },
            #[cfg(feature = "snappy-compression")]
            Compressor::Snappy,
        ]
    }

    fn section(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed;
        (0..len)
            .map(|i| {
                if i % 64 < 48 {
                    b"{\"_id\": 1, \"status\": \"OPEN\"}"[i % 28]
                } else {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    (state >> 56) as u8
                }
            })
            .collect()
    }

    fn round_trip(compressor: &Compressor, flags: &[u8], section: &[u8]) -> Vec<u8> {
        let compressed = compressor.compress(flags, section).unwrap();
        decompress_message(&compressed, compressor.id(), flags.len() + section.len()).unwrap()
    }

    #[test]
    fn round_trips_with_alternating_compressors() {
        let flags = [2u8, 0, 0, 0];
        for (i, len) in [0, 1, 100, 1000, 70_000, 3_000_000].into_iter().enumerate() {
            let section = section(len, i as u64);
            for compressor in compressors() {
                let expected = [&flags[..], &section[..]].concat();
                assert_eq!(
                    round_trip(&compressor, &flags, &section),
                    expected,
                    "{compressor:?}, {len}"
                );
            }
        }
    }

    #[test]
    fn rejects_output_longer_than_declared() {
        let section = section(10_000, 7);
        for compressor in compressors()
            .into_iter()
            .filter(|compressor| compressor.name() != "snappy")
        {
            let compressed = compressor.compress(&[0; 4], &section).unwrap();
            assert!(
                decompress_message(&compressed, compressor.id(), section.len()).is_err(),
                "{compressor:?}"
            );
            assert_eq!(
                round_trip(&compressor, &[0; 4], &section).len(),
                section.len() + 4
            );
        }
    }

    #[test]
    fn recovers_after_corrupt_input() {
        let section = section(5_000, 11);
        for compressor in compressors() {
            let mut compressed = compressor.compress(&[0; 4], &section).unwrap();
            let middle = compressed.len() / 2;
            compressed[middle..].fill(0xff);
            assert!(
                decompress_message(&compressed, compressor.id(), section.len() + 4).is_err(),
                "{compressor:?}"
            );
            assert_eq!(
                round_trip(&compressor, &[0; 4], &section).len(),
                section.len() + 4
            );
        }
    }

    #[test]
    fn round_trips_across_threads() {
        let handles: Vec<_> = (0..8u64)
            .map(|seed| {
                std::thread::spawn(move || {
                    for i in 0..50 {
                        let section = section(
                            1 + (seed as usize * 7919 + i * 104_729) % 40_000,
                            seed + i as u64,
                        );
                        for compressor in compressors() {
                            let expected = [&[1u8, 0, 0, 0][..], &section[..]].concat();
                            assert_eq!(round_trip(&compressor, &[1, 0, 0, 0], &section), expected);
                        }
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
