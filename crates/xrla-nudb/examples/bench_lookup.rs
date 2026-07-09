/// Synthetic benchmark: does NuDB key-file lookup latency depend on total store size?
///
/// Builds real, structurally valid NuDB stores (via `write_nudb_store`, the same writer
/// `xrla-import` uses) at increasing entry counts, and times `Shard::fetch` for the same
/// fixed set of probe keys against each one. A real O(1) hash-table lookup should show
/// flat per-lookup latency as entry count grows 5000x; a design that degrades with size
/// (e.g. growing spill chains, non-scaling bucket table) would show latency climbing with
/// entry count.
///
/// Run: cargo run --release -p xrla-nudb --example bench_lookup
use std::time::Instant;

use xrla_common::shamap::Hash256;
use xrla_nudb::keyfile::Shard;
use xrla_nudb::writer::write_nudb_store;

/// Deterministic pseudo-random 32-byte hash from an index (spread across the hash space
/// the same way real SHA512half node hashes are, for realistic bucket distribution).
fn hash_from_index(i: u64) -> Hash256 {
    let mut h = [0u8; 32];
    for (chunk, seed) in h.chunks_mut(8).zip([
        0x9E3779B97F4A7C15u64,
        0xC2B2AE3D27D4EB4Fu64,
        0x1656667B0F6D0A5Du64,
        0xFF51AFD7ED558CCDu64,
    ]) {
        let mixed = i.wrapping_mul(seed).rotate_left(17) ^ seed;
        chunk.copy_from_slice(&mixed.to_be_bytes());
    }
    h
}

fn main() -> anyhow::Result<()> {
    let sizes: [u64; 5] = [1_000, 10_000, 100_000, 1_000_000, 5_000_000];
    let num_probes = 200u64;
    let trials_per_probe = 50u64;
    let value_size = 200usize; // representative SHAMap node size

    println!(
        "{:>10}  {:>12}  {:>10}  {:>14}",
        "entries", "key_file_B", "build", "avg_lookup_us"
    );

    for &size in &sizes {
        let dir = std::env::temp_dir().join(format!("xrla_bench_lookup_{size}"));
        std::fs::create_dir_all(&dir)?;
        let dat_path = dir.join("nudb.dat");
        let key_path = dir.join("nudb.key");

        let value = vec![0xABu8; value_size];
        let mut entries: Vec<(Hash256, Vec<u8>)> = Vec::with_capacity(size as usize);
        for i in 0..size {
            entries.push((hash_from_index(i), value.clone()));
        }

        let build_start = Instant::now();
        write_nudb_store(&entries, &dat_path, &key_path)?;
        let build_elapsed = build_start.elapsed();

        let shard = Shard::open(&dat_path, &key_path)?;

        // Warm-up pass (not timed) — same for every size, so it doesn't bias the comparison.
        for i in 0..num_probes {
            shard.fetch(&hash_from_index(i))?;
        }

        let lookup_start = Instant::now();
        let mut total_lookups = 0u64;
        for _ in 0..trials_per_probe {
            for i in 0..num_probes {
                let got = shard.fetch(&hash_from_index(i))?;
                assert!(got.is_some(), "probe key {i} missing from a store it was written into");
                total_lookups += 1;
            }
        }
        let lookup_elapsed = lookup_start.elapsed();
        let avg_us = lookup_elapsed.as_nanos() as f64 / total_lookups as f64 / 1000.0;

        println!(
            "{:>10}  {:>12}  {:>10.2?}  {:>14.3}",
            size,
            std::fs::metadata(&key_path)?.len(),
            build_elapsed,
            avg_us
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    Ok(())
}
