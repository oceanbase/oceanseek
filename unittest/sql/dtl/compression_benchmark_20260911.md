# DTL codec microbenchmark, 2026-09-11

## Environment and scope

- Base: LakeBase master `87c7756d094`, with the `dtl_compress_func` changes.
- x86_64 Linux VM, Intel Xeon Platinum 8369B @ 2.90 GHz; 16 online vCPUs.
- Clang 17, repository release configuration (`-O2`, ASAN and BOLT disabled).
- DuckDB 1.4.4 official TPC-H/TPC-DS generators, SF 0.1, at most 65,536 rows
  per table, deterministic `ORDER BY ALL` followed by LIMIT; pipe-delimited CSV.
- Total input: 28,387,138 bytes across six tables. Five timed repetitions per
  compressed chunk, after one warmup. No file reads or equality comparisons in
  the timed sections. Throughput is single-threaded, based on input bytes.
- This is a sample-codec comparison on a development VM, not a full TPC SQL
  workload or an official TPC performance result. See [methodology](README.compression.md).

## 64 KiB chunks

Each suite aggregates its three sampled tables. Payload ratios are byte-weighted;
throughput is total input bytes divided by total codec time, reconstructed from
the per-table results (rounded to two decimal places in the raw CSV).

| Suite | Codec | Payload / input | Compress MiB/s | Decompress MiB/s |
| --- | --- | ---: | ---: | ---: |
| TPCH | none | 100.00% | NA | NA |
| TPCH | lz4_1.0 | 48.90% | 285.6 | 911.3 |
| TPCH | snappy_1.0 | 49.11% | 245.9 | 796.0 |
| TPCH | zstd_1.5.7 | 34.09% | 146.5 | 456.8 |
| TPCDS | none | 100.00% | NA | NA |
| TPCDS | lz4_1.0 | 55.58% | 205.3 | 771.7 |
| TPCDS | snappy_1.0 | 57.85% | 143.5 | 411.3 |
| TPCDS | zstd_1.5.7 | 34.58% | 111.8 | 358.9 |

For these samples, LZ4 has the highest compression and decompression throughput
among the actual compressors. ZSTD 1.5.7 reduces payload by a further ~30% (TPC-H)
and ~38% (TPC-DS) relative to LZ4, at higher codec CPU cost.
Snappy does not outperform LZ4 on these samples. These results support keeping
LZ4 as the default and making ZSTD optional for bandwidth-constrained workloads;
they do not establish the fastest setting for any particular SQL workload.

## Verification and artifacts

- The retained 72 dataset/codec/chunk-size rows cover four DTL settings
  (6 tables x 4 settings x 3 sizes). These measurements are unchanged from the
  2026-09-11 run. ZSTD 1.3.8 is now selectable again, but its previously excluded
  measurements are not restored here; current offline regression coverage is
  documented in [the configuration and test guide](README.compression.md).
- Every actual compressed chunk round-tripped byte-for-byte before and after timing.
- `none` reports raw bytes and no codec timing; it is not measured as memcpy.
- All 4 KiB, 64 KiB and 1 MiB results: [CSV](compression_benchmark_20260911.csv).
- Larger-sample provenance: [manifest](compression_benchmark_corpus_20260911.json).
- Small offline regression corpus: [manifest](compression_data/manifest.json).

No observer deployment, end-to-end SQL speedup, mixed-binary rolling upgrade,
multi-host wire traffic or production CPU utilization was measured in this run.
