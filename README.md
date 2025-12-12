**Project DIP — Key-Value Store (Append-Only + Indexed Read + Compaction)**

Short description:
- **Summary:** A simple key-value store implementing append-only inserts, indexed reads, and background compaction. The implementation is delivered across three development phases; all phases are available in this repository for testing and benchmarking.

Getting started:
- **Run tests:** Execute the test runner to exercise the available database versions and their behavior.

Using the tests:
- **Test runner:** The test harness is [Test_db_versions.py](Test_db_versions.py). Run it from the repository root:

```bash
python Test_db_versions.py
```

- **What it does:** The script runs the included tests for the different database phases (phase 1..3). It will print results to the console.

Benchmarks:
- **Purpose:** Benchmarks measure performance of insert (append-only), indexed reads, and the compaction stage to compare phases and configuration choices.
- **Runner:** The main benchmark script is [BenchmarkComplete.py](BenchmarkComplete.py). To run the benchmark suite:

```bash
python BenchmarkComplete.py
```

- **Output:** Benchmark scripts typically write results into `benchmark_results.csv` and/or print timings to console. Use the [Plot_of_Benchmark.py](Plot_of_Benchmark.py) helper to visualize results after a run.

Files of interest:
- **simple_db.py:** Core append-only key-value store implementation (phase-level functionality).
- **hash_index_db.py:** Hash-based index layer used for fast reads.
- **compaction.py:** Compaction logic to merge/compact append-only segments and reclaim space.
- **benchmark.py / benchmarkAllphases.py / BenchmarkComplete.py:** Benchmark utilities and the complete benchmark runner.
- **Plot_of_Benchmark.py:** Plotting helper for benchmark CSV outputs.
- **Test_db_versions.py:** Test runner for the three phases.

