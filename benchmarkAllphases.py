import time
import csv
import os
from datetime import datetime
from simple_db import Database as AppendOnlyDB
from hash_index_db import DatabaseHashIndex as IndexedDB
from compaction import DatabaseCompaction as CompactionDB

# Import your three database classes

CSV_FILE = "benchmark_results.csv"

def benchmark_phase1():
    """Phase 1 (Append-Only): Write throughput and random read latency"""
    db = AppendOnlyDB('data.db')
    num_writes = 20000
    
    # Benchmark writes
    start = time.time()
    for i in range(num_writes):
        db.db_set(f"key_{i}", f"value_{i}")
    write_duration = time.time() - start
    write_throughput = num_writes / write_duration
    
    # Benchmark random reads
    num_reads = 1000
    start = time.time()
    for i in range(num_reads):
        db.db_get(f"key_{i % num_writes}")
    read_duration = time.time() - start
    read_latency = (read_duration / num_reads) * 1000  # ms
    
    print(f"Phase 1: Write {write_throughput:.0f} ops/sec, Read {1000/read_latency:.0f} ops/sec")
    return ("Phase 1", write_throughput, read_latency, 0)

def benchmark_phase2():
    """Phase 2 (Indexed): Write throughput and O(1) read latency"""
    db = IndexedDB('data_v2.db')
    num_writes = 20000
    
    # Benchmark writes
    start = time.time()
    for i in range(num_writes):
        db.db_set(f"key_{i}", f"value_{i}")
    write_duration = time.time() - start
    write_throughput = num_writes / write_duration
    
    # Benchmark indexed reads (O(1))
    num_reads = 1000
    start = time.time()
    for i in range(num_reads):
        db.db_get(f"key_{i % num_writes}")
    read_duration = time.time() - start
    read_latency = (read_duration / num_reads) * 1000  # ms
    
    print(f"Phase 2: Write {write_throughput:.0f} ops/sec, Read {1000/read_latency:.0f} ops/sec")
    return ("Phase 2", write_throughput, read_latency, 0)

def benchmark_phase3():
    """Phase 3 (Compaction): File size before/after, write/read throughput"""
    db = CompactionDB('data_v3.db')
    num_writes = 20000
    
    # Benchmark writes
    start = time.time()
    for i in range(num_writes):
        db.db_set(f"key_{i}", f"value_{i}")
    write_duration = time.time() - start
    write_throughput = num_writes / write_duration
    
    # File size before compaction
    size_before = db.get_file_size()
    
    # Compact
    db.compact()
    
    # File size after compaction
    size_after = db.get_file_size()
    compression_ratio = (size_before - size_after) / size_before * 100 if size_before > 0 else 0
    
    # Benchmark reads after compaction
    num_reads = 1000
    start = time.time()
    for i in range(num_reads):
        db.db_get(f"key_{i % num_writes}")
    read_duration = time.time() - start
    read_latency = (read_duration / num_reads) * 1000  # ms
    
    print(f"Phase 3: Write {write_throughput:.0f} ops/sec, Read {1000/read_latency:.0f} ops/sec, Compression {compression_ratio:.1f}%")
    return ("Phase 3", write_throughput, read_latency, compression_ratio)

def save_results(phase, write_throughput, read_latency, compression):
    """Append results to CSV file"""
    file_exists = os.path.isfile(CSV_FILE) and os.path.getsize(CSV_FILE) > 0
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Timestamp", "Phase", "Write Throughput (ops/sec)", "Read Latency (ms)", "Compression (%)"])
        writer.writerow([datetime.now().isoformat(), phase, f"{write_throughput:.2f}", f"{read_latency:.4f}", f"{compression:.2f}"])

if __name__ == "__main__":
    # Ensure the CSV exists and is cleared before running this benchmark.
    # Create the file if missing, otherwise truncate it so we start fresh.
    if not os.path.isfile(CSV_FILE):
        open(CSV_FILE, 'w').close()
    else:
        # Truncate existing file (keep it present) so old results are removed.
        open(CSV_FILE, 'w').close()

    print("Starting Database Benchmarks...\n")
    
    results = [
        benchmark_phase1(),
        benchmark_phase2(),
        benchmark_phase3()
    ]
    
    print("\nSaving results to CSV...")
    for phase, write_tp, read_lat, compression in results:
        save_results(phase, write_tp, read_lat, compression)
    
    print(f"Results saved to {CSV_FILE}")