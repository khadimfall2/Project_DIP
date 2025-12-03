import time
import os
import random

# --- 1. THE SLOW DATABASE (Phase 1 Logic) ---
class NaiveDB:
    def __init__(self, filename):
        self.filename = filename
        # Start with a fresh empty file
        if os.path.exists(filename): 
            os.remove(filename)
        with open(filename, 'w') as f: 
            pass

    def db_set(self, key, value):
        # Appends to file (Fast)
        with open(self.filename, 'a') as f:
            f.write(f"{key},{value}\n")

    def db_get(self, key):
        # Scans the whole file (Very Slow: O(n))
        last_val = None
        with open(self.filename, 'r') as f:
            for line in f:
                parts = line.strip().split(',', 1)
                if len(parts) == 2 and parts[0] == key:
                    last_val = parts[1]
        return last_val

# --- 2. THE FAST DATABASE (Phase 3 Logic) ---
class IndexedDB:
    def __init__(self, filename):
        self.filename = filename
        self.index = {}
        # Start with a fresh empty file
        if os.path.exists(filename): 
            os.remove(filename)
        with open(filename, 'w') as f: 
            pass

    def db_set(self, key, value):
        # Appends to file AND updates memory index
        with open(self.filename, 'a') as f:
            offset = f.tell()
            f.write(f"{key},{value}\n")
            self.index[key] = offset

    def db_get(self, key):
        # Uses index to jump directly to data (Very Fast: O(1))
        offset = self.index.get(key)
        if offset is None: 
            return None
        with open(self.filename, 'r') as f:
            f.seek(offset)
            return f.readline().strip().split(',', 1)[1]

# --- 3. THE RACE ---
def run_benchmark():
    # Settings
    N_KEYS = 50000        # Number of records to insert
    N_READS_FAST = 50000  # Number of reads for the fast DB
    N_READS_SLOW = 500    # Number of reads for the slow DB (Lower because it is too slow!)

    print(f"--- BENCHMARK STARTING ({N_KEYS} keys) ---")

    # === ROUND 1: Naive DB (Phase 1) ===
    print("\n[Testing Phase 1: Naive DB]")
    db_naive = NaiveDB("bench_naive.db")
    
    # Measure Write Speed
    start = time.time()
    for i in range(N_KEYS):
        db_naive.db_set(f"key_{i}", f"val_{i}")
    duration = time.time() - start
    print(f"WRITE Speed: {N_KEYS / duration:.0f} ops/sec")

    # Measure Read Speed (Limited to 500 reads)
    start = time.time()
    for i in range(N_READS_SLOW):
        # Pick a random key to force a full scan
        target = f"key_{random.randint(0, N_KEYS-1)}"
        db_naive.db_get(target)
    duration = time.time() - start
    print(f"READ Speed:  {N_READS_SLOW / duration:.0f} ops/sec (Warning: Very Slow!)")

    # === ROUND 2: Indexed DB (Phase 3) ===
    print("\n[Testing Phase 3: Indexed DB]")
    db_fast = IndexedDB("bench_fast.db")
    
    # Measure Write Speed
    start = time.time()
    for i in range(N_KEYS):
        db_fast.db_set(f"key_{i}", f"val_{i}")
    duration = time.time() - start
    print(f"WRITE Speed: {N_KEYS / duration:.0f} ops/sec")

    # Measure Read Speed (Full 50,000 reads)
    start = time.time()
    for i in range(N_READS_FAST):
        target = f"key_{random.randint(0, N_KEYS-1)}"
        db_fast.db_get(target)
    duration = time.time() - start
    print(f"READ Speed:  {N_READS_FAST / duration:.0f} ops/sec")

    # Cleanup files
    if os.path.exists("bench_naive.db"): os.remove("bench_naive.db")
    if os.path.exists("bench_fast.db"): os.remove("bench_fast.db")

if __name__ == "__main__":
    run_benchmark()