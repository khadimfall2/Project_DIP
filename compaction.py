import os

class DatabaseCompaction:
    def __init__(self, filename):
        self.filename = filename
        self.index = {}
        
        # Create file if it doesn't exist
        if not os.path.exists(filename):
            with open(filename, 'w') as f:
                pass
        
        self.load_index()

    def load_index(self):
        """Rebuilds the in-memory index at startup."""
        self.index = {}
        if os.path.getsize(self.filename) == 0:
            return

        with open(self.filename, 'r') as f:
            while True:
                offset = f.tell()
                line = f.readline()
                if not line:
                    break
                
                parts = line.strip().split(',', 1)
                if len(parts) == 2:
                    # Update offset for this key.
                    # Old values are naturally overwritten here.
                    self.index[parts[0]] = offset

    def db_set(self, key, value):
        """Appends a line and updates the index."""
        with open(self.filename, 'a') as f:
            offset = f.tell()
            f.write(f"{key},{value}\n")
            self.index[key] = offset

    def db_get(self, key):
        """Fast O(1) lookup."""
        offset = self.index.get(key)
        if offset is None:
            return None
        
        with open(self.filename, 'r') as f:
            f.seek(offset)
            line = f.readline()
            k, v = line.strip().split(',', 1)
            return v

    # --- PHASE 3 NEW FEATURE: Compaction ---
    def compact(self):
        """
        Reads the current file and writes a new file containing 
        only the latest active values.
        """
        print("--- Starting Compaction ---")
        compact_filename = self.filename + ".compact"
        new_index = {}
        
        # 1. Open temporary file for writing
        with open(compact_filename, 'w') as f_new:
            # 2. Open old file for reading to fetch data
            with open(self.filename, 'r') as f_old:
                
                # 3. Iterate through the current index (contains only ACTIVE KEYS)
                for key, offset in self.index.items():
                    # Jump directly to the correct line in the old file
                    f_old.seek(offset)
                    line = f_old.readline()
                    
                    # Record position in the NEW file
                    new_offset = f_new.tell()
                    
                    # Write line to the new file
                    f_new.write(line)
                    
                    # Update the new in-memory index
                    new_index[key] = new_offset

        # 4. Atomic file replacement (Linux/MacOS)
        # The old "dirty" file is removed and replaced by the "clean" one
        os.replace(compact_filename, self.filename)
        
        # 5. Update in-memory index to point to the new file
        self.index = new_index
        print("--- Compaction Finished ---")

# --- Demo Test ---
if __name__ == "__main__":
    db_name = "data_v3.db"
    if os.path.exists(db_name):
        os.remove(db_name)
    
    db = DatabaseCompaction(db_name)
    
    # 1. Generate "garbage" data
    print("Writing 500 updates for key 'counter'...")
    for i in range(500):
        db.db_set("counter", f"value_{i}")
        
    size_before = os.path.getsize(db_name)
    print(f"File size BEFORE compaction: {size_before} bytes")
    print(f"Current value: {db.db_get('counter')}") # Should be value_499
    
    # 2. Run cleanup
    db.compact()
    
    # 3. Verify results
    size_after = os.path.getsize(db_name)
    print(f"File size AFTER compaction: {size_after} bytes")
    print(f"Value after compaction: {db.db_get('counter')}") # Should still be value_499