import os

class DatabaseHashIndex:
    def __init__(self, filename):
        self.filename = filename
        self.index = {}  # The in-memory Hash Map (Key -> Byte Offset)
        
        # If file doesn't exist, create it
        if not os.path.exists(filename):
            with open(filename, 'w') as f:
                pass
        
        # Build the index from existing data on startup
        self.load_index()

    def load_index(self):
        """
        Reads the file sequentially to populate the in-memory index.
        This happens only once when the database starts.
        """
        if os.path.getsize(self.filename) == 0:
            return

        with open(self.filename, 'r') as f:
            while True:
                # 1. Get current position (Byte Offset) BEFORE reading the line
                offset = f.tell()
                line = f.readline()
                
                if not line:
                    break # End of file
                
                # 2. Extract Key
                # We use parsing similar to Phase 1
                parts = line.strip().split(',', 1)
                if len(parts) == 2:
                    key = parts[0]
                    # 3. Map Key -> Offset
                    # If key appears again later, this will overwrite the old offset
                    # effectively pointing to the "latest" value.
                    self.index[key] = offset

    def db_set(self, key, value):
        """
        Appends data to file AND updates the in-memory index.
        """
        with open(self.filename, 'a') as f:
            # 1. Get the current position (where we are about to write)
            offset = f.tell()
            
            # 2. Write the record
            f.write(f"{key},{value}\n")
            
            # 3. Update the index immediately
            self.index[key] = offset

    def db_get(self, key):
        """
        Retrieves value using O(1) index lookup.
        No more scanning the whole file!
        """
        # 1. Look up the offset in memory
        offset = self.index.get(key)
        
        # If key is not in index, it's not in the DB
        if offset is None:
            return None
        
        # 2. Jump directly to the offset on disk
        with open(self.filename, 'r') as f:
            f.seek(offset) # The Magic Jump
            line = f.readline()
            
            # 3. Parse and return value
            k, v = line.strip().split(',', 1)
            return v

# --- Testing Phase 2 ---
if __name__ == "__main__":
    # We use a DIFFERENT file name to avoid mixing with Phase 1
    db = DatabaseHashIndex("data_v2.db")
    
    print("--- Writing Data (Indexed) ---")
    db.db_set("user_1", "Alice")
    db.db_set("user_2", "Bob")
    db.db_set("user_1", "Alice Cooper") # Update
    
    print("--- Reading Data (Indexed) ---")
    print(f"Value for user_1: {db.db_get('user_1')}")
    print(f"Value for user_2: {db.db_get('user_2')}")
    
    print("--- Internal Index Structure ---")
    # This shows what is in the RAM
    print(db.index)