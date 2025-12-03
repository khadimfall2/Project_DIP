import os

class Database:
    def __init__(self, filename):
        self.filename = filename
        # Check if file exists. If not, create an empty file.
        if not os.path.exists(filename):
            with open(filename, 'w') as f:
                pass

    def db_set(self, key, value):
        """
        Stores a key and value by appending them to the end of the file.
        This operation is very fast because it is sequential.
        """
        with open(self.filename, 'a') as f:
            # We follow the CSV format: key,value
            # Note: This simple format breaks if the value contains a newline.
            f.write(f"{key},{value}\n")

    def db_get(self, key):
        """
        Retrieves the value associated with a key.
        It scans the entire file to find the most recent occurrence.
        """
        last_found_value = None
        
        # Read the file from the beginning (Sequential Scan)
        with open(self.filename, 'r') as f:
            for line in f:
                # Remove the newline character at the end
                line = line.strip()
                if not line: continue
                
                # Split the line into key and value.
                # We split only on the first comma to allow commas in the value.
                parts = line.split(',', 1)
                
                if len(parts) == 2:
                    k, v = parts
                    if k == key:
                        # We found an occurrence. We update the variable.
                        # Since we read sequentially, the last update will be the correct one.
                        last_found_value = v
        
        return last_found_value

# --- Testing the Database ---
if __name__ == "__main__":
    db = Database("data.db")
    
    print("--- Writing Data ---")
    db.db_set("user_1", "Alice")
    db.db_set("user_2", "Bob")
    # Updating user_1: this appends a new line, it does not erase the old one.
    db.db_set("user_1", "Alice Cooper") 
    
    print("--- Reading Data ---")
    print(f"Value for user_1: {db.db_get('user_1')}") # Should be 'Alice Cooper'
    print(f"Value for user_2: {db.db_get('user_2')}") # Should be 'Bob'