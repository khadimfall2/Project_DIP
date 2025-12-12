import time
from simple_db import Database as AppendOnlyStore
from hash_index_db import DatabaseHashIndex as IndexedStore
from compaction import DatabaseCompaction as CompactionStore

def main():
    # Step 1: Choose store type
    print("\n=== Store Type Selection ===")
    print("1. Append-Only")
    print("2. Indexed")
    print("3. Compaction")
    
    store_choice = input("Choose store type (1/2/3): ").strip()
    
    store_map = {
        "1": ("Append-Only", AppendOnlyStore('data.db')),
        "2": ("Indexed", IndexedStore('data_v2.db')),
        "3": ("Compaction", CompactionStore('data_v3.db')),
    }
    
    if store_choice not in store_map:
        print("Invalid store type.")
        return
    
    store_name, store = store_map[store_choice]
    print(f"  Selected: {store_name} Store\n")
    
    # Step 2: Choose operation
    print("=== Operation Selection ===")
    print("1. Write")
    print("2. Read")
    print("3. Update")
    if store_choice == "3":
        print("4. Delete")   # Only available for Compaction

    operation_choice = input("Choose operation: ").strip()
    
    valid_ops = ["1", "2", "3"] + (["4"] if store_choice == "3" else [])
    if operation_choice not in valid_ops:
        print("Invalid operation.")
        return
    
    key = input("Enter key: ").strip()
    
    try:
        if operation_choice == "1":  # Write
            if hasattr(store, 'data') and key in store.data:
                print(f" Key '{key}' exists. Will overwrite.")
            value = input("Enter value: ").strip()
            
            start_time = time.time()
            store.db_set(key, value)
            elapsed_ms = (time.time() - start_time) * 1000
            print(f"  Write successful in {elapsed_ms:.2f}ms")
        
        elif operation_choice == "2":  # Read
            start_time = time.time()
            value = store.db_get(key)
            elapsed_ms = (time.time() - start_time) * 1000
            print(f"  Value: {value}")
            print(f"  Read successful in {elapsed_ms:.2f}ms")
        
        elif operation_choice == "3":  # Update
            value = input("Enter new value: ").strip()
            
            start_time = time.time()
            store.db_set(key, value)
            elapsed_ms = (time.time() - start_time) * 1000
            print(f"  Update successful in {elapsed_ms:.2f}ms")
    
        elif operation_choice == "4" and store_choice == "3":  # Delete (Compaction only)
            start_time = time.time()
            store.db_delete(key) 
            elapsed_ms = (time.time() - start_time) * 1000
            print(f"  Delete successful in {elapsed_ms:.2f}ms")

    except KeyError:
        if operation_choice == "2":
            print(f"Key not found: '{key}'")
        else:
            print(f"Cannot update non-existent key: '{key}'")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()