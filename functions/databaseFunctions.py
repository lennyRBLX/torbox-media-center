from tinydb import TinyDB, Query
import threading
import logging

db_connections = {}
db_locks = {}
global_lock = threading.Lock()

def getDatabase(name: str = "db"):
    """
    Returns the TinyDB database instance with thread-safe storage.
    Uses a connection pool pattern to avoid creating multiple connections.
    """
    global db_connections, db_locks # global cause I'm lazy
    
    with global_lock:
        if name not in db_connections:
            try:
                db_connections[name] = TinyDB(f"{name}.json")
                db_locks[name] = threading.Lock()
            except Exception as e:
                logging.error(f"Error connecting to the database: {e}")
                return None
    
    return db_connections[name]

def getDatabaseLock(name: str = "db"):
    """
    Returns the lock for the specified database.
    """
    global db_locks
    
    getDatabase(name)
    return db_locks.get(name)

def clearDatabase(type: str):
    """
    Clears the entire database with thread safety.
    """
    db = getDatabase(type)
    db_lock = getDatabaseLock(type)
    
    if db is None or db_lock is None:
        return False, "Database connection failed."
    
    with db_lock:
        try:
            db.truncate()
            return True, "Database cleared successfully."
        except Exception as e:
            return False, f"Error clearing the database: {e}"
    
def insertData(data: dict, type: str):
    """
    Inserts data into the database with thread safety.
    """
    db = getDatabase(type)
    db_lock = getDatabaseLock(type)
    
    if db is None or db_lock is None:
        return False, "Database connection failed."
    
    with db_lock:
        try:
            db.insert(data)
            return True, "Data inserted successfully."
        except Exception as e:
            return False, f"Error inserting data. {e}"
    
def getAllData(type: str):
    """
    Retrieves all data from the database with thread safety.
    """
    db = getDatabase(type)
    db_lock = getDatabaseLock(type)
    
    if db is None or db_lock is None:
        return None, False, "Database connection failed."
    
    with db_lock:
        try:
            data = db.all()
            return data, True, "Data retrieved successfully."
        except Exception as e:
            return None, False, f"Error retrieving data. {e}"

def upsertData(data: dict, type: str, key_fields: list[str]):
    db = getDatabase(type)
    db_lock = getDatabaseLock(type)

    if db is None or db_lock is None:
        return False, "Database connection failed."

    query = Query()
    condition = None
    for field in key_fields:
        clause = query[field] == data[field]
        condition = clause if condition is None else (condition & clause)

    with db_lock:
        try:
            db.upsert(data, condition)
            return True, "Data upserted successfully."
        except Exception as e:
            return False, f"Error upserting data: {e}"

def batchUpsertData(records: list[dict], type: str, key_fields: list[str]):
    db = getDatabase(type)
    db_lock = getDatabaseLock(type)

    if db is None or db_lock is None:
        return False, "Database connection failed."

    with db_lock:
        try:
            existing = {}
            for record in db.all():
                key = tuple(record.get(f) for f in key_fields)
                existing[key] = record

            for data in records:
                key = tuple(data.get(f) for f in key_fields)
                existing[key] = data

            db.truncate()
            db.insert_multiple(existing.values())
            return True, f"Batch upserted {len(records)} records ({len(existing)} total)."
        except Exception as e:
            return False, f"Error batch upserting: {e}"

def removeStaleData(type: str, valid_keys: set, key_field: str):
    db = getDatabase(type)
    db_lock = getDatabaseLock(type)

    if db is None or db_lock is None:
        return False, "Database connection failed."

    query = Query()
    with db_lock:
        try:
            db.remove(~query[key_field].test(lambda v: v in valid_keys))
            return True, "Stale data removed."
        except Exception as e:
            return False, f"Error removing stale data: {e}"

def closeDatabase(name: str = "db"):
    """
    Closes a database connection and removes it from the cache.
    """
    global db_connections, db_locks
    
    with global_lock:
        if name in db_connections:
            try:
                db_connections[name].close()
                del db_connections[name]
                del db_locks[name]
                return True, "Database closed successfully."
            except Exception as e:
                return False, f"Error closing database: {e}"
        return True, "Database was not open."

def closeAllDatabases():
    """
    Closes all database connections.
    """
    global db_connections, db_locks
    
    with global_lock:
        closed_count = 0
        for name in list(db_connections.keys()):
            try:
                db_connections[name].close()
                closed_count += 1
            except Exception as e:
                logging.error(f"Error closing database {name}: {e}")
        
        db_connections.clear()
        db_locks.clear()
        return True, f"Closed {closed_count} database connections."