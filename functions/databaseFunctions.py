from tinydb import TinyDB, Query
import threading
import logging

log = logging.getLogger("db")

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
                log.exception(f"Error connecting to database: {e}")
                return None
    
    return db_connections[name]

def getDatabaseLock(name: str = "db"):
    """
    Returns the lock for the specified database.
    """
    global db_locks
    
    getDatabase(name)
    return db_locks.get(name)

def _withDatabase(name: str, op_label: str, fn, failure_value):
    db = getDatabase(name)
    db_lock = getDatabaseLock(name)
    if db is None or db_lock is None:
        return failure_value, False, "Database connection failed."

    with db_lock:
        try:
            result, detail = fn(db)
            return result, True, detail
        except Exception as e:
            log.exception(f"Error during {op_label} on {name}: {e}")
            return failure_value, False, f"Error during {op_label}: {e}"


def clearDatabase(type: str):
    _, ok, detail = _withDatabase(
        type, "clear",
        lambda db: (db.truncate() or None, "Database cleared successfully."),
        None,
    )
    return ok, detail


def insertData(data: dict, type: str):
    _, ok, detail = _withDatabase(
        type, "insert",
        lambda db: (db.insert(data) or None, "Data inserted successfully."),
        None,
    )
    return ok, detail


def getAllData(type: str):
    return _withDatabase(
        type, "getAll",
        lambda db: (db.all(), "Data retrieved successfully."),
        None,
    )


def upsertData(data: dict, type: str, key_fields: list[str]):
    query = Query()
    condition = None
    for field in key_fields:
        clause = query[field] == data[field]
        condition = clause if condition is None else (condition & clause)

    _, ok, detail = _withDatabase(
        type, "upsert",
        lambda db: (db.upsert(data, condition) or None, "Data upserted successfully."),
        None,
    )
    return ok, detail


def batchUpsertData(records: list[dict], type: str, key_fields: list[str]):
    def _do(db):
        existing = {}
        for record in db.all():
            key = tuple(record.get(f) for f in key_fields)
            existing[key] = record
        for d in records:
            key = tuple(d.get(f) for f in key_fields)
            existing[key] = d
        db.truncate()
        db.insert_multiple(existing.values())
        return None, f"Batch upserted {len(records)} records ({len(existing)} total)."

    _, ok, detail = _withDatabase(type, "batchUpsert", _do, None)
    return ok, detail


def removeStaleData(type: str, valid_keys: set, key_field: str):
    query = Query()

    def _do(db):
        stale = db.search(~query[key_field].test(lambda v: v in valid_keys))
        if stale:
            db.remove(~query[key_field].test(lambda v: v in valid_keys))
        return stale, f"Removed {len(stale)} stale records."

    return _withDatabase(type, "removeStale", _do, [])

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
                log.exception(f"Error closing database {name}: {e}")
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
                log.exception(f"Error closing database {name}: {e}")
        
        db_connections.clear()
        db_locks.clear()
        return True, f"Closed {closed_count} database connections."