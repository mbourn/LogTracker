# dbinit.py - Initialize the database connectivity for logtracker

import os
import sqlite3 as lite

from logtracker import log

# Path to database
db = "/path/to/database/file.db"

def initDB():
    # If database doesn't yet exist, initialize it with the table structure
    if os.path.isfile(db):
        log("[-] Database found.  Skipping generation\n")
    else:
        log("[!] No database found.  Generating new database.\n")
        # Initialize database
        try:
            dbconn = lite.connect(db)
            dbc = dbconn.cursor()
            dbc.execute("CREATE TABLE devices (dev_name TEXT, first_seen TEXT, last_seen TEXT, freq INT, crit_sys INT, inactive INT, inactive_date TEXT, not_log INT, notlog_date TEXT, dev_id INTEGER PRIMARY KEY AUTOINCREMENT)")
            dbconn.commit()
            dbconn.close()
        except lite.Error as e:
            log("[!] Error: " + str(e) + "\n")
            log("[!] Quitting.\n\n")
            raise SystemExit

