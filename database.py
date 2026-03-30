import sqlite3
import os

DB_FILE = "schema_sync.db"

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the database and creates tables if they don't exist."""
    conn = get_db_connection()
    
    # Create Users Table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            hashed_password TEXT,
            stripe_customer_id TEXT
        )
    ''')
    
    # Create Jobs Table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            input_path TEXT,
            output_path TEXT,
            price INTEGER,
            paid BOOLEAN DEFAULT 0
        )
    ''')

    # SEED THE MASTER TESTING USER
    user = conn.execute('SELECT * FROM users WHERE username = ?', ("fjvital@gmail.com",)).fetchone()
    if not user:
        conn.execute(
            'INSERT INTO users (username, hashed_password, stripe_customer_id) VALUES (?, ?, ?)',
            ("fjvital@gmail.com", "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6L6s5Wr7Hn/hA2.u", None)
        )
    
    conn.commit()
    conn.close()

# Run database initialization on startup
init_db()

# --- USER FUNCTIONS ---
def get_user(username: str):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
    conn.close()
    return dict(user) if user else None

def update_stripe_customer_id(username: str, customer_id: str):
    conn = get_db_connection()
    conn.execute('UPDATE users SET stripe_customer_id = ? WHERE username = ?', (customer_id, username))
    conn.commit()
    conn.close()
    return True

# --- JOB FUNCTIONS ---
def create_job(job_id: str, input_path: str, output_path: str, price: int):
    conn = get_db_connection()
    conn.execute(
        'INSERT INTO jobs (job_id, input_path, output_path, price, paid) VALUES (?, ?, ?, ?, ?)',
        (job_id, input_path, output_path, price, 0)
    )
    conn.commit()
    conn.close()

def get_job(job_id: str):
    conn = get_db_connection()
    job = conn.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,)).fetchone()
    conn.close()
    return dict(job) if job else None

def mark_job_paid(job_id: str):
    conn = get_db_connection()
    conn.execute('UPDATE jobs SET paid = 1 WHERE job_id = ?', (job_id,))
    conn.commit()
    conn.close()