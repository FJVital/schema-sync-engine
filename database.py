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

    # Create Jobs Table (includes original_filename for branded downloads)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            username TEXT,
            input_path TEXT,
            output_path TEXT,
            original_filename TEXT,
            price INTEGER,
            paid BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

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

def create_user(username: str, hashed_password: str):
    conn = get_db_connection()
    conn.execute(
        'INSERT INTO users (username, hashed_password, stripe_customer_id) VALUES (?, ?, ?)',
        (username, hashed_password, None)
    )
    conn.commit()
    conn.close()
    return True

def update_stripe_customer_id(username: str, customer_id: str):
    conn = get_db_connection()
    conn.execute('UPDATE users SET stripe_customer_id = ? WHERE username = ?', (customer_id, username))
    conn.commit()
    conn.close()
    return True

# --- JOB FUNCTIONS ---
def create_job(job_id: str, username: str, input_path: str, output_path: str, price: int, original_filename: str = "file"):
    conn = get_db_connection()
    conn.execute(
        'INSERT INTO jobs (job_id, username, input_path, output_path, original_filename, price, paid) VALUES (?, ?, ?, ?, ?, ?, ?)',
        (job_id, username, input_path, output_path, original_filename, price, 0)
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

def get_user_history(username: str):
    conn = get_db_connection()
    jobs = conn.execute('SELECT * FROM jobs WHERE username = ? ORDER BY created_at DESC', (username,)).fetchall()
    conn.close()
    return [dict(job) for job in jobs]