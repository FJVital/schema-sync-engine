import sqlite3

USERS = {
    "fjvital@gmail.com": {
        "username": "fjvital@gmail.com",
        "hashed_password": "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6L6s5Wr7Hn/hA2.u",
        "stripe_customer_id": None
    }
}

def get_user(username: str):
    if username in USERS:
        return USERS[username]
    return None

def update_stripe_customer_id(username: str, customer_id: str):
    """Saves the Stripe Customer ID once it's created."""
    if username in USERS:
        USERS[username]["stripe_customer_id"] = customer_id
        return True
    return False