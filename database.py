from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
import bcrypt

# 1. Database Setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./schema_sync.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 2. Native Password Hashing Setup
def get_password_hash(password: str) -> str:
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))

# 3. Tables
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)

class Job(Base):
    __tablename__ = "jobs"
    id = Column(String, primary_key=True, index=True) 
    user_id = Column(Integer)
    row_count = Column(Integer)
    price = Column(Float)
    is_paid = Column(Boolean, default=False)
    file_path = Column(String)

# 4. Create the tables in the database
Base.metadata.create_all(bind=engine)