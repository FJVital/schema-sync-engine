from fastapi import FastAPI, File, UploadFile, Depends, HTTPException, status
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from jose import JWTError, jwt
from datetime import datetime, timedelta
import subprocess
import shutil
import os
import uuid
import stripe
import csv # <--- NEW STRICT CSV PARSER

import database

SECRET_KEY = "million-dollar-project-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# 🔴🔴🔴 YOUR STRIPE KEY GOES HERE 🔴🔴🔴
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")

app = FastAPI(title="Schema-Sync Engine API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = db.query(database.User).filter(database.User.email == email).first()
    if user is None:
        raise credentials_exception
    return user

@app.post("/register", tags=["Authentication"])
def register(email: str, password: str, db: Session = Depends(get_db)):
    db_user = db.query(database.User).filter(database.User.email == email).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    hashed_password = database.get_password_hash(password)
    new_user = database.User(email=email, hashed_password=hashed_password)
    db.add(new_user)
    db.commit()
    return {"message": f"User {email} created successfully!"}

@app.post("/token", tags=["Authentication"])
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(database.User).filter(database.User.email == form_data.username).first()
    if not user or not database.verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect email or password")
    access_token = create_access_token(data={"sub": user.email})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/quote", tags=["Billing Engine"])
async def generate_quote(file: UploadFile = File(...), current_user: database.User = Depends(get_current_user), db: Session = Depends(get_db)):
    with open("input.csv", "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        subprocess.run(['python', 'orchestrator.py'], check=True)
    except subprocess.CalledProcessError:
        raise HTTPException(status_code=500, detail="Pipeline execution failed.")

    if not os.path.exists("output.csv"):
        raise HTTPException(status_code=500, detail="Output file not generated.")

    # --- THE FIX: STRICT CSV PARSING & DATA EXTRACTION ---
    preview_data = []
    preview_headers = []
    row_count = 0
    
    with open("output.csv", "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        try:
            preview_headers = next(reader) # Grab the column names
        except StopIteration:
            pass
        
        for row in reader:
            row_count += 1
            if row_count <= 20: # Grab only the first 20 rows for preview
                preview_data.append(row)

    price_per_row = 0.01
    total_price = round(row_count * price_per_row, 2)

    job_id = str(uuid.uuid4())
    vault_path = f"vault_{job_id}.csv"
    os.rename("output.csv", vault_path)

    new_job = database.Job(
        id=job_id,
        user_id=current_user.id,
        row_count=row_count,
        price=total_price,
        is_paid=False,
        file_path=vault_path
    )
    db.add(new_job)
    db.commit()

    return {
        "message": "File processed successfully.",
        "job_id": job_id,
        "rows_detected": row_count,
        "total_price_usd": total_price,
        "preview_headers": preview_headers,
        "preview_data": preview_data, # Sending the data to the browser
        "status": "AWAITING_PAYMENT"
    }

@app.post("/checkout/{job_id}", tags=["Billing Engine"])
def create_stripe_checkout(job_id: str, current_user: database.User = Depends(get_current_user), db: Session = Depends(get_db)):
    job = db.query(database.Job).filter(database.Job.id == job_id, database.Job.user_id == current_user.id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    
    try:
        checkout_session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{
                'price_data': {
                    'currency': 'usd',
                    'product_data': {
                        'name': f'Schema-Sync Processing ({job.row_count} rows)',
                        'description': 'Automated formatting and schema mapping for Shopify.',
                    },
                    'unit_amount': int(job.price * 100), 
                },
                'quantity': 1,
            }],
            mode='payment',
            success_url="http://127.0.0.1:5500/index.html", 
            cancel_url="http://127.0.0.1:5500/index.html",
        )
        return {"checkout_url": checkout_session.url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/verify-payment/{job_id}", tags=["Billing Engine"])
def verify_payment(job_id: str, current_user: database.User = Depends(get_current_user), db: Session = Depends(get_db)):
    job = db.query(database.Job).filter(database.Job.id == job_id, database.Job.user_id == current_user.id).first()
    if job:
        job.is_paid = True
        db.commit()
    return {"message": "Vault Unlocked."}

@app.get("/download/{job_id}", tags=["Core Engine"])
def download_file(job_id: str, current_user: database.User = Depends(get_current_user), db: Session = Depends(get_db)):
    job = db.query(database.Job).filter(database.Job.id == job_id, database.Job.user_id == current_user.id).first()
    if not job or not job.is_paid:
        raise HTTPException(status_code=402, detail="Payment Required.")
    return FileResponse(job.file_path, media_type="text/csv", filename="shopify_ready_final.csv")