from passlib.context import CryptContext
from app.core.config import settings
from datetime import datetime, timedelta
from jose import jwt, JWTError


SECRET_KEY = settings.SECRET_KEY
ALGORITHM = settings.ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.EXPIRE
REFRESH_TOKEN_EXPIRE_MINUTES = settings.REFRESH_EXPIRE


pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(password: str):
    return pwd.hash(password)

def verify_password(plain_password: str, hashed_password: str):
    return pwd.verify(plain_password, hashed_password)

def create_token(data: dict, expires_delta: timedelta = None, token_type: str = "access"):
    to_encode = data.copy()
    expiry = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expiry, "token_type": token_type})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_access_token(data: dict):
    return create_token(
        data,
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        token_type="access",
    )

def create_refresh_token(data: dict):
    return create_token(
        data,
        expires_delta=timedelta(minutes=REFRESH_TOKEN_EXPIRE_MINUTES),
        token_type="refresh",
    )

def decode_token(token: str):
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise ValueError("Invalid token")

def get_access_token_expiration():
    return ACCESS_TOKEN_EXPIRE_MINUTES