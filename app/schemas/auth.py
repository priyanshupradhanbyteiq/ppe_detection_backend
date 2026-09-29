from datetime import datetime
from pydantic import BaseModel,EmailStr

class UserRegister(BaseModel):
    name: str
    email: EmailStr
    password: str
    phone: str | None = None

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int

class RefreshToken(BaseModel):
    refresh_token: str