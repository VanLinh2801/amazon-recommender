"""
Pydantic schemas cho authentication.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, Field, field_validator


class RegisterRequest(BaseModel):
    """Schema cho request đăng ký."""
    username: str = Field(..., min_length=3, max_length=50, description="Username")
    password: str = Field(..., min_length=6, description="Password (tối thiểu 6 ký tự)")
    phone_number: Optional[str] = Field(None, description="Số điện thoại")
    
    @field_validator('username')
    @classmethod
    def validate_username(cls, v: str) -> str:
        """Validate username không chứa khoảng trắng."""
        if ' ' in v:
            raise ValueError('Username không được chứa khoảng trắng')
        return v.strip().lower()


class LoginRequest(BaseModel):
    """Schema cho request đăng nhập."""
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")


class UserResponse(BaseModel):
    """Schema cho response thông tin user."""
    id: int
    username: str
    phone_number: Optional[str] = None
    created_at: datetime
    last_login: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class AuthResponse(BaseModel):
    """Schema cho response sau khi đăng nhập/đăng ký thành công."""
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class ErrorResponse(BaseModel):
    """Schema cho error response."""
    detail: str

