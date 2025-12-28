"""
Authentication service - xử lý logic đăng ký và đăng nhập.
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.web.schemas.auth import RegisterRequest, LoginRequest, UserResponse
from app.web.utils.password import hash_password, verify_password
from app.web.utils.jwt import create_access_token


class AuthService:
    """Service xử lý authentication."""
    
    @staticmethod
    async def register(
        db: AsyncSession,
        register_data: RegisterRequest
    ) -> tuple[Optional[UserResponse], Optional[str]]:
        """
        Đăng ký user mới.
        
        Args:
            db: Database session
            register_data: Thông tin đăng ký
            
        Returns:
            Tuple (UserResponse, error_message)
            - Nếu thành công: (UserResponse, None)
            - Nếu lỗi: (None, error_message)
        """
        # Hash password
        password_hash = hash_password(register_data.password)
        
        # Tạo user mới
        from sqlalchemy import text
        
        try:
            # Insert user vào database
            result = await db.execute(
                text("""
                    INSERT INTO users (username, password_hash, phone_number)
                    VALUES (:username, :password_hash, :phone_number)
                    RETURNING id, username, phone_number, created_at, last_login
                """),
                {
                    "username": register_data.username,
                    "password_hash": password_hash,
                    "phone_number": register_data.phone_number
                }
            )
            await db.commit()
            
            user_row = result.fetchone()
            if user_row:
                user = UserResponse(
                    id=user_row.id,
                    username=user_row.username,
                    phone_number=user_row.phone_number,
                    created_at=user_row.created_at,
                    last_login=user_row.last_login
                )
                return user, None
            else:
                return None, "Không thể tạo user"
                
        except IntegrityError as e:
            await db.rollback()
            if "username" in str(e.orig).lower() or "unique" in str(e.orig).lower():
                return None, "Username đã tồn tại"
            return None, f"Lỗi đăng ký: {str(e)}"
        except Exception as e:
            await db.rollback()
            return None, f"Lỗi đăng ký: {str(e)}"
    
    @staticmethod
    async def login(
        db: AsyncSession,
        login_data: LoginRequest
    ) -> tuple[Optional[UserResponse], Optional[str]]:
        """
        Đăng nhập user.
        
        Args:
            db: Database session
            login_data: Thông tin đăng nhập
            
        Returns:
            Tuple (UserResponse, error_message)
            - Nếu thành công: (UserResponse, None)
            - Nếu lỗi: (None, error_message)
        """
        from sqlalchemy import text
        
        # Tìm user theo username
        result = await db.execute(
            text("""
                SELECT id, username, password_hash, phone_number, created_at, last_login
                FROM users
                WHERE username = :username
            """),
            {"username": login_data.username.lower().strip()}
        )
        
        user_row = result.fetchone()
        
        if not user_row:
            return None, "Username hoặc password không đúng"
        
        # Verify password
        if not verify_password(login_data.password, user_row.password_hash):
            return None, "Username hoặc password không đúng"
        
        # Update last_login
        await db.execute(
            text("""
                UPDATE users
                SET last_login = NOW()
                WHERE id = :user_id
            """),
            {"user_id": user_row.id}
        )
        await db.commit()
        
        # Tạo UserResponse
        user = UserResponse(
            id=user_row.id,
            username=user_row.username,
            phone_number=user_row.phone_number,
            created_at=user_row.created_at,
            last_login=datetime.utcnow()  # Sử dụng thời gian hiện tại vì đã update
        )
        
        return user, None
    
    @staticmethod
    def create_token(user: UserResponse) -> str:
        """
        Tạo JWT token cho user.
        
        Args:
            user: UserResponse object
            
        Returns:
            JWT token string
        """
        token_data = {
            "sub": str(user.id),  # subject (user_id)
            "username": user.username
        }
        return create_access_token(data=token_data)

