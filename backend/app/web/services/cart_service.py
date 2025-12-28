"""
Shopping cart service - xử lý logic giỏ hàng.
"""
from typing import Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.web.schemas.cart import AddToCartRequest, UpdateCartItemRequest, CartItemResponse, CartResponse


class CartService:
    """Service xử lý shopping cart."""
    
    @staticmethod
    async def get_or_create_cart(
        db: AsyncSession,
        user_id: int
    ) -> int:
        """
        Lấy hoặc tạo active cart cho user.
        
        Args:
            db: Database session
            user_id: User ID
            
        Returns:
            Cart ID
        """
        # Tìm active cart
        result = await db.execute(
            text("""
                SELECT id FROM shopping_carts
                WHERE user_id = :user_id AND status = 'active'
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"user_id": user_id}
        )
        
        cart_row = result.fetchone()
        
        if cart_row:
            return cart_row.id
        
        # Tạo cart mới
        result = await db.execute(
            text("""
                INSERT INTO shopping_carts (user_id, status)
                VALUES (:user_id, 'active')
                RETURNING id
            """),
            {"user_id": user_id}
        )
        await db.commit()
        
        cart_row = result.fetchone()
        return cart_row.id
    
    @staticmethod
    async def get_cart(
        db: AsyncSession,
        user_id: int
    ) -> Optional[CartResponse]:
        """
        Lấy cart với tất cả items và thông tin product.
        
        Args:
            db: Database session
            user_id: User ID
            
        Returns:
            CartResponse hoặc None nếu không có cart
        """
        # Lấy cart
        result = await db.execute(
            text("""
                SELECT id, user_id, status, created_at
                FROM shopping_carts
                WHERE user_id = :user_id AND status = 'active'
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"user_id": user_id}
        )
        
        cart_row = result.fetchone()
        
        if not cart_row:
            return None
        
        cart_id = cart_row.id
        
        # Lấy cart items với product info
        result = await db.execute(
            text("""
                SELECT 
                    ci.asin,
                    ci.quantity,
                    p.title,
                    i.parent_asin,
                    COALESCE(i.primary_image, p.primary_image) as primary_image
                FROM cart_items ci
                JOIN items i ON ci.asin = i.asin
                JOIN products p ON i.parent_asin = p.parent_asin
                WHERE ci.cart_id = :cart_id
                ORDER BY ci.asin
            """),
            {"cart_id": cart_id}
        )
        
        items = []
        total_items = 0
        
        for row in result.fetchall():
            items.append(CartItemResponse(
                asin=row.asin,
                quantity=row.quantity,
                title=row.title,
                parent_asin=row.parent_asin,
                primary_image=row.primary_image,
                price=None  # Price không có trong database, luôn set None
            ))
            total_items += row.quantity
        
        return CartResponse(
            cart_id=cart_id,
            user_id=cart_row.user_id,
            status=cart_row.status,
            created_at=cart_row.created_at,
            items=items,
            total_items=total_items
        )
    
    @staticmethod
    async def add_item(
        db: AsyncSession,
        user_id: int,
        item_data: AddToCartRequest
    ) -> tuple[bool, Optional[str]]:
        """
        Thêm item vào cart hoặc tăng quantity nếu đã có.
        
        Args:
            db: Database session
            user_id: User ID
            item_data: Thông tin item cần thêm
            
        Returns:
            Tuple (success, error_message)
        """
        # Kiểm tra item có tồn tại không
        result = await db.execute(
            text("SELECT asin FROM items WHERE asin = :asin"),
            {"asin": item_data.asin}
        )
        
        if not result.fetchone():
            return False, "Item không tồn tại"
        
        # Lấy hoặc tạo cart
        cart_id = await CartService.get_or_create_cart(db, user_id)
        
        # Kiểm tra item đã có trong cart chưa
        result = await db.execute(
            text("""
                SELECT quantity FROM cart_items
                WHERE cart_id = :cart_id AND asin = :asin
            """),
            {"cart_id": cart_id, "asin": item_data.asin}
        )
        
        existing_item = result.fetchone()
        
        if existing_item:
            # Cập nhật quantity
            new_quantity = existing_item.quantity + item_data.quantity
            await db.execute(
                text("""
                    UPDATE cart_items
                    SET quantity = :quantity
                    WHERE cart_id = :cart_id AND asin = :asin
                """),
                {
                    "cart_id": cart_id,
                    "asin": item_data.asin,
                    "quantity": new_quantity
                }
            )
        else:
            # Thêm item mới
            await db.execute(
                text("""
                    INSERT INTO cart_items (cart_id, asin, quantity)
                    VALUES (:cart_id, :asin, :quantity)
                """),
                {
                    "cart_id": cart_id,
                    "asin": item_data.asin,
                    "quantity": item_data.quantity
                }
            )
        
        await db.commit()
        return True, None
    
    @staticmethod
    async def update_item(
        db: AsyncSession,
        user_id: int,
        asin: str,
        item_data: UpdateCartItemRequest
    ) -> tuple[bool, Optional[str]]:
        """
        Cập nhật quantity của item trong cart.
        
        Args:
            db: Database session
            user_id: User ID
            asin: Item ASIN
            item_data: Thông tin cập nhật
            
        Returns:
            Tuple (success, error_message)
        """
        # Lấy cart
        result = await db.execute(
            text("""
                SELECT id FROM shopping_carts
                WHERE user_id = :user_id AND status = 'active'
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"user_id": user_id}
        )
        
        cart_row = result.fetchone()
        
        if not cart_row:
            return False, "Cart không tồn tại"
        
        cart_id = cart_row.id
        
        # Kiểm tra item có trong cart không
        result = await db.execute(
            text("""
                SELECT quantity FROM cart_items
                WHERE cart_id = :cart_id AND asin = :asin
            """),
            {"cart_id": cart_id, "asin": asin}
        )
        
        if not result.fetchone():
            return False, "Item không có trong cart"
        
        # Cập nhật quantity
        await db.execute(
            text("""
                UPDATE cart_items
                SET quantity = :quantity
                WHERE cart_id = :cart_id AND asin = :asin
            """),
            {
                "cart_id": cart_id,
                "asin": asin,
                "quantity": item_data.quantity
            }
        )
        
        await db.commit()
        return True, None
    
    @staticmethod
    async def remove_item(
        db: AsyncSession,
        user_id: int,
        asin: str
    ) -> tuple[bool, Optional[str]]:
        """
        Xóa item khỏi cart.
        
        Args:
            db: Database session
            user_id: User ID
            asin: Item ASIN
            
        Returns:
            Tuple (success, error_message)
        """
        # Lấy cart
        result = await db.execute(
            text("""
                SELECT id FROM shopping_carts
                WHERE user_id = :user_id AND status = 'active'
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"user_id": user_id}
        )
        
        cart_row = result.fetchone()
        
        if not cart_row:
            return False, "Cart không tồn tại"
        
        cart_id = cart_row.id
        
        # Xóa item
        result = await db.execute(
            text("""
                DELETE FROM cart_items
                WHERE cart_id = :cart_id AND asin = :asin
            """),
            {"cart_id": cart_id, "asin": asin}
        )
        
        await db.commit()
        
        if result.rowcount == 0:
            return False, "Item không có trong cart"
        
        return True, None
    
    @staticmethod
    async def clear_cart(
        db: AsyncSession,
        user_id: int
    ) -> tuple[bool, Optional[str]]:
        """
        Xóa tất cả items khỏi cart.
        
        Args:
            db: Database session
            user_id: User ID
            
        Returns:
            Tuple (success, error_message)
        """
        # Lấy cart
        result = await db.execute(
            text("""
                SELECT id FROM shopping_carts
                WHERE user_id = :user_id AND status = 'active'
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"user_id": user_id}
        )
        
        cart_row = result.fetchone()
        
        if not cart_row:
            return False, "Cart không tồn tại"
        
        cart_id = cart_row.id
        
        # Xóa tất cả items
        await db.execute(
            text("DELETE FROM cart_items WHERE cart_id = :cart_id"),
            {"cart_id": cart_id}
        )
        
        await db.commit()
        return True, None

