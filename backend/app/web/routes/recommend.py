"""
Recommendation API routes
========================

API endpoints để lấy recommendations cho user.
"""

import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.web.schemas.item import RecommendResponse, RecommendedItemResponse
from app.web.schemas.auth import UserResponse
from app.web.services.recommendation_service import get_recommendation_service
from app.web.services.item_service import ItemService
from app.web.services.user_history_service import UserHistoryService
from app.web.utils.database import get_db
from app.web.utils.auth_middleware import get_current_user, get_optional_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/recommend", tags=["recommendations"])


@router.get(
    "/",
    response_model=RecommendResponse,
    summary="Get recommendations for user",
    description="""
    Lấy recommendations cho user sử dụng full pipeline:
    Recall -> Ranking -> Re-ranking
    
    Yêu cầu authentication (user_id từ JWT token).
    """
)
async def get_recommendations(
    top_n: Optional[int] = Query(20, ge=1, le=100, description="Number of recommendations"),
    current_user: UserResponse = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Lấy recommendations cho user hiện tại.
    
    Args:
        top_n: Số lượng recommendations
        current_user: Current user (từ JWT token)
        db: Database session
        
    Returns:
        RecommendResponse
    """
    try:
        # Get recommendation service
        recommendation_service = get_recommendation_service(top_n=top_n)
        
        # Lấy amazon_user_id từ database (cần cho MF recall)
        from sqlalchemy import text
        result = await db.execute(
            text("SELECT amazon_user_id FROM users WHERE id = :user_id"),
            {"user_id": current_user.id}
        )
        user_row = result.fetchone()
        amazon_user_id = user_row.amazon_user_id if user_row and user_row.amazon_user_id else None
        
        # Nếu không có amazon_user_id, dùng user.id làm fallback (sẽ chỉ có Popularity recall)
        user_id_for_recall = amazon_user_id if amazon_user_id else str(current_user.id)
        logger.info(
            f"User {current_user.id}: "
            f"amazon_user_id={amazon_user_id}, "
            f"using user_id_for_recall={user_id_for_recall}"
        )
        
        # Lấy user history để recommend items tương tự
        user_reference_items = []
        try:
            user_reference_items = await UserHistoryService.get_user_reference_items(
                db=db,
                user_id=current_user.id,
                include_cart=True,
                include_purchases=True,
                include_views=False,  # Chỉ dùng cart và purchases
                limit_per_source=10
            )
            logger.info(f"User {current_user.id} has {len(user_reference_items)} reference items from history")
        except Exception as e:
            logger.warning(f"Error getting user history: {e}, continuing without history")
            user_reference_items = []
        
        # Generate recommendations (vẫn có recommendations từ Popularity recall nếu không có history)
        logger.info(f"Generating recommendations for user {current_user.id} (amazon_user_id: {user_id_for_recall})")
        reranked_items, recall_count, ranking_count = recommendation_service.generate_recommendations(
            user_id=user_id_for_recall,
            top_n=top_n,
            user_reference_items=user_reference_items if user_reference_items else None,
            content_score_boost=1.5  # Boost nhẹ cho homepage
        )
        
        logger.info(
            f"Recommendation generation result: "
            f"reranked_items={len(reranked_items)}, "
            f"recall_count={recall_count}, "
            f"ranking_count={ranking_count}"
        )
        
        if not reranked_items:
            # Return empty recommendations
            return RecommendResponse(
                user_id=current_user.id,
                recommendations=[],
                total=0,
                recall_count=0,
                ranking_count=0
            )
        
        # Lấy ASINs từ recommendations
        asins = [item.item_id for item in reranked_items]
        logger.info(f"Fetching details for {len(asins)} items from database")
        
        # Lấy item details từ database với error handling
        items = []
        try:
            items = await ItemService.get_items_by_asins(db, asins)
            logger.info(f"Successfully fetched {len(items)} items from database")
        except Exception as e:
            logger.error(f"Error fetching items from database: {e}", exc_info=True)
            # Rollback transaction
            try:
                await db.rollback()
            except Exception:
                pass
            
            # Fallback: query từng item
            logger.info("Trying fallback: query items one by one")
            items = []
            for asin in asins[:top_n * 2]:  # Giới hạn để tránh quá nhiều queries
                try:
                    item = await ItemService.get_item_by_asin(db, asin)
                    if item:
                        items.append(item)
                except Exception as item_error:
                    logger.warning(f"Error fetching item {asin}: {item_error}")
                    try:
                        await db.rollback()
                    except Exception:
                        pass
                    continue
        
        if not items:
            logger.warning(f"No items found in database for {len(asins)} ASINs. ASINs: {asins[:10]}")
            return RecommendResponse(
                user_id=current_user.id,
                recommendations=[],
                total=0,
                recall_count=recall_count,
                ranking_count=ranking_count
            )
        
        # Tạo mapping ASIN -> ItemResponse
        item_map = {item.asin: item for item in items}
        
        # Deduplication theo parent_asin ở API layer (để tránh recommend nhiều variants cùng sản phẩm)
        seen_parent_asins = set()
        recommendations = []
        rank_counter = 1
        
        for reranked_item in reranked_items:
            item = item_map.get(reranked_item.item_id)
            
            if item:
                # Check duplicate theo parent_asin
                parent_asin = item.parent_asin or item.asin
                
                if parent_asin in seen_parent_asins:
                    logger.debug(
                        f"Skipping duplicate parent_asin: {parent_asin} "
                        f"(asin: {item.asin}, score: {reranked_item.adjusted_score:.4f})"
                    )
                    continue
                
                seen_parent_asins.add(parent_asin)
                
                recommendations.append(RecommendedItemResponse(
                    asin=item.asin,
                    title=item.title,
                    main_category=item.main_category,
                    avg_rating=item.avg_rating,
                    rating_number=item.rating_number,
                    primary_image=item.primary_image,
                    score=reranked_item.adjusted_score,
                    rank=rank_counter,
                    applied_rules=reranked_item.applied_rules
                ))
                rank_counter += 1
                
                # Giới hạn số lượng recommendations
                if len(recommendations) >= top_n:
                    break
        
        logger.info(
            f"Generated {len(recommendations)} recommendations for user {current_user.id}: "
            f"recall={recall_count}, ranking={ranking_count}, "
            f"items_found={len(items)}/{len(asins)}"
        )
        
        return RecommendResponse(
            user_id=current_user.id,
            recommendations=recommendations,
            total=len(recommendations),
            recall_count=recall_count,
            ranking_count=ranking_count
        )
        
    except Exception as e:
        logger.error(f"Error generating recommendations: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate recommendations: {str(e)}"
        )


@router.get(
    "/similar/{asin}",
    response_model=RecommendResponse,
    summary="Get similar items recommendations",
    description="""
    Lấy recommendations cho items tương tự dựa trên category của item hiện tại.
    Không yêu cầu authentication.
    """
)
async def get_similar_items(
    asin: str,
    top_n: Optional[int] = Query(10, ge=1, le=50, description="Number of recommendations"),
    current_user: Optional[UserResponse] = Depends(get_optional_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Lấy recommendations cho items tương tự dựa trên category.
    
    Args:
        asin: Item ASIN để tìm items tương tự
        top_n: Số lượng recommendations
        current_user: Current user (optional)
        db: Database session
        
    Returns:
        RecommendResponse
    """
    try:
        # Lấy thông tin item hiện tại
        item = await ItemService.get_item_by_asin(db, asin)
        if not item:
            raise HTTPException(status_code=404, detail=f"Item {asin} not found")
        
        # Get recommendation service
        recommendation_service = get_recommendation_service(top_n=top_n)
        
        # Generate recommendations CHỈ dựa trên Content-based recall (items tương tự)
        # KHÔNG dùng MF recall và Popularity recall cho trang chi tiết
        reranked_items, recall_count, ranking_count = recommendation_service.generate_recommendations(
            user_id=str(current_user.id) if current_user else "0",
            top_n=top_n,
            reference_item_id=asin,  # Item hiện tại để tính similarity
            content_score_boost=2.5,  # Boost mạnh cho product detail page
            use_only_content_recall=True  # CHỈ dùng Content-based recall, không dùng MF/Popularity
        )
        
        if not reranked_items:
            # Fallback: tìm items cùng category nếu không có recommendations
            target_category = item.category or item.main_category
            if target_category:
                similar_items, total = await ItemService.search_items(
                    db=db,
                    category=target_category,
                    page=1,
                    page_size=top_n
                )
                filtered_items = [item for item in similar_items if item.asin != asin][:top_n]
                
                recommendations = []
                for idx, item in enumerate(filtered_items):
                    recommendations.append(RecommendedItemResponse(
                        asin=item.asin,
                        title=item.title,
                        main_category=item.main_category,
                        avg_rating=item.avg_rating,
                        rating_number=item.rating_number,
                        primary_image=item.primary_image,
                        score=float(item.avg_rating or 0) * (item.rating_number or 1),
                        rank=idx + 1,
                        applied_rules=["similar_category_fallback"]
                    ))
                
                return RecommendResponse(
                    user_id=current_user.id if current_user else 0,
                    recommendations=recommendations,
                    total=len(recommendations),
                    recall_count=len(similar_items),
                    ranking_count=len(recommendations)
                )
            else:
                return RecommendResponse(
                    user_id=current_user.id if current_user else 0,
                    recommendations=[],
                    total=0,
                    recall_count=0,
                    ranking_count=0
                )
        
        # Lấy ASINs từ recommendations
        asins = [item.item_id for item in reranked_items]
        
        # Lấy item details từ database
        items = await ItemService.get_items_by_asins(db, asins)
        
        # Tạo mapping ASIN -> ItemResponse
        item_map = {item.asin: item for item in items}
        
        # Tạo RecommendedItemResponse list
        recommendations = []
        for reranked_item in reranked_items:
            item = item_map.get(reranked_item.item_id)
            
            if item:
                recommendations.append(RecommendedItemResponse(
                    asin=item.asin,
                    title=item.title,
                    main_category=item.main_category,
                    avg_rating=item.avg_rating,
                    rating_number=item.rating_number,
                    primary_image=item.primary_image,
                    score=reranked_item.adjusted_score,
                    rank=reranked_item.rank_position,
                    applied_rules=reranked_item.applied_rules
                ))
        
        logger.info(
            f"Generated {len(recommendations)} similar items for {asin} "
            f"using content-based recommendations"
        )
        
        return RecommendResponse(
            user_id=current_user.id if current_user else 0,
            recommendations=recommendations,
            total=len(recommendations),
            recall_count=recall_count,
            ranking_count=ranking_count
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating similar items for {asin}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate similar items: {str(e)}"
        )


@router.get(
    "/{user_id}",
    response_model=RecommendResponse,
    summary="Get recommendations for specific user (admin)",
    description="""
    Lấy recommendations cho một user cụ thể (có thể dùng cho admin hoặc testing).
    """
)
async def get_recommendations_for_user(
    user_id: int,
    top_n: Optional[int] = Query(20, ge=1, le=100, description="Number of recommendations"),
    db: AsyncSession = Depends(get_db)
):
    """
    Lấy recommendations cho một user cụ thể.
    
    Args:
        user_id: User ID
        top_n: Số lượng recommendations
        db: Database session
        
    Returns:
        RecommendResponse
    """
    try:
        # Get recommendation service
        recommendation_service = get_recommendation_service(top_n=top_n)
        
        # Generate recommendations
        reranked_items, recall_count, ranking_count = recommendation_service.generate_recommendations(
            user_id=str(user_id),
            top_n=top_n
        )
        
        if not reranked_items:
            return RecommendResponse(
                user_id=user_id,
                recommendations=[],
                total=0,
                recall_count=0,
                ranking_count=0
            )
        
        # Lấy ASINs từ recommendations
        asins = [item.item_id for item in reranked_items]
        
        # Lấy item details từ database
        items = await ItemService.get_items_by_asins(db, asins)
        
        # Tạo mapping ASIN -> ItemResponse
        item_map = {item.asin: item for item in items}
        
        # Tạo RecommendedItemResponse list
        recommendations = []
        for reranked_item in reranked_items:
            item = item_map.get(reranked_item.item_id)
            
            if item:
                recommendations.append(RecommendedItemResponse(
                    asin=item.asin,
                    title=item.title,
                    main_category=item.main_category,
                    avg_rating=item.avg_rating,
                    rating_number=item.rating_number,
                    primary_image=item.primary_image,
                    score=reranked_item.adjusted_score,
                    rank=reranked_item.rank_position,
                    applied_rules=reranked_item.applied_rules
                ))
        
        return RecommendResponse(
            user_id=user_id,
            recommendations=recommendations,
            total=len(recommendations),
            recall_count=recall_count,
            ranking_count=ranking_count
        )
        
    except Exception as e:
        logger.error(f"Error generating recommendations for user {user_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate recommendations: {str(e)}"
        )



