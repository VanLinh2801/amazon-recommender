"""
Item schemas cho Item API.
"""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class ItemResponse(BaseModel):
    """
    Response schema cho item detail.
    """
    asin: str = Field(..., description="Item ASIN")
    parent_asin: str = Field(..., description="Parent ASIN (product)")
    title: str = Field(..., description="Product title")
    store: Optional[str] = Field(None, description="Store name")
    main_category: Optional[str] = Field(None, description="Main category")
    category: Optional[str] = Field(None, description="Category from semantic attributes")
    avg_rating: Optional[float] = Field(None, description="Average rating")
    rating_number: Optional[int] = Field(None, description="Number of ratings")
    primary_image: Optional[str] = Field(None, description="Primary image URL")
    variant: Optional[str] = Field(None, description="Item variant")
    raw_metadata: Optional[Dict[str, Any]] = Field(None, description="Raw metadata (JSONB)")
    
    class Config:
        from_attributes = True


class ItemListResponse(BaseModel):
    """
    Response schema cho danh sách items.
    """
    items: List[ItemResponse] = Field(..., description="List of items")
    total: int = Field(..., description="Total number of items")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Page size")
    has_more: bool = Field(..., description="Whether there are more items")


class RecommendedItemResponse(BaseModel):
    """
    Response schema cho recommended item.
    """
    asin: str = Field(..., description="Item ASIN")
    title: str = Field(..., description="Product title")
    main_category: Optional[str] = Field(None, description="Main category")
    avg_rating: Optional[float] = Field(None, description="Average rating")
    rating_number: Optional[int] = Field(None, description="Number of ratings")
    primary_image: Optional[str] = Field(None, description="Primary image URL")
    score: float = Field(..., description="Recommendation score")
    rank: int = Field(..., description="Rank position (1-based)")
    applied_rules: List[str] = Field(default_factory=list, description="Re-ranking rules applied")


class RecommendResponse(BaseModel):
    """
    Response schema cho recommendations.
    """
    user_id: int = Field(..., description="User ID")
    recommendations: List[RecommendedItemResponse] = Field(..., description="List of recommended items")
    total: int = Field(..., description="Total number of recommendations")
    recall_count: Optional[int] = Field(None, description="Number of candidates from recall")
    ranking_count: Optional[int] = Field(None, description="Number of items after ranking")



