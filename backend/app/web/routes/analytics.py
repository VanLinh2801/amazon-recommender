"""
Analytics API routes
====================

API endpoints để cung cấp dữ liệu thống kê cho dashboard.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException
import polars as pl
import numpy as np

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/analytics", tags=["analytics"])

# Đường dẫn đến data directory
def get_project_root() -> Path:
    """Tìm project root directory."""
    script_path = Path(__file__).resolve()
    current = script_path.parent
    # Đi lên từ backend/app/web/routes đến project root
    for _ in range(5):
        if (current / "data").exists() and (current / "backend").exists():
            return current
        current = current.parent
    # Fallback: đi lên 5 levels từ backend/app/web/routes
    return script_path.parent.parent.parent.parent.parent

PROJECT_ROOT = get_project_root()
DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
EMBEDDING_DIR = DATA_DIR / "embedding"


def get_rating_distribution(df: pl.DataFrame) -> List[Dict[str, Any]]:
    """Tính phân bố rating."""
    rating_counts = df.group_by("rating").agg(pl.count().alias("count")).sort("rating")
    return [
        {"rating": int(row["rating"]), "count": int(row["count"])}
        for row in rating_counts.iter_rows(named=True)
    ]


def get_category_distribution(df: pl.DataFrame, top_n: int = 20) -> List[Dict[str, Any]]:
    """Tính phân bố category."""
    if "main_category" not in df.columns:
        return []
    
    category_counts = (
        df.filter(pl.col("main_category").is_not_null())
        .group_by("main_category")
        .agg(pl.count().alias("count"))
        .sort("count", descending=True)
        .head(top_n)
    )
    
    return [
        {"category": row["main_category"], "count": int(row["count"])}
        for row in category_counts.iter_rows(named=True)
    ]


def get_top_items(df: pl.DataFrame, top_n: int = 20) -> List[Dict[str, Any]]:
    """Lấy top items theo số lượng interactions."""
    if "item_id" not in df.columns:
        return []
    
    item_counts = (
        df.group_by("item_id")
        .agg(pl.count().alias("count"))
        .sort("count", descending=True)
        .head(top_n)
    )
    
    return [
        {"item_id": row["item_id"], "count": int(row["count"])}
        for row in item_counts.iter_rows(named=True)
    ]


@router.get("/rating-distribution")
async def get_rating_distribution_endpoint():
    """Lấy phân bố rating từ interactions."""
    try:
        interactions_path = PROCESSED_DIR / "interactions_5core.parquet"
        if not interactions_path.exists():
            raise HTTPException(status_code=404, detail="Interactions file not found")
        
        df = pl.read_parquet(str(interactions_path))
        distribution = get_rating_distribution(df)
        
        return {
            "success": True,
            "data": distribution,
            "total": sum(item["count"] for item in distribution)
        }
    except Exception as e:
        logger.error(f"Error getting rating distribution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/category-distribution")
async def get_category_distribution_endpoint(top_n: int = 20):
    """Lấy phân bố category từ metadata."""
    try:
        metadata_path = PROCESSED_DIR / "metadata_clean.parquet"
        if not metadata_path.exists():
            raise HTTPException(status_code=404, detail="Metadata file not found")
        
        df = pl.read_parquet(str(metadata_path))
        distribution = get_category_distribution(df, top_n)
        
        return {
            "success": True,
            "data": distribution,
            "total_categories": len(distribution)
        }
    except Exception as e:
        logger.error(f"Error getting category distribution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-items")
async def get_top_items_endpoint(top_n: int = 20):
    """Lấy top items theo số lượng interactions."""
    try:
        interactions_path = PROCESSED_DIR / "interactions_5core.parquet"
        if not interactions_path.exists():
            raise HTTPException(status_code=404, detail="Interactions file not found")
        
        df = pl.read_parquet(str(interactions_path))
        top_items = get_top_items(df, top_n)
        
        return {
            "success": True,
            "data": top_items
        }
    except Exception as e:
        logger.error(f"Error getting top items: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/interaction-stats")
async def get_interaction_stats():
    """Lấy thống kê tổng quan về interactions."""
    try:
        train_path = PROCESSED_DIR / "interactions_5core_train.parquet"
        test_path = PROCESSED_DIR / "interactions_5core_test.parquet"
        all_path = PROCESSED_DIR / "interactions_5core.parquet"
        
        if not all_path.exists():
            raise HTTPException(status_code=404, detail="Interactions file not found")
        
        df_all = pl.read_parquet(str(all_path))
        
        stats = {
            "total_interactions": len(df_all),
            "unique_users": df_all["user_id"].n_unique(),
            "unique_items": df_all["item_id"].n_unique(),
            "avg_rating": float(df_all["rating"].mean()),
            "min_rating": float(df_all["rating"].min()),
            "max_rating": float(df_all["rating"].max()),
        }
        
        # Thống kê train/test nếu có
        if train_path.exists() and test_path.exists():
            df_train = pl.read_parquet(str(train_path))
            df_test = pl.read_parquet(str(test_path))
            stats["train_count"] = len(df_train)
            stats["test_count"] = len(df_test)
            stats["train_ratio"] = len(df_train) / (len(df_train) + len(df_test))
            stats["test_ratio"] = len(df_test) / (len(df_train) + len(df_test))
        
        return {
            "success": True,
            "data": stats
        }
    except Exception as e:
        logger.error(f"Error getting interaction stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/embedding-stats")
async def get_embedding_stats():
    """Lấy thống kê về embedding data."""
    try:
        semantic_path = EMBEDDING_DIR / "semantic_attributes.parquet"
        if not semantic_path.exists():
            raise HTTPException(status_code=404, detail="Semantic attributes file not found")
        
        df = pl.read_parquet(str(semantic_path))
        
        stats = {
            "total_items": len(df),
            "columns": df.columns,
            "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
        }
        
        # Thống kê các cột nếu có
        if "main_category" in df.columns:
            stats["unique_categories"] = df["main_category"].n_unique()
        
        return {
            "success": True,
            "data": stats
        }
    except Exception as e:
        logger.error(f"Error getting embedding stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/user-activity")
async def get_user_activity():
    """Lấy thống kê về hoạt động của users."""
    try:
        interactions_path = PROCESSED_DIR / "interactions_5core.parquet"
        if not interactions_path.exists():
            raise HTTPException(status_code=404, detail="Interactions file not found")
        
        df = pl.read_parquet(str(interactions_path))
        
        # Tính số interactions per user
        user_activity = (
            df.group_by("user_id")
            .agg(pl.count().alias("interaction_count"))
            .select("interaction_count")
        )
        
        activity_counts = user_activity["interaction_count"].to_numpy()
        
        # Tạo histogram
        bins = [0, 5, 10, 20, 50, 100, 200, 500, float('inf')]
        labels = ["1-5", "6-10", "11-20", "21-50", "51-100", "101-200", "201-500", "500+"]
        hist, _ = np.histogram(activity_counts, bins=bins)
        
        histogram_data = [
            {"range": label, "count": int(count)}
            for label, count in zip(labels, hist)
        ]
        
        return {
            "success": True,
            "data": {
                "histogram": histogram_data,
                "avg_interactions_per_user": float(np.mean(activity_counts)),
                "median_interactions_per_user": float(np.median(activity_counts)),
                "max_interactions_per_user": int(np.max(activity_counts)),
                "min_interactions_per_user": int(np.min(activity_counts)),
            }
        }
    except Exception as e:
        logger.error(f"Error getting user activity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/item-popularity")
async def get_item_popularity(top_n: int = 20):
    """Lấy thống kê về popularity của items."""
    try:
        popularity_path = PROCESSED_DIR / "item_popularity.parquet"
        if not popularity_path.exists():
            raise HTTPException(status_code=404, detail="Item popularity file not found")
        
        df = pl.read_parquet(str(popularity_path))
        
        # Lấy top items
        top_items = (
            df.sort("interaction_count", descending=True)
            .head(top_n)
            .select(["item_id", "interaction_count", "mean_rating"])
        )
        
        return {
            "success": True,
            "data": [
                {
                    "item_id": row["item_id"],
                    "interaction_count": int(row["interaction_count"]),
                    "mean_rating": float(row["mean_rating"]) if row["mean_rating"] is not None else None
                }
                for row in top_items.iter_rows(named=True)
            ]
        }
    except Exception as e:
        logger.error(f"Error getting item popularity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cleaning-stats")
async def get_cleaning_stats():
    """Lấy thống kê về quá trình cleaning."""
    try:
        # Đọc raw và clean data để so sánh
        reviews_normalized_path = PROCESSED_DIR / "reviews_normalized.parquet"
        reviews_clean_path = PROCESSED_DIR / "reviews_clean.parquet"
        metadata_normalized_path = PROCESSED_DIR / "metadata_normalized.parquet"
        metadata_clean_path = PROCESSED_DIR / "metadata_clean.parquet"
        
        stats = {}
        
        if reviews_normalized_path.exists() and reviews_clean_path.exists():
            reviews_normalized = pl.read_parquet(str(reviews_normalized_path))
            reviews_clean = pl.read_parquet(str(reviews_clean_path))
            
            stats["reviews"] = {
                "before": len(reviews_normalized),
                "after": len(reviews_clean),
                "dropped": len(reviews_normalized) - len(reviews_clean),
                "retention_rate": len(reviews_clean) / len(reviews_normalized) * 100
            }
        
        if metadata_normalized_path.exists() and metadata_clean_path.exists():
            metadata_normalized = pl.read_parquet(str(metadata_normalized_path))
            metadata_clean = pl.read_parquet(str(metadata_clean_path))
            
            stats["metadata"] = {
                "before": len(metadata_normalized),
                "after": len(metadata_clean),
                "dropped": len(metadata_normalized) - len(metadata_clean),
                "retention_rate": len(metadata_clean) / len(metadata_normalized) * 100
            }
        
        return {
            "success": True,
            "data": stats
        }
    except Exception as e:
        logger.error(f"Error getting cleaning stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

