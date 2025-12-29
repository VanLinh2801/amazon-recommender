"""
Analytics API routes
====================

API endpoints để cung cấp dữ liệu thống kê cho dashboard.
"""

import logging
import json
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

# Log paths for debugging
logger.info(f"PROJECT_ROOT: {PROJECT_ROOT}")
logger.info(f"DATA_DIR: {DATA_DIR} (exists: {DATA_DIR.exists()})")
logger.info(f"PROCESSED_DIR: {PROCESSED_DIR} (exists: {PROCESSED_DIR.exists()})")
logger.info(f"EMBEDDING_DIR: {EMBEDDING_DIR} (exists: {EMBEDDING_DIR.exists()})")


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
        if not PROCESSED_DIR.exists():
            return {
                "success": False,
                "message": "Data directory not found. Please run data preprocessing scripts first.",
                "data": None
            }
        
        interactions_path = PROCESSED_DIR / "interactions_5core.parquet"
        if not interactions_path.exists():
            return {
                "success": False,
                "message": f"Interactions file not found: {interactions_path}",
                "data": None
            }
        
        df = pl.read_parquet(str(interactions_path))
        distribution = get_rating_distribution(df)
        
        return {
            "success": True,
            "data": distribution,
            "total": sum(item["count"] for item in distribution)
        }
    except Exception as e:
        logger.error(f"Error getting rating distribution: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Error processing rating distribution: {str(e)}",
            "data": None
        }


@router.get("/model-metrics")
async def get_recommendation_metrics():
    """Lấy recommendation metrics (RMSE, MAE, Precision@K, Recall@K)."""
    try:
        # Tìm metrics file
        metrics_paths = [
            PROJECT_ROOT / "backend" / "artifacts" / "metrics" / "recommendation_metrics.json",
            PROJECT_ROOT / "artifacts" / "metrics" / "recommendation_metrics.json",
            PROJECT_ROOT / "backend" / "artifacts" / "mf" / "metrics.json",
            PROJECT_ROOT / "artifacts" / "mf" / "metrics.json",
        ]
        
        metrics_data = None
        for path in metrics_paths:
            if path.exists():
                with open(path, 'r', encoding='utf-8') as f:
                    metrics_data = json.load(f)
                logger.info(f"Loaded metrics from: {path}")
                break
        
        if not metrics_data:
            return {
                "success": False,
                "message": "Metrics file not found. Please run evaluation script first.",
                "data": None
            }
        
        return {
            "success": True,
            "data": metrics_data
        }
    except Exception as e:
        logger.error(f"Error getting recommendation metrics: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Error loading metrics: {str(e)}",
            "data": None
        }


@router.get("/category-distribution")
async def get_category_distribution_endpoint(top_n: int = 20):
    """Lấy phân bố category từ metadata."""
    try:
        if not PROCESSED_DIR.exists():
            return {
                "success": False,
                "message": "Data directory not found. Please run data preprocessing scripts first.",
                "data": None
            }
        
        metadata_path = PROCESSED_DIR / "metadata_clean.parquet"
        if not metadata_path.exists():
            return {
                "success": False,
                "message": f"Metadata file not found: {metadata_path}",
                "data": None
            }
        
        df = pl.read_parquet(str(metadata_path))
        distribution = get_category_distribution(df, top_n)
        
        return {
            "success": True,
            "data": distribution,
            "total_categories": len(distribution)
        }
    except Exception as e:
        logger.error(f"Error getting category distribution: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Error processing category distribution: {str(e)}",
            "data": None
        }


@router.get("/top-items")
async def get_top_items_endpoint(top_n: int = 20):
    """Lấy top items theo số lượng interactions."""
    try:
        if not PROCESSED_DIR.exists():
            return {
                "success": False,
                "message": "Data directory not found. Please run data preprocessing scripts first.",
                "data": None
            }
        
        interactions_path = PROCESSED_DIR / "interactions_5core.parquet"
        if not interactions_path.exists():
            return {
                "success": False,
                "message": f"Interactions file not found: {interactions_path}",
                "data": None
            }
        
        df = pl.read_parquet(str(interactions_path))
        top_items = get_top_items(df, top_n)
        
        return {
            "success": True,
            "data": top_items
        }
    except Exception as e:
        logger.error(f"Error getting top items: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Error processing top items: {str(e)}",
            "data": None
        }


@router.get("/interaction-stats")
async def get_interaction_stats():
    """Lấy thống kê tổng quan về interactions."""
    try:
        if not PROCESSED_DIR.exists():
            return {
                "success": False,
                "message": "Data directory not found. Please run data preprocessing scripts first.",
                "data": None
            }
        
        train_path = PROCESSED_DIR / "interactions_5core_train.parquet"
        test_path = PROCESSED_DIR / "interactions_5core_test.parquet"
        all_path = PROCESSED_DIR / "interactions_5core.parquet"
        
        if not all_path.exists():
            return {
                "success": False,
                "message": f"Interactions file not found: {all_path}",
                "data": None
            }
        
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
        logger.error(f"Error getting interaction stats: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Error processing interaction stats: {str(e)}",
            "data": None
        }


@router.get("/embedding-stats")
async def get_embedding_stats():
    """Lấy thống kê về embedding data."""
    try:
        # Kiểm tra thư mục data có tồn tại không
        if not DATA_DIR.exists():
            logger.warning(f"DATA_DIR does not exist: {DATA_DIR}")
            return {
                "success": False,
                "message": "Data directory not found. Please ensure data preprocessing has been completed.",
                "data": None
            }
        
        # Thử tìm semantic_attributes.parquet trong embedding directory
        semantic_path = EMBEDDING_DIR / "semantic_attributes.parquet"
        
        # Nếu không có, thử dùng items_for_rs.parquet hoặc metadata_clean.parquet
        if not semantic_path.exists():
            logger.info(f"semantic_attributes.parquet not found at {semantic_path}, trying fallback files...")
            
            # Fallback 1: items_for_rs.parquet
            fallback_path = PROCESSED_DIR / "items_for_rs.parquet"
            if fallback_path.exists():
                logger.info(f"Using fallback: {fallback_path}")
                df = pl.read_parquet(str(fallback_path))
            else:
                # Fallback 2: metadata_clean.parquet
                fallback_path = PROCESSED_DIR / "metadata_clean.parquet"
                if fallback_path.exists():
                    logger.info(f"Using fallback: {fallback_path}")
                    df = pl.read_parquet(str(fallback_path))
                else:
                    logger.error(f"No embedding data files found. Checked: {semantic_path}, {PROCESSED_DIR / 'items_for_rs.parquet'}, {fallback_path}")
                    return {
                        "success": False,
                        "message": "Embedding data files not found. Please run data preprocessing scripts first.",
                        "data": None
                    }
        else:
            df = pl.read_parquet(str(semantic_path))
        
        stats = {
            "total_items": len(df),
            "columns": df.columns,
            "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
        }
        
        # Thống kê các cột nếu có
        if "main_category" in df.columns:
            stats["unique_categories"] = df["main_category"].n_unique()
        elif "category" in df.columns:
            stats["unique_categories"] = df["category"].n_unique()
        
        # Thống kê item_id nếu có
        if "item_id" in df.columns:
            stats["unique_items"] = df["item_id"].n_unique()
        elif "parent_asin" in df.columns:
            stats["unique_items"] = df["parent_asin"].n_unique()
        
        return {
            "success": True,
            "data": stats
        }
    except Exception as e:
        logger.error(f"Error getting embedding stats: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Error processing embedding stats: {str(e)}",
            "data": None
        }


@router.get("/user-activity")
async def get_user_activity():
    """Lấy thống kê về hoạt động của users."""
    try:
        if not PROCESSED_DIR.exists():
            return {
                "success": False,
                "message": "Data directory not found. Please run data preprocessing scripts first.",
                "data": None
            }
        
        interactions_path = PROCESSED_DIR / "interactions_5core.parquet"
        if not interactions_path.exists():
            return {
                "success": False,
                "message": f"Interactions file not found: {interactions_path}",
                "data": None
            }
        
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
        logger.error(f"Error getting user activity: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Error processing user activity: {str(e)}",
            "data": None
        }


@router.get("/item-popularity")
async def get_item_popularity(top_n: int = 20):
    """Lấy thống kê về popularity của items."""
    try:
        if not PROCESSED_DIR.exists():
            return {
                "success": False,
                "message": "Data directory not found. Please run data preprocessing scripts first.",
                "data": None
            }
        
        popularity_path = PROCESSED_DIR / "item_popularity.parquet"
        if not popularity_path.exists():
            return {
                "success": False,
                "message": f"Item popularity file not found: {popularity_path}",
                "data": None
            }
        
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
        logger.error(f"Error getting item popularity: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Error processing item popularity: {str(e)}",
            "data": None
        }


@router.get("/cleaning-stats")
async def get_cleaning_stats():
    """Lấy thống kê chi tiết về quá trình cleaning, 5core interactions và embedding tokens."""
    try:
        # Đọc raw và clean data để so sánh
        reviews_normalized_path = PROCESSED_DIR / "reviews_normalized.parquet"
        reviews_clean_path = PROCESSED_DIR / "reviews_clean.parquet"
        
        # 5core interactions files
        interactions_all_path = PROCESSED_DIR / "interactions_all.parquet"
        interactions_5core_path = PROCESSED_DIR / "interactions_5core.parquet"
        interactions_5core_train_path = PROCESSED_DIR / "interactions_5core_train.parquet"
        interactions_5core_test_path = PROCESSED_DIR / "interactions_5core_test.parquet"
        
        # Embedding data
        embedding_text_path = EMBEDDING_DIR / "embedding_text.parquet"
        
        stats = {}
        
        if reviews_normalized_path.exists() and reviews_clean_path.exists():
            reviews_normalized = pl.read_parquet(str(reviews_normalized_path))
            reviews_clean = pl.read_parquet(str(reviews_clean_path))
            
            initial_count = len(reviews_normalized)
            final_count = len(reviews_clean)
            
            # Tính toán chi tiết từng task
            # Task 1: Missing values - drop records thiếu amazon_user_id, asin, rating
            task1_dropped = initial_count - len(
                reviews_normalized.filter(
                    pl.col("amazon_user_id").is_not_null() &
                    pl.col("asin").is_not_null() &
                    pl.col("rating").is_not_null()
                )
            )
            
            # Task 2: Sanity check - rating ngoài [1, 5]
            after_task1 = reviews_normalized.filter(
                pl.col("amazon_user_id").is_not_null() &
                pl.col("asin").is_not_null() &
                pl.col("rating").is_not_null()
            )
            task2_dropped = len(after_task1) - len(
                after_task1.filter(
                    (pl.col("rating") >= 1) & (pl.col("rating") <= 5)
                )
            )
            
            # Task 3: Deduplication (ước tính)
            after_task2 = after_task1.filter(
                (pl.col("rating") >= 1) & (pl.col("rating") <= 5)
            )
            # Đếm duplicates theo (amazon_user_id, asin)
            duplicates = after_task2.group_by(["amazon_user_id", "asin"]).agg(pl.count().alias("count"))
            duplicates_count = duplicates.filter(pl.col("count") > 1)
            task3_dropped = duplicates_count["count"].sum() - len(duplicates_count) if len(duplicates_count) > 0 else 0
            
            # Rating distribution trước và sau
            rating_dist_before = reviews_normalized.group_by("rating").agg(pl.count().alias("count")).sort("rating")
            rating_dist_after = reviews_clean.group_by("rating").agg(pl.count().alias("count")).sort("rating")
            
            # Missing values breakdown
            missing_breakdown = {
                "amazon_user_id": reviews_normalized.filter(pl.col("amazon_user_id").is_null()).height,
                "asin": reviews_normalized.filter(pl.col("asin").is_null()).height,
                "rating": reviews_normalized.filter(pl.col("rating").is_null()).height,
                "review_title": reviews_normalized.filter(pl.col("review_title").is_null()).height,
                "review_text": reviews_normalized.filter(pl.col("review_text").is_null()).height,
            }
            
            stats["reviews"] = {
                "before": initial_count,
                "after": final_count,
                "dropped": initial_count - final_count,
                "retention_rate": (final_count / initial_count * 100) if initial_count > 0 else 0,
                "tasks": {
                    "task1_missing_values": {
                        "dropped": task1_dropped,
                        "percentage": (task1_dropped / initial_count * 100) if initial_count > 0 else 0
                    },
                    "task2_sanity_check": {
                        "dropped": task2_dropped,
                        "percentage": (task2_dropped / initial_count * 100) if initial_count > 0 else 0
                    },
                    "task3_deduplication": {
                        "dropped": task3_dropped,
                        "percentage": (task3_dropped / initial_count * 100) if initial_count > 0 else 0
                    }
                },
                "rating_distribution_before": [
                    {"rating": float(row["rating"]), "count": int(row["count"])}
                    for row in rating_dist_before.to_dicts()
                ],
                "rating_distribution_after": [
                    {"rating": float(row["rating"]), "count": int(row["count"])}
                    for row in rating_dist_after.to_dicts()
                ],
                "missing_values_breakdown": missing_breakdown,
                "retention_by_task": [
                    {"task": "Initial", "count": initial_count, "percentage": 100.0},
                    {"task": "After Task 1", "count": initial_count - task1_dropped, "percentage": ((initial_count - task1_dropped) / initial_count * 100) if initial_count > 0 else 0},
                    {"task": "After Task 2", "count": initial_count - task1_dropped - task2_dropped, "percentage": ((initial_count - task1_dropped - task2_dropped) / initial_count * 100) if initial_count > 0 else 0},
                    {"task": "After Task 3", "count": initial_count - task1_dropped - task2_dropped - task3_dropped, "percentage": ((initial_count - task1_dropped - task2_dropped - task3_dropped) / initial_count * 100) if initial_count > 0 else 0},
                    {"task": "Final", "count": final_count, "percentage": (final_count / initial_count * 100) if initial_count > 0 else 0}
                ]
            }
        
        # 5-Core Interactions Statistics
        if interactions_all_path.exists() and interactions_5core_path.exists():
            interactions_all = pl.read_parquet(str(interactions_all_path))
            interactions_5core = pl.read_parquet(str(interactions_5core_path))
            
            all_count = len(interactions_all)
            core_count = len(interactions_5core)
            
            # Train/Test split stats
            train_count = 0
            test_count = 0
            if interactions_5core_train_path.exists() and interactions_5core_test_path.exists():
                train_df = pl.read_parquet(str(interactions_5core_train_path))
                test_df = pl.read_parquet(str(interactions_5core_test_path))
                train_count = len(train_df)
                test_count = len(test_df)
            
            # User and item statistics
            all_users = interactions_all["user_id"].n_unique()
            all_items = interactions_all["item_id"].n_unique()
            core_users = interactions_5core["user_id"].n_unique()
            core_items = interactions_5core["item_id"].n_unique()
            
            # Rating distribution in 5core
            rating_dist_5core = interactions_5core.group_by("rating").agg(pl.count().alias("count")).sort("rating")
            
            stats["interactions_5core"] = {
                "all_interactions": {
                    "count": all_count,
                    "unique_users": all_users,
                    "unique_items": all_items
                },
                "core_interactions": {
                    "count": core_count,
                    "unique_users": core_users,
                    "unique_items": core_items,
                    "retention_rate": (core_count / all_count * 100) if all_count > 0 else 0,
                    "users_retention_rate": (core_users / all_users * 100) if all_users > 0 else 0,
                    "items_retention_rate": (core_items / all_items * 100) if all_items > 0 else 0
                },
                "train_test_split": {
                    "train_count": train_count,
                    "test_count": test_count,
                    "train_ratio": (train_count / (train_count + test_count) * 100) if (train_count + test_count) > 0 else 0,
                    "test_ratio": (test_count / (train_count + test_count) * 100) if (train_count + test_count) > 0 else 0
                },
                "rating_distribution": [
                    {"rating": float(row["rating"]), "count": int(row["count"])}
                    for row in rating_dist_5core.to_dicts()
                ],
                "filtering_steps": [
                    {"step": "All Interactions", "count": all_count, "users": all_users, "items": all_items},
                    {"step": "5-Core Filtered", "count": core_count, "users": core_users, "items": core_items}
                ]
            }
        
        # Embedding Token Statistics
        if embedding_text_path.exists():
            embedding_df = pl.read_parquet(str(embedding_text_path))
            
            # Count tokens in embedding_text (simple word count)
            if "embedding_text" in embedding_df.columns:
                # Calculate approximate token counts (split by spaces)
                token_counts = embedding_df.select([
                    pl.col("embedding_text").str.split(" ").list.len().alias("token_count")
                ])
                
                token_stats = token_counts.select([
                    pl.col("token_count").mean().alias("avg_tokens"),
                    pl.col("token_count").median().alias("median_tokens"),
                    pl.col("token_count").min().alias("min_tokens"),
                    pl.col("token_count").max().alias("max_tokens"),
                    pl.col("token_count").std().alias("std_tokens")
                ]).to_dicts()[0]
                
                # Token distribution (bins)
                token_counts_list = token_counts["token_count"].to_list()
                bins = [0, 50, 100, 200, 300, 500, 1000]
                bin_labels = ["0-50", "51-100", "101-200", "201-300", "301-500", "501-1000", "1000+"]
                token_distribution = []
                
                for i in range(len(bins)):
                    if i < len(bins) - 1:
                        bin_min, bin_max = bins[i], bins[i + 1]
                        count = sum(1 for tc in token_counts_list if bin_min <= tc < bin_max)
                    else:
                        # Last bin: >= 1000
                        count = sum(1 for tc in token_counts_list if tc >= bins[-1])
                    
                    if count > 0:
                        token_distribution.append({
                            "range": bin_labels[i],
                            "count": count,
                            "percentage": (count / len(token_counts_list) * 100) if token_counts_list else 0
                        })
                
                stats["embedding_tokens"] = {
                    "total_items": len(embedding_df),
                    "statistics": {
                        "avg_tokens": float(token_stats["avg_tokens"]) if token_stats["avg_tokens"] else 0,
                        "median_tokens": float(token_stats["median_tokens"]) if token_stats["median_tokens"] else 0,
                        "min_tokens": int(token_stats["min_tokens"]) if token_stats["min_tokens"] else 0,
                        "max_tokens": int(token_stats["max_tokens"]) if token_stats["max_tokens"] else 0,
                        "std_tokens": float(token_stats["std_tokens"]) if token_stats["std_tokens"] else 0
                    },
                    "token_distribution": token_distribution
            }
        
        return {
            "success": True,
            "data": stats
        }
    except Exception as e:
        logger.error(f"Error getting cleaning stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data-quality")
async def get_data_quality():
    """Lấy thống kê về data quality: null values, missing data, duplicates."""
    try:
        # Đọc raw và normalized data để so sánh
        reviews_raw_path = PROCESSED_DIR / "reviews_raw.parquet"
        reviews_normalized_path = PROCESSED_DIR / "reviews_normalized.parquet"
        metadata_raw_path = PROCESSED_DIR / "metadata_raw.parquet"
        metadata_normalized_path = PROCESSED_DIR / "metadata_normalized.parquet"
        
        stats = {}
        
        # Reviews Data Quality
        if reviews_raw_path.exists() and reviews_normalized_path.exists():
            reviews_raw = pl.read_parquet(str(reviews_raw_path))
            reviews_normalized = pl.read_parquet(str(reviews_normalized_path))
            
            total_rows = len(reviews_raw)
            
            # Null values per column
            null_counts = {}
            for col in reviews_raw.columns:
                null_count = reviews_raw.filter(pl.col(col).is_null()).height
                null_counts[col] = {
                    "count": null_count,
                    "percentage": (null_count / total_rows * 100) if total_rows > 0 else 0
                }
            
            # Duplicate records (theo amazon_user_id + asin)
            duplicates = reviews_raw.group_by(["amazon_user_id", "asin"]).agg(pl.count().alias("count"))
            duplicate_records = duplicates.filter(pl.col("count") > 1)
            duplicate_count = duplicate_records["count"].sum() - len(duplicate_records) if len(duplicate_records) > 0 else 0
            
            # Invalid ratings (ngoài [1, 5])
            invalid_ratings = reviews_raw.filter(
                (pl.col("rating") < 1) | (pl.col("rating") > 5) | pl.col("rating").is_null()
            ).height
            
            # Empty text fields
            empty_review_text = reviews_raw.filter(
                (pl.col("review_text").is_null()) | (pl.col("review_text").str.strip_chars() == "")
            ).height
            
            stats["reviews"] = {
                "total_rows": total_rows,
                "null_values_by_column": null_counts,
                "duplicate_records": {
                    "count": duplicate_count,
                    "percentage": (duplicate_count / total_rows * 100) if total_rows > 0 else 0
                },
                "invalid_ratings": {
                    "count": invalid_ratings,
                    "percentage": (invalid_ratings / total_rows * 100) if total_rows > 0 else 0
                },
                "empty_review_text": {
                    "count": empty_review_text,
                    "percentage": (empty_review_text / total_rows * 100) if total_rows > 0 else 0
                },
                "data_quality_score": max(0, 100 - (
                    (duplicate_count / total_rows * 100) if total_rows > 0 else 0 +
                    (invalid_ratings / total_rows * 100) if total_rows > 0 else 0 +
                    sum(null_counts.get(col, {}).get("percentage", 0) for col in ["amazon_user_id", "asin", "rating"]) / 3
                ))
            }
        
        # Metadata Data Quality
        if metadata_raw_path.exists() and metadata_normalized_path.exists():
            metadata_raw = pl.read_parquet(str(metadata_raw_path))
            metadata_normalized = pl.read_parquet(str(metadata_normalized_path))
            
            total_rows = len(metadata_raw)
            
            # Null values per column
            null_counts = {}
            for col in metadata_raw.columns:
                null_count = metadata_raw.filter(pl.col(col).is_null()).height
                null_counts[col] = {
                    "count": null_count,
                    "percentage": (null_count / total_rows * 100) if total_rows > 0 else 0
                }
            
            # Duplicate records (theo parent_asin)
            duplicates = metadata_raw.group_by("parent_asin").agg(pl.count().alias("count"))
            duplicate_records = duplicates.filter(pl.col("count") > 1)
            duplicate_count = duplicate_records["count"].sum() - len(duplicate_records) if len(duplicate_records) > 0 else 0
            
            # Missing title
            missing_title = metadata_raw.filter(
                (pl.col("title").is_null()) | (pl.col("title").str.strip_chars() == "")
            ).height
            
            # Missing category (All Beauty hoặc null)
            missing_category = metadata_raw.filter(
                (pl.col("main_category").is_null()) | 
                (pl.col("main_category") == "All Beauty") |
                (pl.col("main_category").str.strip_chars() == "")
            ).height
            
            stats["metadata"] = {
                "total_rows": total_rows,
                "null_values_by_column": null_counts,
                "duplicate_records": {
                    "count": duplicate_count,
                    "percentage": (duplicate_count / total_rows * 100) if total_rows > 0 else 0
                },
                "missing_title": {
                    "count": missing_title,
                    "percentage": (missing_title / total_rows * 100) if total_rows > 0 else 0
                },
                "missing_category": {
                    "count": missing_category,
                    "percentage": (missing_category / total_rows * 100) if total_rows > 0 else 0
                },
                "data_quality_score": max(0, 100 - (
                    (duplicate_count / total_rows * 100) if total_rows > 0 else 0 +
                    (missing_title / total_rows * 100) if total_rows > 0 else 0 +
                    (missing_category / total_rows * 100) if total_rows > 0 else 0
                ))
            }
        
        return {
            "success": True,
            "data": stats
        }
    except Exception as e:
        logger.error(f"Error getting data quality stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/model-metrics")
async def get_model_metrics():
    """Lấy metrics của model MF từ file JSON."""
    try:
        metrics_path = PROJECT_ROOT / "artifacts" / "mf" / "metrics.json"
        
        if not metrics_path.exists():
            # Nếu chưa có metrics, trả về None
            return {
                "success": False,
                "message": "Metrics file not found. Please run test_mf_metrics.py first.",
                "data": None
            }
        
        with open(metrics_path, 'r', encoding='utf-8') as f:
            metrics = json.load(f)
        
        return {
            "success": True,
            "data": metrics
        }
    except Exception as e:
        logger.error(f"Error getting model metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

