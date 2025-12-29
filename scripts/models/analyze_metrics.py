"""Phân tích metrics và đề xuất cải thiện"""
import polars as pl
import numpy as np
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

# Load test data
test_path = BASE_DIR / "data" / "processed" / "interactions_5core_test.parquet"
test_df = pl.read_parquet(str(test_path))

print("=" * 80)
print("PHAN TICH METRICS")
print("=" * 80)

print(f"\n[1] Test Data Statistics:")
print(f"  Total interactions: {len(test_df):,}")
print(f"  Unique users: {test_df['user_id'].n_unique():,}")
print(f"  Unique items: {test_df['item_id'].n_unique():,}")
print(f"  Avg interactions per user: {len(test_df) / test_df['user_id'].n_unique():.2f}")

print(f"\n[2] Rating Distribution:")
rating_dist = test_df.group_by('rating').agg(pl.count().alias('count')).sort('rating')
for row in rating_dist.iter_rows(named=True):
    pct = row['count'] / len(test_df) * 100
    print(f"  Rating {row['rating']}: {row['count']:,} ({pct:.1f}%)")

# Phân tích positive items (rating >= 4.0)
positive_threshold = 4.0
positive_df = test_df.filter(pl.col('rating') >= positive_threshold)
print(f"\n[3] Positive Items (rating >= {positive_threshold}):")
print(f"  Total positive interactions: {len(positive_df):,} ({len(positive_df)/len(test_df)*100:.1f}%)")
print(f"  Users with positive items: {positive_df['user_id'].n_unique():,}")

# Phân tích interactions per user
user_interactions = test_df.group_by('user_id').agg(pl.count().alias('count'))
print(f"\n[4] Interactions per User:")
print(f"  Min: {user_interactions['count'].min()}")
print(f"  Max: {user_interactions['count'].max()}")
print(f"  Mean: {user_interactions['count'].mean():.2f}")
print(f"  Median: {user_interactions['count'].median():.2f}")

# Phân tích items per user
user_items = test_df.group_by('user_id').agg(pl.col('item_id').n_unique().alias('unique_items'))
print(f"\n[5] Unique Items per User:")
print(f"  Min: {user_items['unique_items'].min()}")
print(f"  Max: {user_items['unique_items'].max()}")
print(f"  Mean: {user_items['unique_items'].mean():.2f}")
print(f"  Median: {user_items['unique_items'].median():.2f}")

# Phân tích tại sao Precision/Recall thấp
print(f"\n[6] Phan tich Precision/Recall thap:")
print(f"  - Test set chi co {len(test_df):,} interactions")
print(f"  - Trung binh moi user chi co {user_items['unique_items'].mean():.2f} items")
print(f"  - Voi Precision@10, can 10 items trong top recommendations")
print(f"  - Neu user chi co 1-2 positive items trong test set, Precision se rat thap")

# Đề xuất cải thiện
print(f"\n[7] De xuat cai thien:")
print(f"  1. Tang kich thuoc dataset (nhieu interactions hon)")
print(f"  2. Giam threshold rating (tu 4.0 xuong 3.5 hoac 3.0)")
print(f"  3. Su dung full pipeline (Recall + Ranking + Re-ranking) thay vi chi MF")
print(f"  4. Tinh metrics tren training set de so sanh")
print(f"  5. Su dung leave-one-out evaluation thay vi train/test split")


