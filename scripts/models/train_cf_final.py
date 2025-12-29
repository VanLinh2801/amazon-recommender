"""
Training Pipeline cho Collaborative Filtering - Final Model với Artifacts
=========================================================================

Pipeline huấn luyện mô hình Matrix Factorization trên toàn bộ dữ liệu 5-core
và xuất artifacts để sử dụng cho online serving.

Usage:
    python -m app.models.train_cf_final
"""

import sys
from pathlib import Path
import polars as pl
import numpy as np
import json
import io

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm root directory vào path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

# Import từ scripts/models thay vì app.models
from scripts.models.matrix_factorization import MatrixFactorization


def load_and_merge_data(train_path: str, test_path: str):
    """
    Đọc và gộp dữ liệu train + test thành một tập duy nhất.
    
    Args:
        train_path: Đường dẫn đến file train parquet
        test_path: Đường dẫn đến file test parquet
        
    Returns:
        Polars DataFrame với toàn bộ dữ liệu
    """
    print("\n" + "=" * 80)
    print("BƯỚC 1: ĐỌC VÀ GỘP DỮ LIỆU")
    print("=" * 80)
    
    # Kiểm tra files tồn tại
    train_file = Path(train_path)
    test_file = Path(test_path)
    
    if not train_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file train: {train_path}")
    if not test_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file test: {test_path}")
    
    # Đọc dữ liệu
    print(f"Đang đọc train data từ: {train_path}")
    train_df = pl.read_parquet(train_path)
    print(f"[OK] Train data: {len(train_df):,} samples")
    
    print(f"Đang đọc test data từ: {test_path}")
    test_df = pl.read_parquet(test_path)
    print(f"[OK] Test data: {len(test_df):,} samples")
    
    # Kiểm tra schema
    required_cols = ['user_id', 'item_id', 'rating']
    for col in required_cols:
        if col not in train_df.columns:
            raise ValueError(f"Thiếu cột '{col}' trong train data")
        if col not in test_df.columns:
            raise ValueError(f"Thiếu cột '{col}' trong test data")
    
    # Lấy các cột cần thiết
    train_df = train_df.select(['user_id', 'item_id', 'rating'])
    test_df = test_df.select(['user_id', 'item_id', 'rating'])
    
    # Gộp train + test
    print(f"\nĐang gộp train + test...")
    full_df = pl.concat([train_df, test_df])
    print(f"[OK] Tổng số samples sau khi gộp: {len(full_df):,}")
    
    # Thống kê
    ratings = full_df['rating'].to_numpy()
    print(f"\nThống kê dữ liệu đã gộp:")
    print(f"  - Rating min: {ratings.min():.2f}")
    print(f"  - Rating max: {ratings.max():.2f}")
    print(f"  - Rating mean: {ratings.mean():.2f}")
    print(f"  - Unique users: {full_df['user_id'].n_unique():,}")
    print(f"  - Unique items: {full_df['item_id'].n_unique():,}")
    
    return full_df


def prepare_data(df: pl.DataFrame):
    """
    Chuẩn bị dữ liệu từ Polars DataFrame sang numpy arrays.
    
    Args:
        df: Polars DataFrame với columns: user_id, item_id, rating
        
    Returns:
        Tuple (user_ids, item_ids, ratings) as numpy arrays
    """
    user_ids = df['user_id'].to_numpy()
    item_ids = df['item_id'].to_numpy()
    ratings = df['rating'].to_numpy().astype(np.float32)
    
    return user_ids, item_ids, ratings


def train_final_model(
    user_ids: np.ndarray,
    item_ids: np.ndarray,
    ratings: np.ndarray,
    n_factors: int = 15,
    learning_rate: float = 0.01,
    reg_user: float = 0.1,
    reg_item: float = 0.1,
    reg_bias: float = 0.01,
    n_epochs: int = 50,
    random_state: int = 42
):
    """
    Huấn luyện mô hình Matrix Factorization trên toàn bộ dữ liệu.
    
    Args:
        user_ids: Array user_id strings
        item_ids: Array item_id strings
        ratings: Array ratings
        n_factors: Số latent factors
        learning_rate: Learning rate
        reg_user: Regularization cho user
        reg_item: Regularization cho item
        reg_bias: Regularization cho bias
        n_epochs: Số epochs
        random_state: Random seed
        
    Returns:
        Trained MatrixFactorization model
    """
    print("\n" + "=" * 80)
    print("BƯỚC 2: HUẤN LUYỆN MÔ HÌNH TRÊN TOÀN BỘ DỮ LIỆU")
    print("=" * 80)
    
    # Khởi tạo mô hình với hyperparameters đã được kiểm chứng
    model = MatrixFactorization(
        n_factors=n_factors,
        learning_rate=learning_rate,
        reg_user=reg_user,
        reg_item=reg_item,
        reg_bias=reg_bias,
        n_epochs=n_epochs,
        random_state=random_state
    )
    
    # Huấn luyện
    model.fit(
        user_ids,
        item_ids,
        ratings,
        verbose=True
    )
    
    return model


def save_artifacts(
    model: MatrixFactorization,
    output_dir: Path
):
    """
    Lưu các artifacts của mô hình.
    
    Args:
        model: Trained MatrixFactorization model
        output_dir: Thư mục để lưu artifacts
    """
    print("\n" + "=" * 80)
    print("BƯỚC 3: XUẤT ARTIFACTS")
    print("=" * 80)
    
    # Tạo thư mục nếu chưa tồn tại
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Thư mục output: {output_dir}")
    
    # 1. Lưu user_factors.npy
    user_factors_path = output_dir / "user_factors.npy"
    print(f"\nĐang lưu user_factors.npy...")
    print(f"  Shape: {model.user_factors.shape}")
    print(f"  Dtype: {model.user_factors.dtype}")
    np.save(str(user_factors_path), model.user_factors)
    print(f"[OK] Đã lưu: {user_factors_path}")
    
    # 2. Lưu item_factors.npy
    item_factors_path = output_dir / "item_factors.npy"
    print(f"\nĐang lưu item_factors.npy...")
    print(f"  Shape: {model.item_factors.shape}")
    print(f"  Dtype: {model.item_factors.dtype}")
    np.save(str(item_factors_path), model.item_factors)
    print(f"[OK] Đã lưu: {item_factors_path}")
    
    # 3. Lưu user2idx.json
    # Đảm bảo mapping nhất quán: index i trong array tương ứng với user tại vị trí i
    user2idx_path = output_dir / "user2idx.json"
    print(f"\nĐang lưu user2idx.json...")
    print(f"  Số users: {len(model.user_to_idx)}")
    
    # Kiểm tra tính nhất quán của mapping
    # user_factors[i] phải tương ứng với idx_to_user[i]
    max_idx = max(model.idx_to_user.keys()) if model.idx_to_user else -1
    if max_idx >= model.n_users:
        raise ValueError(f"Mapping không nhất quán: max_idx={max_idx}, n_users={model.n_users}")
    
    # Đảm bảo tất cả indices từ 0 đến n_users-1 đều có trong idx_to_user
    for i in range(model.n_users):
        if i not in model.idx_to_user:
            raise ValueError(f"Thiếu user tại index {i} trong idx_to_user")
    
    # Lưu user2idx (mapping từ user_id string sang index int)
    with open(user2idx_path, 'w', encoding='utf-8') as f:
        json.dump(model.user_to_idx, f, indent=2, ensure_ascii=False)
    print(f"[OK] Đã lưu: {user2idx_path}")
    
    # 4. Lưu idx2item.json
    # Đảm bảo mapping nhất quán: index i trong array tương ứng với item tại vị trí i
    idx2item_path = output_dir / "idx2item.json"
    print(f"\nĐang lưu idx2item.json...")
    print(f"  Số items: {len(model.item_to_idx)}")
    
    # Kiểm tra tính nhất quán của mapping
    max_idx = max(model.idx_to_item.keys()) if model.idx_to_item else -1
    if max_idx >= model.n_items:
        raise ValueError(f"Mapping không nhất quán: max_idx={max_idx}, n_items={model.n_items}")
    
    # Đảm bảo tất cả indices từ 0 đến n_items-1 đều có trong idx_to_item
    for i in range(model.n_items):
        if i not in model.idx_to_item:
            raise ValueError(f"Thiếu item tại index {i} trong idx_to_item")
    
    # Lưu idx2item (mapping từ index int sang item_id string)
    # Chuyển đổi keys từ int sang string để JSON serialization
    idx2item_dict = {str(k): v for k, v in model.idx_to_item.items()}
    with open(idx2item_path, 'w', encoding='utf-8') as f:
        json.dump(idx2item_dict, f, indent=2, ensure_ascii=False)
    print(f"[OK] Đã lưu: {idx2item_path}")
    
    # Kiểm tra tính nhất quán cuối cùng
    print(f"\nKiểm tra tính nhất quán mapping:")
    print(f"  - user_factors.shape[0] = {model.user_factors.shape[0]}, n_users = {model.n_users}")
    print(f"  - item_factors.shape[0] = {model.item_factors.shape[0]}, n_items = {model.n_items}")
    print(f"  - len(user_to_idx) = {len(model.user_to_idx)}, len(idx_to_user) = {len(model.idx_to_user)}")
    print(f"  - len(item_to_idx) = {len(model.item_to_idx)}, len(idx_to_item) = {len(model.idx_to_item)}")
    
    if (model.user_factors.shape[0] == model.n_users == len(model.user_to_idx) == len(model.idx_to_user) and
        model.item_factors.shape[0] == model.n_items == len(model.item_to_idx) == len(model.idx_to_item)):
        print(f"[OK] Mapping nhất quán!")
    else:
        raise ValueError("Mapping không nhất quán giữa arrays và dictionaries!")
    
    print(f"\n" + "=" * 80)
    print("TÓM TẮT ARTIFACTS ĐÃ XUẤT")
    print("=" * 80)
    print(f"1. user_factors.npy: {model.user_factors.shape} - User latent factors")
    print(f"2. item_factors.npy: {model.item_factors.shape} - Item latent factors")
    print(f"3. user2idx.json: {len(model.user_to_idx)} users - Mapping user_id -> index")
    print(f"4. idx2item.json: {len(model.idx_to_item)} items - Mapping index -> item_id")
    print(f"\nLưu ý: Index i trong array tương ứng với user/item tại vị trí i trong mapping")


def main():
    """
    Hàm chính để chạy toàn bộ pipeline.
    """
    print("\n" + "=" * 80)
    print("PIPELINE HUẤN LUYỆN MÔ HÌNH CUỐI CÙNG - MATRIX FACTORIZATION")
    print("Xuất artifacts cho online serving")
    print("=" * 80)
    
    # Đường dẫn dữ liệu
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    train_path = BASE_DIR / "data" / "processed" / "interactions_5core_train.parquet"
    test_path = BASE_DIR / "data" / "processed" / "interactions_5core_test.parquet"
    
    # Thư mục output
    output_dir = BASE_DIR / "artifacts" / "mf"
    
    try:
        # Bước 1: Đọc và gộp dữ liệu
        full_df = load_and_merge_data(str(train_path), str(test_path))
        
        # Chuẩn bị dữ liệu
        user_ids, item_ids, ratings = prepare_data(full_df)
        
        # Bước 2: Huấn luyện mô hình trên toàn bộ dữ liệu
        # Sử dụng hyperparameters đã được kiểm chứng
        model = train_final_model(
            user_ids,
            item_ids,
            ratings,
            n_factors=15,          # Latent dimension nhỏ
            learning_rate=0.01,    # Learning rate vừa phải
            reg_user=0.1,          # Regularization mạnh
            reg_item=0.1,
            reg_bias=0.01,
            n_epochs=50,           # Đủ epochs để hội tụ
            random_state=42
        )
        
        # Bước 3: Xuất artifacts
        save_artifacts(model, output_dir)
        
        print("\n" + "=" * 80)
        print("[OK] PIPELINE HOÀN TẤT THÀNH CÔNG!")
        print("=" * 80)
        print(f"\nArtifacts đã được lưu tại: {output_dir}")
        print("\nCác artifacts này có thể được sử dụng cho:")
        print("  - MF recall trong Hybrid Recommendation System")
        print("  - Online serving với user_factors và item_factors")
        print("  - Mapping user_id/item_id <-> index với user2idx.json và idx2item.json")
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("[ERROR] PIPELINE THẤT BẠI")
        print("=" * 80)
        print(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

