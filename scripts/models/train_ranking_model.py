"""
Train Ranking Model (Logistic Regression Baseline)
==================================================
Huấn luyện ranking model baseline sử dụng Logistic Regression
cho Hybrid Recommendation System.

Input:
- artifacts/ranking/ranking_dataset.parquet

Output:
- In ra kết quả đánh giá và feature importance
- Chưa lưu model ở bước này

Usage:
    python -m app.models.train_ranking_model
"""

import sys
from pathlib import Path
import polars as pl
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, roc_auc_score, confusion_matrix,
    classification_report
)
import io

# Fix encoding cho Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm root directory vào path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))


def load_ranking_dataset(dataset_path: Path) -> tuple:
    """
    Load ranking dataset và chuẩn bị features, labels.
    
    Args:
        dataset_path: Đường dẫn đến ranking dataset parquet
        
    Returns:
        Tuple (X, y) với X là features array, y là labels array
    """
    print("=" * 80)
    print("LOAD RANKING DATASET")
    print("=" * 80)
    
    if not dataset_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {dataset_path}")
    
    print(f"\nĐang đọc file: {dataset_path}")
    df = pl.read_parquet(str(dataset_path))
    print(f"[OK] Đã đọc {len(df):,} samples")
    print(f"  Columns: {df.columns}")
    
    # Kiểm tra schema
    required_cols = ['mf_score', 'content_score', 'popularity_score', 'rating_score', 'label']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Thiếu các cột: {missing_cols}")
    
    # Lấy features (theo thứ tự: mf_score, popularity_score, rating_score, content_score)
    feature_cols = ['mf_score', 'popularity_score', 'rating_score', 'content_score']
    X = df.select(feature_cols).to_numpy()
    y = df['label'].to_numpy()
    
    print(f"\nFeatures shape: {X.shape}")
    print(f"Labels shape: {y.shape}")
    
    # Thống kê features
    print(f"\nThống kê features:")
    for i, col in enumerate(feature_cols):
        print(f"  {col}:")
        print(f"    Min: {X[:, i].min():.4f}")
        print(f"    Max: {X[:, i].max():.4f}")
        print(f"    Mean: {X[:, i].mean():.4f}")
        print(f"    Std: {X[:, i].std():.4f}")
    
    # Thống kê labels
    unique, counts = np.unique(y, return_counts=True)
    print(f"\nThống kê labels:")
    for label, count in zip(unique, counts):
        pct = count / len(y) * 100
        print(f"  Label {label}: {count:,} ({pct:.2f}%)")
    
    return X, y, feature_cols


def train_logistic_regression(X_train: np.ndarray, y_train: np.ndarray) -> LogisticRegression:
    """
    Huấn luyện Logistic Regression với L2 regularization.
    
    Args:
        X_train: Training features
        y_train: Training labels
        
    Returns:
        Trained LogisticRegression model
    """
    print("\n" + "=" * 80)
    print("HUẤN LUYỆN LOGISTIC REGRESSION")
    print("=" * 80)
    
    print(f"\nTraining set size: {len(X_train):,} samples")
    print(f"Features: {X_train.shape[1]}")
    
    # Khởi tạo Logistic Regression với L2 regularization
    # C=1.0 là default, có thể điều chỉnh nếu cần
    # max_iter tăng lên để đảm bảo convergence
    # L2 regularization là default, không cần specify penalty
    model = LogisticRegression(
        C=1.0,                  # Inverse of regularization strength (smaller = stronger)
        max_iter=1000,          # Tăng số iteration để đảm bảo convergence
        random_state=42,        # Reproducibility
        solver='lbfgs'          # Solver phù hợp cho L2 penalty (default)
    )
    
    print(f"\nModel config:")
    print(f"  Penalty: L2 (default)")
    print(f"  C (regularization): {model.C}")
    print(f"  Solver: {model.solver}")
    print(f"  Max iterations: {model.max_iter}")
    
    print(f"\nĐang huấn luyện...")
    model.fit(X_train, y_train)
    print(f"[OK] Đã huấn luyện xong")
    
    # Kiểm tra convergence
    if hasattr(model, 'n_iter_'):
        print(f"  Số iterations thực tế: {model.n_iter_}")
    
    return model


def evaluate_model(
    model: LogisticRegression,
    X_val: np.ndarray,
    y_val: np.ndarray
) -> dict:
    """
    Đánh giá model trên validation set.
    
    Args:
        model: Trained LogisticRegression model
        X_val: Validation features
        y_val: Validation labels
        
    Returns:
        Dict chứa các metrics
    """
    print("\n" + "=" * 80)
    print("ĐÁNH GIÁ MODEL")
    print("=" * 80)
    
    print(f"\nValidation set size: {len(X_val):,} samples")
    
    # Predictions
    y_pred = model.predict(X_val)
    y_pred_proba = model.predict_proba(X_val)[:, 1]  # Probability của class 1
    
    # Tính các metrics
    accuracy = accuracy_score(y_val, y_pred)
    roc_auc = roc_auc_score(y_val, y_pred_proba)
    cm = confusion_matrix(y_val, y_pred)
    
    print(f"\nMetrics:")
    print(f"  Accuracy: {accuracy:.4f} ({accuracy*100:.2f}%)")
    print(f"  ROC-AUC: {roc_auc:.4f}")
    
    # Confusion matrix
    print(f"\nConfusion Matrix:")
    print(f"                Predicted")
    print(f"                0      1")
    print(f"Actual  0    {cm[0,0]:5d}  {cm[0,1]:5d}")
    print(f"        1    {cm[1,0]:5d}  {cm[1,1]:5d}")
    
    # Classification report
    print(f"\nClassification Report:")
    print(classification_report(y_val, y_pred, target_names=['Label 0', 'Label 1']))
    
    return {
        'accuracy': accuracy,
        'roc_auc': roc_auc,
        'confusion_matrix': cm,
        'y_pred': y_pred,
        'y_pred_proba': y_pred_proba
    }


def analyze_feature_importance(
    model: LogisticRegression,
    feature_names: list
) -> None:
    """
    Phân tích feature importance dựa trên coefficients.
    
    Args:
        model: Trained LogisticRegression model
        feature_names: Danh sách tên features
    """
    print("\n" + "=" * 80)
    print("PHÂN TÍCH FEATURE IMPORTANCE")
    print("=" * 80)
    
    # Lấy coefficients (trọng số)
    coefficients = model.coef_[0]  # model.coef_ có shape (1, n_features)
    intercept = model.intercept_[0]
    
    print(f"\nIntercept (bias): {intercept:.6f}")
    print(f"\nFeature Coefficients:")
    print(f"{'Feature':<20} {'Coefficient':<15} {'Abs Value':<15} {'Impact'}")
    print("-" * 70)
    
    # Sắp xếp theo absolute value để xem feature nào quan trọng nhất
    feature_importance = []
    for name, coef in zip(feature_names, coefficients):
        abs_coef = abs(coef)
        # Đánh giá impact dựa trên absolute value
        if abs_coef > 1.0:
            impact = "Very High"
        elif abs_coef > 0.5:
            impact = "High"
        elif abs_coef > 0.1:
            impact = "Medium"
        elif abs_coef > 0.01:
            impact = "Low"
        else:
            impact = "Very Low"
        
        feature_importance.append((name, coef, abs_coef, impact))
        print(f"{name:<20} {coef:>14.6f} {abs_coef:>14.6f} {impact}")
    
    # Sắp xếp theo absolute value
    feature_importance.sort(key=lambda x: x[2], reverse=True)
    
    print(f"\n{'='*80}")
    print("GIẢI THÍCH FEATURE IMPORTANCE")
    print(f"{'='*80}")
    
    print(f"\n1. Feature ảnh hưởng mạnh nhất:")
    top_feature = feature_importance[0]
    print(f"   - {top_feature[0]}: coefficient = {top_feature[1]:.6f}")
    if top_feature[1] > 0:
        print(f"     → Tăng {top_feature[0]} làm tăng xác suất label = 1")
    else:
        print(f"     → Tăng {top_feature[0]} làm giảm xác suất label = 1")
    
    print(f"\n2. Feature ít ảnh hưởng nhất:")
    bottom_feature = feature_importance[-1]
    print(f"   - {bottom_feature[0]}: coefficient = {bottom_feature[1]:.6f}")
    print(f"     → Feature này gần như không ảnh hưởng đến prediction")
    
    print(f"\n3. Tổng quan:")
    print(f"   - Features có coefficient > 0: tăng xác suất label = 1")
    print(f"   - Features có coefficient < 0: giảm xác suất label = 1")
    print(f"   - Absolute value càng lớn → ảnh hưởng càng mạnh")
    
    # Kiểm tra content_score
    content_idx = feature_names.index('content_score')
    content_coef = coefficients[content_idx]
    print(f"\n4. Đặc biệt - content_score:")
    print(f"   - Coefficient: {content_coef:.6f}")
    if abs(content_coef) < 0.01:
        print(f"   - → Như mong đợi, content_score (hiện tại = 0) có weight ≈ 0")
        print(f"   - → Model đã tự học rằng feature này không có thông tin")
    else:
        print(f"   - → Có weight khác 0, nhưng sẽ không ảnh hưởng vì giá trị = 0")


def main():
    """
    Hàm chính để train ranking model.
    """
    print("=" * 80)
    print("TRAIN RANKING MODEL (LOGISTIC REGRESSION BASELINE)")
    print("=" * 80)
    
    # Đường dẫn dataset
    project_root = BASE_DIR
    dataset_path = project_root / "artifacts" / "ranking" / "ranking_dataset.parquet"
    
    try:
        # Bước 1: Load dataset
        X, y, feature_names = load_ranking_dataset(dataset_path)
        
        # Bước 2: Split train/validation (80/20)
        print("\n" + "=" * 80)
        print("SPLIT DATASET")
        print("=" * 80)
        
        X_train, X_val, y_train, y_val = train_test_split(
            X, y,
            test_size=0.2,
            random_state=42,
            stratify=y  # Giữ tỷ lệ label giống nhau giữa train và val
        )
        
        print(f"\nTrain set: {len(X_train):,} samples ({len(X_train)/len(X)*100:.1f}%)")
        print(f"Validation set: {len(X_val):,} samples ({len(X_val)/len(X)*100:.1f}%)")
        
        # Thống kê label trong train và val
        train_labels, train_counts = np.unique(y_train, return_counts=True)
        val_labels, val_counts = np.unique(y_val, return_counts=True)
        
        print(f"\nLabel distribution:")
        print(f"  Train - Label 0: {train_counts[0]:,}, Label 1: {train_counts[1]:,}")
        print(f"  Val   - Label 0: {val_counts[0]:,}, Label 1: {val_counts[1]:,}")
        
        # Bước 3: Train model
        model = train_logistic_regression(X_train, y_train)
        
        # Bước 4: Evaluate model
        metrics = evaluate_model(model, X_val, y_val)
        
        # Bước 5: Analyze feature importance
        analyze_feature_importance(model, feature_names)
        
        # Tóm tắt
        print("\n" + "=" * 80)
        print("[OK] HOÀN TẤT: Ranking model đã được huấn luyện và đánh giá!")
        print("=" * 80)
        print(f"\nTóm tắt:")
        print(f"  - Model: Logistic Regression với L2 regularization")
        print(f"  - Training samples: {len(X_train):,}")
        print(f"  - Validation samples: {len(X_val):,}")
        print(f"  - Validation Accuracy: {metrics['accuracy']:.4f}")
        print(f"  - Validation ROC-AUC: {metrics['roc_auc']:.4f}")
        print(f"\nModel sẵn sàng để sử dụng cho ranking trong Hybrid Recommendation System.")
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("[ERROR] LỖI KHI TRAIN RANKING MODEL")
        print("=" * 80)
        print(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()

