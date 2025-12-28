"""
Save Ranking Model
==================

Script để train và lưu ranking model (Logistic Regression) cho online serving.

Usage:
    python -m app.models.save_ranking_model
"""

import sys
from pathlib import Path
import pickle
import json
import numpy as np

# Thêm root directory vào path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.models.train_ranking_model import (
    load_ranking_dataset,
    train_logistic_regression
)
from sklearn.model_selection import train_test_split


def save_model_and_metadata(
    model,
    feature_names: list,
    output_dir: Path
):
    """
    Lưu model và metadata.
    
    Args:
        model: Trained LogisticRegression model
        feature_names: List of feature names
        output_dir: Thư mục để lưu
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Lưu model
    model_path = output_dir / "ranking_model.pkl"
    print(f"\nĐang lưu model: {model_path}")
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    print(f"[OK] Model đã được lưu")
    
    # 2. Lưu metadata
    metadata = {
        'model_type': 'LogisticRegression',
        'feature_order': feature_names,
        'n_features': len(feature_names),
        'n_classes': len(model.classes_) if hasattr(model, 'classes_') else None,
        'coefficients': model.coef_[0].tolist() if hasattr(model, 'coef_') else None,
        'intercept': float(model.intercept_[0]) if hasattr(model, 'intercept_') else None
    }
    
    metadata_path = output_dir / "model_metadata.json"
    print(f"\nĐang lưu metadata: {metadata_path}")
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    print(f"[OK] Metadata đã được lưu")
    
    return model_path, metadata_path


def main():
    """
    Hàm chính để train và lưu ranking model.
    """
    print("=" * 80)
    print("TRAIN VÀ LƯU RANKING MODEL")
    print("=" * 80)
    
    # Đường dẫn
    project_root = BASE_DIR
    dataset_path = project_root / "artifacts" / "ranking" / "ranking_dataset.parquet"
    output_dir = project_root / "artifacts" / "ranking"
    
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
            stratify=y
        )
        
        print(f"\nTrain set: {len(X_train):,} samples")
        print(f"Validation set: {len(X_val):,} samples")
        
        # Bước 3: Train model trên toàn bộ training set
        print("\n" + "=" * 80)
        print("TRAIN MODEL")
        print("=" * 80)
        
        model = train_logistic_regression(X_train, y_train)
        
        # Bước 4: Evaluate trên validation set
        from app.models.train_ranking_model import evaluate_model
        metrics = evaluate_model(model, X_val, y_val)
        
        print(f"\nValidation Metrics:")
        print(f"  Accuracy: {metrics['accuracy']:.4f}")
        print(f"  ROC-AUC: {metrics['roc_auc']:.4f}")
        
        # Bước 5: Lưu model và metadata
        print("\n" + "=" * 80)
        print("SAVE MODEL")
        print("=" * 80)
        
        model_path, metadata_path = save_model_and_metadata(
            model,
            feature_names,
            output_dir
        )
        
        # Tóm tắt
        print("\n" + "=" * 80)
        print("[OK] HOÀN TẤT: Ranking model đã được train và lưu!")
        print("=" * 80)
        print(f"\nModel saved to: {model_path}")
        print(f"Metadata saved to: {metadata_path}")
        print(f"\nFeature order: {feature_names}")
        print(f"\nModel ready for online serving!")
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("[ERROR] THẤT BẠI")
        print("=" * 80)
        print(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

