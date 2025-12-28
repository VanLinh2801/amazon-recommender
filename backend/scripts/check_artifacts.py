"""
Script kiểm tra artifacts trước khi deploy.

Chạy script này để đảm bảo tất cả artifacts cần thiết đã có sẵn.

Usage:
    python scripts/check_artifacts.py
"""
import sys
from pathlib import Path

# Thêm backend vào path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

def check_artifacts():
    """Kiểm tra tất cả artifacts cần thiết."""
    artifacts_dir = BASE_DIR / "artifacts"
    
    if not artifacts_dir.exists():
        print(f"❌ ERROR: Thư mục artifacts không tồn tại: {artifacts_dir}")
        return False
    
    print(f"📁 Kiểm tra artifacts tại: {artifacts_dir}\n")
    
    # Danh sách artifacts cần thiết
    required_artifacts = {
        "mf": [
            "user_factors.npy",
            "item_factors.npy",
            "user2idx.json",
            "idx2item.json"
        ],
        "popularity": [
            "item_popularity_normalized.parquet"
        ],
        "ranking": [
            "ranking_model.pkl",
            "model_metadata.json"
        ],
        "embeddings": [
            "item_embeddings.npy",
            "item_ids.json"
        ]
    }
    
    all_ok = True
    
    for subdir, files in required_artifacts.items():
        subdir_path = artifacts_dir / subdir
        
        if not subdir_path.exists():
            print(f"❌ Thư mục không tồn tại: {subdir_path}")
            all_ok = False
            continue
        
        print(f"📂 {subdir}/")
        for file in files:
            file_path = subdir_path / file
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"   ✅ {file} ({size_mb:.2f} MB)")
            else:
                print(f"   ❌ {file} - KHÔNG TỒN TẠI")
                all_ok = False
    
    print("\n" + "=" * 60)
    if all_ok:
        print("✅ TẤT CẢ ARTIFACTS ĐÃ SẴN SÀNG!")
        print("=" * 60)
        return True
    else:
        print("❌ THIẾU ARTIFACTS - VUI LÒNG KIỂM TRA LẠI!")
        print("=" * 60)
        print("\n💡 Gợi ý:")
        print("   1. Chạy các script training để tạo artifacts")
        print("   2. Đảm bảo artifacts được commit vào Git hoặc")
        print("   3. Upload artifacts lên storage service (S3, etc.)")
        return False

if __name__ == "__main__":
    success = check_artifacts()
    sys.exit(0 if success else 1)

