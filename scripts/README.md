# Scripts - Training & Preprocessing

Thư mục này chứa các scripts để training models và preprocessing data.

## Cấu trúc

```
scripts/
├── data_preprocessing/    # Data preprocessing scripts
│   ├── phase1_ingest.py
│   ├── phase2_normalize.py
│   ├── phase3_cleaning.py
│   ├── phase4_build_interactions.py
│   ├── phase5_build_5core.py
│   └── ...
├── models/                # Model training scripts
│   ├── train_cf_final.py
│   ├── train_ranking_model.py
│   └── ...
├── embedding/             # Embedding training scripts
│   ├── train_item_embeddings.py
│   └── data_preprocessing/
└── database/              # Database migration scripts
    ├── migrations/
    └── scripts/
```

## Usage

Các scripts này được chạy local để:
1. Preprocess data
2. Train models
3. Generate artifacts (lưu vào `backend/artifacts/`)
4. Run database migrations

**Lưu ý**: Các scripts này không được deploy lên Render, chỉ chạy local hoặc trên máy training.

