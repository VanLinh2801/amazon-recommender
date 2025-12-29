# Hệ Thống Recommendation System - Tài Liệu Kỹ Thuật

## Tổng Quan

Hệ thống recommendation này là một **Hybrid Recommendation System** kết hợp nhiều phương pháp:
- **Collaborative Filtering** (Matrix Factorization)
- **Content-Based Filtering** (Semantic Embeddings)
- **Popularity-Based Filtering**
- **Learning-to-Rank** (Logistic Regression)

Hệ thống sử dụng kiến trúc **3-stage pipeline**: Recall → Ranking → Re-ranking để tạo ra recommendations chất lượng cao và đa dạng.

---

## 1. Dataset

### 1.1 Nguồn Dữ Liệu

- **Dataset**: Amazon Product Reviews - All Beauty Category
- **Format**: JSONL files
  - `All_Beauty.jsonl`: User reviews và ratings
  - `meta_All_Beauty.jsonl`: Product metadata

### 1.2 Cấu Trúc Dữ Liệu

#### Reviews Data (`All_Beauty.jsonl`)
- `reviewerID`: Amazon user ID
- `asin`: Product ASIN
- `overall`: Rating (1-5)
- `reviewText`: Review text
- `summary`: Review summary
- `unixReviewTime`: Timestamp
- `verified`: Verified purchase flag
- `helpful`: Helpful votes

#### Metadata (`meta_All_Beauty.jsonl`)
- `asin`: Product ASIN
- `title`: Product title
- `price`: Product price
- `brand`: Product brand
- `category`: Product categories
- `description`: Product description
- `imageURL`: Product images
- `related`: Related products

### 1.3 Quy Trình Xử Lý Dữ Liệu

Hệ thống xử lý dữ liệu qua **5 phases**:

#### Phase 1: Data Ingestion
- Đọc JSONL files từ `data/raw/`
- Parse và validate dữ liệu
- Lưu vào Parquet format để tối ưu I/O
- **Output**: `reviews_raw.parquet`, `metadata_raw.parquet`

#### Phase 2: Data Normalization
- Chuẩn hóa schema và data types
- Rename columns để nhất quán
- Normalize timestamps
- **Output**: `reviews_normalized.parquet`, `metadata_normalized.parquet`

#### Phase 3: Data Cleaning
**Reviews Cleaning:**
1. **Missing Values**: 
   - Drop records thiếu `amazon_user_id`, `asin`, hoặc `rating`
   - Fill null values: `review_title=""`, `review_text=""`, `helpful_vote=0`, `verified=False`

2. **Sanity Checks**:
   - Filter ratings trong khoảng [1, 5]
   - Fix `helpful_vote < 0` → `0`

3. **Deduplication**:
   - Remove duplicate reviews (same `amazon_user_id` + `asin` + `review_text`)
   - Ưu tiên giữ review có `helpful_vote` cao hơn

4. **Type Normalization**:
   - Convert types đúng format
   - Normalize strings

5. **Feature Pruning**:
   - Remove columns không cần thiết
   - Keep essential features

**Metadata Cleaning:**
1. **Missing Values**:
   - Drop records thiếu `parent_asin` hoặc `title`
   - Fill null: `store="Unknown"`, `rating_number=0`

2. **Sanity Checks**:
   - Fix `rating_number < 0` → `0`

3. **Deduplication**:
   - Remove duplicate `parent_asin`
   - Ưu tiên record có `primary_image` và `details` đầy đủ

4. **Type Normalization**:
   - Normalize categories, prices, ratings

5. **Feature Pruning**:
   - Extract và normalize essential features

**Output**: `reviews_clean.parquet`, `metadata_clean.parquet`

#### Phase 4: Build Interactions
- Merge reviews và metadata
- Tạo interaction records: `(user_id, item_id, rating, timestamp)`
- Tính toán item popularity scores
- **Output**: `interactions_all.parquet`, `item_popularity.parquet`

#### Phase 5: Build 5-Core Dataset
- **5-Core Filtering**: Iterative filtering để đảm bảo:
  - Mỗi user có ≥ 5 interactions
  - Mỗi item có ≥ 5 interactions
- **Train/Test Split**: Time-based split
  - Sort theo `user_id` và `timestamp`
  - Giữ 1 interaction cuối cùng của mỗi user làm test set
  - Phần còn lại làm train set
- **Output**: 
  - `interactions_5core_train.parquet`
  - `interactions_5core_test.parquet`
  - `interactions_5core.parquet` (full dataset)

### 1.4 Thống Kê Dataset

Sau khi xử lý, dataset thường có:
- **Users**: ~10,000 - 50,000 users
- **Items**: ~5,000 - 20,000 items
- **Interactions**: ~100,000 - 500,000 interactions
- **Sparsity**: ~99%+ (rất sparse)
- **Train/Test Ratio**: ~80/20

---

## 2. Models

### 2.1 Matrix Factorization (Collaborative Filtering)

#### Mô Hình
**Công thức dự đoán rating:**
```
r_ui = μ + b_u + b_i + p_u^T * q_i
```

Trong đó:
- `μ`: Global mean rating
- `b_u`: User bias
- `b_i`: Item bias
- `p_u`: User latent factors (k dimensions)
- `q_i`: Item latent factors (k dimensions)

#### Hyperparameters
- `n_factors`: 15 (số latent dimensions)
- `learning_rate`: 0.01
- `reg_user`: 0.1 (L2 regularization cho user factors)
- `reg_item`: 0.1 (L2 regularization cho item factors)
- `reg_bias`: 0.01 (L2 regularization cho bias terms)
- `n_epochs`: 50
- `random_state`: 42

#### Training Process
1. **Initialize parameters**:
   - User/Item biases: khởi tạo = 0
   - Latent factors: random normal distribution với scale = 0.1/√k

2. **SGD Training**:
   - Shuffle training data mỗi epoch
   - Update parameters với gradient descent:
     ```
     error = rating - prediction
     b_u += lr * (error - reg_user * b_u)
     b_i += lr * (error - reg_item * b_i)
     p_u += lr * (error * q_i - reg_user * p_u)
     q_i += lr * (error * p_u - reg_item * q_i)
     ```

3. **Evaluation**:
   - RMSE (Root Mean Squared Error)
   - MAE (Mean Absolute Error)
   - Precision@K, Recall@K

#### Output Artifacts
- `user_factors.npy`: User latent factors matrix (n_users × k)
- `item_factors.npy`: Item latent factors matrix (n_items × k)
- `user_bias.npy`: User bias vector
- `item_bias.npy`: Item bias vector
- `user2idx.json`: Mapping user_id → index
- `item2idx.json`: Mapping item_id → index
- `global_mean.json`: Global mean rating

**Location**: `backend/artifacts/mf/`

### 2.2 Ranking Model (Logistic Regression)

#### Mô Hình
**Logistic Regression** với L2 regularization để rank candidate items.

#### Features (4 features)
1. **mf_score**: Matrix Factorization score (dot product của user và item factors)
2. **popularity_score**: Item popularity score (normalized)
3. **rating_score**: Average rating (normalized từ [1,5] về [0,1])
4. **content_score**: Content-based similarity score (cosine similarity của embeddings)

#### Training Dataset
- **Source**: `artifacts/ranking/ranking_dataset.parquet`
- **Features**: `[mf_score, popularity_score, rating_score, content_score]`
- **Label**: Binary label (1 = positive interaction, 0 = negative)
- **Split**: Train/Validation split (80/20)

#### Hyperparameters
- `C`: 1.0 (inverse of regularization strength)
- `penalty`: L2 (default)
- `solver`: 'lbfgs'
- `max_iter`: 1000
- `random_state`: 42

#### Training Process
1. Load ranking dataset
2. Split train/validation
3. Train Logistic Regression với L2 regularization
4. Evaluate trên validation set:
   - Accuracy
   - ROC-AUC
   - Precision, Recall, F1-score
5. Save model và metadata

#### Output Artifacts
- `ranking_model.pkl`: Trained Logistic Regression model
- `model_metadata.json`: Model metadata (feature order, metrics, etc.)

**Location**: `backend/artifacts/ranking/`

### 2.3 Embedding Model (Content-Based)

#### Mô Hình
**BAAI/bge-large-en-v1.5** - Pretrained sentence transformer model
- **Architecture**: BERT-based
- **Dimensions**: 1024
- **Task**: Semantic text embeddings
- **Fine-tuning**: Không fine-tune, chỉ sử dụng pretrained weights

#### Embedding Text Construction
Mỗi item được biểu diễn bằng text kết hợp:
- Product title
- Main category
- Brand (nếu có)
- Semantic attributes (extracted từ metadata)

**Format**: `"{title} | {category} | {brand} | {attributes}"`

#### Training Process
1. **Data Preparation**:
   - Load `data/embedding/embedding_text.parquet`
   - Clean và normalize text
   - Remove items không có embedding text

2. **Model Loading**:
   - Load pretrained `BAAI/bge-large-en-v1.5` từ HuggingFace
   - Move to GPU nếu có (khuyến nghị chạy trên Google Colab với GPU)

3. **Encoding**:
   - Batch encoding với batch size = 32-64
   - Normalize embeddings (L2 normalization)
   - Save embeddings và item IDs

#### Output Artifacts
- `item_embeddings.npy`: Embedding matrix (n_items × 1024)
- `item_ids.json`: List of item IDs theo thứ tự

**Location**: `backend/artifacts/embeddings/`

### 2.4 Re-ranking Service (Rule-Based)

#### Mục Đích
Điều chỉnh ranking scores dựa trên:
- Short-term user intent (từ Redis)
- Diversity (tránh quá nhiều items cùng category)
- Recent items penalty (tránh recommend items đã xem)

#### Rules

1. **Short-term Intent Boost**:
   - Boost items có category trùng với recent categories (từ Redis)
   - Boost factor: 1.2x

2. **Recent Items Penalty**:
   - Penalize items đã xem gần đây (từ Redis)
   - Penalty factor: 0.5x

3. **Diversity Penalty**:
   - Penalize items nếu quá nhiều items cùng category trong top-N
   - Threshold: 25% (max 4 items cùng category trong top 20)
   - Penalty factor: 0.7x

4. **Popularity Floor** (Optional):
   - Đảm bảo items có ít nhất một số ratings tối thiểu
   - Threshold: 5 ratings

#### Redis Context
- `user:{user_id}:recent_items`: List of recent item ASINs
- `user:{user_id}:recent_categories`: Dict of recent categories với counts

---

## 3. Recommendation Pipeline

### 3.1 Architecture Overview

```
User Request
    ↓
┌─────────────────────────────────────────┐
│         RECALL LAYER                   │
│  ┌──────────┐  ┌──────────┐  ┌──────┐│
│  │ MF Recall│  │Popularity │  │Content││
│  │  (k=100) │  │  (k=50)  │  │(k=50)││
│  └──────────┘  └──────────┘  └──────┘│
│         ↓           ↓           ↓      │
│         └───────────┴───────────┘    │
│              Merge & Dedup           │
└─────────────────────────────────────────┘
              ↓ (~150 candidates)
┌─────────────────────────────────────────┐
│         RANKING LAYER                  │
│  ┌──────────────────────────────────┐  │
│  │  Logistic Regression Model       │  │
│  │  Features:                      │  │
│  │  - mf_score                     │  │
│  │  - popularity_score             │  │
│  │  - rating_score                 │  │
│  │  - content_score                │  │
│  └──────────────────────────────────┘  │
└─────────────────────────────────────────┘
              ↓ (~50 ranked items)
┌─────────────────────────────────────────┐
│         RE-RANKING LAYER               │
│  ┌──────────────────────────────────┐  │
│  │  Rule-Based Re-ranking           │  │
│  │  - Short-term intent boost       │  │
│  │  - Recent items penalty          │  │
│  │  - Diversity penalty             │  │
│  └──────────────────────────────────┘  │
└─────────────────────────────────────────┘
              ↓ (top 20 items)
         Final Recommendations
```

### 3.2 Recall Layer

#### Matrix Factorization Recall
- Tính dot product: `user_factors[u] · item_factors[i]`
- Lấy top-K items có score cao nhất
- **K**: 100 items

#### Popularity Recall
- Lấy top-K items theo popularity score
- Popularity score = normalized interaction count
- **K**: 50 items

#### Content-Based Recall
- Tính cosine similarity giữa:
  - User reference items embeddings (từ history)
  - All item embeddings
- Lấy top-K items có similarity cao nhất
- **K**: 50 items

#### Merge Strategy
- Union tất cả candidates từ 3 sources
- Remove duplicates
- **Total candidates**: ~150 items (có thể ít hơn do overlap)

### 3.3 Ranking Layer

#### Feature Engineering
Với mỗi candidate item:
1. **mf_score**: 
   - Nếu user có trong MF model: `user_factors[u] · item_factors[i]`
   - Nếu không: 0.0

2. **popularity_score**: 
   - Lấy từ popularity lookup table
   - Normalized [0, 1]

3. **rating_score**: 
   - Average rating normalized: `(avg_rating - 1) / 4`
   - Range: [0, 1]

4. **content_score**: 
   - Cosine similarity với user reference items
   - Range: [0, 1]

#### Prediction
- Input: Feature vector `[mf_score, popularity_score, rating_score, content_score]`
- Model: Logistic Regression
- Output: Probability score (0-1)
- Sort: Descending by probability

#### Score Normalization
- Áp dụng normalization để giảm dominance của một feature
- Sử dụng `ScoreNormalizer` để normalize scores trước khi ranking

### 3.4 Re-ranking Layer

#### Process
1. Load Redis context (recent items, recent categories)
2. Với mỗi ranked item, áp dụng rules:
   - Tính adjusted_score từ rank_score
   - Apply boosts và penalties
   - Track applied rules
3. Sort lại theo adjusted_score
4. Apply diversity constraint (max items cùng category)
5. Return top-N items

---

## 4. Training Workflow

### 4.1 Data Preprocessing Pipeline

```bash
# Phase 1: Ingest
python scripts/data_preprocessing/phase1_ingest.py

# Phase 2: Normalize
python scripts/data_preprocessing/phase2_normalize.py

# Phase 3: Clean
python scripts/data_preprocessing/phase3_cleaning.py

# Phase 4: Build Interactions
python scripts/data_preprocessing/phase4_build_interactions.py

# Phase 5: Build 5-Core
python scripts/data_preprocessing/phase5_build_5core.py
```

### 4.2 Embedding Generation

```bash
# Prepare embedding text
python scripts/embedding/data_preprocessing/build_embedding_text.py

# Train embeddings (chạy trên Google Colab với GPU)
python scripts/embedding/train_item_embeddings.py
```

### 4.3 Model Training

#### Matrix Factorization
```bash
# Train MF model
python scripts/models/train_cf_final.py
```

#### Ranking Model
```bash
# Build ranking dataset
python scripts/data_preprocessing/build_ranking_dataset.py

# Train ranking model
python scripts/models/train_ranking_model.py

# Save model
python scripts/models/save_ranking_model.py
```

### 4.4 Evaluation

```bash
# Evaluate recommendation pipeline
python scripts/models/evaluate_recommendation_pipeline_improved.py

# Analyze metrics
python scripts/models/analyze_metrics.py
```

---

## 5. Model Metrics

### 5.1 Matrix Factorization Metrics
- **RMSE**: ~0.8 - 1.2 (trên test set)
- **MAE**: ~0.6 - 0.9
- **Precision@10**: ~0.15 - 0.25
- **Recall@10**: ~0.10 - 0.20

### 5.2 Ranking Model Metrics
- **Accuracy**: ~0.70 - 0.80
- **ROC-AUC**: ~0.75 - 0.85
- **Precision**: ~0.65 - 0.75
- **Recall**: ~0.60 - 0.70

### 5.3 End-to-End Pipeline Metrics
- **Precision@5**: ~0.20 - 0.30
- **Recall@5**: ~0.15 - 0.25
- **Precision@10**: ~0.18 - 0.28
- **Recall@10**: ~0.20 - 0.30
- **Precision@20**: ~0.15 - 0.25
- **Recall@20**: ~0.25 - 0.35

---

## 6. Deployment

### 6.1 Backend Services

- **FastAPI**: REST API server
- **PostgreSQL**: Long-term storage (interactions, items, users)
- **Redis**: Short-term memory (recent items, categories)
- **Qdrant**: Vector database (optional, cho content-based search)

### 6.2 API Endpoints

- `GET /api/recommend`: Get recommendations
- `GET /api/recommend/similar/{asin}`: Get similar items
- `POST /api/event`: Log user interactions
- `GET /api/items/{asin}`: Get item details

### 6.3 Real-time Features

- **Redis Context**: Update real-time khi user tương tác
- **Trending Items**: Tính toán dựa trên recent views (24h)
- **Randomization**: Shuffle recommendations với seed để đa dạng

---

## 7. Fine-tuning và Optimization

### 7.1 Matrix Factorization
- **Hyperparameter Tuning**: Grid search cho `n_factors`, `learning_rate`, `regularization`
- **Early Stopping**: Dựa trên validation RMSE
- **Cold Start Handling**: Fallback to popularity cho users/items mới

### 7.2 Ranking Model
- **Feature Engineering**: Thử thêm features (category similarity, brand preference, etc.)
- **Model Selection**: Thử các models khác (XGBoost, Neural Networks)
- **Class Imbalance**: Xử lý imbalance trong training data

### 7.3 Re-ranking
- **Rule Tuning**: Điều chỉnh boost/penalty factors dựa trên A/B testing
- **Diversity Metrics**: Monitor và optimize diversity metrics (category diversity, etc.)

---

## 8. Future Improvements

1. **Deep Learning Models**: 
   - Neural Collaborative Filtering (NCF)
   - Wide & Deep model
   - Two-tower architecture

2. **Advanced Embeddings**:
   - Fine-tune BGE model trên domain-specific data
   - Multi-modal embeddings (text + image)

3. **Real-time Learning**:
   - Online learning cho ranking model
   - Incremental MF updates

4. **Explainability**:
   - Feature importance analysis
   - Recommendation explanations

5. **Scalability**:
   - Distributed training
   - Model serving optimization
   - Caching strategies

---

## 9. File Structure

```
Recommender/
├── backend/
│   ├── app/
│   │   ├── recommender/
│   │   │   ├── recall_service.py      # Recall layer
│   │   │   ├── ranking_service.py    # Ranking layer
│   │   │   └── reranking_service.py  # Re-ranking layer
│   │   └── web/
│   │       ├── routes/
│   │       │   └── recommend.py      # API endpoints
│   │       └── services/
│   │           └── recommendation_service.py
│   └── artifacts/
│       ├── mf/                       # MF model artifacts
│       ├── ranking/                  # Ranking model artifacts
│       └── embeddings/              # Item embeddings
├── scripts/
│   ├── data_preprocessing/           # Data processing pipeline
│   ├── models/                       # Model training scripts
│   └── embedding/                   # Embedding generation
└── data/
    ├── raw/                          # Raw JSONL files
    └── processed/                    # Processed Parquet files
```

---

## 10. References

- **Matrix Factorization**: Koren et al., "Matrix Factorization Techniques for Recommender Systems"
- **BGE Model**: https://huggingface.co/BAAI/bge-large-en-v1.5
- **Learning to Rank**: Liu, "Learning to Rank for Information Retrieval"
- **Hybrid Recommender Systems**: Burke, "Hybrid Recommender Systems: Survey and Experiments"

---

**Last Updated**: 2024
**Version**: 1.0

