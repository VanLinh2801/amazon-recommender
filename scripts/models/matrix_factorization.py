"""
Matrix Factorization Model với SGD cho Explicit Ratings
========================================================

Mô hình Collaborative Filtering sử dụng Matrix Factorization với:
- User bias và Item bias
- SGD-based training
- Regularization để tránh overfitting
- Phù hợp với dataset nhỏ
"""

import numpy as np
from typing import Dict, Tuple, Optional
import random


class MatrixFactorization:
    """
    Matrix Factorization model với SGD cho explicit ratings.
    
    Mô hình dự đoán rating: r_ui = μ + b_u + b_i + p_u^T * q_i
    
    Trong đó:
    - μ: global mean rating
    - b_u: user bias
    - b_i: item bias
    - p_u: user latent factors (k dimensions)
    - q_i: item latent factors (k dimensions)
    """
    
    def __init__(
        self,
        n_factors: int = 15,
        learning_rate: float = 0.01,
        reg_user: float = 0.1,
        reg_item: float = 0.1,
        reg_bias: float = 0.01,
        n_epochs: int = 50,
        random_state: Optional[int] = None
    ):
        """
        Khởi tạo mô hình Matrix Factorization.
        
        Args:
            n_factors: Số latent factors (k) - khuyến nghị 10-20 cho dataset nhỏ
            learning_rate: Learning rate cho SGD
            reg_user: Regularization cho user factors và bias
            reg_item: Regularization cho item factors và bias
            reg_bias: Regularization cho bias terms
            n_epochs: Số epochs huấn luyện
            random_state: Random seed để reproducibility
        """
        self.n_factors = n_factors
        self.learning_rate = learning_rate
        self.reg_user = reg_user
        self.reg_item = reg_item
        self.reg_bias = reg_bias
        self.n_epochs = n_epochs
        self.random_state = random_state
        
        # Model parameters (sẽ được khởi tạo khi fit)
        self.global_mean: float = 0.0
        self.user_bias: np.ndarray = None  # shape: (n_users,)
        self.item_bias: np.ndarray = None  # shape: (n_items,)
        self.user_factors: np.ndarray = None  # shape: (n_users, n_factors)
        self.item_factors: np.ndarray = None  # shape: (n_items, n_factors)
        
        # Mappings
        self.user_to_idx: Dict[str, int] = {}
        self.item_to_idx: Dict[str, int] = {}
        self.idx_to_user: Dict[int, str] = {}
        self.idx_to_item: Dict[int, str] = {}
        
        # Stats
        self.n_users: int = 0
        self.n_items: int = 0
        self.training_history: list = []
    
    def _init_parameters(self):
        """Khởi tạo các tham số mô hình với giá trị ngẫu nhiên nhỏ."""
        if self.random_state is not None:
            np.random.seed(self.random_state)
            random.seed(self.random_state)
        
        # Khởi tạo bias về 0
        self.user_bias = np.zeros(self.n_users)
        self.item_bias = np.zeros(self.n_items)
        
        # Khởi tạo latent factors với giá trị ngẫu nhiên nhỏ
        # Sử dụng normal distribution với std nhỏ để tránh initialization quá lớn
        scale = 0.1 / np.sqrt(self.n_factors)
        self.user_factors = np.random.normal(0, scale, (self.n_users, self.n_factors))
        self.item_factors = np.random.normal(0, scale, (self.n_items, self.n_factors))
    
    def _build_mappings(self, user_ids: np.ndarray, item_ids: np.ndarray):
        """
        Xây dựng mapping từ user_id/item_id (string) sang index (int).
        
        Args:
            user_ids: Array các user_id strings
            item_ids: Array các item_id strings
        """
        unique_users = np.unique(user_ids)
        unique_items = np.unique(item_ids)
        
        self.user_to_idx = {uid: idx for idx, uid in enumerate(unique_users)}
        self.item_to_idx = {iid: idx for idx, iid in enumerate(unique_items)}
        self.idx_to_user = {idx: uid for uid, idx in self.user_to_idx.items()}
        self.idx_to_item = {idx: iid for iid, idx in self.item_to_idx.items()}
        
        self.n_users = len(unique_users)
        self.n_items = len(unique_items)
    
    def _convert_to_indices(
        self, 
        user_ids: np.ndarray, 
        item_ids: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Chuyển đổi user_id và item_id strings sang internal indices.
        
        Args:
            user_ids: Array các user_id strings
            item_ids: Array các item_id strings
            
        Returns:
            Tuple (user_indices, item_indices)
        """
        user_indices = np.array([self.user_to_idx[uid] for uid in user_ids])
        item_indices = np.array([self.item_to_idx[iid] for iid in item_ids])
        return user_indices, item_indices
    
    def fit(
        self,
        user_ids: np.ndarray,
        item_ids: np.ndarray,
        ratings: np.ndarray,
        verbose: bool = True
    ):
        """
        Huấn luyện mô hình với SGD.
        
        Args:
            user_ids: Array các user_id strings
            item_ids: Array các item_id strings
            ratings: Array các ratings (float, 1-5)
            verbose: In ra progress nếu True
        """
        # Xây dựng mappings
        self._build_mappings(user_ids, item_ids)
        
        # Tính global mean
        self.global_mean = float(np.mean(ratings))
        
        # Khởi tạo parameters
        self._init_parameters()
        
        # Chuyển đổi sang indices
        user_indices, item_indices = self._convert_to_indices(user_ids, item_ids)
        ratings = ratings.astype(np.float32)
        
        # Training với SGD
        n_samples = len(ratings)
        
        if verbose:
            print(f"\nBắt đầu huấn luyện:")
            print(f"  - Số samples: {n_samples:,}")
            print(f"  - Số users: {self.n_users:,}")
            print(f"  - Số items: {self.n_items:,}")
            print(f"  - Latent factors: {self.n_factors}")
            print(f"  - Global mean rating: {self.global_mean:.3f}")
            print(f"  - Learning rate: {self.learning_rate}")
            print(f"  - Regularization: user={self.reg_user}, item={self.reg_item}, bias={self.reg_bias}")
            print(f"  - Epochs: {self.n_epochs}")
        
        self.training_history = []
        
        for epoch in range(self.n_epochs):
            # Shuffle data mỗi epoch
            indices = np.arange(n_samples)
            np.random.shuffle(indices)
            
            epoch_error = 0.0
            
            # SGD: update từng sample một
            for idx in indices:
                u = user_indices[idx]
                i = item_indices[idx]
                r = ratings[idx]
                
                # Dự đoán rating
                pred = self._predict_single(u, i)
                
                # Tính error
                error = r - pred
                epoch_error += error ** 2
                
                # Update user bias
                user_bias_update = self.learning_rate * (error - self.reg_bias * self.user_bias[u])
                self.user_bias[u] += user_bias_update
                
                # Update item bias
                item_bias_update = self.learning_rate * (error - self.reg_bias * self.item_bias[i])
                self.item_bias[i] += item_bias_update
                
                # Update user factors
                user_factor_update = self.learning_rate * (
                    error * self.item_factors[i] - self.reg_user * self.user_factors[u]
                )
                self.user_factors[u] += user_factor_update
                
                # Update item factors
                item_factor_update = self.learning_rate * (
                    error * self.user_factors[u] - self.reg_item * self.item_factors[i]
                )
                self.item_factors[i] += item_factor_update
            
            # Tính RMSE cho epoch này
            epoch_rmse = np.sqrt(epoch_error / n_samples)
            self.training_history.append(epoch_rmse)
            
            if verbose and (epoch + 1) % 10 == 0:
                print(f"  Epoch {epoch + 1}/{self.n_epochs} - RMSE: {epoch_rmse:.4f}")
        
        if verbose:
            final_rmse = self.training_history[-1]
            print(f"\nHuấn luyện hoàn tất!")
            print(f"  Final RMSE: {final_rmse:.4f}")
    
    def _predict_single(self, user_idx: int, item_idx: int) -> float:
        """
        Dự đoán rating cho một (user, item) pair sử dụng internal indices.
        
        Args:
            user_idx: User index (internal)
            item_idx: Item index (internal)
            
        Returns:
            Predicted rating
        """
        pred = (
            self.global_mean +
            self.user_bias[user_idx] +
            self.item_bias[item_idx] +
            np.dot(self.user_factors[user_idx], self.item_factors[item_idx])
        )
        # Clip về range [1, 5]
        return np.clip(pred, 1.0, 5.0)
    
    def predict(
        self,
        user_ids: np.ndarray,
        item_ids: np.ndarray
    ) -> np.ndarray:
        """
        Dự đoán ratings cho nhiều (user, item) pairs.
        
        Args:
            user_ids: Array các user_id strings
            item_ids: Array các item_id strings
            
        Returns:
            Array các predicted ratings
        """
        # Kiểm tra user/item có trong training data không
        unknown_users = set(user_ids) - set(self.user_to_idx.keys())
        unknown_items = set(item_ids) - set(self.item_to_idx.keys())
        
        if unknown_users:
            print(f"[WARNING] Có {len(unknown_users)} users chưa thấy trong training data")
        if unknown_items:
            print(f"[WARNING] Có {len(unknown_items)} items chưa thấy trong training data")
        
        # Chuyển đổi sang indices (sử dụng default cho unknown)
        user_indices = np.array([
            self.user_to_idx.get(uid, 0) for uid in user_ids
        ])
        item_indices = np.array([
            self.item_to_idx.get(iid, 0) for iid in item_ids
        ])
        
        # Dự đoán
        predictions = np.array([
            self._predict_single(u, i) 
            for u, i in zip(user_indices, item_indices)
        ])
        
        return predictions
    
    def get_rmse(
        self,
        user_ids: np.ndarray,
        item_ids: np.ndarray,
        ratings: np.ndarray
    ) -> float:
        """
        Tính RMSE trên một tập dữ liệu.
        
        Args:
            user_ids: Array các user_id strings
            item_ids: Array các item_id strings
            ratings: Array các true ratings
            
        Returns:
            RMSE value
        """
        predictions = self.predict(user_ids, item_ids)
        mse = np.mean((ratings - predictions) ** 2)
        return np.sqrt(mse)

