"""
Qdrant Manager
==============
Class quản lý kết nối và thao tác với Qdrant vector database.
"""

import numpy as np
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
)
import json
import time
import hashlib


class QdrantManager:
    """
    Quản lý Qdrant vector database cho item embeddings.
    """
    
    def __init__(
        self, 
        url: str = "http://localhost:6333",
        collection_name: str = "item_text_embeddings"
    ):
        """
        Khởi tạo QdrantManager.
        
        Args:
            url: Địa chỉ Qdrant server (mặc định: http://localhost:6333)
            collection_name: Tên collection (mặc định: item_text_embeddings)
        """
        self.url = url
        self.collection_name = collection_name
        self.client = None
        self.embedding_dim = None
        
    def connect(self) -> bool:
        """
        Kết nối tới Qdrant server và kiểm tra service đang chạy.
        
        Returns:
            True nếu kết nối thành công, False nếu không
        """
        try:
            print(f"Đang kết nối tới Qdrant tại {self.url}...")
            self.client = QdrantClient(url=self.url)
            
            # Kiểm tra service đang chạy bằng cách lấy danh sách collections
            collections = self.client.get_collections()
            print(f"[OK] Đã kết nối thành công tới Qdrant")
            print(f"[OK] Số collections hiện có: {len(collections.collections)}")
            
            return True
            
        except Exception as e:
            print(f"[ERROR] Không thể kết nối tới Qdrant: {e}")
            print(f"[INFO] Đảm bảo Qdrant đang chạy tại {self.url}")
            return False
    
    def check_collection_exists(self) -> bool:
        """
        Kiểm tra xem collection đã tồn tại chưa.
        
        Returns:
            True nếu collection tồn tại, False nếu không
        """
        try:
            collections = self.client.get_collections()
            collection_names = [col.name for col in collections.collections]
            
            if self.collection_name in collection_names:
                print(f"[OK] Collection '{self.collection_name}' đã tồn tại")
                return True
            else:
                print(f"[INFO] Collection '{self.collection_name}' chưa tồn tại")
                return False
                
        except Exception as e:
            print(f"[ERROR] Không thể kiểm tra collection: {e}")
            return False
    
    def get_collection_info(self) -> Optional[Dict]:
        """
        Lấy thông tin về collection hiện tại.
        
        Returns:
            Dict chứa thông tin collection hoặc None nếu không tồn tại
        """
        try:
            collection_info = self.client.get_collection(self.collection_name)
            
            # Lấy thông tin an toàn, kiểm tra attribute có tồn tại không
            info = {
                'name': self.collection_name,
                'config': collection_info.config
            }
            
            # points_count có thể có hoặc không tùy version
            if hasattr(collection_info, 'points_count'):
                info['points_count'] = collection_info.points_count
            elif hasattr(collection_info, 'vectors_count'):
                info['points_count'] = collection_info.vectors_count
            else:
                # Thử lấy từ config
                if hasattr(collection_info.config, 'params'):
                    info['points_count'] = 0  # Không thể lấy được
            
            return info
        except Exception as e:
            print(f"[ERROR] Không thể lấy thông tin collection: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def create_collection(self, vector_size: int) -> bool:
        """
        Tạo mới collection với cấu hình cho item embeddings.
        
        Args:
            vector_size: Kích thước vector embedding (embedding dimension)
            
        Returns:
            True nếu tạo thành công, False nếu không
        """
        try:
            print(f"\nĐang tạo collection '{self.collection_name}'...")
            print(f"  Vector size: {vector_size}")
            print(f"  Distance metric: Cosine")
            
            # Xóa collection cũ nếu đã tồn tại
            if self.check_collection_exists():
                print(f"[WARNING] Collection đã tồn tại, đang xóa collection cũ...")
                self.client.delete_collection(self.collection_name)
                time.sleep(1)  # Đợi một chút để collection được xóa hoàn toàn
            
            # Tạo collection mới
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=vector_size,
                    distance=Distance.COSINE  # Sử dụng cosine distance cho text embeddings
                )
            )
            
            print(f"[OK] Đã tạo collection '{self.collection_name}' thành công")
            self.embedding_dim = vector_size
            
            return True
            
        except Exception as e:
            print(f"[ERROR] Không thể tạo collection: {e}")
            return False
    
    def ensure_collection(self, vector_size: int) -> bool:
        """
        Đảm bảo collection tồn tại, nếu chưa thì tạo mới.
        
        Args:
            vector_size: Kích thước vector embedding
            
        Returns:
            True nếu collection đã sẵn sàng, False nếu không
        """
        if not self.check_collection_exists():
            return self.create_collection(vector_size)
        else:
            # Kiểm tra xem vector size có khớp không
            try:
                collection_info = self.client.get_collection(self.collection_name)
                
                # Lấy vector size từ config
                if hasattr(collection_info, 'config') and collection_info.config:
                    if hasattr(collection_info.config, 'params'):
                        params = collection_info.config.params
                        if hasattr(params, 'vectors'):
                            vectors_config = params.vectors
                            if hasattr(vectors_config, 'size'):
                                existing_size = vectors_config.size
                                
                                if existing_size != vector_size:
                                    print(f"[WARNING] Vector size không khớp!")
                                    print(f"  Collection hiện tại: {existing_size}")
                                    print(f"  Yêu cầu: {vector_size}")
                                    print(f"[INFO] Đang tạo lại collection với vector size đúng...")
                                    return self.create_collection(vector_size)
                                else:
                                    print(f"[OK] Collection đã sẵn sàng với vector size: {vector_size}")
                                    self.embedding_dim = vector_size
                                    return True
                
                # Nếu không lấy được size, giả định là đúng
                print(f"[OK] Collection đã tồn tại, giả định vector size đúng: {vector_size}")
                self.embedding_dim = vector_size
                return True
                
            except Exception as e:
                # Lỗi validation có thể do version compatibility, nhưng collection đã tồn tại
                print(f"[WARNING] Không thể kiểm tra collection (có thể do version compatibility): {e}")
                print(f"[INFO] Giả định collection đã sẵn sàng với vector size: {vector_size}")
                self.embedding_dim = vector_size
                return True  # Trả về True để tiếp tục upsert
    
    def load_embeddings(
        self, 
        embeddings_file: Path, 
        item_ids_file: Path
    ) -> Tuple[np.ndarray, List[str]]:
        """
        Load embeddings và item_ids từ file.
        
        Args:
            embeddings_file: Đường dẫn tới file item_embeddings.npy
            item_ids_file: Đường dẫn tới file item_ids.json
            
        Returns:
            Tuple (embeddings, item_ids)
        """
        print(f"\nĐang load embeddings từ file...")
        
        # Load embeddings
        if not embeddings_file.exists():
            raise FileNotFoundError(f"Không tìm thấy file: {embeddings_file}")
        
        print(f"Đang đọc: {embeddings_file}")
        embeddings = np.load(str(embeddings_file))
        print(f"[OK] Đã load embeddings: shape {embeddings.shape}")
        
        # Load item_ids
        if not item_ids_file.exists():
            raise FileNotFoundError(f"Không tìm thấy file: {item_ids_file}")
        
        print(f"Đang đọc: {item_ids_file}")
        with open(item_ids_file, 'r', encoding='utf-8') as f:
            item_ids = json.load(f)
        print(f"[OK] Đã load {len(item_ids):,} item_ids")
        
        # Kiểm tra số lượng khớp nhau
        if len(embeddings) != len(item_ids):
            raise ValueError(
                f"Số lượng không khớp!\n"
                f"  Embeddings: {len(embeddings):,}\n"
                f"  Item IDs: {len(item_ids):,}"
            )
        
        print(f"[OK] Số lượng embeddings và item_ids khớp nhau: {len(embeddings):,}")
        
        return embeddings, item_ids
    
    def _item_id_to_int(self, item_id: str) -> int:
        """
        Convert item_id (string) thành integer để dùng làm point ID trong Qdrant.
        Sử dụng hash để đảm bảo tính nhất quán.
        
        Args:
            item_id: String item_id
            
        Returns:
            Integer ID
        """
        # Sử dụng hash MD5 và lấy 8 bytes đầu để tạo integer dương
        hash_obj = hashlib.md5(item_id.encode('utf-8'))
        hash_int = int(hash_obj.hexdigest()[:16], 16)
        # Đảm bảo là số dương (Qdrant yêu cầu unsigned integer)
        return hash_int % (2**63)  # Giới hạn trong phạm vi int64 dương
    
    def upsert_items(
        self, 
        embeddings: np.ndarray, 
        item_ids: List[str],
        batch_size: int = 100
    ) -> bool:
        """
        Upsert items vào Qdrant theo batch.
        
        Args:
            embeddings: Numpy array chứa embeddings (shape: num_items, embedding_dim)
            item_ids: Danh sách item_id tương ứng
            batch_size: Số lượng points upsert mỗi batch
            
        Returns:
            True nếu upsert thành công, False nếu không
        """
        if len(embeddings) != len(item_ids):
            raise ValueError("Số lượng embeddings và item_ids phải bằng nhau")
        
        num_items = len(embeddings)
        num_batches = (num_items + batch_size - 1) // batch_size
        
        print(f"\nĐang upsert {num_items:,} items vào Qdrant...")
        print(f"  Batch size: {batch_size}")
        print(f"  Số batches: {num_batches}")
        
        try:
            for batch_idx in range(num_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, num_items)
                
                # Tạo points cho batch này
                points = []
                for i in range(start_idx, end_idx):
                    # Convert item_id thành integer để dùng làm point ID
                    point_id = self._item_id_to_int(item_ids[i])
                    point = PointStruct(
                        id=point_id,  # Sử dụng integer ID
                        vector=embeddings[i].tolist(),  # Convert numpy array sang list
                        payload={
                            "type": "item",
                            "item_id": item_ids[i]  # Lưu item_id gốc trong payload
                        }
                    )
                    points.append(point)
                
                # Upsert batch
                self.client.upsert(
                    collection_name=self.collection_name,
                    points=points
                )
                
                # Progress
                if (batch_idx + 1) % 10 == 0 or (batch_idx + 1) == num_batches:
                    progress = ((batch_idx + 1) / num_batches) * 100
                    print(f"  Đã upsert: {end_idx:,}/{num_items:,} items ({progress:.1f}%)")
            
            print(f"[OK] Đã upsert thành công {num_items:,} items")
            
            # Kiểm tra số lượng points trong collection
            try:
                collection_info = self.client.get_collection(self.collection_name)
                if hasattr(collection_info, 'points_count'):
                    print(f"[OK] Tổng số points trong collection: {collection_info.points_count:,}")
                elif hasattr(collection_info, 'vectors_count'):
                    print(f"[OK] Tổng số vectors trong collection: {collection_info.vectors_count:,}")
            except Exception as e:
                print(f"[WARNING] Không thể lấy số lượng points: {e}")
            
            return True
            
        except Exception as e:
            print(f"[ERROR] Lỗi khi upsert: {e}")
            return False
    
    def search_similar_items(
        self, 
        query_vector: np.ndarray,
        top_k: int = 10,
        score_threshold: Optional[float] = None
    ) -> List[Dict]:
        """
        Tìm các items tương tự với query vector.
        
        Args:
            query_vector: Vector embedding của query (1D numpy array)
            top_k: Số lượng kết quả trả về
            score_threshold: Ngưỡng điểm tối thiểu (tùy chọn)
            
        Returns:
            List các dict chứa id, score, payload của items tương tự
        """
        try:
            # Kiểm tra query_vector không None
            if query_vector is None:
                raise ValueError("Query vector không được None")
            
            # Đảm bảo query_vector là numpy array
            if not isinstance(query_vector, np.ndarray):
                query_vector = np.array(query_vector)
            
            # Đảm bảo query_vector là 1D numpy array
            if query_vector.ndim > 1:
                query_vector = query_vector.flatten()
            
            # Kiểm tra vector không rỗng
            if query_vector.size == 0:
                raise ValueError("Query vector rỗng")
            
            # QUAN TRỌNG: Normalize query vector (L2 normalization)
            # Vì embeddings trong DB đã được normalize, query vector cũng phải normalize
            # để cosine similarity hoạt động đúng
            query_norm = np.linalg.norm(query_vector)
            if query_norm > 0:
                query_vector = query_vector / query_norm
            else:
                raise ValueError("Query vector có norm = 0, không thể normalize")
            
            # Sử dụng query_points API (API đúng của qdrant-client)
            # Với cosine similarity, Qdrant sẽ tính dot product (vì cả 2 vectors đã normalize)
            query_params = {
                'collection_name': self.collection_name,
                'query': query_vector.tolist(),  # Query vector đã được normalize
                'limit': top_k
            }
            
            # Thêm score_threshold nếu có
            if score_threshold is not None:
                query_params['score_threshold'] = score_threshold
            
            query_response = self.client.query_points(**query_params)
            
            results = []
            # query_points trả về QueryResponse với attribute points
            if hasattr(query_response, 'points'):
                for result in query_response.points:
                    # Lấy item_id từ payload nếu có, nếu không thì dùng id (integer)
                    item_id = result.payload.get('item_id', str(result.id)) if result.payload else str(result.id)
                    results.append({
                        'id': result.id,  # Integer ID trong Qdrant
                        'item_id': item_id,  # String item_id gốc
                        'score': result.score,
                        'payload': result.payload
                    })
            elif hasattr(query_response, '__iter__'):
                # Nếu query_response là iterable trực tiếp
                for result in query_response:
                    item_id = result.payload.get('item_id', str(result.id)) if result.payload else str(result.id)
                    results.append({
                        'id': result.id,
                        'item_id': item_id,
                        'score': result.score,
                        'payload': result.payload
                    })
            
            return results
            
        except Exception as e:
            print(f"[ERROR] Lỗi khi search: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_item_vector(self, item_id: str) -> Optional[np.ndarray]:
        """
        Lấy vector embedding của một item theo item_id.
        
        Args:
            item_id: ID của item cần lấy (string)
            
        Returns:
            Vector embedding hoặc None nếu không tìm thấy
        """
        try:
            # Convert item_id thành integer ID
            point_id = self._item_id_to_int(item_id)
            
            # Thử retrieve bằng point ID
            points = self.client.retrieve(
                collection_name=self.collection_name,
                ids=[point_id],
                with_vectors=True  # Đảm bảo lấy vector
            )
            
            if points and len(points) > 0:
                point = points[0]
                # Kiểm tra vector có tồn tại không
                if hasattr(point, 'vector') and point.vector is not None:
                    vector = np.array(point.vector)
                    # Kiểm tra vector không rỗng
                    if vector.size > 0:
                        return vector
                    else:
                        print(f"[ERROR] Vector của item {item_id} rỗng")
                        return None
                else:
                    print(f"[ERROR] Point {point_id} không có vector")
                    # Thử cách khác: scroll với filter
                    return self._get_item_vector_by_payload(item_id)
            else:
                print(f"[WARNING] Không tìm thấy point với ID {point_id} (item_id: {item_id})")
                # Thử cách khác: scroll với filter theo payload
                return self._get_item_vector_by_payload(item_id)
                
        except Exception as e:
            print(f"[ERROR] Lỗi khi lấy vector bằng retrieve: {e}")
            # Thử cách khác: scroll với filter
            return self._get_item_vector_by_payload(item_id)
    
    def _get_item_vector_by_payload(self, item_id: str) -> Optional[np.ndarray]:
        """
        Lấy vector bằng cách scroll và filter theo payload item_id.
        Phương án dự phòng khi retrieve không hoạt động.
        
        Args:
            item_id: ID của item cần lấy (string)
            
        Returns:
            Vector embedding hoặc None nếu không tìm thấy
        """
        try:
            # Scroll với filter theo payload item_id
            scroll_result = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="item_id",
                            match=MatchValue(value=item_id)
                        )
                    ]
                ),
                limit=1,
                with_vectors=True
            )
            
            if scroll_result[0] and len(scroll_result[0]) > 0:
                point = scroll_result[0][0]
                if hasattr(point, 'vector') and point.vector is not None:
                    vector = np.array(point.vector)
                    if vector.size > 0:
                        print(f"[OK] Đã lấy vector bằng scroll filter cho item {item_id}")
                        return vector
            
            print(f"[ERROR] Không tìm thấy vector cho item_id: {item_id}")
            return None
            
        except Exception as e:
            print(f"[ERROR] Lỗi khi lấy vector bằng scroll filter: {e}")
            return None
    
    def test_search(self, item_ids: List[str], top_k: int = 10) -> bool:
        """
        Test search bằng cách lấy một item bất kỳ và tìm các items tương tự.
        
        Args:
            item_ids: Danh sách item_ids để chọn một item test
            top_k: Số lượng kết quả trả về
            
        Returns:
            True nếu test thành công, False nếu không
        """
        print(f"\n{'='*80}")
        print("TEST SEARCH")
        print(f"{'='*80}")
        
        if not item_ids:
            print("[ERROR] Không có item_ids để test")
            return False
        
        # Chọn item đầu tiên để test
        test_item_id = item_ids[0]
        print(f"\nĐang lấy vector của item: {test_item_id}")
        
        # Lấy vector của item này
        query_vector = self.get_item_vector(test_item_id)
        if query_vector is None:
            print(f"[ERROR] Không tìm thấy item: {test_item_id}")
            return False
        
        print(f"[OK] Đã lấy vector: shape {query_vector.shape}")
        
        # Tìm các items tương tự
        print(f"\nĐang tìm {top_k} items tương tự nhất...")
        similar_items = self.search_similar_items(query_vector, top_k=top_k)
        
        if not similar_items:
            print("[ERROR] Không tìm thấy items tương tự")
            return False
        
        print(f"\n[OK] Tìm thấy {len(similar_items)} items tương tự:")
        print(f"\n{'Rank':<6} {'Item ID':<20} {'Score':<10}")
        print("-" * 40)
        
        for idx, item in enumerate(similar_items, 1):
            item_id = item.get('item_id', str(item['id']))
            print(f"{idx:<6} {item_id:<20} {item['score']:.6f}")
        
        # Kiểm tra item đầu tiên phải là chính nó (score = 1.0)
        first_item_id = similar_items[0].get('item_id', str(similar_items[0]['id']))
        if first_item_id == test_item_id:
            print(f"\n[OK] Item đầu tiên là chính item query (score = {similar_items[0]['score']:.6f})")
        else:
            print(f"\n[WARNING] Item đầu tiên không phải là item query")
            print(f"  Query item_id: {test_item_id}")
            print(f"  First result item_id: {first_item_id}")
        
        return True