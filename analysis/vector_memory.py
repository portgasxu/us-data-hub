#!/usr/bin/env python3
"""
US Data Hub — 语义向量记忆 (v6.0)
=================================
替代 BM25，支持语义相似性匹配。
全 TradingAgents 的 5 个 Memory 实例共享同一个 embedding model。

用法:
  from analysis.vector_memory import VectorMemory
  
  mem = VectorMemory("risk", "/path/to/lessons")
  mem.load()
  results = mem.search("earnings miss warning")
  # 能匹配到"盈利预警"等语义相似但关键词不同的文本
"""

import json
import os
import numpy as np
from typing import List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class VectorMemory:
    """基于向量检索的记忆系统"""
    
    _model = None  # 全局共享单例
    
    @classmethod
    def get_model(cls):
        """懒加载 embedding model（全局单例）"""
        if cls._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                cls._model = SentenceTransformer('BAAI/bge-small-en-v1.5')
                logger.info("VectorMemory: embedding model loaded")
            except ImportError:
                logger.warning("VectorMemory: sentence-transformers not installed, falling back to BM25")
                cls._model = "fallback"
        return cls._model
    
    def __init__(self, name: str, lessons_dir: str = None):
        self.name = name
        if lessons_dir is None:
            lessons_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "tradingagents", "tradingagents", "agents", "utils", "lessons"
            )
        self.lessons_dir = os.path.join(lessons_dir, f"{name}_lessons")
        self.situations: List[str] = []
        self.recommendations: List[str] = []
        self.vectors: Optional[np.ndarray] = None
        self.use_vector = True
    
    def load(self):
        """从磁盘加载记忆"""
        if not os.path.exists(self.lessons_dir):
            logger.debug(f"VectorMemory[{self.name}]: lessons dir not found at {self.lessons_dir}")
            return
        
        for file in os.listdir(self.lessons_dir):
            if file.endswith(".json"):
                try:
                    with open(os.path.join(self.lessons_dir, file)) as f:
                        data = json.load(f)
                        self.situations.append(data.get("situation", ""))
                        self.recommendations.append(data.get("recommendation", ""))
                except Exception as e:
                    logger.debug(f"Failed to load {file}: {e}")
        
        if self.situations:
            model = self.get_model()
            if model == "fallback":
                self.use_vector = False
                logger.info(f"VectorMemory[{self.name}]: loaded {len(self.situations)} memories (BM25 fallback)")
                return
            
            try:
                self.vectors = model.encode(self.situations, normalize_embeddings=True)
                logger.info(f"VectorMemory[{self.name}]: loaded {len(self.situations)} memories with vectors")
            except Exception as e:
                logger.warning(f"VectorMemory[{self.name}]: vector encoding failed, using BM25 fallback: {e}")
                self.use_vector = False
    
    def search(self, query: str, n: int = 2) -> List[dict]:
        """
        语义搜索。
        如果向量模型可用，使用余弦相似度；否则 fallback 到关键词匹配。
        """
        if not self.situations:
            return []
        
        # 向量检索
        if self.use_vector and self.vectors is not None:
            try:
                model = self.get_model()
                query_vec = model.encode([query], normalize_embeddings=True)
                scores = np.dot(self.vectors, query_vec.T).flatten()
                top_idx = scores.argsort()[::-1][:n]
                
                results = []
                for idx in top_idx:
                    if scores[idx] > 0.1:  # 最低相似度阈值
                        results.append({
                            "matched_situation": self.situations[idx],
                            "recommendation": self.recommendations[idx],
                            "similarity": float(scores[idx]),
                        })
                return results
            except Exception as e:
                logger.warning(f"Vector search failed, falling back to keyword: {e}")
        
        # Fallback: 关键词匹配
        query_lower = query.lower()
        results = []
        for i, sit in enumerate(self.situations):
            sit_lower = sit.lower()
            # 简单匹配：查询词出现在记忆中
            if any(w in sit_lower for w in query_lower.split()):
                results.append({
                    "matched_situation": sit,
                    "recommendation": self.recommendations[i],
                    "similarity": 0.3,  # 低相似度标记为 fallback
                })
                if len(results) >= n:
                    break
        return results
    
    def add(self, situation: str, recommendation: str):
        """新增记忆"""
        self.situations.append(situation)
        self.recommendations.append(recommendation)
        
        # 更新向量
        if self.use_vector:
            try:
                model = self.get_model()
                if model != "fallback":
                    self.vectors = model.encode(self.situations, normalize_embeddings=True)
            except Exception:
                pass
        
        # 持久化
        os.makedirs(self.lessons_dir, exist_ok=True)
        idx = len(self.situations) - 1
        path = os.path.join(self.lessons_dir, f"memory_{idx}.json")
        with open(path, "w") as f:
            json.dump({
                "situation": situation,
                "recommendation": recommendation,
            }, f, ensure_ascii=False)
