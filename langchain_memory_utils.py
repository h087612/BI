"""
LangChain记忆管理工具
提供多种记忆类型和持久化选项
"""

import os
import json
import pickle
from typing import Dict, List, Any, Optional
from datetime import datetime
import redis
from pathlib import Path

from langchain.memory import (
    ConversationBufferMemory,
    ConversationSummaryMemory,
    ConversationSummaryBufferMemory,
    ConversationEntityMemory,
    ConversationKGMemory
)
from langchain.memory.chat_message_histories import (
    ChatMessageHistory,
    RedisChatMessageHistory,
    FileChatMessageHistory
)
from langchain.schema import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain.chat_models import ChatOpenAI


class MemoryManager:
    """统一的记忆管理器"""
    
    def __init__(self, redis_url: Optional[str] = None, storage_path: str = "./memory_storage"):
        """
        初始化记忆管理器
        
        Args:
            redis_url: Redis连接URL，格式: redis://localhost:6379/0
            storage_path: 本地文件存储路径
        """
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(exist_ok=True)
        
        # Redis连接（可选）
        try:
            self.redis_client = redis.from_url(self.redis_url) if redis_url else None
        except:
            self.redis_client = None
            print("警告: Redis连接失败，将使用本地文件存储")
    
    def create_buffer_memory(self, session_id: str, k: int = 10) -> ConversationBufferMemory:
        """
        创建缓冲区记忆
        保存最近k轮对话
        """
        chat_history = self._get_chat_history(session_id)
        return ConversationBufferMemory(
            chat_memory=chat_history,
            return_messages=True,
            memory_key="chat_history",
            k=k
        )
    
    def create_summary_memory(self, session_id: str, llm: ChatOpenAI) -> ConversationSummaryMemory:
        """
        创建摘要记忆
        自动总结历史对话
        """
        chat_history = self._get_chat_history(session_id)
        return ConversationSummaryMemory(
            llm=llm,
            chat_memory=chat_history,
            return_messages=True,
            memory_key="chat_history"
        )
    
    def create_summary_buffer_memory(self, session_id: str, llm: ChatOpenAI, 
                                   max_token_limit: int = 2000) -> ConversationSummaryBufferMemory:
        """
        创建摘要缓冲区记忆
        在达到token限制时自动总结
        """
        chat_history = self._get_chat_history(session_id)
        return ConversationSummaryBufferMemory(
            llm=llm,
            chat_memory=chat_history,
            max_token_limit=max_token_limit,
            return_messages=True,
            memory_key="chat_history"
        )
    
    def create_entity_memory(self, session_id: str, llm: ChatOpenAI) -> ConversationEntityMemory:
        """
        创建实体记忆
        提取和记住对话中的实体信息
        """
        chat_history = self._get_chat_history(session_id)
        
        # 加载已保存的实体
        entity_store = self._load_entities(session_id)
        
        memory = ConversationEntityMemory(
            llm=llm,
            chat_memory=chat_history,
            return_messages=True,
            memory_key="chat_history"
        )
        
        # 恢复实体存储
        if entity_store:
            memory.entity_store.store = entity_store
        
        return memory
    
    def create_kg_memory(self, session_id: str, llm: ChatOpenAI) -> ConversationKGMemory:
        """
        创建知识图谱记忆
        构建对话的知识图谱
        """
        chat_history = self._get_chat_history(session_id)
        
        # 加载已保存的知识图谱
        kg_data = self._load_knowledge_graph(session_id)
        
        memory = ConversationKGMemory(
            llm=llm,
            chat_memory=chat_history,
            return_messages=True,
            memory_key="chat_history"
        )
        
        # 恢复知识图谱
        if kg_data:
            memory.kg = kg_data
        
        return memory
    
    def _get_chat_history(self, session_id: str):
        """获取聊天历史"""
        if self.redis_client:
            return RedisChatMessageHistory(
                session_id=session_id,
                url=self.redis_url,
                key_prefix="news_chat:"
            )
        else:
            return FileChatMessageHistory(
                file_path=str(self.storage_path / f"{session_id}.json")
            )
    
    def save_entity_memory(self, memory: ConversationEntityMemory, session_id: str):
        """保存实体记忆"""
        entity_data = memory.entity_store.store
        self._save_entities(session_id, entity_data)
    
    def save_kg_memory(self, memory: ConversationKGMemory, session_id: str):
        """保存知识图谱记忆"""
        kg_data = memory.kg
        self._save_knowledge_graph(session_id, kg_data)
    
    def _save_entities(self, session_id: str, entities: Dict[str, Any]):
        """保存实体数据"""
        if self.redis_client:
            self.redis_client.hset(
                f"news_entities:{session_id}",
                "data",
                json.dumps(entities, ensure_ascii=False)
            )
            self.redis_client.expire(f"news_entities:{session_id}", 86400 * 7)  # 7天过期
        else:
            entity_file = self.storage_path / f"{session_id}_entities.json"
            with open(entity_file, 'w', encoding='utf-8') as f:
                json.dump(entities, f, ensure_ascii=False, indent=2)
    
    def _load_entities(self, session_id: str) -> Optional[Dict[str, Any]]:
        """加载实体数据"""
        if self.redis_client:
            data = self.redis_client.hget(f"news_entities:{session_id}", "data")
            return json.loads(data) if data else None
        else:
            entity_file = self.storage_path / f"{session_id}_entities.json"
            if entity_file.exists():
                with open(entity_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        return None
    
    def _save_knowledge_graph(self, session_id: str, kg_data: Any):
        """保存知识图谱数据"""
        if self.redis_client:
            self.redis_client.hset(
                f"news_kg:{session_id}",
                "data",
                pickle.dumps(kg_data)
            )
            self.redis_client.expire(f"news_kg:{session_id}", 86400 * 7)  # 7天过期
        else:
            kg_file = self.storage_path / f"{session_id}_kg.pkl"
            with open(kg_file, 'wb') as f:
                pickle.dump(kg_data, f)
    
    def _load_knowledge_graph(self, session_id: str) -> Any:
        """加载知识图谱数据"""
        if self.redis_client:
            data = self.redis_client.hget(f"news_kg:{session_id}", "data")
            return pickle.loads(data) if data else None
        else:
            kg_file = self.storage_path / f"{session_id}_kg.pkl"
            if kg_file.exists():
                with open(kg_file, 'rb') as f:
                    return pickle.load(f)
        return None
    
    def get_session_summary(self, session_id: str) -> Dict[str, Any]:
        """获取会话摘要信息"""
        chat_history = self._get_chat_history(session_id)
        messages = chat_history.messages
        
        summary = {
            "session_id": session_id,
            "message_count": len(messages),
            "start_time": None,
            "last_message_time": None,
            "topics": [],
            "entities": self._load_entities(session_id) or {}
        }
        
        if messages:
            # 假设消息有时间戳属性
            summary["start_time"] = str(datetime.now())  # 实际应从消息中获取
            summary["last_message_time"] = str(datetime.now())
            
            # 提取话题（简单实现）
            human_messages = [m for m in messages if isinstance(m, HumanMessage)]
            summary["topics"] = list(set([
                msg.content.split()[0] for msg in human_messages[:5]
                if msg.content
            ]))
        
        return summary
    
    def clear_session(self, session_id: str):
        """清除会话数据"""
        # 清除聊天历史
        chat_history = self._get_chat_history(session_id)
        chat_history.clear()
        
        # 清除实体和知识图谱
        if self.redis_client:
            self.redis_client.delete(f"news_entities:{session_id}")
            self.redis_client.delete(f"news_kg:{session_id}")
        else:
            entity_file = self.storage_path / f"{session_id}_entities.json"
            kg_file = self.storage_path / f"{session_id}_kg.pkl"
            entity_file.unlink(missing_ok=True)
            kg_file.unlink(missing_ok=True)


# 上下文管理器
class ConversationContext:
    """对话上下文管理器"""
    
    def __init__(self, memory_manager: MemoryManager, llm: ChatOpenAI):
        self.memory_manager = memory_manager
        self.llm = llm
        self.active_sessions = {}
    
    def get_or_create_session(self, session_id: str, memory_type: str = "summary_buffer") -> Any:
        """获取或创建会话"""
        if session_id not in self.active_sessions:
            if memory_type == "buffer":
                memory = self.memory_manager.create_buffer_memory(session_id)
            elif memory_type == "summary":
                memory = self.memory_manager.create_summary_memory(session_id, self.llm)
            elif memory_type == "summary_buffer":
                memory = self.memory_manager.create_summary_buffer_memory(session_id, self.llm)
            elif memory_type == "entity":
                memory = self.memory_manager.create_entity_memory(session_id, self.llm)
            elif memory_type == "kg":
                memory = self.memory_manager.create_kg_memory(session_id, self.llm)
            else:
                raise ValueError(f"Unknown memory type: {memory_type}")
            
            self.active_sessions[session_id] = {
                "memory": memory,
                "memory_type": memory_type,
                "created_at": datetime.now()
            }
        
        return self.active_sessions[session_id]["memory"]
    
    def save_session(self, session_id: str):
        """保存会话状态"""
        if session_id in self.active_sessions:
            session = self.active_sessions[session_id]
            memory = session["memory"]
            memory_type = session["memory_type"]
            
            if memory_type == "entity":
                self.memory_manager.save_entity_memory(memory, session_id)
            elif memory_type == "kg":
                self.memory_manager.save_kg_memory(memory, session_id)
    
    def close_session(self, session_id: str):
        """关闭会话"""
        if session_id in self.active_sessions:
            self.save_session(session_id)
            del self.active_sessions[session_id]
    
    def get_all_sessions(self) -> Dict[str, Dict[str, Any]]:
        """获取所有活动会话信息"""
        return {
            session_id: {
                "memory_type": info["memory_type"],
                "created_at": info["created_at"].isoformat(),
                "summary": self.memory_manager.get_session_summary(session_id)
            }
            for session_id, info in self.active_sessions.items()
        }


# 使用示例
if __name__ == "__main__":
    # 初始化
    llm = ChatOpenAI(temperature=0, model_name="gpt-4")
    memory_manager = MemoryManager()
    context_manager = ConversationContext(memory_manager, llm)
    
    # 创建会话
    session_id = "user123_session_001"
    memory = context_manager.get_or_create_session(session_id, "entity")
    
    # 模拟对话
    memory.chat_memory.add_user_message("我想看科技类新闻")
    memory.chat_memory.add_ai_message("好的，为您推荐最新的科技新闻...")
    memory.chat_memory.add_user_message("最近AI领域有什么新进展吗？")
    memory.chat_memory.add_ai_message("AI领域最近有以下重要进展...")
    
    # 保存会话
    context_manager.save_session(session_id)
    
    # 获取会话摘要
    summary = memory_manager.get_session_summary(session_id)
    print("会话摘要:", json.dumps(summary, ensure_ascii=False, indent=2))