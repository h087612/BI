"""
LangChain集成 - 新闻推荐系统API
包含完整的function calling工具定义和记忆功能
"""

import os
import json
import requests
from typing import Optional, Dict, List, Any, Type
from datetime import datetime
from pydantic import BaseModel, Field

from langchain.tools import BaseTool, StructuredTool
from langchain.memory import ConversationSummaryBufferMemory
from langchain.callbacks.manager import CallbackManagerForToolRun
from langchain.schema import SystemMessage, HumanMessage, AIMessage
from langchain.chat_models import ChatOpenAI
from langchain.agents import AgentExecutor, create_openai_functions_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder


# API基础配置
BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080")


# Pydantic模型定义 - 用于工具的输入验证
class NewsListInput(BaseModel):
    category: Optional[str] = Field(None, description="新闻类别筛选")
    topic: Optional[str] = Field(None, description="新闻主题筛选")
    search_text: Optional[str] = Field(None, description="在标题中搜索的文本")
    page: Optional[int] = Field(1, description="页码")
    page_size: Optional[int] = Field(20, description="每页数量")
    sort_order: Optional[str] = Field("desc", description="排序方式 (asc/desc)")


class NewsDetailInput(BaseModel):
    news_id: str = Field(..., description="新闻ID")


class NewsPopularityInput(BaseModel):
    news_id: str = Field(..., description="新闻ID")
    start_date: str = Field(..., description="开始日期，格式: yyyy-MM-dd")
    end_date: str = Field(..., description="结束日期，格式: yyyy-MM-dd")
    interval: Optional[str] = Field("day", description="统计间隔 (day/hour)")


class CategoriesPopularityInput(BaseModel):
    start_date: str = Field(..., description="开始日期，格式: yyyy-MM-dd")
    end_date: str = Field(..., description="结束日期，格式: yyyy-MM-dd")
    categories: Optional[str] = Field(None, description="逗号分隔的类别名称")
    interval: Optional[str] = Field("day", description="统计间隔 (day/hour)")


class UserRecommendationsInput(BaseModel):
    user_id: str = Field(..., description="用户ID")
    timestamp: str = Field(..., description="推荐时间戳，格式: yyyy-MM-dd HH:mm:ss")
    top_n: Optional[int] = Field(20, description="推荐数量")


class NewsRankingInput(BaseModel):
    period: Optional[str] = Field("daily", description="排行周期 (daily/weekly/all)")
    limit: Optional[int] = Field(10, description="返回数量限制")
    date: Optional[str] = Field(None, description="特定日期，格式: yyyyMMdd")


class NewsStatisticsInput(BaseModel):
    category: Optional[str] = Field(None, description="新闻类别筛选")
    topic: Optional[str] = Field(None, description="新闻主题筛选")
    title_length_min: Optional[int] = Field(None, description="标题最小长度")
    title_length_max: Optional[int] = Field(None, description="标题最大长度")
    content_length_min: Optional[int] = Field(None, description="内容最小长度")
    content_length_max: Optional[int] = Field(None, description="内容最大长度")
    start_date: Optional[str] = Field(None, description="开始日期，格式: yyyy-MM-dd")
    end_date: Optional[str] = Field(None, description="结束日期，格式: yyyy-MM-dd")
    like: Optional[bool] = Field(None, description="仅显示点赞的新闻")
    dislike: Optional[bool] = Field(None, description="仅显示点踩的新闻")
    user_id: Optional[str] = Field(None, description="单个用户ID筛选")
    user_ids: Optional[str] = Field(None, description="逗号分隔的多个用户ID")
    page: Optional[int] = Field(1, description="页码")
    page_size: Optional[int] = Field(20, description="每页数量")


class UsersListInput(BaseModel):
    page: Optional[int] = Field(1, description="页码")
    page_size: Optional[int] = Field(20, description="每页数量")


class UserBrowseHistoryInput(BaseModel):
    user_id: str = Field(..., description="用户ID")
    page: Optional[int] = Field(1, description="页码")
    page_size: Optional[int] = Field(20, description="每页数量")


# 自定义工具基类 - 添加错误处理和日志
class NewsAPITool(BaseTool):
    """新闻API工具基类"""
    
    def _handle_error(self, error: Exception) -> str:
        """统一的错误处理"""
        return f"API调用失败: {str(error)}"
    
    def _make_request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """发送HTTP请求的通用方法"""
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(self._handle_error(e))


# 具体工具实现
class GetNewsListTool(NewsAPITool):
    name = "get_news_list"
    description = "获取新闻列表，支持分页、搜索和筛选功能"
    args_schema: Type[BaseModel] = NewsListInput
    
    def _run(self, category: Optional[str] = None, topic: Optional[str] = None,
             search_text: Optional[str] = None, page: int = 1, page_size: int = 20,
             sort_order: str = "desc", run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        params = {
            "page": page,
            "pageSize": page_size,
            "sortOrder": sort_order
        }
        if category:
            params["category"] = category
        if topic:
            params["topic"] = topic
        if search_text:
            params["searchText"] = search_text
        
        result = self._make_request("GET", f"{BASE_URL}/news", params=params)
        return json.dumps(result, ensure_ascii=False, indent=2)


class GetNewsDetailTool(NewsAPITool):
    name = "get_news_detail"
    description = "获取特定新闻的详细信息"
    args_schema: Type[BaseModel] = NewsDetailInput
    
    def _run(self, news_id: str, run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        result = self._make_request("GET", f"{BASE_URL}/news/{news_id}")
        return json.dumps(result, ensure_ascii=False, indent=2)


class GetNewsPopularityTool(NewsAPITool):
    name = "get_news_popularity"
    description = "获取特定新闻的点击量统计数据"
    args_schema: Type[BaseModel] = NewsPopularityInput
    
    def _run(self, news_id: str, start_date: str, end_date: str,
             interval: str = "day", run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "interval": interval
        }
        result = self._make_request("GET", f"{BASE_URL}/news/{news_id}/popularity", params=params)
        return json.dumps(result, ensure_ascii=False, indent=2)


class GetCategoriesPopularityTool(NewsAPITool):
    name = "get_categories_popularity"
    description = "获取新闻类别的点击量统计数据"
    args_schema: Type[BaseModel] = CategoriesPopularityInput
    
    def _run(self, start_date: str, end_date: str, categories: Optional[str] = None,
             interval: str = "day", run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "interval": interval
        }
        if categories:
            params["categories"] = categories
        
        result = self._make_request("GET", f"{BASE_URL}/news/categories/popularity", params=params)
        return json.dumps(result, ensure_ascii=False, indent=2)


class GetUserRecommendationsTool(NewsAPITool):
    name = "get_user_recommendations"
    description = "获取用户的个性化新闻推荐"
    args_schema: Type[BaseModel] = UserRecommendationsInput
    
    def _run(self, user_id: str, timestamp: str, top_n: int = 20,
             run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        params = {
            "timestamp": timestamp,
            "topN": top_n
        }
        result = self._make_request("GET", f"{BASE_URL}/news/users/{user_id}/recommendations", params=params)
        return json.dumps(result, ensure_ascii=False, indent=2)


class GetNewsRankingTool(NewsAPITool):
    name = "get_news_ranking"
    description = "获取新闻热度排行榜"
    args_schema: Type[BaseModel] = NewsRankingInput
    
    def _run(self, period: str = "daily", limit: int = 10, date: Optional[str] = None,
             run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        params = {
            "period": period,
            "limit": limit
        }
        if date:
            params["date"] = date
        
        result = self._make_request("GET", f"{BASE_URL}/news/recommend/rank", params=params)
        return json.dumps(result, ensure_ascii=False, indent=2)


class GetNewsStatisticsTool(NewsAPITool):
    name = "get_news_statistics"
    description = "获取高级新闻点击统计，支持多种筛选条件"
    args_schema: Type[BaseModel] = NewsStatisticsInput
    
    def _run(self, category: Optional[str] = None, topic: Optional[str] = None,
             title_length_min: Optional[int] = None, title_length_max: Optional[int] = None,
             content_length_min: Optional[int] = None, content_length_max: Optional[int] = None,
             start_date: Optional[str] = None, end_date: Optional[str] = None,
             like: Optional[bool] = None, dislike: Optional[bool] = None,
             user_id: Optional[str] = None, user_ids: Optional[str] = None,
             page: int = 1, page_size: int = 20,
             run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        params = {"page": page, "pageSize": page_size}
        
        # 添加所有可选参数
        optional_params = {
            "category": category, "topic": topic,
            "titleLengthMin": title_length_min, "titleLengthMax": title_length_max,
            "contentLengthMin": content_length_min, "contentLengthMax": content_length_max,
            "startDate": start_date, "endDate": end_date,
            "like": like, "dislike": dislike,
            "userId": user_id, "userIds": user_ids
        }
        
        for key, value in optional_params.items():
            if value is not None:
                params[key] = value
        
        result = self._make_request("GET", f"{BASE_URL}/news/statistics", params=params)
        return json.dumps(result, ensure_ascii=False, indent=2)


class GetUsersListTool(NewsAPITool):
    name = "get_users_list"
    description = "获取用户列表"
    args_schema: Type[BaseModel] = UsersListInput
    
    def _run(self, page: int = 1, page_size: int = 20,
             run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        params = {
            "page": page,
            "pageSize": page_size
        }
        result = self._make_request("GET", f"{BASE_URL}/users", params=params)
        return json.dumps(result, ensure_ascii=False, indent=2)


class GetUsersCountTool(NewsAPITool):
    name = "get_users_count"
    description = "获取用户总数"
    
    def _run(self, run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        result = self._make_request("GET", f"{BASE_URL}/users/count")
        return json.dumps(result, ensure_ascii=False, indent=2)


class GetUserBrowseHistoryTool(NewsAPITool):
    name = "get_user_browse_history"
    description = "获取用户浏览历史（兴趣分类）"
    args_schema: Type[BaseModel] = UserBrowseHistoryInput
    
    def _run(self, user_id: str, page: int = 1, page_size: int = 20,
             run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
        params = {
            "page": page,
            "pageSize": page_size
        }
        result = self._make_request("GET", f"{BASE_URL}/users/{user_id}/browse-history", params=params)
        return json.dumps(result, ensure_ascii=False, indent=2)


# 创建所有工具实例
def get_all_tools() -> List[BaseTool]:
    """获取所有可用的工具"""
    return [
        GetNewsListTool(),
        GetNewsDetailTool(),
        GetNewsPopularityTool(),
        GetCategoriesPopularityTool(),
        GetUserRecommendationsTool(),
        GetNewsRankingTool(),
        GetNewsStatisticsTool(),
        GetUsersListTool(),
        GetUsersCountTool(),
        GetUserBrowseHistoryTool()
    ]


# 创建带记忆的Agent
def create_news_agent(llm: Optional[ChatOpenAI] = None, memory_key: str = "chat_history",
                      max_token_limit: int = 2000) -> AgentExecutor:
    """
    创建一个带记忆功能的新闻推荐系统Agent
    
    Args:
        llm: 语言模型实例，如果为None则使用默认配置
        memory_key: 记忆存储的键名
        max_token_limit: 记忆缓冲区的最大token数
    
    Returns:
        配置好的AgentExecutor
    """
    
    # 初始化LLM
    if llm is None:
        llm = ChatOpenAI(
            temperature=0,
            model_name="gpt-4",
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
    
    # 创建记忆组件
    memory = ConversationSummaryBufferMemory(
        llm=llm,
        max_token_limit=max_token_limit,
        memory_key=memory_key,
        return_messages=True
    )
    
    # 系统提示词
    system_prompt = """你是一个新闻推荐系统的智能助手。你可以帮助用户：

1. 搜索和浏览新闻
2. 查看新闻详情和热度统计
3. 获取个性化推荐
4. 分析用户行为和兴趣
5. 查看各类别新闻的流行趋势

记住以下重要信息：
- 用户兴趣评分计算公式：点击*1 + 停留时间*α系数（α为停留时间权重系数）
- 系统会根据用户的点击和停留时间动态调整兴趣评分
- 用户浏览历史保存在Redis中，包含分类评分信息

在回答时，请：
- 根据上下文理解用户意图
- 记住之前的对话内容
- 在适当时候主动推荐相关功能
- 用简洁清晰的语言回复
"""
    
    # 创建提示模板
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        MessagesPlaceholder(variable_name=memory_key),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad")
    ])
    
    # 获取所有工具
    tools = get_all_tools()
    
    # 创建Agent
    agent = create_openai_functions_agent(llm, tools, prompt)
    
    # 创建AgentExecutor
    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        memory=memory,
        verbose=True,
        return_intermediate_steps=True,
        max_iterations=5,
        early_stopping_method="generate"
    )
    
    return agent_executor


# 辅助函数 - 格式化输出
def format_agent_response(response: Dict[str, Any]) -> str:
    """格式化Agent的响应结果"""
    output = response.get("output", "")
    
    # 如果有中间步骤，可以选择性地显示
    if "intermediate_steps" in response and response["intermediate_steps"]:
        output += "\n\n调用的工具："
        for action, observation in response["intermediate_steps"]:
            output += f"\n- {action.tool}: {action.tool_input}"
    
    return output


# 使用示例
if __name__ == "__main__":
    # 创建Agent
    agent = create_news_agent()
    
    # 示例对话
    conversations = [
        "你好，我想了解一下今天的热门新闻",
        "用户user123最近在看什么类型的新闻？",
        "给我推荐一些科技类的新闻",
        "刚才提到的用户，他的兴趣评分是怎么计算的？"
    ]
    
    for user_input in conversations:
        print(f"\n用户: {user_input}")
        
        try:
            response = agent.invoke({"input": user_input})
            formatted_response = format_agent_response(response)
            print(f"助手: {formatted_response}")
        except Exception as e:
            print(f"错误: {str(e)}")