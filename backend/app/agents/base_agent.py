from abc import ABC, abstractmethod
from typing import Dict, List
from app.services.azure_openai_service import AzureOpenAIService


class BaseAgent(ABC):
    def __init__(
        self,
        agent_id: str,
        agent_name: str,
        llm: AzureOpenAIService,
        process_context: Dict,
        guardrails: List[str] = None,
    ):
        self.agent_id = agent_id
        self.agent_name = agent_name
        self.llm = llm
        self.process_context = process_context
        self.guardrails = guardrails or []

    @abstractmethod
    def process(self, input_data: Dict) -> Dict:
        pass