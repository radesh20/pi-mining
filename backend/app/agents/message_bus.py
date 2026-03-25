import uuid
from datetime import datetime
from typing import Dict, List
from dataclasses import dataclass, field


@dataclass
class AgentMessage:
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sender: str = ""
    receiver: str = ""
    message_type: str = ""
    payload: Dict = field(default_factory=dict)
    process_context: Dict = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    urgency: str = "NORMAL"


class MessageBus:
    def __init__(self):
        self.messages: List[AgentMessage] = []

    def send(self, message: AgentMessage):
        self.messages.append(message)
        return message

    def get_messages_for(self, agent_id: str) -> List[AgentMessage]:
        return [m for m in self.messages if m.receiver == agent_id]

    def get_all(self) -> List[Dict]:
        return [
            {
                "id": m.message_id,
                "sender": m.sender,
                "receiver": m.receiver,
                "type": m.message_type,
                "urgency": m.urgency,
                "timestamp": m.timestamp,
                "payload": m.payload,
            }
            for m in self.messages
        ]