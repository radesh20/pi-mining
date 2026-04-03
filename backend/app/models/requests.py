from pydantic import BaseModel
from typing import Any, List, Optional


class InvoiceRequest(BaseModel):
    invoice_id: str
    vendor_id: str = ""
    vendor_name: str = ""
    vendor_type: str = "existing"
    po_reference: str = ""
    invoice_amount: float = 0.0
    po_amount: float = 0.0
    invoice_tax_code: str = ""
    po_tax_code: str = ""
    currency: str = "USD"
    invoice_date: str = ""
    payment_due_date: str = ""
    goods_receipt_recorded: bool = True
    gr_date: str = ""


class TableConfig(BaseModel):
    activity_table: str = ""
    case_column: str = ""
    activity_column: str = ""
    timestamp_column: str = ""
    resource_column: str = ""
    resource_role_column: str = ""
    case_table: str = ""


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    case_id: Optional[str] = None
    vendor_id: Optional[str] = None
    conversation_history: List[ChatMessage] = []


class SQLChatRequest(BaseModel):
    message: str
    table_name: Optional[str] = None
    dialect: str = "PostgreSQL"
    case_id: Optional[str] = None
    vendor_id: Optional[str] = None
    conversation_history: List[ChatMessage] = []
