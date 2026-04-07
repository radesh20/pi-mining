import os
from pathlib import Path
from dotenv import load_dotenv


def _load_env_files() -> None:
    """
    Load .env files deterministically so settings work no matter where uvicorn is started.
    Priority order:
    1) Existing process environment variables (never overridden)
    2) backend/.env
    3) current working directory .env (if present and different)
    """
    backend_dir = Path(__file__).resolve().parent.parent
    candidates = [backend_dir / ".env"]
    cwd_env = Path.cwd() / ".env"
    if cwd_env.resolve() != (backend_dir / ".env").resolve():
        candidates.append(cwd_env)
    for env_path in candidates:
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=False)


_load_env_files()


class Settings:
    # Azure OpenAI
    AZURE_OPENAI_API_KEY: str = os.getenv("AZURE_OPENAI_API_KEY", "")
    AZURE_OPENAI_ENDPOINT: str = os.getenv("AZURE_OPENAI_ENDPOINT", "")
    AZURE_OPENAI_DEPLOYMENT: str = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
    AZURE_OPENAI_API_VERSION: str = os.getenv(
        "AZURE_OPENAI_API_VERSION", "2024-02-15-preview"
    )

    # Celonis
    CELONIS_BASE_URL: str = os.getenv("CELONIS_BASE_URL", os.getenv("CELONIS_URL", ""))
    CELONIS_API_TOKEN: str = os.getenv("CELONIS_API_TOKEN", os.getenv("CELONIS_API_KEY", ""))
    CELONIS_KEY_TYPE: str = os.getenv("CELONIS_KEY_TYPE", "USER_KEY")
    CELONIS_DATA_POOL_ID: str = os.getenv("CELONIS_DATA_POOL_ID", "")
    CELONIS_DATA_MODEL_ID: str = os.getenv("CELONIS_DATA_MODEL_ID", "")

    # Main event/activity table
    ACTIVITY_TABLE: str = os.getenv(
        "ACTIVITY_TABLE",
        "t_o_custom_AccountingDocumentHeader,t_o_custom_VimHeader",
    ).split(",")[0].strip()
    ACTIVITY_TABLES: list = [
        t.strip()
        for t in os.getenv(
            "ACTIVITY_TABLE",
            "t_o_custom_AccountingDocumentHeader,t_o_custom_VimHeader",
        ).split(",")
    ]
    CASE_COLUMN: str = os.getenv("CASE_COLUMN", "CASEKEY")
    ACTIVITY_COLUMN: str = os.getenv("ACTIVITY_COLUMN", "ACTIVITYEN")
    TIMESTAMP_COLUMN: str = os.getenv("TIMESTAMP_COLUMN", "EVENTTIME")
    RESOURCE_COLUMN: str = os.getenv("RESOURCE_COLUMN", "USERNAME")
    RESOURCE_ROLE_COLUMN: str = os.getenv("RESOURCE_ROLE_COLUMN", "USERTYPE")

    # Case/object table
    CASE_TABLE: str = os.getenv("CASE_TABLE", "t_o_custom_PurchasingDocumentHeader")
    CASE_TABLE_ID_COLUMN: str = os.getenv("CASE_TABLE_ID_COLUMN", "ID")
    CASE_TABLE_DOC_COLUMN: str = os.getenv("CASE_TABLE_DOC_COLUMN", "EBELN")
    VENDOR_ID_COLUMN: str = os.getenv("VENDOR_ID_COLUMN", "LIFNR")
    PAYMENT_TERMS_COLUMN: str = os.getenv("PAYMENT_TERMS_COLUMN", "ZTERM")
    CURRENCY_COLUMN: str = os.getenv("CURRENCY_COLUMN", "WAERS")
    AMOUNT_COLUMN: str = os.getenv("AMOUNT_COLUMN", "NETWR")

    # Celonis export controls
    CELONIS_EXPORT_BATCH_SIZE: int = int(os.getenv("CELONIS_EXPORT_BATCH_SIZE", "5000"))
    CELONIS_EXPORT_MAX_ROWS: int = int(os.getenv("CELONIS_EXPORT_MAX_ROWS", "0"))
    CELONIS_EVENT_LOG_MAX_ROWS: int = int(os.getenv("CELONIS_EVENT_LOG_MAX_ROWS", "200000"))
    CELONIS_CONNECT_TIMEOUT_SECONDS: int = int(os.getenv("CELONIS_CONNECT_TIMEOUT_SECONDS", "20"))
    CELONIS_PQL_TIMEOUT_SECONDS: int = int(os.getenv("CELONIS_PQL_TIMEOUT_SECONDS", "90"))
    CELONIS_DISCOVERY_CACHE_TTL_SECONDS: int = int(os.getenv("CELONIS_DISCOVERY_CACHE_TTL_SECONDS", "900"))
    CELONIS_DISCOVERY_MAX_TABLES: int = int(os.getenv("CELONIS_DISCOVERY_MAX_TABLES", "80"))
    WCM_CONTEXT_MODE: str = os.getenv("WCM_CONTEXT_MODE", "full").lower()  # full | legacy
    WCM_ENABLE_GROUPED_EXTRACT: bool = os.getenv("WCM_ENABLE_GROUPED_EXTRACT", "false").lower() == "true"
    WCM_GROUPED_INCLUDE_ROWS: bool = os.getenv("WCM_GROUPED_INCLUDE_ROWS", "false").lower() == "true"
    WCM_GROUPED_MAX_ROWS_PER_TABLE: int = int(os.getenv("WCM_GROUPED_MAX_ROWS_PER_TABLE", "10000"))
    WCM_GROUPED_SAMPLE_MAX_ROWS: int = int(os.getenv("WCM_GROUPED_SAMPLE_MAX_ROWS", "200"))
    WCM_GROUPED_MAX_TABLES: int = int(os.getenv("WCM_GROUPED_MAX_TABLES", "20"))
    WCM_GROUPED_INCLUDE_EVENT_TABLES: bool = os.getenv(
        "WCM_GROUPED_INCLUDE_EVENT_TABLES", "true"
    ).lower() == "true"
    WCM_GROUPED_TABLE_PREFIXES: str = os.getenv(
        "WCM_GROUPED_TABLE_PREFIXES", "t_o_custom_,t_e_custom_"
    )
    WCM_GROUPED_TABLE_ALLOWLIST: str = os.getenv(
        "WCM_GROUPED_TABLE_ALLOWLIST",
        (
            "t_o_custom_VimHeader,t_o_custom_PurchasingDocumentHeader,"
            "t_o_custom_AccountingDocumentSegment,t_o_custom_DocumentItemIncomingInvoice,"
            "t_o_custom_PurchasingDocumentItem,t_o_custom_AccountingDocumentHeader,"
            "t_o_custom_VendorMaster,t_e_custom_VimHeader"
        ),
    )
    WCM_OLAP_MAX_ROWS: int = int(os.getenv("WCM_OLAP_MAX_ROWS", "200000"))

    # Optional explicit OLAP source overrides for environments with non-standard naming
    WCM_OLAP_SOURCE_TABLE: str = os.getenv("WCM_OLAP_SOURCE_TABLE", "")
    WCM_OLAP_COL_COMPANY_CODE: str = os.getenv("WCM_OLAP_COL_COMPANY_CODE", "")
    WCM_OLAP_COL_SUPPLIER_TYPE: str = os.getenv("WCM_OLAP_COL_SUPPLIER_TYPE", "")
    WCM_OLAP_COL_VENDOR_ID: str = os.getenv("WCM_OLAP_COL_VENDOR_ID", "")
    WCM_OLAP_COL_SUPPLIER_NAME: str = os.getenv("WCM_OLAP_COL_SUPPLIER_NAME", "")
    WCM_OLAP_COL_INVOICE_NUMBER: str = os.getenv("WCM_OLAP_COL_INVOICE_NUMBER", "")
    WCM_OLAP_COL_LINE_ITEM: str = os.getenv("WCM_OLAP_COL_LINE_ITEM", "")
    WCM_OLAP_COL_INVOICE_VALUE_USD: str = os.getenv("WCM_OLAP_COL_INVOICE_VALUE_USD", "")
    WCM_OLAP_COL_CURRENCY: str = os.getenv("WCM_OLAP_COL_CURRENCY", "")
    WCM_OLAP_COL_CONVERTED_VALUE_USD: str = os.getenv("WCM_OLAP_COL_CONVERTED_VALUE_USD", "")
    WCM_OLAP_COL_FISCAL_YEAR: str = os.getenv("WCM_OLAP_COL_FISCAL_YEAR", "")
    WCM_OLAP_COL_CLEARING_DOC: str = os.getenv("WCM_OLAP_COL_CLEARING_DOC", "")
    WCM_OLAP_COL_PAYMENT_STATUS: str = os.getenv("WCM_OLAP_COL_PAYMENT_STATUS", "")
    WCM_OLAP_COL_INVOICE_PT: str = os.getenv("WCM_OLAP_COL_INVOICE_PT", "")
    WCM_OLAP_COL_PO_PT: str = os.getenv("WCM_OLAP_COL_PO_PT", "")
    WCM_OLAP_COL_VENDOR_PT: str = os.getenv("WCM_OLAP_COL_VENDOR_PT", "")
    WCM_OLAP_COL_RECOMMENDATION: str = os.getenv("WCM_OLAP_COL_RECOMMENDATION", "")
    WCM_OLAP_COL_DUE_DATE: str = os.getenv("WCM_OLAP_COL_DUE_DATE", "")
    WCM_OLAP_COL_BASELINE_DATE: str = os.getenv("WCM_OLAP_COL_BASELINE_DATE", "")
    WCM_OLAP_COL_POSTING_DATE: str = os.getenv("WCM_OLAP_COL_POSTING_DATE", "")
    WCM_OLAP_COL_CLEARED_DATE: str = os.getenv("WCM_OLAP_COL_CLEARED_DATE", "")

    # Cache controls
    CACHE_TTL_SECONDS: int = int(os.getenv("CACHE_TTL_SECONDS", "1800"))
    CACHE_AUTO_REFRESH_SECONDS: int = int(os.getenv("CACHE_AUTO_REFRESH_SECONDS", "900"))
    CACHE_AUTO_REFRESH_POLICY: str = os.getenv("CACHE_AUTO_REFRESH_POLICY", "stale_only").lower()
    CACHE_ENABLE_STARTUP_WARMUP: bool = os.getenv("CACHE_ENABLE_STARTUP_WARMUP", "true").lower() == "true"
    CACHE_STALE_WHILE_REFRESH: bool = os.getenv("CACHE_STALE_WHILE_REFRESH", "true").lower() == "true"
    CACHE_REFRESH_WAIT_SECONDS: int = int(os.getenv("CACHE_REFRESH_WAIT_SECONDS", "30"))
    CACHE_INITIAL_LOAD_WAIT_SECONDS: int = int(os.getenv("CACHE_INITIAL_LOAD_WAIT_SECONDS", "180"))
    CACHE_REFRESH_HARD_TIMEOUT_SECONDS: int = int(os.getenv("CACHE_REFRESH_HARD_TIMEOUT_SECONDS", "180"))

    # LLM response cache
    LLM_CACHE_ENABLED: bool = os.getenv("LLM_CACHE_ENABLED", "true").lower() == "true"
    LLM_CACHE_TTL_SECONDS: int = int(os.getenv("LLM_CACHE_TTL_SECONDS", "900"))

    # Microsoft Teams
    TEAMS_WEBHOOK_URL: str = os.getenv(
        "TEAMS_WEBHOOK_URL",
        os.getenv("POWER_AUTOMATE_WEBHOOK_URL", ""),
    )


settings = Settings()
