import logging
import uuid
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Literal, Optional, Union

from pycelonis_core.base.base_model import (PyCelonisBaseEnum,
                                            PyCelonisBaseModel)
from pycelonis_core.client.client import Client
from pycelonis_core.utils.ml_workbench import TRACKING_LOGGER

try:
    from pydantic.v1 import (Field, StrictBool, StrictInt,  # type: ignore
                             StrictStr)
except ImportError:
    from pydantic import (Field, StrictBool, StrictInt,  # type: ignore
                          StrictStr)


logger = logging.getLogger(TRACKING_LOGGER)


class DataPushDeleteStrategy(PyCelonisBaseEnum):
    DELETE = "DELETE"
    STORE_IN_STAGING_TABLE = "STORE_IN_STAGING_TABLE"
    IGNORE = "IGNORE"
    DELETE_AND_STORE_IN_STAGING_TABLE = "DELETE_AND_STORE_IN_STAGING_TABLE"


class DynamicVariableOpType(PyCelonisBaseEnum):
    FIND_MAX = "FIND_MAX"
    FIND_MIN = "FIND_MIN"
    LIST = "LIST"


class ExecutionMessageCode(PyCelonisBaseEnum):
    CONNECTOR_BUILDER_INFER_DUPLICATE_COLUMN = "CONNECTOR_BUILDER_INFER_DUPLICATE_COLUMN"
    CONNECTOR_BUILDER_INFER_TYPE_MISMATCH = "CONNECTOR_BUILDER_INFER_TYPE_MISMATCH"
    CONNECTOR_BUILDER_INFER_UNKNOWN_TYPE = "CONNECTOR_BUILDER_INFER_UNKNOWN_TYPE"
    CONNECTOR_BUILDER_INFER_INVALID_JSON = "CONNECTOR_BUILDER_INFER_INVALID_JSON"
    CONNECTOR_BUILDER_RESPONSE_ROOT_NOT_OBJECT = "CONNECTOR_BUILDER_RESPONSE_ROOT_NOT_OBJECT"
    CONNECTOR_BUILDER_INVALID_RESPONSE_ROOT = "CONNECTOR_BUILDER_INVALID_RESPONSE_ROOT"
    CONNECTOR_BUILDER_GET_SAMPLES_FAILED = "CONNECTOR_BUILDER_GET_SAMPLES_FAILED"
    CANCELING_EXTRACTION = "CANCELING_EXTRACTION"
    AT_LEAST_ONE_TABLE_EXTRACTION_FAILED = "AT_LEAST_ONE_TABLE_EXTRACTION_FAILED"
    STARTING_LOADING_TABLE_TO_TARGET = "STARTING_LOADING_TABLE_TO_TARGET"
    STARTING_RUNNING_JOB = "STARTING_RUNNING_JOB"
    EXTRACTION_SUCCESSFUL = "EXTRACTION_SUCCESSFUL"
    WAITING_FOR_SUCCESS_STATE = "WAITING_FOR_SUCCESS_STATE"
    REACHED_DATA_PUSH_JOB_LIMIT = "REACHED_DATA_PUSH_JOB_LIMIT"
    LOADING_TABLE = "LOADING_TABLE"
    EXTRACTION_FAILED = "EXTRACTION_FAILED"
    EXTRACTION_FAILED_WITH_EXCEPTION = "EXTRACTION_FAILED_WITH_EXCEPTION"
    EXTRACTION_FAILED_AFTER_RETRY_EXHAUSTED = "EXTRACTION_FAILED_AFTER_RETRY_EXHAUSTED"
    STOPPED_RETRYING_DATAPUSHJOB_CREATION = "STOPPED_RETRYING_DATAPUSHJOB_CREATION"
    USING_TARGET_TABLE_NAME = "USING_TARGET_TABLE_NAME"
    PUSHING_FILE_FOR_TABLE = "PUSHING_FILE_FOR_TABLE"
    CALLED_UPLOAD_FILE = "CALLED_UPLOAD_FILE"
    STARTING_EXTRACTION_FOR_RESOURCE = "STARTING_EXTRACTION_FOR_RESOURCE"
    NUMBER_OF_RECORDS_THAT_WILL_BE_EXTRACTED = "NUMBER_OF_RECORDS_THAT_WILL_BE_EXTRACTED"
    FINAL_COUNT_FOR_TABLE = "FINAL_COUNT_FOR_TABLE"
    EMPTY_RESPONSE_FROM_EXTRACTOR = "EMPTY_RESPONSE_FROM_EXTRACTOR"
    EXTRACTOR_NOT_REACHABLE = "EXTRACTOR_NOT_REACHABLE"
    INTEGRATION_NOT_USED_BY_TYPE = "INTEGRATION_NOT_USED_BY_TYPE"
    INTERNAL_ERROR_FROM_EXTRACTOR = "INTERNAL_ERROR_FROM_EXTRACTOR"
    ERROR_WHILE_MAKING_CAPABILITIES_REQUEST = "ERROR_WHILE_MAKING_CAPABILITIES_REQUEST"
    UPLINK_NOT_REACHABLE = "UPLINK_NOT_REACHABLE"
    CONNECTION_CONFIGURATION_VALID = "CONNECTION_CONFIGURATION_VALID"
    CONNECTION_CONFIGURATION_INVALID = "CONNECTION_CONFIGURATION_INVALID"
    CONNECTION_CHECK_MISSING_JDBC_DRIVER = "CONNECTION_CHECK_MISSING_JDBC_DRIVER"
    CONNECTION_SERVER_CERTIFICATE_NOT_TRUST = "CONNECTION_SERVER_CERTIFICATE_NOT_TRUST"
    CPP_2013_PACKAGES_NOT_INSTALLED = "CPP_2013_PACKAGES_NOT_INSTALLED"
    CPP_2013_PACKAGES_NOT_INSTALLED_AGENT = "CPP_2013_PACKAGES_NOT_INSTALLED_AGENT"
    EXTRACTOR_VERSION_DOES_NOT_SUPPORT_COMPRESSION = "EXTRACTOR_VERSION_DOES_NOT_SUPPORT_COMPRESSION"
    EXTRACTOR_VERSION_DOES_NOT_SUPPORT_ADVANCED_SETTINGS = "EXTRACTOR_VERSION_DOES_NOT_SUPPORT_ADVANCED_SETTINGS"
    NECESSARY_FUNCTION_NOT_IMPLEMENTED_IN_SAP = "NECESSARY_FUNCTION_NOT_IMPLEMENTED_IN_SAP"
    NECESSARY_ODP_FUNCTION_NOT_AVAILABLE_IN_SAP = "NECESSARY_ODP_FUNCTION_NOT_AVAILABLE_IN_SAP"
    UPDATE_RFC_TO_USE_ZIP = "UPDATE_RFC_TO_USE_ZIP"
    ERROR_DURING_CONNECTION_VALIDATION = "ERROR_DURING_CONNECTION_VALIDATION"
    SAP_ERR_NETWORK = "SAP_ERR_NETWORK"
    SAP_ERR_NO_RFC_PING_AUTH = "SAP_ERR_NO_RFC_PING_AUTH"
    SAP_ERR_NO_AUTH = "SAP_ERR_NO_AUTH"
    RFC_ERR_FILE_PERMISSIONS = "RFC_ERR_FILE_PERMISSIONS"
    RFC_ERR_WRITE_FILE = "RFC_ERR_WRITE_FILE"
    RFC_ERR_COMPRESS_FILE = "RFC_ERR_COMPRESS_FILE"
    RFC_ERR_COMPRESS_FILE_NOT_FOUND = "RFC_ERR_COMPRESS_FILE_NOT_FOUND"
    RFC_ERR_DELETE_FILE = "RFC_ERR_DELETE_FILE"
    RFC_ERR_LIST_FILES = "RFC_ERR_LIST_FILES"
    RFC_ERR_GENERIC = "RFC_ERR_GENERIC"
    RFC_WARN_DEFAULT_TARGET_PATH = "RFC_WARN_DEFAULT_TARGET_PATH"
    JCO_NOT_FOUND = "JCO_NOT_FOUND"
    JCO_NOT_FOUND_AGENT = "JCO_NOT_FOUND_AGENT"
    JCO_NATIVE_LIB_NOT_FOUND = "JCO_NATIVE_LIB_NOT_FOUND"
    JCO_NATIVE_LIB_NOT_FOUND_AGENT = "JCO_NATIVE_LIB_NOT_FOUND_AGENT"
    JCO_NATIVE_LIB_UNSUPPORTED_OS = "JCO_NATIVE_LIB_UNSUPPORTED_OS"
    JCO_NATIVE_LIB_COPY_ERROR = "JCO_NATIVE_LIB_COPY_ERROR"
    OPC_ERR_TIMEOUT = "OPC_ERR_TIMEOUT"
    OPC_ERR_PACKAGE_NOT_FOUND = "OPC_ERR_PACKAGE_NOT_FOUND"
    SAP_CHECK_JCO_JAR_INSTALLED = "SAP_CHECK_JCO_JAR_INSTALLED"
    SAP_CHECK_JCO_NATIVE_LIB_INSTALLED = "SAP_CHECK_JCO_NATIVE_LIB_INSTALLED"
    SAP_CHECK_MSVC_2013_INSTALLED = "SAP_CHECK_MSVC_2013_INSTALLED"
    SAP_CHECK_NETWORK = "SAP_CHECK_NETWORK"
    SAP_CHECK_NECESSARY_FUNCTIONS_IMPLEMENTED_IN_SAP = "SAP_CHECK_NECESSARY_FUNCTIONS_IMPLEMENTED_IN_SAP"
    SAP_CHECK_ODP_FUNCTIONS_SUPPORTED = "SAP_CHECK_ODP_FUNCTIONS_SUPPORTED"
    SAP_CHECK_EXTRACTOR_VERSION_SUPPORT_COMPRESSION = "SAP_CHECK_EXTRACTOR_VERSION_SUPPORT_COMPRESSION"
    SAP_CHECK_PARQUET_WRITING = "SAP_CHECK_PARQUET_WRITING"
    SAP_CHECK_RFC_TEST_FILE_CREATION = "SAP_CHECK_RFC_TEST_FILE_CREATION"
    SAP_CHECK_RFC_TEST_FILE_COMPRESSION = "SAP_CHECK_RFC_TEST_FILE_COMPRESSION"
    SAP_CHECK_RFC_TEST_FILE_DELETION = "SAP_CHECK_RFC_TEST_FILE_DELETION"
    SAP_CHECK_RFC_LIST_FILES = "SAP_CHECK_RFC_LIST_FILES"
    INTERNAL_ERROR_PERFORMING_TEST = "INTERNAL_ERROR_PERFORMING_TEST"
    SAP_CONFIGURATION_VALIDATION_FAILED = "SAP_CONFIGURATION_VALIDATION_FAILED"
    NO_FILE_RECEIVED_FROM_SAP = "NO_FILE_RECEIVED_FROM_SAP"
    CHANGE_LOG_ENABLED_NECESSARY_FUNCTION_NOT_IMPLEMENTED_IN_SAP = (
        "CHANGE_LOG_ENABLED_NECESSARY_FUNCTION_NOT_IMPLEMENTED_IN_SAP"
    )
    ERROR_RUNNING_VALIDATION_FUNCTION = "ERROR_RUNNING_VALIDATION_FUNCTION"
    NO_RUNNABLE_EXTRACTIONS_OR_TRANSFORMATIONS = "NO_RUNNABLE_EXTRACTIONS_OR_TRANSFORMATIONS"
    DATA_CONSUMPTION_LIMIT_EXCEEDED = "DATA_CONSUMPTION_LIMIT_EXCEEDED"
    STARTING_EXECUTION_OF_EXTRACTION = "STARTING_EXECUTION_OF_EXTRACTION"
    JOB_HAS_NO_DATA_SOURCE = "JOB_HAS_NO_DATA_SOURCE"
    DATASOURCE_NOT_REACHABLE = "DATASOURCE_NOT_REACHABLE"
    DATASOURCE_CONFIGURATION_IS_INVALID = "DATASOURCE_CONFIGURATION_IS_INVALID"
    REQUIRED_FEATURE_NOT_ENABLED = "REQUIRED_FEATURE_NOT_ENABLED"
    CANNOT_READ_DATA_SOURCE = "CANNOT_READ_DATA_SOURCE"
    CANNOT_RETRIEVE_EXTRACTOR_METADATA = "CANNOT_RETRIEVE_EXTRACTOR_METADATA"
    AMBIGUOUS_TABLE_NAME_IN_EXTRACTION = "AMBIGUOUS_TABLE_NAME_IN_EXTRACTION"
    METADATA_HAS_CHANGED = "METADATA_HAS_CHANGED"
    NO_TABLE_IN_EXTRACTION = "NO_TABLE_IN_EXTRACTION"
    EXTRACTION_IS_SKIPPED = "EXTRACTION_IS_SKIPPED"
    VARIABLE_RESOLVING_ERROR = "VARIABLE_RESOLVING_ERROR"
    DELETE_ONLY_POSSIBLE_FOR_DELTA = "DELETE_ONLY_POSSIBLE_FOR_DELTA"
    TABLE_MAPPING_ERROR = "TABLE_MAPPING_ERROR"
    JOB_EXECUTION_CANCELLED = "JOB_EXECUTION_CANCELLED"
    ERROR_STARTING_EXTRACTION = "ERROR_STARTING_EXTRACTION"
    VERSION_INFORMATION = "VERSION_INFORMATION"
    DATA_CONNECTION_CONFIGURATION = "DATA_CONNECTION_CONFIGURATION"
    EXTRACTION_CONFIGURATION = "EXTRACTION_CONFIGURATION"
    TABLE_CONFIGURATION = "TABLE_CONFIGURATION"
    TABLE_SUCCESSFULLY_EXTRACTED = "TABLE_SUCCESSFULLY_EXTRACTED"
    ERROR_COMPLETING_TABLE_LOAD = "ERROR_COMPLETING_TABLE_LOAD"
    INVALID_EXTRACTION_IS_RUNNING = "INVALID_EXTRACTION_IS_RUNNING"
    GOT_CHUNK_FOR_TERMINAL_EXTRACTION = "GOT_CHUNK_FOR_TERMINAL_EXTRACTION"
    CANCELING_EXTRACTION_WITH_NAME = "CANCELING_EXTRACTION_WITH_NAME"
    GOT_CHUNK_FOR_TABLE = "GOT_CHUNK_FOR_TABLE"
    FILE_UPLOAD_FAILED = "FILE_UPLOAD_FAILED"
    JOB_ALREADY_RUNNING = "JOB_ALREADY_RUNNING"
    STARTING_RUNNING_JOB_WITH_NAME = "STARTING_RUNNING_JOB_WITH_NAME"
    EARLIER_JOB_IN_SCHEDULE_FAILED_OR_CANCELLED = "EARLIER_JOB_IN_SCHEDULE_FAILED_OR_CANCELLED"
    JOB_COULD_NOT_STARTED = "JOB_COULD_NOT_STARTED"
    EXECUTING_JOB_IN_SCHEDULE = "EXECUTING_JOB_IN_SCHEDULE"
    JOB_ALREADY_RUNNING_CANNOT_EXECUTE_SCHEDULE = "JOB_ALREADY_RUNNING_CANNOT_EXECUTE_SCHEDULE"
    CANNOT_EXECUTE_SCHEDULED_JOBS = "CANNOT_EXECUTE_SCHEDULED_JOBS"
    EXECUTION_CANCELED_ON_REQUEST = "EXECUTION_CANCELED_ON_REQUEST"
    EXECUTION_AUTOMATICALLY_CANCELLED_AFTER_X_MINUTES = "EXECUTION_AUTOMATICALLY_CANCELLED_AFTER_X_MINUTES"
    EXECUTION_CANCELED_AFTER_CONNECTOR_DISCONNECT = "EXECUTION_CANCELED_AFTER_CONNECTOR_DISCONNECT"
    REMOVING_TMP_FOLDER = "REMOVING_TMP_FOLDER"
    CANNOT_REMOVE_TMP_FOLDER = "CANNOT_REMOVE_TMP_FOLDER"
    CHECKING_SOURCE_SYSTEM_METADATA_CHANGE = "CHECKING_SOURCE_SYSTEM_METADATA_CHANGE"
    UNABLE_GET_COLUMNS_FOR_TABLE = "UNABLE_GET_COLUMNS_FOR_TABLE"
    NO_METADATA_FOUND_FOR_COMPARISON = "NO_METADATA_FOUND_FOR_COMPARISON"
    COLUMNS_HAVE_CHANGED = "COLUMNS_HAVE_CHANGED"
    CANNOT_MAP_TABLE_NAME_IN_EXTRACTION = "CANNOT_MAP_TABLE_NAME_IN_EXTRACTION"
    COLUMN_SMALL_FOR_ANONYMIZATION = "COLUMN_SMALL_FOR_ANONYMIZATION"
    COLUMN_INVALID_FOR_ANONYMIZATION = "COLUMN_INVALID_FOR_ANONYMIZATION"
    METADATA_CHANGED_FOR_TABLE = "METADATA_CHANGED_FOR_TABLE"
    FAILED_TO_START_EXECUTION_ITEM = "FAILED_TO_START_EXECUTION_ITEM"
    FAILED_TO_CHANGE_EXECUTION_ITEM_STATUS = "FAILED_TO_CHANGE_EXECUTION_ITEM_STATUS"
    RESUME_EXTRACTION_FROM_BEGINNING = "RESUME_EXTRACTION_FROM_BEGINNING"
    FAILED_EXTRACTION_LOST_CONNECTION = "FAILED_EXTRACTION_LOST_CONNECTION"
    JOB_QUEUING_WAITING_FOR_JOB_IN_ANOTHER_SCHEDULE = "JOB_QUEUING_WAITING_FOR_JOB_IN_ANOTHER_SCHEDULE"
    JOB_QUEUING_WAITING_FOR_JOB_FROM_MANUAL_EXECUTION = "JOB_QUEUING_WAITING_FOR_JOB_FROM_MANUAL_EXECUTION"
    TRANSFORMATION_EXECUTION_UNSUPPORTED_STATEMENT = "TRANSFORMATION_EXECUTION_UNSUPPORTED_STATEMENT"
    TRANSFORMATION_EXECUTION_ACCESS_FORBIDDEN = "TRANSFORMATION_EXECUTION_ACCESS_FORBIDDEN"
    TASK_DISPATCH_FAILED = "TASK_DISPATCH_FAILED"
    FETCHING_METADATA_FROM_CONNECTOR = "FETCHING_METADATA_FROM_CONNECTOR"
    JOB_CANCELLED_NOT_EXECUTING_TRANSFORMATION = "JOB_CANCELLED_NOT_EXECUTING_TRANSFORMATION"
    IGNORE_TRANSFORMATION = "IGNORE_TRANSFORMATION"
    TRANSFORMATION_RUNTIME_RESOLUTION_ERROR = "TRANSFORMATION_RUNTIME_RESOLUTION_ERROR"
    TRANSFORMATION_SKIPPED = "TRANSFORMATION_SKIPPED"
    TRANSFORMATION_VARIABLE_RESOLUTION_ERROR = "TRANSFORMATION_VARIABLE_RESOLUTION_ERROR"
    TRANSFORMATION_ERROR = "TRANSFORMATION_ERROR"
    NO_SQL_IN_TRANSFORMATION = "NO_SQL_IN_TRANSFORMATION"
    TASK_QUEUING_WAITING_FOR_TASK_IN_ANOTHER_SCHEDULE = "TASK_QUEUING_WAITING_FOR_TASK_IN_ANOTHER_SCHEDULE"
    CHANGE_AWARE_EXECUTION_EXTRACTION_UNCHANGED = "CHANGE_AWARE_EXECUTION_EXTRACTION_UNCHANGED"
    CHANGE_AWARE_EXECUTION_TRANSFORMATION_SKIPPABLE = "CHANGE_AWARE_EXECUTION_TRANSFORMATION_SKIPPABLE"
    CHANGE_AWARE_EXECUTION_TRANSFORMATION_SKIPPED = "CHANGE_AWARE_EXECUTION_TRANSFORMATION_SKIPPED"
    CHANGE_AWARE_EXECUTION_DATA_MODEL_LOAD_SKIPPABLE = "CHANGE_AWARE_EXECUTION_DATA_MODEL_LOAD_SKIPPABLE"
    CHANGE_AWARE_EXECUTION_DATA_MODEL_LOAD_STEP_SKIPPED = "CHANGE_AWARE_EXECUTION_DATA_MODEL_LOAD_STEP_SKIPPED"
    CHANGE_AWARE_EXECUTION_DATA_MODEL_LOAD_SKIPPED = "CHANGE_AWARE_EXECUTION_DATA_MODEL_LOAD_SKIPPED"
    CHANGE_AWARE_EXECUTION_REASON_TASK_CHANGED = "CHANGE_AWARE_EXECUTION_REASON_TASK_CHANGED"
    CHANGE_AWARE_EXECUTION_REASON_UPSTREAM_MISSING_CHANGE_INFO = (
        "CHANGE_AWARE_EXECUTION_REASON_UPSTREAM_MISSING_CHANGE_INFO"
    )
    CHANGE_AWARE_EXECUTION_REASON_UPSTREAM_CHANGE_INFO_RESET = (
        "CHANGE_AWARE_EXECUTION_REASON_UPSTREAM_CHANGE_INFO_RESET"
    )
    CHANGE_AWARE_EXECUTION_REASON_UPSTREAM_CHANGE_OUTSIDE = "CHANGE_AWARE_EXECUTION_REASON_UPSTREAM_CHANGE_OUTSIDE"
    CHANGE_AWARE_EXECUTION_REASON_UPSTREAM_WRITE_IN_PROGRESS = (
        "CHANGE_AWARE_EXECUTION_REASON_UPSTREAM_WRITE_IN_PROGRESS"
    )
    CHANGE_AWARE_EXECUTION_REASON_DOWNSTREAM_WRITE_IN_PROGRESS = (
        "CHANGE_AWARE_EXECUTION_REASON_DOWNSTREAM_WRITE_IN_PROGRESS"
    )
    CHANGE_AWARE_EXECUTION_REASON_DOWNSTREAM_CHANGE_OUTSIDE = "CHANGE_AWARE_EXECUTION_REASON_DOWNSTREAM_CHANGE_OUTSIDE"
    CHANGE_AWARE_EXECUTION_REASON_DOWNSTREAM_CHANGED_TABLE = "CHANGE_AWARE_EXECUTION_REASON_DOWNSTREAM_CHANGED_TABLE"
    CHANGE_AWARE_EXECUTION_REASON_DOWNSTREAM_CONCURRENT_WRITE = (
        "CHANGE_AWARE_EXECUTION_REASON_DOWNSTREAM_CONCURRENT_WRITE"
    )
    CHANGE_AWARE_EXECUTION_REASON_DOWNSTREAM_CHANGED_TABLE_RESET = (
        "CHANGE_AWARE_EXECUTION_REASON_DOWNSTREAM_CHANGED_TABLE_RESET"
    )
    DELTA_LOAD_HAS_NO_FILTER = "DELTA_LOAD_HAS_NO_FILTER"
    FILTER_STATEMENT_DUPLICATION = "FILTER_STATEMENT_DUPLICATION"
    WSDL_FILE_NOT_FOUND = "WSDL_FILE_NOT_FOUND"
    WSDL_MULTIPLE_FILES_FOUND = "WSDL_MULTIPLE_FILES_FOUND"
    WSDL_DIRECTORY_NOT_READABLE = "WSDL_DIRECTORY_NOT_READABLE"
    WSDL_DIRECTORY_IS_EMPTY = "WSDL_DIRECTORY_IS_EMPTY"
    WSDL_PORT_NOT_FOUND = "WSDL_PORT_NOT_FOUND"
    SERVICE_NOW_DEFAULT_LIST_OF_RESOURCES_DUE_TO_LIMITED_PERMISSIONS = (
        "SERVICE_NOW_DEFAULT_LIST_OF_RESOURCES_DUE_TO_LIMITED_PERMISSIONS"
    )
    SERVICE_NOW_ERROR_FETCH_TABLES = "SERVICE_NOW_ERROR_FETCH_TABLES"
    EXTRACTING_FROM_ROW = "EXTRACTING_FROM_ROW"
    NO_VALUE_FOUND_IN_FIRST_COLUMN = "NO_VALUE_FOUND_IN_FIRST_COLUMN"
    ERROR_WHILE_EXTRACTING_TABLE = "ERROR_WHILE_EXTRACTING_TABLE"
    ERROR_RETRIEVING_SPREADSHEET = "ERROR_RETRIEVING_SPREADSHEET"
    GOOGLE_SHEETS_API_LIMIT_REACHED = "GOOGLE_SHEETS_API_LIMIT_REACHED"
    MISSING_FULL_LOAD_COLUMN = "MISSING_FULL_LOAD_COLUMN"
    MISSING_DELTA_LOAD_COLUMN = "MISSING_DELTA_LOAD_COLUMN"
    CONTAINS_FAULTY_COLUMN = "CONTAINS_FAULTY_COLUMN"
    ORACLE_BICC_REQUIRED_COLUMNS_FOR_DELTA_LOADING_MISSING = "ORACLE_BICC_REQUIRED_COLUMNS_FOR_DELTA_LOADING_MISSING"
    MISSING_MANDATORY_DATE_FILTER_COLUMN = "MISSING_MANDATORY_DATE_FILTER_COLUMN"
    EMPTY_DIRECTORY_AS_TABLE = "EMPTY_DIRECTORY_AS_TABLE"
    DIRECTORY_CONTAINS_MULTIPLE_TYPES = "DIRECTORY_CONTAINS_MULTIPLE_TYPES"
    DOWNLOAD_FINISHED_FOR_FILE = "DOWNLOAD_FINISHED_FOR_FILE"
    DOWNLOAD_PROGRESS_OF_FILE = "DOWNLOAD_PROGRESS_OF_FILE"
    ERROR_EXECUTING_BATCH = "ERROR_EXECUTING_BATCH"
    NO_RECORDS_FOUND_FOR_TABLE = "NO_RECORDS_FOUND_FOR_TABLE"
    UNKNOWN_COLUMN_IN_FILTER = "UNKNOWN_COLUMN_IN_FILTER"
    INCOMPATIBLE_COMPARISON_IN_FILTER = "INCOMPATIBLE_COMPARISON_IN_FILTER"
    REMOVE_DUPLICATE_WITHOUT_PK = "REMOVE_DUPLICATE_WITHOUT_PK"
    SPECIAL_CHAR_IN_TABLE_NAME = "SPECIAL_CHAR_IN_TABLE_NAME"
    DUPLICATE_COLUMN_DETECTED = "DUPLICATE_COLUMN_DETECTED"
    PARTITIONS_CONFIGURATION_FAILURE = "PARTITIONS_CONFIGURATION_FAILURE"
    PARTITIONS_SPECIFICATIONS_FAILURE = "PARTITIONS_SPECIFICATIONS_FAILURE"
    SCHEDULING_EXECUTION_STARTED = "SCHEDULING_EXECUTION_STARTED"
    DATA_MODEL_SUSPENSION_SKIPPING_LOAD = "DATA_MODEL_SUSPENSION_SKIPPING_LOAD"
    SCHEDULING_EXECUTION_MAX_ATTEMPTS_EXCEEDED = "SCHEDULING_EXECUTION_MAX_ATTEMPTS_EXCEEDED"
    SCHEDULING_EXECUTION_RETRY_FAILED = "SCHEDULING_EXECUTION_RETRY_FAILED"
    SCHEDULING_EXECUTION_FUTURE_RETRY = "SCHEDULING_EXECUTION_FUTURE_RETRY"
    SCHEDULING_EXECUTION_RETRY = "SCHEDULING_EXECUTION_RETRY"
    SCHEDULING_EXECUTION_TASK_RETRYING_NO_RETRIES_LEFT = "SCHEDULING_EXECUTION_TASK_RETRYING_NO_RETRIES_LEFT"
    SCHEDULING_EXECUTION_TASK_RETRYING = "SCHEDULING_EXECUTION_TASK_RETRYING"
    SCHEDULING_EXECUTION_INACTIVE_TEAM = "SCHEDULING_EXECUTION_INACTIVE_TEAM"
    SCHEDULING_EXECUTION_JOBS_ALREADY_RUNNING = "SCHEDULING_EXECUTION_JOBS_ALREADY_RUNNING"
    SMART_ETL_DAG_BUILDING_DEFAULT_ERROR = "SMART_ETL_DAG_BUILDING_DEFAULT_ERROR"
    SMART_ETL_DAG_BUILDING_DYNAMIC_PARAMETERS_USED_IN_TASK = "SMART_ETL_DAG_BUILDING_DYNAMIC_PARAMETERS_USED_IN_TASK"
    SMART_ETL_DAG_BUILDING_DYNAMIC_PARAMETERS_USED_IN_TASKS = "SMART_ETL_DAG_BUILDING_DYNAMIC_PARAMETERS_USED_IN_TASKS"
    SMART_ETL_DAG_BUILDING_NOT_SUPPORTED_FUNCTION_IN_TRANSFORMATION = (
        "SMART_ETL_DAG_BUILDING_NOT_SUPPORTED_FUNCTION_IN_TRANSFORMATION"
    )
    SMART_ETL_DAG_BUILDING_NOT_SUPPORTED_FUNCTION_IN_VIEW_DEFINITION = (
        "SMART_ETL_DAG_BUILDING_NOT_SUPPORTED_FUNCTION_IN_VIEW_DEFINITION"
    )
    SMART_ETL_DAG_BUILDING_TRANSFORMATION_STATEMENT_PARSING_ERROR = (
        "SMART_ETL_DAG_BUILDING_TRANSFORMATION_STATEMENT_PARSING_ERROR"
    )
    SMART_ETL_DAG_BUILDING_UNKNOWN_SQL_STATEMENT = "SMART_ETL_DAG_BUILDING_UNKNOWN_SQL_STATEMENT"
    SMART_ETL_DAG_BUILDING_VARIABLE_PROCESSING_ERROR = "SMART_ETL_DAG_BUILDING_VARIABLE_PROCESSING_ERROR"
    SMART_ETL_DAG_BUILDING_VIEW_DEFINITION_PARSER_ERROR = "SMART_ETL_DAG_BUILDING_VIEW_DEFINITION_PARSER_ERROR"
    SMART_ETL_DAG_BUILDING_VIEW_FETCHING_ERROR = "SMART_ETL_DAG_BUILDING_VIEW_FETCHING_ERROR"
    SMART_ETL_EXECUTION_DAG_ENABLED = "SMART_ETL_EXECUTION_DAG_ENABLED"
    SMART_ETL_EXECUTION_EXTRACTION_NO_HISTORICAL_DATA = "SMART_ETL_EXECUTION_EXTRACTION_NO_HISTORICAL_DATA"
    SMART_ETL_EXECUTION_EXTRACTION_OPTIMAL_ORDER = "SMART_ETL_EXECUTION_EXTRACTION_OPTIMAL_ORDER"
    SMART_ETL_EXECUTION_INDIRECT_DEPENDENCY_FAILED = "SMART_ETL_EXECUTION_INDIRECT_DEPENDENCY_FAILED"
    SMART_ETL_EXECUTION_REQUIRED_DEPENDENCY_FAILED = "SMART_ETL_EXECUTION_REQUIRED_DEPENDENCY_FAILED"
    SMART_ETL_EXECUTION_SCHEDULING_DAG_ENABLED = "SMART_ETL_EXECUTION_SCHEDULING_DAG_ENABLED"
    SMART_ETL_EXECUTION_DATA_JOB_WITHIN_SCHEDULE_DAG_ENABLED = (
        "SMART_ETL_EXECUTION_DATA_JOB_WITHIN_SCHEDULE_DAG_ENABLED"
    )
    SMART_ETL_EXECUTION_WAITING_FOR_FIRST_TASK = "SMART_ETL_EXECUTION_WAITING_FOR_FIRST_TASK"
    SMART_ETL_DAG_BUILDING_SCHEDULING_DEFAULT_ERROR = "SMART_ETL_DAG_BUILDING_SCHEDULING_DEFAULT_ERROR"
    SMART_ETL_SCHEDULE_EXECUTION_INDIRECT_DEPENDENCY_FAILED = "SMART_ETL_SCHEDULE_EXECUTION_INDIRECT_DEPENDENCY_FAILED"
    SMART_ETL_SCHEDULE_EXECUTION_REQUIRED_DEPENDENCY_FAILED = "SMART_ETL_SCHEDULE_EXECUTION_REQUIRED_DEPENDENCY_FAILED"
    DELTA_TRANSFORMATION_PRE_PROCESSED_BOUND = "DELTA_TRANSFORMATION_PRE_PROCESSED_BOUND"
    DELTA_TRANSFORMATION_PRE_PROCESSED_BOUND_PER_TABLE = "DELTA_TRANSFORMATION_PRE_PROCESSED_BOUND_PER_TABLE"
    DELTA_TRANSFORMATION_PRE_PROCESSED_BOUND_DATASOURCE_PER_TABLE = (
        "DELTA_TRANSFORMATION_PRE_PROCESSED_BOUND_DATASOURCE_PER_TABLE"
    )
    DELTA_TRANSFORMATION_RESOLVED_QUERY_FULL_MODE = "DELTA_TRANSFORMATION_RESOLVED_QUERY_FULL_MODE"
    DELTA_TRANSFORMATION_RESOLVED_QUERY_FULL_MODE_DATASOURCE_PER_TABLE = (
        "DELTA_TRANSFORMATION_RESOLVED_QUERY_FULL_MODE_DATASOURCE_PER_TABLE"
    )
    DELTA_TRANSFORMATION_RESOLVED_QUERY_DELTA_MODE_FIRST_RUN = (
        "DELTA_TRANSFORMATION_RESOLVED_QUERY_DELTA_MODE_FIRST_RUN"
    )
    DELTA_TRANSFORMATION_RESOLVED_QUERY_DELTA_MODE_FIRST_RUN_DATASOURCE_PER_TABLE = (
        "DELTA_TRANSFORMATION_RESOLVED_QUERY_DELTA_MODE_FIRST_RUN_DATASOURCE_PER_TABLE"
    )
    DELTA_TRANSFORMATION_RESOLVED_QUERY_DELTA_MODE = "DELTA_TRANSFORMATION_RESOLVED_QUERY_DELTA_MODE"
    DELTA_TRANSFORMATION_RESOLVED_QUERY_DELTA_MODE_DATASOURCE_PER_TABLE = (
        "DELTA_TRANSFORMATION_RESOLVED_QUERY_DELTA_MODE_DATASOURCE_PER_TABLE"
    )
    DELTA_TRANSFORMATION_DATA_JOB_SUCCESS_LOWER_BOUND_UPDATE = (
        "DELTA_TRANSFORMATION_DATA_JOB_SUCCESS_LOWER_BOUND_UPDATE"
    )
    DELTA_TRANSFORMATION_DATA_JOB_SUCCESS_LOWER_BOUND_UPDATE_PER_TABLE = (
        "DELTA_TRANSFORMATION_DATA_JOB_SUCCESS_LOWER_BOUND_UPDATE_PER_TABLE"
    )
    DELTA_TRANSFORMATION_DATA_JOB_SUCCESS_LOWER_BOUND_UPDATE_DATASOURCE_PER_TABLE = (
        "DELTA_TRANSFORMATION_DATA_JOB_SUCCESS_LOWER_BOUND_UPDATE_DATASOURCE_PER_TABLE"
    )
    DELTA_TRANSFORMATION_DATA_JOB_FAILURE_LOWER_BOUND_UPDATE = (
        "DELTA_TRANSFORMATION_DATA_JOB_FAILURE_LOWER_BOUND_UPDATE"
    )
    DELTA_TRANSFORMATION_DATA_JOB_FAILURE_LOWER_BOUND_UPDATE_PER_TABLE = (
        "DELTA_TRANSFORMATION_DATA_JOB_FAILURE_LOWER_BOUND_UPDATE_PER_TABLE"
    )
    DELTA_TRANSFORMATION_DATA_JOB_FAILURE_LOWER_BOUND_UPDATE_DATASOURCE_PER_TABLE = (
        "DELTA_TRANSFORMATION_DATA_JOB_FAILURE_LOWER_BOUND_UPDATE_DATASOURCE_PER_TABLE"
    )
    DELTA_TRANSFORMATION_INITIAL_MESSAGE = "DELTA_TRANSFORMATION_INITIAL_MESSAGE"
    DELTA_TRANSFORMATION_FAILURE_MESSAGE = "DELTA_TRANSFORMATION_FAILURE_MESSAGE"


class FilterParserDataType(PyCelonisBaseEnum):
    DATE = "DATE"
    DOUBLE = "DOUBLE"
    INT = "INT"
    STRING = "STRING"
    COLUMN = "COLUMN"
    QUALIFIED_COLUMN = "QUALIFIED_COLUMN"
    LIST_DOUBLE = "LIST_DOUBLE"
    LIST_INT = "LIST_INT"
    LIST_STRING = "LIST_STRING"
    NULL = "NULL"


class ReplicationStatus(PyCelonisBaseEnum):
    UNINITIALIZED = "UNINITIALIZED"
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    DEGRADED = "DEGRADED"
    ERROR_REPLICATING = "ERROR_REPLICATING"
    ERROR_INITIALIZING = "ERROR_INITIALIZING"


class ReplicationType(PyCelonisBaseEnum):
    SAP = "SAP"
    ODP = "ODP"
    DATABASE = "DATABASE"
    SERVICENOW = "SERVICENOW"


class TableConfigurationParameterKey(PyCelonisBaseEnum):
    BATCH_SIZE = "BATCH_SIZE"
    ROLLING_PAGE_SIZE = "ROLLING_PAGE_SIZE"
    SPLIT_JOB_BY_DAYS = "SPLIT_JOB_BY_DAYS"
    MAX_STRING_LENGTH = "MAX_STRING_LENGTH"
    BINARY_HANDLING = "BINARY_HANDLING"
    DELTA_LOAD_AS_REPLACE_MERGE = "DELTA_LOAD_AS_REPLACE_MERGE"
    EXTRACT_DISPLAY_VALUES = "EXTRACT_DISPLAY_VALUES"
    ENABLE_ORDER_BY = "ENABLE_ORDER_BY"
    METADATA_SOURCE = "METADATA_SOURCE"
    MAX_EXTRACTED_RECORDS = "MAX_EXTRACTED_RECORDS"
    PARTITION_COLUMNS = "PARTITION_COLUMNS"
    ORDER_COLUMNS = "ORDER_COLUMNS"
    REMOVE_DUPLICATES_WITH_ORDER = "REMOVE_DUPLICATES_WITH_ORDER"
    CHANGELOG_EXTRACTION_STRATEGY_OPTIONS = "CHANGELOG_EXTRACTION_STRATEGY_OPTIONS"
    CHANGELOG_TABLE_NAME = "CHANGELOG_TABLE_NAME"
    CHANGELOG_TABLE_NAME_COLUMN = "CHANGELOG_TABLE_NAME_COLUMN"
    CHANGELOG_ID_COLUMN = "CHANGELOG_ID_COLUMN"
    SOURCE_SYSTEM_JOIN_COLUMN = "SOURCE_SYSTEM_JOIN_COLUMN"
    CHANGELOG_JOIN_COLUMN = "CHANGELOG_JOIN_COLUMN"
    CHANGELOG_CHANGE_TYPE_COLUMN = "CHANGELOG_CHANGE_TYPE_COLUMN"
    CHANGELOG_DELETE_CHANGE_TYPE_IDENTIFIER = "CHANGELOG_DELETE_CHANGE_TYPE_IDENTIFIER"
    CHANGELOG_CLEANUP_METHOD = "CHANGELOG_CLEANUP_METHOD"
    CHANGELOG_CLEANUP_STATUS_COLUMN = "CHANGELOG_CLEANUP_STATUS_COLUMN"
    CHANGELOG_CLEANUP_STATUS_VALUE = "CHANGELOG_CLEANUP_STATUS_VALUE"
    CHANGELOG_CHANGE_TIMESTAMP_COLUMN = "CHANGELOG_CHANGE_TIMESTAMP_COLUMN"
    IGNORE_ERRORS_ON_RESPONSE = "IGNORE_ERRORS_ON_RESPONSE"
    STRING_COLUMN_LENGTH = "STRING_COLUMN_LENGTH"
    CURRENCY = "CURRENCY"
    FILE_EXTENSION_OPTIONS = "FILE_EXTENSION_OPTIONS"
    FILE_HAS_HEADER_ROW = "FILE_HAS_HEADER_ROW"
    FILE_ENCODING = "FILE_ENCODING"
    FIELD_SEPARATOR = "FIELD_SEPARATOR"
    QUOTE_CHARACTER = "QUOTE_CHARACTER"
    ESCAPE_SEQUENCE = "ESCAPE_SEQUENCE"
    DECIMAL_SEPARATOR = "DECIMAL_SEPARATOR"
    THOUSAND_SEPARATOR = "THOUSAND_SEPARATOR"
    LINE_ENDING = "LINE_ENDING"
    DATA_FORMAT = "DATA_FORMAT"
    CURRENCY_FROM = "CURRENCY_FROM"
    CURRENCY_TO = "CURRENCY_TO"
    CONVERSION_TYPE = "CONVERSION_TYPE"
    PARTITIONING_PERIOD = "PARTITIONING_PERIOD"
    EXTRACTION_PARAMETER_TIME_UNIT = "EXTRACTION_PARAMETER_TIME_UNIT"
    VIEW_PARAMETERS = "VIEW_PARAMETERS"
    EXTRACTION_SELECTABLE_FILTERS = "EXTRACTION_SELECTABLE_FILTERS"
    EXTRACT_DATA_OFFSET = "EXTRACT_DATA_OFFSET"
    EXTRACT_DATA_OFFSET_TYPE = "EXTRACT_DATA_OFFSET_TYPE"
    CLIENT = "CLIENT"
    USER = "USER"
    TRANSACTION = "TRANSACTION"
    PROGRAM = "PROGRAM"
    TASK_TYPE = "TASK_TYPE"
    RESPONSE_TIME = "RESPONSE_TIME"
    DB_REQUEST_TIME = "DB_REQUEST_TIME"
    CPU_TIME = "CPU_TIME"
    BYTES_REQUEST = "BYTES_REQUEST"
    DB_CHANGES = "DB_CHANGES"
    ELVMC_TIME = "ELVMC_TIME"
    OPTIMIZER_HINTS = "OPTIMIZER_HINTS"
    PAGINATION_WINDOW_IN_DAYS = "PAGINATION_WINDOW_IN_DAYS"
    UNKNOWN = "UNKNOWN"


class VariableType(PyCelonisBaseEnum):
    PRIVATE_CONSTANT = "PRIVATE_CONSTANT"
    PUBLIC_CONSTANT = "PUBLIC_CONSTANT"
    DYNAMIC = "DYNAMIC"


class DataSourceAvailableTables(PyCelonisBaseModel):
    available_tables: Optional['List[Optional[DataSourceTable]]'] = Field(alias="availableTables")
    lookup_successful: Optional['bool'] = Field(alias="lookupSuccessful")
    message: Optional['str'] = Field(alias="message")
    translated_connector_message: Optional['TranslatedConnectorMessage'] = Field(alias="translatedConnectorMessage")


class DataSourceTable(PyCelonisBaseModel):
    alias: Optional['str'] = Field(alias="alias")
    data_source_id: Optional['str'] = Field(alias="dataSourceId")
    name: Optional['str'] = Field(alias="name")
    schema_: Optional['str'] = Field(alias="schema")


class ExceptionReference(PyCelonisBaseModel):
    message: Optional['str'] = Field(alias="message")
    reference: Optional['str'] = Field(alias="reference")
    short_message: Optional['str'] = Field(alias="shortMessage")


class FrontendHandledBackendError(PyCelonisBaseModel):
    error_information: Optional['Any'] = Field(alias="errorInformation")
    frontend_error_key: Optional['str'] = Field(alias="frontendErrorKey")


class InitializationSelectionTransport(PyCelonisBaseModel):
    initialization_script_ids: Optional['List[Optional[str]]'] = Field(alias="initializationScriptIds")
    replication_ids: Optional['List[Optional[str]]'] = Field(alias="replicationIds")
    replication_initialization_selections: Optional['List[Optional[ReplicationInitializationSelectionTransport]]'] = (
        Field(alias="replicationInitializationSelections")
    )


class LogTranslationParameter(PyCelonisBaseModel):
    key: Optional['str'] = Field(alias="key")
    value: Optional['str'] = Field(alias="value")


class PoolVariableTransport(PyCelonisBaseModel):
    data_type: Optional['FilterParserDataType'] = Field(alias="dataType")
    default_settings: Optional['VariableSettingsTransport'] = Field(alias="defaultSettings")
    default_values: Optional['List[Optional[VariableValueTransport]]'] = Field(alias="defaultValues")
    description: Optional['str'] = Field(alias="description")
    dynamic_column: Optional['str'] = Field(alias="dynamicColumn")
    dynamic_data_source_id: Optional['str'] = Field(alias="dynamicDataSourceId")
    dynamic_table: Optional['str'] = Field(alias="dynamicTable")
    dynamic_variable_op_type: Optional['DynamicVariableOpType'] = Field(alias="dynamicVariableOpType")
    id: Optional['str'] = Field(alias="id")
    name: Optional['str'] = Field(alias="name")
    placeholder: Optional['str'] = Field(alias="placeholder")
    pool_id: Optional['str'] = Field(alias="poolId")
    settings: Optional['VariableSettingsTransport'] = Field(alias="settings")
    type_: Optional['VariableType'] = Field(alias="type")
    values: Optional['List[Optional[VariableValueTransport]]'] = Field(alias="values")


class ReplicationCalculatedColumnTransport(PyCelonisBaseModel):
    column_name: Optional['str'] = Field(alias="columnName")
    expression: Optional['str'] = Field(alias="expression")
    id: Optional['str'] = Field(alias="id")
    replication_id: Optional['str'] = Field(alias="replicationId")


class ReplicationColumnTransport(PyCelonisBaseModel):
    anonymized: Optional['bool'] = Field(alias="anonymized")
    column_name: Optional['str'] = Field(alias="columnName")
    id: Optional['str'] = Field(alias="id")
    mandatory_primary_key: Optional['bool'] = Field(alias="mandatoryPrimaryKey")
    primary_key: Optional['bool'] = Field(alias="primaryKey")
    replication_id: Optional['str'] = Field(alias="replicationId")


class ReplicationConfigurationTransport(PyCelonisBaseModel):
    replication: Optional['ReplicationTransport'] = Field(alias="replication")
    replication_calculated_columns: Optional['List[Optional[ReplicationCalculatedColumnTransport]]'] = Field(
        alias="replicationCalculatedColumns"
    )
    replication_columns: Optional['List[Optional[ReplicationColumnTransport]]'] = Field(alias="replicationColumns")
    replication_dependencies: Optional['List[Optional[ReplicationDependencyTransport]]'] = Field(
        alias="replicationDependencies"
    )
    replication_joins: Optional['List[Optional[ReplicationJoinTransport]]'] = Field(alias="replicationJoins")
    replication_transformations: Optional['List[Optional[ReplicationTransformationTransport]]'] = Field(
        alias="replicationTransformations"
    )


class ReplicationDependencyTransport(PyCelonisBaseModel):
    dependent_replication_id: Optional['str'] = Field(alias="dependentReplicationId")
    id: Optional['str'] = Field(alias="id")
    replication_id: Optional['str'] = Field(alias="replicationId")
    table_name: Optional['str'] = Field(alias="tableName")


class ReplicationInitializationScriptTransport(PyCelonisBaseModel):
    data_pool_id: Optional['str'] = Field(alias="dataPoolId")
    data_source_id: Optional['str'] = Field(alias="dataSourceId")
    id: Optional['str'] = Field(alias="id")
    name: Optional['str'] = Field(alias="name")
    statement: Optional['str'] = Field(alias="statement")


class ReplicationInitializationSelectionTransport(PyCelonisBaseModel):
    extraction_selected: Optional['bool'] = Field(alias="extractionSelected")
    replication_id: Optional['str'] = Field(alias="replicationId")
    transformation_selected: Optional['bool'] = Field(alias="transformationSelected")


class ReplicationJoinTransport(PyCelonisBaseModel):
    child_table: Optional['str'] = Field(alias="childTable")
    custom_join_path: Optional['str'] = Field(alias="customJoinPath")
    id: Optional['str'] = Field(alias="id")
    join_filter: Optional['str'] = Field(alias="joinFilter")
    order: Optional['int'] = Field(alias="order")
    parent_schema: Optional['str'] = Field(alias="parentSchema")
    parent_table: Optional['str'] = Field(alias="parentTable")
    replication_id: Optional['str'] = Field(alias="replicationId")
    use_primary_keys: Optional['bool'] = Field(alias="usePrimaryKeys")


class ReplicationTransformationTransport(PyCelonisBaseModel):
    disabled: Optional['bool'] = Field(alias="disabled")
    id: Optional['str'] = Field(alias="id")
    name: Optional['str'] = Field(alias="name")
    order: Optional['int'] = Field(alias="order")
    replication_id: Optional['str'] = Field(alias="replicationId")
    statement: Optional['str'] = Field(alias="statement")


class ReplicationTransport(PyCelonisBaseModel):
    cl_override_enabled: Optional['bool'] = Field(alias="clOverrideEnabled")
    cl_override_table_name: Optional['str'] = Field(alias="clOverrideTableName")
    connector_specific_configuration: Optional['List[Optional[TableConfigurationParameterValue]]'] = Field(
        alias="connectorSpecificConfiguration"
    )
    custom_target_table_name: Optional['str'] = Field(alias="customTargetTableName")
    data_pool_id: Optional['str'] = Field(alias="dataPoolId")
    data_push_delete_strategy: Optional['DataPushDeleteStrategy'] = Field(alias="dataPushDeleteStrategy")
    data_source_id: Optional['str'] = Field(alias="dataSourceId")
    debug_mode_until: Optional['datetime'] = Field(alias="debugModeUntil")
    filter_statement: Optional['str'] = Field(alias="filterStatement")
    id: Optional['str'] = Field(alias="id")
    latest_status_change: Optional['datetime'] = Field(alias="latestStatusChange")
    metadata_delta_filter_id: Optional['str'] = Field(alias="metadataDeltaFilterId")
    rename_target_table: Optional['bool'] = Field(alias="renameTargetTable")
    schema_name: Optional['str'] = Field(alias="schemaName")
    status: Optional['ReplicationStatus'] = Field(alias="status")
    table_name: Optional['str'] = Field(alias="tableName")
    type_: Optional['ReplicationType'] = Field(alias="type")


class TableConfigurationParameterValue(PyCelonisBaseModel):
    key: Optional['TableConfigurationParameterKey'] = Field(alias="key")
    value: Optional['Any'] = Field(alias="value")


class TranslatedConnectorMessage(PyCelonisBaseModel):
    log_translation_parameters: Optional['List[Optional[LogTranslationParameter]]'] = Field(
        alias="logTranslationParameters"
    )
    message_translation_code: Optional['ExecutionMessageCode'] = Field(alias="messageTranslationCode")


class VariableSettingsTransport(PyCelonisBaseModel):
    pool_variable_id: Optional['str'] = Field(alias="poolVariableId")


class VariableValueTransport(PyCelonisBaseModel):
    task_instance_id: Optional['str'] = Field(alias="taskInstanceId")
    value: Optional['str'] = Field(alias="value")


DataSourceAvailableTables.update_forward_refs()
DataSourceTable.update_forward_refs()
ExceptionReference.update_forward_refs()
FrontendHandledBackendError.update_forward_refs()
InitializationSelectionTransport.update_forward_refs()
LogTranslationParameter.update_forward_refs()
PoolVariableTransport.update_forward_refs()
ReplicationCalculatedColumnTransport.update_forward_refs()
ReplicationColumnTransport.update_forward_refs()
ReplicationConfigurationTransport.update_forward_refs()
ReplicationDependencyTransport.update_forward_refs()
ReplicationInitializationScriptTransport.update_forward_refs()
ReplicationInitializationSelectionTransport.update_forward_refs()
ReplicationJoinTransport.update_forward_refs()
ReplicationTransformationTransport.update_forward_refs()
ReplicationTransport.update_forward_refs()
TableConfigurationParameterValue.update_forward_refs()
TranslatedConnectorMessage.update_forward_refs()
VariableSettingsTransport.update_forward_refs()
VariableValueTransport.update_forward_refs()


class ReplicationService:
    @staticmethod
    def get_api_pools_pool_id_data_sources_data_source_id_initialization_scripts(
        client: Client, pool_id: str, data_source_id: str
    ) -> List[Optional[ReplicationInitializationScriptTransport]]:
        logger.debug(
            f"Request: 'GET' -> '/realtime-integration/api/pools/{pool_id}/data-sources/{data_source_id}/initialization-scripts'",
            extra={
                "request_type": "GET",
                "path": "/realtime-integration/api/pools/{pool_id}/data-sources/{data_source_id}/initialization-scripts",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="GET",
            url=f"/realtime-integration/api/pools/{pool_id}/data-sources/{data_source_id}/initialization-scripts",
            parse_json=True,
            type_=List[Optional[ReplicationInitializationScriptTransport]],
        )

    @staticmethod
    def get_api_pools_pool_id_data_sources_data_source_id_search_tables(
        client: Client,
        pool_id: str,
        data_source_id: str,
        replication_id: Optional['str'] = None,
        search_string: Optional['str'] = None,
    ) -> DataSourceAvailableTables:
        logger.debug(
            f"Request: 'GET' -> '/realtime-integration/api/pools/{pool_id}/data-sources/{data_source_id}/search-tables'",
            extra={
                "request_type": "GET",
                "path": "/realtime-integration/api/pools/{pool_id}/data-sources/{data_source_id}/search-tables",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if replication_id:
            params["replicationId"] = replication_id
        if search_string:
            params["searchString"] = search_string
        return client.request(
            method="GET",
            url=f"/realtime-integration/api/pools/{pool_id}/data-sources/{data_source_id}/search-tables",
            params=params,
            parse_json=True,
            type_=DataSourceAvailableTables,
        )

    @staticmethod
    def get_api_pools_pool_id_replications(
        client: Client, pool_id: str, data_source_id: Optional['str'] = None, changed_after: Optional['int'] = None
    ) -> List[Optional[ReplicationTransport]]:
        logger.debug(
            f"Request: 'GET' -> '/realtime-integration/api/pools/{pool_id}/replications'",
            extra={
                "request_type": "GET",
                "path": "/realtime-integration/api/pools/{pool_id}/replications",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if data_source_id:
            params["dataSourceId"] = data_source_id
        if changed_after:
            params["changedAfter"] = changed_after
        return client.request(
            method="GET",
            url=f"/realtime-integration/api/pools/{pool_id}/replications",
            params=params,
            parse_json=True,
            type_=List[Optional[ReplicationTransport]],
        )

    @staticmethod
    def post_api_pools_pool_id_replications(
        client: Client, pool_id: str, request_body: List[Optional[ReplicationTransport]]
    ) -> List[Optional[ReplicationTransport]]:
        logger.debug(
            f"Request: 'POST' -> '/realtime-integration/api/pools/{pool_id}/replications'",
            extra={
                "request_type": "POST",
                "path": "/realtime-integration/api/pools/{pool_id}/replications",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/realtime-integration/api/pools/{pool_id}/replications",
            request_body=request_body,
            parse_json=True,
            type_=List[Optional[ReplicationTransport]],
        )

    @staticmethod
    def delete_api_pools_pool_id_replications(
        client: Client, pool_id: str, data_source_id: Optional['str'] = None
    ) -> None:
        logger.debug(
            f"Request: 'DELETE' -> '/realtime-integration/api/pools/{pool_id}/replications'",
            extra={
                "request_type": "DELETE",
                "path": "/realtime-integration/api/pools/{pool_id}/replications",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if data_source_id:
            params["dataSourceId"] = data_source_id
        return client.request(
            method="DELETE",
            url=f"/realtime-integration/api/pools/{pool_id}/replications",
            params=params,
        )

    @staticmethod
    def get_api_pools_pool_id_replications_configurations_replication_id(
        client: Client, pool_id: str, replication_id: str
    ) -> ReplicationConfigurationTransport:
        logger.debug(
            f"Request: 'GET' -> '/realtime-integration/api/pools/{pool_id}/replications/configurations/{replication_id}'",
            extra={
                "request_type": "GET",
                "path": "/realtime-integration/api/pools/{pool_id}/replications/configurations/{replication_id}",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="GET",
            url=f"/realtime-integration/api/pools/{pool_id}/replications/configurations/{replication_id}",
            parse_json=True,
            type_=ReplicationConfigurationTransport,
        )

    @staticmethod
    def put_api_pools_pool_id_replications_configurations_replication_id(
        client: Client,
        pool_id: str,
        replication_id: str,
        request_body: ReplicationConfigurationTransport,
        validate: Optional['bool'] = None,
    ) -> ReplicationConfigurationTransport:
        logger.debug(
            f"Request: 'PUT' -> '/realtime-integration/api/pools/{pool_id}/replications/configurations/{replication_id}'",
            extra={
                "request_type": "PUT",
                "path": "/realtime-integration/api/pools/{pool_id}/replications/configurations/{replication_id}",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if validate:
            params["validate"] = validate
        return client.request(
            method="PUT",
            url=f"/realtime-integration/api/pools/{pool_id}/replications/configurations/{replication_id}",
            params=params,
            request_body=request_body,
            parse_json=True,
            type_=ReplicationConfigurationTransport,
        )

    @staticmethod
    def post_api_pools_pool_id_replications_initialize(
        client: Client, pool_id: str, request_body: InitializationSelectionTransport
    ) -> List[Optional[ReplicationTransport]]:
        logger.debug(
            f"Request: 'POST' -> '/realtime-integration/api/pools/{pool_id}/replications/initialize'",
            extra={
                "request_type": "POST",
                "path": "/realtime-integration/api/pools/{pool_id}/replications/initialize",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/realtime-integration/api/pools/{pool_id}/replications/initialize",
            request_body=request_body,
            parse_json=True,
            type_=List[Optional[ReplicationTransport]],
        )

    @staticmethod
    def post_api_pools_pool_id_replications_start(
        client: Client, pool_id: str, data_source_id: Optional['str'] = None
    ) -> List[Optional[ReplicationTransport]]:
        logger.debug(
            f"Request: 'POST' -> '/realtime-integration/api/pools/{pool_id}/replications/start'",
            extra={
                "request_type": "POST",
                "path": "/realtime-integration/api/pools/{pool_id}/replications/start",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if data_source_id:
            params["dataSourceId"] = data_source_id
        return client.request(
            method="POST",
            url=f"/realtime-integration/api/pools/{pool_id}/replications/start",
            params=params,
            parse_json=True,
            type_=List[Optional[ReplicationTransport]],
        )

    @staticmethod
    def post_api_pools_pool_id_replications_stop(
        client: Client, pool_id: str, data_source_id: Optional['str'] = None
    ) -> List[Optional[ReplicationTransport]]:
        logger.debug(
            f"Request: 'POST' -> '/realtime-integration/api/pools/{pool_id}/replications/stop'",
            extra={
                "request_type": "POST",
                "path": "/realtime-integration/api/pools/{pool_id}/replications/stop",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if data_source_id:
            params["dataSourceId"] = data_source_id
        return client.request(
            method="POST",
            url=f"/realtime-integration/api/pools/{pool_id}/replications/stop",
            params=params,
            parse_json=True,
            type_=List[Optional[ReplicationTransport]],
        )

    @staticmethod
    def post_api_pools_pool_id_replications_stop_and_cancel(
        client: Client, pool_id: str, data_source_id: Optional['str'] = None
    ) -> List[Optional[ReplicationTransport]]:
        logger.debug(
            f"Request: 'POST' -> '/realtime-integration/api/pools/{pool_id}/replications/stop-and-cancel'",
            extra={
                "request_type": "POST",
                "path": "/realtime-integration/api/pools/{pool_id}/replications/stop-and-cancel",
                "tracking_type": "API_REQUEST",
            },
        )

        params: Dict[str, Any] = {}
        if data_source_id:
            params["dataSourceId"] = data_source_id
        return client.request(
            method="POST",
            url=f"/realtime-integration/api/pools/{pool_id}/replications/stop-and-cancel",
            params=params,
            parse_json=True,
            type_=List[Optional[ReplicationTransport]],
        )

    @staticmethod
    def get_api_pools_pool_id_replications_replication_id(
        client: Client, pool_id: str, replication_id: str
    ) -> ReplicationTransport:
        logger.debug(
            f"Request: 'GET' -> '/realtime-integration/api/pools/{pool_id}/replications/{replication_id}'",
            extra={
                "request_type": "GET",
                "path": "/realtime-integration/api/pools/{pool_id}/replications/{replication_id}",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="GET",
            url=f"/realtime-integration/api/pools/{pool_id}/replications/{replication_id}",
            parse_json=True,
            type_=ReplicationTransport,
        )

    @staticmethod
    def delete_api_pools_pool_id_replications_replication_id(client: Client, pool_id: str, replication_id: str) -> None:
        logger.debug(
            f"Request: 'DELETE' -> '/realtime-integration/api/pools/{pool_id}/replications/{replication_id}'",
            extra={
                "request_type": "DELETE",
                "path": "/realtime-integration/api/pools/{pool_id}/replications/{replication_id}",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="DELETE",
            url=f"/realtime-integration/api/pools/{pool_id}/replications/{replication_id}",
        )

    @staticmethod
    def post_api_pools_pool_id_replications_replication_id_start(
        client: Client, pool_id: str, replication_id: str
    ) -> ReplicationTransport:
        logger.debug(
            f"Request: 'POST' -> '/realtime-integration/api/pools/{pool_id}/replications/{replication_id}/start'",
            extra={
                "request_type": "POST",
                "path": "/realtime-integration/api/pools/{pool_id}/replications/{replication_id}/start",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/realtime-integration/api/pools/{pool_id}/replications/{replication_id}/start",
            parse_json=True,
            type_=ReplicationTransport,
        )

    @staticmethod
    def post_api_pools_pool_id_replications_replication_id_stop(
        client: Client, pool_id: str, replication_id: str
    ) -> ReplicationTransport:
        logger.debug(
            f"Request: 'POST' -> '/realtime-integration/api/pools/{pool_id}/replications/{replication_id}/stop'",
            extra={
                "request_type": "POST",
                "path": "/realtime-integration/api/pools/{pool_id}/replications/{replication_id}/stop",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="POST",
            url=f"/realtime-integration/api/pools/{pool_id}/replications/{replication_id}/stop",
            parse_json=True,
            type_=ReplicationTransport,
        )

    @staticmethod
    def get_api_pools_pool_id_variables(client: Client, pool_id: str) -> List[Optional[PoolVariableTransport]]:
        logger.debug(
            f"Request: 'GET' -> '/realtime-integration/api/pools/{pool_id}/variables/'",
            extra={
                "request_type": "GET",
                "path": "/realtime-integration/api/pools/{pool_id}/variables/",
                "tracking_type": "API_REQUEST",
            },
        )

        return client.request(
            method="GET",
            url=f"/realtime-integration/api/pools/{pool_id}/variables/",
            parse_json=True,
            type_=List[Optional[PoolVariableTransport]],
        )
