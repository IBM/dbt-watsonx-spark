import os
import re
from concurrent.futures import Future
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Union,
    Type,
    Tuple,
    Callable,
    Set,
    FrozenSet,
)

from dbt.adapters.base.relation import InformationSchema
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError, CompilationError
from dbt_common.utils import AttrDict, executor

from typing_extensions import TypeAlias

import agate

from dbt.adapters.base import AdapterConfig, PythonJobHelper
from dbt.adapters.base.impl import catch_as_completed, ConstraintSupport
from dbt.adapters.watsonx_spark.catalog_utils import CatalogUtils
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.watsonx_spark import (
    SparkConnectionManager,
    SparkRelation,
    SparkColumn,
    SparkCredentials,
)
from dbt.adapters.watsonx_spark.python_submissions import (
    JobClusterPythonJobHelper,
    AllPurposeClusterPythonJobHelper,
)
from dbt.adapters.contracts.relation import RelationType, RelationConfig
from dbt_common.clients.agate_helper import DEFAULT_TYPE_TESTER
from dbt_common.contracts.constraints import ConstraintType
from dbt.adapters.base import BaseRelation, available

logger = AdapterLogger("Spark")
packages = ["pyhive.hive", "thrift.transport", "thrift.protocol"]
log_level = os.getenv("DBT_SPARK_LOG_LEVEL", "ERROR")
for package in packages:
    logger.debug(f"Setting {package} logging to {log_level}")
    logger.set_adapter_dependency_log_level(package, log_level)

GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME = "get_columns_in_relation_raw"
LIST_SCHEMAS_MACRO_NAME = "list_schemas"
LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"
LIST_RELATIONS_SHOW_TABLES_MACRO_NAME = "list_relations_show_tables_without_caching"
DESCRIBE_TABLE_EXTENDED_MACRO_NAME = "describe_table_extended_without_caching"
DESCRIBE_TABLE_MACRO_NAME = "describe_table_without_caching"
CREATE_SCHEMA_MACRO_NAME = "create_schema"
CREATE_TABLE_MACRO_NAME = "create_table_as"

KEY_TABLE_OWNER = "Owner"
KEY_TABLE_STATISTICS = "Statistics"

TABLE_OR_VIEW_NOT_FOUND_MESSAGES = (
    "[TABLE_OR_VIEW_NOT_FOUND]",
    "Table or view not found",
    "NoSuchTableException",
)
HEADER_KEYS = ("Type:", "Provider:", "Location:", "Owner:", "Statistics:")

@dataclass
class SparkConfig(AdapterConfig):
    file_format: str = "parquet"
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None
    catalog: Optional[str] = None


class WatsonxSparkAdapter(SQLAdapter):
    COLUMN_NAMES = (
        "table_database",
        "table_schema",
        "table_name",
        "table_type",
        "table_comment",
        "table_owner",
        "column_name",
        "column_index",
        "column_type",
        "column_comment",
        "stats:bytes:label",
        "stats:bytes:value",
        "stats:bytes:description",
        "stats:bytes:include",
        "stats:rows:label",
        "stats:rows:value",
        "stats:rows:description",
        "stats:rows:include",
    )
    INFORMATION_COLUMNS_REGEX = re.compile(r"^ \|-- (.*): (.*) \(nullable = (.*)\b", re.MULTILINE)
    INFORMATION_OWNER_REGEX = re.compile(r"^Owner: (.*)$", re.MULTILINE)
    INFORMATION_STATISTICS_REGEX = re.compile(r"^Statistics: (.*)$", re.MULTILINE)

    HUDI_METADATA_COLUMNS = [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
    ]

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.not_null: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_ENFORCED,
    }

    Relation: TypeAlias = SparkRelation
    RelationInfo = Tuple[str, str, str]
    Column: TypeAlias = SparkColumn
    ConnectionManager: TypeAlias = SparkConnectionManager
    AdapterSpecificConfigs: TypeAlias = SparkConfig

    @classmethod
    def get_adapter_run_info(cls, config):
        """Override to use correct adapter name with underscore."""
        from importlib import import_module
        from dbt.adapters.base import AdapterTrackingRelationInfo
        
        # Use the correct adapter name that matches our package directory
        adapter_name = "watsonx_spark"
        adapter_version = import_module(f"dbt.adapters.{adapter_name}.__version__").version
        
        return AdapterTrackingRelationInfo(
            adapter_name=adapter_name,
            base_adapter_version=import_module("dbt.adapters.__about__").version,
            adapter_version=adapter_version,
            model_adapter_details=cls._get_adapter_specific_run_info(config),
        )

    @classmethod
    def date_function(cls) -> str:
        return "current_timestamp()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "bigint"

    @classmethod
    def convert_integer_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "bigint"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "time"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    def quote(self, identifier: str) -> str:
        return "`{}`".format(identifier)

    def _get_relation_information(self, row: agate.Row) -> RelationInfo:
        """relation info was fetched with SHOW TABLES EXTENDED"""
        try:
            
            _schema, name, _, information = row

        except ValueError:
            raise DbtRuntimeError(
                f'Invalid value from "show tables extended ...", got {len(row)} values, expected 4'
            )

        return _schema, name, information

    def _get_relation_information_using_describe(self, row: agate.Row) -> RelationInfo:
        """Relation info fetched using SHOW TABLES and an auxiliary DESCRIBE statement"""
        try:
            _schema, name, _ = row
        except ValueError:
            raise DbtRuntimeError(
                f'Invalid value from "show tables ...", got {len(row)} values, expected 3'
            )

        # Check if identifier quoting is enabled (default: True)
        creds = self.connections.get_thread_connection().credentials
        quote_identifiers = getattr(creds, 'quote_identifiers', True)
        
        # FALLBACK FIX for Spark SQL ShowTablesExec bug (SPARK-XXXXX)
        # When SHOW TABLES IN catalog.schema returns only "schema" instead of "catalog.schema",
        # we need to preserve the full catalog path from the original query context.
        # This is a temporary workaround until the server-side fix is deployed.
        context_schema = getattr(self, '_list_relations_schema_context', None)
        full_schema = CatalogUtils.normalize_schema_from_show_tables(_schema, context_schema)
        
        if full_schema != _schema:
            logger.debug(
                f"[CATALOG_FIX] SHOW TABLES returned schema='{_schema}', "
                f"using full context='{full_schema}' for table '{name}'"
            )
        
        # Quote schema and table name separately to handle special characters
        if quote_identifiers:
            table_name = CatalogUtils.quote_schema_table(full_schema, name)
        else:
            table_name = f"{full_schema}.{name}"
        
        # Try DESCRIBE EXTENDED first (works for v1 tables, provides more metadata)
        # If it fails for ANY reason, fall back to DESCRIBE TABLE (works for v2 tables)
        try:
            table_results = self.execute_macro(
                DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs={"table_name": table_name}
            )
            logger.debug(f"DESCRIBE EXTENDED succeeded for {table_name}")
        except DbtRuntimeError as e:
            errmsg = getattr(e, "msg", "").lower()
            
            # For ANY DESCRIBE EXTENDED failure, try DESCRIBE TABLE (without EXTENDED)
            # This handles:
            # - v2 tables (Iceberg, Delta, etc.)
            # - Tables with special characters or naming issues
            # - Any other DESCRIBE EXTENDED incompatibilities
            logger.debug(f"DESCRIBE EXTENDED failed for {table_name}, trying DESCRIBE TABLE: {errmsg[:100]}")
            try:
                table_results = self.execute_macro(
                    DESCRIBE_TABLE_MACRO_NAME, kwargs={"table_name": table_name}
                )
                logger.debug(f"DESCRIBE TABLE succeeded for {table_name}")
            except DbtRuntimeError as e2:
                # If both DESCRIBE commands fail, return empty results
                # The table will still be listed, just without detailed metadata
                logger.debug(f"Both DESCRIBE commands failed for {table_name}: {e2.msg[:100]}")
                table_results = AttrDict()

        information = ""
        for info_row in table_results:
            info_type, info_value, _ = info_row
            if not info_type.startswith("#"):
                information += f"{info_type}: {info_value}\n"

        return _schema, name, information

    def _build_spark_relation_list(
        self,
        row_list: agate.Table,
        relation_info_func: Callable[[agate.Row], RelationInfo],
    ) -> List[SparkRelation]:
        """Aggregate relations with format metadata included."""
        relations = []
        for row in row_list:
            _schema, name, information = relation_info_func(row)

            rel_type: RelationType = (
                RelationType.View if "Type: VIEW" in information else RelationType.Table
            )
            is_delta: bool = "Provider: delta" in information
            is_hudi: bool = "Provider: hudi" in information
            is_iceberg: bool = "Provider: iceberg" in information

            relation: BaseRelation = self.Relation.create(
                schema=_schema,
                identifier=name,
                type=rel_type,
                information=information,
                is_delta=is_delta,
                is_iceberg=is_iceberg,
                is_hudi=is_hudi,
            )
            relations.append(relation)

        return relations

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        """Distinct Spark compute engines may not support the same SQL featureset. Thus, we must
        try different methods to fetch relation information.
        
        Strategy (OPTIMIZED WITH FALLBACK):
        1. Try SHOW TABLE EXTENDED first (fast for v1 tables - gets all metadata in one query)
        2. If it fails (v2 tables), fallback to SHOW TABLES + per-table DESCRIBE
        
        This ensures optimal performance:
        - v1 schemas: Fast (one SHOW TABLE EXTENDED query)
        - v2 schemas: Works (fallback to SHOW TABLES + per-table DESCRIBE)
        - Mixed schemas: Works (fallback handles both)
        """

        kwargs = {"schema_relation": schema_relation}
        
        # Store schema context for fallback fix in _get_relation_information_using_describe
        # This preserves the full catalog.schema path when SHOW TABLES loses catalog context
        self._list_relations_schema_context = schema_relation.schema
        
        logger.debug(f"[LIST_RELATIONS] Starting - schema_relation: {schema_relation}")
        logger.debug(f"[LIST_RELATIONS] schema_relation.schema: {schema_relation.schema}")
        logger.debug(f"[LIST_RELATIONS] schema_relation.database: {schema_relation.database}")
        
        # Try 2-part name first (schema only), then fallback to 3-part (catalog.schema)
        schema_to_try = schema_relation.schema
        has_catalog = CatalogUtils.has_catalog_prefix(schema_relation.schema)
        
        logger.debug(f"[LIST_RELATIONS] has_catalog: {has_catalog}")
        
        if has_catalog:
            # Extract 2-part name (schema only) to try first
            schema_to_try = CatalogUtils.get_schema_only(schema_relation.schema)
            logger.debug(f"[LIST_RELATIONS] Schema has catalog prefix, trying 2-part name first")
            logger.debug(f"[LIST_RELATIONS] Original schema: {schema_relation.schema}")
            logger.debug(f"[LIST_RELATIONS] 2-part schema to try: {schema_to_try}")
        
        try:
            # Try SHOW TABLE EXTENDED with 2-part name first (fast path for v1 tables)
            schema_relation_2part = schema_relation.incorporate(path={"schema": schema_to_try})
            logger.debug(f"[LIST_RELATIONS] Created 2-part relation: {schema_relation_2part}")
            logger.debug(f"[LIST_RELATIONS] 2-part relation.schema: {schema_relation_2part.schema}")
            
            kwargs_2part = {"schema_relation": schema_relation_2part}
            logger.debug(f"[LIST_RELATIONS] Calling LIST_RELATIONS_MACRO with 2-part name")
            
            show_table_extended_rows = self.execute_macro(
                LIST_RELATIONS_MACRO_NAME, kwargs=kwargs_2part
            )
            logger.debug(f"[LIST_RELATIONS] SHOW TABLE EXTENDED (2-part) succeeded, got {len(show_table_extended_rows) if show_table_extended_rows else 0} rows")
            
            # Build relations from SHOW TABLE EXTENDED results
            logger.debug(f"[LIST_RELATIONS] Building relations from {len(show_table_extended_rows) if show_table_extended_rows else 0} rows")
            rels = self._build_spark_relation_list(
                row_list=show_table_extended_rows,
                relation_info_func=self._get_relation_information,
            )
            logger.debug(f"[LIST_RELATIONS] Built {len(rels)} relations")
            
            # Restore full catalog.schema if it was present
            if has_catalog:
                logger.debug(f"[LIST_RELATIONS] Restoring full catalog.schema to relations")
                logger.debug(f"[LIST_RELATIONS] Original schema to restore: {schema_relation.schema}")
                rels_before = [f"{r.schema}.{r.identifier}" for r in rels]
                rels = [r.incorporate(path={"schema": schema_relation.schema}) for r in rels]
                rels_after = [f"{r.schema}.{r.identifier}" for r in rels]
                logger.debug(f"[LIST_RELATIONS] Relations before restore: {rels_before}")
                logger.debug(f"[LIST_RELATIONS] Relations after restore: {rels_after}")
            
            logger.info(f"[LIST_RELATIONS] Listed {len(rels)} relations using SHOW TABLE EXTENDED (2-part)")
            return rels
            
        except DbtRuntimeError as e_2part:
            # 2-part name failed, try 3-part name if catalog prefix exists
            if has_catalog:
                logger.debug(f"[LIST_RELATIONS] SHOW TABLE EXTENDED (2-part) failed")
                logger.debug(f"[LIST_RELATIONS] Error: {str(e_2part)[:200]}")
                logger.debug(f"[LIST_RELATIONS] Trying 3-part name: {schema_relation.schema}")
                logger.debug(f"[LIST_RELATIONS] Original kwargs schema: {kwargs['schema_relation'].schema}")
                
                try:
                    logger.debug(f"[LIST_RELATIONS] Calling LIST_RELATIONS_MACRO with 3-part name")
                    show_table_extended_rows = self.execute_macro(
                        LIST_RELATIONS_MACRO_NAME, kwargs=kwargs
                    )
                    logger.debug(f"[LIST_RELATIONS] SHOW TABLE EXTENDED (3-part) succeeded, got {len(show_table_extended_rows) if show_table_extended_rows else 0} rows")
                    
                    # Build relations from SHOW TABLE EXTENDED results
                    logger.debug(f"[LIST_RELATIONS] Building relations from 3-part query results")
                    rels = self._build_spark_relation_list(
                        row_list=show_table_extended_rows,
                        relation_info_func=self._get_relation_information,
                    )
                    # Schema already has catalog prefix, no need to restore
                    logger.debug(f"[LIST_RELATIONS] Built {len(rels)} relations from 3-part query")
                    rels_list = [f"{r.schema}.{r.identifier}" for r in rels]
                    logger.debug(f"[LIST_RELATIONS] Relations from 3-part: {rels_list}")
                    
                    logger.info(f"[LIST_RELATIONS] Listed {len(rels)} relations using SHOW TABLE EXTENDED (3-part)")
                    return rels
                    
                except DbtRuntimeError as e_3part:
                    # Both 2-part and 3-part failed, continue to SHOW TABLES fallback
                    logger.debug(f"[LIST_RELATIONS] SHOW TABLE EXTENDED (3-part) also failed")
                    logger.debug(f"[LIST_RELATIONS] Error: {str(e_3part)[:200]}")
                    logger.debug(f"[LIST_RELATIONS] Falling back to SHOW TABLES")
                    e = e_3part
            else:
                # No catalog prefix, can't try 3-part
                e = e_2part
            
        # SHOW TABLE EXTENDED failed (likely v2 tables), fallback to SHOW TABLES
        errmsg = getattr(e, "msg", "").lower()
        logger.debug(f"SHOW TABLE EXTENDED failed, falling back to SHOW TABLES + per-table DESCRIBE")
        
        # Try 2-part name first, then fallback to 3-part
        try:
            # Try SHOW TABLES with 2-part name first
            kwargs_2part_show = {"schema_relation": schema_relation.incorporate(path={"schema": schema_to_try})}
            show_table_rows = self.execute_macro(
                LIST_RELATIONS_SHOW_TABLES_MACRO_NAME, kwargs=kwargs_2part_show
            )
            logger.debug(f"SHOW TABLES (2-part) succeeded, got {len(show_table_rows) if show_table_rows else 0} rows")
            
            # Use per-table DESCRIBE (tries DESCRIBE EXTENDED first, then DESCRIBE)
            rels = self._build_spark_relation_list(
                row_list=show_table_rows,
                relation_info_func=self._get_relation_information_using_describe,
            )
            # Restore full catalog.schema if it was present
            if has_catalog:
                rels = [r.incorporate(path={"schema": schema_relation.schema}) for r in rels]
            
            logger.info(f"Listed {len(rels)} relations using SHOW TABLES (2-part) + per-table DESCRIBE")
            return rels
            
        except DbtRuntimeError as e2_2part:
            # 2-part SHOW TABLES failed, try 3-part if catalog prefix exists
            if has_catalog:
                logger.debug(f"SHOW TABLES (2-part) failed, trying 3-part name: {schema_relation.schema}")
                try:
                    show_table_rows = self.execute_macro(
                        LIST_RELATIONS_SHOW_TABLES_MACRO_NAME, kwargs=kwargs
                    )
                    logger.debug(f"SHOW TABLES (3-part) succeeded, got {len(show_table_rows) if show_table_rows else 0} rows")
                    
                    # Use per-table DESCRIBE (tries DESCRIBE EXTENDED first, then DESCRIBE)
                    rels = self._build_spark_relation_list(
                        row_list=show_table_rows,
                        relation_info_func=self._get_relation_information_using_describe,
                    )
                    # Schema already has catalog prefix, no need to restore
                    
                    logger.info(f"Listed {len(rels)} relations using SHOW TABLES (3-part) + per-table DESCRIBE")
                    return rels
                    
                except DbtRuntimeError as e2_3part:
                    # Both 2-part and 3-part SHOW TABLES failed
                    errmsg2 = getattr(e2_3part, "msg", "").lower()
                    description = "Error while retrieving information about"
                    logger.debug(f"SHOW TABLES (3-part) also failed for {schema_relation}: {errmsg2[:200]}")
                    logger.debug(f"{description} {schema_relation}: {e2_3part.msg}")
                    e2 = e2_3part
            else:
                # No catalog prefix, can't try 3-part
                errmsg2 = getattr(e2_2part, "msg", "").lower()
                description = "Error while retrieving information about"
                logger.debug(f"SHOW TABLES failed for {schema_relation}: {errmsg2[:200]}")
                logger.debug(f"{description} {schema_relation}: {e2_2part.msg}")
                e2 = e2_2part
            
            # Check if database doesn't exist
            if f"database '{schema_relation}' not found" in errmsg2:
                logger.debug(f"Database not found, returning empty list")
                
                return []

    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        if not self.Relation.get_default_include_policy().database:
            database = None  # type: ignore

        return super().get_relation(database, schema, identifier)

    def parse_describe_extended(
        self, relation: BaseRelation, raw_rows: AttrDict
    ) -> List[SparkColumn]:
        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]
        # Find the separator between the rows and the metadata provided
        # by the DESCRIBE TABLE EXTENDED statement
        pos = self.find_table_information_separator(dict_rows)

        # Remove rows that start with a hash, they are comments
        rows = [row for row in raw_rows[0:pos] if not row["col_name"].startswith("#")]
        metadata = {col["col_name"]: col["data_type"] for col in raw_rows[pos + 1 :]}

        raw_table_stats = metadata.get(KEY_TABLE_STATISTICS)
        table_stats = SparkColumn.convert_table_stats(raw_table_stats)
        return [
            SparkColumn(
                table_database=None,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=str(metadata.get(KEY_TABLE_OWNER)),
                table_stats=table_stats,
                column=column["col_name"],
                column_index=idx,
                dtype=column["data_type"],
            )
            for idx, column in enumerate(rows)
        ]

    @staticmethod
    def find_table_information_separator(rows: List[dict]) -> int:
        pos = 0
        for row in rows:
            if not row["col_name"] or row["col_name"].startswith("#"):
                break
            pos += 1
        return pos

    def get_columns_in_relation(self, relation: BaseRelation) -> List[SparkColumn]:
        columns = []
        try:
            rows: AttrDict = self.execute_macro(
                GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME, kwargs={"relation": relation}
            )
            columns = self.parse_describe_extended(relation, rows)
        except DbtRuntimeError as e:
            # spark would throw error when table doesn't exist, where other
            # CDW would just return and empty list, normalizing the behavior here
            errmsg = getattr(e, "msg", "")
            found_msgs = (msg in errmsg for msg in TABLE_OR_VIEW_NOT_FOUND_MESSAGES)
            if any(found_msgs):
                pass
            else:
                raise e

        # strip hudi metadata columns.
        columns = [x for x in columns if x.name not in self.HUDI_METADATA_COLUMNS]
        return columns

    def _get_active_credentials(self) -> SparkCredentials:
        try:
            return self.connections.get_thread_connection().credentials
        except Exception:
            return self.config.credentials

    @available
    def should_create_schema(self, config: Optional[Any] = None) -> bool:
        """Check if schemas should be created automatically.
        
        Configuration hierarchy: model config → profile config → adapter default (True)
        """
        # Check model-level config first
        if config is not None and "create_schemas" in config:
            return bool(config.get("create_schemas"))
        
        # Fall back to profile-level config
        creds = self._get_active_credentials()
        profile_value = getattr(creds, "create_schemas", None)
        if profile_value is not None:
            return bool(profile_value)
        
        # Default to True (create schemas)
        return True

    @available
    def should_set_location(self, config: Optional[Any] = None) -> bool:
        """Check if LOCATION clause should be added to CREATE statements.
        
        Configuration hierarchy: model config → profile config → adapter default (False)
        """
        # Check model-level config first
        if config is not None:
            try:
                # Try to get auto_location from config (handles both dict and RuntimeConfigObject)
                auto_location_value = config.get("auto_location") if hasattr(config, "get") else getattr(config, "auto_location", None)
                if auto_location_value is not None:
                    result = bool(auto_location_value)
                    logger.info(f"LOCATION clause {'enabled' if result else 'disabled'} (auto_location from model config)")
                    return result
            except (AttributeError, TypeError):
                pass
        
        # Fall back to profile-level config
        creds = self._get_active_credentials()
        profile_value = getattr(creds, "auto_location", None)
        if profile_value is not None:
            result = bool(profile_value)
            logger.info(f"LOCATION clause {'enabled' if result else 'disabled'} (auto_location from profile config)")
            return result
        
        # Default to False (no LOCATION clause)
        logger.info("LOCATION clause disabled (auto_location default)")
        return False


    def create_schema(self, relation: SparkRelation) -> None:
        if not self.should_create_schema():
            logger.info(f"Skipping schema creation for {relation} (create_schemas is disabled)")
            return
        
        relation = relation.without_identifier()
        kwargs = {
            "relation": relation
        }
        self.execute_macro(CREATE_SCHEMA_MACRO_NAME, kwargs=kwargs)
        self.commit_if_has_connection()

    @available.parse_none
    def set_location_root(self, relation: SparkRelation, config: SparkConfig) -> Optional[str]:
        if not self.should_set_location(config):
            return None

        profile_cred: SparkCredentials = self.connections.get_thread_connection().credentials
        profile_location_root = self.validate_location(profile_cred.location_root)
        model_location_root = self.validate_location(config.get("location_root"))

        if model_location_root is not None: #Model location root 
            relation.set_location(model_location_root)
            return model_location_root

        if profile_location_root is not None: #Profile location root
            relation.set_location(profile_location_root)
            return profile_location_root
        
        api_location = self.ConnectionManager.get_location_from_api(profile_cred)
        if api_location is not None:
            location_root, _ = self.get_location_format_api(profile_cred, config)
            relation.set_location(location_root)
            return location_root
        return None
    
    def get_location_format_api(
        self, profile_cred: SparkCredentials, config: SparkConfig
    ) -> Tuple[str, str]:
        catalog = self.set_catalog(config)
        api_location = self.ConnectionManager.get_location_from_api(profile_cred)
        if api_location is None:
            raise DbtRuntimeError("Failed to retrieve catalog location details from API")
        bucket, file_format = api_location
        if profile_cred.schema is None:
            raise DbtRuntimeError("Missing catalog, bucket, or schema when building location")
        location_root = self.build_location(bucket, catalog, profile_cred.schema)
        return location_root, file_format

    def validate_location(self, location_root: Optional[str]) -> Optional[str]:
        if location_root is not None and location_root != "":
            regex = re.compile("^'.*'$")
            if self.check_regex(regex, location_root):
                return location_root
            return f"'{location_root}'"
        return None

    def build_location(self, bucket: str, catalog: str, schema: str) -> str:
        # Extract schema name only (remove catalog prefix if present)
        schema_only = CatalogUtils.get_schema_only(schema)
        return f"'s3a://{bucket}/{catalog}/{schema_only}'"

    def check_regex(self, regex: Any, string: str) -> bool:
        if re.match(regex, string):
            return True
        return False
    
    @available.parse_none
    def set_configuration(self, config: SparkConfig) -> None:
        profile_cred: SparkCredentials = self.connections.get_thread_connection().credentials
        configuration = config.__dict__["model"].config
        location_root, file_format = self.get_location_format_api(profile_cred, config)
        configuration.__setitem__("location_root", location_root.replace("'", ""))
        if configuration.get("file_format") is None:
            configuration.__setitem__("file_format", file_format)
        if configuration.get("catalog") is None:
            configuration.__setitem__("catalog", self.set_catalog(config))
        config.__dict__["model"].config = configuration
        return config

    @available.parse_none
    def set_catalog(self, config: SparkConfig) -> str:
        cred: SparkCredentials = self.connections.get_thread_connection().credentials
        if config.get("catalog") is not None and config.get("catalog") != "":
            return config.get("catalog")

        if cred.catalog is not None and cred.catalog != "":
            return cred.catalog
        return ""

    def parse_columns_from_information(self, relation: BaseRelation) -> List[SparkColumn]:
        if hasattr(relation, "information"):
            information = relation.information or ""
        else:
            information = ""
        owner_match = re.findall(self.INFORMATION_OWNER_REGEX, information)
        owner = owner_match[0] if owner_match else None
        matches = re.finditer(self.INFORMATION_COLUMNS_REGEX, information)
        columns = []
        stats_match = re.findall(self.INFORMATION_STATISTICS_REGEX, information)
        raw_table_stats = stats_match[0] if stats_match else None
        table_stats = SparkColumn.convert_table_stats(raw_table_stats)
        for match_num, match in enumerate(matches):
            column_name, column_type, nullable = match.groups()
            column = SparkColumn(
                table_database=None,
                table_schema=relation.schema,
                table_name=relation.table,
                table_type=relation.type,
                column_index=match_num,
                table_owner=owner,
                column=column_name,
                dtype=column_type,
                table_stats=table_stats,
            )
            columns.append(column)
        return columns

    def _get_columns_for_catalog(self, relation: BaseRelation) -> Iterable[Dict[str, Any]]:
        columns = self.parse_columns_from_information(relation)

        for column in columns:
            # convert SparkColumns into catalog dicts
            as_dict = column.to_column_dict()
            as_dict["column_name"] = as_dict.pop("column", None)
            as_dict["column_type"] = as_dict.pop("dtype")
            as_dict["table_database"] = None
            yield as_dict

    def get_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> Tuple[agate.Table, List[Exception]]:
        schema_map = self._get_catalog_schemas(relation_configs)
        if len(schema_map) > 1:
            raise CompilationError(
                f"Expected only one database in get_catalog, found " f"{list(schema_map)}"
            )

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            relation_configs,
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> agate.Table:
        if len(schemas) != 1:
            raise CompilationError(
                f"Expected only one schema in spark _get_one_catalog, found " f"{schemas}"
            )

        database = information_schema.database
        schema = list(schemas)[0]

        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
            logger.debug("Getting table schema for relation {}", str(relation))
            columns.extend(self._get_columns_for_catalog(relation))
        return agate.Table.from_object(columns, column_types=DEFAULT_TYPE_TESTER)

    def check_schema_exists(self, database: str, schema: str) -> bool:
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def to_agate_table(self, rows_list: List[Tuple[str, str, bool, str]]) -> agate.Table:
        fixed = []
        for db, name, is_temp, info in rows_list:
            # Extract schema name only (remove catalog prefix if present)
            # Note: This is temporary - the full catalog.schema is restored later
            # in list_relations_without_caching via incorporate()
            schema = CatalogUtils.get_schema_only(db)
            fixed.append([schema, name, bool(is_temp), self.normalize_information(info)])
        return agate.Table(fixed, column_names=["database", "tableName", "isTemporary", "information"])

    def normalize_information(self, info: str) -> str:
        """Move header keys out of the schema block; keep proper EXTENDED shape."""
        lines = [ln.strip() for ln in info.splitlines() if ln.strip()]
        header, schema_lines = [], []
        in_schema = False

        for ln in lines:
            if ln.startswith("Schema:"):
                in_schema = True
                continue
            if in_schema:
                # Lines like "|-- col: type (nullable = true)"
                m = re.match(r"^\|--\s+(.*)$", ln)
                if m:
                    raw = m.group(1)
                    if any(raw.startswith(k) for k in HEADER_KEYS):
                        # turn "|-- Type: MANAGED (nullable = true)" -> "Type: MANAGED"
                        k, v = raw.split(":", 1)
                        header.append(f"{k.strip()}: {v.split('(nullable', 1)[0].strip()}")
                    else:
                        schema_lines.append(f" |-- {raw}")
                continue
            # Some DESCRIBE variants list header lines before "Schema:"
            if any(ln.startswith(k) for k in HEADER_KEYS):
                header.append(ln)

        # Compose canonical blob
        return "\n".join(header + ["Schema: root"] + schema_lines)
    

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql

    # This is for use in the test suite
    # Spark doesn't have 'commit' and 'rollback', so this override
    # doesn't include those commands.
    def run_sql_for_tests(self, sql, fetch, conn):  # type: ignore
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if fetch == "one":
                if hasattr(cursor, "fetchone"):
                    return cursor.fetchone()
                else:
                    # AttributeError: 'PyhiveConnectionWrapper' object has no attribute 'fetchone'
                    return cursor.fetchall()[0]
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return
        except BaseException as e:
            print(sql)
            print(e)
            raise
        finally:
            conn.transaction_open = False

    def generate_python_submission_response(self, submission_result: Any) -> AdapterResponse:
        return self.connections.get_response(None)

    @property
    def default_python_submission_method(self) -> str:
        return "all_purpose_cluster"

    @property
    def python_submission_helpers(self) -> Dict[str, Type[PythonJobHelper]]:
        return {
            "job_cluster": JobClusterPythonJobHelper,
            "all_purpose_cluster": AllPurposeClusterPythonJobHelper,
        }

    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        grants_dict: Dict[str, List[str]] = {}
        for row in grants_table:
            grantee = row["Principal"]
            privilege = row["ActionType"]
            object_type = row["ObjectType"]

            # we only want to consider grants on this object
            # (view or table both appear as 'TABLE')
            # and we don't want to consider the OWN privilege
            if object_type == "TABLE" and privilege != "OWN":
                if privilege in grants_dict.keys():
                    grants_dict[privilege].append(grantee)
                else:
                    grants_dict.update({privilege: [grantee]})
        return grants_dict

    def debug_query(self) -> None:
        """Override for DebugTask method"""
        self.execute("select 1 as id")


# spark does something interesting with joins when both tables have the same
# static values for the join condition and complains that the join condition is
# "trivial". Which is true, though it seems like an unreasonable cause for
# failure! It also doesn't like the `from foo, bar` syntax as opposed to
# `from foo cross join bar`.
COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} EXCEPT
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} EXCEPT
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a
    cross join table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
cross join diff_count
""".strip()
