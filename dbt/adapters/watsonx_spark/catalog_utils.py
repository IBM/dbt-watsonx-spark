"""
Utility functions for handling catalog.schema.table three-part names in Spark.

This module provides centralized functions for:
- Detecting catalog prefixes in schema names
- Splitting and extracting catalog/schema components
- Quoting identifiers with special characters
- Building fully qualified table names
"""

from typing import Tuple, Optional


class CatalogUtils:
    """Utilities for handling catalog.schema.table three-part names."""

    @staticmethod
    def has_catalog_prefix(schema: str) -> bool:
        """
        Check if schema contains a catalog prefix (catalog.schema format).
        
        Args:
            schema: Schema name, possibly with catalog prefix
            
        Returns:
            True if schema contains a dot (catalog.schema), False otherwise
            
        Examples:
            >>> CatalogUtils.has_catalog_prefix("iceberg_data.uk0429")
            True
            >>> CatalogUtils.has_catalog_prefix("uk0429")
            False
        """
        return "." in schema if schema else False

    @staticmethod
    def split_catalog_schema(schema: str) -> Tuple[str, str]:
        """
        Split catalog.schema into (catalog, schema) components.
        
        Args:
            schema: Schema name, possibly with catalog prefix
            
        Returns:
            Tuple of (catalog, schema). If no catalog prefix, returns ("", schema)
            
        Examples:
            >>> CatalogUtils.split_catalog_schema("iceberg_data.uk0429")
            ('iceberg_data', 'uk0429')
            >>> CatalogUtils.split_catalog_schema("uk0429")
            ('', 'uk0429')
        """
        if "." in schema:
            return tuple(schema.split(".", 1))  # type: ignore
        return ("", schema)

    @staticmethod
    def get_schema_only(schema: str) -> str:
        """
        Extract schema name from catalog.schema, removing catalog prefix if present.
        
        Args:
            schema: Schema name, possibly with catalog prefix
            
        Returns:
            Schema name without catalog prefix
            
        Examples:
            >>> CatalogUtils.get_schema_only("iceberg_data.uk0429")
            'uk0429'
            >>> CatalogUtils.get_schema_only("uk0429")
            'uk0429'
        """
        return schema.split(".", 1)[-1] if schema else schema

    @staticmethod
    def get_catalog_only(schema: str) -> str:
        """
        Extract catalog name from catalog.schema.
        
        Args:
            schema: Schema name, possibly with catalog prefix
            
        Returns:
            Catalog name if present, empty string otherwise
            
        Examples:
            >>> CatalogUtils.get_catalog_only("iceberg_data.uk0429")
            'iceberg_data'
            >>> CatalogUtils.get_catalog_only("uk0429")
            ''
        """
        if "." in schema:
            return schema.split(".", 1)[0]
        return ""

    @staticmethod
    def ensure_catalog_prefix(schema: str, catalog: str) -> str:
        """
        Ensure schema has catalog prefix, adding it if missing.
        
        Args:
            schema: Schema name, possibly with catalog prefix
            catalog: Catalog name to prepend if not present
            
        Returns:
            Schema with catalog prefix
            
        Examples:
            >>> CatalogUtils.ensure_catalog_prefix("uk0429", "iceberg_data")
            'iceberg_data.uk0429'
            >>> CatalogUtils.ensure_catalog_prefix("iceberg_data.uk0429", "iceberg_data")
            'iceberg_data.uk0429'
        """
        if not catalog:
            return schema
        if "." not in schema:
            return f"{catalog}.{schema}"
        return schema

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        """
        Quote a single identifier using backticks for Spark SQL.
        
        Args:
            identifier: Identifier to quote (schema, table, or column name)
            
        Returns:
            Quoted identifier
            
        Examples:
            >>> CatalogUtils.quote_identifier("my_table")
            '`my_table`'
            >>> CatalogUtils.quote_identifier("test-special-chars")
            '`test-special-chars`'
        """
        return f"`{identifier}`"

    @staticmethod
    def quote_schema_table(schema: str, table: str) -> str:
        """
        Quote schema.table for Spark SQL, handling catalog.schema.table format.
        
        This function properly quotes both the schema (which may contain catalog.schema)
        and the table name separately to handle special characters.
        
        When schema contains a catalog prefix (catalog.schema), each part is quoted
        separately to produce `catalog`.`schema`.`table` instead of `catalog.schema`.`table`.
        
        Args:
            schema: Schema name, possibly with catalog prefix (catalog.schema)
            table: Table name
            
        Returns:
            Fully quoted schema.table or catalog.schema.table
            
        Examples:
            >>> CatalogUtils.quote_schema_table("uk0429", "my_table")
            '`uk0429`.`my_table`'
            >>> CatalogUtils.quote_schema_table("iceberg_data.uk0429", "test-special-chars")
            '`iceberg_data`.`uk0429`.`test-special-chars`'
        """
        # Check if schema contains catalog prefix
        if CatalogUtils.has_catalog_prefix(schema):
            catalog, schema_only = CatalogUtils.split_catalog_schema(schema)
            quoted_catalog = CatalogUtils.quote_identifier(catalog)
            quoted_schema = CatalogUtils.quote_identifier(schema_only)
            quoted_table = CatalogUtils.quote_identifier(table)
            return f"{quoted_catalog}.{quoted_schema}.{quoted_table}"
        else:
            quoted_schema = CatalogUtils.quote_identifier(schema)
            quoted_table = CatalogUtils.quote_identifier(table)
            return f"{quoted_schema}.{quoted_table}"

    @staticmethod
    def build_qualified_name(
        schema: str,
        table: str,
        quote: bool = True
    ) -> str:
        """
        Build a fully qualified table name, optionally with quoting.
        
        Args:
            schema: Schema name, possibly with catalog prefix
            table: Table name
            quote: Whether to quote identifiers (default: True)
            
        Returns:
            Fully qualified table name
            
        Examples:
            >>> CatalogUtils.build_qualified_name("uk0429", "my_table", quote=True)
            '`uk0429`.`my_table`'
            >>> CatalogUtils.build_qualified_name("uk0429", "my_table", quote=False)
            'uk0429.my_table'
        """
        if quote:
            return CatalogUtils.quote_schema_table(schema, table)
        return f"{schema}.{table}"

    @staticmethod
    def normalize_schema_from_show_tables(
        returned_schema: str,
        context_schema: Optional[str] = None
    ) -> str:
        """
        Normalize schema from SHOW TABLES result, handling Spark SQL bug.
        
        Spark SQL's SHOW TABLES IN catalog.schema sometimes returns only "schema"
        instead of "catalog.schema". This function restores the full path using
        the original query context.
        
        Args:
            returned_schema: Schema returned by SHOW TABLES
            context_schema: Original schema from query context (catalog.schema)
            
        Returns:
            Normalized schema with catalog prefix if applicable
            
        Examples:
            >>> CatalogUtils.normalize_schema_from_show_tables("uk0429", "iceberg_data.uk0429")
            'iceberg_data.uk0429'
            >>> CatalogUtils.normalize_schema_from_show_tables("uk0429", None)
            'uk0429'
            >>> CatalogUtils.normalize_schema_from_show_tables("iceberg_data.uk0429", "iceberg_data.uk0429")
            'iceberg_data.uk0429'
        """
        if not context_schema:
            return returned_schema
        
        # If context has catalog but returned schema doesn't, use context
        if CatalogUtils.has_catalog_prefix(context_schema) and not CatalogUtils.has_catalog_prefix(returned_schema):
            return context_schema
        
        return returned_schema

# Made with Bob
