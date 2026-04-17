from typing import Optional, TypeVar
from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.events.logging import AdapterLogger

from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("Spark")

Self = TypeVar("Self", bound="BaseRelation")


@dataclass
class SparkQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class SparkIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass
class SparkIcebergIncludePolicy(Policy):
    """Include policy for Iceberg relations supporting three-part namespace (catalog.schema.identifier)"""
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SparkRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: SparkQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: SparkIncludePolicy())
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    is_hudi: Optional[bool] = None
    is_iceberg: Optional[bool] = None
    # TODO: make this a dict everywhere
    information: Optional[str] = None
    location_root: Optional[str] = None
    catalog: Optional[str] = None

    def __post_init__(self) -> None:
        # Update include policy for Iceberg relations to support three-part namespace
        if self.is_iceberg:
            object.__setattr__(self, 'include_policy', SparkIcebergIncludePolicy())
        
        if self.database != self.schema and self.database:
            raise DbtRuntimeError("Cannot set database in spark!")

    def render(self) -> str:
        # For Iceberg tables and views, allow three-part namespace (catalog.schema.identifier)
        # The schema already contains catalog.schema from connections.py (line 194)
        # So we just need to skip the validation that prevents both database and schema
        if self.is_iceberg:
            # For Iceberg, allow rendering with schema that contains catalog prefix
            return super().render()
        
        # For non-Iceberg relations, maintain the original validation
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                "Got a spark relation with schema and database set to "
                "include, but only one can be set"
            )
        return super().render()

    def set_location(self, location: str) -> None:
        object.__setattr__(self, 'location_root', location)