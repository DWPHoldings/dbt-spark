from typing import Optional, Type, Any

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.contracts.graph.parsed import ParsedSourceDefinition
from dbt.exceptions import RuntimeException

from dbt.adapters.spark.column import Self
from inspire_dbt_spark.external_source_registry import ExternalSourceRegistry


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


@dataclass(frozen=True, eq=False, repr=False)
class SparkRelation(BaseRelation):
    quote_policy: SparkQuotePolicy = SparkQuotePolicy()
    include_policy: SparkIncludePolicy = SparkIncludePolicy()
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    is_iceberg: Optional[bool] = None
    is_hudi: Optional[bool] = None
    information: Optional[str] = None

    @classmethod
    def create_from_source(
            cls: Type[Self], source: ParsedSourceDefinition, **kwargs: Any
    ) -> Self:
        print(f'registering source {source.name}, {source.source_name}, {source.meta}')
        if source.meta.get('external_table'):
            ExternalSourceRegistry.register_source(
                name=source.source_name,
                driver_name=source.source_meta.get('driver_name'),
                options=source.source_meta.get('options', dict()),
            )
            ExternalSourceRegistry.register_external_relation(
                source=source.source_name,
                alias=source.name,
                relation=source.meta['external_table'],
                relation_type=source.meta.get('external_table_type', 'dbtable'),
                options=source.meta.get('options', dict()),
                location=source.meta.get('location'),
                properties=source.meta.get('properties', dict()),
                comment=source.meta.get('comment'),
                cache=source.meta.get('cache'),
                cache_storage_level=source.meta.get('cache_storage_level'),
            )
        return super().create_from_source(source, **kwargs)

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise RuntimeException("Cannot set database in spark!")

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise RuntimeException(
                "Got a spark relation with schema and database set to "
                "include, but only one can be set"
            )
        source = self.path.database or self.path.schema
        if source in ExternalSourceRegistry.source_registry:
            for xr in ExternalSourceRegistry.relation_registry.copy().values():
                if xr.source.name == source and xr.alias == self.path.identifier:
                    return xr.alias
        return super().render()
