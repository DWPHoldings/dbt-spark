from dataclasses import dataclass
from typing import Dict, List


@dataclass()
class RegisteredSource(object):
    """
    A source is a single named connection to external database.
    """
    name: str  # name to reference the source
    driver_name: str  # JDBC driver to use to connect to the source
    options: Dict[str, str]  # Driver specific connection properties


@dataclass()
class RegisteredExternalRelation(object):
    """
    An external relation is a table, view, or query associated with a source.
    """
    source: RegisteredSource
    alias: str  # name of the relation created in dbt-spark
    relation: str  # table or query in the source database
    relation_type: str  # type of relation `dbtable` or `query`
    options: Dict[str, str]  # driver specific options for this relation
    partition_by: List[str]
    location: str
    properties: Dict[str, str]  # tblproperties
    comment: str
    cache: bool  # cache the table
    cache_storage_level: str  # how to cache (default is MEMORY_AND_DISK)


class ExternalSourceRegistry(object):
    source_registry: Dict[str, RegisteredSource] = dict()
    relation_registry: Dict[str, RegisteredExternalRelation] = dict()

    @classmethod
    def register_source(cls, name: str, driver_name: str, options: Dict[str, str]):
        if name not in cls.source_registry:
            cls.source_registry[name] = RegisteredSource(
                name=name,
                driver_name=driver_name,
                options=options
            )

    @classmethod
    def register_external_relation(
            cls,
            source,
            alias,
            relation,
            relation_type,
            options: Dict[str, str],
            partition_by: List[str],
            location: str,
            properties: Dict[str, str],
            comment: str,
            cache: bool,
            cache_storage_level: str,

    ):
        assert source in cls.source_registry, f'Source {source} not registered!'
        cls.relation_registry[alias] = RegisteredExternalRelation(
            source=cls.source_registry[source],
            alias=alias,
            relation=relation,
            relation_type=relation_type,
            options=options,
            partition_by=partition_by,
            location=location,
            properties=properties,
            comment=comment,
            cache=cache,
            cache_storage_level=cache_storage_level,
        )
