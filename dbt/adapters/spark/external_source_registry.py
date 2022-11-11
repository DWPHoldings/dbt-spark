from copy import deepcopy
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


class ExternalSourceRegistry(object):
    source_registry: Dict[str, RegisteredSource] = dict()
    relation_registry: Dict[str, RegisteredExternalRelation] = dict()

    @classmethod
    def register_source(cls, name: str, driver_name: str, options: Dict[str, str]):
        if name not in cls.source_registry:
            sr = deepcopy(cls.source_registry)
            sr[name] = RegisteredSource(
                name=name,
                driver_name=driver_name,
                options=options
            )
            cls.source_registry = sr

    @classmethod
    def register_external_relation(cls, source, alias, relation, relation_type):
        assert source in cls.source_registry, f'Source {source} not registered!'
        if alias not in cls.relation_registry:
            rr = deepcopy(cls.relation_registry)
            rr[alias] = RegisteredExternalRelation(
                source=cls.source_registry[source],
                alias=alias,
                relation=relation,
                relation_type=relation_type)
            cls.relation_registry = rr
