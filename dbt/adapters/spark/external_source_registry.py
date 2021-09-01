from dataclasses import dataclass
from typing import Dict, List


@dataclass()
class RegisteredSource(object):
    name: str
    driver_name: str
    jars: List[str]
    options: Dict[str, str]


@dataclass()
class RegisteredExternalRelation(object):
    source: RegisteredSource
    alias: str
    relation: str
    relation_type: str


class ExternalSourceRegistry(object):
    source_registry: Dict[str, RegisteredSource] = dict()
    relation_registry: Dict[str, RegisteredExternalRelation] = dict()

    @classmethod
    def register_source(cls, name: str, driver_name: str, jars: List[str], options: Dict[str, str]):
        if name not in cls.source_registry:
            cls.source_registry[name] = RegisteredSource(
                name=name,
                driver_name=driver_name,
                jars=jars,
                options=options
            )

    @classmethod
    def register_external_relation(cls, source, alias, relation, relation_type):
        assert source in cls.source_registry, f'Source {source} not registered!'
        cls.relation_registry[alias] = RegisteredExternalRelation(
            source=cls.source_registry[source],
            alias=alias,
            relation=relation,
            relation_type=relation_type)

    @classmethod
    def get_jars(cls):
        return list(set([
            jar for source in cls.source_registry.values() for jar in source.jars
        ]))
