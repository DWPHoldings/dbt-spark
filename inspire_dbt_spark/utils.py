from typing import Dict

from .udfs import UDFRegistry

from .external_relations import ExternalRelationRegistry


def initialize_dbt_spark(spark):
    UDFRegistry.initialize_udfs(spark)
    ExternalRelationRegistry.initialize_external_relations(spark)


def register_external_relation(source: str, relation: str, alias: str, type_: str = 'dbtable'):
    ExternalRelationRegistry.register_relation(source=source, relation=relation, alias=alias, type_=type_)


def register_external_source(source_name: str, driver: str, options: Dict[str, str]):
    ExternalRelationRegistry.register_source(source_name=source_name, driver=driver, options=options)
