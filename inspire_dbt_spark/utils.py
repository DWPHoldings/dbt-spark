from typing import Dict

from .udfs import UDFRegistry

from .external_relations import ExternalRelationRegistry


def initialize_dbt_spark(spark):
    UDFRegistry.load_plugins()
    UDFRegistry.initialize_udfs(spark)
    ExternalRelationRegistry.initialize_external_relations(spark)


def register_external_relation(
        source: str,
        relation: str,
        alias: str,
        options: Dict[str, str],
        location: str,
        properties: Dict[str, str],
        comment: str,
        type_: str = 'dbtable',
):
    ExternalRelationRegistry.register_relation(
        source=source,
        relation=relation,
        alias=alias,
        type_=type_,
        options=options,
        location=location,
        properties=properties,
        comment=comment,
    )


def register_external_source(source_name: str, driver: str, options: Dict[str, str]):
    ExternalRelationRegistry.register_source(source_name=source_name, driver=driver, options=options)
