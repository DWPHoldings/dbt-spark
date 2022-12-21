from dataclasses import dataclass
from typing import Dict, Set

from jinja2 import Environment, DictLoader, select_autoescape

import logging

DATABASE_DRIVERS = ['org.apache.spark.sql.jdbc', 'net.snowflake.spark.snowflake']

templates = {
    'create_temporary_view.sql': """
CREATE OR REPLACE TEMPORARY VIEW {{ alias }}
USING {{ source_driver }}
{%- if options is not none and options|length>0 %}
OPTIONS (
  {% for opt, val in options.items() %}
  {{ opt }} '{{ val }}'{%- if not loop.last %},{%- endif %}
  {% endfor %}
)
{% endif -%}
{%- if location is not none %}
LOCATION '{{ location }}'
{% endif -%}
{% if properties is not none and properties|length>0 -%} 
TBLPROPERTIES (
  {% for key, val in properties.items() %}
  '{{ key }}'='{{ val }}'{%- if not loop.last %},{%- endif %}
  {% endfor %}
)
{% endif -%}
{%- if comment is not none %}
COMMENT '{{ comment }}'
{% endif -%}
    """
}

logger = logging.getLogger(__name__)

env = Environment(
    loader=DictLoader(templates),
    autoescape=select_autoescape(['sql'])
)

CREATE_TEMPORARY_VIEW = env.get_template('create_temporary_view.sql')


@dataclass
class RegisteredSource:
    source: str
    driver: str
    options: Dict[str, str]


@dataclass
class RegisteredRelation:
    source: RegisteredSource
    relation: str
    alias: str
    type_: str
    options: Dict[str, str]
    properties: Dict[str, str]
    location: str
    comment: str
    cache: bool
    cache_storage_level: str

    @property
    def sql(self):
        # merge source and relation level options
        options = {**(self.source.options or {}), **(self.options or {})}

        if self.source.driver in DATABASE_DRIVERS:
            options[self.type_] = self.relation

        return CREATE_TEMPORARY_VIEW.render(
            alias=self.alias,
            relation_type=self.type_,
            relation=self.relation,
            source_driver=self.source.driver,
            options=options,
            location=self.location,
            properties=self.properties,
            comment=self.comment,
            cache=self.cache,
        )


class ExternalRelationRegistry:

    registered_sources: Dict[str, RegisteredSource] = dict()
    registered_relations: Dict[str, RegisteredRelation] = dict()
    _existing_tables: Set[str] = set()

    @classmethod
    def register_source(cls, source_name: str, driver: str, options: Dict[str, str]):
        if source_name not in cls.registered_sources:
            cls.registered_sources[source_name] = RegisteredSource(
                source=source_name,
                driver=driver,
                options=options
            )

    @classmethod
    def register_relation(
            cls,
            source: str,
            relation: str,
            alias: str,
            type_: str,
            options: Dict[str, str],
            location: str,
            properties: Dict[str, str],
            comment: str,
            cache: bool,
            cache_storage_level: str,
    ):
        if alias not in cls.registered_relations:
            assert source in cls.registered_sources, f'Source {source} not registered!'
            cls.registered_relations[alias] = RegisteredRelation(
                source=cls.registered_sources[source],
                relation=relation,
                alias=alias,
                type_=type_,
                options=options,
                location=location,
                properties=properties,
                comment=comment,
                cache=cache,
                cache_storage_level=cache_storage_level,
            )

    @classmethod
    def initialize_external_relations(cls, spark):
        if len(cls._existing_tables) == 0:
            print('Existing Tables:')
            cls._existing_tables = set([t.name for t in spark.catalog.listTables('default')])
            print(cls._existing_tables)
        # filter views that have already been created
        for rel in filter(lambda r: r.alias not in cls._existing_tables, cls.registered_relations.copy().values()):
            # register the view
            print(rel.sql)
            try:
                result = spark.sql(rel.sql)
                print(result)
                if rel.cache:
                    storage_level = rel.cache_storage_level or 'MEMORY_AND_DISK'
                    spark.sql(f"CACHE LAZY TABLE {rel.alias} OPTIONS('storageLevel'='{storage_level}')")
                cls._existing_tables.add(rel.alias)
            except Exception as ex:
                logger.error(f"error registring source: {rel.alias}\n{str(ex)}")
