import os
from dataclasses import dataclass
from typing import Dict

from jinja2 import Environment, DictLoader, select_autoescape

import logging

templates = {
    'create_temporary_view.sql': """
CREATE TEMPORARY VIEW {{ alias }}
USING {{ source_driver }}
OPTIONS (
  {{ relation_type }} '{{ relation }}',
  {% for opt, val in options.items() %}
  {{ opt }} '{{ val }}'{%- if not loop.last %},{%- endif %}
  {% endfor %}
)
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

    @property
    def sql(self):
        return CREATE_TEMPORARY_VIEW.render(
            alias=self.alias,
            relation_type=self.type_,
            relation=self.relation,
            source_driver=self.source.driver,
            options=self.source.options,
        )


class ExternalRelationRegistry:

    registered_sources: Dict[str, RegisteredSource] = dict()
    registered_relations: Dict[str, RegisteredRelation] = dict()

    @classmethod
    def register_source(cls, source_name: str, driver: str, options: Dict[str, str]):
        if source_name not in cls.registered_sources:
            s_options = {k: v.format(**os.environ) for k, v in options.items()}
            cls.registered_sources[source_name] = RegisteredSource(
                source=source_name,
                driver=driver,
                options=s_options
            )

    @classmethod
    def register_relation(cls, source: str, relation: str, alias: str, type_: str):
        if alias not in cls.registered_relations:
            assert source in cls.registered_sources, f'Source {source} not registered!'
            cls.registered_relations[alias] = RegisteredRelation(
                source=cls.registered_sources[source],
                relation=relation,
                alias=alias,
                type_=type_
            )

    @classmethod
    def initialize_external_relations(cls, spark):
        existing_tables = set([t.name for t in spark.catalog.listTables('default')])
        # filter views that have already been created
        for rel in filter(lambda r: r.alias not in existing_tables, cls.registered_relations.values()):
            # register the view
            spark.sql(rel.sql)
