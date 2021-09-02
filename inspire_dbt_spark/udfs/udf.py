from dataclasses import dataclass
from typing import Callable, Dict
from importlib import metadata

import logging

logger = logging.getLogger(__name__)

plugins = metadata.entry_points()['spark_udf']
logger.info('Registering Plugins . . .')
for plugin in plugins:
    logger.info(f'Found plugin: {plugin.name}: {plugin.value}')
    plugin.load()


@dataclass
class RegisteredUDF:
    alias: str
    udf: Callable


class UDFRegistry:

    registry: Dict[str, RegisteredUDF] = dict()

    @classmethod
    def udf(cls, alias: str):
        def wrapper(fn: Callable):
            logger.info(f'Found custom UDF: {alias}')
            cls.register_udf(alias, fn)
        return wrapper

    @classmethod
    def register_udf(cls, alias: str, udf: Callable):
        cls.registry[alias] = RegisteredUDF(alias=alias, udf=udf)

    @classmethod
    def initialize_udfs(cls, spark):
        for udf in cls.registry.values():
            logger.info(f'Registering UDF: {udf.alias} [{udf.udf}]')
            spark.udf.register(udf.alias, udf.udf)
