from dataclasses import dataclass
from typing import Callable, Dict, List, Any, Set

import pyspark.sql.pandas.functions as pdf

try:
    from importlib import metadata
except ImportError:  # for Python<3.8
    import importlib_metadata as metadata

import logging

logger = logging.getLogger(__name__)


@dataclass
class RegisteredUDF:
    alias: str
    udf: Callable
    return_type: Any
    pandas_udf: bool


class UDFRegistry:

    registry: Dict[str, RegisteredUDF] = dict()
    registered_plugins: List[str] = []
    _existing_functions: Set[str] = set()

    @classmethod
    def udf(cls, alias: str, return_type: Any = None, pandas_udf: bool = False):
        def wrapper(fn: Callable):
            logger.info(f'Found custom UDF: {alias}')
            cls.register_udf(alias, fn, return_type, pandas_udf)
            return fn
        return wrapper

    @classmethod
    def register_udf(cls, alias: str, udf: Callable, return_type: Any = None, pandas_udf: bool = False):
        cls.registry[alias] = RegisteredUDF(alias=alias, udf=udf, return_type=return_type, pandas_udf=pandas_udf)

    @classmethod
    def load_plugins(cls):
        plugins = metadata.entry_points()['spark_udf']
        logger.info('Registering Plugins . . .')
        for plugin in plugins:
            if plugin.value not in UDFRegistry.registered_plugins:
                logger.info(f'Found plugin: {plugin.name}: {plugin.value}')
                UDFRegistry.registered_plugins.append(plugin.value)
                plugin.load()

    @classmethod
    def initialize_udfs(cls, spark):
        if len(cls._existing_functions) == 0:
            cls._existing_functions = set([t.name for t in spark.catalog.listFunctions('default')])
        for udf in filter(lambda u: u.alias not in cls._existing_functions, cls.registry.values()):
            logger.info(f'Registering UDF: {udf.alias} [{udf.udf}]')
            if udf.pandas_udf:
                logger.debug(f'Registering Pandas UDF: {udf.alias} [{udf.udf}]')
                print(f'Registering Pandas UDF: {udf.alias} [{udf.udf}]')
                pdf.pandas_udf(udf.udf, udf.return_type)
            spark.udf.register(udf.alias, udf.udf, udf.return_type)
            cls._existing_functions.add(udf.alias)
