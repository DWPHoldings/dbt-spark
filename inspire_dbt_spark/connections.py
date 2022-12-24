import asyncio
import atexit
import threading
import re
import traceback

from pyspark.sql import SparkSession

import dbt.exceptions
from dbt.events import AdapterLogger

from inspire_dbt_spark.external_source_registry import ExternalSourceRegistry

from inspire_dbt_spark import register_external_source, register_external_relation, initialize_dbt_spark

logger = AdapterLogger("Spark")


# coroutines for running queries asynchronously and logging when they take a long time
def _query_session(session, query):
    try:
        sc = session.sparkContext
        model_description = re.search("(?<=spark_model_name:)\s*(\w+)", query)
        if model_description is not None:
            model_description = model_description[0].strip()
            sc.setLocalProperty("callSite.short", model_description)
            sc.setLocalProperty("callSite.long", model_description)
            sc.setJobDescription(model_description)
            print(f'Running model: {model_description}')
    except Exception as ex:
        traceback.print_exc()
        logger.warn(ex)
    return session.sql(query)


async def _wait_loop(event: threading.Event):
    logger.info('Starting watchdog thread.')
    await asyncio.sleep(60)
    # print a message to log to ensure that airflow knows the job is still running
    n = 0
    while not event.is_set():
        print(f'SparkSQL Query is running waiting for results ({60 + (30 * n)}) . . . ')
        await asyncio.sleep(30)
        n = n + 1
    print('wait loop completed.')


async def _execute_query_main(session, query):
    event = threading.Event()
    wait_loop = asyncio.create_task(_wait_loop(event))

    result_df = await asyncio.get_running_loop().run_in_executor(None, _query_session, session, query)

    try:
        n = 0
        while not wait_loop.done():
            n = n + 1
            if n % 10 == 0:
                logger.warning(f'Watchdog still running after {n} attempts to cancel')
            event.set()
            wait_loop.cancel()
            await asyncio.sleep(0)
        logger.info('Query execution complete.')
    except Exception as ex:
        logger.error(f"Error canceling wait_loop {ex}")
    return result_df


def execute_query_async(session, query):
    try:

        logger.debug('Registering External Sources . . .')
        for source in ExternalSourceRegistry.source_registry.copy().values():
            logger.debug(f'Registering source [{source}]')
            register_external_source(source_name=source.name, driver=source.driver_name, options=source.options)
        for relation in ExternalSourceRegistry.relation_registry.copy().values():
            logger.debug(f'Registering relation [{relation}]')
            register_external_relation(
                source=relation.source.name,
                relation=relation.relation,
                alias=relation.alias,
                type_=relation.relation_type,
                options=relation.options,
                partition_by=relation.partition_by,
                location=relation.location,
                properties=relation.properties,
                comment=relation.comment,
                cache=relation.cache,
                cache_storage_level=relation.cache_storage_level,
            )

        logger.debug('Initializing dbt-spark . . .')
        initialize_dbt_spark(session)

        logger.debug(f'Executing Query [{query}')

        results = asyncio.run(_execute_query_main(session, query))

        logger.debug(f'results: [{results}]')

        return results

    except Exception as ex:
        logger.error(ex)
        dbt.exceptions.raise_database_error(ex)


def __finalize_spark():
    spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark_session.stop()


atexit.register(__finalize_spark)
