import gc
from datetime import datetime

from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import add_etl_columns
from mkpipe.utils import get_logger

JAR_PACKAGES = [
    'com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.10.0',
    'com.clickhouse:clickhouse-http-client:0.7.2',
    'org.apache.httpcomponents.client5:httpclient5:5.3.1',
]

logger = get_logger(__name__)


class ClickhouseLoader(BaseLoader, variant='clickhouse'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.host = connection.host
        self.port = connection.port or 8123
        self.username = connection.user or 'default'
        self.password = str(connection.password or '')
        self.database = connection.database

    def _base_options(self) -> dict:
        return {
            'host': self.host,
            'port': str(self.port),
            'user': self.username,
            'password': self.password,
            'database': self.database,
        }

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        write_mode = data.write_mode
        df = data.df

        if df is None:
            logger.info(
                {'table': target_name, 'status': 'skipped', 'reason': 'no data'}
            )
            return

        df = add_etl_columns(df, datetime.now(), dedup_columns=table.dedup_columns)

        if table.write_partitions:
            df = df.coalesce(table.write_partitions)

        logger.info(
            {'table': target_name, 'status': 'loading', 'write_mode': write_mode}
        )

        opts = {**self._base_options(), 'dbtable': target_name}
        writer = df.write.format('clickhouse').mode(write_mode)
        for k, v in opts.items():
            writer = writer.option(k, v)
        writer.save()

        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
