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

    def _execute_query(self, query: str) -> None:
        """Execute a query against ClickHouse via HTTP."""
        import urllib.request

        url = f'http://{self.host}:{self.port}'
        req = urllib.request.Request(
            url,
            data=query.encode('utf-8'),
            method='POST',
        )
        req.add_header('X-ClickHouse-User', self.username)
        req.add_header('X-ClickHouse-Key', self.password)
        with urllib.request.urlopen(req) as resp:
            resp.read()

    def _drop_if_exists(self, full_table: str) -> None:
        """Drop a ClickHouse table if it exists."""
        try:
            self._execute_query(f'DROP TABLE IF EXISTS {full_table}')
            logger.info({'table': full_table, 'status': 'dropped'})
        except Exception as e:
            logger.debug({'table': full_table, 'drop_skipped': str(e)})

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

        full_table = f'{self.database}.{target_name}'
        opts = {**self._base_options(), 'table': full_table}

        # ClickHouse MergeTree requires ORDER BY when creating new tables.
        # Use dedup_columns if defined, otherwise fall back to first column.
        if table.dedup_columns:
            opts['order_by'] = ', '.join(table.dedup_columns)
        else:
            opts['order_by'] = df.columns[0]

        # Spark ClickHouse connector auto-creates tables only in append mode.
        # For overwrite: drop existing table, then append (re-creates with fresh schema).
        if write_mode == 'overwrite':
            self._drop_if_exists(full_table)
            spark_mode = 'append'
        else:
            spark_mode = write_mode

        writer = df.write.format('clickhouse').mode(spark_mode)
        for k, v in opts.items():
            writer = writer.option(k, v)
        writer.save()

        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
