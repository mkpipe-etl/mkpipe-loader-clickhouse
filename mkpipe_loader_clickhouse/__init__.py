import gc
from datetime import datetime
from typing import List

from mkpipe.exceptions import ConfigError, LoadError
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig, WriteStrategy
from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import add_etl_columns
from mkpipe.strategy import resolve_write_strategy
from mkpipe.utils import get_logger

JAR_PACKAGES = [
    'com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.10.0',
    'com.clickhouse:clickhouse-http-client:0.7.2',
    'org.apache.httpcomponents.client5:httpclient5:5.3.1',
]

logger = get_logger(__name__)

# Spark -> ClickHouse type mapping
_SPARK_TO_CH_TYPES = {
    'byte': 'Int8',
    'short': 'Int16',
    'int': 'Int32',
    'long': 'Int64',
    'float': 'Float32',
    'double': 'Float64',
    'decimal': 'Decimal',
    'string': 'String',
    'boolean': 'Bool',
    'date': 'Date',
    'timestamp': 'DateTime64(3)',
    'timestamp_ntz': 'DateTime64(3)',
    'binary': 'String',
}


def _spark_type_to_ch(spark_type_str: str) -> str:
    """Convert a Spark type string to a ClickHouse type string."""
    key = spark_type_str.lower()
    if key.startswith('decimal'):
        return spark_type_str.replace('decimal', 'Decimal')
    return _SPARK_TO_CH_TYPES.get(key, 'String')


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

    def _create_table(self, full_table: str, df, order_by: str) -> None:
        """Create a ClickHouse MergeTree table from a Spark DataFrame schema."""
        columns = []
        for field in df.schema.fields:
            ch_type = _spark_type_to_ch(field.dataType.simpleString())
            nullable = 'Nullable({})'.format(ch_type) if field.nullable else ch_type
            columns.append(f'    `{field.name}` {nullable}')

        cols_sql = ',\n'.join(columns)
        ddl = (
            f'CREATE TABLE IF NOT EXISTS {full_table}\n'
            f'(\n{cols_sql}\n)\n'
            f'ENGINE = MergeTree()\n'
            f'ORDER BY ({order_by})\n'
            f'SETTINGS allow_nullable_key = 1'
        )
        self._execute_query(ddl)
        logger.info({'table': full_table, 'status': 'created'})

    def _base_options(self) -> dict:
        return {
            'host': self.host,
            'port': str(self.port),
            'user': self.username,
            'password': self.password,
            'database': self.database,
        }

    def _create_replacing_merge_tree(
        self, full_table: str, df, order_by: str, version_col: str = 'etl_time',
    ) -> None:
        """Create a ClickHouse ReplacingMergeTree table for upsert semantics."""
        columns = []
        for field in df.schema.fields:
            ch_type = _spark_type_to_ch(field.dataType.simpleString())
            nullable = 'Nullable({})'.format(ch_type) if field.nullable else ch_type
            columns.append(f'    `{field.name}` {nullable}')

        cols_sql = ',\n'.join(columns)
        ddl = (
            f'CREATE TABLE IF NOT EXISTS {full_table}\n'
            f'(\n{cols_sql}\n)\n'
            f'ENGINE = ReplacingMergeTree({version_col})\n'
            f'ORDER BY ({order_by})\n'
            f'SETTINGS allow_nullable_key = 1'
        )
        self._execute_query(ddl)
        logger.info({'table': full_table, 'status': 'created', 'engine': 'ReplacingMergeTree'})

    def _write_to_ch(self, df, full_table: str, order_by: str) -> None:
        opts = {**self._base_options(), 'table': full_table, 'order_by': order_by}
        writer = df.write.format('clickhouse').mode('append')
        for k, v in opts.items():
            writer = writer.option(k, v)
        writer.save()

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        df = data.df

        if df is None:
            logger.info(
                {'table': target_name, 'status': 'skipped', 'reason': 'no data'}
            )
            return

        df = add_etl_columns(df, datetime.now(), dedup_columns=table.dedup_columns)

        if table.write_partitions:
            df = df.coalesce(table.write_partitions)

        strategy = resolve_write_strategy(table, data)

        logger.info(
            {'table': target_name, 'status': 'loading', 'write_strategy': strategy.value}
        )

        full_table = f'{self.database}.{target_name}'

        # Determine ORDER BY key
        if table.write_key:
            order_by = ', '.join(table.write_key)
        elif table.dedup_columns:
            order_by = ', '.join(table.dedup_columns)
        else:
            order_by = df.columns[0]

        try:
            match strategy:
                case WriteStrategy.REPLACE:
                    self._drop_if_exists(full_table)
                    self._create_table(full_table, df, order_by)
                    self._write_to_ch(df, full_table, order_by)
                case WriteStrategy.APPEND:
                    self._create_table(full_table, df, order_by)
                    self._write_to_ch(df, full_table, order_by)
                case WriteStrategy.UPSERT:
                    if not table.write_key:
                        raise ConfigError(
                            f"write_strategy 'upsert' requires write_key for table '{target_name}'"
                        )
                    self._drop_if_exists(full_table)
                    self._create_replacing_merge_tree(full_table, df, order_by)
                    self._write_to_ch(df, full_table, order_by)
                case _:
                    raise ConfigError(
                        f"ClickHouse loader does not support write_strategy: {strategy.value}"
                    )
        except (ConfigError, LoadError):
            raise
        except Exception as e:
            raise LoadError(f"Failed to write '{target_name}': {e}") from e

        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
