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

        # Determine ORDER BY key
        if table.dedup_columns:
            order_by = ', '.join(table.dedup_columns)
        else:
            order_by = df.columns[0]

        # For overwrite: drop + create fresh table with new schema.
        # For append: create if not exists (idempotent).
        if write_mode == 'overwrite':
            self._drop_if_exists(full_table)

        self._create_table(full_table, df, order_by)

        # Always append -- table is guaranteed to exist at this point.
        opts = {**self._base_options(), 'table': full_table, 'order_by': order_by}
        writer = df.write.format('clickhouse').mode('append')
        for k, v in opts.items():
            writer = writer.option(k, v)
        writer.save()

        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
