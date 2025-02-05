import time
from pathlib import Path
from urllib.parse import quote_plus
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from mkpipe.config import load_config
from mkpipe.functions_db import get_db_connector
from mkpipe.functions_spark import remove_partitioned_parquet, get_parser
from mkpipe.utils import log_container, Logger
from mkpipe.utils.base_class import PipeSettings


class ClickhouseLoader:
    def __init__(self, config, settings):
        if isinstance(settings, dict):
            self.settings = PipeSettings(**settings)
        else:
            self.settings = settings
        self.connection_params = config['connection_params']
        self.host = self.connection_params['host']
        self.port = self.connection_params['port']
        self.username = self.connection_params['user']
        self.password = quote_plus(str(self.connection_params['password']))
        self.database = self.connection_params['database']

        self.driver_name = 'clickhouse'
        self.driver_jdbc = 'com.clickhouse.jdbc.ClickHouseDriver'
        self.settings.driver_name = self.driver_name
        self.jdbc_url = f'jdbc:{self.driver_name}://{self.host}:{self.port}/{self.database}?user={self.username}&password={self.password}'

        config = load_config()
        connection_params = config['settings']['backend']
        db_type = connection_params['database_type']
        self.backend = get_db_connector(db_type)(connection_params)

    def add_custom_columns(self, df, elt_start_time):
        if 'etl_time' in df.columns:
            df = df.drop('etl_time')

        df = df.withColumn('etl_time', F.lit(elt_start_time).cast(TimestampType()))
        return df

    @log_container(__file__)
    def load(self, data, elt_start_time):
        try:
            logger = Logger(__file__)
            start_time = time.time()
            name = data['table_name']

            #write_mode = data.get('write_mode', None)
            write_mode = 'append'
            file_type = data.get('file_type', None)
            last_point_value = data.get('last_point_value', None)
            iterate_column_type = data.get('iterate_column_type', None)
            replication_method = data.get('replication_method', 'full')
            batchsize = data.get('fetchsize', 100_000)
            pass_on_error = data.get('pass_on_error', None)

            if not file_type:
                'means that the data fetched before no new data'
                self.backend.manifest_table_update(
                    name=name,
                    value=None,  # Last point remains unchanged
                    value_type=None,  # Type remains unchanged
                    status='completed',  # ('completed', 'failed', 'extracting', 'loading')
                    replication_method=replication_method,  # ('incremental', 'full')
                    error_message='',
                )
                return

            self.backend.manifest_table_update(
                name=name,
                value=None,  # Last point remains unchanged
                value_type=None,  # Type remains unchanged
                status='loading',  # ('completed', 'failed', 'extracting', 'loading')
                replication_method=replication_method,  # ('incremental', 'full')
                error_message='',
            )

            df = get_parser(file_type)(data, self.settings)
            df = self.add_custom_columns(df, elt_start_time)
            message = dict(
                table_name=name,
                status='loading',
                total_partition_count=df.rdd.getNumPartitions(),
            )
            logger.info(message)

            (
                df.write.format('jdbc')
                .mode(
                    write_mode
                )  # Use write_mode for the first iteration, 'append' for others
                .option('url', self.jdbc_url)
                .option('dbtable', name)
                .option('driver', self.driver_jdbc)
                .option('batchsize', batchsize)
                .save()
            )

            # Update last point in the mkpipe_manifest table if applicable
            self.backend.manifest_table_update(
                name=name,
                value=last_point_value,
                value_type=iterate_column_type,
                status='completed',
                replication_method=replication_method,
                error_message='',
            )

            message = dict(table_name=name, status=write_mode)
            logger.info(message)

            # remove the parquet to reduce the storage
            remove_partitioned_parquet(data['path'])

            run_time = time.time() - start_time
            message = dict(table_name=name, status='success', run_time=run_time)
            logger.info(message)

        except Exception as e:
            # Log the error message and update the mkpipe_manifest with the error details
            message = dict(
                table_name=name,
                status='failed',
                type='loading',
                error_message=str(e),
                etl_start_time=str(elt_start_time),
            )

            self.backend.manifest_table_update(
                name=name,
                value=None,  # Last point remains unchanged
                value_type=None,  # Type remains unchanged
                status='failed',
                replication_method=replication_method,
                error_message=str(e),
            )

            if pass_on_error:
                logger.warning(message)
                return
            else:
                logger.error(message)
                raise Exception(message) from e
        return
