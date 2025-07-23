import os
import time
import gc
from mkpipe.functions_spark import remove_partitioned_parquet, get_parser
from mkpipe.utils import log_container, Logger
from .upload_to_clickhouse import upload_folder
from mkpipe.functions_spark import BaseLoader


class ClickhouseLoader(BaseLoader):
    def __init__(self, config, settings):
        super().__init__(
            config,
            settings,
            driver_name='clickhouse',
            driver_jdbc='com.clickhouse.jdbc.ClickHouseDriver',
        )

    def build_jdbc_url(self):
        return f'http://{self.host}:{self.port}/?database={self.database}&user={self.username}&password={self.password}'

    @log_container(__file__)
    def load(self, data, elt_start_time):
        try:
            logger = Logger(__file__)
            start_time = time.time()
            t = self.table
            name = t['target_name']
            iterate_column_type = t['iterate_column_type']
            batchsize = t.get('batchsize', 100_000)
            replication_method = t.get('replication_method', 'full')

            write_mode = data.get('write_mode', None)
            last_point_value = data.get('last_point_value', None)
            df = data['df']

            self.backend.manifest_table_update(
                name=name,
                value=None,
                value_type=None,
                status='loading',
                replication_method=replication_method,
                error_message='',
            )

            if not df:
                self.backend.manifest_table_update(
                    name=name,
                    value=last_point_value,
                    value_type=iterate_column_type,
                    status='completed',
                    replication_method=replication_method,
                    error_message='no new record',
                )
                return

            df = self.add_custom_columns(df, elt_start_time)
            message = dict(
                table_name=name,
                status='loading',
                total_partitions_count=df.rdd.getNumPartitions(),
            )
            logger.info(message)

            clickhouse_temp_df_path = os.path.abspath(
                os.path.join(self.settings.ROOT_DIR, 'artifacts', 'clickhouse', name)
            )
            df.write.parquet(clickhouse_temp_df_path, mode='overwrite')
            upload_folder(
                folder_path=clickhouse_temp_df_path,
                table_name=name,
                clickhouse_url=self.jdbc_url,
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
            remove_partitioned_parquet(clickhouse_temp_df_path)

            run_time = time.time() - start_time
            message = dict(table_name=name, status='success', run_time=run_time)
            df.unpersist()
            gc.collect()
            logger.info(message)

        except Exception as e:
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

            if self.pass_on_error:
                logger.warning(message)
                return
            else:
                logger.error(message)
                raise Exception(message) from e
        return
