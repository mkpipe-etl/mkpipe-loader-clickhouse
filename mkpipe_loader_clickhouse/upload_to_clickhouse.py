import os
import requests
from mkpipe.utils import log_container, Logger


def upload_folder(folder_path, table_name, clickhouse_url):
    logger = Logger(__file__)
    try:
        # Walk through the directory
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)

                    """Uploads a single Parquet file to ClickHouse."""
                    url = f'{clickhouse_url}&query=INSERT INTO {table_name} FORMAT Parquet'

                    with open(file_path, 'rb') as f:
                        response = requests.post(url, data=f)

                        if response.status_code == 200:
                            message = dict(
                                table_name=table_name,
                                status='loading',
                                message=f'Successfully uploaded {file_path}',
                            )
                            logger.info(message)
                        else:
                            message = dict(
                                table_name=table_name,
                                status='loading',
                                message=f'Failed to upload {file_path}. Status Code: {response.status_code} Response: {response.text}',
                            )
                            logger.error(message)

        message = dict(
            table_name=table_name,
            status='loading',
            message=f"All files in '{folder_path}' have been uploaded to table '{table_name}'.",
        )
        logger.info(message)

    except FileNotFoundError:
        message = dict(
            table_name=table_name,
            status='loading',
            message=f'The folder {folder_path} was not found.',
        )
        logger.error(message)
        raise
    except Exception as e:
        message = dict(
            table_name=table_name, status='loading', message=f'An error occurred: {e}'
        )
        logger.error(message)
        raise

    return message
