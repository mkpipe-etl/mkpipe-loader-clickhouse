import os
import requests


def upload_folder(folder_path, table_name, clickhouse_url):
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
                            print(f'Successfully uploaded {file_path}')
                        else:
                            print(
                                f'Failed to upload {file_path}. Status Code: {response.status_code}'
                            )
                            print(f'Response: {response.text}')

        message = (
            f"All files in '{folder_path}' have been uploaded to table '{table_name}'."
        )

    except FileNotFoundError:
        print(f'The folder {folder_path} was not found.')
        raise
    except Exception as e:
        print(f'An error occurred: {e}')
        raise

    return message
