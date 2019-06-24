import os
import pytest

from google.cloud import bigquery

PROJECT_ID = os.environ['GOOGLE_CLOUD_PROJECT_ID']

bigquery_client = bigquery.Client()


@pytest.fixture
def dataset(scope='function'):
    name = f'{PROJECT_ID}.quickstart_test_dataset'
    dataset = bigquery_client.create_dataset(name)
    yield dataset
    bigquery_client.delete_dataset(dataset)


@pytest.fixture
def table(dataset, scope='function'):
    name = f'{PROJECT_ID}.{dataset.dataset_id}.quickstart_test_table_2'
    schema = [
        bigquery.SchemaField('name', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('email', 'STRING', 'REQUIRED')
    ]
    table = bigquery_client.create_table(bigquery.Table(name, schema=schema))
    yield table
    bigquery_client.delete_table(table)
