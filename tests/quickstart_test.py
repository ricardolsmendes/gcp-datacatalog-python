import os
import pytest

from google.cloud import bigquery

from quickstart import search_catalog

ORGANIZATION_ID = os.environ['GOOGLE_CLOUD_ORGANIZATION_ID']
PROJECT_ID = os.environ['GOOGLE_CLOUD_PROJECT_ID']

bigquery = bigquery.Client()


@pytest.fixture
def dataset():
    name = f'{PROJECT_ID}.quickstart_test_dataset'
    dataset = bigquery.create_dataset(name)
    yield dataset
    bigquery.delete_dataset(dataset)


def test_search_catalog_failure():
    results = search_catalog(ORGANIZATION_ID, 'system=bigquery type=dataset nonexistent_dataset')
    assert len(results) == 0


def test_search_catalog_success(dataset):
    results = search_catalog(ORGANIZATION_ID, 'system=bigquery type=dataset quickstart_test_dataset')
    assert len(results) > 0
