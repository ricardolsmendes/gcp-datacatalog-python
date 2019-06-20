import os

from quickstart import search_catalog

ORGANIZATION_ID = os.environ['GOOGLE_CLOUD_ORGANIZATION_ID']


def test_search_catalog_failure():
    results = search_catalog(ORGANIZATION_ID, 'system=bigquery type=dataset nonexistent_dataset')
    assert len(results) == 0


def test_search_catalog_success():
    results = search_catalog(ORGANIZATION_ID, 'system=bigquery type=dataset test')
    assert len(results) > 0
