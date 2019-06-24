import os

from google.api_core.exceptions import PermissionDenied

from .bigquery import dataset, table

from quickstart import DataCatalogHelper

ORGANIZATION_ID = os.environ['GOOGLE_CLOUD_ORGANIZATION_ID']
PROJECT_ID = os.environ['GOOGLE_CLOUD_PROJECT_ID']

datacatalog_helper = DataCatalogHelper()


def test_lookup_entry_with_result(table):
    assert datacatalog_helper.lookup_entry(f'//bigquery.googleapis.com/projects/{PROJECT_ID}'
                                           f'/datasets/quickstart_test_dataset/tables/quickstart_test_table_2')


def test_lookup_entry_with_no_result(table):
    try:
        datacatalog_helper.lookup_entry(f'//bigquery.googleapis.com/projects/{PROJECT_ID}'
                                        f'/datasets/quickstart_test_dataset/tables/quickstart_test_table_20')
        assert False
    except PermissionDenied:
        assert True


def test_search_catalog_bigquery_dataset_with_results(dataset):
    results = datacatalog_helper.search_catalog(
        ORGANIZATION_ID, 'system=bigquery type=dataset quickstart')
    assert len(results) > 0
    assert next(result for result in results if 'quickstart_test_dataset' in result.linked_resource)


def test_search_catalog_bigquery_dataset_with_no_results():
    results = datacatalog_helper.search_catalog(
        ORGANIZATION_ID, 'system=bigquery type=dataset abc_xyz')
    assert len(results) == 0


def test_search_catalog_bigquery_table_column_with_results(table):
    results = datacatalog_helper.search_catalog(
        ORGANIZATION_ID, 'system=bigquery type=table column:email')
    assert len(results) > 0
    assert next(result for result in results if 'quickstart_test_table_2' in result.linked_resource)


def test_search_catalog_bigquery_table_column_with_no_results():
    results = datacatalog_helper.search_catalog(
        ORGANIZATION_ID, 'system=bigquery type=table column:abc-xyz')
    assert len(results) == 0
