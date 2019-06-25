import os

from google.api_core.exceptions import InvalidArgument, PermissionDenied

from .bigquery import dataset, table

from quickstart import DataCatalogHelper

ORGANIZATION_ID = os.environ['GOOGLE_CLOUD_ORGANIZATION_ID']
PROJECT_ID = os.environ['GOOGLE_CLOUD_PROJECT_ID']

datacatalog_helper = DataCatalogHelper()


def test_get_entry_success(table):
    results = datacatalog_helper.search_catalog(
        ORGANIZATION_ID, 'system=bigquery type=table quickstart_table_2')

    table_resource_name = \
        f'//bigquery.googleapis.com/projects/{PROJECT_ID}' \
        f'/datasets/data_catalog_quickstart/tables/quickstart_table_2'
    table_search_result = next(result for result in results if result.linked_resource == table_resource_name)

    assert datacatalog_helper.get_entry(table_search_result.relative_resource_name)


def test_get_entry_failure_invalid_argument(table):
    try:
        datacatalog_helper.lookup_entry(f'projects/{PROJECT_ID}/locations/US/entryGroups/@bigquery/entries/abc')
        assert False
    except InvalidArgument:
        assert True


def test_lookup_entry_success(table):
    assert datacatalog_helper.lookup_entry(f'//bigquery.googleapis.com/projects/{PROJECT_ID}'
                                           f'/datasets/quickstart_test_dataset/tables/quickstart_test_table_2')


def test_lookup_entry_failure_permission_denied(table):
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
