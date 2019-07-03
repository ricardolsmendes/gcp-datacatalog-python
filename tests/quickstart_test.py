import os

from google.api_core.exceptions import InvalidArgument, PermissionDenied

from .bigquery import dataset, table
from .datacatalog import tag_template

from quickstart import DataCatalogHelper

TEST_ORGANIZATION_ID = os.environ['GOOGLE_CLOUD_TEST_ORGANIZATION_ID']
TEST_PROJECT_ID = os.environ['GOOGLE_CLOUD_TEST_PROJECT_ID']

datacatalog_helper = DataCatalogHelper()


def test_create_tag_template():
    template = datacatalog_helper.create_tag_template(
        project_id=TEST_PROJECT_ID,
        template_id='quickstart_test_template',
        display_name='Testing Tag Templates',
        primitive_fields_descriptors=[
            {'id': 'boolean_field', 'type': 'BOOL', 'display_name': 'Testing boolean fields'},
            {'id': 'double_field', 'type': 'DOUBLE', 'display_name': 'Testing double fields'},
            {'id': 'string_field', 'type': 'STRING', 'display_name': 'Testing string fields'},
            {'id': 'datetime_field', 'type': 'TIMESTAMP', 'display_name': 'Testing timestamp fields'}
        ]
    )

    assert template.name == f'projects/{TEST_PROJECT_ID}/locations/us-central1/tagTemplates/quickstart_test_template'

    # Clean up.
    datacatalog_helper.delete_tag_template(template.name)


def test_create_tag_template_field(tag_template):
    field = datacatalog_helper.create_tag_template_field(
        template_name=tag_template.name,
        field_id='quickstart_test_tag_template_field',
        display_name='Testing enum fields',
        enum_values=[
            {'display_name': 'VALUE 1'},
            {'display_name': 'VALUE 2'}
        ]
    )

    assert field.type.enum_type

    # Clean up.
    datacatalog_helper.delete_tag_template_field(
        name=f'{tag_template.name}/fields/quickstart_test_tag_template_field')


def test_get_entry_success(table):
    results = datacatalog_helper.search_catalog(
        TEST_ORGANIZATION_ID, 'system=bigquery type=table name:quickstart_table_2')

    table_resource_name = \
        f'//bigquery.googleapis.com/projects/{TEST_PROJECT_ID}' \
        f'/datasets/data_catalog_quickstart/tables/quickstart_table_2'
    table_search_result = next(result for result in results if result.linked_resource == table_resource_name)

    assert datacatalog_helper.get_entry(table_search_result.relative_resource_name)


def test_get_entry_failure_invalid_argument(table):
    try:
        datacatalog_helper.lookup_entry(f'projects/{TEST_PROJECT_ID}/locations/US/entryGroups/@bigquery/entries/abc')
        assert False
    except InvalidArgument:
        assert True


def test_lookup_entry_success(table):
    assert datacatalog_helper.lookup_entry(f'//bigquery.googleapis.com/projects/{TEST_PROJECT_ID}'
                                           f'/datasets/quickstart_test_dataset/tables/quickstart_test_table_2')


def test_lookup_entry_failure_permission_denied(table):
    try:
        datacatalog_helper.lookup_entry(f'//bigquery.googleapis.com/projects/{TEST_PROJECT_ID}'
                                        f'/datasets/quickstart_test_dataset/tables/quickstart_test_table_20')
        assert False
    except PermissionDenied:
        assert True


def test_search_catalog_bigquery_dataset_with_results(dataset):
    results = datacatalog_helper.search_catalog(
        TEST_ORGANIZATION_ID, 'system=bigquery type=dataset name:quickstart')
    assert len(results) > 0
    assert next(result for result in results if 'quickstart_test_dataset' in result.linked_resource)


def test_search_catalog_bigquery_dataset_with_no_results():
    results = datacatalog_helper.search_catalog(
        TEST_ORGANIZATION_ID, 'system=bigquery type=dataset name:abc_xyz')
    assert len(results) == 0


def test_search_catalog_bigquery_table_column_with_results(table):
    results = datacatalog_helper.search_catalog(
        TEST_ORGANIZATION_ID, 'system=bigquery type=table column:email')
    assert len(results) > 0
    assert next(result for result in results if 'quickstart_test_table_2' in result.linked_resource)


def test_search_catalog_bigquery_table_column_with_no_results():
    results = datacatalog_helper.search_catalog(
        TEST_ORGANIZATION_ID, 'system=bigquery type=table column:abc-xyz')
    assert len(results) == 0
