import os
import re

from google.api_core.exceptions import InvalidArgument, PermissionDenied
from google.cloud import datacatalog
from google.cloud.datacatalog import enums

from quickstart import DataCatalogFacade

TEST_ORGANIZATION_ID = os.environ['GOOGLE_CLOUD_TEST_ORGANIZATION_ID']
TEST_PROJECT_ID = os.environ['GOOGLE_CLOUD_TEST_PROJECT_ID']

datacatalog = datacatalog.DataCatalogClient()
datacatalog_facade = DataCatalogFacade()


def test_datacatalog_facade_search_catalog_bigquery_dataset_with_results(bigquery_dataset):
    results = datacatalog_facade.search_catalog(TEST_ORGANIZATION_ID, 'system=bigquery type=dataset')
    assert len(results) > 0
    assert next(result for result in results if bigquery_dataset.dataset_id in result.linked_resource)


def test_datacatalog_facade_search_catalog_bigquery_dataset_with_no_results():
    results = datacatalog_facade.search_catalog(TEST_ORGANIZATION_ID, 'system=bigquery type=dataset name:abc_xyz')
    assert len(results) == 0


def test_datacatalog_facade_search_catalog_bigquery_table_column_with_results(bigquery_table):
    results = datacatalog_facade.search_catalog(TEST_ORGANIZATION_ID, 'system=bigquery type=table column:email')
    assert len(results) > 0
    assert next(result for result in results if bigquery_table.table_id in result.linked_resource)


def test_datacatalog_facade_search_catalog_bigquery_table_column_with_no_results():
    results = datacatalog_facade.search_catalog(TEST_ORGANIZATION_ID, 'system=bigquery type=table column:abc_xyz')
    assert len(results) == 0


def test_datacatalog_facade_search_catalog_tag_template_with_results(datacatalog_tag):
    entry_name = re.search('(.+?)/tags/(.+?)$', datacatalog_tag.name).group(1)
    linked_resource = datacatalog_facade.get_entry(entry_name).linked_resource

    template_id = re.search('(.+?)/tagTemplates/(.+?)$', datacatalog_tag.template).group(2)

    results = datacatalog_facade.search_catalog(TEST_ORGANIZATION_ID, f'tag:{template_id}')

    assert len(results) > 0
    assert next(result for result in results if linked_resource == result.linked_resource)


def test_datacatalog_facade_search_catalog_tag_template_with_no_results():
    results = datacatalog_facade.search_catalog(TEST_ORGANIZATION_ID, 'tag:abc_xyz')
    assert len(results) == 0


def test_datacatalog_facade_search_catalog_tag_value_with_results(datacatalog_tag):
    entry_name = re.search('(.+?)/tags/(.+?)$', datacatalog_tag.name).group(1)
    linked_resource = datacatalog_facade.get_entry(entry_name).linked_resource

    template_id = re.search('(.+?)/tagTemplates/(.+?)$', datacatalog_tag.template).group(2)

    results = datacatalog_facade.search_catalog(TEST_ORGANIZATION_ID, f'tag:{template_id}.double_field=10.5')

    assert len(results) > 0
    assert next(result for result in results if linked_resource == result.linked_resource)


def test_datacatalog_facade_search_catalog_tag_value_with_no_results():
    results = datacatalog_facade.search_catalog(TEST_ORGANIZATION_ID, 'tag:quickstart.double_field=10.5')
    assert len(results) == 0


def test_datacatalog_facade_get_entry(bigquery_table):
    results = datacatalog_facade.search_catalog(
        TEST_ORGANIZATION_ID, f'system=bigquery type=table name:{bigquery_table.table_id}')

    table_resource_name = f'//bigquery.googleapis.com/projects/{bigquery_table.project}'\
                          f'/datasets/{bigquery_table.dataset_id}/tables/{bigquery_table.table_id}'
    table_search_result = next(result for result in results if result.linked_resource == table_resource_name)

    assert datacatalog.get_entry(name=table_search_result.relative_resource_name)


def test_datacatalog_facade_get_entry_fail_invalid_argument(bigquery_table):
    try:
        datacatalog_facade.lookup_entry(
            f'projects/{bigquery_table.project}/locations/US/entryGroups/@bigquery/entries/quickstart')
        assert False
    except InvalidArgument:
        assert True


def test_datacatalog_facade_lookup_entry(bigquery_table):
    assert datacatalog_facade.lookup_entry(f'//bigquery.googleapis.com/projects/{bigquery_table.project}'
                                           f'/datasets/{bigquery_table.dataset_id}/tables/{bigquery_table.table_id}')


def test_datacatalog_facade_lookup_entry_fail_permission_denied(bigquery_table):
    try:
        datacatalog_facade.lookup_entry(f'//bigquery.googleapis.com/projects/{bigquery_table.project}'
                                        f'/datasets/{bigquery_table.dataset_id}/tables/quickstart')
        assert False
    except PermissionDenied:
        assert True


def test_datacatalog_facade_create_tag_template():
    template = datacatalog_facade.create_tag_template(
        project_id=TEST_PROJECT_ID,
        template_id='quickstart_test_template',
        display_name='Testing Tag Templates',
        primitive_fields_descriptors=[
            {
                'id': 'boolean_field',
                'primitive_type': enums.FieldType.PrimitiveType.BOOL,
                'display_name': 'Testing boolean fields'
            },
            {
                'id': 'double_field',
                'primitive_type': enums.FieldType.PrimitiveType.DOUBLE,
                'display_name': 'Testing double fields'
            },
            {
                'id': 'string_field',
                'primitive_type': enums.FieldType.PrimitiveType.STRING,
                'display_name': 'Testing string fields'
            },
            {
                'id': 'datetime_field',
                'primitive_type': enums.FieldType.PrimitiveType.TIMESTAMP,
                'display_name': 'Testing timestamp fields'
            }
        ]
    )

    assert template.name == f'projects/{TEST_PROJECT_ID}/locations/us-central1/tagTemplates/quickstart_test_template'

    # Clean up.
    datacatalog.delete_tag_template(name=template.name, force=True)


def test_datacatalog_facade_create_tag_template_field(datacatalog_tag_template):
    field = datacatalog_facade.create_tag_template_field(
        template_name=datacatalog_tag_template.name,
        field_id='quickstart_test_tag_template_enum_field',
        display_name='Testing enum fields',
        enum_values=[
            {'display_name': 'VALUE 1'},
            {'display_name': 'VALUE 2'}
        ]
    )

    assert field.type.enum_type

    # Clean up.
    datacatalog.delete_tag_template_field(
        name=f'{datacatalog_tag_template.name}/fields/quickstart_test_tag_template_enum_field', force=True)


def test_datacatalog_facade_create_tag(bigquery_table, datacatalog_tag_template):
    entry = datacatalog_facade.lookup_entry(f'//bigquery.googleapis.com/projects/{bigquery_table.project}'
                                            f'/datasets/{bigquery_table.dataset_id}/tables/{bigquery_table.table_id}')

    tag = datacatalog_facade.create_tag(
        entry=entry,
        tag_template=datacatalog_tag_template,
        fields_descriptors=[
            {
                'id': 'boolean_field',
                'primitive_type': enums.FieldType.PrimitiveType.BOOL,
                'value': True
            },
            {
                'id': 'double_field',
                'primitive_type': enums.FieldType.PrimitiveType.DOUBLE,
                'value': 10.5
            },
            {
                'id': 'string_field',
                'primitive_type': enums.FieldType.PrimitiveType.STRING,
                'value': 'test'
            },
            {
                'id': 'timestamp_field',
                'primitive_type': enums.FieldType.PrimitiveType.TIMESTAMP,
                'value': '2019-07-04T01:00:30Z'
            },
            {
                'id': 'enum_field',
                'primitive_type': None,
                'value': 'VALUE 1'
            }
        ]
    )

    assert entry.name in tag.name

    # Clean up.
    datacatalog.delete_tag(name=tag.name)
