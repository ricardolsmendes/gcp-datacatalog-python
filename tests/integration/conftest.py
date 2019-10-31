import os
import pytest
import time
import uuid

from google.api_core.exceptions import PermissionDenied
from google.cloud import bigquery
from google.cloud import datacatalog
from google.cloud.datacatalog import enums, types

TEST_PROJECT_ID = os.environ['GOOGLE_CLOUD_TEST_PROJECT_ID']

bigquery_client = bigquery.Client()
datacatalog_client = datacatalog.DataCatalogClient()


def __generate_uuid(length=5):
    random = str(uuid.uuid4())  # Convert UUID format to a Python string
    random = random.replace('-', '')  # Remove the '-' character
    return random[0:length]  # Return the random string


@pytest.fixture
def bigquery_dataset(scope='function'):
    name = f'{TEST_PROJECT_ID}.{__generate_uuid()}_quickstart_test_dataset'
    dataset = bigquery_client.create_dataset(name)

    time.sleep(2)  # Wait a few seconds for Data Catalog's search index sync/update.
    yield dataset

    bigquery_client.delete_dataset(dataset)
    time.sleep(2)  # Wait a few seconds for Data Catalog's search index sync/update.


@pytest.fixture
def bigquery_table(bigquery_dataset, scope='function'):
    name = f'{TEST_PROJECT_ID}.{bigquery_dataset.dataset_id}.{__generate_uuid()}_quickstart_test_table'
    schema = [
        bigquery.SchemaField('name', 'STRING', 'REQUIRED'),
        bigquery.SchemaField('email', 'STRING', 'REQUIRED')
    ]
    table = bigquery_client.create_table(bigquery.Table(name, schema=schema))

    time.sleep(2)  # Wait a few seconds for Data Catalog's search index sync/update.
    yield table

    bigquery_client.delete_table(table)
    time.sleep(2)  # Wait a few seconds for Data Catalog's search index sync/update.


@pytest.fixture
def datacatalog_table_entry(bigquery_table, scope='function'):
    entry = datacatalog_client.lookup_entry(
        linked_resource=f'//bigquery.googleapis.com/projects/{bigquery_table.project}'
                        f'/datasets/{bigquery_table.dataset_id}/tables/{bigquery_table.table_id}')

    yield entry


@pytest.fixture
def datacatalog_tag(datacatalog_table_entry, datacatalog_tag_template, scope='function'):
    tag = types.Tag()
    tag.template = datacatalog_tag_template.name

    tag.fields['boolean_field'].bool_value = True
    tag.fields['double_field'].double_value = 10.5
    tag.fields['string_field'].string_value = 'test'
    tag.fields['timestamp_field'].timestamp_value.FromJsonString('2019-07-04T01:00:30Z')
    tag.fields['enum_field'].enum_value.display_name = 'VALUE 1'

    tag = datacatalog_client.create_tag(parent=datacatalog_table_entry.name, tag=tag)

    time.sleep(2)  # Wait a few seconds for Data Catalog's search index sync/update.
    yield tag

    datacatalog_client.delete_tag(tag.name)
    time.sleep(2)  # Wait a few seconds for Data Catalog's search index sync/update.


@pytest.fixture
def datacatalog_tag_template(scope='function'):
    location = datacatalog.DataCatalogClient.location_path(TEST_PROJECT_ID, 'us-central1')

    # Delete a Tag Template with the same name if it already exists.
    try:
        datacatalog_client.delete_tag_template(
            name=f'{location}/tagTemplates/{__generate_uuid()}_quickstart_test_tag_template', force=True)
    except PermissionDenied:
        pass

    template = types.TagTemplate()
    template.fields['boolean_field'].type.primitive_type = enums.FieldType.PrimitiveType.BOOL
    template.fields['double_field'].type.primitive_type = enums.FieldType.PrimitiveType.DOUBLE
    template.fields['string_field'].type.primitive_type = enums.FieldType.PrimitiveType.STRING
    template.fields['timestamp_field'].type.primitive_type = enums.FieldType.PrimitiveType.TIMESTAMP

    template.fields['enum_field'].type.enum_type.allowed_values.add().display_name = 'VALUE 1'
    template.fields['enum_field'].type.enum_type.allowed_values.add().display_name = 'VALUE 2'

    tag_template = datacatalog_client.create_tag_template(
        parent=location, tag_template_id='quickstart_test_tag_template', tag_template=template)

    time.sleep(2)  # Wait a few seconds for Data Catalog's search index sync/update.
    yield tag_template

    datacatalog_client.delete_tag_template(tag_template.name, force=True)
    time.sleep(2)  # Wait a few seconds for Data Catalog's search index sync/update.
