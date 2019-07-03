import os
import pytest

from google.api_core.exceptions import PermissionDenied
from google.cloud import datacatalog_v1beta1

TEST_PROJECT_ID = os.environ['GOOGLE_CLOUD_TEST_PROJECT_ID']

datacatalog_client = datacatalog_v1beta1.DataCatalogClient()


@pytest.fixture
def tag_template(scope='function'):
    location = datacatalog_client.location_path(TEST_PROJECT_ID, 'us-central1')

    # Delete a Tag Template with the same name if it already exists.
    try:
        datacatalog_client.delete_tag_template(
            name=f'{location}/tagTemplates/quickstart_test_tag_template', force=True)
    except PermissionDenied:
        pass

    template = datacatalog_v1beta1.types.TagTemplate()
    template.fields['initial_bool_field'].type.primitive_type = \
        datacatalog_v1beta1.enums.FieldType.PrimitiveType.BOOL

    tag_template = datacatalog_client.create_tag_template(
        parent=location, tag_template_id='quickstart_test_tag_template', tag_template=template)

    yield tag_template

    datacatalog_client.delete_tag_template(tag_template.name, force=True)
