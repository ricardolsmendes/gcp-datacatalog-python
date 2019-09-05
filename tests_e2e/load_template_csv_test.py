import os

from google.cloud import datacatalog_v1beta1

from load_template_csv import TemplateMaker

TEST_FILES_FOLDER = os.environ['INPUT_FILES_FOLDER']
TEST_PROJECT_ID = os.environ['GOOGLE_CLOUD_TEST_PROJECT_ID']


def test_tempate_maker_run():
    TemplateMaker().run(
        files_folder=TEST_FILES_FOLDER,
        project_id=TEST_PROJECT_ID,
        template_id='template_abc',
        display_name='Testing Load Tag Templates from CSV files',
        delete_existing=True)

    location_name = f'projects/{TEST_PROJECT_ID}/locations/us-central1'
    main_template_name = f'{location_name}/tagTemplates/template_abc'
    multivalued_attr_template_name = f'{location_name}/tagTemplates/template_abc_multivalued_attribute_xyz'

    datacatalog = datacatalog_v1beta1.DataCatalogClient()

    assert datacatalog.get_tag_template(name=main_template_name)
    assert datacatalog.get_tag_template(name=multivalued_attr_template_name)

    # Clean up.
    datacatalog.delete_tag_template(name=main_template_name, force=True)
    datacatalog.delete_tag_template(name=multivalued_attr_template_name, force=True)
