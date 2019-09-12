import os

from google.cloud import datacatalog_v1beta1

from load_template_csv import TemplateMaker

TEST_PROJECT_ID = os.environ['GOOGLE_CLOUD_TEST_PROJECT_ID']


def test_tempate_maker_run():
    TemplateMaker().run(
        files_folder=f'{os.getcwd()}/sample-input/load-template-csv',
        project_id=TEST_PROJECT_ID,
        template_id='template_abc',
        display_name='Testing Load Tag Templates from CSV files',
        delete_existing=True)

    location_name = f'projects/{TEST_PROJECT_ID}/locations/us-central1'
    main_template_name = f'{location_name}/tagTemplates/template_abc'
    multivalued_field_template_name = f'{location_name}/tagTemplates/template_abc_multivalued_field_xyz'

    datacatalog = datacatalog_v1beta1.DataCatalogClient()

    assert datacatalog.get_tag_template(name=main_template_name)
    assert datacatalog.get_tag_template(name=multivalued_field_template_name)

    # Clean up.
    datacatalog.delete_tag_template(name=main_template_name, force=True)
    datacatalog.delete_tag_template(name=multivalued_field_template_name, force=True)
