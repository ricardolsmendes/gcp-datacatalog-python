import json
import os

from google.cloud import datacatalog

import load_template_google_sheets

TEST_PROJECT_ID = os.environ['GOOGLE_CLOUD_TEST_PROJECT_ID']


def test_tempate_maker_run():
    with open(f'{os.getcwd()}/sample-input/load-template-google-sheets/template-abc.gsheet') \
            as json_file:
        spreadsheet_id = json.load(json_file)['doc_id']

    load_template_google_sheets.TemplateMaker().run(
        spreadsheet_id=spreadsheet_id,
        project_id=TEST_PROJECT_ID,
        template_id='template_abc',
        display_name='Testing Load Tag Templates from Google Sheets',
        delete_existing=True)

    location_name = f'projects/{TEST_PROJECT_ID}/locations/us-central1'
    main_template_name = f'{location_name}/tagTemplates/template_abc'
    multivalued_field_template_name = \
        f'{location_name}/tagTemplates/template_abc_multivalued_field_xyz'

    datacatalog_client = datacatalog.DataCatalogClient()

    assert datacatalog_client.get_tag_template(name=main_template_name)
    assert datacatalog_client.get_tag_template(name=multivalued_field_template_name)

    # Clean up.
    datacatalog_client.delete_tag_template(name=main_template_name, force=True)
    datacatalog_client.delete_tag_template(name=multivalued_field_template_name, force=True)
