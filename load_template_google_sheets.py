"""
This application demonstrates how to create a Tag Template in Data Catalog,
loading its information from Google Sheets.
"""
import argparse
import logging
import re
import stringcase
import unicodedata

from google.api_core.exceptions import PermissionDenied
from google.cloud import datacatalog_v1beta1
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials


"""
Constants
========================================
"""


_CLOUD_PLATFORM_REGION = 'us-central1'

_CUSTOM_MULTIVALUED_TYPE = 'MULTI'
_DATA_CATALOG_BOOL_TYPE = 'BOOL'
_DATA_CATALOG_ENUM_TYPE = 'ENUM'
_DATA_CATALOG_NATIVE_TYPES = ['BOOL', 'DOUBLE', 'ENUM', 'STRING', 'TIMESTAMP']

_LOOKING_FOR_SHEET_LOG_FORMAT = 'Looking for {} sheet {} | {}...'


"""
Template maker
========================================
"""


class TemplateMaker:

    def __init__(self):
        self.__sheets_reader = GoogleSheetsReader()
        self.__datacatalog_helper = DataCatalogHelper()

    def run(self, spreadsheet_id, project_id, template_id, display_name, delete_existing=False):
        master_template_fields = self.__sheets_reader.read_master(spreadsheet_id, stringcase.spinalcase(template_id))
        self.__process_native_fields(spreadsheet_id, project_id, template_id, display_name,
                                     master_template_fields, delete_existing)
        self.__process_custom_multivalued_fields(spreadsheet_id, project_id, template_id, display_name,
                                                 master_template_fields, delete_existing)

    def __process_native_fields(self, spreadsheet_id, project_id, template_id, display_name,
                                master_template_fields, delete_existing_template):

        native_fields = self.__filter_fields_by_types(master_template_fields, _DATA_CATALOG_NATIVE_TYPES)
        StringFormatter.format_elements_to_snakecase(native_fields, 0)

        enums_names = {}
        for field in native_fields:
            if not field[2] == _DATA_CATALOG_ENUM_TYPE:
                continue

            names_from_sheet = self.__sheets_reader.read_helper(spreadsheet_id, stringcase.spinalcase(field[0]))
            enums_names[field[0]] = [name[0] for name in names_from_sheet]

        template_name = datacatalog_v1beta1.DataCatalogClient.tag_template_path(
            project_id, _CLOUD_PLATFORM_REGION, template_id)

        if delete_existing_template:
            self.__datacatalog_helper.delete_tag_template(template_name)

        if not self.__datacatalog_helper.tag_template_exists(template_name):
            self.__datacatalog_helper.create_tag_template(
                project_id, template_id, display_name, native_fields, enums_names)

    def __process_custom_multivalued_fields(self, spreadsheet_id, project_id, template_id, display_name,
                                            master_template_fields, delete_existing_template):

        multivalued_fields = self.__filter_fields_by_types(master_template_fields, [_CUSTOM_MULTIVALUED_TYPE])
        StringFormatter.format_elements_to_snakecase(multivalued_fields, 0)

        for field in multivalued_fields:
            try:
                values_from_sheet = self.__sheets_reader.read_helper(spreadsheet_id, stringcase.spinalcase(field[0]))
                fields = [(StringFormatter.format_to_snakecase(value[0]), value[0], _DATA_CATALOG_BOOL_TYPE)
                          for value in values_from_sheet]
            except HttpError as err:
                if err.resp.status in [400]:
                    logging.info('NOT FOUND. Ignoring...')
                    continue  # Ignore creating a new template representing the multivalued field
                else:
                    raise

            custom_template_id = f'{template_id}_{field[0]}'
            custom_display_name = f'{display_name} - {field[1]}'

            template_name = datacatalog_v1beta1.DataCatalogClient.tag_template_path(
                project_id, _CLOUD_PLATFORM_REGION, custom_template_id)

            if delete_existing_template:
                self.__datacatalog_helper.delete_tag_template(template_name)

            if not self.__datacatalog_helper.tag_template_exists(template_name):
                self.__datacatalog_helper.create_tag_template(
                    project_id, custom_template_id, custom_display_name, fields)

    @classmethod
    def __filter_fields_by_types(cls, fields, types):
        return [field for field in fields if field[2] in types]


"""
Input reader
========================================
"""


class GoogleSheetsReader:

    def __init__(self):
        self.__sheets_helper = GoogleSheetsHelper()

    def read_master(self, spreadsheet_id, sheet_name, values_per_line=3):
        return self.__read(spreadsheet_id, sheet_name, 'master', values_per_line)

    def read_helper(self, spreadsheet_id, sheet_name, values_per_line=1):
        return self.__read(spreadsheet_id, sheet_name, 'helper', values_per_line)

    def __read(self, spreadsheet_id, sheet_name, sheet_type, values_per_line):
        """
        Read the requested values from each line and store them into a list.

        :param spreadsheet_id: Spreadsheet ID.
        :param sheet_name: Sheet name.
        :param sheet_type: Sheet type {'master', 'helper'}.
        :param values_per_line: Number of consecutive values to be read from each line.
        """
        logging.info(_LOOKING_FOR_SHEET_LOG_FORMAT.format(sheet_type, spreadsheet_id, sheet_name))
        sheet_data = self.__sheets_helper.read_sheet(spreadsheet_id, sheet_name, values_per_line)

        data = []

        logging.info(f'Reading spreadsheet {spreadsheet_id} | {sheet_name}...')
        for row in sheet_data.get('valueRanges')[0].get('values'):
            row_data = []
            for counter in range(values_per_line):
                row_data.append(row[counter].strip())
            data.append(row_data)

        # The first line is usually used for headers, so it's discarded.
        del (data[0])

        logging.info('DONE')
        return data


"""
API communication classes
========================================
"""


class DataCatalogHelper:
    """
    Manage Templates by communicating to Data Catalog's API.
    """

    def __init__(self):
        # Initialize the API client.
        self.__datacatalog = datacatalog_v1beta1.DataCatalogClient()

    def create_tag_template(self, project_id, template_id, display_name, fields_descriptors, enums_names=None):
        """Create a Tag Template."""

        location = datacatalog_v1beta1.DataCatalogClient.location_path(project_id, _CLOUD_PLATFORM_REGION)

        tag_template = datacatalog_v1beta1.types.TagTemplate()
        tag_template.display_name = display_name

        for descriptor in fields_descriptors:
            field_id = descriptor[0]
            field_type = descriptor[2]

            tag_template.fields[field_id].display_name = descriptor[1]

            if not field_type == _DATA_CATALOG_ENUM_TYPE:
                tag_template.fields[field_id].type.primitive_type = \
                    datacatalog_v1beta1.enums.FieldType.PrimitiveType[field_type]
            else:
                for enum_name in enums_names[field_id]:
                    tag_template.fields[field_id].type.enum_type.allowed_values.add().display_name = enum_name

        created_tag_template = self.__datacatalog.create_tag_template(
            parent=location, tag_template_id=template_id, tag_template=tag_template)

        logging.info(f'===> Template created: {created_tag_template.name}')

    def delete_tag_template(self, name):
        """Delete a Tag Template."""

        try:
            self.__datacatalog.delete_tag_template(name=name, force=True)
            logging.info(f'===> Template deleted: {name}')
        except PermissionDenied:
            pass

    def tag_template_exists(self, name):
        """Check if a Tag Template with the provided name already exists."""

        try:
            self.__datacatalog.get_tag_template(name=name)
            return True
        except PermissionDenied:
            return False


class GoogleSheetsHelper:
    """
    Access spreadsheets data by communicating to the Google Sheets API.
    """

    def __init__(self):
        # Initialize the API client.
        self.__service = discovery.build(
            serviceName='sheets', version='v4',
            credentials=ServiceAccountCredentials.get_application_default(), cache_discovery=False)

    def read_sheet(self, spreadsheet_id, sheet_name, values_per_line):
        return self.__service.spreadsheets().values().batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=f'{sheet_name}!A:{chr(ord("@") + values_per_line)}').execute()


"""
Tools & utilities
========================================
"""


class StringFormatter:

    @classmethod
    def format_elements_to_snakecase(cls, a_list, internal_index=None):
        if internal_index is None:
            for counter in range(len(a_list)):
                a_list[counter] = cls.format_to_snakecase(a_list[counter])
        else:
            for element in a_list:
                element[internal_index] = cls.format_to_snakecase(element[internal_index])

    @classmethod
    def format_to_snakecase(cls, string):
        normalized_str = unicodedata.normalize('NFKD', string).encode('ASCII', 'ignore').decode()
        normalized_str = re.sub(r'[^a-zA-Z0-9]+', ' ', normalized_str)
        normalized_str = normalized_str.strip()
        normalized_str = normalized_str.lower() if (' ' in normalized_str) or (normalized_str.isupper()) \
            else stringcase.camelcase(normalized_str)  # FooBarBaz => fooBarBaz

        return stringcase.snakecase(normalized_str)  # foo-bar-baz => foo_bar_baz


"""
Main program entry point
========================================
"""
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleapiclient.discovery').setLevel(logging.ERROR)
    logging.getLogger('oauth2client.client').setLevel(logging.ERROR)
    logging.getLogger('oauth2client.transport').setLevel(logging.ERROR)

    parser = argparse.ArgumentParser(description='Load Tag Template from CSV')

    parser.add_argument('--template-id', help='the template ID', required=True)
    parser.add_argument('--display-name', help='template\'s Display Name', required=True)
    parser.add_argument('--project-id', help='GCP Project in which the Template will be created', required=True)
    parser.add_argument('--spreadsheet-id', help='Google Spreadsheet ID', required=True)
    parser.add_argument('--delete-existing', action='store_true',
                        help='delete existing Templates and recreate them with the provided metadata')

    args = parser.parse_args()

    TemplateMaker().run(
        args.spreadsheet_id, args.project_id, args.template_id, args.display_name, args.delete_existing)
