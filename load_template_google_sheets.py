"""
This application demonstrates how to create a Tag Template in Data Catalog
based on information retrieved from Google Sheets.
"""

import argparse
import logging
import re
import stringcase
import unicodedata

from google.api_core.exceptions import PermissionDenied
from google.cloud import datacatalog_v1beta1
from googleapiclient.discovery import build
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

_LOOKING_FOR_SHEET_FORMAT = 'Looking for {} sheet {} | {}...'


"""
Template maker
========================================
"""


class TemplateMaker:

    @staticmethod
    def __filter_attributes_by_types(attributes, types):
        filtered_attributes = []
        for attribute in attributes:
            if attribute[2] in types:
                filtered_attributes.append(attribute)

        return filtered_attributes

    def __init__(self):
        self.__sheets_reader = GoogleSheetsReader()
        self.__datacatalog_helper = DataCatalogHelper()

    def run(self, spreadsheet_id, project_id, template_id, display_name, delete_existing=False):
        master_template_attributes = self.__sheets_reader.read_master(spreadsheet_id,
                                                                      stringcase.spinalcase(template_id))
        self.__process_native_attributes(spreadsheet_id, project_id, template_id, display_name,
                                         master_template_attributes, delete_existing)
        self.__process_custom_multivalued_attributes(spreadsheet_id, project_id, template_id, display_name,
                                                     master_template_attributes, delete_existing)

    def __process_native_attributes(self, spreadsheet_id, project_id, template_id, display_name,
                                    master_template_attributes, delete_existing_template):

        native_attrs = TemplateMaker.__filter_attributes_by_types(
            master_template_attributes, _DATA_CATALOG_NATIVE_TYPES)

        StringFormatter.format_elements_to_snakecase(native_attrs, 0)

        enums_names = {}
        for attr in native_attrs:
            if not attr[2] == _DATA_CATALOG_ENUM_TYPE:
                continue

            names_from_sheet = self.__sheets_reader.read_helper(spreadsheet_id, stringcase.spinalcase(attr[0]))
            enums_names[attr[0]] = [name[0] for name in names_from_sheet]

        template_name = datacatalog_v1beta1.DataCatalogClient.tag_template_path(
            project_id, _CLOUD_PLATFORM_REGION, template_id)

        if delete_existing_template:
            self.__datacatalog_helper.delete_tag_template(template_name)

        if not self.__datacatalog_helper.tag_template_exists(template_name):
            self.__datacatalog_helper.create_tag_template(
                project_id, template_id, display_name, native_attrs, enums_names)

    def __process_custom_multivalued_attributes(self, spreadsheet_id, project_id, template_id, display_name,
                                                master_template_attributes, delete_existing_template):

        multivalued_attrs = TemplateMaker.__filter_attributes_by_types(
            master_template_attributes, [_CUSTOM_MULTIVALUED_TYPE])

        StringFormatter.format_elements_to_snakecase(multivalued_attrs, 0)

        for attr in multivalued_attrs:
            try:
                values_from_sheet = self.__sheets_reader.read_helper(spreadsheet_id, stringcase.spinalcase(attr[0]))
                attributes = [(StringFormatter.format_to_snakecase(value[0]), value[0], _DATA_CATALOG_BOOL_TYPE)
                              for value in values_from_sheet]
            except HttpError as err:
                if err.resp.status in [400]:
                    logging.info('NOT FOUND. Ignoring...')
                    continue  # Just ignore creating a new template representing the multivalued attribute
                else:
                    raise

            custom_template_id = f'{template_id}_{attr[0]}'
            custom_display_name = f'{display_name} - {attr[1]}'

            template_name = datacatalog_v1beta1.DataCatalogClient.tag_template_path(
                project_id, _CLOUD_PLATFORM_REGION, custom_template_id)

            if delete_existing_template:
                self.__datacatalog_helper.delete_tag_template(template_name)

            if not self.__datacatalog_helper.tag_template_exists(template_name):
                self.__datacatalog_helper.create_tag_template(
                    project_id, custom_template_id, custom_display_name, attributes)


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
        logging.info(_LOOKING_FOR_SHEET_FORMAT.format(sheet_type, spreadsheet_id, sheet_name))
        return self.__load_content(spreadsheet_id, sheet_name, values_per_line)

    def __load_content(self, spreadsheet_id, sheet_name, values_per_line):
        """
        Load the initial values from each line and store them into a list.

        Args:
            spreadsheet_id: The spreadsheet ID.
            sheet_name : Sheet's name
            values_per_line: The number of sequential values to read from each line.
        """
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

        location = self.__datacatalog.location_path(project_id, _CLOUD_PLATFORM_REGION)

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
        self.__service = build(serviceName='sheets', version='v4',
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

    @staticmethod
    def format_elements_to_snakecase(a_list, internal_index=None):
        if internal_index is None:
            for counter in range(len(a_list)):
                a_list[counter] = StringFormatter.format_to_snakecase(a_list[counter])
        else:
            for element in a_list:
                element[internal_index] = StringFormatter.format_to_snakecase(element[internal_index])

    @staticmethod
    def format_to_snakecase(string):
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

    parser.add_argument('template_id', help='the template ID')
    parser.add_argument('display_name', help='template\'s Display Name')
    parser.add_argument('project_id', help='GCP Project in which Template will be created')
    parser.add_argument('spreadsheet_id', help='Google Spreadsheet ID')
    parser.add_argument('--delete-existing', action='store_true',
                        help='delete existing Templates and recreate them with the provided metadata')

    args = parser.parse_args()

    TemplateMaker().run(
        args.spreadsheet_id, args.project_id, args.template_id, args.display_name, args.delete_existing)