"""
This application demonstrates how to create a Tag Template in Data Catalog,
loading its information from a CSV file.
"""
import argparse
import csv
import logging
import re
import stringcase
import unicodedata

from google.api_core import exceptions
from google.cloud import datacatalog

_CLOUD_PLATFORM_REGION = 'us-central1'

_CUSTOM_MULTIVALUED_TYPE = 'MULTI'
_DATA_CATALOG_BOOL_TYPE = 'BOOL'
_DATA_CATALOG_ENUM_TYPE = 'ENUM'
_DATA_CATALOG_NATIVE_TYPES = ['BOOL', 'DOUBLE', 'ENUM', 'STRING', 'TIMESTAMP']

_FOLDER_PLUS_CSV_FILENAME_FORMAT = '{}/{}.csv'
_LOOKING_FOR_FILE_LOG_FORMAT = 'Looking for {} file {}...'


class TemplateMaker:

    def __init__(self):
        self.__datacatalog_facade = DataCatalogFacade()

    def run(self, files_folder, project_id, template_id, display_name, delete_existing=False):
        master_template_fields = CSVFilesReader.read_master(files_folder,
                                                            stringcase.spinalcase(template_id))
        self.__process_native_fields(files_folder, project_id, template_id, display_name,
                                     master_template_fields, delete_existing)
        self.__process_custom_multivalued_fields(files_folder, project_id, template_id,
                                                 display_name, master_template_fields,
                                                 delete_existing)

    def __process_native_fields(self, files_folder, project_id, template_id, display_name,
                                master_template_fields, delete_existing_template):

        native_fields = self.__filter_fields_by_types(master_template_fields,
                                                      _DATA_CATALOG_NATIVE_TYPES)
        StringFormatter.format_elements_to_snakecase(native_fields, 0)

        enums_names = {}
        for field in native_fields:
            if not field[2] == _DATA_CATALOG_ENUM_TYPE:
                continue

            names_from_file = CSVFilesReader.read_helper(files_folder,
                                                         stringcase.spinalcase(field[0]))
            enums_names[field[0]] = [name[0] for name in names_from_file]

        template_name = datacatalog.DataCatalogClient.tag_template_path(
            project_id, _CLOUD_PLATFORM_REGION, template_id)

        if delete_existing_template:
            self.__datacatalog_facade.delete_tag_template(template_name)

        if not self.__datacatalog_facade.tag_template_exists(template_name):
            self.__datacatalog_facade.create_tag_template(project_id, template_id, display_name,
                                                          native_fields, enums_names)

    def __process_custom_multivalued_fields(self, files_folder, project_id, template_id,
                                            display_name, master_template_fields,
                                            delete_existing_template):

        multivalued_fields = self.__filter_fields_by_types(master_template_fields,
                                                           [_CUSTOM_MULTIVALUED_TYPE])
        StringFormatter.format_elements_to_snakecase(multivalued_fields, 0)

        for field in multivalued_fields:
            try:
                values_from_file = CSVFilesReader.read_helper(files_folder,
                                                              stringcase.spinalcase(field[0]))
                fields = [(StringFormatter.format_to_snakecase(value[0]), value[0],
                           _DATA_CATALOG_BOOL_TYPE) for value in values_from_file]
            except FileNotFoundError:
                logging.info('NOT FOUND. Ignoring...')
                continue  # Ignore creating a new template representing the multivalued field

            custom_template_id = f'{template_id}_{field[0]}'
            custom_display_name = f'{display_name} - {field[1]}'

            template_name = datacatalog.DataCatalogClient.tag_template_path(
                project_id, _CLOUD_PLATFORM_REGION, custom_template_id)

            if delete_existing_template:
                self.__datacatalog_facade.delete_tag_template(template_name)

            if not self.__datacatalog_facade.tag_template_exists(template_name):
                self.__datacatalog_facade.create_tag_template(project_id, custom_template_id,
                                                              custom_display_name, fields)

    @classmethod
    def __filter_fields_by_types(cls, fields, valid_types):
        return [field for field in fields if field[2] in valid_types]


"""
Input reader
========================================
"""


class CSVFilesReader:

    @classmethod
    def read_master(cls, folder, file_id, values_per_line=3):
        return cls.__read(folder, file_id, 'master', values_per_line)

    @classmethod
    def read_helper(cls, folder, file_id, values_per_line=1):
        return cls.__read(folder, file_id, 'helper', values_per_line)

    @classmethod
    def __read(cls, folder, file_id, file_type, values_per_line):
        """
        Read the requested values from each line and store them into a list.
        Example: CSV name,display name,type => Python list ['name','display name','type'].

        :param folder: CSV file container folder.
        :param file_id: File name with no .csv suffix.
        :param file_type: File type {'master', 'helper'}.
        :param values_per_line: The number of consecutive values to be read from each line.
        """
        file_path = cls.__normalize_path(_FOLDER_PLUS_CSV_FILENAME_FORMAT.format(folder, file_id))

        logging.info(_LOOKING_FOR_FILE_LOG_FORMAT.format(file_type, file_path))

        data = []

        with open(file_path, mode='r') as csv_file:
            logging.info(f'Reading file {file_path}...')
            for row in csv.reader(csv_file):
                row_data = []
                for counter in range(values_per_line):
                    row_data.append(row[counter].strip())
                data.append(row_data)

        # The first line is usually used for headers, so it's discarded.
        del (data[0])

        logging.info('DONE')
        return data

    @classmethod
    def __normalize_path(cls, path):
        return re.sub(r'/+', '/', path)


"""
API communication classes
========================================
"""


class DataCatalogFacade:
    """
    Manage Templates by communicating to Data Catalog's API.
    """

    def __init__(self):
        # Initialize the API client.
        self.__datacatalog = datacatalog.DataCatalogClient()

    def create_tag_template(self,
                            project_id,
                            template_id,
                            display_name,
                            fields_descriptors,
                            enums_names=None):
        """Create a Tag Template."""

        location = datacatalog.DataCatalogClient.common_location_path(
            project_id, _CLOUD_PLATFORM_REGION)

        tag_template = datacatalog.TagTemplate()
        tag_template.display_name = display_name

        for descriptor in fields_descriptors:
            field = datacatalog.TagTemplateField()
            field.display_name = descriptor[1]

            field_id = descriptor[0]
            field_type = descriptor[2]
            if not field_type == _DATA_CATALOG_ENUM_TYPE:
                field.type_.primitive_type = datacatalog.FieldType.PrimitiveType[field_type]
            else:
                for enum_name in enums_names[field_id]:
                    enum_value = datacatalog.FieldType.EnumType.EnumValue()
                    enum_value.display_name = enum_name
                    field.type_.enum_type.allowed_values.append(enum_value)

            tag_template.fields[field_id] = field

        created_tag_template = self.__datacatalog.create_tag_template(parent=location,
                                                                      tag_template_id=template_id,
                                                                      tag_template=tag_template)

        logging.info(f'===> Template created: {created_tag_template.name}')

    def delete_tag_template(self, name):
        """Delete a Tag Template."""

        try:
            self.__datacatalog.delete_tag_template(name=name, force=True)
            logging.info(f'===> Template deleted: {name}')
        except exceptions.PermissionDenied:
            pass

    def tag_template_exists(self, name):
        """Check if a Tag Template with the provided name already exists."""

        try:
            self.__datacatalog.get_tag_template(name=name)
            return True
        except exceptions.PermissionDenied:
            return False


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
        normalized_str = normalized_str.lower() \
            if (' ' in normalized_str) or (normalized_str.isupper()) \
            else stringcase.camelcase(normalized_str)  # FooBarBaz => fooBarBaz

        return stringcase.snakecase(normalized_str)  # foo-bar-baz => foo_bar_baz


"""
Main program entry point
========================================
"""
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Load Tag Template from CSV')

    parser.add_argument('--template-id', help='the template ID', required=True)
    parser.add_argument('--display-name', help='template\'s Display Name', required=True)
    parser.add_argument('--project-id',
                        help='GCP Project in which the Template will be created',
                        required=True)
    parser.add_argument('--files-folder', help='path to CSV files container folder', required=True)
    parser.add_argument(
        '--delete-existing',
        action='store_true',
        help='delete existing Templates and recreate them with the provided metadata')

    args = parser.parse_args()

    TemplateMaker().run(args.files_folder, args.project_id, args.template_id, args.display_name,
                        args.delete_existing)
