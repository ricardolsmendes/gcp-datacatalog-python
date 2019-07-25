import argparse
import csv
import logging
import re
import stringcase
import unicodedata

from google.api_core.exceptions import PermissionDenied
from google.cloud import datacatalog_v1beta1


"""
Constants
========================================
"""


_CLOUD_PLATFORM_REGION = 'us-central1'

_CUSTOM_MULTIVALUED_TYPE = 'MULTI'
_DATA_CATALOG_ENUM_TYPE = 'ENUM'
_DATA_CATALOG_NATIVE_TYPES = ['BOOL', 'DOUBLE', 'ENUM', 'STRING', 'TIMESTAMP']

_FOLDER_PLUS_CSV_FILENAME_FORMAT = '{}/{}.csv'
_LOOKING_FOR_FILE_FORMAT = 'Looking for {} file {}...'


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
        self.__datacatalog_helper = DataCatalogHelper()

    def run(self, files_folder, project_id, template_id, display_name, delete_existing=False):
        master_template_attributes = CSVFilesReader.read_master(files_folder,
                                                                stringcase.spinalcase(template_id))
        self.__process_native_attributes(files_folder, project_id, template_id, display_name,
                                         master_template_attributes, delete_existing)
        self.__process_custom_multivalued_attributes(files_folder, project_id, template_id, display_name,
                                                     master_template_attributes, delete_existing)

    def __process_native_attributes(self, files_folder, project_id, template_id, display_name,
                                    master_template_attributes, delete_existing_template):

        native_attrs = TemplateMaker.__filter_attributes_by_types(
            master_template_attributes, _DATA_CATALOG_NATIVE_TYPES)

        StringFormatter.format_elements_to_snakecase(native_attrs, 0)

        enums_names = {}
        for attr in native_attrs:
            if not attr[2] == _DATA_CATALOG_ENUM_TYPE:
                continue

            names_from_file = CSVFilesReader.read_helper(files_folder, stringcase.spinalcase(attr[0]))
            enums_names[attr[0]] = [name[0] for name in names_from_file]

        template_name = datacatalog_v1beta1.DataCatalogClient.tag_template_path(
            project_id, _CLOUD_PLATFORM_REGION, template_id)

        if delete_existing_template:
            self.__datacatalog_helper.delete_tag_template(template_name)

        if not self.__datacatalog_helper.tag_template_exists(template_name):
            self.__datacatalog_helper.create_tag_template(
                project_id, template_id, display_name, native_attrs, enums_names)

    def __process_custom_multivalued_attributes(self, files_folder, project_id, template_id, display_name,
                                                master_template_attributes, delete_existing_template):

        multivalued_attrs = TemplateMaker.__filter_attributes_by_types(
            master_template_attributes, [_CUSTOM_MULTIVALUED_TYPE])

        StringFormatter.format_elements_to_snakecase(multivalued_attrs, 0)

        for attr in multivalued_attrs:
            try:
                values_from_file = CSVFilesReader.read_helper(files_folder, stringcase.spinalcase(attr[0]))
                attributes = [(StringFormatter.format_to_snakecase(value[0]), value[0], 'BOOL')
                              for value in values_from_file]
            except FileNotFoundError:
                logging.info('NOT FOUND. Ignoring...')
                continue  # Just ignore creating a new template representing the multivalued attribute

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


class CSVFilesReader:

    @staticmethod
    def read_master(folder, file_id, values_per_line=3):
        return CSVFilesReader.__read(folder, file_id, 'master', values_per_line)

    @staticmethod
    def read_helper(folder, file_id, values_per_line=1):
        return CSVFilesReader.__read(folder, file_id, 'helper', values_per_line)

    @staticmethod
    def __read(folder, file_id, file_type, values_per_line):
        file_path = CSVFilesReader.__normalize_path(
            _FOLDER_PLUS_CSV_FILENAME_FORMAT.format(folder, file_id))

        logging.info(_LOOKING_FOR_FILE_FORMAT.format(file_type, file_path))
        return CSVFilesReader.__load_content(file_path, values_per_line)

    @staticmethod
    def __normalize_path(file_path):
        return re.sub(r'/+', '/', file_path)

    @staticmethod
    def __load_content(file_path, values_per_line, delimiter=','):
        """
        Load the initial values from each line and store them into a list.
        Example: CSV name,display name,type => Python list ['name','display name','type']

        Args:
            file_path: CSV file path.
            values_per_line: The number of sequential values to read from each line.
            delimiter: Values delimiter.
        """
        data = []

        with open(file_path, mode='r') as csv_file:
            logging.info(f'Reading file {file_path}...')
            for row in csv.reader(csv_file, delimiter=delimiter):
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
    Manage Templates by communicating to Data Catalog's API
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

        self.__datacatalog.delete_tag_template(name=name, force=True)

        logging.info(f'===> Template deleted: {name}')

    def tag_template_exists(self, name):
        try:
            self.__datacatalog.get_tag_template(name=name)
            return True
        except PermissionDenied:
            return False


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

    parser = argparse.ArgumentParser(description='Load Tag Template from CSV')

    parser.add_argument('template_id', help='the template ID')
    parser.add_argument('display_name', help='template\'s Display Name')
    parser.add_argument('project_id', help='GCP Project in which Template will be created')
    parser.add_argument('files_folder', help='path to CSV files container folder')
    parser.add_argument('--delete-existing', action='store_true',
                        help='delete existing Templates and recreate them with the provided metadata')

    args = parser.parse_args()

    TemplateMaker().run(args.files_folder, args.project_id, args.template_id, args.display_name, args.delete_existing)
