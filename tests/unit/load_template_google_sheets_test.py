import httplib2
import unittest
from unittest import mock

from google.api_core import exceptions
from googleapiclient import errors

import load_template_google_sheets


class TemplateMakerTest(unittest.TestCase):

    @mock.patch('load_template_google_sheets.DataCatalogFacade')
    @mock.patch('load_template_google_sheets.GoogleSheetsReader')
    def setUp(self, mock_sheets_reader, mock_datacatalog_facade):
        self.__template_maker = load_template_google_sheets.TemplateMaker()
        # Shortcut for the object assigned to self.__template_maker.__sheets_reader
        self.__sheets_reader = mock_sheets_reader.return_value
        # Shortcut for the object assigned to self.__template_maker.__datacatalog_facade
        self.__datacatalog_facade = mock_datacatalog_facade.return_value

    def test_constructor_should_set_instance_attributes(self):
        self.assertIsNotNone(self.__template_maker.__dict__['_TemplateMaker__sheets_reader'])
        self.assertIsNotNone(self.__template_maker.__dict__['_TemplateMaker__datacatalog_facade'])

    def test_run_should_create_master_template_with_primitive_fields(self):
        sheets_reader = self.__sheets_reader
        sheets_reader.read_master.return_value = [['val1', 'val2', 'BOOL']]

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(spreadsheet_id=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template')

        sheets_reader.read_master.assert_called_once()
        datacatalog_facade.tag_template_exists.assert_called_once()
        datacatalog_facade.create_tag_template.assert_called_once()

    def test_run_should_create_master_template_with_enum_fields(self):
        sheets_reader = self.__sheets_reader
        sheets_reader.read_master.return_value = [['val1', 'val2', 'ENUM']]
        sheets_reader.read_helper.return_value = [['helper_val1']]

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(spreadsheet_id=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template')

        sheets_reader.read_helper.assert_called_once()

    def test_run_should_create_helper_template_for_multivalued_fields(self):
        sheets_reader = self.__sheets_reader
        sheets_reader.read_master.return_value = [['val1', 'val2', 'MULTI']]
        sheets_reader.read_helper.return_value = [['helper_val1']]

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(spreadsheet_id=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template',
                                  delete_existing=True)

        sheets_reader.read_helper.assert_called_once()
        # Both master and helper Templates are created.
        self.assertEqual(2, datacatalog_facade.create_tag_template.call_count)

    def test_run_should_ignore_template_for_multivalued_fields_if_sheet_not_found(self):
        sheets_reader = self.__sheets_reader
        sheets_reader.read_master.return_value = [['val1', 'val2', 'MULTI']]
        error_response = httplib2.Response({'status': 400, 'reason': 'Not Found'})
        sheets_reader.read_helper.side_effect = \
            errors.HttpError(resp=error_response, content=b'{}')

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(spreadsheet_id=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template')

        sheets_reader.read_helper.assert_called_once()
        # Only the master Template is created.
        datacatalog_facade.create_tag_template.assert_called_once()

    def test_run_should_raise_exception_template_for_multivalued_fields_if_unknown_error(self):
        sheets_reader = self.__sheets_reader
        sheets_reader.read_master.return_value = [['val1', 'val2', 'MULTI']]
        error_response = httplib2.Response({'status': 500, 'reason': 'Internal Server Error'})
        sheets_reader.read_helper.side_effect = \
            errors.HttpError(resp=error_response, content=b'{}')

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        with self.assertRaises(errors.HttpError):
            self.__template_maker.run(spreadsheet_id=None,
                                      project_id=None,
                                      template_id='test-template-id',
                                      display_name='Test Template')

        sheets_reader.read_helper.assert_called_once()
        # Only the master Template is created.
        datacatalog_facade.create_tag_template.assert_called_once()

    def test_run_should_not_delete_existing_template_by_default(self):
        sheets_reader = self.__sheets_reader
        sheets_reader.read_master.return_value = [['val1', 'val2', 'BOOL']]

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(spreadsheet_id=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template')

        datacatalog_facade.delete_tag_template.assert_not_called()

    def test_run_should_delete_existing_template_if_flag_set(self):
        sheets_reader = self.__sheets_reader
        sheets_reader.read_master.return_value = [['val1', 'val2', 'BOOL']]

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(spreadsheet_id=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template',
                                  delete_existing=True)

        datacatalog_facade.delete_tag_template.assert_called_once()


class GoogleSheetsReaderTest(unittest.TestCase):

    @mock.patch('load_template_google_sheets.GoogleSheetsFacade')
    def setUp(self, mock_sheets_facade):
        self.__sheets_reader = load_template_google_sheets.GoogleSheetsReader()
        # Shortcut for the object assigned to self.__sheets_reader.__sheets_facade
        self.__sheets_facade = mock_sheets_facade.return_value

    def test_constructor_should_set_instance_attributes(self):
        self.assertIsNotNone(self.__sheets_reader.__dict__['_GoogleSheetsReader__sheets_facade'])

    def test_read_master_should_return_content_as_list(self):
        sheets_facade = self.__sheets_facade
        sheets_facade.read_sheet.return_value = {
            'valueRanges': [{
                'values': [
                    ['col1', 'col2', 'col3'],
                    ['val1', 'val2', 'val3']
                ]
            }]
        }

        content = self.__sheets_reader.read_master('test-id', 'test-name')

        self.assertEqual(1, len(content))  # The first line (header) is ignored.
        self.assertEqual(3, len(content[0]))
        self.assertEqual('val2', content[0][1])

    def test_read_helper_should_return_content_as_list(self):
        sheets_facade = self.__sheets_facade
        sheets_facade.read_sheet.return_value = {
            'valueRanges': [{
                'values': [
                    ['col1'],
                    ['val1']
                ]
            }]
        }

        content = self.__sheets_reader.read_helper('test-id', 'test-name')

        self.assertEqual(1, len(content))  # The first line (header) is ignored.
        self.assertEqual(1, len(content[0]))
        self.assertEqual('val1', content[0][0])

    def test_read_should_return_exact_number_values_per_line(self):
        sheets_facade = self.__sheets_facade
        sheets_facade.read_sheet.return_value = {
            'valueRanges': [{
                'values': [
                    ['col1', 'col2', 'col3'],
                    ['val1', 'val2', 'val3']
                ]
            }]
        }

        content = self.__sheets_reader.read_master(None, None, values_per_line=2)

        self.assertEqual(2, len(content[0]))

    def test_read_should_return_stripped_content(self):
        sheets_facade = self.__sheets_facade
        sheets_facade.read_sheet.return_value = {
            'valueRanges': [{
                'values': [
                    ['col1', 'col2', 'col3'],
                    ['val1', ' val2  ', 'val3']
                ]
            }]
        }

        self.assertEqual('val2', self.__sheets_reader.read_master(None, None)[0][1])


class DataCatalogFacadeTest(unittest.TestCase):

    @mock.patch('load_template_google_sheets.datacatalog.DataCatalogClient')
    def setUp(self, mock_datacatalog_client):
        self.__datacatalog_facade = load_template_google_sheets.DataCatalogFacade()
        # Shortcut for the object assigned to self.__datacatalog_facade.__datacatalog
        self.__datacatalog_client = mock_datacatalog_client.return_value

    def test_constructor_should_set_instance_attributes(self):
        self.assertIsNotNone(self.__datacatalog_facade.__dict__['_DataCatalogFacade__datacatalog'])

    def test_create_tag_template_should_handle_described_fields(self):
        self.__datacatalog_facade.create_tag_template(
            project_id='project-id',
            template_id='template_id',
            display_name='Test Display Name',
            fields_descriptors=[
                ['test-string-field-id', 'Test String Field Display Name', 'STRING'],
                ['test-enum-field-id', 'Test ENUM Field Display Name', 'ENUM']
            ],
            enums_names={'test-enum-field-id': ['TEST_ENUM_VALUE']}
        )

        datacatalog_client = self.__datacatalog_client
        datacatalog_client.create_tag_template.assert_called_once()

    def test_delete_tag_template_should_call_client_library_method(self):
        self.__datacatalog_facade.delete_tag_template('template_name')

        datacatalog_client = self.__datacatalog_client
        datacatalog_client.delete_tag_template.assert_called_once()

    def test_delete_tag_template_should_handle_nonexistent(self):
        datacatalog_client = self.__datacatalog_client
        datacatalog_client.delete_tag_template.side_effect = \
            exceptions.PermissionDenied(message='')

        self.__datacatalog_facade.delete_tag_template('template_name')

        datacatalog_client.delete_tag_template.assert_called_once()

    def test_tag_template_exists_should_return_true_existing(self):
        tag_template_exists = self.__datacatalog_facade.tag_template_exists('template_name')

        self.assertTrue(tag_template_exists)
        datacatalog_client = self.__datacatalog_client
        datacatalog_client.get_tag_template.assert_called_once()

    def test_tag_template_exists_should_return_false_nonexistent(self):
        datacatalog_client = self.__datacatalog_client
        datacatalog_client.get_tag_template.side_effect = exceptions.PermissionDenied(message='')

        tag_template_exists = self.__datacatalog_facade.tag_template_exists('template_name')

        self.assertFalse(tag_template_exists)
        datacatalog_client = self.__datacatalog_client
        datacatalog_client.get_tag_template.assert_called_once()


class GoogleSheetsFacadeTest(unittest.TestCase):

    @mock.patch('load_template_google_sheets.service_account.ServiceAccountCredentials'
                '.get_application_default', lambda: None)
    @mock.patch('load_template_google_sheets.discovery.build')
    def setUp(self, mock_build):
        self.__sheets_facade = load_template_google_sheets.GoogleSheetsFacade()
        self.__mock_build = mock_build

    def test_constructor_should_set_instance_attributes(self):
        self.assertIsNotNone(self.__sheets_facade.__dict__['_GoogleSheetsFacade__service'])
        self.__mock_build.assert_called_once()

    def test_read_sheet_should_get_all_lines_from_requested_columns(self):
        self.__mock_build.return_value\
            .spreadsheets.return_value\
            .values.return_value\
            .batchGet.return_value\
            .execute.return_value = {}

        sheet_data = self.__sheets_facade.read_sheet(spreadsheet_id='test-id',
                                                     sheet_name='test-name',
                                                     values_per_line=2)

        self.assertEqual({}, sheet_data)

        self.__mock_build.return_value\
            .spreadsheets.return_value\
            .values.return_value\
            .batchGet.assert_called_with(spreadsheetId='test-id', ranges='test-name!A:B')


class StringFormatterTest(unittest.TestCase):

    def test_format_elements_snakecase_list(self):
        test_list = ['AA-AA', 'BB-BB']
        load_template_google_sheets.StringFormatter.format_elements_to_snakecase(test_list)
        self.assertListEqual(['aa_aa', 'bb_bb'], test_list)

    def test_format_elements_snakecase_internal_index(self):
        test_list = [['AA-AA', 'Test A'], ['BB-BB', 'Test B']]
        load_template_google_sheets.StringFormatter.format_elements_to_snakecase(test_list,
                                                                                 internal_index=0)
        self.assertListEqual([['aa_aa', 'Test A'], ['bb_bb', 'Test B']], test_list)

    def test_format_string_to_snakecase_abbreviation(self):
        self.assertEqual('aaa',
                         load_template_google_sheets.StringFormatter.format_to_snakecase('AAA'))
        self.assertEqual(
            'aaa_aaa', load_template_google_sheets.StringFormatter.format_to_snakecase('AAA-AAA'))

    def test_format_string_to_snakecase_camelcase(self):
        self.assertEqual(
            'camel_case',
            load_template_google_sheets.StringFormatter.format_to_snakecase('camelCase'))

    def test_format_string_to_snakecase_leading_number(self):
        self.assertEqual(
            '1_number',
            load_template_google_sheets.StringFormatter.format_to_snakecase('1 number'))

    def test_format_string_to_snakecase_repeated_special_chars(self):
        self.assertEqual(
            'repeated_special_chars',
            load_template_google_sheets.StringFormatter.format_to_snakecase(
                'repeated   special___chars'))

    def test_format_string_to_snakecase_whitespaces(self):
        self.assertEqual(
            'no_leading_and_trailing',
            load_template_google_sheets.StringFormatter.format_to_snakecase(
                ' no leading and trailing '))
        self.assertEqual(
            'no_leading_and_trailing',
            load_template_google_sheets.StringFormatter.format_to_snakecase(
                '\nno leading and trailing\t'))

    def test_format_string_to_snakecase_special_chars(self):
        self.assertEqual(
            'special_chars',
            load_template_google_sheets.StringFormatter.format_to_snakecase('special!#@-_ chars'))
        self.assertEqual(
            'special_chars',
            load_template_google_sheets.StringFormatter.format_to_snakecase('! special chars ?'))

    def test_format_string_to_snakecase_unicode(self):
        self.assertEqual(
            'a_a_e_o_u',
            load_template_google_sheets.StringFormatter.format_to_snakecase(u'å ä ß é ö ü'))

    def test_format_string_to_snakecase_uppercase(self):
        self.assertEqual(
            'uppercase',
            load_template_google_sheets.StringFormatter.format_to_snakecase('UPPERCASE'))
        self.assertEqual(
            'upper_case',
            load_template_google_sheets.StringFormatter.format_to_snakecase('UPPER CASE'))
