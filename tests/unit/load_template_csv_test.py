import io
import unittest
from unittest import mock

from google.api_core import exceptions

import load_template_csv


@mock.patch('load_template_csv.CSVFilesReader')
class TemplateMakerTest(unittest.TestCase):

    @mock.patch('load_template_csv.DataCatalogFacade')
    def setUp(self, mock_datacatalog_facade):
        self.__template_maker = load_template_csv.TemplateMaker()
        # Shortcut for the object assigned to self.__template_maker.__datacatalog_facade
        self.__datacatalog_facade = mock_datacatalog_facade.return_value

    def test_constructor_should_set_instance_attributes(self, mock_csv_files_reader):
        self.assertIsNotNone(self.__template_maker.__dict__['_TemplateMaker__datacatalog_facade'])

    def test_run_should_create_master_template_with_primitive_fields(self, mock_csv_files_reader):
        mock_csv_files_reader.read_master.return_value = [['val1', 'val2', 'BOOL']]

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(files_folder=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template')

        mock_csv_files_reader.read_master.assert_called_once()
        datacatalog_facade.tag_template_exists.assert_called_once()
        datacatalog_facade.create_tag_template.assert_called_once()

    def test_run_should_create_master_template_with_enum_fields(self, mock_csv_files_reader):
        mock_csv_files_reader.read_master.return_value = [['val1', 'val2', 'ENUM']]
        mock_csv_files_reader.read_helper.return_value = [['helper_val1']]

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(files_folder=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template')

        mock_csv_files_reader.read_helper.assert_called_once()

    def test_run_should_create_helper_template_for_multivalued_fields(self, mock_csv_files_reader):
        mock_csv_files_reader.read_master.return_value = [['val1', 'val2', 'MULTI']]
        mock_csv_files_reader.read_helper.return_value = [['helper_val1']]

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(files_folder=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template',
                                  delete_existing=True)

        mock_csv_files_reader.read_helper.assert_called_once()
        # Both master and helper Templates are created.
        self.assertEqual(2, datacatalog_facade.create_tag_template.call_count)

    def test_run_should_ignore_template_for_multivalued_fields_if_file_not_found(
        self, mock_csv_files_reader):  # noqa

        mock_csv_files_reader.read_master.return_value = [['val1', 'val2', 'MULTI']]
        mock_csv_files_reader.read_helper.side_effect = FileNotFoundError()

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(files_folder=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template')

        mock_csv_files_reader.read_helper.assert_called_once()
        # Only the master Template is created.
        datacatalog_facade.create_tag_template.assert_called_once()

    def test_run_should_not_delete_existing_template_by_default(self, mock_csv_files_reader):
        mock_csv_files_reader.read_master.return_value = [['val1', 'val2', 'BOOL']]

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(files_folder=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template')

        datacatalog_facade.delete_tag_template.assert_not_called()

    def test_run_should_delete_existing_template_if_flag_set(self, mock_csv_files_reader):
        mock_csv_files_reader.read_master.return_value = [['val1', 'val2', 'BOOL']]

        datacatalog_facade = self.__datacatalog_facade
        datacatalog_facade.tag_template_exists.return_value = False

        self.__template_maker.run(files_folder=None,
                                  project_id=None,
                                  template_id='test-template-id',
                                  display_name='Test Template',
                                  delete_existing=True)

        datacatalog_facade.delete_tag_template.assert_called_once()


@mock.patch('load_template_csv.open', new_callable=mock.mock_open())
class CSVFilesReaderTest(unittest.TestCase):

    def test_read_master_should_return_content_as_list(self, mock_open):
        mock_open.return_value = io.StringIO(
            'col1,col2,col3\n'
            'val1,val2,val3\n'
        )

        content = load_template_csv.CSVFilesReader.read_master('test-folder', 'test-file-id')

        mock_open.assert_called_with('test-folder/test-file-id.csv', mode='r')
        self.assertEqual(1, len(content))  # The first line (header) is ignored.
        self.assertEqual(3, len(content[0]))
        self.assertEqual('val2', content[0][1])

    def test_read_helper_should_return_content_as_list(self, mock_open):
        mock_open.return_value = io.StringIO(
            'col1\n'
            'val1\n'
        )

        content = load_template_csv.CSVFilesReader.read_helper('test-folder', 'test-file-id')

        mock_open.assert_called_with('test-folder/test-file-id.csv', mode='r')
        self.assertEqual(1, len(content))  # The first line (header) is ignored.
        self.assertEqual(1, len(content[0]))
        self.assertEqual('val1', content[0][0])

    def test_read_should_return_exact_number_values_per_line(self, mock_open):
        mock_open.return_value = io.StringIO(
            'col1,col2,col3\n'
            'val1,val2,val3\n'
        )

        content = load_template_csv.CSVFilesReader.read_master(None, None, values_per_line=2)

        self.assertEqual(2, len(content[0]))

    def test_read_should_return_stripped_content(self, mock_open):
        mock_open.return_value = io.StringIO(
            'col1,col2,col3\n'
            'val1, val2  ,val3\n'
        )

        self.assertEqual('val2', load_template_csv.CSVFilesReader.read_master(None, None)[0][1])


class DataCatalogFacadeTest(unittest.TestCase):

    @mock.patch('load_template_csv.datacatalog.DataCatalogClient')
    def setUp(self, mock_datacatalog_client):
        self.__datacatalog_facade = load_template_csv.DataCatalogFacade()
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
        datacatalog_client.get_tag_template.assert_called_once()


class StringFormatterTest(unittest.TestCase):

    def test_format_elements_snakecase_list(self):
        test_list = ['AA-AA', 'BB-BB']
        load_template_csv.StringFormatter.format_elements_to_snakecase(test_list)
        self.assertListEqual(['aa_aa', 'bb_bb'], test_list)

    def test_format_elements_snakecase_internal_index(self):
        test_list = [['AA-AA', 'Test A'], ['BB-BB', 'Test B']]
        load_template_csv.StringFormatter.format_elements_to_snakecase(test_list, internal_index=0)
        self.assertListEqual([['aa_aa', 'Test A'], ['bb_bb', 'Test B']], test_list)

    def test_format_string_to_snakecase_abbreviation(self):
        self.assertEqual('aaa', load_template_csv.StringFormatter.format_to_snakecase('AAA'))
        self.assertEqual('aaa_aaa',
                         load_template_csv.StringFormatter.format_to_snakecase('AAA-AAA'))

    def test_format_string_to_snakecase_camelcase(self):
        self.assertEqual('camel_case',
                         load_template_csv.StringFormatter.format_to_snakecase('camelCase'))

    def test_format_string_to_snakecase_leading_number(self):
        self.assertEqual('1_number',
                         load_template_csv.StringFormatter.format_to_snakecase('1 number'))

    def test_format_string_to_snakecase_repeated_special_chars(self):
        self.assertEqual(
            'repeated_special_chars',
            load_template_csv.StringFormatter.format_to_snakecase('repeated   special___chars'))

    def test_format_string_to_snakecase_whitespaces(self):
        self.assertEqual(
            'no_leading_and_trailing',
            load_template_csv.StringFormatter.format_to_snakecase(' no leading and trailing '))
        self.assertEqual(
            'no_leading_and_trailing',
            load_template_csv.StringFormatter.format_to_snakecase('\nno leading and trailing\t'))

    def test_format_string_to_snakecase_special_chars(self):
        self.assertEqual(
            'special_chars',
            load_template_csv.StringFormatter.format_to_snakecase('special!#@-_ chars'))
        self.assertEqual(
            'special_chars',
            load_template_csv.StringFormatter.format_to_snakecase('! special chars ?'))

    def test_format_string_to_snakecase_unicode(self):
        self.assertEqual('a_a_e_o_u',
                         load_template_csv.StringFormatter.format_to_snakecase(u'å ä ß é ö ü'))

    def test_format_string_to_snakecase_uppercase(self):
        self.assertEqual('uppercase',
                         load_template_csv.StringFormatter.format_to_snakecase('UPPERCASE'))
        self.assertEqual('upper_case',
                         load_template_csv.StringFormatter.format_to_snakecase('UPPER CASE'))
