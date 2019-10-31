import io
import unittest
from unittest import mock

from google.api_core import exceptions

import load_template_csv

_PATCHED_DATACATALOG_CLIENT = 'load_template_csv.datacatalog.DataCatalogClient'
_PATCHED_DATACATALOG_FACADE = 'load_template_csv.DataCatalogFacade'


@mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.__init__', lambda self: None)
class TemplateMakerTest(unittest.TestCase):

    def test_constructor_should_set_instance_attributes(self):
        self.assertIsNotNone(load_template_csv.TemplateMaker().__dict__['_TemplateMaker__datacatalog_facade'])

    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.create_tag_template')
    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.tag_template_exists')
    @mock.patch('load_template_csv.CSVFilesReader.read_master')
    def test_run_should_create_master_template_with_primitive_fields(
            self, mock_read_master, mock_tag_template_exists, mock_create_tag_template):

        mock_read_master.return_value = [['val1', 'val2', 'BOOL']]
        mock_tag_template_exists.return_value = False

        load_template_csv.TemplateMaker().run(
            files_folder=None, project_id=None, template_id='test-template-id', display_name='Test Template')

        mock_read_master.assert_called_once()
        mock_tag_template_exists.assert_called_once()
        mock_create_tag_template.assert_called_once()

    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.create_tag_template', lambda self, *args: None)
    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.tag_template_exists', lambda self, *args: False)
    @mock.patch('load_template_csv.CSVFilesReader.read_helper')
    @mock.patch('load_template_csv.CSVFilesReader.read_master', lambda self, *args: [['val1', 'val2', 'ENUM']])
    def test_run_should_create_master_template_with_enum_fields(self, mock_read_helper):
        mock_read_helper.return_value = [['helper_val1']]

        load_template_csv.TemplateMaker().run(
            files_folder=None, project_id=None, template_id='test-template-id', display_name='Test Template')

        mock_read_helper.assert_called_once()

    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.create_tag_template')
    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.tag_template_exists', lambda self, *args: False)
    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.delete_tag_template', lambda self, *args: None)
    @mock.patch('load_template_csv.CSVFilesReader.read_helper')
    @mock.patch('load_template_csv.CSVFilesReader.read_master', lambda self, *args: [['val1', 'val2', 'MULTI']])
    def test_run_should_create_helper_template_for_multivalued_fields(
            self, mock_read_helper, mock_create_tag_template):

        mock_read_helper.return_value = [['helper_val1']]

        load_template_csv.TemplateMaker().run(
            files_folder=None, project_id=None, template_id='test-template-id', display_name='Test Template',
            delete_existing=True)

        mock_read_helper.assert_called_once()
        self.assertEqual(2, mock_create_tag_template.call_count)  # Both master and helper Templates are created.

    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.create_tag_template')
    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.tag_template_exists', lambda self, *args: False)
    @mock.patch('load_template_csv.CSVFilesReader.read_helper')
    @mock.patch('load_template_csv.CSVFilesReader.read_master', lambda self, *args: [['val1', 'val2', 'MULTI']])
    def test_run_should_ignore_template_for_multivalued_fields_if_file_not_found(
            self, mock_read_helper, mock_create_tag_template):

        mock_read_helper.side_effect = FileNotFoundError()

        load_template_csv.TemplateMaker().run(
            files_folder=None, project_id=None, template_id='test-template-id', display_name='Test Template')

        mock_read_helper.assert_called_once()
        mock_create_tag_template.assert_called_once()  # Only the master Template is created.

    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.create_tag_template', lambda self, *args: None)
    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.tag_template_exists', lambda self, *args: False)
    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.delete_tag_template')
    @mock.patch('load_template_csv.CSVFilesReader.read_master', lambda self, *args: [['val1', 'val2', 'BOOL']])
    def test_run_should_not_delete_existing_template_by_default(self, mock_delete_tag_template):
        load_template_csv.TemplateMaker().run(
            files_folder=None, project_id=None, template_id='test-template-id', display_name='Test Template')

        mock_delete_tag_template.assert_not_called()

    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.create_tag_template', lambda self, *args: None)
    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.tag_template_exists', lambda self, *args: False)
    @mock.patch(f'{_PATCHED_DATACATALOG_FACADE}.delete_tag_template')
    @mock.patch('load_template_csv.CSVFilesReader.read_master', lambda self, *args: [['val1', 'val2', 'BOOL']])
    def test_run_should_delete_existing_template_if_flag_set(self, mock_delete_tag_template):
        load_template_csv.TemplateMaker().run(
            files_folder=None, project_id=None, template_id='test-template-id', display_name='Test Template',
            delete_existing=True)

        mock_delete_tag_template.assert_called_once()


class CSVFilesReaderTest(unittest.TestCase):

    @mock.patch('builtins.open', new_callable=mock.mock_open())
    def test_read_master_should_return_content_as_list(self, mock_builtins_open):
        mock_builtins_open.return_value = io.StringIO(
            'col1,col2,col3\n'
            'val1,val2,val3\n'
        )

        content = load_template_csv.CSVFilesReader.read_master('test-folder', 'test-file-id')

        mock_builtins_open.assert_called_with('test-folder/test-file-id.csv', mode='r')
        self.assertEqual(1, len(content))  # The first line (header) is ignored.
        self.assertEqual(3, len(content[0]))
        self.assertEqual('val2', content[0][1])

    @mock.patch('builtins.open', new_callable=mock.mock_open())
    def test_read_helper_should_return_content_as_list(self, mock_builtins_open):
        mock_builtins_open.return_value = io.StringIO(
            'col1\n'
            'val1\n'
        )

        content = load_template_csv.CSVFilesReader.read_helper('test-folder', 'test-file-id')

        mock_builtins_open.assert_called_with('test-folder/test-file-id.csv', mode='r')
        self.assertEqual(1, len(content))  # The first line (header) is ignored.
        self.assertEqual(1, len(content[0]))
        self.assertEqual('val1', content[0][0])

    @mock.patch('builtins.open', new_callable=mock.mock_open())
    def test_read_should_return_exact_number_values_per_line(self, mock_builtins_open):
        mock_builtins_open.return_value = io.StringIO(
            'col1,col2,col3\n'
            'val1,val2,val3\n'
        )

        content = load_template_csv.CSVFilesReader.read_master(None, None, values_per_line=2)

        self.assertEqual(2, len(content[0]))

    @mock.patch('builtins.open', new_callable=mock.mock_open())
    def test_read_should_return_stripped_content(self, mock_builtins_open):
        mock_builtins_open.return_value = io.StringIO(
            'col1,col2,col3\n'
            'val1, val2  ,val3\n'
        )

        self.assertEqual('val2', load_template_csv.CSVFilesReader.read_master(None, None)[0][1])


@mock.patch(f'{_PATCHED_DATACATALOG_CLIENT}.__init__', lambda self: None)
class DataCatalogFacadeTest(unittest.TestCase):

    def test_constructor_should_set_instance_attributes(self):
        self.assertIsNotNone(load_template_csv.DataCatalogFacade().__dict__['_DataCatalogFacade__datacatalog'])

    @mock.patch(f'{_PATCHED_DATACATALOG_CLIENT}.create_tag_template')
    def test_create_tag_template_should_handle_described_fields(self, mock_create_tag_template):
        load_template_csv.DataCatalogFacade().create_tag_template(
            project_id=None,
            template_id=None,
            display_name='Test Display Name',
            fields_descriptors=[
                ['test-string-field-id', 'Test String Field Display Name', 'STRING'],
                ['test-enum-field-id', 'Test ENUM Field Display Name', 'ENUM']
            ],
            enums_names={'test-enum-field-id': ['TEST_ENUM_VALUE']}
        )

        mock_create_tag_template.assert_called_once()

    @mock.patch(f'{_PATCHED_DATACATALOG_CLIENT}.delete_tag_template')
    def test_delete_tag_template_should_call_client_library_method(self, mock_delete_tag_template):
        load_template_csv.DataCatalogFacade().delete_tag_template(None)
        mock_delete_tag_template.assert_called_once()

    @mock.patch(f'{_PATCHED_DATACATALOG_CLIENT}.delete_tag_template')
    def test_delete_tag_template_should_handle_nonexistent(self, mock_delete_tag_template):
        mock_delete_tag_template.side_effect = exceptions.PermissionDenied(message='')
        load_template_csv.DataCatalogFacade().delete_tag_template(None)
        mock_delete_tag_template.assert_called_once()

    @mock.patch(f'{_PATCHED_DATACATALOG_CLIENT}.get_tag_template')
    def test_tag_template_exists_should_return_true_existing(self, mock_get_tag_template):
        tag_template_exists = load_template_csv.DataCatalogFacade().tag_template_exists(None)
        self.assertTrue(tag_template_exists)
        mock_get_tag_template.assert_called_once()

    @mock.patch(f'{_PATCHED_DATACATALOG_CLIENT}.get_tag_template')
    def test_tag_template_exists_should_return_false_nonexistent(self, mock_get_tag_template):
        mock_get_tag_template.side_effect = exceptions.PermissionDenied(message='')
        tag_template_exists = load_template_csv.DataCatalogFacade().tag_template_exists(None)
        self.assertFalse(tag_template_exists)
        mock_get_tag_template.assert_called_once()


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
        self.assertEqual('aaa_aaa', load_template_csv.StringFormatter.format_to_snakecase('AAA-AAA'))

    def test_format_string_to_snakecase_camelcase(self):
        self.assertEqual('camel_case',
                         load_template_csv.StringFormatter.format_to_snakecase('camelCase'))

    def test_format_string_to_snakecase_leading_number(self):
        self.assertEqual('1_number',
                         load_template_csv.StringFormatter.format_to_snakecase('1 number'))

    def test_format_string_to_snakecase_repeated_special_chars(self):
        self.assertEqual('repeated_special_chars',
                         load_template_csv.StringFormatter.format_to_snakecase(
                             'repeated   special___chars'))

    def test_format_string_to_snakecase_whitespaces(self):
        self.assertEqual('no_leading_and_trailing',
                         load_template_csv.StringFormatter.format_to_snakecase(
                             ' no leading and trailing '))
        self.assertEqual('no_leading_and_trailing',
                         load_template_csv.StringFormatter.format_to_snakecase(
                             '\nno leading and trailing\t'))

    def test_format_string_to_snakecase_special_chars(self):
        self.assertEqual('special_chars',
                         load_template_csv.StringFormatter.format_to_snakecase('special!#@-_ chars'))
        self.assertEqual('special_chars',
                         load_template_csv.StringFormatter.format_to_snakecase('! special chars ?'))

    def test_format_string_to_snakecase_unicode(self):
        self.assertEqual('a_a_e_o_u',
                         load_template_csv.StringFormatter.format_to_snakecase(u'å ä ß é ö ü'))

    def test_format_string_to_snakecase_uppercase(self):
        self.assertEqual('uppercase',
                         load_template_csv.StringFormatter.format_to_snakecase('UPPERCASE'))
        self.assertEqual('upper_case',
                         load_template_csv.StringFormatter.format_to_snakecase('UPPER CASE'))
