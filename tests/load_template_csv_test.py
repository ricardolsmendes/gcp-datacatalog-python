from io import StringIO
from unittest import TestCase
from unittest.mock import mock_open, patch

from google.api_core.exceptions import PermissionDenied

from load_template_csv import CSVFilesReader, DataCatalogHelper, StringFormatter

_PATCHED_DATACATALOG_CLIENT = 'load_template_csv.datacatalog_v1beta1.DataCatalogClient'


class CSVFilesReaderTest(TestCase):

    @patch('builtins.open', new_callable=mock_open())
    def test_read_master_should_return_content_as_list(self, mock_builtins_open):
        mock_builtins_open.return_value = StringIO(
            'col1,col2,col3\n'
            'val1,val2,val3\n'
        )

        content = CSVFilesReader.read_master('test-folder', 'test-file-id')

        mock_builtins_open.assert_called_with('test-folder/test-file-id.csv', mode='r')
        self.assertEqual(1, len(content))  # The first line (header) is ignored.
        self.assertEqual(3, len(content[0]))
        self.assertEqual('val2', content[0][1])

    @patch('builtins.open', new_callable=mock_open())
    def test_read_helper_should_return_content_as_list(self, mock_builtins_open):
        mock_builtins_open.return_value = StringIO(
            'col1\n'
            'val1\n'
        )

        content = CSVFilesReader.read_helper('test-folder', 'test-file-id')

        mock_builtins_open.assert_called_with('test-folder/test-file-id.csv', mode='r')
        self.assertEqual(1, len(content))  # The first line (header) is ignored.
        self.assertEqual(1, len(content[0]))
        self.assertEqual('val1', content[0][0])

    @patch('builtins.open', new_callable=mock_open())
    def test_read_should_return_exact_number_values_per_line(self, mock_builtins_open):
        mock_builtins_open.return_value = StringIO(
            'col1,col2,col3\n'
            'val1,val2,val3\n'
        )

        content = CSVFilesReader.read_master(None, None, values_per_line=2)

        self.assertEqual(2, len(content[0]))

    @patch('builtins.open', new_callable=mock_open())
    def test_read_should_return_stripped_content(self, mock_builtins_open):
        mock_builtins_open.return_value = StringIO(
            'col1,col2,col3\n'
            'val1, val2  ,val3\n'
        )

        self.assertEqual('val2', CSVFilesReader.read_master(None, None)[0][1])


@patch(f'{_PATCHED_DATACATALOG_CLIENT}.__init__', lambda self, *args: None)
class DataCatalogHelperTest(TestCase):

    @patch(f'{_PATCHED_DATACATALOG_CLIENT}.create_tag_template')
    def test_create_tag_template_should_handle_provided_fields(self, mock_create_tag_template):
        DataCatalogHelper().create_tag_template(
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

    @patch(f'{_PATCHED_DATACATALOG_CLIENT}.delete_tag_template')
    def test_delete_tag_template_should_call_client_library_method(self, mock_delete_tag_template):
        DataCatalogHelper().delete_tag_template(None)
        mock_delete_tag_template.assert_called_once()

    @patch(f'{_PATCHED_DATACATALOG_CLIENT}.delete_tag_template')
    def test_delete_tag_template_should_handle_nonexistent(self, mock_delete_tag_template):
        mock_delete_tag_template.side_effect = PermissionDenied(message='')
        DataCatalogHelper().delete_tag_template(None)
        mock_delete_tag_template.assert_called_once()

    @patch(f'{_PATCHED_DATACATALOG_CLIENT}.get_tag_template')
    def test_tag_template_exists_should_return_true_existing(self, mock_get_tag_template):
        tag_template_exists = DataCatalogHelper().tag_template_exists(None)
        self.assertTrue(tag_template_exists)
        mock_get_tag_template.assert_called_once()

    @patch(f'{_PATCHED_DATACATALOG_CLIENT}.get_tag_template')
    def test_tag_template_exists_should_return_false_nonexistent(self, mock_get_tag_template):
        mock_get_tag_template.side_effect = PermissionDenied(message='')
        tag_template_exists = DataCatalogHelper().tag_template_exists(None)
        self.assertFalse(tag_template_exists)
        mock_get_tag_template.assert_called_once()


class StringFormatterTest(TestCase):

    def test_format_elements_snakecase_list(self):
        test_list = ['AA-AA', 'BB-BB']
        StringFormatter.format_elements_to_snakecase(test_list)
        self.assertListEqual(['aa_aa', 'bb_bb'], test_list)

    def test_format_elements_snakecase_internal_index(self):
        test_list = [['AA-AA', 'Test A'], ['BB-BB', 'Test B']]
        StringFormatter.format_elements_to_snakecase(test_list, internal_index=0)
        self.assertListEqual([['aa_aa', 'Test A'], ['bb_bb', 'Test B']], test_list)

    def test_format_string_to_snakecase_abbreviation(self):
        self.assertEqual('aaa', StringFormatter.format_to_snakecase('AAA'))
        self.assertEqual('aaa_aaa', StringFormatter.format_to_snakecase('AAA-AAA'))

    def test_format_string_to_snakecase_camelcase(self):
        self.assertEqual('camel_case', StringFormatter.format_to_snakecase('camelCase'))

    def test_format_string_to_snakecase_leading_number(self):
        self.assertEqual('1_number', StringFormatter.format_to_snakecase('1 number'))

    def test_format_string_to_snakecase_repeated_special_chars(self):
        self.assertEqual('repeated_special_chars', StringFormatter.format_to_snakecase('repeated   special___chars'))

    def test_format_string_to_snakecase_whitespaces(self):
        self.assertEqual('no_leading_and_trailing', StringFormatter.format_to_snakecase(' no leading and trailing '))
        self.assertEqual('no_leading_and_trailing', StringFormatter.format_to_snakecase('\nno leading and trailing\t'))

    def test_format_string_to_snakecase_special_chars(self):
        self.assertEqual('special_chars', StringFormatter.format_to_snakecase('special!#@-_ chars'))
        self.assertEqual('special_chars', StringFormatter.format_to_snakecase('! special chars ?'))

    def test_format_string_to_snakecase_unicode(self):
        self.assertEqual('a_a_e_o_u', StringFormatter.format_to_snakecase(u'å ä ß é ö ü'))

    def test_format_string_to_snakecase_uppercase(self):
        self.assertEqual('uppercase', StringFormatter.format_to_snakecase('UPPERCASE'))
        self.assertEqual('upper_case', StringFormatter.format_to_snakecase('UPPER CASE'))
