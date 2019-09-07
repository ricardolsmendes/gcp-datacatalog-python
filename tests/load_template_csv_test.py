from unittest import TestCase
from unittest.mock import patch

from google.api_core.exceptions import PermissionDenied

from load_template_csv import DataCatalogHelper, StringFormatter

_LOOKED_UP_DATACATALOG_CLIENT = 'load_template_csv.datacatalog_v1beta1.DataCatalogClient'


@patch(f'{_LOOKED_UP_DATACATALOG_CLIENT}.__init__', lambda self, *args: None)
class DataCatalogHelperTest(TestCase):

    @patch(f'{_LOOKED_UP_DATACATALOG_CLIENT}.delete_tag_template')
    def test_delete_tag_template_should_call_client_library_method(self, mock_delete_tag_template):
        DataCatalogHelper().delete_tag_template(None)
        mock_delete_tag_template.assert_called_once()

    @patch(f'{_LOOKED_UP_DATACATALOG_CLIENT}.delete_tag_template')
    def test_delete_tag_template_should_succeed_nonexistent(self, mock_delete_tag_template):
        mock_delete_tag_template.side_effect = PermissionDenied(message='')
        DataCatalogHelper().delete_tag_template(None)
        mock_delete_tag_template.assert_called_once()

    @patch(f'{_LOOKED_UP_DATACATALOG_CLIENT}.get_tag_template')
    def test_tag_template_exists_should_return_true_existing(self, mock_get_tag_template):
        tag_template_exists = DataCatalogHelper().tag_template_exists(None)
        self.assertTrue(tag_template_exists)
        mock_get_tag_template.assert_called_once()

    @patch(f'{_LOOKED_UP_DATACATALOG_CLIENT}.get_tag_template')
    def test_tag_template_exists_should_return_false_nonexistent(self, mock_get_tag_template):
        mock_get_tag_template.side_effect = PermissionDenied(message='')
        tag_template_exists = DataCatalogHelper().tag_template_exists(None)
        self.assertFalse(tag_template_exists)
        mock_get_tag_template.assert_called_once()


class StringFormatterTest(TestCase):

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
