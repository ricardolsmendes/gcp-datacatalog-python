from unittest import TestCase

from load_template_csv import StringFormatter


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
