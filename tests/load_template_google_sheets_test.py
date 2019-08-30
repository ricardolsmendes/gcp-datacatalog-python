from unittest import TestCase

from load_template_google_sheets import StringFormatter


class StringFormatterTest(TestCase):

    def test_format_string_to_snakecase_abbreviation(self):
        self.assertEqual(StringFormatter.format_to_snakecase('AAA'), 'aaa')
        self.assertEqual(StringFormatter.format_to_snakecase('AAA-AAA'), 'aaa_aaa')

    def test_format_string_to_snakecase_camelcase(self):
        self.assertEqual(StringFormatter.format_to_snakecase('camelCase'), 'camel_case')

    def test_format_string_to_snakecase_leading_number(self):
        self.assertEqual(StringFormatter.format_to_snakecase('1 number'), '1_number')

    def test_format_string_to_snakecase_repeated_special_chars(self):
        self.assertEqual(StringFormatter.format_to_snakecase('repeated   special___chars'), 'repeated_special_chars')

    def test_format_string_to_snakecase_whitespaces(self):
        self.assertEqual(StringFormatter.format_to_snakecase(' no leading and trailing '), 'no_leading_and_trailing')
        self.assertEqual(StringFormatter.format_to_snakecase('\nno leading and trailing\t'), 'no_leading_and_trailing')

    def test_format_string_to_snakecase_special_chars(self):
        self.assertEqual(StringFormatter.format_to_snakecase('special!#@-_ chars'), 'special_chars')
        self.assertEqual(StringFormatter.format_to_snakecase('! special chars ?'), 'special_chars')

    def test_format_string_to_snakecase_unicode(self):
        self.assertEqual(StringFormatter.format_to_snakecase(u'å ä ß é ö ü'), 'a_a_e_o_u')

    def test_format_string_to_snakecase_uppercase(self):
        self.assertEqual(StringFormatter.format_to_snakecase('UPPERCASE'), 'uppercase')
        self.assertEqual(StringFormatter.format_to_snakecase('UPPER CASE'), 'upper_case')
