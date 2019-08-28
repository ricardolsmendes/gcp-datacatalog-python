from load_template_google_sheets import StringFormatter


def test_format_string_to_snakecase_abbreviation():
    assert StringFormatter.format_to_snakecase('AAA') == 'aaa'
    assert StringFormatter.format_to_snakecase('AAA-AAA') == 'aaa_aaa'


def test_format_string_to_snakecase_camelcase():
    assert StringFormatter.format_to_snakecase('camelCase') == 'camel_case'


def test_format_string_to_snakecase_leading_number():
    assert StringFormatter.format_to_snakecase('1 number') == '1_number'


def test_format_string_to_snakecase_repeated_special_chars():
    assert StringFormatter.format_to_snakecase('repeated   special___chars') == 'repeated_special_chars'


def test_format_string_to_snakecase_whitespaces():
    assert StringFormatter.format_to_snakecase(' no leading and trailing ') == 'no_leading_and_trailing'
    assert StringFormatter.format_to_snakecase('\nno leading and trailing\t') == 'no_leading_and_trailing'


def test_format_string_to_snakecase_special_chars():
    assert StringFormatter.format_to_snakecase('special!#@-_ chars') == 'special_chars'
    assert StringFormatter.format_to_snakecase('! special chars ?') == 'special_chars'


def test_format_string_to_snakecase_unicode():
    assert StringFormatter.format_to_snakecase(u'å ä ß é ö ü') == 'a_a_e_o_u'


def test_format_string_to_snakecase_uppercase():
    assert StringFormatter.format_to_snakecase('UPPERCASE') == 'uppercase'
    assert StringFormatter.format_to_snakecase('UPPER CASE') == 'upper_case'
