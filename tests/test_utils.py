from hypothesis import given
from hypothesis.strategies import text

from src.weather_pipe.utils import clean_str


@given(text())
def test_clean_str(input_url):
    result = clean_str(input_url)
    assert "__" not in result
    assert result == result.lower()
    assert all(char.isalnum() for char in result if char != "_")
