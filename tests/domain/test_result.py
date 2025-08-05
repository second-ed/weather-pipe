import pytest

from weather_pipe.domain.result import Err, Ok, safe


@pytest.mark.parametrize(
    ("monad", "expected_result"),
    [
        pytest.param(Ok(3), Ok(7)),
        pytest.param(Err((), ValueError("Err path")), Err((), ValueError("Err path"))),
    ],
)
def test_string_of_oks(monad, expected_result):
    res = monad.bind(lambda x: Ok(x * 2)).map(safe(lambda x: x + 1)).flatten()
    if res.is_ok():
        assert res == expected_result
    else:
        assert not res.is_ok()
        assert res.error.__class__ is ValueError
        assert res.error.args == ("Err path",)
