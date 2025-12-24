from spark.lib.utils import multiply_by_two


def test_multiply_by_two():
    assert multiply_by_two(3) == 6
    assert multiply_by_two(-1) == -2
