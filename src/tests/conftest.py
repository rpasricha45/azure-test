import pytest

pytest.register_assert_rewrite('src.tests.test_functions')

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "asyncio: mark test as async"
    ) 