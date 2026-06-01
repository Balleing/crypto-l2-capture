"""pytest configuration — sets asyncio_mode so all async tests run cleanly."""
# asyncio_mode = "auto" is set in pyproject.toml [tool.pytest.ini_options].
# This file exists to make pytest find the tests/ package correctly.
