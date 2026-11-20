"""Isolate application tests from the developer's local .env file."""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from control_plane.config import settings

def pytest_configure(config) -> None:
    """Hide the development file before test modules assemble their default app."""
    directory = TemporaryDirectory(prefix="control-plane-settings-")
    config.add_cleanup(directory.cleanup)
    patch = pytest.MonkeyPatch()
    config.add_cleanup(patch.undo)
    patch.setattr(settings, "ENV_FILE", Path(directory.name) / ".env")


@pytest.fixture
def isolated_subprocess_startup(tmp_path: Path) -> str:
    """Point fresh interpreter startup at a test-local .env file."""
    return (
        "from pathlib import Path\n"
        "from control_plane.config import settings as _settings\n"
        f"_settings.ENV_FILE = Path({str(tmp_path / '.env')!r})\n"
    )
