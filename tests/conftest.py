import pathlib
import sys

import pytest

from folio_data_anonymization.plugins.configurations import configurations

root_directory = pathlib.Path(__file__).parent.parent
dir = root_directory
sys.path.append(str(dir))


@pytest.fixture
def configs():
    project_dir = root_directory / "folio_data_anonymization"
    config_dir = "plugins/config"
    config = configurations(project_dir, config_dir)
    yield config
