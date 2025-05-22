import logging
import json


logger = logging.getLogger(__name__)


def configuration_file(project_directory, configs_dir, conf_file):
    return [(project_directory / configs_dir) / conf_file]


def configuration_files(project_directory, configs_dir):
    configuration_files = []
    config_path = project_directory / configs_dir
    for file_path in config_path.glob("*.json"):
        configuration_files.append(file_path)

    return configuration_files


def configurations(project_directory, configs_directory, config_file=None):
    configuration = {}
    if config_file:
        conf_files = configuration_file(
            project_directory, configs_directory, config_file
        )
    else:
        conf_files = configuration_files(project_directory, configs_directory)

    logger.info(f"CONFIGURATION FILES:{conf_files}")
    for file_path in conf_files:
        config_contents = json.loads(file_path.read_text())
        configuration[file_path.stem] = config_contents

    return configuration
