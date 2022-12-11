import os
from FluxPythonUtils.config.core_configuration import CoreConfiguration


class Configuration(CoreConfiguration):

    def __init__(self, project_name):
        super().__init__(project_name)
        self.base_dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.log_file_path = os.path.join(self.base_dir_path, "scripts", "logs")
        self.yaml_config_path = os.path.join(self.base_dir_path, "config", "config.yaml")
        self.yaml_secret_path = os.path.join(self.base_dir_path, "config", "secret.yaml")
