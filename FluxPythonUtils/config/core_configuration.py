from typing import Dict
import threading
from abc import ABC, abstractmethod


class CoreConfiguration(ABC):
    """
    CoreConfiguration file to be inherited by base classes to be used in projects
    """
    project_to_config_dict = {}
    get_instance_mutex: threading.Lock = threading.Lock()

    def __init__(self, project_name):
        self.project_name = project_name

    @classmethod
    @abstractmethod
    def get_instance(cls, project_name: str):
        raise NotImplementedError
