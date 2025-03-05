# Standard imports
import copy
import os
import logging
import re
import threading
from typing import List, Dict, Type, Final
import yaml
from pathlib import PurePath, Path
from datetime import datetime

# 3rd party packages
import pandas as pd

# FluxPythonUtils Modules
from FluxPythonUtils.scripts.yaml_importer import YAMLImporter
from FluxPythonUtils.scripts.model_base_utils import MsgspecBaseModel


LOG_FORMAT: Final[str] = "%(asctime)s : %(levelname)s : [%(filename)s : %(lineno)d] : %(message)s"


def find_acronyms_in_string(data: str) -> List[str]:
    return re.findall(r"[A-Z]{2,}", data)


def convert_camel_case_to_specific_case(data: str, char_to_be_added: str = '_', lower_case: bool = True):
    """
    Converts Camel cased vale to specific case. For Example snake case
    Parameters:
    -----------
        data: [str] Camel cased value to be converted
        char_to_be_added: [str]: Default: '_'
            character to be added between words to replace camel
            case to specific case like snake_case using '_' between words
        lower_case: [bool] make return as lower_value if True else ignore
    """

    if acronyms_list := find_acronyms_in_string(data):
        for acronym in acronyms_list:
            # If whole provided data is acronym
            if data == acronym:
                return data.lower()
            # else making acronym in data snake-cased before whole data
            # is going to be converted to snake case (or some specific case)
            else:
                if data.startswith(acronym):
                    data = data.replace(acronym, acronym[:-1].lower() + acronym[-1])
                elif data.endswith(acronym):
                    data = data.replace(acronym, char_to_be_added + acronym.lower())
                else:
                    data = data.replace(acronym, char_to_be_added + acronym[:-1].lower() + acronym[-1])
    # else not required: If data doesn't contain acronym then ignore

    if lower_case:
        return re.sub(r'(?<!^)(?=[A-Z])', char_to_be_added, data).lower()
    else:
        return re.sub(r'(?<!^)(?=[A-Z])', char_to_be_added, data)


def file_exist(path: str | PurePath) -> bool:
    """
    Function to check if file exists.

    Arguments
    ---------
    path: Takes complete path as input argument.

    Returns
    -------
    bool: Returns True if file exists and else otherwise.
    """
    return os.path.exists(path if isinstance(path, str) else str(path))


def load_yaml(file_path: str) -> Dict:
    with open(file_path) as f:
        data = yaml.load(f, Loader=YAMLImporter)
        return data


def is_file_updated(file_to_check: Path, last_read_ts=None):
    if file_to_check.is_file():
        file_to_check = file_to_check.resolve()
        # get modification time
        last_mod_timestamp = file_to_check.stat().st_mtime
        if not last_read_ts:
            return last_mod_timestamp
        # convert timestamp into DateTime object
        # last_mod_datetime = datetime.datetime.fromtimestamp(last_mod_timestamp)
        if last_mod_timestamp > last_read_ts:
            return last_mod_timestamp
        # else return last_read_ts (same as in final else return)
    # all else return last_read_ts
    return last_read_ts


def _get_file_handler(log_file_dir_path: str, log_file_name: str):
    if log_file_dir_path is not None:
        os.makedirs(log_file_dir_path, exist_ok=True)
    log_file_path_name: str = os.path.join(log_file_dir_path, log_file_name)

    if file_exist(log_file_path_name):
        datetime_str: str = datetime.now().strftime("%Y%m%d.%H%M%S")
        os.rename(log_file_path_name, f"{log_file_path_name}.{datetime_str}")

    with open(log_file_path_name, "w+"):
        pass
    file_handler = logging.FileHandler(log_file_path_name)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return file_handler


def pandas_df_to_model_obj_list(read_df, MsgspecType: Type[MsgspecBaseModel],
                                rename_col_names_to_snake_case: bool = False,
                                rename_col_names_to_lower_case: bool = True):

    if rename_col_names_to_snake_case:
        # replace any space in col name with _ and convert name to snake_case
        col_names: List[str] = read_df.columns.tolist()
        old_to_new_col_name_dict: Dict[str, str] = {}
        for col_name in col_names:
            orig_col_name: str = copy.deepcopy(col_name)
            col_name = col_name.replace('/', '')
            col_name = col_name.replace(' ', '')
            col_name = col_name.replace('(', '')
            col_name = col_name.replace(')', '')
            new_col_name: str = convert_camel_case_to_specific_case(col_name,
                                                                    lower_case=rename_col_names_to_lower_case)
            new_col_name.replace(' ', '')
            old_to_new_col_name_dict[orig_col_name] = new_col_name
        read_df.rename(columns=old_to_new_col_name_dict, inplace=True)
    read_df = pd.DataFrame(read_df).replace({'': None})
    data_dict_list = read_df.to_dict(orient='records')
    msgspec_obj_dict = MsgspecType.from_dict_list(data_dict_list, strict=False)

    return msgspec_obj_dict


class YAMLConfigurationManager:
    """
    Class handling to make the fetching of yaml configurations efficient

    Created the cache of fetched content in dict with path as key and returns from
    there if exists already else creates entry in cache
    """
    load_yaml_mutex: threading.Lock = threading.Lock()
    first: bool = True

    @staticmethod
    def proxy_setting_boilerplate():
        if YAMLConfigurationManager.first:
            # add proxy settings
            YAMLConfigurationManager.first = False

    @classmethod
    def _yaml_loader(cls, file_path: str) -> Dict:
        with open(file_path) as f:
            data = yaml.load(f, Loader=YAMLImporter)
        return data

    @classmethod
    def _str_loader(cls, file_path: str) -> str:
        with open(file_path) as f:
            data = f.read()
        return data

    @classmethod
    def load_yaml_configurations(cls, config_file_path: str | None = None,
                                 default_config_file_path: str | None = "configurations.yaml",
                                 load_as_str: bool = False) -> Dict[any, any] | str:
        # boilerplate debug prints for all projects proxy settings, DO NOT DELETE
        YAMLConfigurationManager.proxy_setting_boilerplate()

        if config_file_path is None:
            with cls.load_yaml_mutex:
                if file_exist(default_config_file_path):
                    if not load_as_str:
                        return cls._yaml_loader(default_config_file_path)
                    else:
                        return cls._str_loader(default_config_file_path)
                else:
                    err_str = f"No {default_config_file_path} exists in this script's directory. " \
                              f"Either make one or pass another file's path as parameter;;;"
                    logging.exception(err_str)
                    raise Exception(err_str)
        else:
            with cls.load_yaml_mutex:
                if file_exist(config_file_path):
                    if not load_as_str:
                        return cls._yaml_loader(config_file_path)
                    else:
                        return cls._str_loader(config_file_path)
                else:
                    err_str = f"No file: {config_file_path} exist"
                    logging.exception(err_str)
                    raise FileNotFoundError(err_str)

    @classmethod
    def update_yaml_configurations(cls, yaml_content: Dict | str, config_file_path: str | None = None,
                                   default_config_file_path: str | None = "configurations.yaml") -> None:
        if isinstance(yaml_content, Dict):
            is_dict = True
        else:
            is_dict = False

        if config_file_path is None:
            if file_exist(default_config_file_path):
                with open(default_config_file_path, "w") as f:
                    if is_dict:
                        yaml.dump(yaml_content, f)
                    else:
                        f.write(yaml_content)
            else:
                err_str = f"No {default_config_file_path} exists in this script's directory. " \
                          f"Either make one or pass another file's path as parameter;;;"
                logging.exception(err_str)
                raise Exception(err_str)
        else:
            if file_exist(config_file_path):
                with open(config_file_path, "w") as f:
                    if is_dict:
                        yaml.dump(yaml_content, f)
                    else:
                        f.write(yaml_content)
            else:
                err_str = f"No file: {config_file_path} exist"
                logging.exception(err_str)
                raise Exception(err_str)
