# Standard imports
import asyncio
import copy
import inspect
import math
import os
import logging
import pickle
import re
import threading
import time
from typing import List, Dict, TypeVar, Callable, Tuple, Type, Set, Any, Iterable, Final
import sys
import socket
from contextlib import closing
import pandas
import pexpect
import yaml
from enum import IntEnum
import json
from pathlib import PurePath, Path
import csv
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError, ConnectionClosed
from requests import Response
from datetime import datetime, timedelta
import timeit
import functools
import psutil
import shutil

# 3rd party packages
from pydantic import BaseModel
import pandas as pd
from pymongo import MongoClient
import pymongoarrow.monkey
from beanie.odm.documents import DocType
from pendulum import DateTime
from fastapi import WebSocket
from fastapi import HTTPException

# FluxPythonUtils Modules
from FluxPythonUtils.scripts.yaml_importer import YAMLImporter

# Adds extra utility methods of pymongoarrow.monkey to pymongo collection objects
pymongoarrow.monkey.patch_all()

BaseModelOrItsDerivedType = TypeVar('BaseModelOrItsDerivedType', BaseModel, Callable)


# Script containing all the utility: handy functions / classes / enums / decorators

class EmptyFileError(Exception):
    """Exception raised for unexpected empty file.
    Attributes:
        file_path -- file path that was found with empty file
        message -- brief explanation of the error (file path passed is auto-appended f-str below)
    """

    def __init__(self, file_path: str, message: str = "Empty file found!"):
        self.file_path = file_path
        self.message = message
        super().__init__(f"{self.message}, file_path: {self.file_path}")


class ServieUnavailable(Exception):
    """Exception raised to represent service unavailability.

    Attributes:
        file_path -- file path that was found with empty file
        message -- brief explanation of the error (file path passed is auto-appended f-str below)
    """

    def __init__(self, file_path: str, message: str = "Empty file found!"):
        self.file_path = file_path
        self.message = message
        super().__init__(f"{self.message}, file_path: {self.file_path}")


def log_n_except(original_function):
    def wrapper_function(*args, **kwargs):
        try:
            result = original_function(*args, **kwargs)
            return result
        except Exception as e:
            err_str = f"Client Error Occurred in function: {original_function.__name__};;;args: {args}, " \
                      f"kwargs: {kwargs}, exception: {e}"
            logging.exception(err_str)
            raise Exception(err_str)

    return wrapper_function


class HTTPRequestType(IntEnum):
    UNSPECIFIED = 0
    GET = 1
    POST = 2
    DELETE = 3
    PUT = 4
    PATCH = 5


def http_response_as_class_type(url, response, expected_status_code, pydantic_type: BaseModelOrItsDerivedType,
                                http_request_type: HTTPRequestType):
    status_code, response_json = handle_http_response(response)
    if status_code == expected_status_code:
        if isinstance(response_json, bool):
            return response_json
        else:
            if isinstance(response_json, list):
                return [pydantic_type(**response_obj) for response_obj in response_json]
            else:
                return pydantic_type(**response_json)
    else:
        raise Exception(f"failed for url: {url}, http_request_type: {str(http_request_type)} "
                        f"http_error: {response_json}, status_code: {status_code}")


def handle_http_response(response: Response):
    if response is None:
        return '{"error: passed response is None - no http response to handle!"}'
    if response.ok:
        return response.status_code, json.loads(response.content)
    response_err_str: str | None = ""
    if response.content is not None:
        response_err_str += f"content: {response.content} "
    if response.reason is not None:
        response_err_str += f"reason: {response.reason} "
    if response.text is not None:
        response_err_str += f"text: {response.text} "
    if len(response_err_str) == 0:
        response_err_str = None
    return response.status_code, response_err_str


def handle_http_response_extended(response: Response):
    if response is None:
        return '{"error: passed response is None - no http response to handle!"}'
    if response.ok:
        return response.status_code, json.loads(response.content)
    if response.content is not None:
        try:
            content = json.loads(response.content)
            if 'errors' in content:
                return response.status_code, content['errors']
            if 'error' in content:
                if 'message' in content:
                    content['error'] = content['error'] + " message- " + content['message']
                return response.status_code, content['error']
            if 'messages' in content:
                return response.status_code, content['messages']
            if 'detail' in content:
                return response.status_code, content['detail']
        except json.JSONDecodeError:
            # handle as error
            if response.reason is not None:
                content = response.reason
                if response.text is not None:
                    content += (" text: " + response.text)
                return response.status_code, content
            elif response.text is not None:
                return response.status_code, response.text
            else:
                return response.status_code, None
    return response.status_code, None


def csv_to_xlsx(file_name: str, csv_data_dir: PurePath | None = None, xlsx_data_dir: PurePath | None = None):
    if csv_data_dir is None:
        csv_data_dir = PurePath(__file__).parent / "data"
    if xlsx_data_dir is None:
        xlsx_data_dir = csv_data_dir
    csv_path = PurePath(csv_data_dir / f"{file_name}.csv")
    xlsx_path = PurePath(xlsx_data_dir / f"{file_name}.xlsx")
    csv_as_df = pd.read_csv(str(csv_path))
    xlsx_writer = pd.ExcelWriter(str(xlsx_path))
    csv_as_df.to_excel(xlsx_writer, index=False, header=True, sheet_name="target")
    xlsx_writer.close()


def dict_or_list_records_csv_writer_ext(file_name: str, records: Dict | List, record_type,
                                        data_dir: PurePath | None = None):
    fieldnames = record_type.schema()["properties"].keys()
    dict_or_list_records_csv_writer(file_name, records, fieldnames, record_type, data_dir)


def dict_or_list_records_csv_writer(file_name: str, records: Dict | List, fieldnames, record_type,
                                    data_dir: PurePath | None = None):
    """
    fieldnames can be subset of fields you wish to write in CSV
    constraints:
    1. records can be collection of Dict or List
    2. specific record MUST support dict() method
    """
    if data_dir is None:
        data_dir = PurePath(__file__).parent / "data"
    csv_path = PurePath(data_dir / f"{file_name}.csv")
    with open(csv_path, "w", encoding="utf-8", newline='') as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        if isinstance(records, Dict):
            for record in records.values():
                json_dict = json.loads(record_type(**record.dict()).json())
                writer.writerow(json_dict)
        elif isinstance(records, List):
            for record in records:
                writer.writerow(json.loads(record_type(**record.dict()).json()))
        else:
            raise Exception(
                f"Unexpected: Un-supported type passed, expected Dict or List found type: {type(records)} of "
                f"object: {str(records)}")


def get_csv_path_from_name_n_dir(file_name: str, data_dir: PurePath | None = None):
    if data_dir is None:
        data_dir = PurePath(__file__).parent / "data"
    return PurePath(data_dir / f"{file_name}.csv")


def pandas_df_to_pydantic_obj_list(read_df, PydanticType: BaseModelOrItsDerivedType,
                                   rename_col_names_to_snake_case: bool = False,
                                   rename_col_names_to_lower_case: bool = True):
    class PydanticClassTypeList(BaseModel):
        __root__: List[PydanticType]

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
    record_dict = {"__root__": data_dict_list}
    pydantic_obj_list: PydanticClassTypeList = PydanticClassTypeList(**record_dict)
    return pydantic_obj_list.__root__


def dict_or_list_records_csv_reader(file_name: str, PydanticType: BaseModelOrItsDerivedType,
                                    data_dir: PurePath | None = None, rename_col_names_to_snake_case: bool = False,
                                    rename_col_names_to_lower_case: bool = True,
                                    no_throw: bool = False) -> List[BaseModel]:
    """
    At this time the method only supports list of pydantic_type extraction form csv
    """
    if data_dir is None:
        data_dir = PurePath(__file__).parent / "data"
    if not file_name.endswith(".csv"):
        file_name = f"{file_name}.csv"
    csv_path = data_dir / file_name
    str_csv_path = str(csv_path)
    if os.path.exists(str_csv_path) and os.path.getsize(str_csv_path) > 0:
        read_df = pd.read_csv(csv_path, keep_default_na=False)
        return pandas_df_to_pydantic_obj_list(read_df, PydanticType, rename_col_names_to_snake_case,
                                              rename_col_names_to_lower_case)
    elif not no_throw:
        raise Exception(f"dict_or_list_records_csv_reader invoked on empty or no csv file: {str(csv_path)}")
    return []


def str_from_file(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8", newline='') as fp:
        return fp.read()


def get_json_array_as_pydantic_dict(json_key: str, json_data_list, PydanticType: Callable) \
        -> Dict[str, BaseModelOrItsDerivedType]:
    pydantic_dict: Dict[str, BaseModelOrItsDerivedType] = dict()
    for json_data in json_data_list:
        pydantic_key = json_data[json_key]
        pydantic_dict[pydantic_key] = PydanticType(**json_data)
    return pydantic_dict


def store_json_or_dict_to_file(file_name: str, json_dict, data_dir: PurePath | None = None):
    # Serialize json dict
    json_str = json.dumps(json_dict, indent=4)
    # write to json file
    store_json_str_to_file(file_name, json_str, data_dir)


def store_json_str_to_file(file_name: str, json_str, data_dir: PurePath | None = None, mode="w"):
    if data_dir is None:
        data_dir = PurePath(__file__).parent / "data"
    json_file_path = PurePath(data_dir / f"{file_name}.json")
    with open(json_file_path, mode) as outfile:
        outfile.write(json_str)


def store_str_list_to_file(file_name: str, str_list: List[str], data_dir: PurePath | None = None, no_ext: bool = True,
                           mode="w", separator="\n"):
    if data_dir is None:
        data_dir = PurePath(__file__).parent / "data"
    file_path: PurePath
    if no_ext:
        file_path = PurePath(data_dir / f"{file_name}")
    else:
        file_path = PurePath(data_dir / f"{file_name}.txt")

    with open(file_path, mode) as outfile:
        str_: str
        for str_ in str_list:
            outfile.write(str_ + separator)


def scp_handler(scp_src_path: PurePath, scp_src_user, scp_src_server, scp_dest_dir: PurePath, scp_password: str) -> bool:
    scp_command = f"scp -q {scp_src_user}@{scp_src_server}:{scp_src_path} {scp_dest_dir}/."
    expect_ = "password:"
    if not pexpect_command_expect_response_handler(scp_command, expect_, scp_password):
        logging.error(f"scp_command failed: likely key error or connection timeout, try cmd manually once - happens "
                      f"once for new cert new server, cmd: {scp_command}")
        return False
    return True  # TODO: needs improvement - failures also report True it appears


def pexpect_command_expect_response_handler(command_: str, expect_: str, response_: str) -> int:
    retval: bool = False
    try:
        handler = pexpect.spawn(command_)
        i = handler.expect([expect_, pexpect.EOF])
        if i == 0:  # all good send password
            handler.sendline(response_)
            handler.expect(pexpect.EOF)
            retval = True
        elif i == 1:
            logging.error(f"pexpect_command_expect_response_handler command failed, try cmd manually cmd: {command_}")
        else:
            logging.error(f"pexpect_command_expect_response_handler: unexpected expect() returned: {i} for cmd "
                          f"response of: {command_}, expected 0 or 1")
        handler.close()
        return retval
    except Exception as e:
        logging.exception(f"pexpect_scp_handler: failed for cmd: {command_}, exception: {e}")
        return retval


def load_json_dict_from_file(file_name: str, data_dir: PurePath | None = None, must_exist: bool = True):
    if not file_name.endswith(".json"):
        if data_dir is None:
            data_dir = PurePath(__file__).parent / "data"
        # else use passed data dir
        json_file_path = PurePath(data_dir / f"{file_name}.json")
    else:  # file name passed is complete file path
        json_file_path = file_name  # filename has path and suffix
    if (not must_exist) and (not file_exist(str(json_file_path))):
        return None
    if os.path.getsize(json_file_path) > 0:
        # Open JSON file
        json_file_obj = open(json_file_path)
        # return JSON object as a dictionary
        json_dict = json.load(json_file_obj)
        return json_dict
    else:
        raise EmptyFileError(json_file_path, f"load_json_dict_from_file: json file found, but its empty!")


def get_match_file_from_path(file_name_prefix: str, file_name_suffix: str, file_store_root_dir_path: PurePath):
    matched_files: List[any] = list()
    files_matched = (Path(file_store_root_dir_path)).glob(f"{file_name_prefix}*{file_name_suffix}")
    if files_matched is not None:
        for filename in files_matched:
            matched_files.append(filename)
    return matched_files


def archive_match_files(file_name_prefix: str, file_store_root_dir_path: PurePath,
                        ignore_files: List | None = None, clear_archive: bool = False):
    # check match files exist
    files_to_archive = (Path(file_store_root_dir_path)).glob(f"{file_name_prefix}*")
    if files_to_archive is not None:
        # delete old archive
        archive_pure_path = file_store_root_dir_path / "archive"
        archive_path = Path(archive_pure_path)
        if not Path.exists(archive_path):
            # create archive dir if it does not exist
            os.mkdir(str(archive_path))
        else:
            # cleanup since archive exists
            # remove all file with matching prefixes
            if clear_archive:
                files_to_delete = archive_path.glob(f"{file_name_prefix}*")
                for filename in files_to_delete:
                    os.remove(str(filename))
        # move files_to_archive to archive
        for filename in files_to_archive:
            if ignore_files is None or (os.path.basename(filename) not in ignore_files):
                archive_filename = archive_pure_path / os.path.basename(filename)
                os.rename(filename, archive_filename)


def store_to_pickle_file(file_name: str, python_object, data_dir: PurePath | None = None, mode='wb'):
    if data_dir is None:
        data_dir = PurePath(__file__).parent / "data"
    pickle_file_path = PurePath(data_dir / f"{file_name}.pickle")
    with open(pickle_file_path, mode) as fp:
        pickle.dump(python_object, fp)


def load_from_pickle_file(file_name: str, data_dir: PurePath | None = None, mode='rb'):
    if data_dir is None:
        data_dir = PurePath(__file__).parent / "data"
    pickle_file_path = PurePath(data_dir / f"{file_name}.pickle")
    if file_exist(str(pickle_file_path)):
        with open(pickle_file_path, mode) as fp:
            file_content = fp.read()
            python_object = pickle.loads(bytes(file_content))
            return python_object
    else:
        return None  # file not found


def makedir(path: str) -> None:
    """
    Function to make directory. Takes complete path as input argument.
    """
    os.mkdir(path)


def delete_file(path: str) -> None:
    """
    Function to delete file. Takes complete path as input argument.
    """
    os.remove(path)


def file_exist(path: str) -> bool:
    """
    Function to check if file exists.

    Arguments
    ---------
    path: Takes complete path as input argument.

    Returns
    -------
    bool: Returns True if file exists and else otherwise.
    """
    return os.path.exists(path)


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


LOG_FORMAT: Final[str] = "%(asctime)s : %(levelname)s : [%(filename)s : %(lineno)d] : %(message)s"


def configure_logger(level: str | int, log_file_dir_path: str | None = None, log_file_name: str | None = None) -> None:
    """
    Function to config the logger in your trigger script of your project, creates log file in given log_dir_path.
    Takes project_name as parameter to fetch Level from configurations.py.
    """
    if log_file_name is None:
        # TODO LAZY: rename default log file name to "holder_dir_name.log"
        log_file_name = "logs.log"
    # else not required: if file exists then using that name

    if log_file_dir_path is not None:
        os.makedirs(log_file_dir_path, exist_ok=True)
        log_file_path: str = os.path.join(log_file_dir_path, log_file_name)
    else:
        log_file_path: str = log_file_name

    if file_exist(log_file_path):
        datetime_str: str = datetime.now().strftime("%Y%m%d.%H%M%S")
        os.rename(log_file_path, f"{log_file_path}.{datetime_str}")

    with open(log_file_path, "w+"):
        pass

    if level is not None:
        if isinstance(level, str):
            """
            CRITICAL	50
            ERROR	    40
            WARNING	    30
            INFO	    20
            DEBUG	    10
            """
            match level.lower():
                case "debug":
                    level = logging.DEBUG
                case "info":
                    level = logging.INFO
                case "warning":
                    level = logging.WARNING
                case "error":
                    level = logging.ERROR
                case "critical":
                    level = logging.CRITICAL
                case other:
                    error_msg: str = f"Unsupported logging level: {other}"
                    raise Exception(error_msg)
        # else not required: else taking int log lvl value provided

    else:
        error_msg: str = f"logger level cant be none"
        raise Exception(error_msg)

    logging.basicConfig(
        filename=log_file_path,
        level=level,
        format=LOG_FORMAT,
        force=True
    )


def add_logging_level(level_name: str, level_num: int, method_name: str | None = None):
    """
    Important: Impl took from
    https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945

    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    > addLoggingLevel('TRACE', logging.DEBUG - 5)
    > logging.getLogger(__name__).setLevel("TRACE")
    > logging.getLogger(__name__).trace('that worked')
    > logging.trace('so did this')
    > logging.TRACE
    5
    """
    if not method_name:
        method_name = level_name.lower()

    if hasattr(logging, level_name):
        raise AttributeError('{} already defined in logging module'.format(level_name))
    if hasattr(logging, method_name):
        raise AttributeError('{} already defined in logging module'.format(method_name))
    if hasattr(logging.getLoggerClass(), method_name):
        raise AttributeError('{} already defined in logger class'.format(method_name))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def log_for_level(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(level_num, message, args, **kwargs)

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(logging.getLoggerClass(), method_name, log_for_level)
    setattr(logging, method_name, log_to_root)


def add_logging_levels(log_lvl_to_log_value_dict_list: List[Dict[str, int]]):
    for log_lvl_to_log_value_dict in log_lvl_to_log_value_dict_list:
        log_lvl_name_list = list(log_lvl_to_log_value_dict.keys())
        if (log_key_len := len(log_lvl_name_list)) == 1:
            log_lvl_name = log_lvl_name_list[0]
            log_lvl_value = parse_to_int(log_lvl_to_log_value_dict[log_lvl_name])
            add_logging_level(log_lvl_name, log_lvl_value)
        else:
            err_str = f"Expected only one key for log_lvl_name with value log_lvl_value, received: {log_key_len}, " \
                      f";;; log_lvl_to_log_value_dict_list: {log_lvl_to_log_value_dict_list}"
            logging.exception(err_str)
            raise Exception(err_str)


def set_logger_level(log_level: str):
    logging.getLogger().setLevel(log_level)


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


def create_logger(logger_name: str, log_lvl: int | str, log_file_dir_path: str, log_file_name: str) -> logging.Logger:
    logger: logging.Logger = logging.getLogger(logger_name)
    logger.setLevel(log_lvl)  # better to have too much log than not enough
    logger.addHandler(_get_file_handler(log_file_dir_path, log_file_name))
    # the Logger class has a propagate attribute that controls whether log messages are propagated
    # to parent loggers. The default value of propagate is True, which means that log messages are propagated
    # up the logger hierarchy until they reach the root logger
    logger.propagate = False
    return logger


def filter_keywords(filter_str: str, keywords_list: List[str]) -> bool:
    """
    Function to provide Boolean if data available in given filter list.
    Returns True if regex finds data else False
    """

    for keyword in keywords_list:
        if re.search(str(keyword), str(filter_str)):
            # If it founds filter word, returns True else returns False
            return True
    # Else returning False if filter_keywords not present in user's cache json already

    return False


class YAMLConfigurationManager:
    """
    Class handling to make the fetching of yaml configurations efficient

    Created the cache of fetched content in dict with path as key and returns from
    there if exists already else creates entry in cache
    """
    load_yaml_mutex: threading.Lock = threading.Lock()
    path_to_content_dict: Dict[str, str] | Dict[str, Dict] = {}

    @classmethod
    def _yaml_loader(cls, file_path: str) -> Dict:
        with open(file_path) as f:
            data = yaml.load(f, Loader=YAMLImporter)
        cls.path_to_content_dict[f"{file_path}_dict"] = data
        return data

    @classmethod
    def _str_loader(cls, file_path: str) -> str:
        with open(file_path) as f:
            data = f.read()
        cls.path_to_content_dict[f"{file_path}_str"] = data
        return data

    @classmethod
    def load_yaml_configurations(cls, config_file_path: str | None = None,
                                 default_config_file_path: str | None = "configurations.yaml",
                                 load_as_str: bool = False) -> Dict | str:
        if config_file_path is None:
            with cls.load_yaml_mutex:
                if default_config_file_path in cls.path_to_content_dict:
                    key = default_config_file_path + "_str" if load_as_str else "_dict"
                    return cls.path_to_content_dict.get(key)
                else:
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
                if config_file_path in cls.path_to_content_dict:
                    key = config_file_path + "_str" if load_as_str else "_dict"
                    return cls.path_to_content_dict.get(key)
                else:
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


def capitalized_to_camel_case(value: str) -> str:
    if acronyms_list := find_acronyms_in_string(value):
        for acronym in acronyms_list:
            if value.startswith(acronym):
                return value.replace(acronym, acronym[:-1].lower() + acronym[-1])
    # else not required: If data doesn't contain acronym then ignore

    return "".join([value[0].lower(), value[1:]])


def non_capitalized_to_camel_case(value: str) -> str:
    value = re.sub(r"(_|-)+", " ", value).title().replace(" ", "")
    return "".join([value[0].lower(), value[1:]])


def convert_to_camel_case(value: str) -> str:
    if value != value.lower() and value != value.upper() and "_" not in value:
        if value[0].isupper():
            return capitalized_to_camel_case(value)
        else:
            return value
    else:
        return non_capitalized_to_camel_case(value)


def convert_to_capitalized_camel_case(value: str) -> str:
    value_camel_cased = convert_to_camel_case(value)
    return value_camel_cased[0].upper() + value_camel_cased[1:]


def update_bucketed_list(bucket_size: int, records_in: List[Any] | Iterable[Any],
                         bucketed_records_list_out: List[List[Any]]) -> None:
    records: List[Any] | None = None
    for idx, record in enumerate(records_in):
        if idx % bucket_size == 0:
            if records:
                bucketed_records_list_out.append(records)
            records = [record]
        else:
            records.append(record)
    if records:
        bucketed_records_list_out.append(records)


def avg_of_new_val_sum_to_avg(avg: int | float, new_val: int | float, total_length: int) -> int | float:
    """
    Computes average of, average of n numbers + n+1 number
    :param avg: Last Average val
    :param new_val: new val to be added
    :param total_length: Total observations (including new value)
    :return: new computed average
    """
    return avg + ((new_val - avg) / total_length)


def avg_of_new_val_sub_to_avg(avg: int | float, new_val: int | float, total_length: int) -> int | float:
    """
    Computes average of, average of n numbers - n+1 number
    :param avg: Last Average val
    :param new_val: new val to be added
    :param total_length: Total observations (including new value)
    :return: new computed average
    """
    return (avg * total_length - new_val) / (total_length - 1)


def parse_string_to_original_types(value: str) -> str | int | bool | float:
    """
    Returns int if value string contains only numbers, float if contains only float value,
    bool if value contains string parsed bool and returns same value if all cases are not matched
    """
    # bool check
    if value in ["True", "False", "true", "false"]:
        return True if value in ["True", "true"] else False
    # int check
    elif value.isdigit():
        return int(value)
    # float check
    elif re.match(r'^-?\d+(?:\.\d+)$', value) is not None:
        return float(value)
    # else same str
    else:
        return value.strip()


# TODO LAZY - At this time it only prevents adding duplicates - no logical merge - investigate more and generalize
def _find_matching_list(underlying_updated_list: List, stored_list_of_list: List[List[any]]) -> List[any] | None:
    matching_list = list()
    for stored_list in stored_list_of_list:
        if stored_list == underlying_updated_list:
            matching_list.append(stored_list)
    return matching_list


def compare_n_patch_list(stored_list: List, updated_list: List):
    if stored_list:
        # get datatype of 1st list element, others must be same datatype (multi datatype list patch not-supported)
        if isinstance(stored_list[0], list):  # list of list
            if isinstance(updated_list[0], list):
                for underlying_updated_list in updated_list:
                    underlying_stored_list = _find_matching_list(underlying_updated_list, stored_list)
                    if underlying_stored_list is None:
                        # this underlying updated list is new - append to stored_list (stored list of list)
                        stored_list.append(underlying_updated_list)
                    else:
                        compare_n_patch_list(underlying_stored_list, underlying_updated_list)
            else:
                err_str = "updated_list's elements are not same datatypes as stored_list's elements;;;"
                logging.exception(err_str)
                raise Exception(err_str)
        elif isinstance(stored_list[0], dict):
            # If elements are of dict type then checking if id key is present in elements
            if stored_list[0].get("_id") is not None:
                stored_id_idx_dict: Dict = {stored_obj.get("_id"): idx for idx, stored_obj in enumerate(stored_list)}
                for index, update_dict in enumerate(updated_list):
                    if isinstance(update_dict, dict):
                        if (updated_id := update_dict.get("_id")) is not None:
                            # If id is new then appending update_dict to main list
                            if updated_id not in stored_id_idx_dict:
                                # If update_dict only has id and the id is not in stored_list - avoid append
                                # Update list has id checked above + len == 1 confirms all it has is id
                                if len(update_dict) == 1:
                                    err_str = ("repeated update obj only has id and that id value is not found in any "
                                               f"existing repeated objects - ignoring this update, "
                                               f"update object: {update_dict}")
                                    logging.exception(err_str)
                                    raise Exception(err_str)
                                else:
                                    stored_list.append(update_dict)
                            else:
                                stored_index = stored_id_idx_dict[updated_id]
                                # If update_dict only has id and the id is same as in stored_list - delete entry
                                # Update list has id checked above + len == 1 confirms all it has is id
                                if len(update_dict) == 1:
                                    stored_list.remove(stored_list[stored_index])
                                    stored_id_idx_dict = \
                                        {stored_obj.get("_id"): idx for idx, stored_obj in enumerate(stored_list)}
                                else:
                                    # patch operation on dict in stored_list to update
                                    stored_list[stored_index] = \
                                        compare_n_patch_dict(stored_list[stored_index], update_dict)
                        else:
                            err_str = "updated_list's dict elements don't have id field but stored_list's " \
                                      "elements do;;;"
                            logging.exception(err_str)
                            raise Exception(err_str)
                    else:
                        err_str = "updated_list's elements are not same datatypes as stored_list's elements"
                        logging.exception(err_str)
                        raise Exception(err_str)
                return stored_list
            # If no id key found in dicts, since no way to compare just append any new updates to main list
            else:
                stored_list.extend(updated_list)
                return stored_list
        else:  # non container type list are just extended
            stored_list.extend(updated_list)
            return stored_list
    else:
        stored_list.extend(updated_list)
        return stored_list


def compare_n_patch_dict(stored_dict: Dict, updated_dict: Dict):
    for key, updated_value in updated_dict.items():
        stored_value = stored_dict[key]
        if isinstance(stored_value, dict):
            if updated_value is not None:
                # dict value type is container, pass extracted (reference modified directly)
                compare_n_patch_dict(stored_value, updated_value)
            else:
                stored_dict[key] = updated_value
        elif isinstance(stored_value, list):
            if updated_value is not None:
                # list value type is container, pass extracted (reference modified directly)
                compare_n_patch_list(stored_value, updated_value)
            else:
                stored_dict[key] = updated_value
        elif stored_value != updated_value:  # avoid unwarranted lookup(simple types)/construction(complex types)
            # non container types are just assigned (+ no patch support: add/overwrite if set)
            stored_dict[key] = updated_value
        # else not required - old and new val are same
    return stored_dict


def get_beanie_host_port_from_env(project_name: str, default_host: str = "127.0.0.1",
                                  default_port: int = 8020) -> Tuple[str, int]:
    port_env_var_name: str = f"{project_name.upper()}_BEANIE_PORT"
    host_str: str = default_host if ((host_env := os.getenv("HOST")) is None or len(host_env) == 0) else host_env
    port_str: str = str(default_port) if ((port_env := (os.getenv(port_env_var_name))) is None or
                                          len(port_env) == 0) else port_env
    int_port: int = parse_to_int(port_str)
    return host_str, int_port


def get_cache_host_port_from_env(project_name: str, default_host: str = "127.0.0.1",
                                 default_port: int = 8030) -> Tuple[str, int]:
    port_env_var_name: str = f"{project_name.upper()}_CACHE_PORT"
    host_str: str = default_host if ((host_env := os.getenv("HOST")) is None or len(host_env) == 0) else host_env
    port_str: str = str(default_port) if ((port_env := (os.getenv(port_env_var_name))) is None or
                                          len(port_env) == 0) else port_env
    int_port: int = parse_to_int(port_str)
    return host_str, int_port


def db_collections(mongo_server_uri: str, database_name: str, ignore_collections: List[str] | None = None):
    """
    Generator to get collection instance from mongodb (ignores collections present in ignore_collections list)
    :param mongo_server_uri:
    :param database_name: Name of db
    :param ignore_collections: name of collections to be ignored
    :return: collections instance
    """
    client: MongoClient | None = None
    try:
        client = MongoClient(mongo_server_uri)
        db = client.get_database(name=database_name)
        collections: List[str] = db.list_collection_names()
        for collection in collections:
            if collection not in ignore_collections and "." not in collection:
                yield db[collection]
    except Exception as e:
        err_str = f"drop_mongo_collections failed for DB: {database_name};;;exception: {e}"
        logging.exception(err_str)
        raise e
    finally:
        client.close()


def drop_mongo_collections(mongo_server_uri: str, database_name: str, ignore_collections: List[str] | None = None) -> None:
    """
    Drops all collections present in collections except ``ignore_collections``
    :param mongo_server_uri: Mongo Server that requires Cleaning
    :param database_name: Name of db
    :param ignore_collections: name of collections to be ignored from getting dropped
    :return: None
    """
    for collection in db_collections(mongo_server_uri, database_name, ignore_collections):
        collection.drop()


def drop_mongo_database(mongo_server_uri: str, database_name: str) -> None:
    """
    Drops mongo database
    :param mongo_server_uri: Mongo Server that requires Cleaning
    :param database_name: Name of db
    :return: None
    """
    client = MongoClient(mongo_server_uri)
    db = client.get_database(database_name)
    db.command("dropDatabase")


def clean_mongo_collections(mongo_server_uri: str, database_name: str, ignore_collections: List[str] | None = None) -> None:
    """
    Cleans all collections (deletes all documents) present in collections except ``ignore_collections``
    :param mongo_server_uri: Mongo Server that requires Cleaning
    :param database_name: Name of db
    :param ignore_collections: name of collections to be ignored from getting cleaned
    :return: None
    """
    for collection in db_collections(mongo_server_uri, database_name, ignore_collections):
        collection.delete_many({})


def get_version_from_mongodb_uri(mongo_server_uri: str) -> str:
    client = MongoClient(mongo_server_uri)
    return client.server_info().get("version")


def check_db_exist_from_mongodb_uri(mongo_server_uri: str, check_db_name: str) -> bool:
    client = MongoClient(mongo_server_uri)
    if check_db_name in client.list_database_names():
        return True
    return False


def get_mongo_db_list(mongo_server_uri: str) -> List[str]:
    client = MongoClient(mongo_server_uri)
    return client.list_database_names()


def get_immediate_prev_weekday(any_date: datetime = datetime.now()) -> datetime:
    """
    iso-weekday 1 == Monday ;; 7 == SUNDAY
    only skips standard weekends [Sat/Sun], holidays not accounted [ country specific holidays should be added on top
    separately by caller if desired ]
    """
    prev_day_offset: int = 1
    if any_date.isoweekday() == 1:
        prev_day_offset = 3
    elif any_date.isoweekday() == 7:
        prev_day_offset = 2
    any_date -= timedelta(days=prev_day_offset)
    return any_date


def get_immediate_next_weekday(any_date: datetime = datetime.now()) -> datetime:
    """
    iso-weekday 1 == Monday ;; 7 == SUNDAY
    only skips standard weekends [Sat/Sun], holidays not accounted [ country specific holidays should be added on top
    separately by caller if desired ]
    """
    next_day_offset: int = 1
    if any_date.isoweekday() == 5:
        next_day_offset = 3
    elif any_date.isoweekday() == 6:
        next_day_offset = 2
    any_date += timedelta(days=next_day_offset)
    return any_date


def year_month_day_str_from_datetime(any_date: datetime = datetime.now()) -> Tuple[str | None, str | None, str | None]:
    if any_date:
        year_str: str = str(any_date.year)
        month_str: str = str(any_date.month)
        if len(month_str) == 1:
            month_str = "0" + month_str
        day_str: str = str(any_date.day)
        if len(day_str) == 1:
            day_str = "0" + day_str
        return year_str, month_str, day_str
    else:
        return None, None, None


def get_time_it_log_pattern(callable_name: str, start_time: DateTime, delta: float):
    pattern_str = f"_timeit_{callable_name}~{start_time}~{delta}_timeit_"
    return pattern_str


# Decorator Function
def perf_benchmark(func_callable):
    @functools.wraps(func_callable)
    async def benchmarker(*args, **kwargs):
        call_date_time = DateTime.utcnow()
        start_time = timeit.default_timer()
        return_val = await func_callable(*args, **kwargs)
        end_time = timeit.default_timer()
        delta = parse_to_float(f"{(end_time - start_time):.6f}")

        pattern_str = get_time_it_log_pattern(func_callable.__name__, call_date_time, delta)
        logging.timing(pattern_str)
        return return_val
    return benchmarker


def parse_to_int(int_str: str | int | float) -> int:
    try:
        parsed_int = int(int_str)
        return parsed_int
    except ValueError as e:
        err_str = f"{type(int_str)} is not parsable to integer, exception: {e}"
        logging.exception(err_str)
        raise Exception(err_str)


def parse_to_float(float_str: str) -> float:
    try:
        parsed_float = float(float_str)
        return parsed_float
    except ValueError as e:
        err_str = f"{type(float_str)} is not parsable to float, exception: {e}"
        logging.exception(err_str)
        raise Exception(err_str)


def _connect_mongo(db: str, host: str | None = 'localhost', port: int | None = 27017,
                   username: str | None = None, password: str | None = None):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = f'mongodb://{username}:{password}@{host}:{port}/{db}'
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[db]


def read_mongo_collection_as_dataframe(db: str, collection: str, agg_pipeline: List | None = None,
                                       host: str | None = 'localhost', port: int | None = 27017,
                                       username: str | None = None, password: str | None = None,
                                       no_id: bool | None = True) -> pandas.DataFrame:
    """ Read from Mongo and Store into DataFrame """


    # Connect to MongoDB
    db = _connect_mongo(db=db, host=host, port=port, username=username, password=password)

    if agg_pipeline is None:
        agg_pipeline = []

    collection = db.get_collection(collection)

    # construct the DataFrame
    df: pandas.DataFrame = collection.aggregate_pandas_all(agg_pipeline)

    # Delete the _id
    if no_id and not df.empty:
        del df['_id']

    return df


def get_primary_native_host_n_port_from_config_dict(primary_config_dict: Dict,
                                                    project_data_dir_path: PurePath) -> Tuple[str, int]:
    primary_server_port = primary_config_dict.get("main_server_beanie_port")
    if primary_server_port is None:
        err_str = "Could not find 'main_server_beanie_port' key in project's primary config yaml"
        logging.exception(err_str)
        raise Exception(err_str)

    config_yaml_name = f"server_{primary_server_port}_config.yaml"
    config_yaml_path = project_data_dir_path / config_yaml_name
    if os.path.exists(config_yaml_path):
        config_dict = YAMLConfigurationManager.load_yaml_configurations(str(config_yaml_path))
        return get_native_host_n_port_from_config_dict(config_dict)
    else:
        err_str = f"server_{primary_server_port}_config.yaml doesn't exist"
        logging.exception(err_str)
        raise Exception(err_str)


def get_native_host_n_port_from_config_dict(config_dict: Dict) -> Tuple[str, int]:
    cache_override_type = config_dict.get("cache_override_type")

    if cache_override_type is not None and cache_override_type.lower() == "native":
        host, port = config_dict.get("cache_host"), parse_to_int(config_dict.get("cache_port"))
    else:
        host, port = config_dict.get("beanie_host"), parse_to_int(config_dict.get("beanie_port"))
    return host, port


async def execute_tasks_list_with_all_completed(tasks_list: List[asyncio.Task],
                                                pydantic_class_type: Type[DocType] | Type[BaseModel] | None = None,
                                                timeout: float = 20.0):
    pending_tasks: Set[asyncio.Task] | None = None
    completed_tasks: Set[asyncio.Task] | None = None
    if tasks_list:
        try:
            # wait doesn't raise TimeoutError! Futures that aren't done when timeout occurs are returned in 2nd set
            completed_tasks, pending_tasks = await asyncio.wait(tasks_list, return_when=asyncio.ALL_COMPLETED,
                                                                timeout=timeout)
        except Exception as e:
            logging.exception(f"await asyncio.wait raised exception: {e}")
    else:
        debug_str = "unexpected: Called execute_tasks_list_with_all_completed with empty tasks_list"
        if pydantic_class_type is not None:
            debug_str += f" for model: {pydantic_class_type.__name__}"
        logging.debug(debug_str)

    if not completed_tasks:
        if pending_tasks:
            logging.error("Unexpected: Received no completed task from return of asyncio.wait"
                          f"dropped PendingTasks: {[pending_task for pending_task in pending_tasks]}")
        else:
            logging.error("Unexpected: Received no completed or pending task from return of asyncio.wait "
                          f"in-spite sending tasks_list: {[task for task in tasks_list]}")
        return

    while completed_tasks:
        completed_task = None
        try:
            completed_task = completed_tasks.pop()
        except Exception as e:
            logging.exception(f"execute_tasks_list_with_all_completed failed for task name: {completed_task.get_name()}"
                              f", task: {completed_task} with exception: {e}")
    if pending_tasks:
        logging.error("Received timed out pending tasks from asyncio.wait, dropping them. "
                      f"PendingTasks: {[pending_task for pending_task in pending_tasks]}")


async def execute_tasks_list_with_first_completed(tasks_list: List[asyncio.Task],
                                                  pydantic_class_type: Type[DocType],
                                                  timeout: float = 20.0):
    pending_tasks: Set[asyncio.Task] = set(tasks_list)
    completed_tasks: Set[asyncio.Task] | None = None
    while len(pending_tasks):
        try:
            # wait doesn't raise TimeoutError! Futures that aren't done when timeout occurs are returned in 2nd set
            completed_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED,
                                                                timeout=timeout)
        except Exception as e:
            logging.exception(f"for model: {pydantic_class_type.__name__} await asyncio.wait raised exception: {e}")

        # completed_tasks will be set of tasks or empty set or None
        while completed_tasks:
            completed_task = None
            try:
                completed_task = completed_tasks.pop()
            except ConnectionClosedOK as e:
                logging.debug('\n', f"ConnectionClosedOK error in task with name: "
                                    f"{completed_task.get_name()};;; Exception: {e}")
            except ConnectionClosedError as e:
                logging.exception('\n', f"ConnectionClosedError error in task with name: "
                                        f"{completed_task.get_name()};;; Exception: {e}")
            except ConnectionClosed as e:
                logging.debug('\n', f"ConnectionClosed error in task with name: "
                                    f"{completed_task.get_name()};;; Exception: {e}")
            except Exception as e:
                logging.debug('\n', f"execute_tasks_list_with_first_completed failed for task "
                                    f"{completed_task.get_name()};;; Exception: {e}")


def get_symbol_side_key(symbol_side_tuple_list: List[Tuple[str, str]]) -> str:
    key_str = ",".join([f"symbol-side={symbol}-{side}" for symbol, side in symbol_side_tuple_list])
    return f"%%{key_str}%%"


def except_n_log_alert():
    def decorator_function(original_function):
        def wrapper_function(*args, **kwargs):
            result = None
            try:
                result = original_function(*args, **kwargs)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                alert_brief: str = f"exception: {e} while attempting {original_function.__name__}, " \
                                   f"date-time: {DateTime.now()}"
                alert_details: str = f"{exc_type}: file: {filename}, line: {exc_tb.tb_lineno}, args: {args}, " \
                                     f"kwargs: {kwargs}"
                logging.error(f"{alert_brief};;; {alert_details}")
            return result
        return wrapper_function
    return decorator_function


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def clear_semaphore(semaphore_obj: threading.Semaphore):
    while 1:
        if not semaphore_obj.acquire(blocking=False):
            break


async def handle_ws(ws: WebSocket, is_new_ws: bool):
    need_disconnect = False
    if is_new_ws:
        while True:
            json_data = await ws.receive()  # {"type": "websocket.disconnect", "code": exc.code}
            if json_data["type"] == "websocket.disconnect":
                need_disconnect = True
                break
            else:
                logging.error(
                    f"Unexpected! WS client send data to server (ignoring) where none is expected, data: {json_data}")
                continue
    # else not required - some other path has invoked the websocket.receive(), we can ignore
    return need_disconnect


def get_cpu_usage() -> None:
    cpu_usage = psutil.cpu_percent(interval=10)
    logging.info(f"cpu usage in percent: {cpu_usage}")
    if cpu_usage > 90:
        logging.error(f"cpu usage exceeded 90%. current cpu usage: {cpu_usage}")
    elif cpu_usage > 70:
        logging.warning(f"cpu usage exceeded 70%. current cpu usage: {cpu_usage}")


def get_ram_memory_usage() -> None:
    virtual_memory_details = psutil.virtual_memory()
    total_memory = virtual_memory_details[0]
    available_memory = virtual_memory_details[1]
    used_memory = ((total_memory - available_memory) / total_memory) * 100
    logging.info(f"ram memory usage in percent: {used_memory}")
    if used_memory > 90:
        logging.error(f"ram memory usage exceeded 90%. current ram usage: {used_memory}")
    elif used_memory > 70:
        logging.warning(f"ram memory usage exceeded 70%. current ram usage: {used_memory}")


def get_disk_usage(locations: List[str] | None = None) -> None:
    if locations is None:
        partitions = psutil.disk_partitions(True)
        locations = [part.mountpoint for part in partitions]
    for loc in locations:
        usage_stats = shutil.disk_usage(loc)
        total_memory = usage_stats[0]
        if math.isclose(total_memory, 0):
            logging.warning(f"disk total memory is 0 for mount location: {loc}")
            continue
        used_memory = (usage_stats[1] / total_memory) * 100
        logging.info(f"disk usage in percent for location {loc}: {used_memory}")
        if used_memory > 90:
            logging.error(f"disk usage exceeded 90% for location {loc}. current disk usage: {used_memory}")
        elif used_memory > 70:
            logging.warning(f"disk usage exceeded 70% for location {loc}. current disk usage: {used_memory}")


def get_pid_from_port(port: int):
    try:
        for conn in psutil.net_connections(kind='inet'):
            if conn.laddr.port == port:
                return conn.pid
    except Exception as e:
        print(f"Error: {e}")

    return None
