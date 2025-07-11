# Standard imports
import asyncio
import copy
import math
import os
import logging
import pickle
import re
import shlex
import sys
import threading
from typing import List, Dict, Tuple, Type, Set, Any, Iterable, Final, Optional, Callable, get_type_hints
import inspect
import socket
from contextlib import closing
import multiprocessing
from zoneinfo import ZoneInfo

import msgspec
import pandas
import pexpect
import yaml
import signal
import time
from enum import IntEnum
from pathlib import PurePath, Path
import csv
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError, ConnectionClosed
from requests import Response
from datetime import datetime, timedelta
import psutil
import shutil
import subprocess
from dataclasses import dataclass
import json
import getpass

# 3rd party packages
import pandas as pd
from pymongo import MongoClient
# import pymongoarrow.monkey
from beanie.odm.documents import DocType
from pendulum import DateTime
from fastapi import WebSocket
from fastapi import HTTPException
import orjson
import pendulum
import moviepy
import polars as pl

# FluxPythonUtils Modules
from FluxPythonUtils.scripts.model_base_utils import MsgspecBaseModel
from FluxPythonUtils.scripts.file_n_general_utility_functions import (
    LOG_FORMAT, find_acronyms_in_string, file_exist, YAMLConfigurationManager, load_yaml, is_file_updated,
    _get_file_handler)

# Adds extra utility methods of pymongoarrow.monkey to pymongo collection objects
# https://mongo-arrow.readthedocs.io/en/pymongoarrow-0.1.1/quickstart.html#extending-pymongo
# pymongoarrow.monkey.patch_all()   # not being used currently + causing issues in cpp so file usage in ctypes.CDLL


# Script containing all the utility: handy functions / classes / enums / decorators

class ClientError(Exception):
    """Exception raised for unexpected client error.
        Attributes:
            message -- brief explanation of the error
        """

    def __init__(self, message: str):
        super().__init__(message)


class ServiceUnavailable(Exception):
    """Exception raised to represent service unavailability.

    Attributes:
        file_or_dir_path -- file path that was found with empty file
        message -- brief explanation of the error (file path passed is auto-appended f-str below)
    """

    def __init__(self, file_path: str, message: str = "service unavailable!"):
        self.file_path = file_path
        self.message = message
        super().__init__(f"{self.message}, file_path: {self.file_path}")


class ThreadSafeAsyncLock:
    """
    # Critical section for asyncio coroutines
        async with thread_safe_async_lock:

    # Critical section for threads
        with thread_safe_async_lock:
    """
    def __init__(self):
        self._thread_lock = threading.Lock()
        self._async_lock = asyncio.Lock()

    async def __aenter__(self):
        await self._async_lock.acquire()
        self._thread_lock.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        self._thread_lock.release()
        self._async_lock.release()

    def __enter__(self):
        self._thread_lock.acquire()

    def __exit__(self, exc_type, exc, tb):
        self._thread_lock.release()


def get_years_remaining_in_float(end_date: datetime):
    """
    Calculate the remaining period to end_date in years

    Args:
        end_date (datetime): Obvious

    Returns:
        float: The remaining period to maturity in years
    """
    today = datetime.now(tz=ZoneInfo("Asia/Singapore"))

    # If the end date has already passed
    if end_date < today:
        return 0.0

    # Calculate the difference in days
    days_remaining = (end_date - today).days + ((end_date - today).seconds / 86400)

    # Convert days to years (approximately)
    years_remaining = days_remaining / 365.25  # Account for leap years

    return years_remaining


def log_n_except(original_function):
    def wrapper_function(*args, **kwargs):
        try:
            result = original_function(*args, **kwargs)
            return result
        except Exception as exception:
            orig_func_name: str
            if original_function:
                orig_func_name = original_function.__name__ if hasattr(original_function, "__name__") else \
                    f"original_function_dunder_name_attr_not_found: {original_function=}"
            else:
                orig_func_name = f"{original_function=}"
            err_str = f"Client Error Occurred in: {orig_func_name=};;; {exception=}, {args=}, {kwargs=}"
            # 1mb = 1048576 bytes
            # logging.exception(f"{err_str:.1048576}")
            # raise Exception(f"{err_str:.1048576}")
            logging.exception(f"{err_str:.1000}")
            raise ClientError(f"{err_str:.1000}")

    return wrapper_function


def empty_as_none(val: float | int | str | None):
    if val == "":
        val = None
    return val


def transform_to_str(val: float | int | str | None):
    if val is not None:
        val = str(val)
    return val


def str_to_datetime(val: str | None, datetime_format: str = "%Y-%m-%d %H:%M:%S%z"):
    if val is not None and isinstance(val, str):
        # convert from string format to datetime format
        return datetime.strptime(val, datetime_format)
    return val


def nan_inf_as_none(val: float | int | None):
    """
    useful for data coming from data frame sources where we may get Nan or INF numeric values
    """
    if val and math.isnan(val) or (not math.isfinite(val)):
        val = None
    return val


def nan_inf_0_as_none(val: float | int | None):
    """
    useful for data coming from data frame sources where we may get Nan or INF numeric values
    """
    if val and math.isnan(val) or (not math.isfinite(val)) or math.isclose(val, 0):
        val = None
    return val


def float_str(var: float | int | None, precision: int = 3):
    return f"{(float(var) if var else 0.0):.{precision}f}"


class HTTPRequestType(IntEnum):
    UNSPECIFIED = 0
    GET = 1
    POST = 2
    DELETE = 3
    PUT = 4
    PATCH = 5


def http_response_as_class_type(url, response, expected_status_code, msgspec_type: Type[MsgspecBaseModel],
                                http_request_type: HTTPRequestType):
    status_code, response_json = handle_http_response(response)
    if status_code == expected_status_code:
        if isinstance(response_json, bool):
            return response_json
        else:
            if isinstance(response_json, list):
                return [msgspec_type.from_dict(response_obj) for response_obj in response_json]
            else:
                return msgspec_type.from_dict(response_json)
    else:
        raise Exception(f"failed for url: {url}, http_request_type: {str(http_request_type)} "
                        f"http_error: {response_json}, status_code: {status_code}")


def http_response_as_df(url, response, expected_status_code, http_request_type: HTTPRequestType):
    status_code, response_json = handle_http_response(response)
    if status_code == expected_status_code:
        if isinstance(response_json, bool):
            return response_json
        else:
            if isinstance(response_json, list):
                return pl.DataFrame(response_json)
            else:
                raise Exception(f"Unsupported response type: {type(response_json)} for converting to polars dataframe")
    else:
        raise Exception(f"failed for url: {url}, http_request_type: {str(http_request_type)} "
                        f"http_error: {response_json}, status_code: {status_code}")


def http_response_as_json(url, response, expected_status_code, http_request_type: HTTPRequestType):
    status_code, response_json = handle_http_response(response)
    if status_code == expected_status_code:
        return response_json
    else:
        raise Exception(f"failed for url: {url}, http_request_type: {str(http_request_type)} "
                        f"http_error: {response_json}, status_code: {status_code}")


def handle_http_response(response: Response):
    if response is None:
        return '{"error: passed response is None - no http response to handle!"}'
    if response.ok:
        content = response.content
        if content == b'True' or content == b'False':
            return response.status_code, True
        else:
            return response.status_code, orjson.loads(response.content)
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
        return response.status_code, orjson.loads(response.content)
    if response.content is not None:
        try:
            content = orjson.loads(response.content)
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
        except orjson.JSONDecodeError:
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


def dec_hook(type: Type, obj: Any) -> Any:
    if type == DateTime and isinstance(obj, str):
        return pendulum.parse(obj)
    elif type == DateTime and isinstance(obj, DateTime):
        return obj
    elif type == DateTime and isinstance(obj, datetime):
        return pendulum.parse(str(obj))


def get_json_array_as_msgspec_dict(json_key: str, json_data_list, MsgspecType: Type[MsgspecBaseModel]
                                   ) -> Dict[str, msgspec.Struct]:
    msgspec_dict: Dict[str, msgspec.Struct] = dict()
    for json_data in json_data_list:
        msgspec_key = json_data[json_key]
        msgspec_dict[msgspec_key] = MsgspecType.from_kwargs(**json_data)
    return msgspec_dict


def scp_handler(scp_src_path: PurePath, scp_src_user: str, scp_src_server: str,
                scp_dest_dir: PurePath, scp_password: str) -> bool:
    """Handle SCP file transfer with password authentication.

    Args:
        scp_src_path: Source file path on remote server
        scp_src_user: Username for remote server
        scp_src_server: Remote server hostname/IP
        scp_dest_dir: Local destination directory
        scp_password: Password for authentication

    Returns:
        True if transfer successful, False otherwise
    """
    # Better parameter validation
    if not str(scp_src_path).strip():
        logging.error("SCP handler: source path is empty")
        return False
    if not str(scp_src_user).strip():
        logging.error("SCP handler: username is empty")
        return False
    if not str(scp_src_server).strip():
        logging.error("SCP handler: server is empty")
        return False
    if not str(scp_dest_dir).strip():
        logging.error("SCP handler: destination directory is empty")
        return False

    # Proper Path handling - convert PurePath to concrete Path safely
    try:
        dest_path = Path(str(scp_dest_dir))
    except Exception as e:
        logging.error(f"Invalid destination path {scp_dest_dir}: {e}")
        return False

    # Better directory creation with file vs directory check
    try:
        if dest_path.exists():
            if not dest_path.is_dir():
                logging.error(f"Destination exists but is not a directory: {dest_path}")
                return False
        else:
            dest_path.mkdir(parents=True, exist_ok=True)
            logging.info(f"Created destination directory: {dest_path}")
    except PermissionError as e:
        logging.error(f"Permission denied creating destination directory {dest_path}: {e}")
        return False
    except OSError as e:
        logging.error(f"Cannot create destination directory {dest_path}: {e}")
        return False

    # Quote only the path part, not the entire remote spec
    # Handle Windows path separators properly
    safe_src_path = shlex.quote(str(scp_src_path))
    remote_spec = f"{scp_src_user}@{scp_src_server}:{safe_src_path}"

    # Use forward slashes for destination (SCP expects POSIX-style paths)
    dest_with_dot = str(dest_path).replace('\\', '/') + "/."
    safe_dest = shlex.quote(dest_with_dot)

    scp_command = f"scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {remote_spec} {safe_dest}"

    # Log the actual command for debugging
    logging.info(f"Executing SCP command: {scp_command}")

    # Execute with improved error handling
    success = pexpect_command_expect_response_handler(scp_command, scp_password)

    if not success:
        logging.error(
            f"SCP command failed for {scp_src_user}@{scp_src_server}:{scp_src_path} -> {dest_path};;;"
            f"MANUAL COMMAND TO TRY: {scp_command} ;;;This could be due to: 1. Incorrect credentials "
            f"2. File not found on remote server 3. Network connectivity issues 4. Permission denied on remote server "
            f"5. SSH key conflicts")
        return False

    # More specific exception handling - don't mask real failures
    try:
        src_filename = Path(str(scp_src_path)).name
        if not src_filename:
            logging.warning("Cannot verify file transfer: source path appears to be a directory")
            return True

        expected_file = dest_path / src_filename
        if not expected_file.exists():
            logging.error(f"SCP reported success but file not found at: {expected_file}")
            return False

        # Check file size
        file_size = expected_file.stat().st_size
        logging.info(f"SCP transfer verified successful: {scp_src_path} -> {expected_file} ({file_size} bytes)")
        return True

    except FileNotFoundError:
        # This is a real failure - don't mask it
        logging.error("File verification failed: transferred file not found")
        return False
    except PermissionError as e:
        logging.warning(f"Cannot verify file transfer due to permissions: {e}")
        return True  # Assume success since SCP didn't fail
    except Exception as e:
        logging.warning(f"Unexpected error during file verification: {e}")
        return True  # Only for truly unexpected errors


def pexpect_command_expect_response_handler(command_: str, password_: str) -> bool:
    """Execute SCP command with pexpect and handle expected responses.

    Handles both password prompts and host key verification prompts.
    """
    process = None
    try:
        # Enable more verbose logging for debugging
        process = pexpect.spawn(command_, timeout=60)  # Increased timeout

        # Enable logging of all pexpect interactions for debugging
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            process.logfile = sys.stdout.buffer

        # More precise host key prompt regex
        host_key_prompt = r"Are you sure you want to continue connecting \(yes/no(/\[fingerprint\])?\)\?"
        password_prompt = r"[Pp]assword:"
        permission_denied = r"Permission denied"
        no_such_file = r"No such file or directory"

        # Add iteration limit to prevent infinite loops
        max_iterations = 10
        iteration_count = 0

        while iteration_count < max_iterations:
            iteration_count += 1

            index = process.expect([
                host_key_prompt,  # 0: Host key verification
                password_prompt,  # 1: Password prompt
                permission_denied,  # 2: Permission denied
                no_such_file,  # 3: File not found
                pexpect.EOF,  # 4: Command completed
                pexpect.TIMEOUT  # 5: Timeout
            ])

            if index == 0:  # Host key verification
                logging.info("Responding to host key verification prompt")
                process.sendline("yes")
                continue

            elif index == 1:  # Password prompt
                if not password_:
                    logging.error("Password prompt received but no password provided")
                    return False
                logging.info("Responding to password prompt")
                process.sendline(password_)
                continue

            elif index == 2:  # Permission denied
                logging.error("Permission denied - check credentials and file permissions")
                return False

            elif index == 3:  # File not found
                logging.error("File not found on remote server")
                return False

            elif index == 4:  # EOF - command completed
                break

            else:  # Timeout
                logging.error(f"Command timed out after {iteration_count} iterations")
                # Try to read any remaining output for debugging
                try:
                    remaining_output = process.read_nonblocking(size=1000, timeout=1)
                    logging.error(f"Remaining output: {remaining_output}")
                except:
                    pass
                return False

        if iteration_count >= max_iterations:
            logging.error(f"Too many prompt iterations ({max_iterations}), possible infinite loop")
            return False

        # Check both exit status and signal status
        process.close()

        if process.exitstatus is not None:
            if process.exitstatus == 0:
                logging.info("SCP command completed successfully")
                return True
            else:
                logging.error(f"Command failed with exit code: {process.exitstatus}")
                # Try to get more details about the error
                if hasattr(process, 'before') and process.before:
                    logging.error(f"Command output: {process.before}")
                return False
        elif process.signalstatus is not None:
            logging.error(f"Command terminated by signal: {process.signalstatus}")
            return False
        else:
            logging.error("Command completed but status is unknown")
            return False

    except pexpect.exceptions.TIMEOUT:
        logging.error(f"Command timed out: {command_}")
        return False
    except pexpect.exceptions.EOF:
        logging.error(f"Unexpected EOF from command: {command_}")
        if process and hasattr(process, 'before') and process.before:
            logging.error(f"Output before EOF: {process.before}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error executing command: {e}")
        return False
    finally:
        # Correct process cleanup for pexpect
        if process and process.isalive():
            try:
                process.close(force=True)
            except:
                try:
                    # Fallback: send SIGTERM signal directly
                    process.kill(signal.SIGTERM)
                except:
                    pass  # Best effort cleanup

    # Explicit return for any case that falls through
    return False


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
            # Validation all elements of updated list must be of list type
            for nested_updated_list in updated_list:
                if not isinstance(nested_updated_list, list):
                    err_str = (f"element from updated list is of different type than stored elements type: "
                               f"Must be list found one element of type {type(nested_updated_list)} - "
                               f"Ignoring this update call;;; mismatched element: {nested_updated_list}, "
                               f"update_list: {updated_list}")
                    logging.exception(err_str)
                    raise Exception(err_str)

            for nested_updated_list in updated_list:
                nested_stored_list = _find_matching_list(nested_updated_list, stored_list)
                if nested_stored_list is None:
                    # this underlying updated list is new - append to stored_list (stored list of list)
                    stored_list.append(nested_updated_list)
                else:
                    compare_n_patch_list(nested_stored_list, nested_updated_list)

        elif isinstance(stored_list[0], dict):
            # Validation all elements of updated list must be of dict type
            for update_dict in updated_list:
                if not isinstance(update_dict, dict):
                    err_str = (f"element from updated list is of different type than stored elements type: "
                               f"Must be dict found one element with type {type(update_dict)} - "
                               f"ignoring this update call;;; mismatched element: {update_dict}, "
                               f"update_list: {updated_list}")
                    logging.exception(err_str)
                    raise Exception(err_str)

            # If elements are of dict type then checking if id key is present in elements
            if stored_list[0].get("_id") is not None:
                stored_id_idx_dict: Dict = {stored_obj.get("_id"): idx for idx, stored_obj in enumerate(stored_list)}
                for index, update_dict in enumerate(updated_list):
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
                return stored_list
            # If no id key found in dicts, since no way to compare just append any new updates to main list
            else:
                stored_list.extend(updated_list)
                return stored_list
        else:  # non container type list are just extended [no deletes possible - use put if delete is your use case]
            stored_list.extend(updated_list)
            return stored_list
    else:
        if updated_list:
            update_dict_type: Type = type(updated_list[0])
            for update_item in updated_list:
                # Validating all elements of update list with taking first element as reference
                if not type(update_item) == update_dict_type:
                    err_str = (f"All updated_list's elements must be of same type, found mismatch in element types "
                               f"- ignoring this update call;;;{update_item=}")
                    logging.exception(err_str)
                    raise Exception(err_str)

                # checking no update dict element is found for delete since stored dict is completely empty
                if update_dict_type == dict:
                    if update_item.get("_id") is not None and len(update_item) == 1:
                        err_str = ("Unexpected: updated_list contains dict element with only id set but stored "
                                   f"list is empty - ignoring this update call;;; update_list: {updated_list}")
                        logging.exception(err_str)
                        raise Exception(err_str)

            stored_list.extend(updated_list)


def compare_n_patch_dict(stored_dict: Dict, updated_dict: Dict):
    for key, updated_value in updated_dict.items():
        stored_value = stored_dict.get(key)
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
            if collection.endswith(".files") or collection.endswith(".chunks"):
                yield db[collection]
            elif collection not in ignore_collections and "." not in collection:
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


def log_weekday_file_fetch_failure(failure_data_list: List[Tuple[PurePath, datetime, datetime, datetime]],
                                   err_prefix: str):
    """
    Args:
        failure_data_list: tuple; data_file_local_path, try_day, immediate_prev_weekday, next_immediate_prev_weekday
        err_prefix:
    Returns:

    """
    if not failure_data_list:
        return
    failure_data: Tuple[PurePath, datetime, datetime, datetime]
    for failure_data in failure_data_list:
        data_file_local_path, try_day, immediate_prev_weekday, next_immediate_prev_weekday = failure_data
        err_prefix += (f"{try_day} immediate_prev_weekday: {immediate_prev_weekday} data_file_path either not found"
                       f" or is 0 bytes: {data_file_local_path}, tried dwh server/local both; re-trying with next "
                       f"previous work day: {next_immediate_prev_weekday}")
    if err_prefix:
        logging.error(err_prefix)


def delete_mongo_document(mongo_server_uri: str, database_name: str,
                          collection_name: str, delete_filter: Dict) -> bool:
    client: MongoClient | None = None
    try:
        client = MongoClient(mongo_server_uri)
        db = client.get_database(name=database_name)
        collection = db.get_collection(name=collection_name)
        res = collection.delete_many(delete_filter)
        if res.deleted_count > 0:
            return True
        else:
            return False
    except Exception as e:
        err_str = (f"delete_mongo_document failed for {mongo_server_uri=}, {database_name=}, "
                   f"{collection_name=}, {delete_filter=};;;exception: {e}")
        logging.exception(err_str)
        raise e
    finally:
        if client is not None:
            client.close()


def create_mongo_document(mongo_server_uri: str, database_name: str,
                          collection_name: str, insert_json: Dict) -> bool:
    client: MongoClient | None = None
    try:
        client = MongoClient(mongo_server_uri)
        db = client.get_database(name=database_name)
        collection = db.get_collection(name=collection_name)
        res = collection.insert_one(insert_json)
        if res.inserted_id > 0:
            return True
        else:
            return False
    except Exception as e:
        err_str = (f"create_mongo_document failed for {mongo_server_uri=}, {database_name=}, "
                   f"{collection_name=}, {insert_json=};;;exception: {e}")
        logging.exception(err_str)
        raise e
    finally:
        if client is not None:
            client.close()


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


def parse_to_int(int_str: str | int | float, raise_exception: bool = True) -> int | None:
    try:
        parsed_int = int(int_str)
        return parsed_int
    except ValueError as e:
        err_str = f"{type(int_str)} is not parsable to integer, exception: {e}"
        logging.exception(err_str)
        if raise_exception:
            raise ValueError(err_str)
        return None


def parse_to_float(float_str: str) -> float:
    try:
        parsed_float = float(float_str)
        return parsed_float
    except ValueError as e:
        err_str = f"{type(float_str)} is not parsable to float, exception: {e}"
        logging.exception(err_str)
        raise ValueError(err_str)


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


async def execute_tasks_list_with_all_completed(
        tasks_list: List[asyncio.Task],
        model_class_type: Type[DocType] | Type[dataclass] | Type[MsgspecBaseModel] | None = None,
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
        if model_class_type is not None:
            debug_str += f" for model: {model_class_type.__name__}"
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
            _ = completed_task.result()  # triggers exceptions if any raised
        except Exception as e:
            logging.exception(f"execute_tasks_list_with_all_completed failed for {completed_task.get_name()=} with "
                              f"exception: {e};;;{completed_task=}")
    if pending_tasks:
        logging.error("Received timed out pending tasks from asyncio.wait, dropping them. "
                      f"PendingTasks: {[pending_task for pending_task in pending_tasks]}")


async def execute_tasks_list_with_first_completed(tasks_list: List[asyncio.Task],
                                                  model_class_type: Type[DocType],
                                                  timeout: float = 20.0):
    pending_tasks: Set[asyncio.Task] = set(tasks_list)
    completed_tasks: Set[asyncio.Task] | None = None
    while len(pending_tasks):
        try:
            # wait doesn't raise TimeoutError! Futures that aren't done when timeout occurs are returned in 2nd set
            completed_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED,
                                                                timeout=timeout)
        except Exception as e:
            logging.exception(f"for model: {model_class_type.__name__} await asyncio.wait raised exception: {e}")

        # completed_tasks will be set of tasks or empty set or None
        while completed_tasks:
            completed_task = None
            try:
                completed_task = completed_tasks.pop()
                _ = completed_task.result()  # triggers exceptions if any raised
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


def get_symbol_side_pattern():
    return "%%"


def get_launcher_name():
    component_path: str = sys.argv[0]
    # rpartition splits from the right-hand-side of the string
    component_file_name: str = (component_path.rpartition("/"))[-1]
    return (component_file_name.split('.'))[0]


def extract_core_config_data(config_dict: Dict, inst_id_prefix: str) -> Tuple[bool, str, str]:
    component_name = get_launcher_name()

    env_type: str | None = config_dict.get("env_type")
    is_uat: bool | None = True if env_type and env_type.lower() == "uat" else False

    trading_link_int_id: int | None = config_dict.get("trading_link_int_id")
    if "basket" not in component_name.lower():
        if is_uat:  # strat executor found UAT mode
            inst_id: str | None = f"{inst_id_prefix}-UAT-{trading_link_int_id}"  # sample: SIM-UAT-1
        else:  # strat executor found prod mode
            inst_id: str | None = f"{inst_id_prefix}-{trading_link_int_id}"  # sample: SIM-1
    else:
        if is_uat:  # basket executor found UAT mode
            inst_id: str | None = f"{inst_id_prefix}B-UAT-{trading_link_int_id}"  # sample: SIMB-UAT-1
        else:  # basket executor found prod mode
            inst_id: str | None = f"{inst_id_prefix}B-{trading_link_int_id}"  # sample: SIMB-1

    inst_id: str = inst_id if inst_id else "NO-INST-ID"  # overwritten by Trading Link with strat ID

    return is_uat, inst_id, component_name


async def submit_task_with_first_completed_wait(tasks_list: List[asyncio.Task],
                                                timeout: float = 60.0):
    res_list = []
    completed_tasks: Set[asyncio.Task] | None = None
    pending_tasks: Set[asyncio.Task] | None = None
    while True:
        try:
            completed_tasks, pending_tasks = \
                await asyncio.wait(tasks_list, return_when=asyncio.FIRST_COMPLETED, timeout=timeout)
        except Exception as e:
            logging.exception(f"submit_task_with_first_completed_wait asyncio.wait failed with exception: {e}")
        while completed_tasks:
            completed_task = None
            try:
                completed_task = completed_tasks.pop()
                res = completed_task.result()
                if res is not None:
                    res_list.append(res)
            except Exception as e:
                logging.exception(f"task failed for task_name: {completed_task.get_name()};;; exception: {e}")
        if pending_tasks:
            logging.info(f"{len(tasks_list)=}, {len(pending_tasks)=};;; {[task.get_name() for task in pending_tasks]}")
            tasks_list = [*pending_tasks, ]
        else:
            break
    return res_list


def except_n_log_alert():
    def decorator_function(original_function):
        def wrapper_function(*args, **kwargs):
            result = None
            try:
                result = original_function(*args, **kwargs)
            except Exception as e:
                logging.exception(f"exception caught in decorator_function@wrapper_function: {e}")
            return result
        return wrapper_function
    return decorator_function


def find_free_port() -> int:
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
                if conn.pid:
                    return conn.pid
        else:
            logging.error(f"Can't find pid with {port=}")
    except Exception as e:
        logging.error(f"get_pid_from_port failed, exception: {e}")

    return None

def find_pids_by_command(command_substring, ignore_case=False):
    """
    Finds process IDs (PIDs) of running processes whose command line
    contains the given substring.

    Args:
        command_substring (str): The substring to search for in the process
                                 command lines.
        ignore_case (bool): Whether the search should be case-insensitive.
                            Defaults to False.

    Returns:
        list[int]: A list of PIDs for matching processes. Returns an empty
                   list if no matches are found or if the substring is empty.
    """
    if not command_substring:
        print("Warning: Empty command substring provided.")
        return []

    matching_pids = []
    search_term = command_substring.lower() if ignore_case else command_substring

    # Iterate over all running processes
    for process in psutil.process_iter(['pid', 'cmdline']):
        try:
            # Get the full command line arguments for the process
            # cmdline() returns a list of strings (e.g., ['python', 'my_script.py', '-a'])
            cmdline_list = process.info.get('cmdline')

            if cmdline_list:
                # Join the list into a single string for easier searching
                # Using shlex.join might be more precise if dealing with complex quoting,
                # but simple space join is usually sufficient for searching.
                full_cmdline = ' '.join(cmdline_list)

                # Perform the search (case-insensitive or sensitive)
                cmd_to_check = full_cmdline.lower() if ignore_case else full_cmdline
                if search_term in cmd_to_check:
                    matching_pids.append(process.info['pid'])

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # Process might have terminated, or we lack permissions
            continue
        except Exception as e:
            # Catch other potential errors during process info retrieval
            print(f"Warning: Could not inspect process {process.pid}: {e}")
            continue
    return matching_pids

# @@@ deprecated: not recommended for first choice - only use if really required
# def is_process_running(pid: int) -> bool:
#     try:
#         process = psutil.Process(pid)
#         try:
#             # wait is required to retrieve the exit status else killed/terminated background subprocess
#             # remains defunct and is_running() returns True - raises TimeoutExpired if no status found
#             # within timout period, meaning process is running
#             # BONUS info: we can use process.poll() without also instead of wait if we had
#             # subprocess.popen object to check its status - process.poll() returns None till process
#             # is running and once it is completed it returns exit code of process
#             process.wait(1)
#         except psutil.TimeoutExpired:
#             return True
#         return process.is_running()
#     except psutil.NoSuchProcess:
#         return False


def is_process_running(pid: int | None) -> bool:
    if pid is None:
        return False
    if psutil.pid_exists(pid):
        try:
            process = psutil.Process(pid)
            # defunct process
            if process.status() == psutil.STATUS_ZOMBIE:
                return False
            return True
        except psutil.NoSuchProcess:
            return False
    return False


def terminate_process(pid):
    try:
        # First try sending SIGTERM (graceful termination)
        os.kill(pid, signal.SIGTERM)

        # Give the process some time to terminate
        for _ in range(10):  # Wait up to 1 second
            time.sleep(0.1)
            try:
                # Check if process still exists
                os.kill(pid, 0)
            except OSError:
                # Process has terminated
                logging.info(f"Process {pid} terminated successfully")
                return True

        # If we get here, process didn't terminate with SIGTERM
        logging.info(f"Process {pid} didn't terminate with SIGTERM, trying SIGKILL")
        os.kill(pid, signal.SIGKILL)
        return True

    except OSError as e:
        logging.exception(f"Error terminating process {pid}: {e}")
        return False


def re_pattern_to_grep(pattern: str) -> str:
    # Escape characters that have special meaning in grep
    pattern = pattern.replace("+", "\\+")
    pattern = pattern.replace("?", "\\?")
    pattern = pattern.replace("|", "\\|")
    pattern = pattern.replace("(", "\\(")
    pattern = pattern.replace(")", "\\)")
    pattern = pattern.replace("{", "\\{")
    pattern = pattern.replace("}", "\\}")

    # Convert python regex syntax to grep syntax
    pattern = pattern.replace("\\d", "[0-9]")
    pattern = pattern.replace("\\D", "[^0-9]")
    pattern = pattern.replace("\\s", "[[:space:]]")
    pattern = pattern.replace("\\S", "[^[:space:]]")
    pattern = pattern.replace("\\w", "[a-zA-Z0-9_]")
    pattern = pattern.replace("\\W", "[^a-zA-Z0-9_]")

    return pattern


def convert_pattern_for_awk_match(pattern_str: str) -> str:
    regex_pattern = r'\\d\{(\d+)\}'
    matches = re.findall(regex_pattern, pattern_str)
    for match in matches:
        rep_num = parse_to_int(match, raise_exception=False)
        if rep_num is not None:
            matched_pattern = f'\\d{{{match}}}'
            pattern_str = pattern_str.replace(matched_pattern, "[0-9]"*rep_num)
        else:
            err_str = f"match found from pattern is not int, found {match} from {pattern_str=}"
            raise Exception(err_str)

    # handling remaining num patterns
    pattern_str = pattern_str.replace(r"\d", "[0-9]")

    return pattern_str


def _get_log_line_no_from_timestamp_grep_cmd(log_file_path: str, timestamp_pattern_list: List[str], timestamp: str):
    grep_cmd_list: List[str] = [f"grep -n '^{timestamp_pattern}' {log_file_path}" for timestamp_pattern
                                in timestamp_pattern_list]
    cmd: str = f"({' || '.join(grep_cmd_list)}) | head -n 1"
    logging.debug(f"fetching line no for {timestamp=}, {cmd=}")
    process: subprocess.Popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                                 text=True)
    output, error = process.communicate()

    if error:
        logging.error(f"get_log_line_no_from_timestamp failed, {error=}")
        return None
    if output:
        closest_line_no: str = output.split(":")[0]
        return closest_line_no
    # else return None in all other case
    return None


def get_log_line_no_from_timestamp(log_file_path: str, timestamp: str) -> str | None:
    if not os.path.exists(log_file_path):
        return None

    # if exact datetime is found - taking next line from it else taking best match
    grep_timestamp_pattern: str = timestamp
    timestamp_pattern_list: List[str] = [grep_timestamp_pattern]

    exact_line_num = _get_log_line_no_from_timestamp_grep_cmd(log_file_path, timestamp_pattern_list, timestamp)
    if exact_line_num is not None:
        return f"+{parse_to_int(exact_line_num)+1}"
    # else not required: if exact line is not found - finding the best match

    timestamp_pattern_list: List[str] = []
    while True:
        # generate patterns for time only
        last_timestamp_char: str = grep_timestamp_pattern[-1]
        if last_timestamp_char == " " or last_timestamp_char == "-":
            break
        if last_timestamp_char == ":" or last_timestamp_char == ",":
            grep_timestamp_pattern = grep_timestamp_pattern[:-2]
        else:
            grep_timestamp_pattern = grep_timestamp_pattern[:-1]
        timestamp_pattern_list.append(grep_timestamp_pattern)

    # returning best match for timestamp - returns None if no match found
    closest_line_no = _get_log_line_no_from_timestamp_grep_cmd(log_file_path, timestamp_pattern_list, timestamp)
    if closest_line_no is not None:
        return f"+{closest_line_no}"
    # else return None in all other case
    return None


def get_last_log_line_date_time(log_file_path: str) -> str | None:
    if not os.path.exists(log_file_path):
        return None
    cmd = f"tail -n 1 {log_file_path} | awk " + "'{print $1, $2}'"
    out = subprocess.check_output(cmd, shell=True)
    line_no = out.decode("utf-8")
    if not line_no:
        return None
    return line_no.strip()


def run_gbd_terminal_with_pid(pid: int, show_msg: str | None = None):
    gdb_script: PurePath = PurePath(__file__).parent.parent / "data" / "init.gdb"
    # Define the commands you want to run
    if show_msg is None:
        show_msg = f"Terminal for PID: {pid}"
        # gdb_script_path: str = ""
    commands = [
        f"echo '{show_msg}'",
        f"gdb -p {pid} -x {str(gdb_script)}"
    ]

    # Open a single terminal window and run the commands
    terminal_command = " && ".join(commands) + " && bash"
    subprocess.Popen(["gnome-terminal", "--", "bash", "-c", terminal_command])


def submitted_task_result(future):
    try:
        # block for task to finish
        return future.result()
    except HTTPException as http_e:
        err_str_ = f"_task_result failed with http_exception: {http_e.detail}"
        logging.error(err_str_)
        raise Exception(err_str_)
    except Exception as e:
        err_str_ = f"_task_result failed with exception: {e}"
        logging.error(err_str_)
        raise Exception(err_str_)


def handle_refresh_configurable_data_members(
        callback_override_obj, config_key_to_data_member_name_dict: Dict[str, str],
        config_dict_path: str):
    config_dict_yaml_ = load_yaml(config_dict_path)
    for config_key, data_member_name in config_key_to_data_member_name_dict.items():
        updated_value = config_dict_yaml_.get(config_key)
        if updated_value is not None:
            setattr(callback_override_obj, data_member_name, updated_value)
            logging.debug(f"Updated {data_member_name=} with new value {updated_value=}")
        else:
            logging.error(f"Can't find key {config_key=!r} in updated snapshot of config yaml - "
                          f"ignoring this update")


def getmtime_from_linux_cmd(file_path: str) -> float | None:
    if not file_exist(file_path):
        return None

    cmd: str = f"stat -c %Y {file_path}"
    process: subprocess.Popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                                 text=True)
    output, error = process.communicate()

    if error:
        logging.error(f"getmtime_from_linux_cmd failed, {error=}")
        return None
    if output:
        modified_time_str: str = output.strip()
        modified_time: float = parse_to_float(modified_time_str)
        return modified_time
    # else not needed - return None in all other case
    return None


def is_file_modified(file_path: str, last_modified_time: float | None = None) -> Tuple[bool, float | None]:
    modified_status_from_python_os: bool = False
    modified_status_from_linux_cmd: bool = False
    # ignore milliseconds comparison
    if last_modified_time:
        last_modified_time = float(int(last_modified_time))

    if not file_exist(file_path):
        # file is removed, return modified status as True
        if last_modified_time:
            return True, None
        # no last_modified_time implies file did not exist previously
        # return modified status as False
        return False, None

    # file exists
    modified_time_from_linux_cmd: float | None = getmtime_from_linux_cmd(file_path)
    if modified_time_from_linux_cmd != last_modified_time:
        modified_status_from_linux_cmd = True
    modified_time_from_python_os: float | None = os.path.getmtime(file_path)
    modified_time_from_python_os_without_ms: float = float(int(modified_time_from_python_os))
    if modified_time_from_python_os_without_ms != last_modified_time:
        modified_status_from_python_os = True
    # verify consistency of python os module vs linux cmd
    if modified_status_from_linux_cmd != modified_status_from_python_os:
        logging.error(f"Incorrect file modified status found for {os.path.basename(file_path)=};;;"
                      f"{modified_status_from_python_os=}, {modified_status_from_linux_cmd=}, "
                      f"{modified_time_from_python_os=}, {modified_time_from_linux_cmd=}, {file_path=}")
        return True, modified_time_from_python_os
    # modified status is consistent for python os and linux cmd
    return modified_status_from_python_os, modified_time_from_python_os


def set_package_logger_level(package_name: str, level: int):
    if not package_name:
        err_str_ = (f"set_package_logger_level failed, {package_name=} found None. setting logging level on "
                    f"root logger is not supported")
        logging.error(err_str_)
        return
    logging.debug(f"setting logger level for {package_name=} to {level=}")
    logging.getLogger(package_name).setLevel(level)


def non_jsonable_types_handler(obj):
    """
    Converts datetime type to iso formated string while serializing to json str/bytes
    For any other type - converts to str
    :param obj:
    :return:
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return str(obj)


def convert_pendulum_to_datetime(pendulum_dt_obj: DateTime):
    datetime_obj = datetime(pendulum_dt_obj.year, pendulum_dt_obj.month, pendulum_dt_obj.day,
                            pendulum_dt_obj.hour, pendulum_dt_obj.minute, pendulum_dt_obj.second,
                            pendulum_dt_obj.microsecond, pendulum_dt_obj.tzinfo)
    return datetime_obj


def get_decimal_places(value: int | float) -> int:
    value_str = str(value)
    if "." in value_str:
        return len(value_str.split(".")[1])
    return 0  # no decimal point


def convert_video_to_audio(video_file_path: str | PurePath, audio_file_path: str | PurePath):
    """
    Converts video file to audio file
    :param video_file_path: video file path to be converted to audio
    :param audio_file_path: generated audio file path
    :return:
    """
    video = moviepy.VideoFileClip(video_file_path)
    audio = video.audio
    audio.write_audiofile(audio_file_path)


if __name__ == "__main__":
    def main():
        print(f"pre-create: \n{os.listdir()}")
        file_path = Path("test.py")
        file_path.touch()
        print(f"post-create: \n{os.listdir()}")
        update_time = is_file_updated(file_path)
        file_path.touch()  # update time stamp is newer now
        new_update_time = is_file_updated(file_path)
        assert(new_update_time > update_time)
        file_path.unlink()
        print(f"post-delete: \n{os.listdir()}")
        print("Done")

    main()


def get_transaction_counts_n_timeout_from_config(config_yaml_dict: Dict | None,
                                                 default_transaction_counts: int = 1,
                                                 default_transaction_timeout_secs: int = 2,
                                                 is_server_config: bool = True):
    if not config_yaml_dict:
        transaction_counts_per_call = default_transaction_counts
        transaction_timeout_secs = default_transaction_timeout_secs
    else:
        if is_server_config:
            if (transaction_counts_per_call := config_yaml_dict.get("transaction_counts_per_call_for_server")) is None:
                transaction_counts_per_call = default_transaction_counts
        else:
            if (transaction_counts_per_call := config_yaml_dict.get("transaction_counts_per_call_for_tail_ex")) is None:
                transaction_counts_per_call = default_transaction_counts
        if (transaction_timeout_secs := config_yaml_dict.get("transaction_timeout_secs")) is None:
            transaction_timeout_secs = default_transaction_timeout_secs
    return transaction_counts_per_call, transaction_timeout_secs


def is_first_param_list_type(func: Callable):
    # Get signature information
    signature = inspect.signature(func)

    # Get list of parameters
    params = list(signature.parameters.values())

    # Check if there's at least one parameter
    if not params:
        return False

    # Get the first parameter
    first_param = params[0]

    # Try to get type hint from function annotations
    type_hints = get_type_hints(func)
    first_param_name = first_param.name

    # Check if the first parameter is annotated with list type
    if first_param_name in type_hints:
        param_type = type_hints[first_param_name]
        # Check for various list type annotations (List, list, etc.)
        return (param_type == list or
                getattr(param_type, "__origin__", None) == list or
                getattr(param_type, "__origin__", None) == List)
    elif first_param.annotation is not inspect.Parameter.empty:
        param_type = first_param.annotation
        return (param_type == list or
                getattr(param_type, "__origin__", None) == list or
                getattr(param_type, "__origin__", None) == List)
    return False


def find_files_with_regex(directory, pattern):
    regex = re.compile(pattern)
    matching_files = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            if regex.match(file):
                matching_files.append(str(os.path.join(root, file)))
    return matching_files


def get_all_subclasses(cls):
    """
    A helper to find all descendants of a class
    """
    all_subclasses = []
    for subclass in cls.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))
    return all_subclasses
