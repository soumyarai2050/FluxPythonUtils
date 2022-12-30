import os
import pexpect as px
import logging
import re
from typing import List, Type
import yaml
from enum import IntEnum
import json
from requests import Response, post
# FluxPythonUtils Modules
from FluxPythonUtils.scripts.yaml_importer import YAMLImporter

# Script containing all the utility: handy functions / classes / decorators


def log_n_except(original_function):
    def wrapper_function(*args, **kwargs):
        try:
            result = original_function(*args, **kwargs)
            return result
        except Exception as e:
            err_str = f"Client Error Occurred in function: {original_function.__name__}, args: {args}, kwargs: {kwargs}, exception: {e}"
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


def http_response_as_class_type(url, response, expected_status_code, pydantic_type: Type, http_request_type: HTTPRequestType):
    status_code, response_json = handle_http_response(response)
    if status_code == expected_status_code:
        return pydantic_type(**response_json)
    else:
        raise Exception(f"failed for url: {url}, http_+request_type: {str(http_request_type)} http_error: {response_json}, status_code: {status_code}")


def handle_http_response(response: Response):
    if response is None:
        return '{"error: passed response is None - no http response to handle!"}'
    if response.ok:
        return response.status_code, json.loads(response.content)
    if response.content is not None:
        try:
            content = json.loads(response.content)
        except json.JSONDecodeError as e:
            if response.reason is not None:
                content = response.reason
                if response.text is not None:
                    content += (" text: " + response.text)
                return response.status_code, content
            elif response.text is not None:
                return response.status_code, response.text
            else:
                return response.status_code, None


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


def package_install_checker(package_name: str) -> bool:
    """
    Function to check if package is installed, returns True if installed and False otherwise.
    """
    chk_cmd = px.spawn(f"apt-cache policy {package_name}")
    # chk_cmd.logfile = sys.stdout.buffer  # Just shows output of cmd when executed, not-needed (for debug)
    chk_cmd.timeout = None
    chk_cmd.expect(px.EOF)
    cmd_output = chk_cmd.before.decode("utf-8").splitlines()
    if "N: Unable to locate package" in cmd_output[0] or "Installed: (none)" in cmd_output[1]:
        return False
    return True


def configure_logger(level: str, log_file_dir_path: str | None = None, log_file_name: str | None = None) -> None:
    """
    Function to config the logger in your trigger script of your project, creates log file in given log_dir_path.
    Takes project_name as parameter to fetch Level from configurations.py.
    """
    if log_file_name is None:
        log_file_name = "logs.log"
    # else not required: if file exists then using that name

    if log_file_dir_path is not None:
        os.makedirs(log_file_dir_path, exist_ok=True)
        log_file_path: str = os.path.join(log_file_dir_path, log_file_name)
    else:
        log_file_path: str = log_file_name

    with open(log_file_path, "w+") as fl:
        pass

    if level is not None:
        """
        CRITICAL	50
        ERROR	    40
        WARNING	    30
        INFO	    20
        DEBUG	    10
        """
        match level:
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
                error_msg: str = f"Unsupported logging level: {level}"
                raise Exception(error_msg)
    else:
        error_msg: str = f"logger level cant be none"
        raise Exception(error_msg)

    logging.basicConfig(
        filename=log_file_path,
        level=level,
        format="%(asctime)s : %(levelname)s : [%(filename)s : %(lineno)d] : %(message)s"
    )


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


def yaml_loader(file_path: str):
    with open(file_path) as f:
        data = yaml.load(f, Loader=YAMLImporter)
    return data


def load_yaml_configurations(config_file_path: str | None = None,
                             default_config_file_path: str | None = "configurations.yaml"):
    if config_file_path is None:
        if file_exist(default_config_file_path):
            return yaml_loader(default_config_file_path)
        else:
            err_str = f"No {default_config_file_path} exists in this script's directory. " \
                      f"Either make one or pass another file's path as parameter"
            logging.exception(err_str)
            raise Exception(err_str)
    else:
        if file_exist(config_file_path):
            return yaml_loader(config_file_path)
        else:
            err_str = f"No file: {config_file_path} exist"
            logging.exception(err_str)
            raise Exception(err_str)
