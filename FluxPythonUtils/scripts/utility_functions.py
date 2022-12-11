import os
import pexpect as px
import logging
import re
from typing import List, Optional
import yaml
# FluxPythonUtils Modules
from FluxPythonUtils.scripts.yaml_importer import YAMLImporter

# Script containing all the utility and handy functions


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
    # chk_cmd.logfile = sys.stdout.buffer        # Just showing output of cmd when executed, so commented after debugging
    chk_cmd.timeout = None
    chk_cmd.expect(px.EOF)
    cmd_output = chk_cmd.before.decode("utf-8").splitlines()
    if "N: Unable to locate package" in cmd_output[0] or "Installed: (none)" in cmd_output[1]:
        return False
    return True


def configure_logger(level: str, log_file_path: str) -> None:
    """
    Function to config the logger in your trigger script of your project, creates log file in given log_dir_path.
    Takes project_name as parameter to fetch Level from configurations.py.
    """
    if ".log" not in (log_file_name := log_file_path.split(os.sep)[-1]):
        makedir(log_file_path)
        log_file_path: str = os.path.join(log_file_path, "logs.log")
    else:
        # Creating directory if not already exist (removing file name from path below)
        makedir(f"{os.sep}".join(log_file_path.split(os.sep)[:-1]))
        log_file_path: str = os.path.join(log_file_path, log_file_name)

    if level is not None:
        """
        CRITICAL	50
        ERROR	    40
        WARNING	    30
        INFO	    20
        DEBUG	    10
        """
        if level.lower() == "debug":
            level = logging.DEBUG
        elif level.lower() == "info":
            level = logging.INFO
        elif level.lower() == "warning":
            level = logging.WARNING
        elif level.lower() == "error":
            level = logging.ERROR
        elif level.lower() == "critical":
            level = logging.CRITICAL
        else:
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
