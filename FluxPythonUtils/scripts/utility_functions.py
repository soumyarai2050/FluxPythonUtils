import os
import logging
import re
from typing import List, Dict, Type
import yaml
from enum import IntEnum
import json
from pathlib import PurePath
import csv
from requests import Response

# other packages
from pydantic import BaseModel
import pandas as pd

# FluxPythonUtils Modules
from FluxPythonUtils.scripts.yaml_importer import YAMLImporter


# Script containing all the utility: handy functions / classes / enums /decorators


def log_n_except(original_function):
    def wrapper_function(*args, **kwargs):
        try:
            result = original_function(*args, **kwargs)
            return result
        except Exception as e:
            err_str = f"Client Error Occurred in function: {original_function.__name__}, args: {args}, " \
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


def http_response_as_class_type(url, response, expected_status_code, pydantic_type: Type,
                                http_request_type: HTTPRequestType):
    status_code, response_json = handle_http_response(response)
    if status_code == expected_status_code:
        return pydantic_type(**response_json)
    else:
        raise Exception(f"failed for url: {url}, http_+request_type: {str(http_request_type)} "
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
                    content ['error'] = content['error'] + " message- " + content ['message']
                return response.status_code, content['error']
            if 'messages' in content:
                return response. status_code, content['messages']
            if 'detail' in content:
                return response.status_code, content['detail']
        except json.JSONDecodeError as e:
            # handle as error
            if response.reason is not None:
                content = response. reason
                if response.text is not None:
                    content += (" text: " + response.text)
                return response.status_code, content
            elif response.text is not None:
                return response.status_code, response.text
            else:
                return response.status_code, None
    return response.status_code, None


def dict_or_list_records_csv_writer(file_name: str, records: Dict | List, fieldnames, record_type,
                                    data_dir: PurePath | None = None):
    """
    fieldnames can be subset of flelds you wish to write in esy constraints:
    records can be collection of Dict or List
    specific record MUST support dict() method
    """
    if data_dir is None:
        data_dir = PurePath(__file__).parent / "data"
    csv_path = PurePath(data_dir / f"{file_name}.csv")
    with open(csv_path, "w", encoding="utf-8", newline='') as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        if isinstance(records, Dict):
            for record in records.values():
                writer.writerow(json.loads(record_type(**record.dict()).json()))
        elif isinstance(records, List):
            for record in records:
                writer.writerow(json.loads(record_type(**record.dict()).json()))
        else:
            raise Exception(
                f"Unexpected: Un-supported type passed, expected Dict or List found type: {type(records)} of "
                f"object: {str(records)}")


def dict_or_list_records_csv_reader(file_name: str, PydanticType: Type[BaseModel], data_dir: PurePath | None = None) \
        -> List[BaseModel]:
    """
    At this time the method only supports list of pydantic_type extraction form csv
    """

    class PydanticClassTypeList(BaseModel):
        __root__: List[PydanticType]

    if data_dir is None:
        data_dir = PurePath(__file__).parent / "data"
    csv_path = PurePath(data_dir / f"{file_name}.csv")
    read_df = pd.read_csv(csv_path, keep_default_na=False)
    data_dict_list = read_df.to_dict(orient='records')
    record_dict = {"__root__": data_dict_list}
    pydantic_obj_list: PydanticClassTypeList = PydanticClassTypeList(**record_dict)
    return pydantic_obj_list.__root__


def str_from_file(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8", newline='') as fp:
        return fp.read()


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
            if data.startswith(acronym):
                data = data.replace(acronym, acronym[:-1].lower() + acronym[-1])
            elif data.endswith(acronym):
                data = data.replace(acronym, "_" + acronym.lower())
            else:
                data = data.replace(acronym, "_" + acronym[:-1].lower() + acronym[-1])
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
