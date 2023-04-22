import os
import logging
import pickle
import re
from typing import List, Dict, TypeVar, Callable, Tuple
import yaml
from enum import IntEnum
import json
from pathlib import PurePath, Path
import csv
from requests import Response
from datetime import datetime

# other packages
from pydantic import BaseModel
import pandas as pd
from pymongo import MongoClient

# FluxPythonUtils Modules
from FluxPythonUtils.scripts.yaml_importer import YAMLImporter

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
            err_str = f"Client Error Occurred in function: {original_function.__name__}, args: {args}, " \
                      f"kwargs: {kwargs};;;exception: {e}"
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


def dict_or_list_records_csv_reader(file_name: str, PydanticType: BaseModelOrItsDerivedType,
                                    data_dir: PurePath | None = None) \
        -> List[BaseModel]:
    """
    At this time the method only supports list of pydantic_type extraction form csv
    """

    class PydanticClassTypeList(BaseModel):
        __root__: List[PydanticType]

    if data_dir is None:
        data_dir = PurePath(__file__).parent / "data"
    csv_path = PurePath(data_dir / f"{file_name}.csv")
    if os.path.getsize(str(csv_path)) > 0:
        read_df = pd.read_csv(csv_path, keep_default_na=False)
        data_dict_list = read_df.to_dict(orient='records')
        record_dict = {"__root__": data_dict_list}
        pydantic_obj_list: PydanticClassTypeList = PydanticClassTypeList(**record_dict)
        return pydantic_obj_list.__root__
    else:
        raise Exception(f"dict_or_list_records_csv_reader invoked on empty csv file: {str(csv_path)}")


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

    if file_exist(log_file_path):
        datetime_str: str = datetime.now().strftime("%Y%m%d.%H%M%S")
        os.rename(log_file_path, f"{log_file_path}.{datetime_str}")

    with open(log_file_path, "w+"):
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
                error_msg: str = f"Unsupported logging level: {other}"
                raise Exception(error_msg)
    else:
        error_msg: str = f"logger level cant be none"
        raise Exception(error_msg)

    logging.basicConfig(
        filename=log_file_path,
        level=level,
        format="%(asctime)s : %(levelname)s : [%(filename)s : %(lineno)d] : %(message)s",
        force=True
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
                             default_config_file_path: str | None = "configurations.yaml",
                             load_as_str: bool = False) -> Dict | str:
    if config_file_path is None:
        if file_exist(default_config_file_path):
            if not load_as_str:
                return yaml_loader(default_config_file_path)
            else:
                with open(default_config_file_path) as f:
                    return f.read()
        else:
            err_str = f"No {default_config_file_path} exists in this script's directory. " \
                      f"Either make one or pass another file's path as parameter;;;"
            logging.exception(err_str)
            raise Exception(err_str)
    else:
        if file_exist(config_file_path):
            if not load_as_str:
                return yaml_loader(config_file_path)
            else:
                with open(config_file_path) as f:
                    return f.read()
        else:
            err_str = f"No file: {config_file_path} exist"
            logging.exception(err_str)
            raise Exception(err_str)


def update_yaml_configurations(yaml_content: Dict | str, config_file_path: str | None = None,
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


def _compare_n_patch_list(stored_list: List, updated_list: List):
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
                        _compare_n_patch_list(underlying_stored_list, underlying_updated_list)
            else:
                err_str = "updated_list's elements are not same datatypes as stored_list's elements;;;"
                logging.exception(err_str)
                raise Exception(err_str)
        elif isinstance(stored_list[0], dict):
            # If elements are of dict type then checking if id key is present in elements
            if stored_list[0].get("id") is not None:
                stored_id_idx_dict: Dict = {stored_obj.get("id"): idx for idx, stored_obj in enumerate(stored_list)}
                for index, update_dict in enumerate(updated_list):
                    if isinstance(update_dict, dict):
                        if (updated_id := update_dict.get("id")) is not None:
                            # If id is new then appending update_dict to main list
                            if updated_id not in stored_id_idx_dict:
                                stored_list.append(update_dict)
                            else:
                                stored_index = stored_id_idx_dict[updated_id]
                                # If update_dict only has id and the id is same as in stored_list - delete entry
                                # Update list has id checked above + len == 1 confirms all it has is id
                                if len(update_dict) == 1:
                                    stored_list.remove(stored_list[stored_index])
                                    stored_id_idx_dict = \
                                        {stored_obj.get("id"): idx for idx, stored_obj in enumerate(stored_list)}
                                else:
                                    # patch operation on dict in stored_list to update
                                    stored_list[stored_index] = \
                                        compare_n_patch_dict(stored_list[stored_index], update_dict)
                        else:
                            err_str = "updated_list's dict elements don't have id field but stored_list's elements do;;;"
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
        if updated_value is not None:
            stored_value = stored_dict[key]
            if isinstance(stored_value, dict):
                # dict value type is container, pass extracted (reference modified directly)
                compare_n_patch_dict(stored_value, updated_value)
            elif isinstance(stored_value, list):
                # list value type is container, pass extracted (reference modified directly)
                _compare_n_patch_list(stored_value, updated_value)
            elif stored_value != updated_value:  # avoid unwarranted lookup(simple types)/construction(complex types)
                # non container types are just assigned (+ no patch support: add/overwrite if set)
                stored_dict[key] = updated_value
            # else not required - old and new val are same
    return stored_dict


def get_host_port_from_env(default_host: str = "127.0.0.1", default_port: int = 8020) -> Tuple[str, int]:
    host_str: str = default_host if (host_env := os.getenv("HOST")) is None or len(host_env) == 0 else host_env
    port_str: str = str(default_port) if (port_env := (os.getenv("PORT"))) is None or len(port_env) == 0 else port_env
    int_port: int = int(port_str)
    return host_str, int_port


def db_collections(mongo_server: str, database_name: str, ignore_collections: List[str] | None = None):
    """
    Generator to get collection instance from mongodb (ignores collections present in ignore_collections list)
    :param mongo_server:
    :param database_name: Name of db
    :param ignore_collections: name of collections to be ignored
    :return: collections instance
    """
    client: MongoClient | None = None
    try:
        client = MongoClient(mongo_server)
        db = client.get_database(name=database_name)
        collections: List[str] = db.list_collection_names()
        for collection in collections:
            if collection not in ignore_collections:
                yield db[collection]
    except Exception as e:
        err_str = f"drop_mongo_collections failed for DB: {database_name};;;exception: {e}"
        logging.exception(err_str)
        raise e
    finally:
        client.close()


def drop_mongo_collections(mongo_server: str, database_name: str, ignore_collections: List[str] | None = None) -> None:
    """
    Drops all collections present in collections except ``ignore_collections``
    :param mongo_server: Mongo Server that requires Cleaning
    :param database_name: Name of db
    :param ignore_collections: name of collections to be ignored from getting dropped
    :return: None
    """
    for collection in db_collections(mongo_server, database_name, ignore_collections):
        collection.drop()


def clean_mongo_collections(mongo_server: str, database_name: str, ignore_collections: List[str] | None = None) -> None:
    """
    Cleans all collections (deletes all documents) present in collections except ``ignore_collections``
    :param mongo_server: Mongo Server that requires Cleaning
    :param database_name: Name of db
    :param ignore_collections: name of collections to be ignored from getting cleaned
    :return: None
    """
    for collection in db_collections(mongo_server, database_name, ignore_collections):
        collection.delete_many({})
