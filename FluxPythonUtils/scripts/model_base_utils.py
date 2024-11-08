# standard imports
import dataclasses
import datetime
import logging
from typing import ClassVar, Dict, Any, List, Type
from threading import Lock
from re import sub
import types

# 3rd party imports
import orjson
from pydantic import ConfigDict, BaseModel
from pendulum import DateTime, Date, parse as pendulum_parse
from bson import ObjectId
import msgspec
import os
from fastapi.encoders import jsonable_encoder
from fastapi.responses import Response
from pandas import Timestamp


def to_camel(value):
    value = sub(r"(_|-)+", " ", value).title().replace(" ", "")
    return "".join([value[0].lower(), value[1:]])


class CacheBaseModel(BaseModel):
    _cache_obj_id_to_obj_dict: ClassVar[Dict[Any, Any]] = {}
    _mutex: ClassVar[Lock] = Lock()

    @classmethod
    async def get(cls, obj_id: Any):
        with cls._mutex:
            if obj_id not in cls._cache_obj_id_to_obj_dict:
                return None
            else:
                return cls._cache_obj_id_to_obj_dict[obj_id]

    @classmethod
    def find_all(cls):
        return cls

    @classmethod
    async def to_list(cls):
        return list(cls._cache_obj_id_to_obj_dict.values())

    async def create(self):
        with self._mutex:
            if self.id in self._cache_obj_id_to_obj_dict:
                err_str = f"Id: {self.id} already exists"
                raise Exception(err_str)
            else:
                self._cache_obj_id_to_obj_dict[self.id] = self.copy(deep=True)
                return self

    async def update(self, request_obj: Dict):
        update_data = dict(request_obj)["$set"]
        self.__dict__.update(dict(update_data))

    async def delete(self):
        with self._mutex:
            if self.id not in self._cache_obj_id_to_obj_dict:
                err_str = f"Id: {self.id} Doesn't exists"
                raise Exception(err_str)
            else:
                del self._cache_obj_id_to_obj_dict[self.id]


class BareBaseModel(BaseModel):

    @classmethod
    async def get(cls, obj_id: Any):
        return None

    @classmethod
    def find_all(cls):
        return cls

    @classmethod
    async def to_list(cls):
        return []

    async def create(self):
        return self

    async def update(self, request_obj: Dict):
        return self

    async def delete(self):
        return None


class CamelBaseModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class CamelCacheBaseModel(CacheBaseModel, CamelBaseModel):
    ...


class BareCamelBaseModel(BareBaseModel, CamelBaseModel):
    ...


class PydanticBaseModel(BaseModel):
    @classmethod
    def from_json_str(cls, json_data: bytes | str):
        json_data_dict = orjson.loads(json_data)
        return cls(**json_data_dict)

    @classmethod
    def from_dict(cls, args_dict: Dict):
        return cls(**args_dict)

    @classmethod
    def from_kwargs(cls, **kwargs):
        return cls(**kwargs)

    def to_dict(self, **kwargs) -> Dict:
        return jsonable_encoder(self, **kwargs)

    def to_json_str(self, **kwargs) -> str:
        return self.model_dump_json(**kwargs)


@dataclasses.dataclass(kw_only=True)
class DataclassBaseModel:

    @classmethod
    def from_json_str(cls, json_data: bytes | str):
        json_data_dict = orjson.loads(json_data)
        return cls(**json_data_dict)

    @classmethod
    def from_dict(cls, args_dict: Dict):
        return cls(**args_dict)

    @classmethod
    def from_kwargs(cls, **kwargs):
        return cls(**kwargs)

    def to_dict(self, **kwargs) -> Dict:
        return orjson.loads(orjson.dumps(self, default=str))

    def to_json_str(self, **kwargs) -> str | bytes:
        return orjson.dumps(self, default=str)


def dec_hook(type: Type, obj: Any) -> Any:
    if type == DateTime and isinstance(obj, str):
        return pendulum_parse(obj)
    elif type == DateTime and isinstance(obj, DateTime):
        return obj
    elif type == DateTime and isinstance(obj, datetime.datetime):
        return pendulum_parse(str(obj))


def enc_hook(obj: Any) -> Any:
    if isinstance(obj, DateTime):
        return obj.isoformat()
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, Timestamp):
        return obj.isoformat()


class MsgspecBaseModel(msgspec.Struct, kw_only=True):
    custom_builtin_types = [DateTime, Date, types.FunctionType, Timestamp, datetime.datetime]

    @classmethod
    def from_json_str(cls, json_str: bytes | str, **kwargs):
        strict = kwargs.pop("strict", True)
        return msgspec.json.decode(json_str, type=cls, dec_hook=dec_hook, strict=strict)

    @classmethod
    def from_dict(cls, args_dict: Dict, **kwargs):
        strict = kwargs.pop("strict", True)
        return msgspec.convert(args_dict, type=cls, dec_hook=dec_hook, strict=strict)

    @classmethod
    def from_dict_list(cls, args_dict_list: List[Dict], **kwargs):
        strict = kwargs.pop("strict", True)
        return msgspec.convert(args_dict_list, type=List[cls], dec_hook=dec_hook, strict=strict)

    @classmethod
    def from_kwargs(cls, **kwargs):
        return msgspec.convert(kwargs, type=cls, dec_hook=dec_hook)

    def to_dict(self, **kwargs) -> Dict:
        exclude_none = kwargs.get("exclude_none", False)
        return_val = msgspec.to_builtins(self, builtin_types=MsgspecBaseModel.custom_builtin_types)
        if exclude_none:
            return_val = remove_none_values(return_val)
        return return_val

    def to_json_dict(self, **kwargs) -> Dict:
        """
        Converts obj to jsonable dict which converts datetime obj to str
        """
        exclude_none = kwargs.get("exclude_none", False)
        return_val = msgspec.to_builtins(self, enc_hook=enc_hook)
        if exclude_none:
            return_val = remove_none_values(return_val)
        return return_val

    def to_json_str(self, **kwargs) -> str | bytes:
        return msgspec.json.encode(self, enc_hook=enc_hook)


class IncrementalIdBaseModel(PydanticBaseModel):
    _max_id_val: ClassVar[int | None] = None
    _max_update_id_val: ClassVar[int | None] = None
    _mutex: ClassVar[Lock] = Lock()
    read_ws_path_ws_connection_manager: ClassVar[Any] = None
    read_ws_path_with_id_ws_connection_manager: ClassVar[Any] = None

    @classmethod
    def init_max_id(cls, max_id_val: int, max_update_id_val: int) -> None:
        """
        This method must be called just after db is initialized, and it must be
        passed with current max id (if recovering) or 0 (if starting fresh)
        """
        cls._max_id_val = max_id_val
        cls._max_update_id_val = max_update_id_val

    @classmethod
    def peek_max_id(cls) -> int:
        return cls._max_id_val

    @classmethod
    def peek_max_update_id(cls) -> int:
        return cls._max_update_id_val

    @classmethod
    def next_id(cls) -> int:
        with cls._mutex:
            if cls._max_id_val is not None:
                cls._max_id_val += 1
                return cls._max_id_val
            else:
                err_str = ("init_max_id needs to be called to initialize _max_id_val before calling "
                           f"get_auto_increment_id, occurred in model: {cls.__name__}")
                logging.exception(err_str)
                raise Exception(err_str)

    @classmethod
    def next_update_id(cls) -> int:
        with cls._mutex:
            if cls._max_update_id_val is not None:
                cls._max_update_id_val += 1
                return cls._max_update_id_val
            else:
                err_str = ("init_max_id needs to be called to initialize _max_update_id_val before calling "
                           f"get_auto_increment_id, occurred in model: {cls.__name__}")
                logging.exception(err_str)
                raise Exception(err_str)


@dataclasses.dataclass(kw_only=True)
class IncrementalIdDataClass(DataclassBaseModel):
    _max_id_val: ClassVar[int | None] = None
    _max_update_id_val: ClassVar[int | None] = None
    _mutex: ClassVar[Lock] = Lock()
    read_ws_path_ws_connection_manager: ClassVar[Any] = None
    read_ws_path_with_id_ws_connection_manager: ClassVar[Any] = None

    @classmethod
    def init_max_id(cls, max_id_val: int, max_update_id_val: int) -> None:
        """
        This method must be called just after db is initialized, and it must be
        passed with current max id (if recovering) or 0 (if starting fresh)
        """
        cls._max_id_val = max_id_val
        cls._max_update_id_val = max_update_id_val

    @classmethod
    def peek_max_id(cls) -> int:
        return cls._max_id_val

    @classmethod
    def peek_max_update_id(cls) -> int:
        return cls._max_update_id_val

    @classmethod
    def next_id(cls) -> int:
        with cls._mutex:
            if cls._max_id_val is not None:
                cls._max_id_val += 1
                return cls._max_id_val
            else:
                err_str = ("init_max_id needs to be called to initialize _max_id_val before calling "
                           f"get_auto_increment_id, occurred in model: {cls.__name__}")
                logging.exception(err_str)
                raise Exception(err_str)

    @classmethod
    def next_update_id(cls) -> int:
        with cls._mutex:
            if cls._max_update_id_val is not None:
                cls._max_update_id_val += 1
                return cls._max_update_id_val
            else:
                err_str = ("init_max_id needs to be called to initialize _max_update_id_val before calling "
                           f"get_auto_increment_id, occurred in model: {cls.__name__}")
                logging.exception(err_str)
                raise Exception(err_str)


class IncrementalIdMsgspec(MsgspecBaseModel, kw_only=True):
    _max_id_val: ClassVar[int | None] = None
    _max_update_id_val: ClassVar[int | None] = None
    _mutex: ClassVar[Lock] = Lock()
    read_ws_path_ws_connection_manager: ClassVar[Any] = None
    read_ws_path_with_id_ws_connection_manager: ClassVar[Any] = None

    @classmethod
    def init_max_id(cls, max_id_val: int, max_update_id_val: int, force_set: bool | None = False) -> None:
        """
        This method must be called just after db is initialized, and it must be
        passed with current max id (if recovering) or 0 (if starting fresh)
        """
        if force_set:
            cls._max_id_val = max_id_val
        else:
            if cls._max_id_val is None:
                cls._max_id_val = max_id_val
            else:
                if cls._max_id_val < max_id_val:    # sets whatever is max if is called for nested type inits
                    cls._max_id_val = max_id_val
                # else not required: if set values is already bigger than passed value then ignoring

        if force_set:
            cls._max_update_id_val = max_update_id_val
        else:
            if max_update_id_val is not None:
                cls._max_update_id_val = max_update_id_val
            # else not required: set value if not None

    @classmethod
    def peek_max_id(cls) -> int:
        return cls._max_id_val

    @classmethod
    def peek_max_update_id(cls) -> int:
        return cls._max_update_id_val

    @classmethod
    def next_id(cls) -> int:
        with cls._mutex:
            if cls._max_id_val is not None:
                cls._max_id_val += 1
                return cls._max_id_val
            else:
                err_str = ("init_max_id needs to be called to initialize _max_id_val before calling "
                           f"get_auto_increment_id, occurred in model: {cls.__name__}")
                logging.exception(err_str)
                raise Exception(err_str)

    @classmethod
    def next_update_id(cls) -> int:
        with cls._mutex:
            if cls._max_update_id_val is not None:
                cls._max_update_id_val += 1
                return cls._max_update_id_val
            else:
                err_str = ("init_max_id needs to be called to initialize _max_update_id_val before calling "
                           f"get_auto_increment_id, occurred in model: {cls.__name__}")
                logging.exception(err_str)
                raise Exception(err_str)


class IncrementalIdCamelBaseModel(IncrementalIdBaseModel, CamelBaseModel):
    _max_id_val: ClassVar[int | None] = None
    _mutex: ClassVar[Lock] = Lock()
    read_ws_path_ws_connection_manager: ClassVar[Any] = None
    read_ws_path_with_id_ws_connection_manager: ClassVar[Any] = None


class IncrementalIdCacheBaseModel(CacheBaseModel, IncrementalIdBaseModel):
    _max_id_val: ClassVar[int | None] = None
    _mutex: ClassVar[Lock] = Lock()
    read_ws_path_ws_connection_manager: ClassVar[Any] = None
    read_ws_path_with_id_ws_connection_manager: ClassVar[Any] = None


class IncrementalIdCamelCacheBaseModel(CamelCacheBaseModel, IncrementalIdBaseModel):
    _max_id_val: ClassVar[int | None] = None
    _mutex: ClassVar[Lock] = Lock()
    read_ws_path_ws_connection_manager: ClassVar[Any] = None
    read_ws_path_with_id_ws_connection_manager: ClassVar[Any] = None


class UniqueStrIdBaseModel(PydanticBaseModel):
    _mutex: ClassVar[Lock] = Lock()

    @classmethod
    def next_id(cls) -> str:
        with cls._mutex:
            return f"{ObjectId()}"


@dataclasses.dataclass(kw_only=True)
class UniqueStrIdDataClass(DataclassBaseModel):
    _mutex: ClassVar[Lock] = Lock()

    @classmethod
    def next_id(cls) -> str:
        with cls._mutex:
            return f"{ObjectId()}"


class UniqueStrIdMsgspec(MsgspecBaseModel, kw_only=True):
    _mutex: ClassVar[Lock] = Lock()

    @classmethod
    def next_id(cls) -> str:
        with cls._mutex:
            return f"{ObjectId()}"


class UniqueStrIdCamelBaseModel(UniqueStrIdBaseModel, CamelBaseModel):
    _mutex: ClassVar[Lock] = Lock()
    read_ws_path_ws_connection_manager: ClassVar[Any] = None
    read_ws_path_with_id_ws_connection_manager: ClassVar[Any] = None


class UniqueStrIdCacheBaseModel(CacheBaseModel, UniqueStrIdBaseModel):
    _mutex: ClassVar[Lock] = Lock()
    read_ws_path_ws_connection_manager: ClassVar[Any] = None
    read_ws_path_with_id_ws_connection_manager: ClassVar[Any] = None


class UniqueStrIdCamelCacheBaseModel(CamelCacheBaseModel, UniqueStrIdBaseModel):
    _mutex: ClassVar[Lock] = Lock()
    read_ws_path_ws_connection_manager: ClassVar[Any] = None
    read_ws_path_with_id_ws_connection_manager: ClassVar[Any] = None


class ListModelMsgspec(msgspec.Struct, kw_only=True):
    root: List[Any]      # name root is used to make it consistent as pydantic impl

    @classmethod
    def from_json_str(cls, json_str: bytes | str) -> 'ListModelMsgspec':
        obj_list = msgspec.json.decode(json_str, type=cls.__annotations__['root'], dec_hook=dec_hook)
        return cls(root=obj_list)


@dataclasses.dataclass
class ListModelBase:
    root: List[Any]      # name root is used to make it consistent as pydantic impl

    @classmethod
    def from_json_str(cls, json_str: bytes | str) -> 'ListModelBase':
        json_data_dict = orjson.loads(json_str)
        return cls(root=json_data_dict)


@dataclasses.dataclass(slots=True)
class JsonNDataClassHandler:
    dataclass_type: Any
    _is_clean: bool = False
    json_dict: Dict[str, Any] | None = None
    stored_json_dict: Dict[str, Any] | None = None
    dataclass_obj: dataclasses.dataclass = None
    json_dict_list: List[Dict[str, Any]] | None = None
    stored_json_dict_list: List[Dict[str, Any]] | None = None
    dataclass_obj_list: List[dataclasses.dataclass] | None = None

    def clear(self):
        self._is_clean = True
        self.json_dict = None
        self.stored_json_dict = None
        self.dataclass_obj = None
        self.json_dict_list = None
        self.stored_json_dict_list = None
        self.dataclass_obj_list = None

    def set_json_dict(self, json_dict):
        self._is_clean = False
        self.json_dict = json_dict
        # id_val = self.json_dict.get("_id")
        # if id_val is None:
        #     self.json_dict["_id"] = self.dataclass_type.next_id()

    def set_stored_json_dict(self, json_dict):
        self._is_clean = False
        self.stored_json_dict = json_dict

    def set_json_dict_list(self, json_dict_list):
        self._is_clean = False
        self.json_dict_list = json_dict_list
        # for json_dict in self.json_dict_list:
        #     id_val = json_dict.get("_id")
        #     if id_val is None:
        #         json_dict["_id"] = self.dataclass_type.next_id()

    def set_stored_json_dict_list(self, json_dict_list):
        self._is_clean = False
        self.stored_json_dict_list = json_dict_list

    def get_json_dict(self):
        return self.json_dict

    def get_json_dict_list(self):
        return self.json_dict_list

    def update_dataclass_obj(self):
        self.dataclass_obj = self.dataclass_type(**self.json_dict)

    def update_dataclass_obj_list(self):
        self.dataclass_obj_list = [self.dataclass_type(**json_dict) for json_dict in self.json_dict_list]

    def get_dataclass_obj(self):
        if self.dataclass_obj is None:
            self.dataclass_obj = self.dataclass_type(**self.json_dict)
            return self.dataclass_obj
        else:
            return self.dataclass_obj

    def get_dataclass_obj_list(self):
        if self.dataclass_obj_list is None:
            self.dataclass_obj_list = [self.dataclass_type(**json_dict) for json_dict in self.json_dict_list]
            return self.dataclass_obj_list
        else:
            return self.dataclass_obj_list


class CustomFastapiResponse(Response):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return content


def remove_none_values(data: Dict) -> Dict | List[Dict]:
    """
    Recursively remove keys with None values from the dictionary, including nested dictionaries.
    """
    if isinstance(data, list):
        for i, d in enumerate(data):
            data[i] = remove_none_values(d)
        return data
    if not isinstance(data, dict):
        return data
    return {
        k: remove_none_values(v) for k, v in data.items()
        if v is not None and (not isinstance(v, dict) or remove_none_values(v))
    }
