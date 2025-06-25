import os
import sys
import time
from typing import Type, Callable, ClassVar, List, Dict, Final
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError, ConnectionClosed
import asyncio
import logging
from threading import RLock
import urllib.parse
import orjson

from FluxPythonUtils.scripts.ws_reader_lite import WSReaderLite


class WSReader(WSReaderLite):

    @classmethod
    def start(cls):
        WSReader.shutdown = False
        try:
            # Start the connection
            loop = None
            if sys.version_info < (3, 10):
                loop = asyncio.get_event_loop()
            else:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(cls.ws_client())
        except KeyboardInterrupt:
            pass

    @staticmethod
    def stop():
        WSReader.shutdown = True
        # ideally 20 instead of 2 : allows for timeout to notice shutdown is triggered and thread to teardown
        time.sleep(2)

    ws_cont_list: ClassVar[List['WSReader']] = []
    new_ws_cont_list: ClassVar[List['WSReader']] = []

    # callable accepts List of ModelClassType or None; None implies WS connection closed
    def __init__(self, uri: str, ModelClassType: Type, ModelClassTypeList: Type, callback: Callable,
                 query_kwargs: Dict | None = None, notify: bool = True):
        super().__init__(uri, callback, query_kwargs)
        self.ModelClassType: Type = ModelClassType
        self.ModelClassTypeList = ModelClassTypeList
        self.notify = notify
        self.expired: bool = False

    def __str__(self):
        return (f"{super().__str__()}-ModelClassType: {self.ModelClassType}-ModelClassTypeList: "
                f"{self.ModelClassTypeList}-expired: {self.expired}")

    def register_to_run(self):
        WSReader.ws_cont_list.append(self)

    def new_register_to_run(self):
        WSReader.new_ws_cont_list.append(self)

    @staticmethod
    def read_model_obj_list(json_data: str, ModelClassListType):
        try:
            # how do we safely & efficiently test if JSON data is of type list, to avoid except & return None instead?
            model_obj_list = ModelClassListType.from_json_bytes(json_data)
            return model_obj_list
        except Exception as e:
            logging.exception(f"list type: {ModelClassListType} json decode failed;;;json_data: {json_data}, "
                              f"exception: {e}")
            return None

    @staticmethod
    def read_model_obj(json_data: str, ModelClassType):
        try:
            model_obj = ModelClassType.from_json_bytes(json_data)
            return model_obj
        except Exception as e:
            logging.exception(f"{ModelClassType.__name__} json decode failed;;;"
                              f"exception: {e}, json_data: {json_data}")
            return None

    @staticmethod
    def handle_json_str(json_str: str, ws_cont):
        json_data = None
        try:
            json_data = orjson.loads(json_str)
        except Exception as e:
            logging.exception(f"dropping update, json loads failed, no json_data from json_str"
                              f"first update for {ws_cont.ModelClassTypeList}"
                              f";;;Json str: {json_str}, exception: {e}")
        if json_str.startswith("{") and json_str.endswith("}"):
            model_obj = WSReader.read_model_obj(json_str, ws_cont.ModelClassType)
            if model_obj:
                WSReader.dispatch_model_obj(model_obj, ws_cont.callback)
        elif json_str.startswith("[") and json_str.endswith("]"):
            model_obj_list = WSReader.read_model_obj_list(json_str, ws_cont.ModelClassTypeList)
            if model_obj_list is not None:
                for model_obj in model_obj_list.root:
                    WSReader.dispatch_model_obj(model_obj, ws_cont.callback)
        else:
            logging.error(f"Dropping update: loaded json from json_str is not instance of list or dict, "
                          f"json type: = for {ws_cont.ModelClassType};;; json_str: {json_str}")

    # handle connection and communication with the server
    # TODO BUF FIX: current implementation would lose connection permanently if server is restarted forcing meed for
    #  client restart. The fix is to move websockets.connect in a periodic task and here we just mark the ws_cont
    #  disconnected
    @classmethod
    async def ws_client(cls):
        # Connect to the server (don't send timeout=None to prod, used for debug only)
        pending_tasks = []
        for idx, ws_cont in enumerate(WSReader.ws_cont_list):
            try:
                # default max buffer size is: 10MB, pass max_size=value to connect and increase / decrease the default
                # size, for eg: max_size=2**24 to change the limit to 16 MB
                ws_cont.ws = await websockets.connect(ws_cont.uri, ping_timeout=None,
                                                      max_size=WSReaderLite.max_ws_buff_size)
                task = asyncio.create_task(ws_cont.ws.recv(), name=str(idx))
            except websockets.exceptions.InvalidURI:
                logging.error(f"ws_client error invalid URI: {ws_cont.uri}")
                ws_cont.force_disconnected = True
            except Exception as exp:
                logging.exception(f"ws_client error while connecting/async task submission ws_cont: {ws_cont}, "
                                  f"exception: {exp}")
                ws_cont.force_disconnected = True
            else:
                pending_tasks.append(task)

        ws_remove_set = set()
        while len(pending_tasks):
            try:
                # wait doesn't raise TimeoutError! Futures that aren't done when timeout occurs are returned in 2nd set
                # make timeout configurable with default 2 in debug explicitly keep 20++ by configuring it such ?
                completed_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED,
                                                                    timeout=20.0)
            except Exception as exp:
                logging.exception(f"await asyncio.wait raised {exp=}")
                if not WSReader.shutdown:
                    continue
                else:
                    break  # here we don't close any web-socket on purpose - as we can't be sure of the exception reason
            ws_remove_set.clear()
            if WSReader.shutdown:  # disconnect pending_tasks here, completed_tasks after pop and result extraction
                await cls.disconnect_ws_tasks(pending_tasks)
                pending_tasks = []  # breaks outer loop
            while completed_tasks:
                data_found_task = None
                json_data: bytes | None = None
                try:
                    data_found_task = completed_tasks.pop()
                    json_data = data_found_task.result()
                except ConnectionClosedOK as exp:
                    idx = int(data_found_task.get_name())
                    logging.debug('\n', f"web socket connection closed gracefully within while loop for "
                                        f"{idx=};;;{exp=}")
                    ws_remove_set.add(idx)
                except ConnectionClosedError as exp:
                    idx = int(data_found_task.get_name())
                    logging.exception('\n', f"web socket connection closed with error within while loop for "
                                            f"{idx=};;;{exp=}")
                    ws_remove_set.add(idx)
                except ConnectionClosed as exp:
                    idx = int(data_found_task.get_name())
                    logging.debug('\n', f"web socket connection closed within while loop for {idx=}, "
                                        f"{data_found_task.get_name()};;;{exp=}")
                    ws_remove_set.add(idx)
                except Exception as exp:
                    idx = int(data_found_task.get_name())
                    logging.debug('\n', f"web socket future returned exception within while loop for {idx=}, "
                                        f"{data_found_task.get_name()};;;{exp=}")
                    # TODO: should we remove this ws: maybe it is an intermittent error, improve handling case by case
                    ws_remove_set.add(idx)

                if isinstance(json_data, bytes):
                    json_str = json_data.decode("utf-8")
                else:
                    json_str = json_data
                idx = int(data_found_task.get_name())

                if ws_remove_set and idx in ws_remove_set:
                    logging.error(f"dropped idx: {idx} of uri: {WSReader.ws_cont_list[idx].uri};;;"
                                  f"ws_reader: {WSReader.ws_cont_list[idx]}, found in ws_remove_set")
                    WSReader.ws_cont_list[idx].force_disconnected = True
                    continue
                if json_str is not None:
                    if WSReader.shutdown:
                        # callback not to be called / relied upon once shutdown detected
                        await cls.disconnect_ws_tasks([WSReader.ws_cont_list[idx]])
                        logging.warning(f"dropping ws {idx} {json_str}, due to {WSReader.shutdown=};;;{idx=}; "
                                        f"{[WSReader.ws_cont_list[idx]]=}")
                        continue
                    # else non shutdown - proceed as normal
                    recreated_task = asyncio.create_task(WSReader.ws_cont_list[idx].ws.recv(), name=str(idx))
                    pending_tasks.add(recreated_task)
                    json_len: int = len(json_str)
                    ws_cont_ = WSReader.ws_cont_list[idx]
                    if json_len > cls.no_warn_size_mb:
                        ws_json_len_mb = json_len / cls.size_1_mb
                        max_ws_buff_len_mb = cls.max_ws_buff_size / cls.size_1_mb
                        logging.warning(
                            f"{ws_json_len_mb=:.1f} detected in ws recv, current {max_ws_buff_len_mb=:.1f} on "
                            f"{ws_cont_.uri=};;;First 1024 bytes: {json_str[:1024]=}")
                    WSReader.handle_json_str(json_str, ws_cont_)
                else:
                    logging.error(f"dropping {WSReader.ws_cont_list[idx]} - json_str found None")
                    await WSReader.ws_cont_list[idx].disconnect_ws()
                    WSReader.ws_cont_list[idx].force_disconnected = True
                    continue

            for idx, ws_cont in enumerate(WSReader.new_ws_cont_list):
                if WSReader.shutdown:
                    # callback not to be called / relied upon once shutdown detected
                    logging.warning(f"dropping {ws_cont.uri=} of WSReader.new_ws_cont_list, due to {WSReader.shutdown=}"
                                    f";;;{ws_cont=}")
                    continue
                # else non shutdown - proceed as normal
                WSReader.ws_cont_list.append(ws_cont)
                idx = WSReader.ws_cont_list.index(ws_cont)

                # default max buffer size is: 10MB, pass max_size=value to connect and increase / decrease the default
                # size, for eg: max_size=2**24 to change the limit to 16 MB
                try:
                    ws_cont.ws = await websockets.connect(ws_cont.uri, ping_timeout=None,
                                                          max_size=WSReaderLite.max_ws_buff_size)
                    task = asyncio.create_task(ws_cont.ws.recv(), name=str(idx))
                except websockets.exceptions.InvalidURI:
                    logging.error(f"ws_client error invalid URI: {ws_cont.uri}")
                    ws_cont.force_disconnected = True
                except Exception as exp:
                    logging.exception(f"ws_client exception while connecting/async task submission for {ws_cont.uri=};"
                                      f" {ws_cont.ModelClassType=}; {exp=};;;{ws_cont=}")
                    ws_cont.force_disconnected = True
                else:
                    pending_tasks.add(task)
                    logging.debug(f"Added new ws in pending list for {ws_cont.uri=}")
                WSReader.new_ws_cont_list.remove(ws_cont)
        logging.warning(f"WSReader instance going down")

    @classmethod
    async def disconnect_ws_tasks(cls, pending_tasks):
        if pending_tasks and 0 != len(pending_tasks):
            disconnected_ws_names: List[str] = []
            disconnected_ws_desc: List[str] = []
            for task in pending_tasks:
                idx = int(task.get_name())
                await WSReader.ws_cont_list[idx].disconnect_ws()
                disconnected_ws_names.append(f"{idx=}; ")
                disconnected_ws_desc.append(f"{idx=}-{WSReader.ws_cont_list[idx]=}; ")
            logging.warning(f"closed web-socket connections gracefully within while loop for: "
                            f"{[name for name in disconnected_ws_names]};;;"
                            f"{[desc for desc in disconnected_ws_desc]}")
