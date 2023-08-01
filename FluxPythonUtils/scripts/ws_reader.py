import os
import sys
import time
from typing import Type, Callable, ClassVar, List
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError, ConnectionClosed
import asyncio
import json
import logging
from threading import RLock

os.environ["DBType"] = "beanie"


class WSReader:
    shutdown: ClassVar[bool] = True

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

    ws_cont_list: ClassVar[List['WSReader']] = list()

    # callable accepts List of PydanticClassType or None; None implies WS connection closed
    def __init__(self, uri: str, PydanticClassType: Type, PydanticClassTypeList: Type, callback: Callable,
                 notify: bool = True):
        self.uri = uri
        self.PydanticClassType: Type = PydanticClassType
        self.PydanticClassTypeList = PydanticClassTypeList
        self.callback = callback
        self.ws = None
        self.is_first = True
        self.single_obj_lock = RLock()
        self.notify = notify
        self.force_disconnected = False
        WSReader.ws_cont_list.append(self)

    @staticmethod
    def read_pydantic_obj_list(json_data, PydanticClassListType):
        try:
            # how do we safely & efficiently test if JSON data is of type list, to avoid except & return None instead?
            pydantic_obj_list = PydanticClassListType(__root__=json_data)
            return pydantic_obj_list
        except Exception as e:
            logging.exception(f"list type: {PydanticClassListType} json decode failed;;;json_data: {json_data}, "
                              f"exception: {e}")
            return None

    @staticmethod
    def read_pydantic_obj(json_data, PydanticClassType):
        try:
            pydantic_obj = PydanticClassType(**json_data)
            return pydantic_obj
        except Exception as e:
            logging.exception(f"{PydanticClassType} json decode failed;;;exception: {e}, json_data: {json_data}")
            return None

    @staticmethod
    def dispatch_pydantic_obj(pydantic_obj, callback):
        try:
            callback(pydantic_obj)
        except Exception as e:
            logging.exception(f"dropping this update - ws invoked callback threw exception;;;"
                          f"PydanticObj: {pydantic_obj}, Exception {e}")

    @staticmethod
    def handle_json_str(json_str, ws_cont):
        json_data = None
        try:
            json_data = json.loads(json_str)
        except Exception as e:
            logging.exception(f"dropping update, json loads failed, no json_data from json_str"
                              f"first update for {ws_cont.PydanticClassTypeList}"
                              f";;;Json str: {json_str}, exception: {e}")
        # logging.debug(f"ws received json data;;;{json_data}---")
        if ws_cont.is_first:
            pydantic_obj_list = WSReader.read_pydantic_obj_list(json_data, ws_cont.PydanticClassTypeList)
            if pydantic_obj_list is not None:
                for pydantic_obj in pydantic_obj_list.__root__:
                    WSReader.dispatch_pydantic_obj(pydantic_obj, ws_cont.callback)
            else:
                logging.error(f"first message is not-list, attempting direct object conversion instead with for "
                              f"{ws_cont.PydanticClassType};;;json_str: {json_str}")
                pydantic_obj = WSReader.read_pydantic_obj(json_data, ws_cont.PydanticClassType)
                if pydantic_obj:
                    WSReader.dispatch_pydantic_obj(pydantic_obj, ws_cont.callback)
                else:
                    logging.error(f"dropping update, list/non-list both attempts failed to parse received "
                                  f"first update for {ws_cont.PydanticClassTypeList}"
                                  f";;;Json data: {json_data}")
            ws_cont.is_first = False
        else:
            pydantic_obj = WSReader.read_pydantic_obj(json_data, ws_cont.PydanticClassType)
            if pydantic_obj:
                WSReader.dispatch_pydantic_obj(pydantic_obj, ws_cont.callback)
            else:
                logging.error("parsing failed, likely non-first list message, attempting list object "
                              f"conversion with {ws_cont.PydanticClassTypeList};;;json_data: {json_data}")
                pydantic_obj_list = WSReader.read_pydantic_obj_list(json_data, ws_cont.PydanticClassTypeList)
                if pydantic_obj_list is not None:
                    for pydantic_obj in pydantic_obj_list.__root__:
                        WSReader.dispatch_pydantic_obj(pydantic_obj, ws_cont.callback)
                else:
                    logging.error(f"dropping update, list: {ws_cont.PydanticClassTypeList}/non-list: "
                                  f"{ws_cont.PydanticClassType} both attempts failed to parse received non-first data"
                                  f";;;Json data: {json_data}")

    # handle connection and communication with the server
    # TODO BUF FIX: current implementation would lose connection permanently if server is restarted forcing meed for
    #  client restart. The fix is to move websockets.connect in a periodic task and here we just mark the ws_cont
    #  disconnected
    @staticmethod
    async def ws_client():
        # Connect to the server (don't send timeout=None to prod, used for debug only)
        pending_tasks = list()
        json_str = "{\"Done\": 1}"
        for idx, ws_cont in enumerate(WSReader.ws_cont_list):
            # default max buffer size is: 10MB, pass max_size=value to connect and increase / decrease the default
            # size, for eg: max_size=2**24 to change the limit to 16 MB
            try:
                ws_cont.ws = await websockets.connect(ws_cont.uri, ping_timeout=None, max_size=2 ** 24)
                task = asyncio.create_task(ws_cont.ws.recv(), name=str(idx))
                pending_tasks.append(task)
            except Exception as e:
                logging.exception(f"ws_client error while connecting/async task submission ws_cont: {ws_cont}, "
                                  f"exception: {e}")
        ws_remove_set = set()
        while len(pending_tasks):
            try:
                # wait doesn't raise TimeoutError! Futures that aren't done when timeout occurs are returned in 2nd set
                # make timeout configurable with default 2 in debug explicitly keep 20++ by configuring it such ?
                completed_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED,
                                                                    timeout=20.0)
            except Exception as e:
                logging.exception(f"await asyncio.wait raised exception: {e}")
                if not WSReader.shutdown:
                    continue
                else:
                    break  # here we don't close any web-socket on purpose - as we can't be sure of the exception reason
            ws_remove_set.clear()
            while completed_tasks:
                if WSReader.shutdown:
                    for task in pending_tasks:
                        idx = int(task.get_name())
                        logging.debug(f"closing web socket connection gracefully within while loop for idx {idx};;;"
                                      f"ws_cont: {WSReader.ws_cont_list[idx]}")
                        WSReader.ws_cont_list[idx].ws.disconnect()
                        break

                data_found_task = None
                try:
                    data_found_task = completed_tasks.pop()
                except ConnectionClosedOK as e:
                    idx = int(data_found_task.get_name())
                    logging.debug('\n', f"web socket connection closed gracefully within while loop for idx {idx};;;"
                                        f" Exception: {e}")
                    ws_remove_set.add(idx)
                except ConnectionClosedError as e:
                    idx = int(data_found_task.get_name())
                    logging.exception('\n', f"web socket connection closed with error within while loop for idx {idx};;;"
                                      f" Exception: {e}")
                    ws_remove_set.add(idx)
                except ConnectionClosed as e:
                    idx = int(data_found_task.get_name())
                    logging.debug('\n', f"web socket connection closed within while loop for idx  {idx}"
                                        f"{data_found_task.get_name()};;; Exception: {e}")
                    ws_remove_set.add(idx)
                except Exception as e:
                    idx = int(data_found_task.get_name())
                    logging.debug('\n', f"web socket future returned exception within while loop for idx  {idx}"
                                        f"{data_found_task.get_name()};;; Exception: {e}")
                    # should we remove this ws - maybe this is an intermittent error, improve handling case by case
                    ws_remove_set.add(idx)

                idx = int(data_found_task.get_name())

                if ws_remove_set and idx in ws_remove_set:
                    logging.error(f"dropping {WSReader.ws_cont_list[idx]} - found in ws_remove_set")
                    # TODO important add bug fix for server side restart driven reconnect logic by using
                    #  force_disconnected
                    WSReader.ws_cont_list[idx].force_disconnected = True
                    continue

                json_str = data_found_task.result()
                if json_str is not None:
                    recreated_task = asyncio.create_task(WSReader.ws_cont_list[idx].ws.recv(), name=str(idx))
                    pending_tasks.add(recreated_task)
                    if len(json_str) > (2 ** 20):
                        logging.warning(
                            f"> 1 MB json_str detected, size: {len(json_str)} in ws recv;;;json_str: {json_str}")
                    WSReader.handle_json_str(json_str, WSReader.ws_cont_list[idx])
                    json_str = None
                    json_data = None
                else:
                    logging.error(f"dropping {WSReader.ws_cont_list[idx]} - json_str found None")
                    WSReader.ws_cont_list[idx].ws.disconnect()
                    WSReader.ws_cont_list[idx].force_disconnected = True
                    continue
        logging.warning("Executor instance going down")
