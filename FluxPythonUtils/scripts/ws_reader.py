import os
import sys
from typing import Type, Callable, ClassVar, List
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError, ConnectionClosed
import asyncio
from asyncio.exceptions import TimeoutError
import json
import logging
from threading import RLock

os.environ["DBType"] = "beanie"


class WSReader:
    @classmethod
    def start(cls):
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

    ws_cont_list: ClassVar[List] = list()

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
        WSReader.ws_cont_list.append(self)

    # handle connection and communication with the server
    @staticmethod
    async def ws_client():
        # Connect to the server (don't send timeout=None to prod, used for debug only)
        ws_list = list()
        pending_tasks = list()
        json_str = "{\"Done\": 1}"
        for idx, ws_cont in enumerate(WSReader.ws_cont_list):
            # default 10MB pass max_size=value to connect and increase / decrease the default size, eg. max_size=4**20
            ws_cont.ws = await websockets.connect(ws_cont.uri, ping_timeout=None)
            task = asyncio.create_task(ws_cont.ws.recv(), name=str(idx))
            pending_tasks.append(task)
        while True:
            try:
                data_found_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED,
                                                                     timeout=20.0)
            except TimeoutError as t:
                logging.debug(f'timeout: {t}')
                continue
            except ConnectionClosedOK as e:
                logging.debug('\n', f"web socket connection closed gracefully within while loop: {e}")
                break
            except ConnectionClosedError as e:
                logging.debug('\n', f"web socket connection closed with error within while loop: {e}")
                break
            except RuntimeError as e:
                logging.debug('\n', f"web socket raised runtime error within while loop: {e}")
                break
            except ConnectionClosed as e:
                logging.debug('\n', f"web socket connection closed within while loop: {e}")
                break
            while data_found_tasks:
                data_found_task = data_found_tasks.pop()
                idx = int(data_found_task.get_name())
                recreated_task = asyncio.create_task(WSReader.ws_cont_list[idx].ws.recv(), name=str(idx))
                pending_tasks.add(recreated_task)
                json_str = data_found_task.result()
                if json_str is not None:
                    json_data = json.loads(json_str)
                    # logging.debug(f"ws received json data;;;{json_data}---")
                    if WSReader.ws_cont_list[idx].is_first:
                        try:
                            pydantic_obj_list = WSReader.ws_cont_list[idx].PydanticClassTypeList(__root__=json_data)
                            for pydantic_obj in pydantic_obj_list.__root__:
                                WSReader.ws_cont_list[idx].callback(pydantic_obj)
                        except Exception as e:
                            logging.error("first message is not-list, attempting direct object conversion instead")
                            pydantic_obj = WSReader.ws_cont_list[idx].PydanticClassType(**json_data)
                            WSReader.ws_cont_list[idx].callback(pydantic_obj)
                        WSReader.ws_cont_list[idx].is_first = False
                    else:
                        pydantic_obj = WSReader.ws_cont_list[idx].PydanticClassType(**json_data)
                        WSReader.ws_cont_list[idx].callback(pydantic_obj)
                    json_str = None
                    json_data = None

        for ws in ws_list:
            ws.disconnect()


