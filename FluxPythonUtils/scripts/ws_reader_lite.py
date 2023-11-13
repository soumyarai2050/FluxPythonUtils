import os
import sys
import time
from typing import Type, Callable, ClassVar, List, Dict
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError, ConnectionClosed
import asyncio
import json
import logging
from threading import RLock
import urllib.parse

os.environ["DBType"] = "beanie"


class WSReaderLite:
    shutdown: ClassVar[bool] = True
    ws_cont_list: ClassVar[List['WSReaderLite']] = list()

    def __init__(self, uri: str, callback: Callable, query_kwargs: Dict | None = None):
        if query_kwargs is None:
            self.uri = uri
        else:
            # Adding kwargs as query params in uri
            self.uri = uri + "?" + urllib.parse.urlencode(query_kwargs)
        self.callback = callback
        self.ws = None
        self.is_first = True
        self.single_obj_lock = RLock()
        self.force_disconnected = False
        self.current_ws_shutdown: bool = True

    def register_to_run(self):
        WSReaderLite.ws_cont_list.append(self)

    @staticmethod
    def dispatch_pydantic_obj(pydantic_obj, callback):
        try:
            callback(pydantic_obj)
        except Exception as e:
            logging.exception(f"dropping this update - ws invoked callback threw exception;;;"
                              f"PydanticObj: {pydantic_obj}, Exception {e}")

    @staticmethod
    def handle_json_str(json_str: str, ws_cont):
        WSReaderLite.dispatch_pydantic_obj(json_str, ws_cont.callback)

    async def current_ws_client(self):
        pending_tasks = []
        try:
            self.ws = await websockets.connect(self.uri, ping_timeout=None, max_size=2 ** 24)
            task = asyncio.create_task(self.ws.recv())
            pending_tasks.append(task)
        except Exception as e:
            logging.exception(f"current_ws_client error while connecting/async task submission ws: {self.ws}, "
                              f"exception: {e}")

        ws_remove = False
        while len(pending_tasks):
            try:
                # wait doesn't raise TimeoutError! Futures that aren't done when timeout occurs are returned in 2nd set
                # make timeout configurable with default 2 in debug explicitly keep 20++ by configuring it such ?
                completed_tasks, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED,
                                                                    timeout=20.0)
            except Exception as e:
                logging.exception(f"await asyncio.wait raised exception: {e}")
                if not self.current_ws_shutdown:
                    continue
                else:
                    break  # here we don't close any web-socket on purpose - as we can't be sure of the exception reason
            ws_remove = False
            while completed_tasks:
                if self.current_ws_shutdown:
                    self.ws.disconnect()
                    break

                data_found_task = None
                try:
                    data_found_task = completed_tasks.pop()
                except ConnectionClosedOK as e:
                    logging.debug('\n', f"web socket connection closed gracefully within while loop for "
                                        f"ws of uri {self.uri};;; Exception: {e}")
                    ws_remove = True
                except ConnectionClosedError as e:
                    logging.exception('\n', f"web socket connection closed with error within while loop "
                                            f"for ws of uri {self.uri};;; Exception: {e}")
                    ws_remove = True
                except ConnectionClosed as e:
                    logging.debug('\n', f"web socket connection closed within while loop for "
                                        f"ws of uri {self.uri};;; Exception: {e}")
                    ws_remove = True
                except Exception as e:
                    logging.debug('\n', f"web socket future returned exception within while "
                                        f"loop for ws of uri {self.uri};;; Exception: {e}")
                    # should we remove this ws - maybe this is an intermittent error, improve handling case by case
                    ws_remove = True

                # idx = int(data_found_task.get_name())

                if ws_remove:
                    logging.error(f"dropping ws with url: {self.uri}")
                    # TODO important add bug fix for server side restart driven reconnect logic by using
                    #  force_disconnected
                    self.force_disconnected = True
                    continue

                json_str = data_found_task.result()
                if json_str is not None:
                    recreated_task = asyncio.create_task(self.ws.recv())
                    pending_tasks.add(recreated_task)
                    if len(json_str) > (2 ** 20):
                        logging.warning(
                            f"> 1 MB json_str detected, size: {len(json_str)} in ws recv;;;json_str: {json_str}")
                    WSReaderLite.handle_json_str(json_str, self)

                    json_str = None
                    json_data = None
                else:
                    logging.error(f"dropping {self} - json_str found None")
                    self.ws.disconnect()
                    self.force_disconnected = True
                    continue
        logging.warning("Executor instance going down")

    def current_start(self):
        self.current_ws_shutdown = False
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
                loop.run_until_complete(self.current_ws_client())
        except KeyboardInterrupt:
            pass

    def current_stop(self):
        self.current_ws_shutdown = True
        # ideally 20 instead of 2 : allows for timeout to notice shutdown is triggered and thread to teardown
        time.sleep(2)
