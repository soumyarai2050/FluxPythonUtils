import os
import sys
import time
from typing import Type, Callable, ClassVar, List, Dict, Final
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError, ConnectionClosed
import asyncio
import json
import logging
from threading import RLock
import urllib.parse

from FluxPythonUtils.scripts.general_utility_functions import get_launcher_name


class WSReaderLite:
    shutdown: ClassVar[bool] = True
    size_1_mb: Final[int] = 2 ** 20
    size_16_mb: Final[int] = size_1_mb * 16
    size_8_mb: Final[int] = size_1_mb * 8
    size_10_mb: Final[int] = size_1_mb * 10
    size_12_mb: Final[int] = size_1_mb * 12
    no_warn_size_mb: Final[int] = size_12_mb
    max_ws_buff_size: ClassVar[int] = size_16_mb

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

    def __str__(self):
        return (f"is_first: {self.is_first}-force_disconnected: {self.force_disconnected}-current_ws_shutdown: "
                f"{self.current_ws_shutdown}")

    async def disconnect_ws(self):
        if self.ws and not self.ws.closed:
            try:
                await self.ws.close()
            except Exception as e:
                logging.error(f"Error closing websocket: {e}")

    def register_to_run(self):
        WSReaderLite.ws_cont_list.append(self)

    @staticmethod
    def dispatch_model_obj(model_obj, callback):
        try:
            callback(model_obj)
        except Exception as e:
            logging.exception(f"dropping this update - ws invoked callback threw exception;;;"
                              f"ModelObj: {model_obj}, Exception {e}")

    @staticmethod
    def handle_json_str(json_str: str, ws_cont: 'WSReaderLite'):
        WSReaderLite.dispatch_model_obj(json_str, ws_cont.callback)

    async def _cleanup_tasks(self, tasks: set):
        """Helper method to clean up resources"""
        if self.ws:
            await self.disconnect_ws()
        for task in tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def current_ws_client(self) -> None:
        pending_tasks = set()
        try:
            self.ws = await websockets.connect(self.uri, ping_timeout=None,
                                               max_size=WSReaderLite.max_ws_buff_size)
            task = asyncio.create_task(self.ws.recv())
            pending_tasks.add(task)
        except websockets.exceptions.InvalidURI:
            logging.error(f"{get_launcher_name()} instance going down, current_ws_client error invalid URI: {self.uri}")
            await self._cleanup_tasks(pending_tasks)
            return
        except websockets.exceptions.ConnectionClosed as e:
            logging.error(f"Connection closed: {e}")
            await self._cleanup_tasks(pending_tasks)
            return
        except Exception as e:
            logging.exception(f"{get_launcher_name()} instance going down, current_ws_client error while "
                              f"connecting/asyncio.create_task ws: {self.ws}, exception: {e}")
            await self._cleanup_tasks(pending_tasks)
            return

        ws_remove = False
        while pending_tasks:
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
            try:
                while completed_tasks:
                    if self.current_ws_shutdown:
                        await self.disconnect_ws()
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
                        if len(json_str) > WSReaderLite.max_ws_buff_size:
                            logging.warning(
                                f"> 1 MB json_str detected, size: {len(json_str)} in ws recv;;;json_str: {json_str}")
                        WSReaderLite.handle_json_str(json_str, self)

                        json_str = None
                        json_data = None
                    else:
                        logging.error(f"dropping {self} - json_str found None")
                        await self.disconnect_ws()
                        self.force_disconnected = True
                        continue
            finally:
                # Cleanup connections and tasks
                for task in pending_tasks:
                    task.cancel()
                if self.ws and not self.ws.closed:
                    await self.ws.close()
        logging.warning(f"{get_launcher_name()} instance going down")

    def current_start(self):
        self.current_ws_shutdown = False
        try:
            # Start the connection
            loop = None
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            loop.run_until_complete(self.current_ws_client())
        except KeyboardInterrupt:
            pass

    async def current_stop(self):
        self.current_ws_shutdown = True
        await self.disconnect_ws()
        await asyncio.sleep(2)
