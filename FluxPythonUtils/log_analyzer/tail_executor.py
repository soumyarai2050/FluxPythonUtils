# standard imports
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
import re
import subprocess
from typing import Dict, List, Callable, Type, Any, Tuple
from threading import Thread, current_thread, Lock
import signal
import select
import glob
import queue
from pathlib import PurePath
import multiprocessing
from filelock import FileLock
import inspect

# 3rd part imports
from pendulum import DateTime
import pendulum
import setproctitle

# project imports
from FluxPythonUtils.scripts.general_utility_functions import (
    parse_to_int, re_pattern_to_grep,
    get_log_line_no_from_timestamp, is_file_modified)
from Flux.PyCodeGenEngine.FluxCodeGenCore.perf_benchmark_decorators import (get_timeit_pattern,
                                                                            get_timeit_field_separator)
from FluxPythonUtils.scripts.model_base_utils import MsgspecBaseModel
from FluxPythonUtils.log_analyzer.log_analyzer_shm import LogAnalyzerSHM


class LogDetail(MsgspecBaseModel, kw_only=True):
    service: str
    log_file_path: str
    is_running: bool = True
    force_kill: bool = False
    critical: bool = False
    log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str]
    log_prefix_regex_pattern_to_log_date_time_regex_pattern: Dict[str, str] | None = None
    log_prefix_regex_pattern_to_log_source_patter_n_line_num_regex_pattern: Dict[str, str] | None = None
    log_file_path_is_regex: bool = False
    process: subprocess.Popen | None = None
    tail_executor_process: multiprocessing.context.SpawnProcess | None = None
    poll_timeout: float = 60.0   # seconds
    processed_timestamp: str | None = None
    data_snapshot_version: float | None = None


class TailExecutor(ABC):
    timestamp_regex_pattern: str = r'\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\b'
    max_str_size_in_bytes: int = 2048
    log_seperator: str = ';;;'

    def __init__(self, log_detail: LogDetail, regex_file_dir_path: str, config_yaml_dict: Dict,
                 log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str] | None = None):
        self.regex_file_dir_path: str = regex_file_dir_path

        regex_file_name = config_yaml_dict.get("regex_file_name")
        if regex_file_name is not None:
            self.regex_file: str = PurePath(self.regex_file_dir_path) / regex_file_name
        else:
            err_str = "Can't find key 'regex_file_name' in config dict - can't initiate tail executor"
            logging.error(err_str)
            raise Exception(err_str)

        regex_lock_file_name = config_yaml_dict.get("regex_lock_file_name")
        if regex_lock_file_name is not None:
            self.regex_lock_file: str = PurePath(self.regex_file_dir_path) / regex_lock_file_name
        else:
            err_str = "Can't find key 'regex_file_name' in config dict - can't initiate tail executor"
            logging.error(err_str)
            raise Exception(err_str)

        self.regex_file_data_snapshot_version: float | None = None
        self.regex_list: List[str] = list()
        self.is_running: bool = True

        self.config_yaml_dict: Dict = config_yaml_dict
        self.log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str] = \
            log_prefix_regex_pattern_to_callable_name_dict \
            if log_prefix_regex_pattern_to_callable_name_dict is not None else {}
        self.timeit_pattern: str = get_timeit_pattern()
        self.timeit_field_separator: str = get_timeit_field_separator()

        self.signal_handler_lock: Lock = Lock()
        self.log_refresh_threshold: int = 60
        self.log_details_queue: queue.Queue = queue.Queue()
        self.log_detail = log_detail
        self.component_file_path = log_detail.log_file_path
        self.log_analyzer_shm = LogAnalyzerSHM(self.get_process_name(log_detail))

        # running refresh_regex_list thread
        refresh_regex_list_thread = Thread(target=self.refresh_regex_list, daemon=True)
        refresh_regex_list_thread.start()

        self.tail_update_queue: queue.Queue = queue.Queue()

        self.terminate_triggered: bool = False
        signal.set_wakeup_fd(-1)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signal_type: int, *args) -> None:
        if not self.terminate_triggered:
            self.terminate_triggered = True
            self.log_detail.is_running = False
            logging.warning(f"{signal.Signals(signal_type).name} received. Gracefully terminating all subprocess.")
            self.is_running = False
            process = self.log_detail.process

            logging.warning(f"Killing tail subprocess: {process.pid}")
            os.killpg(process.pid, signal.SIGKILL)

            self.tail_update_queue.put("EXIT")

        # else not required: avoiding multiple terminate calls

    @staticmethod
    def clear_cache_file_path_queue_handler(clear_cache_file_path_queue: multiprocessing.Queue,
                                            pattern_matched_added_file_path_to_service_dict: Dict[str, str]) -> None:
        while True:
            log_file_path = clear_cache_file_path_queue.get()   # event based blocking call
            if log_file_path not in pattern_matched_added_file_path_to_service_dict:
                logging.info(
                    f"Can't find {log_file_path=} in log analyzer cache dict keys used to avoid repeated file "
                    f"tail executor start{TailExecutor.log_seperator}{pattern_matched_added_file_path_to_service_dict=}")
            else:
                pattern_matched_added_file_path_to_service_dict.pop(log_file_path)
                logging.debug(f"Removed cache for {log_file_path=}")

    @staticmethod
    def file_watcher_cache_key(log_detail: LogDetail):
        file_name = log_detail.log_file_path.split(os.sep)[-1]  # taking file name from file path
        return f"{log_detail.service}~{file_name.split('.')[0]}"

    @staticmethod
    def _handle_log_file_path_not_regex(log_details_queue: queue.Queue, log_detail: LogDetail) -> None:
        log_details_queue.put(log_detail)
        logging.info(f"putting log detail with {log_detail.log_file_path=} to init tail executor")


    @staticmethod
    def _handle_log_file_path_is_regex(log_detail_type: Type[LogDetail], log_details_queue: queue.Queue,
                                       log_detail: LogDetail, pattern_matched_file_path: str):
        new_log_detail = log_detail_type(**log_detail.to_dict())
        new_log_detail.log_file_path = pattern_matched_file_path
        log_details_queue.put(new_log_detail)
        logging.info(
            f"putting log detail with {new_log_detail.log_file_path=} to init tail executor")

    @classmethod
    def log_file_watcher(cls, log_details: List[LogDetail], log_details_queue: multiprocessing.Queue,
                         log_detail_type: Type[LogDetail], log_file_watcher_err_handler: Callable[..., Any],
                         clear_cache_file_path_queue: multiprocessing.Queue):
        if log_details is None or len(log_details) == 0:
            raise Exception(f"No log files provided for analysis{TailExecutor.log_seperator} "
                            f"log_details: {log_details}")

        # LOW todo: add file create date time handling + file removed handling - currently works since tail is
        #           started with -f which starts tail when new file with same name is created - only side effect
        #           is non-activity alerts will keep on generating

        # contains {"file_path": ["log_detail1", "log_detail2", ...]}
        # used to check if file has already running tail_executor with this log_detail - avoids duplicate tail_executor
        tail_executor_started_files_cache_dict: Dict[str, List[str]] = {}

        # separate thread to handle log analyzer server query based cache clear
        Thread(target=cls.clear_cache_file_path_queue_handler, args=(clear_cache_file_path_queue,
                                                                     tail_executor_started_files_cache_dict,),
               daemon=True).start()

        try:
            # pattern matched added files
            while True:
                for log_detail in log_details:
                    if not log_detail.log_file_path_is_regex:
                        if os.path.exists(log_detail.log_file_path):

                            if log_detail.log_file_path not in tail_executor_started_files_cache_dict:
                                TailExecutor._handle_log_file_path_not_regex(log_details_queue, log_detail)
                                # avoids any pattern matched file in regex case to get started again
                                tail_executor_started_files_cache_dict[log_detail.log_file_path] = [log_detail.service]
                                logging.debug(f"Creating static file path cache entry "
                                              f"for {log_detail.log_file_path=} with "
                                              f"{log_detail.service=}")
                            else:
                                log_detail_service_list: List[str] = (
                                    tail_executor_started_files_cache_dict.get(log_detail.log_file_path))
                                if log_detail.service not in log_detail_service_list:
                                    TailExecutor._handle_log_file_path_not_regex(log_details_queue, log_detail)
                                    # avoids any pattern matched file in regex case to get started again
                                    tail_executor_started_files_cache_dict[log_detail.log_file_path].append(
                                        log_detail.service)
                                    logging.debug(f"Adding static file path cache entry "
                                                  f"in {log_detail.log_file_path=} for "
                                                  f"{log_detail.service=}")
                                # else not required: avoiding duplicate tail executor
                    else:
                        pattern_matched_file_paths = glob.glob(log_detail.log_file_path)
                        for pattern_matched_file_path in pattern_matched_file_paths:
                            # avoiding recently added files with this log_detail object
                            if pattern_matched_file_path not in tail_executor_started_files_cache_dict:
                                TailExecutor._handle_log_file_path_is_regex(log_detail_type, log_details_queue,
                                                                            log_detail, pattern_matched_file_path)
                                tail_executor_started_files_cache_dict[pattern_matched_file_path] = [log_detail.service]
                                logging.debug(f"Creating dynamic file path cache entry "
                                              f"for {pattern_matched_file_path=} with "
                                              f"{log_detail.service=}")
                            else:
                                # handling multiple service for same log file
                                log_detail_service_list: List[str] = (
                                    tail_executor_started_files_cache_dict.get(pattern_matched_file_path))
                                if log_detail.service not in log_detail_service_list:
                                    TailExecutor._handle_log_file_path_is_regex(log_detail_type, log_details_queue,
                                                                                log_detail, pattern_matched_file_path)
                                    tail_executor_started_files_cache_dict[pattern_matched_file_path].append(
                                        log_detail.service)
                                    logging.debug(f"Adding dynamic file path cache entry "
                                                  f"in {pattern_matched_file_path=} for "
                                                  f"{log_detail.service=}")
                                # else not required: avoiding duplicate tail executor
                time.sleep(0.5)     # delay for while loop
        except Exception as e:
            err_brief = f"log_file_watcher failed with exception: {e}"
            logging.exception(err_brief)
            log_file_watcher_err_handler(err_brief, source_file_name=PurePath(__file__).name,
                                         line_num=inspect.currentframe().f_lineno,
                                         alert_create_date_time=DateTime.utcnow())

    @classmethod
    def run_single_tail_executor(cls, log_detail, **kwargs):
        # changing process name
        p_name = multiprocessing.current_process().name
        setproctitle.setproctitle(p_name)   # renames process

        log_analyzer_obj = cls(log_detail, **kwargs)
        log_analyzer_obj.listen()

    @staticmethod
    def get_process_name(log_detail: LogDetail) -> str:
        file_name = log_detail.log_file_path.split(os.sep)[-1]  # taking file name from file path
        return f"tail_executor~{log_detail.service}~{file_name.split('.')[0]}"

    @classmethod
    def dynamic_start_log_analyzer_for_log_details(
            cls, log_details_queue: multiprocessing.Queue,
            file_path_to_log_detail_cache_lock: Lock,
            file_path_to_log_detail_cache_dict: Dict[str, List[LogDetail]],
            spawn: multiprocessing.context.SpawnContext, start_datetime_fmt_str: str,
            file_path_to_log_analyzer_shm_obj_dict: Dict[str, LogAnalyzerSHM], **kwargs):
        while True:
            log_detail: LogDetail = log_details_queue.get()  # blocking call

            if log_detail == "EXIT":
                return

            # submitting new tail_executor for new file
            process_name = cls.get_process_name(log_detail)

            # if it is fresh start-up then since tail_executor can take some time to start it can miss some
            # initial logs so putting time of log analyzer service start which will always be older than tail_executor
            # start time
            if log_detail.processed_timestamp is None:
                log_detail.processed_timestamp = start_datetime_fmt_str
                # creating shm for this file before starting tail executor
                log_analyzer_shm = LogAnalyzerSHM(process_name, create=True)
                log_analyzer_shm.set(start_datetime_fmt_str)    # setting start timestamp
                file_path_to_log_analyzer_shm_obj_dict[log_detail.log_file_path] = log_analyzer_shm
            # else not required: taking value of processed_timestamp to restart set before passing log_detail to restart

            spawn_process = spawn.Process(target=cls.run_single_tail_executor, args=(log_detail,),
                                          kwargs=kwargs, daemon=True, name=process_name)
            spawn_process.start()
            logging.info(f"started tail executor for {log_detail.log_file_path}")
            log_detail.tail_executor_process = spawn_process

            with file_path_to_log_detail_cache_lock:
                log_detail_list = file_path_to_log_detail_cache_dict.get(log_detail.log_file_path)
                if log_detail_list is None:
                    file_path_to_log_detail_cache_dict[log_detail.log_file_path] = [log_detail]
                else:
                    log_detail_list.append(log_detail)

    def _load_regex_list(self) -> None:
        if os.path.exists(self.regex_file):
            with open(self.regex_file, "r") as f:
                self.regex_list = [line.strip() for line in f.readlines()]
        else:
            logging.warning(f"No regex file found at {self.regex_file}. Using empty regex list")
            self.regex_list = []

    def refresh_regex_list(self) -> None:
        """
        checks if the supress alert regex file is updated. If updated loads the updated regex list
        return True if regex file is modified
        return False if regex file is not present or not modified
        """
        regex_list_refresh_time_wait = self.config_yaml_dict.get("regex_list_refresh_time_wait")
        if regex_list_refresh_time_wait is None:
            regex_list_refresh_time_wait = 30
        while True:
            regex_list_updated: bool = False
            if os.path.exists(self.regex_file):
                is_modified, modified_time = is_file_modified(self.regex_file, self.regex_file_data_snapshot_version)
                if is_modified:
                    with FileLock(self.regex_lock_file):
                        # regex file updated. loading regex list
                        self._load_regex_list()
                        self.regex_file_data_snapshot_version = modified_time
                        regex_list_updated = True
                # else not required, regex file not updated. returning False
            elif len(self.regex_list) != 0:
                # regex file is deleted while script execution
                self.regex_list = []
                regex_list_updated = True
            # else not required if both regex file is not present and regex list is empty
            if regex_list_updated:
                logging.info(f"suppress alert regex list updated. regex_list: {self.regex_list}")
            time.sleep(regex_list_refresh_time_wait)

    def listen(self) -> None:
        logging.debug(f"called listen for file {self.log_detail.log_file_path} ...")
        thread: Thread = current_thread()
        thread.name = self.log_detail.service
        if self.log_detail.log_prefix_regex_pattern_to_callable_name_dict is None:
            self.log_detail.log_prefix_regex_pattern_to_callable_name_dict = \
                self.log_prefix_regex_pattern_to_callable_name_dict

        setattr(thread, "service_detail", self.log_detail)

        processed_timestamp = self.log_detail.processed_timestamp

        process, poll = self._run_tail_process_n_poll_register(self.log_detail, processed_timestamp)

        if process is not None:
            self.log_detail.is_running = True
            Thread(target=self.tail_poll_handler, args=(process, poll, self.log_detail,), daemon=True).start()
            res = self._analyze_log(self.log_detail)

            if res == 1:
                logging.warning(f"Internal restart triggered for {self.log_detail.log_file_path=}")
                threading.Thread(target=self.handle_tail_restart, args=(self.log_detail, ), daemon=True).start()
                time.sleep(2)   # wait for handle_tail_restart handling to complete in sep thread
            else:
                logging.warning(f"Gracefully stopping tail_executor for {self.log_detail.log_file_path=}")
        # else not required: Logging and Quiting if initiating process got some exception

    def _run_tail_process_n_poll_register(self, log_detail: LogDetail, restart_timestamp: str | None = None):
        grep_regex_pattern = r"\|".join([re_pattern_to_grep(regex_pattern) for regex_pattern in
                                        log_detail.log_prefix_regex_pattern_to_callable_name_dict.keys()])
        grep_regex_pattern += r"\|tail:"
        restart_line_no: str = "0"
        if restart_timestamp is not None:
            restart_line_no = get_log_line_no_from_timestamp(log_detail.log_file_path, restart_timestamp)
            if restart_line_no is None:
                # fetch recent 10 lines
                restart_line_no = "10"

        tail_args = ["tail", "-n", f"{restart_line_no}", "-F", f"{log_detail.log_file_path}", "|", "stdbuf", "-oL",
                     "grep", f"'{grep_regex_pattern}'"]

        cmd = " ".join(tail_args)
        logging.info(f"Running process for {cmd=}, {restart_timestamp=}, {restart_line_no=}")
        try:
            process: subprocess.Popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                                         preexec_fn=os.setpgrp, shell=True)

            logging.debug(f"Started tail process for log_file: {log_detail.log_file_path}, {process.pid=}")
            os.set_blocking(process.stdout.fileno(), False)    # makes stdout.readlines() non-blocking

            # add poll for process stdout for non-blocking tail of log file
            poll: select.poll = select.poll()
            poll.register(process.stdout, select.POLLIN | select.POLLERR | select.POLLOUT |
                                          select.POLLPRI | select.POLLHUP | select.POLLRDHUP | select.POLLNVAL)
        except Exception as e:
            logging.error(f"Something went wrong while initiating tail subprocess, exception: {e}")
            return None, None
        else:
            log_detail.process = process
            return process, poll

    def tail_poll_handler(self, process: subprocess.Popen, poll: select.poll, log_detail: LogDetail):
        timeout_ms = log_detail.poll_timeout * 1000  # poll.poll takes timeout time in milliseconds
        while log_detail.is_running:
            start_time = DateTime.utcnow()
            res = poll.poll(timeout_ms)  # file descriptor's event based blocking call
            end_time = DateTime.utcnow()
            delta = (end_time - start_time).total_seconds()

            if res:
                lines = process.stdout.readlines()

                for line in lines:
                    line = line.decode().strip()
                    if not line:
                        continue
                    self.tail_update_queue.put(line)

            else:
                # if critical service, periodically check if new logs are generated. if no new logs are
                # generated, send an alert
                if log_detail.critical and delta >= log_detail.poll_timeout:
                    self.notify_no_activity(log_detail)
                # else not required: service is not critical or poll_timeout is not breached - skipping periodic
                # check for new logs

    def _analyze_log(self, log_detail: LogDetail) -> int | None:
        source_file_name = PurePath(__file__).name
        while log_detail.is_running:
            try:
                line = self.tail_update_queue.get(timeout=log_detail.poll_timeout)

                # handling graceful shutdown
                if line == "EXIT":
                    return 0

                # tail file header
                if line.startswith("==>"):
                    continue

                timestamp_pattern = re.compile(TailExecutor.timestamp_regex_pattern)
                match = timestamp_pattern.search(line)
                if match:
                    timestamp = match.group(0)
                    log_detail.processed_timestamp = timestamp
                    self.log_analyzer_shm.set(timestamp)

                if line.startswith("tail:"):
                    if "giving up on this name" in line:
                        brief_msg_str: str = (f"tail error encountered in log service: {log_detail.service}, "
                                              f"restarting...")
                        logging.critical(f"{brief_msg_str}{TailExecutor.log_seperator}{line}")
                        self.notify_tail_event_in_log_service("warning", brief_msg_str, line, PurePath(__file__).name,
                                                              inspect.currentframe().f_lineno, DateTime.utcnow())
                        return 1
                    elif "has appeared;  following new file" in line:   # tail reconnected notify
                        brief_msg_str: str = f"tail reconnected to {log_detail.log_file_path=}"
                        logging.critical(f"{brief_msg_str}{TailExecutor.log_seperator}{line}")
                        self.notify_tail_event_in_log_service("warning", brief_msg_str, line, PurePath(__file__).name,
                                                              inspect.currentframe().f_lineno, DateTime.utcnow())
                    logging.warning(line)
                    continue

                for log_prefix_regex_pattern, callable_name in (
                        log_detail.log_prefix_regex_pattern_to_callable_name_dict.items()):
                    # ignore processing log line that not matches log_prefix_regex_pattern
                    if not re.compile(log_prefix_regex_pattern).search(line):
                        continue

                    log_prefix: str | None
                    log_message: str | None
                    log_date_time_regex_pattern: str | None = None
                    if log_detail.log_prefix_regex_pattern_to_log_date_time_regex_pattern is not None:
                        log_date_time_regex_pattern = (
                            log_detail.log_prefix_regex_pattern_to_log_date_time_regex_pattern.get(
                                log_prefix_regex_pattern))
                    log_source_patter_n_line_num_regex_pattern: str | None = None
                    if log_detail.log_prefix_regex_pattern_to_log_source_patter_n_line_num_regex_pattern is not None:
                        log_source_patter_n_line_num_regex_pattern = (
                            log_detail.log_prefix_regex_pattern_to_log_source_patter_n_line_num_regex_pattern.get(
                                log_prefix_regex_pattern))
                    log_prefix, log_message, log_date_time, log_source_file_name, line_num = \
                        self._get_log_prefix_n_message_n_log_data(
                            log_line=line, log_prefix_pattern=log_prefix_regex_pattern,
                            log_date_time_regex_pattern=log_date_time_regex_pattern,
                            log_source_patter_n_line_num_regex_pattern=log_source_patter_n_line_num_regex_pattern)

                    # error already logged. continue processing next line
                    if not log_prefix or not log_message:
                        continue

                    # reducing the size of log_message brief if exceeds limit before
                    # going to check skil patterns
                    log_seperator_index = log_message.find(TailExecutor.log_seperator)
                    if log_seperator_index != -1:
                        log_msg_brief = log_message[:log_seperator_index]
                    else:
                        # if log_seperator is found in log then taking whole log_message as strat_brief
                        log_msg_brief = log_message

                    if self._is_str_limit_breached(log_msg_brief):
                        log_msg_brief = log_msg_brief[:TailExecutor.max_str_size_in_bytes]

                        if log_seperator_index != -1:
                            err_str = ("Log string brief is too long, adjusting the string length for "
                                       f"optimization reasons - please reduce the size of log brief, "
                                       f"adjusted log brief: {log_msg_brief}")
                        else:
                            err_str = ("Log string doesn't contain log seperator to slice msg brief from it "
                                       "and whole log string is too long, adjusting the string length for "
                                       f"optimization reasons - please use log_seperator: "
                                       f"{TailExecutor.log_seperator} to specify brief and detail in log, "
                                       f"adjusted log brief: {log_msg_brief}")

                        self.notify_error(err_str, source_file_name, inspect.currentframe().f_lineno, DateTime.utcnow())

                    regex_match: bool = False
                    for regex_pattern in self.regex_list:
                        try:
                            if re.compile(fr"{regex_pattern}").search(log_msg_brief):
                                logging.info(f"regex pattern matched, skipping{TailExecutor.log_seperator} "
                                             f"log_message: {log_msg_brief[:200]}")
                                regex_match = True
                                break
                        except re.error as e:
                            err_str_ = (f"Failed to compile regex pattern, pattern: {regex_pattern}, "
                                        f"exception: {e}")
                            logging.exception(err_str_)
                            self.notify_error(err_str_, source_file_name, inspect.currentframe().f_lineno,
                                              DateTime.utcnow())
                    # ignore processing the log line that matches the regex list
                    if regex_match:
                        break

                    match_callable: Callable[[...], ...] | None = None
                    try:
                        match_callable: Callable = getattr(self, callable_name)
                    except AttributeError as e:
                        err_str_ = (f"Couldn't find callable {callable_name} in inherited log_analyzer, "
                                    f"exception: {e}, inheriting log_analyzer name: "
                                    f"{self.__class__.__name__}")
                        logging.exception(err_str_)
                        self.notify_error(err_str_, source_file_name, inspect.currentframe().f_lineno,
                                          DateTime.utcnow())
                    if match_callable:
                        match_callable(log_prefix, log_message, log_detail,
                                       log_date_time, log_source_file_name, line_num)

            except queue.Empty:
                logging.info(f"No Data found for last {log_detail.poll_timeout} secs in _analyze_log")
            except Exception as e:
                err_str_ = f"_analyze_log failed{TailExecutor.log_seperator} exception: {e}"
                logging.exception(err_str_)
                self.notify_error(err_str_, source_file_name, inspect.currentframe().f_lineno, DateTime.utcnow())

    def _is_str_limit_breached(self, text: str) -> bool:
        if len(text.encode("utf-8")) > TailExecutor.max_str_size_in_bytes:
            return True
        return False

    def _truncate_str(self, text: str) -> str:
        if self._is_str_limit_breached(text):
            text = text.encode("utf-8")[:TailExecutor.max_str_size_in_bytes].decode()
            service_detail: LogDetail = getattr(current_thread(), "service_detail")
            text += f"...check the file: {service_detail.log_file_path} to see the entire log"
        return text

    def _get_log_prefix_n_message_n_log_data(
            self, log_line: str, log_prefix_pattern: str,
            log_date_time_regex_pattern: str | None = None,
            log_source_patter_n_line_num_regex_pattern: str | None = None) -> (Tuple[str, str, DateTime, str, int] |
                                                                               Tuple[None, None, None, None, None]):
        pattern: re.Pattern = re.compile(log_prefix_pattern)
        match = pattern.search(log_line)
        if not match:
            # logging.error(f"_get_log_prefix_n_message failed. Failed to find match for {log_prefix_pattern=} "
            #               f"in {log_line=}")
            return None, None, None, None, None

        log_prefix: str = match.group(0)
        log_message: str = log_line.replace(log_prefix, "").strip()

        log_date_time = None
        if log_date_time_regex_pattern:
            try:
                match = re.search(log_date_time_regex_pattern, log_line)
                if match:
                    timestamp = match.group(1)
                    # logs are stored in local datetime - converting to UTC
                    log_date_time = pendulum.parse(timestamp, tz=pendulum.local_timezone()).in_tz('UTC')
                else:
                    logging.error(f"Can't find match for {log_date_time_regex_pattern=} in {log_line=}")
            except Exception as e:
                err_str_ = (f"Something went wrong while searching log date_time using regex pattern, "
                            f"{log_date_time_regex_pattern=};;; {log_line=}, exception: {e}")
                logging.exception(err_str_)

        source_file_name: str | None = None
        line_num: int | None = None
        if log_source_patter_n_line_num_regex_pattern:
            try:
                match = re.search(log_source_patter_n_line_num_regex_pattern, log_line)
                if match:
                    source_file_name = match.group(1).strip()

                    source_file_name_sep = source_file_name.split(os.sep)
                    # if source file name is instead complete path then fetching only source file name from it
                    if len(source_file_name_sep) > 1:
                        source_file_name = source_file_name_sep[-1]
                    line_num = parse_to_int(match.group(2), raise_exception=False)
                else:
                    logging.error(f"Can't find match for {log_source_patter_n_line_num_regex_pattern=} in {log_line=}")
            except Exception as e:
                err_str_ = (f"Something went wrong while searching source_file_name and log line using regex pattern, "
                            f"{log_source_patter_n_line_num_regex_pattern=};;; {log_line=}, exception: {e}")
                logging.exception(err_str_)

        return log_prefix, log_message, log_date_time, source_file_name, line_num

    @staticmethod
    def _get_restart_datetime_from_log_detail(last_processed_utc_datetime: DateTime):
        restart_datetime: str = last_processed_utc_datetime.format("YYYY-MM-DD HH:mm:ss,SSS")
        return restart_datetime

    @abstractmethod
    def notify_no_activity(self, log_detail: LogDetail):
        """
        Handling to be implemented to notify in derived class when no activity is
        found in log file for `self.log_refresh_threshold` seconds
        :param log_detail: Object of LogDetail at the time of no activity is found
        :return: None
        """
        raise NotImplementedError("notify_no_activity not implemented in derived class")

    @abstractmethod
    def notify_unexpected_activity(self, log_detail: LogDetail):
        """
        Handling to be implemented to notify in derived class when there is activity in the log file but not expected
        :param log_detail: Object of LogDetail at the time of unexpected activity
        :return:
        """
        raise NotImplementedError("notify_unexpected_activity not implemented in derived class")

    @abstractmethod
    def notify_tail_event_in_log_service(self, severity: str, brief_msg_str: str, detail_msg_str: str,
                                         source_file_name: str, line_num: int,
                                         alert_create_date_time: DateTime):
        """
        Handling to be implemented to notify in derived class when tail encounters as error in base class
        :param severity: severity string
        :param brief_msg_str: brief msg sent by base regarding error
        :param detail_msg_str: detailed msg sent by base regarding error
        :param source_file_name: file name contained error
        :param line_num: line number at which error occurred
        :param alert_create_date_time: date_time of error
        :return: None
        """
        raise NotImplementedError("notify_tail_error_in_log_service not implemented in derived class")

    @abstractmethod
    def notify_error(self, error_msg: str, source_name: str, line_num: int, log_create_date_time: DateTime):
        """
        Handling to be implemented to notify in derived class when some error occurred in base log_analyzer class
        :param error_msg: error msg to be notified by derived implementation
        :param source_name: source file where error occurred
        :param line_num: line num on which error occurred
        :param log_create_date_time: date_time at which error occurred
        :return: None
        """
        raise NotImplementedError("notify_error not implemented in derived class")

    @abstractmethod
    def handle_tail_restart(self, log_detail: LogDetail):
        """
        Handling to be implemented to restart tail for same file
        :return:
        """
        raise NotImplementedError("handle_tail_restart not implemented in derived class")
