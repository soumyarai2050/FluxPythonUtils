# standard imports
import datetime
import logging
import os
import sys
import time
from abc import ABC, abstractmethod
import re
import subprocess
from typing import Dict, List, Callable, Type, Any
from threading import Thread, current_thread, Lock
import signal
import select
import glob
import queue
from pathlib import PurePath

# 3rd part imports
from pydantic import BaseModel, ConfigDict
from pendulum import DateTime, parse

# project imports
from FluxPythonUtils.scripts.utility_functions import (parse_to_float, parse_to_int, get_timeit_pattern,
                                                       get_timeit_field_separator)

perf_benchmark_log_prefix_regex_pattern: str = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} : (" \
                                               r"TIMING) : \[[a-zA-Z._]* : \d*] : "


def get_transaction_counts_n_timeout_from_config(config_yaml_dict: Dict | None,
                                                 default_transaction_counts: int = 1,
                                                 default_transaction_timeout_secs: int = 2):
    if not config_yaml_dict:
        transaction_counts_per_call = default_transaction_counts
        transaction_timeout_secs = default_transaction_timeout_secs
    else:
        if (transaction_counts_per_call := config_yaml_dict.get("transaction_counts_per_call")) is None:
            transaction_counts_per_call = default_transaction_counts
        if (transaction_timeout_secs := config_yaml_dict.get("transaction_timeout_secs")) is None:
            transaction_timeout_secs = default_transaction_timeout_secs
    return transaction_counts_per_call, transaction_timeout_secs


class LogDetail(BaseModel):
    service: str
    log_file_path: str
    is_running: bool = True
    force_kill: bool = False
    critical: bool = False
    log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str] | None
    log_file_path_is_regex: bool = False
    process: subprocess.Popen | None = None
    poll_timeout: float = 60.0   # seconds
    processed_timestamp: str | None = None
    last_processed_utc_datetime: DateTime = DateTime.utcnow()
    # required to use WebSocket as field type since it is arbitrary type
    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)


class LogAnalyzer(ABC):
    timestamp_regex_pattern: str = r'\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\b'
    max_str_size_in_bytes: int = 2048
    log_seperator: str = ';;;'

    def __init__(self, regex_file: str, config_yaml_dict: Dict, performance_benchmark_webclient_object,
                 raw_performance_data_model_type: Type[BaseModel],
                 log_details: List[LogDetail] | None = None,
                 log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str] | None = None,
                 log_detail_type: Type[LogDetail] | None = None):
        self.regex_file: str = regex_file
        self.regex_file_lock: Lock = Lock()
        self.regex_file_data_snapshot_version: float | None = None
        self.regex_list: List[str] = list()
        self.log_detail_type = log_detail_type
        if log_detail_type is None:
            self.log_detail_type: Type[LogDetail] = LogDetail

        self.config_yaml_dict: Dict = config_yaml_dict
        self.webclient_object = performance_benchmark_webclient_object
        self.raw_performance_data_model_type = raw_performance_data_model_type
        self.log_details: List[LogDetail] | None = log_details if log_details is not None else []
        self.non_existing_log_details: List[LogDetail] = []
        self.log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str] = \
            log_prefix_regex_pattern_to_callable_name_dict \
            if log_prefix_regex_pattern_to_callable_name_dict is not None else {}
        self.timeit_pattern: str = get_timeit_pattern()
        self.timeit_field_separator: str = get_timeit_field_separator()

        self.log_file_path_to_log_detail_dict: Dict[str, LogDetail] = {}
        self.signal_handler_lock: Lock = Lock()
        self.log_refresh_threshold: int = 60
        self.log_details_queue: queue.Queue = queue.Queue()
        self.running_log_analyzer_thread_list: List[Thread] = []
        self.running_pattern_to_file_name_dict: Dict[str, str] = {}
        self.raw_performance_data_queue: queue.Queue = queue.Queue()
        self.pattern_matched_added_file_path_to_service_dict: Dict[str, str] = {}
        # signal.signal(signal.SIGINT, self._signal_handler)
        # signal.signal(signal.SIGTERM, self._signal_handler)

        # running thread to load log_analyzers dynamically based on entries in log_details_queue
        dynamic_start_log_analyzer_for_log_details_thread: Thread = (
            Thread(target=self.dynamic_start_log_analyzer_for_log_details, daemon=True))
        dynamic_start_log_analyzer_for_log_details_thread.start()

        # running raw_performance thread
        raw_performance_handler_thread = Thread(target=self._handle_raw_performance_data_queue, daemon=True)
        raw_performance_handler_thread.start()

    @staticmethod
    def queue_handler(queue_obj: queue.Queue, bulk_transactions_counts_per_call: int,
                      bulk_transaction_timeout: int, web_client_callable, err_handling_callable,
                      client_connection_fail_retry_secs: int | None = None):
        pydantic_obj_list = []
        oldest_entry_time: DateTime = DateTime.utcnow()
        while True:
            if not pydantic_obj_list:
                remaining_timeout_secs = bulk_transaction_timeout
            else:
                remaining_timeout_secs = (
                        bulk_transaction_timeout - (DateTime.utcnow() - oldest_entry_time).total_seconds())

            if not remaining_timeout_secs < 1:
                try:
                    pydantic_obj = queue_obj.get(timeout=remaining_timeout_secs)  # timeout based blocking call
                    pydantic_obj_list.append(pydantic_obj)
                except queue.Empty:
                    # since bulk update timeout limit has breached, will call update
                    pass
                else:
                    if len(pydantic_obj_list) < bulk_transactions_counts_per_call:
                        continue
                    # else, since bulk update count limit has breached, will call update
            # since bulk update remaining timeout limit <= 0, will call update

            if pydantic_obj_list:
                try:
                    web_client_callable(pydantic_obj_list)
                except Exception as e:
                    # Handling patch-all race-condition if some obj got removed before getting updated due to wait

                    pattern = "'.*objects with ids: {(.*)} out of requested .*'"
                    match_list: List[str] = re.findall(pattern, str(e))
                    if match_list:
                        # taking first occurrence
                        non_existing_id_list: List[int] = [parse_to_int(_id.strip())
                                                           for _id in match_list[0].split(",")]
                        non_existing_obj = []
                        for pydantic_obj in pydantic_obj_list:
                            if pydantic_obj.get("_id") in non_existing_id_list:
                                non_existing_obj.append(pydantic_obj)
                            else:
                                queue_obj.put(pydantic_obj)     # putting back all other existing jsons
                        logging.debug(f"Calling Error handler func provided with param: {non_existing_obj}")
                        err_handling_callable(non_existing_obj)
                    elif "Failed to establish a new connection: [Errno 111] Connection refused" in str(e):
                        logging.exception(
                            f"Connection Error occurred while calling {web_client_callable.__name__}, "
                            f"will stay on wait for 5 secs and again retry - ignoring all data for this call")

                        if client_connection_fail_retry_secs is None:
                            client_connection_fail_retry_secs = 5*60    # 5 minutes
                        time.sleep(client_connection_fail_retry_secs)
                    else:
                        logging.exception(
                            f"Some Error Occurred while calling {web_client_callable.__name__}, "
                            f"sending all updates to err_handling_callable, {str(e)}")
                        err_handling_callable(pydantic_obj_list)
                pydantic_obj_list.clear()  # cleaning list to start fresh cycle
            oldest_entry_time = DateTime.utcnow()
            # else not required since even after timeout no data found

    def run(self):
        if self.log_details is None or len(self.log_details) == 0:
            raise Exception(f"No log files provided for analysis{LogAnalyzer.log_seperator} "
                            f"log_details: {self.log_details}")

        # adding all log_details to non_existing_log_details list and keep removing those found
        # existing dynamically
        for log_detail in self.log_details:
            self.non_existing_log_details.append(log_detail)

        # pattern matched added files
        while True:
            for log_detail in self.non_existing_log_details:
                if not log_detail.log_file_path_is_regex:
                    if os.path.exists(log_detail.log_file_path):
                        self.log_details_queue.put(log_detail)

                        # updating list of tuple of pattern and log_file_path
                        for pattern, _ in log_detail.log_prefix_regex_pattern_to_callable_name_dict.items():
                            self.running_pattern_to_file_name_dict[f"{log_detail.service}~{pattern}"] = (
                                log_detail.log_file_path)
                        self.non_existing_log_details.remove(log_detail)
                else:
                    pattern_matched_file_paths = glob.glob(log_detail.log_file_path)
                    for pattern_matched_file_path in pattern_matched_file_paths:
                        # avoiding recently added files with this log_detail object
                        if pattern_matched_file_path in self.pattern_matched_added_file_path_to_service_dict:
                            service: str = (
                                self.pattern_matched_added_file_path_to_service_dict)[pattern_matched_file_path]
                            if service != log_detail.service:
                                err_str_ = ("Dropping This LogDetail: Found log_analyzer init request for "
                                            "unix-pattern matched file having same service and file_name already "
                                            f"present in running log_analyzer's cache {LogAnalyzer.log_seperator} "
                                            f"log_detail: {log_detail}, "
                                            f"pattern_matched_file_path: {pattern_matched_file_path}")
                                logging.error(err_str_)
                                self.notify_error(err_str_)
                            # else not required: would be a file which got tracked by glob again due to pattern
                            continue

                        new_log_detail = self.log_detail_type(**log_detail.model_dump())
                        new_log_detail.log_file_path = pattern_matched_file_path
                        self.log_details_queue.put(new_log_detail)

                        self.pattern_matched_added_file_path_to_service_dict[pattern_matched_file_path] = (
                            log_detail.service)

                        # updating list of tuple of pattern and log_file_path
                        for pattern, _ in log_detail.log_prefix_regex_pattern_to_callable_name_dict.items():
                            self.running_pattern_to_file_name_dict[f"{log_detail.service}~{pattern}"] = (
                                pattern_matched_file_path)
                        # else not required: if file_path already exists in existing_log_details that means file
                        # is already in run
            time.sleep(0.5)     # delay for while loop

    def dynamic_start_log_analyzer_for_log_details(self):
        while True:
            log_detail = self.log_details_queue.get()   # blocking call
            new_thread = Thread(target=self._listen, args=(log_detail,), daemon=True)
            self.running_log_analyzer_thread_list.append(new_thread)
            new_thread.start()

    def handle_raw_performance_data_queue_err_handler(self, pydantic_obj_list):
        pass

    def _handle_raw_performance_data_queue(self):
        raw_performance_data_bulk_create_counts_per_call, raw_perf_data_bulk_create_timeout = (
            get_transaction_counts_n_timeout_from_config(self.config_yaml_dict.get("raw_perf_data_config")))
        LogAnalyzer.queue_handler(
            self.raw_performance_data_queue, raw_performance_data_bulk_create_counts_per_call,
            raw_perf_data_bulk_create_timeout,
            self.webclient_object.create_all_raw_performance_data_client,
            self.handle_raw_performance_data_queue_err_handler)

    def _load_regex_list(self) -> None:
        if os.path.exists(self.regex_file):
            with open(self.regex_file, "r") as f:
                self.regex_list = [line.strip() for line in f.readlines()]
        else:
            logging.warning(f"No regex file found at {self.regex_file}. Using empty regex list")
            self.regex_list = []

    def refresh_regex_list(self) -> bool:
        """
        checks if the suppress alert regex file is updated. If updated loads the updated regex list
        return True if regex file is modified
        return False if regex file is not present or not modified
        """
        with self.regex_file_lock:
            if os.path.exists(self.regex_file):
                modified_time: float = os.path.getmtime(self.regex_file)
                if modified_time != self.regex_file_data_snapshot_version:
                    # regex file updated. loading regex list
                    self._load_regex_list()
                    self.regex_file_data_snapshot_version = modified_time
                    return True
                # else not required, regex file not updated. returning False
            elif len(self.regex_list) != 0:
                # regex file is deleted while script execution
                self.regex_list = []
                return True
            # else not required if both regex file is not present and regex list is empty. returning False
            return False

    def _listen(self, log_detail: LogDetail) -> None:
        thread: Thread = current_thread()
        thread.name = log_detail.service
        if log_detail.log_prefix_regex_pattern_to_callable_name_dict is None:
            log_detail.log_prefix_regex_pattern_to_callable_name_dict = \
                self.log_prefix_regex_pattern_to_callable_name_dict

        # adding performance benchmark regex_pattern and handler callable
        log_detail.log_prefix_regex_pattern_to_callable_name_dict[perf_benchmark_log_prefix_regex_pattern] = \
            "handle_perf_benchmark_matched_log_message"

        setattr(thread, "service_detail", log_detail)

        processed_timestamp = log_detail.processed_timestamp

        process, poll = self._run_tail_process_n_poll_register(log_detail, processed_timestamp)
        log_detail.is_running = True
        self._analyze_log(process, poll, log_detail)
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        process.wait()

        if not log_detail.force_kill:
            logging.info(f"Restarting tail process for log_file: {log_detail.log_file_path}")
            self.log_details_queue.put(log_detail)
        else:
            log_detail.force_kill = False   # reverting force_kill to False
            logging.info(f"Force Killing log_detail for file: {log_detail.log_file_path}")

    def _run_tail_process_n_poll_register(self, log_detail: LogDetail, restart_timestamp: str | None = None):
        tail_log_file_path = PurePath(__file__).parent.parent / "scripts" / "tail_logs.sh"
        tail_args = [tail_log_file_path, log_detail.log_file_path]
        if restart_timestamp is not None:
            tail_args.append(restart_timestamp)
        process: subprocess.Popen = subprocess.Popen(tail_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                                     preexec_fn=os.setpgrp)
        logging.debug(f"Started tail process for log_file: {log_detail.log_file_path}")
        os.set_blocking(process.stdout.fileno(), False)    # makes stdout.readlines() non-blocking
        # add poll for process stdout for non-blocking tail of log file
        poll: select.poll = select.poll()
        poll.register(process.stdout)

        log_detail.process = process
        self.log_file_path_to_log_detail_dict[log_detail.log_file_path] = log_detail
        return process, poll

    def _analyze_log(self, process: subprocess.Popen, poll: select.poll, log_detail: LogDetail) -> None:
        while log_detail.is_running:
            try:
                start_time = DateTime.utcnow()
                timeout_ms = log_detail.poll_timeout * 1000     # poll.poll takes timeout time in milliseconds
                res = poll.poll(timeout_ms)    # file descriptor's event based blocking call
                end_time = DateTime.utcnow()
                delta = (end_time - start_time).total_seconds()
                if res:
                    lines = process.stdout.readlines()

                    for line in lines:
                        line = line.decode().strip()
                        if not line:
                            continue

                        if line.startswith("tail"):
                            logging.warning(line)

                        log_detail: LogDetail = self.log_file_path_to_log_detail_dict.get(log_detail.log_file_path)

                        timestamp_pattern = re.compile(LogAnalyzer.timestamp_regex_pattern)
                        match = timestamp_pattern.search(line)
                        if match:
                            timestamp = match.group(0)
                            log_detail.processed_timestamp = timestamp

                        if "tail:" in line and "giving up on this name" in line:
                            brief_msg_str: str = (f"tail error encountered in log service: {log_detail.service}, "
                                                  f"restarting...")
                            logging.critical(f"{brief_msg_str}{LogAnalyzer.log_seperator}{line}")
                            self.notify_tail_error_in_log_service(brief_msg_str, line)
                            return

                        if self.refresh_regex_list():
                            logging.info(f"suppress alert regex list updated. regex_list: {self.regex_list}")

                        last_update_date_time = DateTime.utcnow()
                        for log_prefix_regex_pattern, callable_name in (
                                log_detail.log_prefix_regex_pattern_to_callable_name_dict.items()):
                            # ignore processing log line that not matches log_prefix_regex_pattern
                            if not re.compile(log_prefix_regex_pattern).search(line):
                                continue

                            log_prefix: str
                            log_message: str
                            log_prefix, log_message = \
                                self._get_log_prefix_n_message(log_line=line,
                                                               log_prefix_pattern=log_prefix_regex_pattern)

                            # reducing the size of log_message brief if exceeds limit before
                            # going to check skil patterns
                            log_seperator_index = log_message.find(LogAnalyzer.log_seperator)
                            if log_seperator_index != -1:
                                log_msg_brief = log_message[:log_seperator_index]
                            else:
                                # if log_seperator is found in log then taking whole log_message as strat_brief
                                log_msg_brief = log_message

                            if self._is_str_limit_breached(log_msg_brief):
                                log_msg_brief = log_msg_brief[:LogAnalyzer.max_str_size_in_bytes]

                                if log_seperator_index != -1:
                                    err_str = ("Log string brief is too long, adjusting the string length for "
                                               f"optimization reasons - please reduce the size of log brief, "
                                               f"adjusted log brief: {log_msg_brief}")
                                else:
                                    err_str = ("Log string doesn't contain log seperator to slice msg brief from it "
                                               "and whole log string is too long, adjusting the string length for "
                                               f"optimization reasons - please use log_seperator: "
                                               f"{LogAnalyzer.log_seperator} to specify brief and detail in log, "
                                               f"adjusted log brief: {log_msg_brief}")

                                self.notify_error(err_str)

                            regex_match: bool = False
                            for regex_pattern in self.regex_list:
                                try:
                                    if re.compile(fr"{regex_pattern}").search(log_msg_brief):
                                        logging.info(f"regex pattern matched, skipping{LogAnalyzer.log_seperator} "
                                                     f"log_message: {log_msg_brief[:200]}")
                                        regex_match = True
                                        break
                                except re.error as e:
                                    err_str_ = (f"Failed to compile regex pattern, pattern: {regex_pattern}, "
                                                f"exception: {e}")
                                    logging.exception(err_str_)
                                    self.notify_error(err_str_)
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
                                self.notify_error(err_str_)
                            if match_callable:
                                match_callable(log_prefix, log_message, log_detail)

                            # updating last_processed_utc_datetime for this log_detail
                            log_detail.last_processed_utc_datetime = DateTime.utcnow()
                else:
                    # if critical service, periodically check if new logs are generated. if no new logs are
                    # generated, send an alert
                    if log_detail.critical and delta >= log_detail.poll_timeout:
                        self.notify_no_activity(log_detail)
                    # else not required: service is not critical or poll_timeout is not breached - skipping periodic
                    # check for new logs

            except Exception as e:
                err_str_ = f"_analyze_log failed{LogAnalyzer.log_seperator} exception: {e}"
                logging.exception(err_str_)
                self.notify_error(err_str_)

    def _is_str_limit_breached(self, text: str) -> bool:
        if len(text.encode("utf-8")) > LogAnalyzer.max_str_size_in_bytes:
            return True
        return False

    def _truncate_str(self, text: str) -> str:
        if self._is_str_limit_breached(text):
            text = text.encode("utf-8")[:LogAnalyzer.max_str_size_in_bytes].decode()
            service_detail: LogDetail = getattr(current_thread(), "service_detail")
            text += f"...check the file: {service_detail.log_file_path} to see the entire log"
        return text

    def _get_log_prefix_n_message(self, log_line: str, log_prefix_pattern: str) -> List[str]:
        pattern: re.Pattern = re.compile(log_prefix_pattern)
        log_prefix: str = pattern.search(log_line)[0]
        log_message_without_prefix: str = log_line.replace(log_prefix, "").strip()
        return [log_prefix, log_message_without_prefix]

    def handle_perf_benchmark_matched_log_message(self, log_prefix: str, log_message: str, log_detail: LogDetail):
        pattern = re.compile(f"{self.timeit_pattern}.*{self.timeit_pattern}")
        if search_obj := re.search(pattern, log_message):
            found_pattern = search_obj.group()
            found_pattern = found_pattern[8:-8]  # removing beginning and ending _timeit_
            found_pattern_list = found_pattern.split(self.timeit_field_separator)  # splitting pattern values
            if len(found_pattern_list) == 3:
                callable_name, start_time, delta = found_pattern_list
                if callable_name != "underlying_create_raw_performance_data_http":
                    raw_performance_data_obj = self.raw_performance_data_model_type()
                    raw_performance_data_obj.callable_name = callable_name
                    raw_performance_data_obj.start_time = parse(start_time)
                    raw_performance_data_obj.delta = parse_to_float(delta)

                    self.raw_performance_data_queue.put(raw_performance_data_obj)
                    logging.debug(f"Created raw_performance_data entry in queue for callable {callable_name} "
                                  f"with start_datetime {start_time}")
                # else not required: avoiding callable underlying_create_raw_performance_data to avoid infinite loop
            else:
                err_str_: str = f"Found timeit pattern but internally only contains {found_pattern_list}, " \
                                f"ideally must contain callable_name, start_time & delta " \
                                f"seperated by '~'"
                logging.exception(err_str_)
        # else not required: if no pattern is matched ignoring this log_message

    @abstractmethod
    def notify_no_activity(self, log_detail: LogDetail):
        """
        Handling to be implemented to notify in derived class when no activity is
        found in log file for `self.log_refresh_threshold` seconds
        :param log_detail: Object of LogDetail at the time of no activity is found
        :return: None
        """
        raise NotImplementedError("handle_no_activity not implemented in derived class")

    @abstractmethod
    def notify_tail_error_in_log_service(self, brief_msg_str: str, detail_msg_str: str):
        """
        Handling to be implemented to notify in derived class when tail encounters as error in base class
        :param brief_msg_str: brief msg sent by base regarding error
        :param detail_msg_str: detailed msg sent by base regarding error
        :return: None
        """
        raise NotImplementedError("notify_tail_error_in_log_service not implemented in derived class")

    @abstractmethod
    def notify_error(self, error_msg: str):
        """
        Handling to be implemented to notify in derived class when some error occurred in base log_analyzer class
        :param error_msg: error msg to be notified by derived implementation
        :return: None
        """
        raise NotImplementedError("notify_error not implemented in derived class")
