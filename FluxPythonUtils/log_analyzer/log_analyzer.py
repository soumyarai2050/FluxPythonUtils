# standard imports
import logging
import os
import sys
import time
from abc import ABC, abstractmethod
import re
import subprocess
from typing import Dict, List, Callable, Type
from threading import Thread, current_thread, Lock
import signal
import select
import glob
import queue

# 3rd part imports
from pydantic import BaseModel
from pendulum import DateTime, parse

# project imports
from FluxPythonUtils.scripts.utility_functions import parse_to_float, get_native_host_n_port_from_config_dict

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
    critical: bool = False
    log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str] | None
    log_file_path_is_regex: bool = False


class LogAnalyzer(ABC):

    def __init__(self, regex_file: str, config_yaml_dict: Dict, webclient_object,
                 raw_performance_data_model_type: Type[BaseModel],
                 log_details: List[LogDetail] | None = None,
                 log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str] | None = None):
        self.regex_file: str = regex_file
        self.regex_file_lock: Lock = Lock()
        self.regex_file_data_snapshot_version: float | None = None
        self.regex_list: List[str] = list()

        self.config_yaml_dict: Dict = config_yaml_dict
        self.webclient_object = webclient_object
        self.raw_performance_data_model_type = raw_performance_data_model_type
        self.log_details: List[LogDetail] | None = log_details if log_details is not None else []
        self.non_existing_log_details: List[LogDetail] = []
        self.log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str] = \
            log_prefix_regex_pattern_to_callable_name_dict \
            if log_prefix_regex_pattern_to_callable_name_dict is not None else {}

        self.process_list: List[subprocess.Popen] = []
        self.run_mode: bool = True
        self.signal_handler_lock: Lock = Lock()
        self.log_refresh_threshold: int = 60
        self.log_details_queue: queue.Queue = queue.Queue()
        self.running_log_analyzer_thread_list: List[Thread] = []
        self.running_pattern_to_file_name_dict: Dict[str, str] = {}
        self.raw_performance_data_queue: queue.Queue = queue.Queue()
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # running thread to load log_analyzers dynamically based on entries in log_details_queue
        dynamic_start_log_analyzer_for_log_details_thread: Thread = (
            Thread(target=self.dynamic_start_log_analyzer_for_log_details, daemon=True))
        dynamic_start_log_analyzer_for_log_details_thread.start()

        # running raw_performance thread
        raw_performance_handler_thread = Thread(target=self._handle_raw_performance_data_queue, daemon=True)
        raw_performance_handler_thread.start()

    @staticmethod
    def queue_handler(queue_obj: queue.Queue, bulk_transactions_counts_per_call: int,
                      bulk_transaction_timeout: int, web_client_callable):
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
                web_client_callable(pydantic_obj_list)
                pydantic_obj_list.clear()  # cleaning list to start fresh cycle
            oldest_entry_time = DateTime.utcnow()
            # else not required since even after timeout no data found

    def run(self):
        if self.log_details is None or len(self.log_details) == 0:
            raise Exception(f"No log files provided for analysis;;; log_details: {self.log_details}")

        # adding all log_details to non_existing_log_details list and keep removing those found
        # existing dynamically
        for log_detail in self.log_details:
            self.non_existing_log_details.append(log_detail)

        # pattern matched added files
        pattern_matched_added_file_path_to_service_dict: Dict[str, str] = {}
        while self.run_mode:
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
                        if pattern_matched_file_path in pattern_matched_added_file_path_to_service_dict:
                            service: str = pattern_matched_added_file_path_to_service_dict[pattern_matched_file_path]
                            if service != log_detail.service:
                                logging.error("Dropping This LogDetail: Found log_analyzer init request for "
                                              "unix-pattern matched file having same service and file_name already "
                                              f"present in running log_analyzer's cache ;;; log_detail: {log_detail}, "
                                              f"pattern_matched_file_path: {pattern_matched_file_path}")
                            # else not required: would be a file which got tracked by glob again due to pattern
                            continue

                        new_log_detail = LogDetail(service=log_detail.service,
                                                   log_file_path=pattern_matched_file_path,
                                                   critical=log_detail.critical,
                                                   log_prefix_regex_pattern_to_callable_name_dict=
                                                   log_detail.log_prefix_regex_pattern_to_callable_name_dict,
                                                   log_file_path_is_regex=log_detail.log_file_path_is_regex)
                        self.log_details_queue.put(new_log_detail)

                        pattern_matched_added_file_path_to_service_dict[pattern_matched_file_path] = log_detail.service

                        # updating list of tuple of pattern and log_file_path
                        for pattern, _ in log_detail.log_prefix_regex_pattern_to_callable_name_dict.items():
                            self.running_pattern_to_file_name_dict[f"{log_detail.service}~{pattern}"] = (
                                pattern_matched_file_path)
                        # else not required: if file_path already exists in existing_log_details that means file
                        # is already in run
            time.sleep(0.5)     # delay for while loop

    def dynamic_start_log_analyzer_for_log_details(self):
        while self.run_mode:
            log_detail = self.log_details_queue.get()   # blocking call
            new_thread = Thread(target=self._listen, args=(log_detail,))
            self.running_log_analyzer_thread_list.append(new_thread)
            new_thread.start()

    def _handle_raw_performance_data_queue(self):
        raw_performance_data_bulk_create_counts_per_call, raw_perf_data_bulk_create_timeout = (
            get_transaction_counts_n_timeout_from_config(self.config_yaml_dict.get("raw_perf_data_config")))
        LogAnalyzer.queue_handler(
            self.raw_performance_data_queue, raw_performance_data_bulk_create_counts_per_call,
            raw_perf_data_bulk_create_timeout,
            self.webclient_object.create_all_raw_performance_data_client)

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

    def _signal_handler(self, signal_type: int, *args) -> None:
        logging.warning(f"{signal.Signals(signal_type).name} received. Gracefully terminating all subprocess.")
        if not self.run_mode:
            logging.warning("run_mode already set to False. ignoring")
            sys.exit(0)
        logging.info("Setting run_mode to False")
        self.run_mode = False

        for thread in self.running_log_analyzer_thread_list:
            thread.join()

        for process in self.process_list:
            process.kill()
        self.process_list.clear()
        sys.exit(0)

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
        process: subprocess.Popen = subprocess.Popen(['tail', '-F', log_detail.log_file_path], stdout=subprocess.PIPE,
                                                     stderr=subprocess.STDOUT)
        # add poll for process stdout for non-blocking tail of log file
        poll: select.poll = select.poll()
        poll.register(process.stdout)
        last_update_date_time: DateTime = DateTime.utcnow()
        self.process_list.append(process)
        self._analyze_log(process, poll, log_detail, last_update_date_time)

    def _reconnect_process(self, process: subprocess.Popen, log_detail: LogDetail) -> List:
        process.kill()
        self.process_list.remove(process)
        process: subprocess.Popen = subprocess.Popen(['tail', '-F', log_detail.log_file_path], stdout=subprocess.PIPE,
                                                     stderr=subprocess.STDOUT)
        # add poll for process stdout for non-blocking tail of log file
        poll: select.poll = select.poll()
        poll.register(process.stdout)
        self.process_list.append(process)
        return [process, poll]

    def _analyze_log(self, process: subprocess.Popen, poll: select.poll, log_detail: LogDetail,
                     last_update_date_time: DateTime) -> None:
        while self.run_mode:
            try:
                # if critical service, periodically check if new logs are generated. if no new logs are
                # generated, send an alert
                if log_detail.critical:
                    if (DateTime.utcnow() - last_update_date_time).seconds > self.log_refresh_threshold:
                        self.notify_no_activity(log_detail)
                        # updating the timer again to prevent continuous alert generation
                        last_update_date_time = DateTime.utcnow()
                        continue
                    # else not required: last update threshold not breached
                # else not required: service is not critical. skipping periodic check for new logs

                if poll.poll(1):
                    line = process.stdout.readline().decode().strip()
                    if not line:
                        continue
                else:
                    time.sleep(0.5)
                    continue

                if line.startswith("tail"):
                    logging.warning(line)

                if "tail:" in line and "giving up on this name" in line:
                    brief_msg_str: str = f"tail error encountered in log service: {log_detail.service}, restarting..."
                    logging.critical(f"{brief_msg_str};;;{line}")
                    self.notify_tail_error_in_log_service(brief_msg_str, line)
                    process, poll = self._reconnect_process(process=process, log_detail=log_detail)
                    continue

                last_update_date_time = DateTime.utcnow()
                for log_prefix_regex_pattern, callable_name in (
                        log_detail.log_prefix_regex_pattern_to_callable_name_dict.items()):
                    # ignore processing log line that not matches log_prefix_regex_pattern
                    if not re.compile(log_prefix_regex_pattern).search(line):
                        continue

                    if self.refresh_regex_list():
                        logging.info(f"suppress alert regex list updated. regex_list: {self.regex_list}")

                    log_prefix, log_message = \
                        self._get_log_prefix_n_message(log_line=line,
                                                       log_prefix_pattern=log_prefix_regex_pattern)
                    regex_match: bool = False
                    for regex_pattern in self.regex_list:
                        if re.compile(fr"{regex_pattern}").search(log_message):
                            logging.info(f"regex pattern matched, skipping;;; log_message: {log_message[:200]}")
                            regex_match = True
                            break
                    # ignore processing the log line that matches the regex list
                    if regex_match:
                        continue

                    match_callable: Callable[[...], ...] | None = None
                    try:
                        match_callable: Callable = getattr(self, callable_name)
                    except AttributeError as e:
                        logging.error(f"Couldn't find callable {callable_name} in inherited log_analyzer, "
                                      f"exception: {e}, inheriting log_analyzer name: {self.__class__.__name__}")
                    if match_callable:
                        match_callable(log_prefix, log_message)

            except Exception as e:
                logging.exception(f"_analyze_log failed;;; exception: {e}")
                with self.signal_handler_lock:
                    if self.run_mode:
                        self._signal_handler(signal.Signals.SIGTERM)

    def _truncate_str(self, text: str, max_size_in_bytes: int = 2048) -> str:
        if len(text.encode("utf-8")) > max_size_in_bytes:
            text = text.encode("utf-8")[:max_size_in_bytes].decode()
            service_detail: LogDetail = getattr(current_thread(), "service_detail")
            text += f"...check the file: {service_detail.log_file_path} to see the entire log"
        return text

    def _get_log_prefix_n_message(self, log_line: str, log_prefix_pattern: str) -> List[str]:
        pattern: re.Pattern = re.compile(log_prefix_pattern)
        log_prefix: str = pattern.search(log_line)[0]
        log_message_without_prefix: str = log_line.replace(log_prefix, "").strip()
        return [log_prefix, log_message_without_prefix]

    def handle_perf_benchmark_matched_log_message(self, log_prefix: str, log_message: str):
        pattern = re.compile("_timeit_.*_timeit_")
        if search_obj := re.search(pattern, log_message):
            found_pattern = search_obj.group()
            found_pattern = found_pattern[8:-8]  # removing beginning and ending _timeit_
            found_pattern_list = found_pattern.split("~")  # splitting pattern values
            if len(found_pattern_list) == 5:
                callable_name, start_time, delta = found_pattern_list
                if callable_name != "underlying_create_raw_performance_data_http":
                    raw_performance_data_obj = self.raw_performance_data_model_type()
                    raw_performance_data_obj.callable_name = callable_name
                    raw_performance_data_obj.start_time = parse(start_time)
                    raw_performance_data_obj.delta = parse_to_float(delta)

                    self.raw_performance_data_queue.put(raw_performance_data_obj)
                    logging.debug(f"Created raw_performance_data in db for callable {callable_name} "
                                  f"with start_datetime {start_time}")
                # else not required: avoiding callable underlying_create_raw_performance_data to avoid infinite loop
            else:
                err_str_: str = f"Found timeit pattern but internally only contains {found_pattern_list}, " \
                                f"ideally must contain callable_name, date_time, start_time, end_time & delta " \
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

