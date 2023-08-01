import logging
import os
import sys
import time
from abc import ABC, abstractmethod
import re
import subprocess
from typing import Dict, List, Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, current_thread, Lock
import signal
from pendulum import DateTime
import select
from pydantic import BaseModel


class LogDetail(BaseModel):
    service: str
    log_file: str
    critical: bool = False
    log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str] | None


class LogAnalyzer(ABC):

    def __init__(self, regex_file: str, log_details: List[LogDetail] | None = None,
                 log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str] | None = None):
        self.regex_file: str = regex_file
        self.regex_file_lock: Lock = Lock()
        self.regex_file_data_snapshot_version: float | None = None
        self.regex_list: List[str] = list()
        self.log_details: List[LogDetail] | None = log_details if log_details is not None else []
        self.log_prefix_regex_pattern_to_callable_name_dict: Dict[str, str] = \
            log_prefix_regex_pattern_to_callable_name_dict \
            if log_prefix_regex_pattern_to_callable_name_dict is not None else {}
        self.process_list: List[subprocess.Popen] = []
        self.run_mode: bool = True
        self.signal_handler_lock: Lock = Lock()
        self.log_refresh_threshold: int = 60
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def run(self):
        if self.log_details is None or len(self.log_details) == 0:
            raise Exception(f"No log files provided for analysis;;; log_details: {self.log_details}")

        with ThreadPoolExecutor(max_workers=len(self.log_details)) as exe:
            results = exe.map(self._listen, self.log_details)

        for res in results:
            if res is not None:
                logging.error(res)

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
        setattr(thread, "service_detail", log_detail)
        if not os.path.exists(log_detail.log_file):
            with open(log_detail.log_file, "w+"):
                pass
        process: subprocess.Popen = subprocess.Popen(['tail', '-F', log_detail.log_file], stdout=subprocess.PIPE,
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
        process: subprocess.Popen = subprocess.Popen(['tail', '-F', log_detail.log_file], stdout=subprocess.PIPE,
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

                    match_callable: Callable = getattr(self, callable_name)
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
            text += f"...check the file: {service_detail.log_file} to see the entire log"
        return text

    def _get_log_prefix_n_message(self, log_line: str, log_prefix_pattern: str) -> List[str]:
        pattern: re.Pattern = re.compile(log_prefix_pattern)
        log_prefix: str = pattern.search(log_line)[0]
        log_message_without_prefix: str = log_line.replace(log_prefix, "").strip()
        return [log_prefix, log_message_without_prefix]

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

