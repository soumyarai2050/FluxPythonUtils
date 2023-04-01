import logging
import sys
from abc import ABC, abstractmethod
import re
import subprocess
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, current_thread
import signal


class LogAnalyzer(ABC):

    def __init__(self, log_files: List[str] | None, debug_mode: bool = False):
        self.log_files: List[str] | None = log_files
        self.process_list: List[subprocess.Popen] = []
        self.run_mode: bool = True
        self.debug_mode: bool = debug_mode
        self.error_patterns = {
            'error': re.compile(r': ERROR :'),
            'critical': re.compile(r': CRITICAL :'),
            'warning': re.compile(r': WARNING :')
        }
        self.severity_map = {
            'error': 'Severity_ERROR',
            'critical': 'Severity_CRITICAL',
            'warning': 'Severity_WARNING'
        }
        if self.debug_mode:
            logging.warning(f"Running log analyzer in DEBUG mode;;; log_files: {self.log_files}, "
                            f"debug_mode: {self.debug_mode}")
            self.error_patterns.update({
                'info': re.compile(r': INFO :'),
                'debug': re.compile(r': DEBUG :')
            })
            self.severity_map.update({
                'info': 'Severity_INFO',
                'debug': 'Severity_DEBUG'
            })

        if self.log_files is None or len(self.log_files) == 0:
            raise Exception(f"No log files provided for analysis;;; log_files: {self.log_files}")

        signal.signal(signal.SIGTERM, self._signal_handler)

        with ThreadPoolExecutor(max_workers=len(self.log_files)) as exe:
            exe.map(self._listen, self.log_files)

    def _signal_handler(self, *args):
        logging.warning("SIGTERM received. Gracefully terminating all subprocess.")
        for process in self.process_list:
            process.kill()
        self.process_list.clear()
        logging.info("Setting run_mode to False")
        self.run_mode = False
        sys.exit(0)

    def _listen(self, log_file_name: str):
        thread: Thread = current_thread()
        thread.name = log_file_name
        process: subprocess.Popen = subprocess.Popen(['tail', '-F', log_file_name], stdout=subprocess.PIPE,
                                                     stderr=subprocess.PIPE)
        self.process_list.append(process)
        self._analyze_log(process)

    def _analyze_log(self, process: subprocess.Popen):
        while self.run_mode:
            line = process.stdout.readline().decode()
            if not line or not line.strip().replace("\n", ""):
                continue
            line = line.strip()
            # ignore processing the log line that ends with "---"
            if line.strip().endswith("---"):
                continue

            log_message = self._remove_log_prefix(log_line=line)
            if log_message.startswith("$$$"):
                # handle trade simulator message
                logging.info(f"Trade simulator message: {log_message}")
                self._process_trade_simulator_message(log_message)
                continue

            logging.debug(f"Processing log line: {line[:100]}...")
            for error_type, pattern in self.error_patterns.items():
                match = pattern.search(str(line))
                if match:
                    error_dict: Dict = {
                        'type': error_type,
                        'line': line.strip()
                    }
                    logging.info(f"Error pattern matched, creating alert. error_dict: {error_dict}")
                    self._create_alert(error_dict)
                    break
                # else not required: if pattern doesn't match, no need to append error

    def _get_severity(self, error_type: str) -> str:
        error_type = error_type.lower()
        if error_type in self.severity_map:
            return self.severity_map[error_type]
        else:
            return 'Severity_UNKNOWN'

    def _create_alert(self, error_dict: Dict):
        alert_brief_n_detail_lists = error_dict["line"].split(";;;", 1)
        if len(alert_brief_n_detail_lists) == 2:
            alert_brief = alert_brief_n_detail_lists[0]
            alert_details = alert_brief_n_detail_lists[1]
        else:
            alert_brief = error_dict["line"]
            alert_details = ""
        alert_brief = self._truncate_str(alert_brief)
        alert_details = self._truncate_str(alert_details)
        severity = self._get_severity(error_dict["type"])
        self._send_alerts(severity, alert_brief.strip(), alert_details.strip())

    def _truncate_str(self, text: str, max_size_in_bytes: int = 2048) -> str:
        if len(text.encode("utf-8")) > max_size_in_bytes:
            text = text.encode("utf-8")[:max_size_in_bytes].decode()
            text += f"...check the file: {current_thread().name} to see the entire log"
        return text

    def _remove_log_prefix(self, log_line: str):
        if log_line is not None:
            return log_line.split(":", maxsplit=6)[-1].strip()
        return log_line

    @abstractmethod
    def _send_alerts(self, severity: str, alert_brief: str, alert_details: str):
        """
        derived to override with its alert API implementation
        """

    @abstractmethod
    def _process_trade_simulator_message(self, message):
        """
        derived class to override with the implementation of processing of trade simulation message
        """