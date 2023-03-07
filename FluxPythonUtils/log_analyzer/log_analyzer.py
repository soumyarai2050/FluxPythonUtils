import logging
from abc import ABC, abstractmethod
import re
import subprocess
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor


class LogAnalyzer(ABC):

    def __init__(self, log_files: Optional[List[str]], debug_mode: bool = False):
        self.log_files: Optional[List[str]] = log_files
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

        with ThreadPoolExecutor(max_workers=len(self.log_files)) as exe:
            exe.map(self._listen, self.log_files)

    def _listen(self, log_file_name: str):
        process: subprocess.Popen = subprocess.Popen(['tail', '-F', log_file_name], stdout=subprocess.PIPE,
                                                     stderr=subprocess.PIPE)
        self._analyze_log(process)

    def _analyze_log(self, process: subprocess.Popen):
        while True:
            line = process.stdout.readline().decode()
            for error_type, pattern in self.error_patterns.items():
                match = pattern.search(str(line))
                if match:
                    error_dict: Dict = {
                        'type': error_type,
                        'line': line.strip()
                    }
                    self._create_alert(error_dict)
                # else not required: if pattern doesn't match no need to append error

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
        severity = self._get_severity(error_dict["type"])
        self._send_alerts(severity, alert_brief.strip(), alert_details.strip())

    @abstractmethod
    def _send_alerts(self, severity: str, alert_brief: str, alert_details: str):
        """
        derived to override with its alert API implementation
        """
