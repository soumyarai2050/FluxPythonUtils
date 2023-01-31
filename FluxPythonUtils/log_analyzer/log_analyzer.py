from abc import ABC, abstractmethod
import re
import subprocess
from pathlib import PurePath
from typing import Dict


class LogAnalyzer(ABC):

    def __init__(self, log_file_path_and_name: str | None):
        # todo: if log_file_path_and_name in None then get it from environment variable.
        if log_file_path_and_name is not None:
            self.log_file_path_and_name = log_file_path_and_name
        else:
            self.log_file_path_and_name = PurePath(__file__) / "logs.log"

        self.error_patterns = {
            'info': re.compile(r'INFO'),
            'debug': re.compile(r'DEBUG'),
            'error': re.compile(r'ERROR'),
        }
        self.severity_map = {
            'error': 'Severity_ERROR',
            'info': 'Severity_INFO',
            'debug': 'Severity_DEBUG'
        }
        self.f = subprocess.Popen(['tail', '-F', self.log_file_path_and_name], stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)

    def _analyze_log(self):
        while True:
            line = self.f.stdout.readline()
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