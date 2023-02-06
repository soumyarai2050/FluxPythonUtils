import re
import os
import logging
from pathlib import PurePath
import pytest
from typing import Dict

# project imports
from FluxPythonUtils.log_analyzer.log_analyzer import LogAnalyzer


class MockLogAnalyzer(LogAnalyzer):
    def __init__(self, log_file_path_and_name: str):
        super().__init__(log_file_path_and_name)
        self.alerts: Dict = {}

    def _send_alerts(self, severity: str, alert_brief: str, alert_details: str):
        self.alerts: Dict = {
            'severity': severity,
            'brief': alert_brief,
            'details': alert_details
        }


@pytest.fixture(scope="session")
def log_file_path_and_name():
    root_path: PurePath = PurePath(__file__).parent
    log_file_path_and_name: PurePath = root_path / 'logs.log'

    yield str(log_file_path_and_name)


@pytest.fixture(scope='class')
def log_analyzer(log_file_path_and_name: str):
    log_analyzer: MockLogAnalyzer = MockLogAnalyzer(log_file_path_and_name)
    return log_analyzer


def test_log_analyzer_init(log_analyzer: MockLogAnalyzer, log_file_path_and_name: str):
    assert log_analyzer.log_file_path_and_name == log_file_path_and_name
    assert isinstance(log_analyzer.error_patterns, dict)
    assert log_analyzer.error_patterns == {
                                           'info': re.compile(r'INFO'),
                                           'debug': re.compile(r'DEBUG'),
                                           'error': re.compile(r'ERROR')
                                           }
    assert isinstance(log_analyzer.severity_map, dict)
    assert log_analyzer.severity_map == {
                                         'error': 'Severity_ERROR',
                                         'info': 'Severity_INFO',
                                         'debug': 'Severity_DEBUG'
                                         }


def test_log_analyzer_get_severity(log_analyzer: MockLogAnalyzer):
    assert log_analyzer._get_severity('error') == 'Severity_ERROR'
    assert log_analyzer._get_severity('Info') == 'Severity_INFO'
    assert log_analyzer._get_severity('DEBUG') == 'Severity_DEBUG'
    assert log_analyzer._get_severity('Unknown') == 'Severity_UNKNOWN'
    assert log_analyzer._get_severity('ERROR') == 'Severity_ERROR'
    assert log_analyzer._get_severity('INFO') == 'Severity_INFO'
    assert log_analyzer._get_severity('test') == 'Severity_UNKNOWN'


@pytest.mark.parametrize("line, expected_severity, expected_brief, expected_details", [
    ("this should be in error brief;;;this should be in my detail", "Severity_ERROR",
     "this should be in error brief", "this should be in my detail"),
    (";;;this should be in error detail and has no brief", "Severity_ERROR", "",
     "this should be in error detail and has no brief"),
    ("this should be in error brief and has no detail;;;", "Severity_ERROR",
     "this should be in error brief and has no detail", ""),
    ("this should be in error brief and has no detail", "Severity_ERROR",
     "this should be in error brief and has no detail", ""),
    ("this should be in error brief;;;this in error detail ;;; this also in error detail", "Severity_ERROR",
     "this should be in error brief", "this in error detail ;;; this also in error detail"),
    (" this should be in error brief with surrounding spaces trimmed ;;; this in error detail ;;; this also in error "
     " detail with surrounding spaces trimmed ", "Severity_ERROR",
     "this should be in error brief with surrounding spaces trimmed", "this in error detail ;;; this also in error "
                                                                      "detail with surrounding spaces trimmed"),
])
def test_create_alert(log_analyzer: MockLogAnalyzer, line, expected_severity, expected_brief, expected_details):
    error_dict: Dict = {'type': 'error', 'line': line}

    log_analyzer._create_alert(error_dict)

    alerts: Dict = log_analyzer.alerts
    assert alerts["severity"] == expected_severity
    assert alerts["brief"] == expected_brief
    assert alerts["details"] == expected_details
