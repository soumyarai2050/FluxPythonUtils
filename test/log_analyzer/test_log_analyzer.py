import copy
import queue
import os
import logging
import threading
import time
from pathlib import PurePath
import pytest
from typing import List, Set, Dict
from queue import Queue
from threading import Thread

# 3rd party imports
from pendulum import DateTime
from pydantic import BaseModel, Field, ConfigDict
from fastapi.encoders import jsonable_encoder

# project imports
from FluxPythonUtils.log_analyzer.log_analyzer import LogAnalyzer, LogDetail
from FluxPythonUtils.scripts.general_utility_functions import add_logging_level, configure_logger


@pytest.fixture(scope="function")
def data_queue():
    queue = Queue()
    yield queue


@pytest.fixture(scope="function")
def data_list():
    yield []


@pytest.fixture(scope="function")
def err_obj_list():
    yield []


def test_timeout_1_for_queue_handler(data_queue, data_list, err_obj_list):
    """
    Test to check if timeout is 5 seconds and queue if populated with data every .99 secs,
    by the timeout of 5 secs, web_client callable must be called with expected 5 objects.
    :param data_queue:
    :param data_list:
    :return:
    """
    def mock_web_client(obj_list):
        # deepcopy because passed obj_list is container type, by the time we reach one timeout it gets cleaned
        data_list.append(copy.deepcopy(obj_list))

    def err_handling_callable(obj):
        err_obj_list.append(obj)

    transaction_limit = 10
    timeout_secs = 5
    thread = Thread(target=LogAnalyzer.queue_handler,
                    args=(data_queue, transaction_limit, timeout_secs, mock_web_client, err_handling_callable,),
                    daemon=True)
    thread.start()

    for i in range(5):
        time.sleep(0.99)
        data_queue.put(f"obj-{i+1}")

    # time for queue to get remaining timeout, since total was 5 secs and five times 0.9 secs is consumed
    time.sleep(0.05)

    expected_list = [["obj-1", "obj-2", "obj-3", "obj-4", "obj-5"]]
    assert data_list == expected_list, f'Mismatched, expected: {expected_list}, received: {data_list}'


def test_timeout_2_for_queue_handler(data_queue, data_list):
    """
    Test to check if timeout is 5 secs and no data is passed to queue till timeout,
    and if data is populated to queue after timeout, web_client callable must not have
    been called before next timeout since no data was present in last timeout
    :param data_queue:
    :param data_list:
    :return:
    """
    def mock_web_client(obj_list):
        # deepcopy because passed obj_list is container type, by the time we reach one timeout it gets cleaned
        data_list.append(copy.deepcopy(obj_list))

    def err_handling_callable(obj):
        pass

    transaction_limit = 10
    timeout_secs = 5
    thread = Thread(target=LogAnalyzer.queue_handler,
                    args=(data_queue, transaction_limit, timeout_secs, mock_web_client, err_handling_callable,),
                    daemon=True)
    thread.start()

    # time for queue to get remaining timeout
    time.sleep(timeout_secs)

    data_queue.put(f"obj-1")    # putting data after first timeout
    time.sleep(1)

    # first timeout check
    expected_list = []
    assert data_list == expected_list, f'Mismatched check-1, expected: {expected_list}, received: {data_list}'

    # second timeout check
    time.sleep(4)
    expected_list = [["obj-1"]]
    assert data_list == expected_list, f'Mismatched check-2, expected: {expected_list}, received: {data_list}'


def test_timeout_3_for_queue_handler(data_queue, data_list):
    """
    Test to check if timeout is 5 seconds and transaction limit is very high,
    if queue is populated with within very less duration (prob micro secs),
    by the timeout of 5 secs web_client callable must be called with populated objects in data_list.
    :param data_queue:
    :param data_list:
    :return:
    """
    def mock_web_client(obj_list):
        # deepcopy because passed obj_list is container type, by the time we reach one timeout it gets cleaned
        data_list.append(copy.deepcopy(obj_list))

    def err_handling_callable(obj):
        pass

    transaction_limit = 10000000
    timeout_secs = 5
    thread = Thread(target=LogAnalyzer.queue_handler,
                    args=(data_queue, transaction_limit, timeout_secs, mock_web_client, err_handling_callable,),
                    daemon=True)
    thread.start()

    i = 1
    start_time = DateTime.utcnow()
    while (DateTime.utcnow() - start_time).total_seconds() < timeout_secs:
        data_queue.put(f"obj-{i+1}")
        i += 1

    assert data_list, "web_client obj must have called, but has not called even once"


def test_transaction_limit_for_queue_handler(data_queue, data_list):
    """
    Test to check if transaction_limit is 5 & timeout is 10 seconds and queue if populated
    with data every .99 secs, in 10 rounds loop, web_client must have called 2 times with 5 objs each time.
    Again if one more data is added to queue, after timout webclient must have been called 3 times
    (including last 2) and last one must have been called with single last obj.
    :param data_queue:
    :param data_list:
    :return:
    """
    def mock_web_client(obj_list):
        # deepcopy because passed obj_list is container type, by the time we reach one timeout it gets cleaned
        data_list.append(copy.deepcopy(obj_list))

    def err_handling_callable(obj):
        pass

    transaction_limit = 5
    timeout_secs = 10
    thread = Thread(target=LogAnalyzer.queue_handler,
                    args=(data_queue, transaction_limit, timeout_secs, mock_web_client, err_handling_callable,),
                    daemon=True)
    thread.start()

    for i in range(10):
        time.sleep(0.99)
        data_queue.put(f"obj-{i+1}")

    time.sleep(0.01)

    # web_client callable called due to transaction limit
    expected_list = [["obj-1", "obj-2", "obj-3", "obj-4", "obj-5"],
                     ["obj-6", "obj-7", "obj-8", "obj-9", "obj-10"]]
    assert data_list == expected_list, f'Mismatched check-1, expected: {expected_list}, received: {data_list}'

    data_queue.put("obj-11")

    # time for queue to get timeout, since total was 5 secs and five times 0.9 secs is consumed
    time.sleep(timeout_secs)

    # web_client callable called die to timeout
    expected_list = [["obj-1", "obj-2", "obj-3", "obj-4", "obj-5"],
                     ["obj-6", "obj-7", "obj-8", "obj-9", "obj-10"], ["obj-11"]]
    assert data_list == expected_list, f'Mismatched check-2, expected: {expected_list}, received: {data_list}'


sample_config_dict = {
    "raw_perf_data_config": {
        "transaction_counts_per_call": 500,
        "transaction_timeout_secs": 60
    }
}

sample_regex_file = str(PurePath(__file__).parent / "sample_regex.txt")
mock_client_callable_data_list = []


class MockWebClient:
    def create_all_raw_performance_data_client(self, obj):
        pass

    def mock_client_callable(self, obj_list):
        # deepcopy because passed obj_list is container type, by the time we reach one timeout it gets cleaned
        mock_client_callable_data_list.append(copy.deepcopy(obj_list))


mock_web_client = MockWebClient()


class MockRawPerformanceData(BaseModel):
    id: int | None = Field(alias='_id')
    callable_name: str | None = None
    call_date_time: DateTime | None = None
    start_time_it_val: float | None = None
    end_time_it_val: float | None = None
    delta: float | None = None
    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)


@pytest.fixture(scope="function")
def config_logger_n_get_log_details() -> List[LogDetail]:
    log_dir = PurePath(__file__).parent
    log_file_name = "sample_log.log"

    add_logging_level("TESTRUN", 2)
    configure_logger(2, log_file_dir_path=str(log_dir), log_file_name=log_file_name)

    log_file_path = log_dir / log_file_name
    log_detail_obj = LogDetail(service="test_log_analyzer", log_file_path=str(log_file_path), critical=True,
                               log_prefix_regex_pattern_to_callable_name_dict={
                                   r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} : (TESTRUN) : \[[a-zA-Z._]* : \d*] : ":
                                       "handle_test_matched_log_message"
                               })

    yield [log_detail_obj]

    # deleting log file
    os.remove(log_dir / log_file_name)


no_activity_log_detail_list = []


class MockLogAnalyzer(LogAnalyzer):
    def __init__(self, log_details: List[LogDetail]):
        super().__init__(sample_regex_file, sample_config_dict, mock_web_client, MockRawPerformanceData,
                         log_details=log_details)
        self.test_check_queue: queue.Queue = queue.Queue()

        thread = Thread(target=self._handle_test_check_queue, daemon=True)
        thread.start()

    def err_handling_callable(self):
        pass

    def _handle_test_check_queue(self):
        transaction_counts_per_call = 5
        timeout_secs = 2
        MockLogAnalyzer.queue_handler(
            self.test_check_queue, transaction_counts_per_call,
            timeout_secs,
            self.webclient_object.mock_client_callable,
            self.err_handling_callable)

    def notify_no_activity(self, log_detail: LogDetail):
        no_activity_log_detail_list.append(log_detail)

    def notify_tail_error_in_log_service(self, brief_msg_str: str, detail_msg_str: str):
        pass

    def notify_error(self, error_msg: str):
        pass

    def handle_test_matched_log_message(self, log_prefix: str, log_message: str, log_detail: LogDetail):
        self.test_check_queue.put(log_message)


def tail_process_exists(mock_log_analyzer, log_file_path) -> bool:
    for cached_log_file_path, log_detail in mock_log_analyzer.log_file_path_to_log_detail_dict.items():
        if cached_log_file_path == log_file_path:
            if (str(log_detail.process.args[0]).endswith("tail_logs.sh") and
                    log_detail.process.args[-1] == str(log_file_path)):
                break
    else:
        return False
    return True


def test_dynamic_log_files_load_and_start_tail():
    """
    Checks log files dynamic loading based on file names
    :return:
    """
    log_dir = PurePath(__file__).parent

    log_detail_list = []
    for i in range(5):
        log_file_path = log_dir / f"sample_log_{i+1}.log"
        log_detail_obj = (
            LogDetail(service="test_log_analyzer", log_file_path=str(log_file_path), critical=True,
                      log_prefix_regex_pattern_to_callable_name_dict={
                          r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} : (TESTRUN) : \[[a-zA-Z._]* : \d*] : ":
                              "handle_test_matched_log_message"
                      }))
        log_detail_list.append(log_detail_obj)

    mock_log_analyzer = MockLogAnalyzer(log_detail_list)

    thread = threading.Thread(target=mock_log_analyzer.log_file_watcher, daemon=True)
    thread.start()

    # checking if no process exist for files before files doesn't exist
    for log_detail in log_detail_list:
        if tail_process_exists(mock_log_analyzer, log_detail.log_file_path):
            assert False, f"Process for file contains before file exists, file_name: {log_detail.log_file_path}"
    else:
        assert True

    # creating files and checking dynamic feature
    for log_detail in log_detail_list:
        with open(log_detail.log_file_path, "w") as f:
            pass

        time.sleep(1)

        if not tail_process_exists(mock_log_analyzer, log_detail.log_file_path):
            assert False, ("Could not find any process in process list data member of log analyzer object having tail "
                           f"for file_name: {log_detail.log_file_path}")

    # deleting created files
    for log_detail in log_detail_list:
        os.remove(log_detail.log_file_path)


def test_dynamic_pattern_load_and_start_tail():
    """
    Checks log files dynamic loading based on file names patterns
    :return:
    """
    log_dir = PurePath(__file__).parent

    log_detail_list = []
    for i in range(5):
        log_file_path = log_dir / f"sample_log_{i + 1}*.log"
        log_detail_obj = (
            LogDetail(service="test_log_analyzer", log_file_path=str(log_file_path), critical=True,
                      log_prefix_regex_pattern_to_callable_name_dict={
                          r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} : (TESTRUN) : \[[a-zA-Z._]* : \d*] : ":
                              "handle_test_matched_log_message",
                      }, log_file_path_is_regex=True))
        log_detail_list.append(log_detail_obj)

    mock_log_analyzer = MockLogAnalyzer(log_detail_list)

    thread = threading.Thread(target=mock_log_analyzer.log_file_watcher, daemon=True)
    thread.start()

    # creating files and checking dynamic feature
    created_files = []
    for index, log_detail in enumerate(log_detail_list):
        file_pattern = log_detail.log_file_path
        file_path = file_pattern.replace("*", f"{index + 1}")
        with open(file_path, "w") as f:
            pass
        created_files.append(file_path)

        time.sleep(1)

        print(mock_log_analyzer.log_file_path_to_log_detail_dict)
        if not tail_process_exists(mock_log_analyzer, file_path):
            assert False, ("Could not find any process in process list data member of log analyzer object having tail "
                           f"for file_name: {file_path}")

    # deleting created files
    for created_file in created_files:
        os.remove(created_file)


def test_no_activity(config_logger_n_get_log_details):
    """
    Checks no activity handling
    :param config_logger_n_get_log_details:
    :return:
    """
    no_activity_log_detail_list.clear()

    log_details = config_logger_n_get_log_details

    mock_log_analyzer = MockLogAnalyzer(log_details)

    thread = threading.Thread(target=mock_log_analyzer.log_file_watcher, daemon=True)
    thread.start()

    logging.testrun("Mock log message")

    time.sleep(60.5)  # no activity wait

    assert log_details[0] in no_activity_log_detail_list, \
        (f"Could not find log_detail in no_activity_log_detail_list, log_detail: {log_details}, "
         f"no_activity_log_detail_list: {no_activity_log_detail_list}")


def test_log_matched_web_client_call(config_logger_n_get_log_details):
    """
    Checks if correct type of log message is matched and web_client is called with that log message data
    :param config_logger_n_get_log_details:
    :return:
    """
    mock_client_callable_data_list.clear()

    log_details = config_logger_n_get_log_details

    mock_log_analyzer = MockLogAnalyzer(log_details)

    thread = threading.Thread(target=mock_log_analyzer.log_file_watcher, daemon=True)
    thread.start()

    assert not mock_client_callable_data_list, \
        ("mock_client_callable_data_list must be empty, found containing data, "
         f"mock_client_callable_data_list: {mock_client_callable_data_list}")

    for i in range(5):
        debug_mock_msg = f"debug Mock log message - {i}"
        test_run_mock_msg = f"testrun Mock log message - {i}"
        logging.debug(debug_mock_msg)
        time.sleep(0.5)
        logging.testrun(test_run_mock_msg)

        time.sleep(2)   # timeout wait

        assert [debug_mock_msg] not in mock_client_callable_data_list, \
            (f"Pattern must not match debug message, found debug_matched data: {[debug_mock_msg]} in "
             f"mock_client_callable_data_list: {mock_client_callable_data_list}")
        assert [test_run_mock_msg] in mock_client_callable_data_list, \
            (f"Could not find expected data in mock_client_callable_data_list, expected data: {[test_run_mock_msg]}, "
             f"mock_client_callable_data_list: {mock_client_callable_data_list}")


class SampleBaseModel(BaseModel):
    id: int | None = Field(alias='_id')
    message: str | None = None
    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)


class MockPatchClientError(Exception):
    def __init__(self, non_existing_ids):
        self.non_existing_ids: Set = set(non_existing_ids)
        super().__init__(f"'objects with ids: {self.non_existing_ids} out of requested '")


def test_patch_error_handling_in_queue_handler(data_queue, data_list, err_obj_list):

    def mock_web_client(obj_list):
        non_existing_ids = []
        for obj in obj_list:
            if obj.get("_id") % 2 != 0:
                non_existing_ids.append(obj["_id"])
            else:
                data_list.append(obj)

        raise MockPatchClientError(non_existing_ids)

    def err_handling_callable(obj_list):
        err_obj_list.extend(obj_list)

    transaction_limit = 10
    timeout_secs = 5
    for i in range(10):
        sample_basemodel: SampleBaseModel = SampleBaseModel()
        sample_basemodel.id = i + 1
        data_queue.put(jsonable_encoder(sample_basemodel, by_alias=True, exclude_none=True))
    thread = Thread(target=LogAnalyzer.queue_handler,
                    args=(data_queue, transaction_limit, timeout_secs, mock_web_client, err_handling_callable,),
                    daemon=True)
    thread.start()
    time.sleep(0.5)

    expected_list = [{'_id': 2}, {'_id': 4}, {'_id': 6}, {'_id': 8}, {'_id': 10}]
    assert data_list == expected_list, f'Mismatched - data_list, expected: {expected_list}, received: {data_list}'
    expected_err_obj_list = [{'_id': 1}, {'_id': 3}, {'_id': 5}, {'_id': 7}, {'_id': 9}]
    assert err_obj_list == expected_err_obj_list, f'Mismatched - err_obj_list, expected: {expected_list}, received: {err_obj_list}'


class MockConnectionError(Exception):
    def __init__(self):
        super().__init__("Failed to establish a new connection: [Errno 111] Connection refused")


def test_connection_error_handling_in_queue_handler(data_queue, data_list, err_obj_list):

    call_counter = 0
    def mock_web_client(obj_list):
        nonlocal call_counter
        call_counter += 1

        if call_counter <= 3:
            raise MockConnectionError
        else:
            data_list.extend(obj_list)

    def err_handling_callable(obj_list):
        err_obj_list.extend(obj_list)

    transaction_limit = 10
    timeout_secs = 5
    client_connection_fail_retry_secs = 5
    for i in range(40):
        sample_basemodel: SampleBaseModel = SampleBaseModel()
        sample_basemodel.id = i + 1
        data_queue.put(jsonable_encoder(sample_basemodel, by_alias=True, exclude_none=True))
    thread = Thread(target=LogAnalyzer.queue_handler,
                    args=(data_queue, transaction_limit, timeout_secs, mock_web_client,
                          err_handling_callable, client_connection_fail_retry_secs),
                    daemon=True)
    thread.start()
    time.sleep(16)

    assert call_counter == 4, \
        "call counter variable must be updated to 4 since web_client must be called 4 times"
    expected_data_list = []
    for i in range(31, 41):
        expected_data_list.append({'_id': i})
    assert data_list == expected_data_list, \
        f"Mismatched data_list: expected: {expected_data_list}, received: {data_list}"
    print(data_list)
    assert err_obj_list == [], \
        ("Since err_handling_callable is not called in connection exception, err_obj_list must be empty but found some "
         f"data, err_obj_list: {err_obj_list}")


def test_connection_failure_handling_in_queue_handler(data_queue, data_list, err_obj_list):

    def mock_web_client(obj_list):
        raise Exception("Some Error Occurred")

    def err_handling_callable(obj_list):
        err_obj_list.extend(obj_list)

    transaction_limit = 5
    timeout_secs = 5
    for i in range(5):
        sample_basemodel: SampleBaseModel = SampleBaseModel()
        sample_basemodel.id = i + 1
        data_queue.put(jsonable_encoder(sample_basemodel, by_alias=True, exclude_none=True))
    thread = Thread(target=LogAnalyzer.queue_handler,
                    args=(data_queue, transaction_limit, timeout_secs, mock_web_client,
                          err_handling_callable),
                    daemon=True)

    thread.start()

    expected_err_obj_list: List[Dict] = [{'_id': 1}, {'_id': 2}, {'_id': 3}, {'_id': 4}, {'_id': 5}]

    assert err_obj_list == expected_err_obj_list, (f'Mismatched - err_obj_list, expected: {expected_err_obj_list}, '
                                                   f'received: {err_obj_list}')
