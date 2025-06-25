import logging
from datetime import datetime
import orjson
import requests
import json
from json import JSONDecodeError
import os
from typing import Dict


class JSONReplayOverHTTP:
    def __init__(self, url: str, json_dir: str, replay_date: str):
        self.url: str = url
        self.json_dir: str = json_dir
        self.replay_date: str = replay_date
        self.headers: Dict = {"Content-Type": "application/json"}

    def replay_messages(self) -> None:
        try:
            datetime.strptime(self.replay_date, "%Y-%m-%d")
        except ValueError as e:
            err_str = f"Invalid replay_date format. Expected format is YYYY-MM-DD, found: " \
                      f"{self.replay_date};;; exception: {e}"
            logging.exception(err_str)
            raise Exception(err_str)

        if not os.path.exists(self.json_dir):
            error_str = f"Invalid json_dir path: {self.json_dir}"
            logging.error(error_str)
            raise FileNotFoundError(error_str)

        json_msg_dir = os.path.join(self.json_dir, self.replay_date)
        if not os.path.exists(json_msg_dir):
            error_str = f"No message exists for {self.replay_date}"
            logging.error(error_str)
            raise Exception(error_str)

        files = os.listdir(json_msg_dir)
        processed_files = [f for f in files if f.endswith(".json")]
        for file in processed_files:
            try:
                with open(os.path.join(json_msg_dir, file)) as f:
                    json_dict = orjson.loads(f.read())
                    self._publish_message(json_dict)
            except JSONDecodeError as e:
                logging.exception(f"Failed to load json message! {e}")
            except Exception as e:
                logging.exception(f"Failed to publish message! {e}")

    def _publish_message(self, message: Dict):
        message = json.dumps(message)
        response = requests.post(self.url, data=message, headers=self.headers)
        if response.status_code == 201:
            logging.info("Message published")
        else:
            logging.error(f"Failed to publish message! {response.text}")