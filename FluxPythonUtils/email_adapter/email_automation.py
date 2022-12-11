import os
import logging
from typing import List, Final

from FluxPythonUtils.email_adapter.email import Email
from FluxPythonUtils.email_adapter.email_client import EmailClient
from config.configuration import Configuration

# FluxPythonUtils modules
from FluxPythonUtils.scripts.utility_functions import yaml_loader, configure_logger


class EmailUser:
    def __init__(self, username: str, email_address: str):
        self.__username: str = username
        self.__email_address: str = email_address

    @property
    def username(self):
        return self.__username

    @property
    def email_address(self):
        return self.__email_address

    def __str__(self):
        return f"{self.__username} <{self.__email_address}>"


class Attachment:
    """
    Class for attachments to be used as objects for every attachment in read/write email method
    """

    def __init__(self, file_path: str, payload):
        if file_path is None:
            logging.exception("file_path not provided as parameter in attachment initialization")
            raise Exception("file_path can't be empty, provide it at the time of initialization")
        self.__file_path: Final[str] = file_path
        self.__payload: Final = payload
        self.__size: Final[int] = len(self.__payload)

    @property
    def file_path(self):
        return self.__file_path

    @property
    def payload(self):
        return self.__payload

    @property
    def size(self):
        return self.__size

    def __str__(self):
        return f"attachment file_path: {self.__file_path}"


if __name__ == "__main__":
    def test():
        project_name: str = "Pythoncore"
        config_obj = Configuration.get_instance(project_name)
        config = yaml_loader(config_obj.yaml_config_path)
        secret = yaml_loader(config_obj.yaml_secret_path)

        logger_level: str = config["email_logger_level"]
        configure_logger(logger_level, config_obj.log_file_path)

        service_name: str = config["service_name"]

        # use username or email to log in
        username: str = config["cyber_investigator_gmail_email"]["address"]
        password: str = secret["cyber_investigator_gmail_pw"]
        attachments_paths: List[str] = [
            os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__))), "test.jpg"),
            os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__))), "test.txt")
        ]

        attachments_list: List[Attachment] = list()
        for path in attachments_paths:
            with open(path, "rb") as payload:
                attachment: Attachment = Attachment(path, payload.read())
            attachments_list.append(attachment)

        to_addresses: List[EmailUser] = [EmailUser(
            config["cyinve_email"]["username"],
            config["cyinve_email"]["address"]
        )]
        cc_addresses: List[EmailUser] = [EmailUser(
            config["tanishq_email"]["username"],
            config["tanishq_email"]["address"]
        )]
        mail_subject: str = "Test"
        mail_body: str = "Testing mail with attachments"
        sender_obj: EmailUser = EmailUser(
            config["cyber_investigator_gmail_email"]["username"],
            config["cyber_investigator_gmail_email"]["address"]
        )

        email_obj: Email = Email(sender_obj, to_addresses, cc_addresses, mail_subject, attachments_list, mail_body)

        email_automation: EmailClient = EmailClient(project_name, service_name, username, password)

        # Test-case: Send Email
        # print(email_automation.send_mail(email_obj))

        # Test-case: Read Email
        # email_list: List[Email] = email_automation.read_mail(number_of_mails=10, unread=True)
        # for email_obj in email_list:
        #     logging.info(email_obj)
        #     logging.info(email_automation.summarize_body(email_obj))

        # Test-case: Search Email
        # searched_mails: List[Email] = email_automation.search_mails(
        #     from_address= "tanishq.chandra19@outlook.com",
        #     contained_string_subject="test",
        #     contained_string_body="test")
        #
        # print("*"*100)

        # Test-case: Delete Email
        # status: bool = email_automation.delete_mail(searched_mails[-1])
        # print(status)

        # Test-case: Create Telegram group
        email_automation.get_mail_objects_of_given_subject()

    test()
