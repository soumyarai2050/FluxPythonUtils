from datetime import datetime
from typing import List, Optional, Final

# from FluxPythonUtils.email_adapter.email_automation import EmailUser, Attachment


class EmailHandler:
    def __init__(self, sender_user_obj: "EmailUser", to_user_obj_list: List['EmailUser'],
                 cc_user_obj_list: Optional[List['EmailUser']] = None,
                 subject: str | None = None,
                 attachments_list: Optional[List['Attachment']] = None,
                 content: str | None = None,
                 message_id: str | None = None, mail_time: datetime | None = None):
        self.__sender_user_obj: 'EmailUser' = sender_user_obj
        self.__to_user_obj_list: Final[List['EmailUser']] = to_user_obj_list
        self.__cc_user_obj_list: Final[List['EmailUser']] = cc_user_obj_list if cc_user_obj_list is not None else []
        self.__subject: str = subject if subject is not None else ""
        self.__attachments_list: List['Attachment'] = attachments_list if attachments_list is not None else []
        self.__content: str = content
        self.__message_id: str = message_id if message_id is not None else ""
        self.__mail_time: datetime = mail_time if mail_time is not None else datetime.now()

    @property
    def message_id(self):
        return self.__message_id

    @property
    def sender_user_obj(self):
        return self.__sender_user_obj

    @property
    def to_user_obj_list(self):
        return self.__to_user_obj_list

    @property
    def cc_user_obj_list(self):
        return self.__cc_user_obj_list

    @property
    def subject(self):
        return self.__subject

    @property
    def content(self):
        return self.__content

    @property
    def mail_time(self):
        return self.__mail_time

    @property
    def attachments_list(self):
        return self.__attachments_list

    def __str__(self):
        return f"""
                msg-id: {self.__message_id}
                from: {self.__sender_user_obj.username} <{self.__sender_user_obj.email_address}>
                to: {[str(to) for to in self.__to_user_obj_list]}
                cc: {[str(cc) for cc in self.__cc_user_obj_list]}
                time: {self.__mail_time.strftime("%d-%m-%y %H:%M:%S")}
                subject: {self.__subject}
                content: {self.__content}
                attachments: {[str(attachment) for attachment in self.__attachments_list]}
                """
