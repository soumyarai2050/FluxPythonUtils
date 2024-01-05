import base64
import email
import imaplib
import json
import logging
import quopri
import re
import smtplib
import time
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parseaddr
from typing import List, Tuple, Final

from bs4 import BeautifulSoup
from dateutil import parser

from FluxPythonUtils.email_adapter.email_handler import EmailHandler
from FluxPythonUtils.scripts.utility_functions import file_exist


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


class EmailClient:
    """
    Class to send and receive mails using script.
    Takes 3 Parameters:
    service_name: If service is gmail, enter gmail, if it is hotmail enter hotmail.(currently working with these only)
    user_name: Email form which mail needs to be sent
    password: password of mail from which mail needs to be sent
    """

    def __init__(self, project_name: str, service_name: str, user_name, password):
        self.__project_name: str = project_name
        self.__service_name = service_name
        if service_name.lower() == "gmail":
            self.__smtp_host = 'smtp.gmail.com'
            self.__smtp_port = 587
            self.__imap_server_host = 'imap.gmail.com'
        elif service_name.lower() == "hotmail" or service_name.lower() == "outlook":
            self.__smtp_host = 'smtp-mail.outlook.com'
            self.__smtp_port = 587
            self.__imap_server_host = 'outlook.office365.com'
        elif service_name.lower() == "yahoo":
            # @@@ yahoo is not tested as it is having issue from server side in creating app password
            self.__smtp_host = 'smtp.mail.yahoo.com'
            self.__smtp_port = 587
            self.__imap_server_host = 'imap.mail.yahoo.com'
        elif service_name.lower() == "aol":
            # @@@ AOL is not tested as it is having issue from server side in creating app password
            self.__smtp_host = "smtp.aol.com"
            self.__smtp_port = 465
            self.__imap_server_host = "imap.aol.com"
        elif service_name.lower() == "mail":
            # @@@ AOL is not tested as it is having issue from server side in creating app password
            self.__smtp_host = "smtp.mail.com"
            self.__smtp_port = 587
            self.__imap_server_host = "imap.mail.com"
        else:
            error = f"Unsupported server name: {service_name}, contact admin"
            logging.critical(error)
            raise Exception(error)

        # use username or email to log in
        self.__username = user_name
        self.__password = password

        self.__smtp_server = None
        self.__imap_server = None
        self.__smtp_server = self.__init_send_mail()
        self.__imap_server = self.__init_read_mail()

    def __init_send_mail(self) -> smtplib.SMTP:
        server = smtplib.SMTP(self.__smtp_host, self.__smtp_port)
        server.starttls()

        # to interact with the server, first we log in
        # and, then we send the message
        try:
            server.login(self.__username, self.__password)
        except Exception as e:
            raise Exception(f"Email Login failed fro SMTP for username: {self.__username}: exception: {e}")
        else:
            return server

    def __init_read_mail(self) -> imaplib.IMAP4_SSL:
        # connect to the server and go to its inbox
        imap = imaplib.IMAP4_SSL(self.__imap_server_host)
        try:
            imap.login(self.__username, self.__password)
        except Exception as e:
            raise Exception(f"Email Login failed for IMAP for username: {self.__username}: exception: {e}")
        else:
            return imap

    def __del__(self):
        self.__smtp_server.quit()
        self.__imap_server.logout()

    @property
    def project_name(self) -> str:
        return self.__project_name

    @staticmethod
    def __encoded_words_to_text(encoded_words) -> str:
        """
        Method to decode encoded words to readable text
        """
        encoded_word_regex = r'=\?{1}(.+)\?{1}([B|Q])\?{1}(.+)\?{1}='
        charset, encoding, encoded_text = re.match(encoded_word_regex, encoded_words).groups()
        if encoding == 'B':
            byte_string = base64.b64decode(encoded_text)
        elif encoding == 'Q':
            byte_string = quopri.decodestring(encoded_text)
        else:
            return encoded_words
        return byte_string.decode(charset)

    def send_mail(self, email_obj: EmailHandler) -> bool:
        """
        Method to send mail
        Takes 3 parameters:
        msg_subject: subject of mail
        msg_body: body/content of mail
        to_mail_address: List of recipient email id's
        attachment_filepaths_list (Optional): List of paths of files to be attached with mail
        Returns bool for success confirmation
        """
        try:
            msg = MIMEMultipart('alternative')
            # setup parameters of the message
            msg['From'] = email_obj.sender_user_obj.email_address

            to_mail_addresses: List[str] = [user_obj.email_address for user_obj in email_obj.to_user_obj_list]
            msg['To'] = ', '.join(to_mail_addresses)

            cc_mail_addresses: List[str] = [user_obj.email_address for user_obj in email_obj.cc_user_obj_list]
            msg['Cc'] = ', '.join(cc_mail_addresses)

            msg['Subject'] = email_obj.subject

            # add in the message body
            msg.attach(MIMEText(email_obj.content, 'plain'))

            if email_obj.attachments_list:

                for attachment in email_obj.attachments_list:
                    # open the file to be sent
                    filename = attachment.file_path.split("/")[-1]

                    # instance of MIMEBase and named as mime_base
                    mime_base = MIMEBase('application', 'octet-stream')

                    # To change the payload into encoded form
                    mime_base.set_payload(attachment.payload)

                    # encode into base64
                    encoders.encode_base64(mime_base)

                    mime_base.add_header('Content-Disposition', "attachment; filename= %s" % filename)

                    # attach the instance 'mime_base' to instance 'msg'
                    msg.attach(mime_base)

            self.__smtp_server.send_message(msg)
            return True
        except Exception as e:
            logging.exception(f"Some Error occurred while sending mail: {email_obj}: {e} ")
            return False

    @staticmethod
    def __handle_fetch_body_and_attachments(message_in_bytes) -> Tuple[str, List[Attachment]]:
        """
        Method to handle fetching of body and attachments from the received email's payload
        returns Tuple of fetched body and List of Attachment object
        """
        attachments_list: List[Attachment] = list()
        body = ""
        for part in message_in_bytes.walk():
            content_type = part.get_content_type()
            content_dispo = str(part.get('Content-Disposition'))

            if content_type == 'text/plain' and 'attachment' not in content_dispo:
                body = part.get_payload(decode=True)  # decode
            elif 'attachment' in content_dispo:
                file_name = part.get_filename()
                attachments_list.append(Attachment(file_name, part.get_payload(decode=True)))
                logging.debug(f"Downloaded: {file_name}")

        return body, attachments_list

    @staticmethod
    def __handle_only_fetched_body(message_in_bytes) -> str:
        """
        Method to handle fetching of body from the received email's payload
        returns fetched body
        """
        body = message_in_bytes.get_payload(decode=True)
        return body

    def read_mail(self, latest: bool = True, mark_read: bool = False, unread: bool = True, number_of_mails: int = 10,
                  mail_section: str = "inbox") -> List[EmailHandler]:
        """
        Method to read received mails in user's account
        Takes 3 optional parameters:
        latest: Default is true, means latest mails will be provided. If turned false starts from first mail of inbox.
        unread: If unread is True, UNSEEN mails are fetched else All mails are fetched
        number_of_mails: Total number of mails to return, if not provided will fetch last 10 mails
        mail-section: default is inbox
        Returns List of Email Objects
        """
        received_emails_list: List[EmailHandler] = list()

        # we choose the inbox but you can select others
        if not mark_read:
            self.__imap_server.select(mail_section, readonly=True)
        else:
            self.__imap_server.select(mail_section)

        # we'll search using the ALL criteria to retrieve
        # every message inside the inbox
        # it will return with its status and a list of ids
        # @@@ To know more about search parameters and queries:
        #       https://datatracker.ietf.org/doc/html/rfc3501#page-49
        if unread:
            received_emails_list += self.fetch_mails_from_server("UNSEEN", latest, number_of_mails)
        else:
            received_emails_list += self.fetch_mails_from_server("ALL", latest, number_of_mails)

        return received_emails_list

    def __email_username_address_seperator(self, fetched_email_sender: str) -> Tuple[str, str]:
        """
        Method to separate username and user_email_address from provided data fetched from received mails
        Returns Tuple of username and user_email_address
        """
        sender_username, sender_email_address = parseaddr(fetched_email_sender)
        if "UTF" in sender_username:
            sender_username = self.__encoded_words_to_text(sender_username)
        return sender_username, sender_email_address

    def fetch_mails_from_server(self, search_query: str, latest: bool = True,
                                number_of_mails: int | None = None, delete_mails: bool = False) -> List[EmailHandler]:
        """
        Method to fetch and return the list of email objects from the server of service.
        Takes search_query to fetch specific mails, latest as bool which, if true means latest mails will be first
        in order else oldest mails will be first in order, number of mails to get specific number of mails in
        returned list and delete mail as bool to specify if True means selected mails needs to be deleted else
        just returned as list of mails
        This method returns either empty list of email objects, that means fetched mails where deleted as deleted
        parameter was True, or returns list of fetched email objects
        """
        fetched_emails_list: List[EmailHandler] = list()

        status, data = self.__imap_server.search(None, search_query)

        if delete_mails:
            # convert messages to a list of email IDs
            data = data[0].split(b' ')
            for num in data:
                self.__imap_server.store(num, '+FLAGS', '\\Deleted')
            self.__imap_server.expunge()
            return fetched_emails_list

        # the list returned is a list of bytes separated
        # by white spaces on this format: [b'1 2 3', b'4 5 6']
        # so, to separate it first we create an empty list
        mail_ids = []
        # then we go through the list splitting its blocks
        # of bytes and appending to the mail_ids list
        for block in data:
            # the split function called without parameter
            # transforms the text or bytes into a list using
            # as separator the white spaces:
            # b'1 2 3'.split() => [b'1', b'2', b'3']
            mail_ids += block.split()

        if latest:
            if number_of_mails is not None:
                mail_ids = mail_ids[:-(number_of_mails + 1): -1]
            else:
                mail_ids = mail_ids[::-1]

        # now for every id we'll fetch the email
        # to extract its content
        for i in mail_ids:
            # the fetch function fetch the email given its id
            # and format that you want the message to be
            status, data = self.__imap_server.fetch(i, '(RFC822)')

            typ, msg_id_data = self.__imap_server.fetch(i, '(BODY[HEADER.FIELDS (MESSAGE-ID)])')
            msg_str = email.message_from_string(msg_id_data[0][1].decode('utf-8'))
            message_id = msg_str.get('Message-ID')

            # the content data at the '(RFC822)' format comes on
            # a list with a tuple with header, content, and the closing
            # byte b')'
            for response_part in data:
                # so if its a tuple...
                if isinstance(response_part, tuple):
                    # we go for the content at its second element
                    # skipping the header at the first and the closing
                    # at the third
                    message = email.message_from_bytes(response_part[1])

                    # with the content we can extract the info about
                    # who sent the message and its subject
                    mail_from: str = message['from']
                    sender_username, sender_email_address = self.__email_username_address_seperator(mail_from)
                    mail_from_user_obj: EmailUser = EmailUser(sender_username, sender_email_address)

                    cc_addresses: str = message["cc"]
                    cc_user_obj_list: List[EmailUser] = list()
                    if cc_addresses is not None:
                        if "," in cc_addresses:
                            cc_list: List[str] = cc_addresses.split(",")
                        else:
                            cc_list: List[str] = [cc_addresses]
                        for each_cc in cc_list:
                            cc_username, cc_email_address = \
                                self.__email_username_address_seperator(each_cc.strip())
                            cc_user_obj_list.append(EmailUser(cc_username, cc_email_address))
                    # else not required: if the cc_addresses is None then empty list will be used as cc_user_obj_list

                    to_addresses: str = message["to"]
                    to_user_obj_list: List[EmailUser] = list()
                    if to_addresses is not None:
                        if "," in to_addresses:
                            to_list: List[str] = to_addresses.split(",")
                        else:
                            to_list: List[str] = [to_addresses]
                        for each_to in to_list:
                            to_username, to_email_address = \
                                self.__email_username_address_seperator(each_to.strip())
                            to_user_obj_list.append(EmailUser(to_username, to_email_address))
                    # else not required: if the cc_addresses is None then empty list will be used as cc_user_obj_list

                    mail_subject: str = message['subject']
                    mail_time: datetime = parser.parse(message["Date"])

                    # then for the text we have a little more work to do
                    # because it can be in plain text or multipart
                    # if its not plain text we need to separate the message
                    # from its annexes to get the text
                    attachments_list: List[Attachment] = list()
                    if message.is_multipart():
                        body, attachments_list = self.__handle_fetch_body_and_attachments(message)
                    else:
                        # if the message isn't multipart, just extract it
                        body = self.__handle_only_fetched_body(message)

                    # Parsing html content to readable string
                    body = BeautifulSoup(body, "html.parser").text

                    email_obj = EmailHandler(mail_from_user_obj, to_user_obj_list, cc_user_obj_list,
                                             mail_subject, attachments_list, body, message_id, mail_time)
                    fetched_emails_list.append(email_obj)

        return fetched_emails_list

    @staticmethod
    def summarize_body(email_obj: EmailHandler, body_slice_length: int = 50) -> str:
        """
        method to summarize the body of provided email object and returns the formatted string having details of mail
        like notification.
        Takes email object and body_slice_length as parameters
        returns formatted string notification
        """
        username: str = email_obj.sender_user_obj.username
        subject: str = email_obj.subject
        date_time: datetime = email_obj.mail_time
        empty_lines_removed_body: str = "".join(email_obj.content.splitlines())
        if "." in empty_lines_removed_body and (index := empty_lines_removed_body.index(".")) < body_slice_length:
            summarized_body = empty_lines_removed_body[:index + 1]
        elif "." in empty_lines_removed_body and (empty_lines_removed_body.index(".")) > body_slice_length:
            summarized_body = empty_lines_removed_body[:body_slice_length] + "..."
        elif "." not in empty_lines_removed_body and len(empty_lines_removed_body) < body_slice_length:
            summarized_body = empty_lines_removed_body
        else:
            summarized_body = empty_lines_removed_body[:body_slice_length] + "..."

        summarized_content: str = f"""
                                  {username}        {date_time.strftime("%H:%M")}
                                  {subject}
                                  {summarized_body} 
                                  """

        return summarized_content

    def delete_mail(self, email_obj: EmailHandler) -> bool:
        """
        Method to delete particular mail from the server of the service
        Takes email object as parameter and return bool for success confirmation
        """

        delete_mails_list: List[EmailHandler] = self.search_mails(email_obj=email_obj, mark_read=True,
                                                                  delete_mails=True)
        if not delete_mails_list:
            return True
        else:
            return False

    def search_mails(self, email_obj: EmailHandler | None = None, from_address: str | None = None,
                     contained_string_subject: str | None = None, contained_string_body: str | None = None,
                     on_date: str | None = None, from_date: str | None = None,
                     before_date: str | None = None, mail_section: str = "inbox", mark_read: bool = False,
                     delete_mails: bool = False) -> List[EmailHandler]:
        searched_mails: List[EmailHandler] = list()
        message_id: str | None = email_obj.message_id if email_obj else None
        if message_id is None and from_address is None and contained_string_body is None \
                and contained_string_subject is None and from_date is None and before_date is None:
            return searched_mails
        if message_id is not None:
            msg_id_query: str = f'HEADER Message-ID "{message_id}"'
        else:
            msg_id_query: str = f''
        if from_address is not None:
            from_query: str = f'FROM "{from_address}"'
        else:
            from_query: str = f''
        if contained_string_subject is not None:
            subject_query: str = f'SUBJECT "{contained_string_subject}"'
        else:
            subject_query: str = f''
        if contained_string_body is not None:
            body_query: str = f'TEXT "{contained_string_body}"'
        else:
            body_query: str = f''
        if on_date is not None:
            date = parser.parse(on_date)
            formatted_on_date = date.strftime("%d-%b-%Y")
            on_date_query: str = f'ON {formatted_on_date}'
        else:
            on_date_query: str = f''
        if from_date is not None:
            date = parser.parse(on_date)
            formatted_on_date = date.strftime("%d-%b-%Y")
            from_date_query: str = f'ON {formatted_on_date}'
        else:
            from_date_query: str = f''
        if before_date is not None:
            date = parser.parse(on_date)
            formatted_on_date = date.strftime("%d-%b-%Y")
            before_date_query: str = f'ON {formatted_on_date}'
        else:
            before_date_query: str = f''

        search_combination: str = f'{msg_id_query} {from_query} {subject_query} {body_query} {on_date_query} ' \
                                  f'{from_date_query} {before_date_query}'
        search_query: str = f'({search_combination.strip()})'

        if not mark_read:
            self.__imap_server.select(mail_section, readonly=True)
        else:
            self.__imap_server.select(mail_section)

        searched_mails += self.fetch_mails_from_server(search_query, delete_mails=delete_mails)

        return searched_mails

    @staticmethod
    def download_attachment(attachment: Attachment):
        """
        Method to download provided attachment
        Takes attachment object as input
        """
        open(attachment.file_path, 'wb').write(attachment.payload)

    @staticmethod
    def __load_delete_rules(delete_rules_file_path: str):
        if file_exist(delete_rules_file_path):
            with open(delete_rules_file_path, "r") as json_fl:
                json_content = json.load(json_fl)

            return json_content["sender_mail_id_list"], json_content["subject_container_string_list"], \
                   json_content["body_container_string_list"]
        else:
            json_content = {
                "sender_mail_id_list": [],
                "subject_container_string_list": [],
                "body_container_string_list": []
            }
            json_content = json.dumps(json_content, indent=2)
            with open(delete_rules_file_path, "w") as json_fl:
                json_fl.write(json_content)

    def __delete_if_required(self, email_obj: EmailHandler, delete_rule_file_path: str) -> bool:
        sender_mail_id_list, subject_container_string_list, body_container_string_list = \
            self.__load_delete_rules(delete_rule_file_path)
        if email_obj.sender_user_obj.email_address in sender_mail_id_list or \
            any(subject_container_string in email_obj.subject.lower() for subject_container_string in
                subject_container_string_list) or \
                any(body_container_string in email_obj.content.lower() for body_container_string in
                    body_container_string_list):
            return self.delete_mail(email_obj)
        else:
            return False

    def get_mail_objects_of_given_subject(self, delete_rule_file_path: str, match_subject_string: str,
                                          loop_wait_time: int = 2, number_of_mails=10, fetch_unread=False,
                                          mark_read=True) -> List[EmailHandler]:
        return_email_obj_list = []
        email_list: List[EmailHandler] = \
            self.read_mail(number_of_mails=number_of_mails, unread=fetch_unread, mark_read=mark_read)
        for email_obj in email_list:

            if email_obj.subject == match_subject_string:
                return_email_obj_list.append(email_obj)

            self.__delete_if_required(email_obj, delete_rule_file_path)

            # else not required: if subject is not appropriate for group creation or
            # message_id already exist in class data-member then ignore process and loop to next mail object
            time.sleep(loop_wait_time)
        return return_email_obj_list
