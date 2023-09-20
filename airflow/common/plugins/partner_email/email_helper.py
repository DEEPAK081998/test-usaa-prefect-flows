import base64
import json
import logging
import os
import re
import sys
from typing import List

import pandas as pd
from partner_email.config_helper import EmailConfig
from partner_email.date_helper import DateHelper
from partner_email.general_config import (
    BI_EMAIL, CARY_EMAIL, DEV_EMAIL, JTG_DEV_EMAIL, KRIS_EMAIL, MARKO_EMAIL, MAX_BATCH_LENGTH, MAX_BATCH_SIZE,
    PAYLOAD_JSON, TEST_RECIPIENTS, TIJANA_EMAIL, VIC_EMAIL)  # REP-1775
from partner_email.postmark_helper import PostmarkHelper
from partner_email.s3_helper import read_csv_from_s3, read_file_from_s3
from partner_email.template_helper import HTMLParser


def check_unique_email_sent(report_type: str, send_to: str, postmark_api_secret: str) -> bool:
    """Check if emails have already been sent."""
    sent_id = assemble_sent_id(report_type, send_to)
    df = PostmarkHelper(postmark_api_secret).list_all_metadata(sent_id)
    if df.empty:
        uniqueness = True
    else:
        uniqueness = False
    print(f"uniqueness = {uniqueness}")
    return uniqueness


def assemble_sent_id(report_type: str, send_to: str) -> str:
    """Assemble sent_id based in report_type, sent_to, and current date."""
    today = DateHelper.get_today_date()
    sent_id = f"{send_to}_{report_type}_{today}"
    return sent_id


def convert_email_to_name(email: str) -> str:
    """Create full name based on email address."""

    email_without_domain = extract_prefix_from_email(email)
    email_parts = email_without_domain.split('.')
    email_parts_in_title = [email.title() for email in email_parts]
    name = ' '.join(email_parts_in_title)
    return name


def concatenate_test_recipients():
    """Concatenate all recipients."""

    recipients = ','.join(TEST_RECIPIENTS)
    logging.debug(f"recipients = {recipients}")
    return recipients

def concatenate_recipients(RECIPIENTS):
    """Concatenate all recipients."""

    recipients = ','.join(RECIPIENTS)
    logging.debug(f"recipients = {recipients}")
    return recipients


def assemble_email_prefix_from_name(name: str) -> str:
    """Return email prefix by converting 'name' to lower case separated by a dot.

    Example:
        name "John Doe" returns "john.doe".
    """

    email_prefix = '.'.join(name.lower().split(' '))
    return email_prefix


def extract_prefix_from_email(full_email: str) -> str:
    """Extract email prefix by dropping domain from email address. Drop all after '@'."""

    if isinstance(full_email, str):
        left_part = full_email.split('@')[0]
        return left_part
    else:
        return full_email


def encode_file(s3_bucket_name, filename: str):
    """Encode file."""

    data = read_file_from_s3(s3_bucket=s3_bucket_name, s3_key=filename, binary=True)
    base64_encoded = base64.b64encode(data).decode('UTF-8')
    return base64_encoded


def _reoder_and_drop_airbyte_columns(df: pd.DataFrame, columns_order: List[str]) -> pd.DataFrame:
    """Reorder columns. Remove Airbyte metadata columns."""
    df.drop(columns=['_airbyte_ab_id', '_airbyte_emitted_at'], inplace=True)
    df = df.reindex(columns=columns_order)
    return df


def _encode_df_for_email_attachment(df: pd.DataFrame) -> str:
    """Encode dataframe for csv email attachment."""
    encoded_df_csv = df.to_csv(index=False).encode('utf-8')
    encoded_attachment = base64.b64encode(encoded_df_csv).decode('UTF-8')
    return encoded_attachment


def process_csv_attachment(config: EmailConfig, report_relative_path: str) -> str:
    """Read csv from S3. Reorder columns. Drop Airbyte columns. Enconde csv for attachment."""

    df = read_csv_from_s3(config.s3_bucket_name, report_relative_path)
    df = _reoder_and_drop_airbyte_columns(df, config.columns_order)
    encoded_attachment = _encode_df_for_email_attachment(df)
    return encoded_attachment


def get_attachment_name(fullpath: str, name_mask: str) -> str:
    """Get attachment name, either from full path or from name_mask."""

    def _get_report_name_from_fullpath(fullpath: str) -> str:
        attachment_name = os.path.basename(fullpath)
        return attachment_name

    def _mask_name_based_on_template(attachment_name_mask):
        kwargs = {
            'date': DateHelper.get_today_date(),
        }
        report_name = attachment_name_mask.format(**kwargs)
        return report_name

    if name_mask:
        attachment_name = _mask_name_based_on_template(name_mask)
    else:
        attachment_name = _get_report_name_from_fullpath(fullpath)

    return attachment_name


def read_html_content(filename: str):
    """Read HTML content."""

    with open(filename, mode='r') as f:
        html_content = f.read()

    return html_content


class Attachment:
    def __init__(self,
                 name: str,
                 content: str,
                 content_type: str = 'application/vnd.ms-excel') -> None:

        self.Name: str = name
        self.Content: str = content
        self.ContentType: str = content_type

    def __repr__(self) -> str:
        """Return object attributes when printing an object."""
        return self.to_dict()

    def __str__(self) -> str:
        """Return object attributes when printing an object."""
        return str(self.to_dict())

    def to_dict(self) -> dict:
        """Get all attributes."""
        return self.__dict__


class EmailComposer:
    def __init__(
        self,
        from_: str,
        to: str,
        subject: str,
        html_body: str,
        tag: str,
        report_type: str,
        sent_id: str,
        cc_email: str,
        reply_to: str,
        attachments: List[Attachment] = None
    ) -> None:
        self.From = from_
        self.To = to
        self.Subject = subject
        self.HtmlBody = html_body
        self.Attachments: List[Attachment] = attachments
        self.Tag: str = tag
        self.Metadata: dict = {'report_type': report_type, 'sent_id': sent_id}
        self.CC: str = cc_email
        self.ReplyTo: str = reply_to

    def __repr__(self) -> str:
        """Return object attributes when printing an EmailComposer object."""
        return str(self.get_attributes())

    def __str__(self) -> str:
        return f"Email to: '{self.To}'"

    def get_attributes(self) -> dict:
        """Get all attributes from class instance."""
        attributes = self.__dict__
        return attributes

    def get_payload(self) -> str:
        """Prepare payload json."""
        payload_json = json.dumps(self.get_attributes())
        return payload_json

    def export(self):
        """Export payload to json."""
        attributes = self.get_attributes()
        with open(PAYLOAD_JSON, 'w') as f:
            json.dump(attributes, f, indent=4)
        logging.info(f"Exported payload json to = {PAYLOAD_JSON}")

    def get_payload_size(self):
        """Get size of this object's payload."""
        payload = self.get_payload()
        size = sys.getsizeof(payload)
        logging.info(f"size = {size} bytes")
        return size


class Batch:
    def __init__(self,
                 size_tolerance: int = 1000,
                 max_batch_size: int = MAX_BATCH_SIZE,
                 max_batch_len: int = MAX_BATCH_LENGTH,
                 ) -> None:
        self.size_tolerance = size_tolerance
        self.max_batch_size = max_batch_size * 1024  # in kilo bytes
        self.max_batch_len = max_batch_len
        logging.info(f"Maximum number of emails in a batch: {self.max_batch_len}")
        logging.info(f"Maximum batch size: {self.max_batch_size} Kb")

        self._init_batch_variables_and_bytes_sum()

    def _init_batch_variables_and_bytes_sum(self):
        """Initialize aggregation variables."""
        self.bytes_sum: int = 0
        self.batch: list = []
        self.batch_index: list = []
        self.batches: list = []
        self.batches_index: list = []

    def _check_email_size(self, email_size: int):
        """Check if email size is inside limit."""
        check = email_size < self.max_batch_size
        if not check:
            logging.error(f"File ({email_size}) is above limit '({self.max_batch_size})'.")
        return

    def _get_email_size(self, message: str) -> int:
        """Get email size with tolerance."""

        message_json = json.dumps(message)
        email_size = sys.getsizeof(message_json) + self.size_tolerance
        email_size_in_kb = email_size / 1024
        return email_size_in_kb

    def _exceed_size(self, email_size: int):
        """Check if email size has passed limits."""
        total_size = email_size + self.bytes_sum
        return total_size >= self.max_batch_size

    def _exceed_length(self):
        """Check inside batch length limit."""
        batch_length = len(self.batch)
        return batch_length >= self.max_batch_len

    def _add_email_to_batch(self, index: int, message: str, email_size: int):
        """Add email to current batch."""
        self.batch_index.append(index)
        self.batch.append(message)
        self.bytes_sum += email_size
        batch_percentage = (self.bytes_sum / self.max_batch_size) * 100

        msg = [
            f"Email size: {round(email_size, 2)} Kb",
            f"Kb sum: {round(self.bytes_sum, 2)} Kb",
            f"Batch percentage: {round(batch_percentage, 2)}%"
        ]

        logging.debug('. '.join(msg))

    def _reset_batch(self):
        """Reset batch list and count of total bytes."""
        self.batch = []
        self.batch_index = []
        self.bytes_sum = 0

    def _add_current_batch(self):
        """Add current batch to 'all_batches'."""
        self.batches.append(self.batch)
        self.batches_index.append(self.batch_index)

    def _add_last_batch(self):
        """Add last batch to 'all_batches'."""
        self._add_current_batch()

    def _log(self):
        """Log batches metadata."""
        logging.info(f"Number of batches = {len(self.batches_index)}")
        logging.info(f"self.batches_index = {self.batches_index}")
        logging.info('Finished to process batches.______________________________________________\n')

    def distribute_batches(self, messages: List[dict]) -> List[List[dict]]:
        """Distribute emails in batches respecting max length and size."""

        for i, message in enumerate(messages):
            email_size = self._get_email_size(message)
            self._check_email_size(email_size)

            if self._exceed_size(email_size) or self._exceed_length():
                self._add_current_batch()
                self._reset_batch()

            self._add_email_to_batch(i, message, email_size)

        self._add_last_batch()
        self._log()
        return self.batches


class EmailSender:
    @staticmethod
    def check_instructions_type(df_manifest: pd.DataFrame) -> List[List[str]]:
        """Check instructions type."""

        df = df_manifest
        columns = df.columns
        if 'report_relative_path' in columns and 'html_relative_path' in columns:
            instructions_type = 'report_and_html'
        elif 'report_relative_path' in columns:
            instructions_type = 'only_report'
        elif 'html_relative_path' in columns:
            instructions_type = 'only_html'
        else:
            raise Exception(f"Invalid instructions from manifest csv. Columns: {columns}")

        print(f"instructions_type = {instructions_type}")
        return instructions_type

    @staticmethod
    def fill_nan_column(df: pd.DataFrame, nan_col: str) -> pd.DataFrame:
        df[nan_col] = df[nan_col].fillna('')
        return df

    @staticmethod
    def prepare_instructions(
            df_manifest: pd.DataFrame,
            instructions_type: str) -> List[List[str]]:
        """Read sending instructions from csv file."""
        # TODO: Improve conditional logic below.

        df = df_manifest

        if instructions_type == 'only_report':
            # TODO: Rename column filename during manifest generation, not here. Temp fix bellow.
            df.rename(columns={'filename': 'report_relative_path'}, inplace=True)  # Temp fix

            df['html_relative_path'] = None
        elif instructions_type == 'only_html':
            df['report_relative_path'] = None
        else:
            df = EmailSender.fill_nan_column(df, 'report_relative_path')

        # handle reply_to_email, add if not existing
        if 'reply_to_email' not in df.columns:
            df['reply_to_email'] = ''
        else:
            df = EmailSender.fill_nan_column(df, 'reply_to_email')

        df = df.reindex(columns=[
            'name',
            'email',
            'html_relative_path',
            'report_relative_path',
            'reply_to_email'])

        instructions = df.values
        print(f"instructions = {instructions}")
        return instructions

    @staticmethod
    def reduce_emails_to_send(
            instructions: List[List[str]],
            limit: int) -> List[List[str]]:
        """Reduce number of emails to send. Only first emails will be sent."""

        print(f"Limiting emails send to = {limit}")
        print(f"instructions = {instructions}")
        if limit:
            return instructions[:limit]
        else:
            return instructions

    @staticmethod
    def confirm_send(send_to: str):
        """Ask the user to confirm sending emails to clients."""

        if send_to == 'clients':
            text = "Emails will be sent to clients. Type 'clients' if you wish to proceed: "
            confirmation_text = input(text)

            if confirmation_text == 'clients':
                print('Sending email to clients...')
                confirm = True
                return confirm
            else:
                quit()

    @staticmethod
    def select_recipient(email: str, send_to: str = 'dev') -> str:
        """Select email recipient from one of the options: dev, testers, clients, etc., or from
        the provided email in send_to."""

        OPTIONS = ['jtg-dev', 'dev', 'testers', 'clients', 'vic',
                   'marko', 'tijana', 'kris', 'bi', 'cary']

        # if: send_to is an email address
        if '@' in send_to:
            email_address = send_to

        # else: send_to should be in the OPTIONS. Raise exception if not.
        else:
            if send_to not in OPTIONS:
                raise Exception(f"Select one of: {OPTIONS}")

            recipients = {
                'bi': BI_EMAIL,
                'dev': DEV_EMAIL,
                'vic': VIC_EMAIL,
                'kris': KRIS_EMAIL,
                'marko': MARKO_EMAIL,
                'tijana': TIJANA_EMAIL,
                'cary': CARY_EMAIL,
                'testers': concatenate_test_recipients(),
                'clients': email,
                'jtg-dev': concatenate_recipients(JTG_DEV_EMAIL)
            }
            email_address = recipients[send_to]
        return email_address

    @staticmethod
    def select_cc(send_to: str, config: EmailConfig):
        """Select CC (carbon copy) based on who to send email to."""

        if config.email_cc:
            if send_to == 'clients':
                cc_email = config.email_cc
            else:
                cc_email = EmailSender.select_recipient('', send_to)
        else:
            cc_email = None
        return cc_email

    @staticmethod
    def check_report_exist(report_fullpath: str):
        """Check if report exists."""

        report_exist = os.path.isfile(report_fullpath)
        try:
            assert report_exist
        except Exception as e:
            logging.warning(f"No report in: {report_fullpath}. exc: {e}")

    def check_html_exist(html_relative_path: str):
        """Check if html file exists."""

        report_exist = os.path.isfile(html_relative_path)
        try:
            assert report_exist
        except Exception as e:
            logging.warning(f"No html in: {html_relative_path}. exc: {e}")

    @staticmethod
    def check_email_is_valid(email_address):
        """Check if email is valid."""

        email = email_address
        # regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'  For single email
        regex = r'^\s*([\w\.\+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-\.]+)(\s*,\s*([\w\.\+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-\.]+))*\s*$'  # noqa

        if isinstance(email, str):
            if (re.fullmatch(regex, email)):
                return True
            else:
                logging.error(f"Not a valid email: {email}")
                return False
        else:
            logging.error(f"Not a valid email: {email}")
            return False

    @staticmethod
    def check_for_attachment(instructions_path: str) -> bool:
        """Check if there are reports to be attached."""

        df = pd.read_csv(instructions_path)
        if 'report_relative_path' in df.columns:
            has_attachment = True
        else:
            has_attachment = False
        return has_attachment


def override_send_to(config: EmailConfig, send_to: str) -> str:  # REP-1775
    """Override send_to definition in variables with definition in config from json file."""

    if config.send_to:
        # Use specific `send_to`` from json dag parametrization file
        selected_send_to = config.send_to
    else:
        # Use general `send_to`` Airflow variables
        selected_send_to = send_to
    print(f"selected_send_to = {selected_send_to}")

    return selected_send_to


def send_emails_in_batches(
        config: EmailConfig,
        postmark_api_secret: str,
        send_to: str = 'dev',
        limit: int = None,
        send: bool = False
):
    """Send email according to instructions in manifest csv file.

    Args:
        send_to: One of 'dev', 'testers', 'clients'.
    """

    def prepare_payload_with_attachment(instructions):
        """Prepare payload with attachment."""

        payloads = list()
        for name, email_address, html_relative_path, report_relative_path, reply_to_email in instructions:  # noqa
            if not reply_to_email:
                reply_to_email = config.reply_to

            EmailSender.check_email_is_valid(email_address)
            if html_relative_path:
                # Open customized html file
                EmailSender.check_html_exist(html_relative_path)
                html_body = read_html_content(html_relative_path)
            else:
                # Customize template
                assert config.html_template_name
                html_body = HTMLParser(
                    template_filename=config.html_template_name,
                    s3_bucket_name=config.s3_bucket_name,
                    parameters={'recipient_name': name, 'signature': config.signature}
                ).load()

            if report_relative_path:
                attachment = Attachment(
                    name=get_attachment_name(report_relative_path, config.attachment_name_template),
                    # content=encode_file(config.s3_bucket_name, report_relative_path),
                    content=process_csv_attachment(config, report_relative_path),
                    content_type='application/vnd.ms-excel'
                )
                attachments = [attachment.to_dict()]
            else:
                attachments = None

            selected_send_to = override_send_to(config, send_to)  # REP-1775
            email_address = EmailSender.select_recipient(email_address, selected_send_to)
            composed_email = EmailComposer(
                from_=config.email_from,
                to=email_address,
                subject=config.email_subject,
                html_body=html_body,
                tag=config.email_tag,
                report_type=config.report_type,
                sent_id=assemble_sent_id(config.report_type, send_to),
                cc_email=EmailSender.select_cc(selected_send_to, config),  # REP-1775
                reply_to=reply_to_email,
                attachments=attachments
            )
            payload = composed_email.get_attributes()
            payloads.append(payload)
        return payloads

    def _prepare_batches():
        """Return email batches. It also export batches to a json file."""

        df_manifest = pd.read_csv(config.manifest_path)
        # df_manifest = read_csv_from_s3(config.s3_bucket_name, config.manifest_path)
        instructions_type = EmailSender.check_instructions_type(df_manifest)
        instructions = EmailSender.prepare_instructions(df_manifest, instructions_type)
        instructions = EmailSender.reduce_emails_to_send(instructions, limit)
        # EmailSender.confirm_send(send_to)
        payloads = prepare_payload_with_attachment(instructions)
        batches = Batch().distribute_batches(payloads)
        return batches

    def _send_batches(batches: List[List[dict]]):
        """Send email batches with requests."""

        logging.info('Sending email batches.')

        if send:
            responses = dict()
            for i, batch in enumerate(batches):
                responses.update({f"batch_{i}": PostmarkHelper(
                    postmark_api_secret).send_batch_emails(payload=batch)})

            # yaml_file = QA_SENDING_RESPONSE_YAML.format(
            #     report_type=config.report_type,
            #     today=DateHelper.get_today_date()
            # )
            # write_yaml({config.email_tag: responses}, yaml_file)  # TO DO implement in cloud

    unprecedented = check_unique_email_sent(config.report_type, send_to, postmark_api_secret)
    if unprecedented:
        batches = _prepare_batches()
        _send_batches(batches)
    else:
        raise Exception('Will not send emails because they have already been sent.')

    if not send:
        logging.warning("Only preparing batches. Set 'send' to True in order to send emails.")
