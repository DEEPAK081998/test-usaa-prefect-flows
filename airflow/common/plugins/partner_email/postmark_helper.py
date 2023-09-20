"""
References:
https://postmarkapp.com/developer/user-guide/send-email-with-api/batch-emails
https://postmarkapp.com/blog/how-to-send-and-track-the-status-of-email-using-python-and-the-postmark-api
"""


import json
import logging
from urllib.parse import urlencode

import pandas as pd
import requests
from partner_email.aws_helper import read_secret


def to_json(payload_dict: dict) -> str:
    """Return json if input is dict."""

    if isinstance(payload_dict, list):
        payload_json = json.dumps(payload_dict)
        return payload_json
    else:
        return payload_dict


class PostmarkHelper:
    def __init__(self, postmark_api_secret: str):
        self.token = read_secret(postmark_api_secret)
        self.base_url = 'https://api.postmarkapp.com'
        self.headers = {
            'Content-Type': 'application/json',
            'X-Postmark-Server-Token': self.token,
            'Accept': 'application/json'
        }

    def _request(self,
                 method: str,
                 specific_url: str,
                 data: dict = dict()
                 ) -> requests.Response:
        """Return endpoint response based on method and specific_url."""

        if not data:
            response = requests.request(
                method=method,
                url=self.base_url + specific_url,
                headers=self.headers
            )
        else:
            response = requests.request(
                method=method,
                url=self.base_url + specific_url,
                headers=self.headers,
                data=data
            )

        res_text = response.text
        logging.info(f"response.status_code = {response.status_code}")
        logging.debug(f"response.json = {response.json()}")
        logging.debug(f"res_text = {res_text}")

        return response.json()

    def send_batch_emails(self, payload):
        """Send batch emails.

        Limited to 500 emails and 50 MB payload size.
        """

        payload_json = to_json(payload)
        assert isinstance(payload_json, str)

        response = self._request(
            method='POST',
            specific_url='/email/batch',
            data=payload_json
        )
        return response

    def list_metadata(self, sent_id: str, max_messages=500, offset=0):
        query_kwargs = {
            'count': max_messages,
            'offset': offset,
            'metadata_sent_id': sent_id,
        }
        query_string = urlencode(query_kwargs)
        response = self._request(
            method='GET',
            specific_url=f"/messages/outbound?{query_string}"
        )
        return response

    def list_all_metadata(self, sent_id: str, max_messages=500):

        logging.info(f"Looking for sent_id = '{sent_id}'")
        response = self.list_metadata(sent_id)
        total_count = response['TotalCount']
        number_of_api_calls = total_count // max_messages + 1
        messages = list()
        for i in range(number_of_api_calls):
            # iterate over each api call
            offset = 500 * i
            response = self.list_metadata(sent_id, max_messages, offset)
            messages.extend(response['Messages'])

        df = pd.DataFrame.from_records(messages)
        logging.info(f"df_duplicated_metadata = \n{df}")
        return df
