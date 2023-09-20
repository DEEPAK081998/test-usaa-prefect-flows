import json
import logging
import time
from typing import List

import requests
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from pennymac_clicks.helpers.aws_helper import read_secret
from pennymac_clicks.helpers.variable_helper import sendgrid_secret_id


class SendGridHelper:
    def __init__(self):
        self.api_key = read_secret(sendgrid_secret_id())['api_key']
        assert self.api_key
        self.base_url = 'https://api.sendgrid.com'
        self.headers = {
            'authorization': f"Bearer {self.api_key}",
            'content-type': "application/json"
        }

    def _request_response(self,
                          method: str,
                          specific_url: str,
                          payload: dict = dict()
                          ) -> requests.Response:
        """Return endpoint response based on method and specific_url."""

        if not payload:
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
                data=json.dumps(payload)
            )

        res_text = response.text
        logging.info(f"response.status_code = {response.status_code}")
        logging.info(f"response.json = {response.json}")
        logging.info(f"res_text = {res_text}")

        if response.status_code not in [200, 202]:
            raise AirflowFailException('API response is not 200 or 202.')

        return response.json()

    def add_or_update_a_contact(self, list_id: str, contacts: List):
        """Add or update a contact.
        Limited to 100 contacts.
        https://docs.sendgrid.com/api-reference/contacts/add-or-update-a-contact
        """

        response = self._request_response(
            method='PUT',
            specific_url='/v3/marketing/contacts',
            payload={
                'list_ids': [list_id],
                'contacts': contacts
            }
        )
        return response

    def get_contacts_by_emails(self, emails: List):
        """Get contacts by emails.
        https://docs.sendgrid.com/api-reference/contacts/get-contacts-by-emails
        """

        response = self._request_response(
            method='POST',
            specific_url='/v3/marketing/contacts/search/emails',
            payload={
                'emails': emails
            }
        )
        return response


@task(retries=3)
def confirm_added_contacts(emails_to_add: List[str]):
    """Confirm that hte given emails are correctly added to SendGrid."""

    confirmed_emails = list()

    if emails_to_add:
        # wait for 5 seconds
        time.sleep(5)

        # limit to 100 due to API limitation
        emails_to_add = emails_to_add[:100]

        sg = SendGridHelper()
        response = sg.get_contacts_by_emails(emails_to_add)
        contacts = response['result']
        for email, info in contacts.items():
            if 'contact' in info:
                confirmed_emails.append(email)
            if 'error' in info:
                raise ValueError(f"Not all emails were added to SendGrid: {email}")

        logging.info(f"contacts = {contacts}")
        logging.info(f"confirmed_emails = {confirmed_emails}")
