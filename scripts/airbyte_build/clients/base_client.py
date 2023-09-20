import json
from typing import Union

import requests


class BaseAPIClass:
    BASE_URL = NotImplemented
    API_KEY_NAME = NotImplemented

    def __init__(self, api_key: str = None):
        self.api_key = api_key

    @staticmethod
    def join_path(path1: str, path2: str) -> str:
        """
        concatenate Base url with the given path and return the url
        :param path1: path to join
        :param path2: path to join
        :return: final url
        """
        if path1[-1] == '/' and path2[0] == '/':
            path2 = path2[1:]
        elif path1[-1] != '/' and path2[0] != '/':
            path2 = '/' + path2
        return path1 + path2

    def _compose_url(self, path: str) -> str:
        """
        concatenate Base url with the given path and return the url
        :param path: endpoint to append in base url
        :return: final url
        """
        return self.join_path(self.BASE_URL, path)

    def _generate_headers(self, custom_headers: dict) -> dict:
        """
        create the headers for the api and add api key if available
        :param custom_headers: extra headers to add in the api
        :return: final headers dict
        """
        headers = {'content-type': 'application/json'}
        if self.api_key and self.API_KEY_NAME:
            headers.update({self.API_KEY_NAME: self.api_key})
        if custom_headers:
            headers.update(custom_headers)
        return headers

    def _handle_response(self, response: requests.Response) -> (int, dict):
        """
        resolve the response and return the status code and message
        :param response: response to resolve
        :return: status code and message
        :rtype: (int, dict)
        """
        data = {}
        if response.content != b'':
            data = json.loads(response.content)
        status_code = response.status_code
        return status_code, data

    def get(self, path: str, param: dict = None, headers: dict = None) -> (int, dict):
        """
        Makes the GET request
        :param path: api to make the request
        :param param: extra params
        :param headers: extra headers
        :return: status code and response message
        """
        url = self._compose_url(path)
        headers = self._generate_headers(headers)
        response = requests.get(url=url, headers=headers, params=param)
        return self._handle_response(response)

    def post(self, path: str, data: Union[dict, bytes, list, str] = None, headers: dict = None) -> (int, dict):
        """
        Makes the POST request
        :param path: api to make the request
        :param data: data to post
        :param headers: extra headers
        :return: status code and response message
        """
        url = self._compose_url(path)
        headers = self._generate_headers(headers)
        if data and type(data) != str:
            data = json.dumps(data, default=str)
        response = requests.post(url=url, data=data, headers=headers)
        return self._handle_response(response)
