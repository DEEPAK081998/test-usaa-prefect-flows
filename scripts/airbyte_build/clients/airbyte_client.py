import logging
from http import HTTPStatus

import constants
from clients.base_client import BaseAPIClass

logging.basicConfig(level=logging.INFO)
logging.getLogger('airbyte').setLevel(level=logging.INFO)


class AirbyteAPIClass(BaseAPIClass):

    def __init__(self, base_url: str) -> None:
        """
        Initialize the Airbyte API class
        :param base_url: API base url
        """
        super().__init__()
        self.BASE_URL = base_url

    @staticmethod
    def _check_for_error(status_code: int, data: dict or str) -> None:
        """
        Check if the response have any error or not
        :param status_code: Response status code
        :param data: response data
        """
        if status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
            raise ValueError(f'Validation failed got response {status_code}, error {data}')
        elif status_code == HTTPStatus.NOT_FOUND:
            raise ConnectionError(f'Object with given id was not found got response {status_code}, error {data}')
        elif status_code != HTTPStatus.OK:
            raise ConnectionError(f'Got response {status_code}, error: {data}')

    def _create(self, body: dict, path: str) -> dict:
        """
        Makes a Create request
        :param body: request body
        :param path: api path
        :return: response dict
        """
        path = self.join_path(path, constants.CREATE)
        logging.debug(f'body={body} path={path}')
        status_code, data = self.post(path=path, data=body)
        self._check_for_error(status_code=status_code, data=data)
        return data

    def _update(self, body: dict, path: str) -> dict:
        """
        Makes a Update request
        :param body: request body
        :param path: api path
        :return: response dict
        """
        path = self.join_path(path, constants.UPDATE)
        logging.debug(f'body={body} path={path}')
        status_code, data = self.post(path=path, data=body)
        self._check_for_error(status_code=status_code, data=data)
        return data

    def _get(self, body: dict, path: str) -> dict:
        """
        Makes a Get request
        :param body: request body
        :param path: api path
        :return: response dict
        """
        path = self.join_path(path, constants.GET)
        logging.debug(f'body={body} path={path}')
        status_code, data = self.post(path=path, data=body)
        self._check_for_error(status_code=status_code, data=data)
        return data

    def _list(self, path: str, data=None):
        """
        Makes a List request
        :param path: api path
        :return: response dict
        """
        path = self.join_path(path, constants.LIST)
        logging.debug(f'path={path}')
        status_code, data = self.post(path=path, data=data)
        self._check_for_error(status_code=status_code, data=data)
        return data

    def create_workspace(self, body: dict) -> dict:
        """
        Creates a Workspace
        :param body: Workspace data
        :return: Workspace data
        """
        return self._create(body=body, path=constants.WORKSPACES)

    def create_source(self, body: dict) -> dict:
        """
        Creates a Source
        :param body: Source data
        :return: Source data
        """
        return self._create(body=body, path=constants.SOURCES)

    def create_destination(self, body: dict) -> dict:
        """
        Creates a Destination
        :param body: Destination data
        :return: Destination data
        """
        return self._create(body=body, path=constants.DESTINATIONS)

    def create_connection(self, body: dict) -> dict:
        """
        Creates a Connection
        :param body: Connection data
        :return: Connection data
        """
        return self._create(body=body, path=constants.CONNECTIONS)

    def update_source(self, body: dict) -> dict:
        """
        Updates the Source
        :param body: Source data
        :return: Source data
        """
        return self._update(body=body, path=constants.SOURCES)

    def update_destination(self, body: dict) -> dict:
        """
        Updates the Destination
        :param body: Destination data
        :return: Destination data
        """
        return self._update(body=body, path=constants.DESTINATIONS)

    def update_connection(self, body: dict) -> dict:
        """
        Updates the Connection
        :param body: Connection data
        :return: Connection data
        """
        return self._update(body=body, path=constants.CONNECTIONS)

    def get_connection(self, connection_id) -> dict:
        """
        Fetch the Connection
        :param connection_id: connection id
        :return: Connection data
        """
        return self._get(body={constants.CONNECTION_ID: connection_id}, path=constants.CONNECTIONS)

    def get_source(self, source_id) -> dict:
        """
        Fetch the Source
        :param source_id: source id
        :return: source data
        """
        return self._get(body={constants.SOURCE_ID: source_id}, path=constants.SOURCES)

    def get_destination(self, destination_id) -> dict:
        """
        Fetch the Destination
        :param destination_id: destination id
        :return: destination data
        """
        return self._get(body={constants.DESTINATION_ID: destination_id}, path=constants.DESTINATIONS)

    def list_workspaces(self) -> list:
        """
        List all workspaces
        :return: Workspace list
        """
        data = self._list(path=constants.WORKSPACES)
        return data[constants.WORKSPACES]

    def list_source_definitions(self) -> list:
        """
        List all Sources
        :return: Sources list
        """
        data = self._list(path=constants.SOURCE_DEFINITIONS)
        return data['sourceDefinitions']

    def list_destination_definitions(self) -> list:
        """
        List all Destination
        :return: Destination list
        """
        data = self._list(path=constants.DESTINATION_DEFINITIONS)
        return data['destinationDefinitions']

    def list_connections(self, workspace_id: str) -> list:
        data = self._list(path=constants.CONNECTIONS, data={constants.WORKSPACE_ID: workspace_id})
        return data[constants.CONNECTIONS]
