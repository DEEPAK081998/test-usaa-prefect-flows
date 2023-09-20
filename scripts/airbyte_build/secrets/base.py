from abc import ABC, abstractmethod


class BaseSecretResolver(ABC):
    """
    Base class for secret rsolver
    """
    BASE_KEY = 'connectionConfiguration'
    CONNECTOR_TYPE = NotImplemented

    @staticmethod
    def _generate_placeholder(connector_type: str, connector_name: str, key_name: str) -> str:
        """
        Creates a variable name which will be placed in key value instead of orignal value
        :param connector_type: type of connector(source, destination)
        :param connector_name: name of the connector
        :param key_name: the key name for whose the placeholder will be generated
        :return: generated variable name
        """
        return f'${connector_type}_{connector_name}_{key_name}'.replace(' ', '_').upper()

    def change_key_values(self, data_dict: dict, keys: list, connector_name: str) -> dict:
        """
        Change the value of keys in dict with the generated placeholder value
        :param data_dict: data dict in which values should be changed
        :param keys: the keys for which the values are changed
        :param connector_name: the name of the connector for which the data_dict belong to
        :return: modified data_dict
        """
        for key in keys:
            if not data_dict.get(key):
                raise KeyError(f'Invalid Key {key} for {self.CONNECTOR_TYPE} connector {connector_name}')
            data_dict[key] = self._generate_placeholder(
                connector_type=self.CONNECTOR_TYPE,
                connector_name=connector_name,
                key_name=key
            )
        return data_dict

    @abstractmethod
    def resolve_secrets(self, data_dict: dict) -> dict:
        """
        The main function which resolve the secrets in data_dict
        :param data_dict: data dict
        :return: modified data dict
        """
        raise NotImplementedError('Sub class should implement this function')
