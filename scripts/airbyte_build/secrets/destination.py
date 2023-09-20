import constants
from secrets.base import BaseSecretResolver


class DestinationSecretResolver(BaseSecretResolver):
    """
    Class for resolving Destination connector secrets
    """
    CONNECTOR_TYPE = 'Destination'
    DESTINATION_SECRETS_CONNECTOR_NAME_MAP = {
        constants.SourceConnectorName.SENDGRID: '_sendgrid',
        constants.SourceConnectorName.SENDGRID_HOMESTORY: '_sendgrid',
        constants.SourceConnectorName.POSTGRES: '_postgres'
    }

    def resolve_secrets(self, data_dict: dict) -> dict:
        """
        The main function which resolve the secrets in data_dict
        :param data_dict: data dict
        :return: modified data dict
        """
        function_name = self.DESTINATION_SECRETS_CONNECTOR_NAME_MAP.get(
            data_dict[constants.DESTINATION_DEFINITION_NAME]
        )
        if function_name:
            data_dict[self.BASE_KEY] = getattr(self, function_name)(data_dict[self.BASE_KEY])
        return data_dict

    def _postgres(self, data_dict) -> dict:
        """
        Resolves secret for Postgres connector
        :param data_dict: data dict
        :return: resolved data dict
        """
        keys = ['password', 'username']
        return self.change_key_values(
            data_dict=data_dict,
            keys=keys,
            connector_name=constants.SourceConnectorName.POSTGRES
        )
