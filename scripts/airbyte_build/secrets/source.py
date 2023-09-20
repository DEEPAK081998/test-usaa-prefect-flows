import constants
from secrets.base import BaseSecretResolver


class SourceSecretResolver(BaseSecretResolver):
    """
    Class for resolving Source connector secrets
    """
    CONNECTOR_TYPE = 'Source'
    SOURCE_SECRETS_CONNECTOR_NAME_MAP = {
        constants.SourceConnectorName.SENDGRID: '_sendgrid',
        constants.SourceConnectorName.SENDGRID_HOMESTORY: '_sendgrid',
        constants.SourceConnectorName.POSTGRES: '_postgres',
        constants.SourceConnectorName.MYSQL: '_mysql',
        constants.SourceConnectorName.TWILIO: '_twilio',
        constants.SourceConnectorName.GOOGLE_ANALYTICS: '_google_analytics',
        constants.SourceConnectorName.DYNAMO_INVITATIONS: '_dynamo'
    }

    def resolve_secrets(self, data_dict: dict) -> dict:
        """
        The main function which resolve the secrets in data_dict
        :param data_dict: data dict
        :return: modified data dict
        """
        function_name = self.SOURCE_SECRETS_CONNECTOR_NAME_MAP.get(data_dict[constants.SOURCE_DEFINITION_NAME])
        if function_name:
            data_dict[self.BASE_KEY] = getattr(self, function_name)(data_dict[self.BASE_KEY])
        return data_dict

    def _sendgrid(self, data_dict: dict) -> dict:
        """
        Resolves secret for Sendgrid connector
        :param data_dict: data dict
        :return: resolved data dict
        """
        keys = ['apikey']
        return self.change_key_values(
            data_dict=data_dict,
            keys=keys,
            connector_name=constants.SourceConnectorName.SENDGRID
        )

    def _postgres(self, data_dict: dict) -> dict:
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

    def _mysql(self, data_dict: dict) -> dict:
        """
        Resolves secret for MySQL connector
        :param data_dict: data dict
        :return: resolved data dict
        """
        keys = ['password', 'username']
        return self.change_key_values(
            data_dict=data_dict,
            keys=keys,
            connector_name=constants.SourceConnectorName.MYSQL
        )

    def _google_analytics(self, data_dict: dict) -> dict:
        """
        Resolves secret for Google Analytics connector
        :param data_dict: data dict
        :return: resolved data dict
        """
        keys = list()
        auth_type = data_dict['credentials']['auth_type']
        if auth_type == 'Client':
            keys = ['client_id', 'access_token', 'client_secret', 'refresh_token']
        else:
            keys = ['credentials_json']
        data_dict['credentials'] = self.change_key_values(
            data_dict=data_dict['credentials'],
            keys=keys,
            connector_name=constants.SourceConnectorName.GOOGLE_ANALYTICS
        )
        return data_dict

    def _twilio(self, data_dict: dict) -> dict:
        """
        Resolves secret for Twilio connector
        :param data_dict: data dict
        :return: resolved data dict
        """
        keys = ['auth_token', 'account_sid']
        return self.change_key_values(
            data_dict=data_dict,
            keys=keys,
            connector_name=constants.SourceConnectorName.TWILIO
        )

    def _dynamo(self, data_dict: dict) -> dict:
        """
        Resolves secret for Dynamo connector
        :param data_dict: data dict
        :return: resolved data dict
        """
        keys = ['access_key_id', 'secret_access_key']
        return self.change_key_values(
            data_dict=data_dict,
            keys=keys,
            connector_name=constants.SourceConnectorName.DYNAMO_INVITATIONS
        )
