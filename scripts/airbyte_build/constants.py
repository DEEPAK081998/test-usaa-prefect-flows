LIST = 'list'
CREATE = 'create'
UPDATE = 'update'
GET = 'get'
DELETE = 'delete'

WORKSPACES = 'workspaces'
WORKSPACE = 'workspace'
WORKSPACE_ID = 'workspaceId'

SOURCE_DEFINITIONS = 'source_definitions'
SOURCE_DEFINITION_NAME = 'sourceDefinitionName'
SOURCE_DEFINITION_ID = 'sourceDefinitionId'
SOURCES = 'sources'
SOURCE = 'source'
SOURCE_NAME = 'sourceName'
SOURCE_ID = 'sourceId'
SOURCE_CONNECTOR_FILE = 'sourceConnectorFile'

DESTINATION_ID = 'destinationId'
DESTINATIONS = 'destinations'
DESTINATION = 'destination'
DESTINATION_NAME = 'destinationName'
DESTINATION_DEFINITIONS = 'destination_definitions'
DESTINATION_DEFINITION_NAME = 'destinationDefinitionName'
DESTINATION_DEFINITION_ID = 'destinationDefinitionId'
DESTINATION_CONNECTOR_FILE = 'destinationConnectorFile'

CONNECTIONS = 'connections'
CONNECTION = 'connection'
CONNECTION_ID = 'connectionId'

NAME = 'name'

PRIMARY_KEY = 'project_name'
SORT_KEY = 'file_name'

SAMPLE_WORKSPACE_DATA = {
    "email": "test@example.com",
    "anonymousDataCollection": False,
    "name": "string",
    "news": False,
    "securityUpdates": False,
    "notifications": [
        {
            "notificationType": "slack",
            "sendOnSuccess": False,
            "sendOnFailure": False,
            "slackConfiguration": {
                "webhook": "string"
            }
        }
    ],
    "displaySetupWizard": False
}


class SourceConnectorName:
    DYNAMO_INVITATIONS = 'Dynamo Invitations'
    SENDGRID = 'Sendgrid'
    SENDGRID_HOMESTORY = 'Sendgrid Homestory Connector'
    POSTGRES = 'Postgres'
    MYSQL = 'MySQL'
    GOOGLE_ANALYTICS = 'Google Analytics'
    TWILIO = 'Twilio'


class DestinationConnectorName:
    POSTGRES = 'Postgres'


STATE_FILE_NAME_PREFIX = 'state'
S3_PATH_PREFIX = 's3://'
ZIP_FILE_TEMPORARY_PATH = '/tmp/octavia_state_files.zip'
