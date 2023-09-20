import http
import json
import logging
import os
from os import walk
from typing import Any

import boto3

import constants

logging.basicConfig(level=logging.INFO)


class QuickSight:
    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize QuickSight class
        :param kwargs: additional kwargs required for initialization
        """
        self.deploy_datasets = kwargs['deploy_datasets']
        self.quicksight_directory = kwargs['quicksight_directory']
        self.projects_directory = kwargs['projects_directory']
        self.project_name_list = kwargs['projects']
        self.exclude = kwargs['exclude']
        self.datasets_list = kwargs['datasets']
        self.s3_quicksight_directory = kwargs['s3_quicksight_directory']
        self.s3_bucket_name = kwargs['s3_bucket_name']
        self.logger = logging.getLogger('quicksight_class')
        self.logger.setLevel(level=logging.INFO)

    @staticmethod
    def _generate_permission_map(permission_list: list) -> dict:
        """
        Parse the permission list and create a key,value dict of permissions where key correspond
        to the principal or user and value corresponds to the actions this role has
        :param permission_list: permissions list
        :return: key, value dict of permissions
        """
        return {permission['Principal']: permission['Actions'] for permission in permission_list}

    def _create_update_dataset(self, dataset_file: bytes) -> None:
        """
        Create or Updated the dataset provided by the dataset file
        :param dataset_file: dataset file
        """
        dataset = json.load(open(dataset_file))
        dataset_id = dataset[constants.DATASET_CONSTANTS.DATASET_ID]
        aws_account_id = dataset[constants.DATASET_CONSTANTS.AWS_ACCOUNT_ID]
        client = boto3.client('quicksight')
        create = False

        try:
            client.describe_data_set(
                AwsAccountId=aws_account_id,
                DataSetId=dataset_id
            )
        except client.exceptions.ResourceNotFoundException as e:
            self.logger.info(f'The given dataset {dataset_id} for account {aws_account_id} does not exists')
            create = True

        if create:
            self.logger.info(f'Creating dataset with id {dataset_id} in account {aws_account_id}')
            response = client.create_data_set(**dataset)
            if response['Status'] == http.HTTPStatus.CREATED:
                self.logger.info(f'Dataset successfully successfully')
            else:
                self.logger.warning(
                    f'Got response {response} when creating dataset {dataset_id} in account {aws_account_id}'
                )
        else:
            if dataset.get(constants.DATASET_CONSTANTS.PERMISSION):
                response = client.describe_data_set_permissions(
                    AwsAccountId=aws_account_id,
                    DataSetId=dataset_id
                )
                old_permissions_map = self._generate_permission_map(permission_list=response['Permissions'])
                new_permission_map = self._generate_permission_map(dataset[constants.DATASET_CONSTANTS.PERMISSION])
                grant_permissions = [
                    {'Principal': principal, 'Actions': action} for principal, action in new_permission_map.items() if
                    principal not in old_permissions_map
                ] if set(new_permission_map.keys()) - set(old_permissions_map.keys()) else []
                revoke_permissions = [
                    {'Principal': principal, 'Actions': action} for principal, action in old_permissions_map.items() if
                    principal not in new_permission_map
                ] if set(old_permissions_map.keys()) - set(new_permission_map.keys()) else []

                for principal, action in new_permission_map.items():
                    if principal in old_permissions_map:
                        old_action = old_permissions_map[principal]
                        new_grant_permission = list(set(action) - set(old_action))
                        new_revoke_permission = list(set(old_action) - set(action))
                        if new_grant_permission:
                            grant_permissions.append({'Principal': principal, 'Actions': new_grant_permission})
                        if new_revoke_permission:
                            revoke_permissions.append({'Principal': principal, 'Actions': new_revoke_permission})

                del dataset[constants.DATASET_CONSTANTS.PERMISSION]
                self.logger.info(f'Updating dataset with id {dataset_id} in account {aws_account_id}')
                response = client.update_data_set(**dataset)

                if response['Status'] == http.HTTPStatus.OK:
                    self.logger.info(f'Dataset updated successfully')
                else:
                    self.logger.warning(
                        f'Got response {response} when updating dataset {dataset_id} in account {aws_account_id}'
                    )

                if grant_permissions or revoke_permissions:
                    self.logger.info(
                        f'Updating permissions for the following dataset {dataset_id} in account {aws_account_id}'
                    )
                    message_body = dict(
                        AwsAccountId=aws_account_id,
                        DataSetId=dataset_id
                    )
                    if grant_permissions:
                        self.logger.info(f'Granting following permissions to dataset {dataset_id}: {grant_permissions}')
                        message_body.update({'GrantPermissions': grant_permissions})
                    if revoke_permissions:
                        self.logger.info(
                            f'Revoking following permissions from dataset {dataset_id}: {revoke_permissions}'
                        )
                        message_body.update({'RevokePermissions': revoke_permissions})
                    response = client.update_data_set_permissions(**message_body)
                    if response['Status'] == http.HTTPStatus.OK:
                        self.logger.info(f'Dataset permissions updated successfully')
                    else:
                        self.logger.warning(
                            f'Got response {response} when updating dataset {dataset_id} permissions '
                            f'in account {aws_account_id}'
                        )

    def _upload_dataset_to_s3(self, file_path: str, project: str) -> None:
        """
        Upload all datasets to s3 bucket
        :param file_path: path of file that needs to upload
        :param project: project name
        """
        s3_client = boto3.client('s3')
        file_name = file_path.split('/')[-1]
        self.logger.info(
            f'uploading file {file_path} to s3 bucket {self.s3_bucket_name} '
            f'in directory {os.path.join(self.s3_quicksight_directory, project, file_name)}'
        )
        s3_client.upload_file(
            file_path,
            self.s3_bucket_name,
            os.path.join(self.s3_quicksight_directory, project, file_name)
        )

    def _deploy_datasets(self) -> None:
        """
        Process all datasets and do required operations on them
        """
        quicksight_projects = os.path.join(self.quicksight_directory, self.projects_directory)
        projects = set(
            next(walk(quicksight_projects))[1]
        ) - constants.IGNORE_FOLDER_SET
        if self.project_name_list:
            projects = projects - set(self.project_name_list) if self.exclude else projects.intersection(
                set(self.project_name_list)
            )
        for project in projects:
            root, _, files = next(walk(os.path.join(quicksight_projects, project)))
            files = {os.path.join(root, file) for file in files if file.endswith(constants.DATASET_JSON_FILE_FORMAT)}
            if self.datasets_list:
                files = files - set(self.datasets_list) if self.exclude else files.intersection(set(self.datasets_list))
            self.logger.info(f'Processing {len(files)} datasets from project: {project}')
            for file in files:
                self._create_update_dataset(file)
                if self.s3_bucket_name:
                    self._upload_dataset_to_s3(file_path=str(file), project=project)

    def run(self) -> None:
        """
        Class main run function
        """
        if self.deploy_datasets:
            self._deploy_datasets()
