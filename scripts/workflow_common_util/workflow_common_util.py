import os
import boto3
import zipfile
import logging
import constants as workflow_common_util_constants

logging.basicConfig(level=logging.INFO)
logging.getLogger('workflow_common_util').setLevel(level=logging.INFO)

class WorkflowCommonUtil:
    def __init__(
        self, s3_bucket_name: str = None, projects_folder_path: str = None, project_names: str = None,
        test: bool = False, s3_path: str = None, exclude: bool = False,
    ) -> None:
        """
        Initialize the WorkflowCommonUtil class
        :param s3_bucket_name: S3 Bucket name where to store all project zip files
        :param s3_path: Path of the directory where to store workflow projects on s3
        :param project_names: List of all workflow project names needed to be zipped and uploaded
        :param exclude: Boolean to exclude the provided workflow projects
        :param projects_folder_path: Path of the folder where workflow projects are stored
        :param test: Runs test only skips project upload
        """
        self.test = test
        self.project_name_list = project_names
        self.projects_folder_path = projects_folder_path
        self.exclude = exclude
        self.s3_path = s3_path
        self.s3_bucket = self._initialize_s3_bucket(s3_bucket_name=s3_bucket_name)

    def _initialize_s3_bucket(self, s3_bucket_name) -> boto3.resource:
        """
        Initialize the S3 bucket object
        :param s3_bucket_name: S3 Bucket name
        :return: boto client resource
        """
        if self.test:
            return None
        s3 = boto3.resource('s3')
        return s3.Bucket(s3_bucket_name)

    def _upload_file_to_s3(self, file_path: dict, file_name: str) -> None:
        """
        Upload all the workflow project zip files to S3
        :param file_path: path of the zip file
        :param file_name: name of the zip file
        """
        logging.info(f'Uploading {file_path} to s3 bucket')
        try:
            self.s3_bucket.upload_file(file_path, file_name)
            logging.info(f'Successfully uploaded {file_path} to s3')
        except Exception as e:
            logging.error(f'Error while uploading file {file_path} to s3 {e}')
            raise e

    def _create_s3_dest(self, s3_path: str, file_name: str) -> str:
        """
        Creates s3 destination path
        :param s3_path: path to the directory where to store workflow project on S3
        :param file_name: Name of the workflow project zip
        :return: (string) S3 destination string
        """
        if s3_path[-1] == '/':
            return f'{s3_path}{file_name}'
        return f'{s3_path}/{file_name}'

    def _zip_and_upload_workflow_project(self, folder_path: str) -> None:
        """
        Zip and upload workflow project to S3
        :param folder_path: path of the workflow project
        """
        if folder_path is None:
            return

        zip_name = f"{folder_path.split('/')[-1]}.zip"
        zip_file_path = f"{workflow_common_util_constants.ZIP_FILES_DIRECTORY_PATH}{zip_name}"
        with zipfile.ZipFile(zip_file_path, 'w') as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), folder_path))

        if not self.test:
            s3_dest = self._create_s3_dest(s3_path=self.s3_path, file_name=zip_name)
            self._upload_file_to_s3(zip_file_path, s3_dest)

    def process_data(self):
        """
        Process the projects directory to upload the workflow projects to s3
        """
        # read directories inside the provided projects directory
        (root, projects, files) = next(os.walk(os.path.join(self.projects_folder_path)))

        # warning for test mode
        if self.test:
            logging.info(f'Test mode is active')

        # zip and upload them to s3
        for project in projects:
            if (
                self.project_name_list is None
                or (self.project_name_list and (project in self.project_name_list and not self.exclude))
                or (self.project_name_list and project not in self.project_name_list and self.exclude)
            ):
                dir_path = os.path.join(root, project)
                self._zip_and_upload_workflow_project(folder_path=dir_path) # zip the directory
