import boto3
import filecmp
import logging
import os
import shutil
import zipfile
from os import path
from uuid import uuid4

import constants as airflow_constants

logging.basicConfig(level=logging.INFO)
logging.getLogger('airflow')


def are_dir_trees_identical(dir1: str, dir2: str) -> bool:
    """
    Compare two directories recursively. Files in each directory are assumed to be equal
    if their names and contents are equal.
    :param dir1: First directory path
    :param dir2: Second directory path
    :return: True if the directory trees are the same and there were no errors while
    accessing the directories or files, False otherwise.
    """
    dirs_cmp = filecmp.dircmp(dir1, dir2)
    if len(dirs_cmp.left_only) > 0 or len(dirs_cmp.right_only) > 0 or len(dirs_cmp.funny_files) > 0:
        return False
    (_, mismatch, errors) = filecmp.cmpfiles(dir1, dir2, dirs_cmp.common_files, shallow=False)
    if len(mismatch) > 0 or len(errors) > 0:
        return False
    for common_dir in dirs_cmp.common_dirs:
        new_dir1 = os.path.join(dir1, common_dir)
        new_dir2 = os.path.join(dir2, common_dir)
        if not are_dir_trees_identical(new_dir1, new_dir2):
            return False
    return True

def check_if_identical_txt_files(s3_bucket: boto3.resource, local_path: str, s3_path: str) -> bool:
    """
    Check if the given txt files are identical on s3 and local file system
    :param s3_bucket: S3 Bucket resource
    :param local_path: Path to the file on local
    :param s3_path: Path to the file on s3
    :return: Boolean result indicating if the text files are identical
    """
    try:
        s3_bucket.download_file(s3_path, airflow_constants.DUPLICATE_REQUIREMENT_FILE_PATH) # download file to local
        is_identical = filecmp.cmp(local_path, airflow_constants.DUPLICATE_REQUIREMENT_FILE_PATH, shallow=False) # compare files
        if path.isfile(airflow_constants.DUPLICATE_REQUIREMENT_FILE_PATH):
            os.remove(airflow_constants.DUPLICATE_REQUIREMENT_FILE_PATH)
        return is_identical
    except Exception as e:
        logging.error(f'Exception occured while checking for identical txt files: {e}')
        return False

def check_if_identical_zip_files(s3_bucket: boto3.resource, local_path: str, s3_path: str) -> bool:
    """
    Check if the given txt files are identical on s3 and local file system
    :param s3_bucket: S3 Bucket resource
    :param local_path: Path to the file on local
    :param s3_path: Path to the file on s3
    :return: Boolean result indicating if the zip files are identical
    """
    dir1_path = local_path.split('.')[0]
    dir2_path = airflow_constants.DUPLICATE_PLUGINS_PATH.split('.')[0]

    try:
        s3_bucket.download_file(s3_path, airflow_constants.DUPLICATE_PLUGINS_PATH) # download file to local

        # unzip files
        os.mkdir(dir1_path)
        os.mkdir(dir2_path)
        with zipfile.ZipFile(local_path,"r") as zip_ref:
            zip_ref.extractall(dir1_path)
        with zipfile.ZipFile(airflow_constants.DUPLICATE_PLUGINS_PATH,"r") as zip_ref:
            zip_ref.extractall(dir2_path)

        # check if unzipped dirs are identical
        is_identical = are_dir_trees_identical(dir1=dir1_path, dir2=dir2_path)

        # delete files after checking
        if path.isfile(airflow_constants.DUPLICATE_PLUGINS_PATH):
            os.remove(airflow_constants.DUPLICATE_PLUGINS_PATH)
        if path.isdir(dir1_path):
            shutil.rmtree(dir1_path)
        if path.isdir(dir2_path):
            shutil.rmtree(dir2_path)
        return is_identical
    except Exception as e:
        logging.error(f'Exception occured while checking for identical zip files: {e}')

        # delete files if error occurred
        if path.isfile(airflow_constants.DUPLICATE_PLUGINS_PATH):
            os.remove(airflow_constants.DUPLICATE_PLUGINS_PATH)
        if path.isdir(dir1_path):
            shutil.rmtree(dir1_path)
        if path.isdir(dir2_path):
            shutil.rmtree(dir2_path)
        return False


def generate_hash(n) -> str:
    """
    Generate random uuid n bit hash
    :param n: bit length
    :return: n bit hash
    """
    return str(uuid4().hex[:n])


def get_dir_file_set_from_path(
    dir_path: str, ignore_set: set = None, names_list: list = None, exclude: bool = False, fetch_dir: bool = True
) -> set:
    """
    Fetch list of directory or files from given path
    :param dir_path: directory path in which need to fetch directory or files list
    :param ignore_set: set consisting of items to ignore during fetch
    :param names_list: list of files/directories name to include in set
    :param exclude: excludes the names specified in names_list
    :param fetch_dir: whether to fetch directory or file
    :return: set of directory/file names
    """
    ignore_set = set() if ignore_set is None else ignore_set
    index = 1 if fetch_dir else 2
    dir_file_set = set()
    if os.path.exists(dir_path):
        dir_file_set = set(next(os.walk(dir_path))[index]) - ignore_set
        if names_list:
            dir_file_set = dir_file_set - set(names_list) if exclude else dir_file_set.intersection(set(names_list))
    else:
        logging.warn(f'Directory path: {dir_path} does not exist.')
    return dir_file_set


def upload_file_to_s3(s3_bucket: boto3.resource, local_path: str, s3_path: str) -> bool:
    """
    Upload file to s3 bucket if they are not identical (currently support zip and txt file only)
    :param s3_bucket: S3 Bucket resource
    :param local_path: Path to the file on local
    :param s3_path: Path to the file on s3
    :return if file is identical or not
    """
    is_identical = False
    if local_path.endswith('.zip'):
        is_identical = check_if_identical_zip_files(
            s3_bucket=s3_bucket,
            local_path=local_path,
            s3_path=s3_path
        )
    elif local_path.endswith('.txt'):
        is_identical = check_if_identical_txt_files(
            s3_bucket=s3_bucket,
            local_path=local_path,
            s3_path=s3_path
        )
    if not is_identical:
        logging.info(f'uploading {local_path} to s3 bucket path {s3_path}')
        s3_bucket.upload_file(local_path, s3_path)
    else:
        logging.info(f'Skipped uploading {local_path} to S3 bucket because of no change')
    return is_identical
