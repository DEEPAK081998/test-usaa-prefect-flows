"""
Script to build and push docker image to aws ecr
Usage:
docker_deploy.py [-h] --dockerfile-dir DOCKERFILE_DIR --docker-name DOCKER_NAME [--docker-tag DOCKER_TAG]
                 [--registry REGISTRY] [--username USERNAME] [--password PASSWORD] [--aws-profile AWS_PROFILE]
                 [--aws-region AWS_REGION] [--debug]

Script to build and push docker image to aws ecr

optional arguments:
  -h, --help            show this help message and exit
  --dockerfile-dir DOCKERFILE_DIR
                        path to dockerfile
  --docker-name DOCKER_NAME
                        Name of docker image
  --docker-tag DOCKER_TAG
                        docker image tag
  --registry REGISTRY   URL to the registry
  --username USERNAME   registry username
  --password PASSWORD   registry password
  --aws-profile AWS_PROFILE
                        AWS Named profile
  --aws-region AWS_REGION
                        AWS Region
  --debug               Include debug logs
"""
import argparse
import base64
import logging
import os

import boto3
import docker


def _define_arguments() -> argparse.Namespace:
    """
    Define all the named arguments which are required for script to run
    :return: (object) arguments object
    """
    parser = argparse.ArgumentParser(description='Script to build and push docker image to aws ecr')
    parser.add_argument('--dockerfile-dir', type=str, help='path to dockerfile', required=True)
    parser.add_argument('--docker-name', type=str, help='Name of docker image', required=True)
    parser.add_argument('--docker-tag', type=str, help='docker image tag', default='latest')
    parser.add_argument('--registry', type=str, help='URL to the registry')
    parser.add_argument('--username', type=str, help='registry username')
    parser.add_argument('--password', type=str, help='registry password')
    parser.add_argument('--aws-profile', type=str, help='AWS Named profile')
    parser.add_argument('--aws-region', type=str, help='AWS Region', default='us-east-1')
    parser.add_argument('--debug', help='Include debug logs', action='store_true')
    return parser.parse_args()


def _set_default_env(envs: dict, unset: bool = False) -> None:
    """
    Set/Unset the defined environments
    :param envs: dict containing the environment variable as key and its value as value
    :param unset: unset the set environments
    """
    if unset:
        for key in envs.keys():
            del os.environ[key]
    else:
        for key, value in envs.items():
            if value is not None:
                os.environ[key] = value


def _create_and_auth_docker_client(
    ecr_client: boto3.client, registry: str = None, username: str = None, password: str = None
) -> (docker.DockerClient, str):
    """
    Create a docker client and authenticate it with ecr
    :param ecr_client: boto3 ecr_client client
    :param registry: docker registry url
    :param username: account username
    :param password: account password
    :return: configured docker client
    """
    docker_client = docker.from_env()
    if registry is None or username is None or password is None:
        try:
            token = ecr_client.get_authorization_token()
            username, password = base64.b64decode(
                token['authorizationData'][0]['authorizationToken']
            ).decode().split(':')
            registry = token['authorizationData'][0]['proxyEndpoint'].split('//')[1]
        except Exception as e:
            logging.error(f'[ Cannot fetch password error:- {e} ]')
    logging.debug(f'[ Username fetched:- {username} ]')
    logging.debug(f'[ Password fetched:- {password} ]')
    logging.debug(f'[ Registry fetched:- {registry} ]')
    logging.info(
        f'[ Attempting Docker Login:- '
        f'{docker_client.login(registry=registry, username=username, password=password)} ]'
    )
    return docker_client, registry


def _get_account_id() -> str:
    """
    Fetch and return the AWS account id
    :return: account id
    """
    sts = boto3.client("sts")
    return sts.get_caller_identity()["Account"]


def main():
    args = _define_arguments()
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level)
    default_envs = {'AWS_PROFILE': args.aws_profile, 'AWS_DEFAULT_REGION': args.aws_region}
    _set_default_env(default_envs)
    docker_client = None
    client = boto3.client('ecr')
    if args.registry is None or args.username is None or args.password is None:
        docker_client, args.registry = _create_and_auth_docker_client(ecr_client=client)
    else:
        docker_client, _ = _create_and_auth_docker_client(
            ecr_client=client,
            registry=args.registry,
            username=args.username,
            password=args.password
        )
    image = docker_client.images.build(
        path=args.dockerfile_dir,
        tag=f'{args.registry}/{args.docker_name}:{args.docker_tag}'
    )[0]
    logging.debug(f'[ Image created:- {image} ]')
    push_response = docker_client.images.push(repository=f'{args.registry}/{args.docker_name}:{args.docker_tag}')
    logging.info(
        f"[ Pushing Docker image:- "
        f"{push_response} ]"
    )
    logging.info(f'[ Removing docker image ]')
    docker_client.images.remove(image=image.id, force=True)
    logging.info(f'[ Docker Image removed ]')
    _set_default_env(default_envs, unset=True)


if __name__ == '__main__':
    main()
