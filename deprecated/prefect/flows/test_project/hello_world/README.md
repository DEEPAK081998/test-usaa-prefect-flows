# README #

A sample hello-world flow

## Usage:

1. register flow with command
   ```bash
   python scripts/prefect_build/build.py [--aws-profile <profile>] --labels <Labels> --s3-bucket <S3 Bucket Name> --flows flows/test_project/hello_world/flow.py
   ```