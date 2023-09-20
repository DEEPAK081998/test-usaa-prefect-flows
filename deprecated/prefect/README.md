## Prefect Flows

### Directory structure:

* All flows are stored inside the flows directory and the directory structure it follows is

```text
.
├── readme.md
├── ...
├── ...
├── flows
│   ├── project_1
│   │    ├──flow1
│   │    │     ├── flow.py
│   │    │     ├── tasks.py
│   │    │     ├── utils.py
│   │    │     ├── tests.py  
│   │    │     └── ...
│   │    ├──flow2  
│   │    │     └── ...  
│   │    └── ...    
│   ├── project_2  
│   │    └── ...
│   └── ... 
├── ...
└── ... 
```

### Creating new flows:

* New flow can be created under [flows](flows) directory in respective sub folder denoting the project name
* The flow folder can be of any name but must contain the `flow.py` file having the actual flow
* Add run config and/or storage if required otherwise they will use the default values
* If required add a supporting README.md to denote what that flow do and its configuration/usage parameters

### Registering Flows

* To register all flows use the below command. This will register all flows with default *run config* and default
  *storage* but this can be changed by providing required *run config* and *storage* in flows itself
  ```shell
  python scripts/prefect_build/build.py [--aws-profile <aws profile>] --labels <ecs labels> --s3-bucket <S3 bucket name> 
  ```
* To register a set of flows use the below command
  ```shell
  python scripts/prefect_build/build.py [--aws-profile <aws profile>] --labels <ecs labels> --s3-bucket <S3 bucket name> --flows [<flow_path1> <flow_path_2> ...] 
  ```
* To register a set of projects use the below command
  ```shell
  python scripts/prefect_build/build.py [--aws-profile <aws profile>] --labels <ecs labels> --s3-bucket <S3 bucket name> --projects [<project_1> <project_2> ...] 
  ```
* For more help on how to use the script run the following command
  ```shell
  python scripts/prefect_build/build.py --help
  ```

### Deploying Docker Image:

* **Option 1: Using docker deploy script**
    1. Create aws ecr registry if already present using the given command
       ```shell
       aws ecr create-repository --region us-east-1 --repository-name custom-prefect-0.15.13-python3.7
       ```
    2. Run the below command to build and deploy(use `python scripts/docker_deploy.py --help` for help)
       ```shell
       python scripts/docker_deploy.py --dockerfile-dir ./ --docker-name custom-prefect-0.15.13-python3.7
       ```

* **Option 1: Without using docker deploy script**
    1. Retrieve an authentication token and authenticate your Docker client to your registry
       ```shell
       aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com
       ``` 
    2. Build your Docker image using the following command. You can skip this step if your image is already built:
       ```shell
       docker build -t custom-prefect-0.15.13-python3.7 ./
       ```
    3. After the build completes, tag your image, so you can push the image to this repository:
       ```shell
       docker tag custom-prefect-0.15.13-python3.7:latest <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/custom-prefect-0.15.13-python3.7:latest
       ```
    4. Run the following command to push this image to your newly created AWS repository:
       ```shell
       docker push <aws_account_id>.dkr.ecr.us-east-1.amazonaws.com/custom-prefect-0.15.13-python3.7:latest
       ```
