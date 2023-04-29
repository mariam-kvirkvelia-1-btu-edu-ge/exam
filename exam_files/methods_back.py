import magic
from hashlib import md5
from time import localtime
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import logging
import datetime
from os import getenv
from dotenv import load_dotenv
import json
load_dotenv()


def init_client():
    try:
        client = boto3.client("s3",
                              aws_access_key_id = getenv("aws_access_key_id"),
                              aws_secret_access_key = getenv("aws_secret_access_key"),
                              aws_session_token = getenv("aws_session_token"),
                              region_name = getenv("aws_region_name"))
        client.list_buckets()
        return client
    except ClientError as e:
        logging.error(e)
    except:
        logging.error(("Unexpected error"))


def create_bucket(aws_s3_client, bucket_name, region) -> bool:
    location = {'LocationConstraint': region}
    response = aws_s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration=location
    )
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False

########################## check if the file exists ##########################
def bucket_exists(aws_s3_client, bucket_name) -> bool:
    try:
        response = aws_s3_client.head_bucket(Bucket=bucket_name)
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 200:
            return True
    except ClientError:

        return False
##############################################################################


########################## list all buckets ##########################
def list_buckets(aws_s3_client):
    try:
        buckets = aws_s3_client.list_buckets()
        if buckets:
            for bucket in buckets['Buckets']:
                print(f"    {bucket['Name']}")
    except ClientError as e:
        logging.error(e)

##############################################################################


########################## delete bucket ##########################
def delete_bucket(aws_s3_client, bucket_name) -> bool:
    response = aws_s3_client.delete_bucket(Bucket=bucket_name)
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 204:
        return True
    return False
##############################################################################


########################## turn on and off versioning ##########################
def versioning(aws_s3_client, bucket_name, status: bool):
    versioning_status = "Enabled" if status else "Suspended"
    aws_s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={
            "Status": versioning_status
        }
    )
##############################################################################


def delete_old_version_or_object(aws_s3_client, bucket_name):
    current_date = datetime.datetime.now(datetime.timezone.utc)

    versions = aws_s3_client.list_object_versions(Bucket=bucket_name)['Versions']

    for version in versions:
        creation_date = version['LastModified']
        age = current_date - creation_date

        if age > datetime.timedelta(days=180):
            aws_s3_client.delete_object(Bucket=bucket_name, Key=version['Key'], VersionId=version['VersionId'])


# def delete_version(aws_s3_client, bucket_name):
#     vers = aws_s3_client.list_object_versions(Bucket=bucket_name)['Versions']
#
#     for i in vers:
#         now = datetime.datetime.now()
#         modified_at = i['LastModified']
#         is_latest = i['IsLatest']
#         m = now - modified_at
#
#         if m > datetime.timedelta(days=180):
#             aws_s3_client.delete_version(Bucket=bucket_name, Key=i['Key'], VersionId=i['VersionId'])
##############################################################################


########################## set bucket policy ##########################
def public_read_policy(bucket_name):
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/*",
            }
        ],
    }

    return json.dumps(policy)


def multiple_policy(bucket_name):
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "s3:ListBucketVersions",
                    "s3:PutObjectAcl",
                    "s3:GetObject",
                    "s3:GetObjectAcl",
                    "s3:DeleteObject"
                ],
                "Resource": "*",
                "Effect": "Allow",
                "Principal": "*"
            }
        ]
    }

    return json.dumps(policy)


def assign_policy(aws_s3_client, policy_function, bucket_name):
    policy = None
    response = None
    if policy_function == "public_read_policy":
        policy = public_read_policy(bucket_name)
        response = "public read policy assigned!"
    elif policy_function == "multiple_policy":
        policy = multiple_policy(bucket_name)
        response = "multiple policy assigned!"

    if (not policy):
        print('please provide policy')
        return

    aws_s3_client.put_bucket_policy(
        Bucket=bucket_name, Policy=policy
    )

    print(response)


def read_bucket_policy(aws_s3_client, bucket_name):
    policy = aws_s3_client.get_bucket_policy(Bucket=bucket_name)

    status_code = policy["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return policy["Policy"]
    return False



##############################################################################


########################## upload local files with upload_fileobj and put_object ##########################
def upload_local_file(aws_s3_client,
                      bucket_name,
                      filename,
                      keep_file_name,
                      upload_type="upload_fileobj"):

    allowed = {
        "jpeg": "image/jpeg",
        "png": "image/png",
        "mp4": "video/mp4",
        "txt": "text/plain"
    }

    file_path = f"static/{filename}"
    mime_type = magic.from_file(file_path, mime=True)
    content_type = None
    file_name = None

    for type, ctype in allowed.items():
        if mime_type == ctype:
            content_type = ctype
            file_name = filename if keep_file_name else generate_file_name(type)

    if not content_type:
        raise ValueError("Invalid type")

    # if upload_type == "upload_file":
    #   aws_s3_client.upload_file(file_path,
    #                             bucket_name,
    #                             file_name,
    #                             ExtraArgs={'ContentType': content_type})

    if upload_type == "upload_fileobj":
        with open(file_path, "rb") as file:
            aws_s3_client.upload_fileobj(file,
                                         bucket_name,
                                         file_name,
                                         ExtraArgs={'ContentType': content_type})
    elif upload_type == "put_object":
        with open(file_path, "rb") as file:
            aws_s3_client.put_object(Body=file.read(),
                                     Bucket=bucket_name,
                                     Key=file_name,
                                     ExtraArgs={'ContentType': content_type})

    # public URL
    return "https://s3-{0}.amazonaws.com/{1}/{2}".format(
        getenv("aws_s3_region_name", "us-west-2"), bucket_name, file_name)



# def upload_local_file(aws_s3_client, bucket_name, filename, keep_file_name=False):
#     mime_type = magic.Magic(mime=True)
#     content_type = mime_type.from_file(filename)
#     print(content_type)
#     file_name = filename.split('/')[-1] \
#         if keep_file_name \
#         else generate_file_name(filename.split('/')[-1])
#     print(file_name)


def generate_file_name(file_extension) -> str:
  return f'up_{md5(str(localtime()).encode("utf-8")).hexdigest()}.{file_extension}'
##############################################################################


########################## show all objects in bucket ##########################
def show_bucket_tree(aws_s3_client, bucket_name, prefix, is_last):
    if prefix:
        print(prefix + ('└── ' if is_last else '├── ') + prefix.split('/')[-2])
    else:
        print(prefix)

    prefix += '' if is_last else '│  '

    response = aws_s3_client.list_objects_v2(Bucket=bucket_name,
                                             Delimiter='/',
                                             Prefix=prefix)

    # If there are no subdirectories or files, return
    if 'CommonPrefixes' not in response and 'Contents' not in response:
        return

    # If there are subdirectories, recursively call show_bucket_tree() for each subdirectory
    if 'CommonPrefixes' in response:
        num_prefixes = len(response['CommonPrefixes'])
        for i, sub_prefix in enumerate(response['CommonPrefixes']):
            is_last_subdir = i == num_prefixes - 1
            show_bucket_tree(aws_s3_client, bucket_name, sub_prefix['Prefix'],
                             is_last_subdir)

    # If there are files, print each file's name with the appropriate formatting
    if 'Contents' in response:
        num_files = len(response['Contents'])
        for i, file in enumerate(response['Contents']):
            is_last_file = i == num_files - 1
            print(prefix + ('└── ' if is_last_file else '├── ') +
                  file['Key'].split('/')[-1])

##############################################################################