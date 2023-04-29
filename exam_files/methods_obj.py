
import argparse
from hashlib import md5
from time import localtime

import boto3
from os import getenv
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError
from urllib.request import urlopen
import io

from magic import magic

load_dotenv()



########################## delete object ##########################
def delete_object(aws_s3_client, bucket_name, filename):

    response = aws_s3_client.list_objects_v2(Bucket=bucket_name, Prefix=filename)

    if 'Contents' in response:

        aws_s3_client.delete_object(Bucket=bucket_name, Key=filename)
        print( f" object'{filename}' წაიშალა ბაკეტიდან '{bucket_name}'.")
    else:
        print(f"  object '{filename}' არ არსებობს '{bucket_name}'.")

##############################################################################


########################## change file version  ##########################
def rollback_to_version(aws_s3_client, bucket_name, file_name, version):
    aws_s3_client.copy_object(
        Bucket=bucket_name,
        Key=file_name,
        CopySource={'Bucket': bucket_name, 'Key': file_name, 'VersionId': version}
    )

##############################################################################


########################## set object policy  ##########################
def set_object_access_policy(aws_s3_client, bucket_name, file_name):
    response = aws_s3_client.put_object_acl(
        ACL="public-read",
        Bucket=bucket_name,
        Key=file_name
    )
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False
##############################################################################


########################## list object versions ##########################
def list_object_versions(aws_s3_client, bucket_name, file_name):
    versions = aws_s3_client.list_object_versions(
        Bucket=bucket_name,
        Prefix=file_name
    )
    for version in versions['Versions']:
        version_id = version['VersionId']
        file_key = version['Key'],
        is_latest = version['IsLatest']
        modified_at = version['LastModified']

        print(version_id, file_key, is_latest, modified_at)

##############################################################################

########################## download file and upload on s3 ##########################
def download_file_and_upload_to_s3(aws_s3_client,
                                   bucket_name,
                                   url,
                                   s3_region=getenv("aws_s3_region_name", "us-west-2"),
                                   keep_local=True) -> str:
    m = {
        "jpeg": "image/jpeg",
        "png": "image/png",
        "mp4": "video/mp4"
    }

    with urlopen(url) as response:
        content = response.read()
        mime_type = magic.from_buffer(content, mime=True)
        content_type = None
        file_name = None

        for type, ctype in m.items():
            if mime_type == ctype:
                content_type = ctype
                file_name = generate_file_name(type)

        if not content_type:
            raise ValueError("Invalid type")

        aws_s3_client.upload_fileobj(Fileobj=io.BytesIO(content),
                                     Bucket=bucket_name,
                                     ExtraArgs={'ContentType': content_type},
                                     Key=file_name)

    if keep_local:
        with open(f"static/{file_name}", mode="wb") as file:
            file.write(content)

    # Public URL
    return "https://s3-{0}.amazonaws.com/{1}/{2}".format(s3_region, bucket_name,
                                                         file_name)
##############################################yvela gafartoebistvis ############################################

def download_file_and_upload_to_s3(aws_s3_client,
                                   bucket_name,
                                   url,
                                   keep_local=False) -> str:
    m = str(url).split(".")
    file_name = f'image_file_{md5(str(localtime()).encode("utf-8")).hexdigest()}.{m[-1]}'
    with urlopen(url) as response:
        content = response.read()
        aws_s3_client.upload_fileobj(Fileobj=io.BytesIO(content),
                                     Bucket=bucket_name,
                                     ExtraArgs={'ContentType': 'image/jpg'},
                                     Key=file_name)
    if keep_local:
        with open(file_name, mode='wb') as jpg_file:
            jpg_file.write(content)


    return "https://s3-{0}.amazonaws.com/{1}/{2}".format('us-west-2',
                                                         bucket_name, file_name)

#############################################################################################################
def generate_file_name(file_extension) -> str:
  return f'up_{md5(str(localtime()).encode("utf-8")).hexdigest()}.{file_extension}'