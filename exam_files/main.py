from methods_back import *
from methods_obj import *
import argparse

# parser = argparse.ArgumentParser(
#     description="CLI program that helps with uploading files on S3 buckets.",
#     usage='''
#     How to upload file:
#     short:
#         python week4_hw1.py -up -bn <bucket_name> -fn <file_path>
#     long:
#        python week4_hw1.py --upload_file --bucket_name <bucket_name> --file_name <file_path>
#     How to list buckets:
#     short:
#         python week4_hw1.py -lb
#     long:
#         python week4_hw1.py --list_buckets
#
#     How to set versioning to a bucket:
#     short:
#          python week4_hw1.py -bn <bucket_name> -vers <Bool>
#     long:
#         python week4_hw1.py --bucket_name <bucket_name> --versioning <Bool>
#
#     How to check objects version:
#     short:
#         python week4_hw1.py -bucket_name <bucket_name> -l_v -fn <file_path>
#     long:
#         python week4_hw1.py -bucket_name <bucket_name> -list_versions --file_name <file_path>
#
#     How to delete old version of object:
#     short:
#         python week4_hw1.py -bn <bucket_name> -d_o_o_v
#     long:
#         python week4_hw1.py --bucket_name <bucket_name> --delete_old_objects_version
#     ''',
#     prog='main.py',
#     epilog='midterm-exam')
parser = argparse.ArgumentParser(
    description="CLI program that helps with S3 buckets.",
    prog='main.py',
    epilog='DEMO APP - 2 FOR BTU_AWS'
)

parser.add_argument("-bn",
                    "--bucket_name",
                    type=str,
                    help="Pass bucket name.",
                    default=None)


parser.add_argument("-be",
                    "--bucket_exists",
                    help="bucket exists",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-fn",
                    "--file_name",
                    type=str,
                    help="Pass file name.",
                    default=None)

parser.add_argument("-k_f_n",
                    "--keep_file_name",
                    help="file name",
                    action='store_false')

parser.add_argument("-lb",
                    "--list_buckets",
                    help=" list bucket",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-uf",
                    "--upload_file",
                    help="upload file",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-ver",
                    "--versioning",
                    type=str,
                    help="list bucket object",
                    nargs="?",
                    default=None)

parser.add_argument("-l_o_v",
                    "--list_object_versions",
                    help="list versions",
                    action='store_true')

parser.add_argument("-db",
                    "--delete_bucket",
                    help="flag to delete bucket",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-arp",
                    "--assign_read_policy",
                    help="flag to assign read bucket policy.",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-amp",
                    "--assign_missing_policy",
                    help="flag to assign read bucket policy.",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-sobap",
                    "--set_object_access_policy",
                    help="set object access policy",
                    action='store_true')

parser.add_argument("-sbt",
                    "--show_bucket_tree",
                    help="file name",
                    action='store_true')

parser.add_argument("-ol",
                    "--object_link",
                    type=str,
                    help="link to download and upload to bucket",
                    default=None)

parser.add_argument("-du",
                    "--download_upload",
                    choices=["False", "True"],
                    help="download and upload to bucket",
                    type=str,
                    nargs="?",
                    const="True",
                    default="False")

parser.add_argument("-region",
                    "--region",
                    type=str,
                    help="Region variable.",
                    default=None)

parser.add_argument("-bc",
                    "--bucket_check",
                    help="Check if bucket already exists.",
                    choices=["False", "True"],
                    type=str,
                    nargs="?",
                    const="True",
                    default="True")

s3_client = init_client()
args = parser.parse_args()
if s3_client:
    if args.bucket_name:
        if args.create_bucket == "True":
            if not args.region:
                parser.error("Please provide region for bucket --region REGION_NAME")
            if (args.bucket_check == "True") and bucket_exists(
                    s3_client, args.bucket_name):
                parser.error("Bucket already exists")
            if create_bucket(s3_client, args.bucket_name, args.region):
                print("Bucket successfully created")

        if args.bucket_exists == "True":
            print(f"Bucket არსებობს: {bucket_exists(s3_client, args.bucket_name)}")

        if (args.delete_bucket == "True") and delete_bucket(s3_client, args.bucket_name):
            print("ბაკეტი წაიშალა")

        if args.versioning == "True":
            versioning(s3_client, args.bucket_name, True)
            print("Enabled versioning on bucket %s." % args.bucket_name)

        # if args.versioning == "True":
        #     versioning(s3_client, args.bucket_name, True)

        if args.versioning == "False":
            versioning(s3_client, args.bucket_name, False)
            print("Disabled versioning on bucket %s." % args.bucket_name)

        # if args.roll_back_to and args.file_name:
        #     rollback_to_version(s3_client, args.bucket_name, args.file_name, args.roll_back_to)

        if args.file_name and args.upload_file == "True":
            upload_local_file(s3_client, args.bucket_name, args.file_name, args.keep_file_name)

        if args.list_object_versions and args.file_name:
            list_object_versions(s3_client, args.bucket_name, args.file_name)

        if args.assign_read_policy == "True":
            assign_policy(s3_client, "public_read_policy", args.bucket_name)

        if args.assign_missing_policy == "True":
            assign_policy(s3_client, "multiple_policy", args.bucket_name)

        if args.set_object_access_policy and args.file_name:
            set_object_access_policy(s3_client, args.bucket_name, args.file_name)

        if args.file_name and args.keep_file_name and args.upload_type:
            print(upload_local_file(s3_client, args.bucket_name, args.file_name, args.keep_file_name,
                                    args.upload_type))

        if args.show_bucket_tree:
            show_bucket_tree(s3_client, args.bucket_name, '', True)

        if args.object_link and args.download_upload == "True" and args.keep_file_name:
            print(download_file_and_upload_to_s3(s3_client, args.bucket_name, args.object_link, args.keep_file_name))

    if args.list_buckets == "True":
        list_buckets(s3_client)