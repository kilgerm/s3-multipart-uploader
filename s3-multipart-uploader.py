#!/usr/bin/env pytyhon3
import hashlib
import logging
import math
import sys
from argparse import ArgumentParser, Namespace
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.create_multipart_upload

BLOCKSIZE_FOR_HASHING = 1024 * 1024

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger("botocore").setLevel(logging.INFO)
logging.getLogger("botocore.utils").setLevel(logging.WARNING)

LOG = logging.getLogger()


def parse_args() -> Namespace:
    parser = ArgumentParser("S3 Multipart Uploader")
    subparsers = parser.add_subparsers(dest="operation")

    start_parser = subparsers.add_parser("start")

    abort_parser = subparsers.add_parser("abort")
    #abort_parser.add_argument("bucket", type=str, help="Destination bucket name")
    #abort_parser.add_argument("key", type=str, help="Destination key in the bucket")
    abort_parser.add_argument("upload_id", metavar="UPLOAD_ID", type=str, help="UploadId from previously started upload")


    return parser.parse_args()


class S3MultipartUploader:
    def __init__(
            self,
            bucket: str,
            filepath_to_upload: Path,
            dest_key: str,
            part_size_in_bytes: int,
            args: Namespace,
    ):
        self.part_size_in_bytes = part_size_in_bytes
        self.dest_key = dest_key
        self.filepath_to_upload = filepath_to_upload
        self.bucket = bucket
        self.args = args
        self.s3 = boto3.client("s3")

    def start(self):
        LOG.info(f"Start multipart upload for {self.filepath_to_upload} to s3://{self.bucket}/{self.dest_key}")

        part_count = math.ceil(self.filepath_to_upload.stat().st_size / self.part_size_in_bytes)
        LOG.info(f"Will use {part_count} parts of max. size {self.part_size_in_bytes}")

        LOG.info("Computing md5...")
        md5 = self.compute_md5(self.filepath_to_upload)
        LOG.info(f"{md5=}")

        filesize = self.filepath_to_upload.stat().st_size
#        with TemporaryDirectory(prefix="s3-multipart-uploader-") as tmpdir:
#            LOG.info(f"tempdir: {tmpdir}")

        create_response = self.s3.create_multipart_upload(
            Bucket=self.bucket,
            Key=self.dest_key,
            Metadata={"md5": md5},
        )
        LOG.debug(create_response)

        upload_id = create_response["UploadId"]
        LOG.info(f"Got {upload_id=}")

        for part_index in range(0, part_count):
            start_offset = min(part_index * self.part_size_in_bytes, filesize)
            end_offset = min((part_index + 1) * self.part_size_in_bytes - 1, filesize)
            LOG.debug(f"Part #{part_index} (byte offset {start_offset}-{end_offset})")

    @staticmethod
    def compute_md5(filepath_to_upload: Path):
        hasher = hashlib.md5()
        with filepath_to_upload.open("rb") as file:
            while r := file.read(BLOCKSIZE_FOR_HASHING):
                hasher.update(r)
        md5 = hasher.hexdigest()
        return md5

    def cont(self):
        pass

    def abort(self):
        print(self.s3.abort_multipart_upload(
            Bucket=self.bucket,
            Key=self.dest_key,
            UploadId=self.args.upload_id,
        ))


def main():
    args = parse_args()
    LOG.info(args)

    # TODO: read from cli
    bucket = "kilgerm1"
    filepath_name_to_upload = "testdata/testfile"
    filepath_to_upload = Path(filepath_name_to_upload)
    dest_key = "testfile"  # from filepath
    part_size = 1024 * 1024

    s3_multipart_uploader = S3MultipartUploader(
        bucket=bucket,
        filepath_to_upload=filepath_to_upload,
        dest_key=dest_key,
        part_size_in_bytes=part_size,
        args=args,
    )


    s3_multipart_uploader.__getattribute__(args.operation)()


if __name__ == "__main__":
    main()
