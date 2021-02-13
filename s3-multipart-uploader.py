#!/usr/bin/env pytyhon3
from __future__ import annotations

import hashlib
import logging
import math
import sys
import time
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

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
    start_parser.add_argument("--upload_id", metavar="UPLOAD_ID", type=str, default="",
                              help="UploadId from previously started upload")

    abort_parser = subparsers.add_parser("abort")
    # abort_parser.add_argument("bucket", type=str, help="Destination bucket name")
    # abort_parser.add_argument("key", type=str, help="Destination key in the bucket")
    abort_parser.add_argument("upload_id", metavar="UPLOAD_ID", type=str,
                              help="UploadId from previously started upload")

    return parser.parse_args()


class ProgressMeter:
    def __init__(self, label: str, limit: int, auto_status_secs: Optional[int] = 1):
        self.label = label
        self.limit = limit
        self.current = 0
        self.auto_status_secs = auto_status_secs
        self.start_time = time.perf_counter()
        self.last_update_time = self.start_time

    def increment(self, by: int = 1):
        self.current = min(self.current + by, self.limit)
        delta = time.perf_counter() - self.last_update_time
        self.last_update_time = time.perf_counter()
        if delta > self.auto_status_secs:
            self.log_status()

    def log_status(self):
        percent = 100 * self.current / self.limit
        time_passed_secs = self.last_update_time - self.start_time
        estimated_remaining_time_secs = (
                                                self.limit - self.current) / self.current * time_passed_secs if self.current > 0 else -1
        LOG.info(f"{self.label} {percent:.4f}% "
                 f"(elapsed: {int(time_passed_secs)}s, ~ETA: {int(estimated_remaining_time_secs)}s)"
                 f"‚ ({self.current}/{self.limit})")

    def __enter__(self) -> ProgressMeter:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.log_status()


@dataclass
class MultipartUploadInProgress:
    started_at: str
    upload_id: str
    initiator: str


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

    def check_existing_uploads(self) -> Optional[MultipartUploadInProgress]:
        response = self.s3.list_multipart_uploads(
            Bucket=self.bucket
        )
        LOG.debug(response)
        matching = [MultipartUploadInProgress(
            upload_id=upload["UploadId"],
            started_at=upload["Initiated"],
            initiator=upload["Initiator"]["ID"])
            for upload in response.get("Uploads", []) if upload["Key"] == self.dest_key]
        uploads_for_key = len(matching)

        upload_id = self.args.upload_id
        if upload_id:
            matching = [mpupload for mpupload in matching if mpupload.upload_id == upload_id]

        num_matches = len(matching)
        if num_matches == 0:
            if upload_id:
                LOG.error(
                    f"No upload found for upload_id {upload_id} (found {uploads_for_key} total uploads for this bucket and key)")
                exit(1)
            else:
                LOG.info(f"No uploads found for this bucket and key")
                return None
        if num_matches > 1:
            if upload_id:
                LOG.error(f"Found {num_matches} uploads for this upload_id - this is probably a bug")
            else:
                LOG.error(f"Found {num_matches} uploads in progress, please specify an upload_id")
            exit(1)
        match = matching[0]
        LOG.info(f"Found existing upload started at {match.started_at} by {match.initiator}, upload_id: \"{match.upload_id}\"")
        return match

    def start(self):
        upload = self.check_existing_uploads()
        if upload:
            self.continue_upload(upload)
            return

        LOG.info(f"Start multipart upload for {self.filepath_to_upload} to s3://{self.bucket}/{self.dest_key}")

        part_count = math.ceil(self.filepath_to_upload.stat().st_size / self.part_size_in_bytes)
        LOG.info(f"Will use {part_count} parts of max. size {self.part_size_in_bytes}")

        LOG.info("Computing md5...")
        md5 = self.compute_md5(self.filepath_to_upload)
        LOG.info(f"{md5=}")
        filesize = self.filepath_to_upload.stat().st_size
        #        with TemporaryDirectory(prefix="s3-multipart-uploader-") as tmpdir:
        #            LOG.info(f"tempdir: {tmpdir}")
        exit(1)

        create_response = self.s3.create_multipart_upload(
            Bucket=self.bucket,
            Key=self.dest_key,
            Metadata={"md5": md5,
                      "original_filename": self.filepath_to_upload.name
                      },
        )
        LOG.debug(create_response)

        upload_id = create_response["UploadId"]
        LOG.info(f"Got {upload_id=}")
        exit(1)

        for part_index in range(0, part_count):
            start_offset = min(part_index * self.part_size_in_bytes, filesize)
            end_offset = min((part_index + 1) * self.part_size_in_bytes - 1, filesize)
            LOG.debug(f"Part #{part_index} (byte offset {start_offset}-{end_offset})")

    @staticmethod
    def compute_md5(filepath_to_upload: Path):
        hasher = hashlib.md5()
        file_size = filepath_to_upload.stat().st_size
        with filepath_to_upload.open("rb") as file:
            with ProgressMeter("Computing file hash", limit=file_size) as progress:
                while r := file.read(BLOCKSIZE_FOR_HASHING):
                    hasher.update(r)
                    progress.increment(BLOCKSIZE_FOR_HASHING)
        md5 = hasher.hexdigest()
        return md5

    def continue_upload(self, upload: MultipartUploadInProgress):
        raise Exception("Not yet implemented")

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
