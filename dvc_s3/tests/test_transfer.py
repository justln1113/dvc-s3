"""Integration tests for s3transfer-based file transfers.

These tests use moto to verify that the patched _put_file / _get_file
methods (backed by s3transfer) work correctly for uploads and downloads.
"""

import os
import tempfile

import pytest

from dvc_s3 import S3FileSystem


@pytest.fixture()
def fs(s3_config, s3_bucket):
    """Create an S3FileSystem pointing at the moto server."""
    host_port = s3_config["endpoint_url"]
    return S3FileSystem(
        endpointurl=host_port,
        access_key_id=s3_config["aws_access_key_id"],
        secret_access_key=s3_config["aws_secret_access_key"],
        region="us-east-1",
    )


@pytest.fixture()
def bucket(s3_bucket):
    return s3_bucket


class TestTransferPatching:
    def test_put_file_is_patched(self, fs):
        assert "_fast_put_file" in fs.fs._put_file.__qualname__

    def test_get_file_is_patched(self, fs):
        assert "_fast_get_file" in fs.fs._get_file.__qualname__


class TestSmallFileTransfer:
    """Small files (below multipart threshold) — single PUT/GET."""

    def test_upload_download_roundtrip(self, fs, bucket):
        data = b"hello world"
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(data)
            src = f.name
        try:
            remote = f"{bucket}/small.txt"
            fs.put_file(src, remote)
            assert fs.exists(remote)
            assert fs.info(remote)["size"] == len(data)

            dst = src + ".dl"
            fs.get_file(remote, dst)
            with open(dst, "rb") as fh:
                assert fh.read() == data
        finally:
            os.unlink(src)
            if os.path.exists(src + ".dl"):
                os.unlink(src + ".dl")

    def test_empty_file(self, fs, bucket):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            src = f.name
        try:
            remote = f"{bucket}/empty.bin"
            fs.put_file(src, remote)
            assert fs.info(remote)["size"] == 0

            dst = src + ".dl"
            fs.get_file(remote, dst)
            assert os.path.getsize(dst) == 0
        finally:
            os.unlink(src)
            if os.path.exists(src + ".dl"):
                os.unlink(src + ".dl")


class TestLargeFileTransfer:
    """Files above the default 8 MB multipart threshold."""

    def test_multipart_upload_download(self, fs, bucket):
        size = 10 * 1024 * 1024  # 10 MB
        data = os.urandom(size)
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(data)
            src = f.name
        try:
            remote = f"{bucket}/large.bin"
            fs.put_file(src, remote)
            assert fs.info(remote)["size"] == size

            dst = src + ".dl"
            fs.get_file(remote, dst)
            with open(dst, "rb") as fh:
                assert fh.read() == data
        finally:
            os.unlink(src)
            if os.path.exists(src + ".dl"):
                os.unlink(src + ".dl")


class TestCustomTransferConfig:
    """Verify user-supplied transfer tuning params are applied."""

    def test_config_params_reach_transfer(self, s3_config, s3_bucket):
        fs = S3FileSystem(
            endpointurl=s3_config["endpoint_url"],
            access_key_id=s3_config["aws_access_key_id"],
            secret_access_key=s3_config["aws_secret_access_key"],
            region="us-east-1",
            max_concurrent_requests="30",
            multipart_threshold="16MB",
            multipart_chunksize="16MB",
        )
        # Trigger fs_args / _prepare_credentials so _transfer_config is set
        _ = fs.fs

        assert fs._transfer_config["max_concurrency"] == 30
        assert fs._transfer_config["multipart_threshold"] == 16 * 1024 * 1024
        assert fs._transfer_config["multipart_chunksize"] == 16 * 1024 * 1024

        # Should still be able to upload/download
        data = b"config test"
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(data)
            src = f.name
        try:
            remote = f"{s3_bucket}/config-test.txt"
            fs.put_file(src, remote)

            dst = src + ".dl"
            fs.get_file(remote, dst)
            with open(dst, "rb") as fh:
                assert fh.read() == data
        finally:
            os.unlink(src)
            if os.path.exists(src + ".dl"):
                os.unlink(src + ".dl")


class TestMetadataStillUsesS3fs:
    """Metadata operations (ls, info, exists) should still work via s3fs."""

    def test_ls_info_exists(self, fs, bucket):
        data = b"metadata test"
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(data)
            src = f.name
        try:
            fs.put_file(src, f"{bucket}/a.txt")
            fs.put_file(src, f"{bucket}/b.txt")

            assert fs.exists(f"{bucket}/a.txt")
            assert not fs.exists(f"{bucket}/nonexistent.txt")

            info = fs.info(f"{bucket}/a.txt")
            assert info["size"] == len(data)

            listing = fs.ls(f"{bucket}/")
            names = [os.path.basename(p) for p in listing]
            assert "a.txt" in names
            assert "b.txt" in names
        finally:
            os.unlink(src)
