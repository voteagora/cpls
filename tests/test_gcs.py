import pytest
import json
import gzip
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime


class TestGCSClientUploadDict:

    @pytest.fixture
    def gcs(self):
        with patch("cpls.gcs.storage"), \
             patch("cpls.gcs.Path"), \
             patch("cpls.gcs.WRITE_TO_DISK", False):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()
            client.write_local_copies = False
            client.local_copy_dir = "/tmp/test"
            return client

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_upload_compressed_json(self, gcs):
        mock_blob = MagicMock()
        gcs.bucket.blob.return_value = mock_blob

        result = await gcs.upload_dict(
            {"key": "value"},
            "data/test.json.gz",
            cache_control="public, max-age=300",
            metadata={"m1": "v1"},
        )
        assert result is True
        # Should upload compressed version
        mock_blob.upload_from_string.assert_called()
        assert mock_blob.cache_control == "public, max-age=300"
        assert mock_blob.metadata == {"m1": "v1"}

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_upload_uncompressed_json(self, gcs):
        mock_blob = MagicMock()
        gcs.bucket.blob.return_value = mock_blob

        result = await gcs.upload_dict(
            {"key": "value"},
            "data/test.json",
        )
        assert result is True
        call_args = mock_blob.upload_from_string.call_args
        assert call_args[1]["content_type"] == "application/json"

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "dev")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_dev_uploads_both_compressed_and_uncompressed(self, gcs):
        mock_blob = MagicMock()
        gcs.bucket.blob.return_value = mock_blob

        await gcs.upload_dict({"k": "v"}, "data/test.json.gz")
        # In dev, both compressed and uncompressed should be uploaded
        assert mock_blob.upload_from_string.call_count == 2

    @pytest.mark.asyncio
    async def test_returns_false_when_no_client(self, gcs):
        gcs.client = None
        result = await gcs.upload_dict({"k": "v"}, "test.json.gz")
        assert result is False

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_returns_false_on_exception(self, gcs):
        gcs.bucket.blob.side_effect = Exception("GCS error")
        result = await gcs.upload_dict({"k": "v"}, "test.json.gz")
        assert result is False


class TestGCSClientReadDict:

    @pytest.fixture
    def gcs(self):
        with patch("cpls.gcs.storage"), \
             patch("cpls.gcs.Path"), \
             patch("cpls.gcs.WRITE_TO_DISK", False):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()
            client.write_local_copies = False
            return client

    @pytest.mark.asyncio
    async def test_read_compressed_json(self, gcs):
        data = {"hello": "world"}
        compressed = gzip.compress(json.dumps(data).encode())

        mock_blob = MagicMock()
        mock_blob.generation = 1
        mock_blob.download_as_bytes.return_value = compressed
        gcs.bucket.blob.return_value = mock_blob

        result = await gcs.read_dict("data/test.json.gz")
        assert result == data

    @pytest.mark.asyncio
    async def test_read_uncompressed_json(self, gcs):
        data = {"hello": "world"}
        mock_blob = MagicMock()
        mock_blob.generation = 1
        mock_blob.download_as_string.return_value = json.dumps(data)
        gcs.bucket.blob.return_value = mock_blob

        result = await gcs.read_dict("data/test.json")
        assert result == data

    @pytest.mark.asyncio
    async def test_raises_on_non_json(self, gcs):
        with pytest.raises(Exception):
            await gcs.read_dict("data/test.txt")

    @pytest.mark.asyncio
    async def test_returns_none_when_no_client(self, gcs):
        gcs.client = None
        result = await gcs.read_dict("test.json")
        assert result is None


class TestGCSClientUploadNdjson:

    @pytest.fixture
    def gcs(self):
        with patch("cpls.gcs.storage"), \
             patch("cpls.gcs.Path"), \
             patch("cpls.gcs.WRITE_TO_DISK", False):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()
            client.write_local_copies = False
            return client

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_upload_compressed_ndjson(self, gcs):
        mock_blob = MagicMock()
        gcs.bucket.blob.return_value = mock_blob

        data = [{"a": 1}, {"b": 2}]
        result = await gcs.upload_ndjson(data, "data/test.ndjson.gz")
        assert result is True

        call_args = mock_blob.upload_from_string.call_args
        uploaded_bytes = call_args[0][0]
        decompressed = gzip.decompress(uploaded_bytes).decode()
        lines = decompressed.strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"a": 1}
        assert json.loads(lines[1]) == {"b": 2}

    @pytest.mark.asyncio
    async def test_returns_false_when_no_client(self, gcs):
        gcs.client = None
        result = await gcs.upload_ndjson([{"a": 1}], "test.ndjson.gz")
        assert result is False


class TestGCSClientReadNdjson:

    @pytest.fixture
    def gcs(self):
        with patch("cpls.gcs.storage"), \
             patch("cpls.gcs.Path"), \
             patch("cpls.gcs.WRITE_TO_DISK", False):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()
            client.write_local_copies = False
            return client

    @pytest.mark.asyncio
    async def test_read_compressed_ndjson(self, gcs):
        data = [{"a": 1}, {"b": 2}]
        ndjson = "\n".join(json.dumps(d) for d in data)
        compressed = gzip.compress(ndjson.encode())

        mock_blob = MagicMock()
        mock_blob.generation = 1
        mock_blob.download_as_bytes.return_value = compressed
        gcs.bucket.blob.return_value = mock_blob

        result = await gcs.read_ndjson("data/test.ndjson.gz")
        assert result == data

    @pytest.mark.asyncio
    async def test_skips_empty_lines(self, gcs):
        ndjson = '{"a": 1}\n\n{"b": 2}\n'
        compressed = gzip.compress(ndjson.encode())

        mock_blob = MagicMock()
        mock_blob.generation = 1
        mock_blob.download_as_bytes.return_value = compressed
        gcs.bucket.blob.return_value = mock_blob

        result = await gcs.read_ndjson("data/test.ndjson.gz")
        assert len(result) == 2


class TestGCSClientUploadJobResult:

    @pytest.fixture
    def gcs(self):
        with patch("cpls.gcs.storage"), \
             patch("cpls.gcs.Path"), \
             patch("cpls.gcs.WRITE_TO_DISK", False):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()
            client.write_local_copies = False
            return client

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_upload_job_result_builds_correct_blob(self, gcs):
        from cpls.jobs import Job, JobStatus
        job = Job(
            id="test-job-123",
            type="scheduled",
            payload={"infra_dao_slug": "optimism"},
            status=JobStatus.COMPLETED,
            created_at=datetime(2024, 1, 15, 10, 0),
            started_at=datetime(2024, 1, 15, 10, 0, 1),
            completed_at=datetime(2024, 1, 15, 10, 5),
        )

        mock_blob = MagicMock()
        gcs.bucket.blob.return_value = mock_blob

        await gcs.upload_job_result(job)
        # Should have called upload_from_string
        assert mock_blob.upload_from_string.called

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_safe_upload_swallows_exception(self, gcs):
        from cpls.jobs import Job, JobStatus
        job = Job(
            id="fail-job",
            type="scheduled",
            payload={},
            status=JobStatus.FAILED,
            created_at=datetime(2024, 1, 1),
        )
        gcs.bucket.blob.side_effect = Exception("GCS unavailable")
        # Should not raise
        await gcs.safe_upload_job_result(job)


class TestWriteLocalCopy:

    def test_skips_when_disabled(self):
        with patch("cpls.gcs.storage"), \
             patch("cpls.gcs.Path"), \
             patch("cpls.gcs.WRITE_TO_DISK", False):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.write_local_copies = False
            # Should return without error
            client._write_local_copy("test.json", b'{}')

    def test_writes_binary_when_enabled(self, tmp_path):
        with patch("cpls.gcs.storage"), \
             patch("cpls.gcs.WRITE_TO_DISK", True):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.write_local_copies = True
            client.local_copy_dir = str(tmp_path)
            client._write_local_copy("data/sub/test.bin", b'\x00\x01', is_text=False)
            written = (tmp_path / "data" / "sub" / "test.bin").read_bytes()
            assert written == b'\x00\x01'

    def test_writes_text_when_enabled(self, tmp_path):
        with patch("cpls.gcs.storage"), \
             patch("cpls.gcs.WRITE_TO_DISK", True):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.write_local_copies = True
            client.local_copy_dir = str(tmp_path)
            client._write_local_copy("data/test.json", b'{"a":1}', is_text=True)
            written = (tmp_path / "data" / "test.json").read_text()
            assert written == '{"a":1}'

    def test_handles_write_exception_silently(self):
        with patch("cpls.gcs.storage"), \
             patch("cpls.gcs.WRITE_TO_DISK", True):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.write_local_copies = True
            client.local_copy_dir = "/nonexistent/readonly/path"
            with patch("cpls.gcs.Path") as mock_path:
                mock_path.return_value.__truediv__.return_value.parent.mkdir.side_effect = PermissionError
                mock_path.return_value.__truediv__.return_value.write_bytes.side_effect = PermissionError
                # Should not raise
                client._write_local_copy("test.bin", b'data')


class TestGCSClientInit:

    def test_init_with_google_credentials_env(self):
        """When GOOGLE_CREDENTIALS env var is set, use from_service_account_info."""
        fake_creds = '{"type": "service_account", "project_id": "test"}'
        with patch("cpls.gcs.storage") as mock_storage, \
             patch("cpls.gcs.Path"), \
             patch("cpls.gcs.WRITE_TO_DISK", False), \
             patch.dict("os.environ", {"GOOGLE_CREDENTIALS": fake_creds}):
            from cpls.gcs import GCSClient
            client = GCSClient("test-bucket")
            mock_storage.Client.from_service_account_info.assert_called_once_with(
                {"type": "service_account", "project_id": "test"}
            )

    def test_init_without_google_credentials_env(self):
        """Without GOOGLE_CREDENTIALS, use default Client()."""
        with patch("cpls.gcs.storage") as mock_storage, \
             patch("cpls.gcs.Path"), \
             patch("cpls.gcs.WRITE_TO_DISK", False), \
             patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("GOOGLE_CREDENTIALS", None)
            from importlib import reload
            import cpls.gcs as gcs_module
            from cpls.gcs import GCSClient
            client = GCSClient("test-bucket")
            mock_storage.Client.assert_called_once()


class TestUploadDictWriteToDisk:

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", True)
    async def test_write_to_disk_calls_local_copy(self):
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()
            client.write_local_copies = False
            client.bucket.blob.return_value = MagicMock()

            with patch.object(client, '_write_local_copy') as mock_write:
                result = await client.upload_dict({"k": "v"}, "data/test.json.gz")
                assert result is True
                mock_write.assert_called_once()

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", True)
    async def test_ndjson_write_to_disk_calls_local_copy(self):
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()
            client.write_local_copies = False
            client.bucket.blob.return_value = MagicMock()

            with patch.object(client, '_write_local_copy') as mock_write:
                result = await client.upload_ndjson([{"a": 1}], "data/test.ndjson.gz")
                assert result is True
                mock_write.assert_called_once()


class TestUploadNdjsonUncompressed:

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_uncompressed_ndjson_blob_name(self):
        """Blob names ending in .ndjson (no .gz) use uncompressed upload."""
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()
            mock_blob = MagicMock()
            client.bucket.blob.return_value = mock_blob

            result = await client.upload_ndjson([{"a": 1}], "data/test.ndjson")
            assert result is True
            call_args = mock_blob.upload_from_string.call_args
            assert call_args[1]["content_type"] == "application/x-ndjson"

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_upload_ndjson_with_cache_control_and_metadata(self):
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()
            mock_blob = MagicMock()
            client.bucket.blob.return_value = mock_blob

            result = await client.upload_ndjson(
                [{"x": 1}], "test.ndjson.gz",
                cache_control="public, max-age=60",
                metadata={"k": "v"}
            )
            assert result is True
            assert mock_blob.cache_control == "public, max-age=60"
            assert mock_blob.metadata == {"k": "v"}

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_uncompressed_ndjson_with_cache_control_and_metadata(self):
        """Lines 238/241: uncompressed ndjson blob with cache_control + metadata."""
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()
            mock_blob = MagicMock()
            client.bucket.blob.return_value = mock_blob

            result = await client.upload_ndjson(
                [{"y": 2}], "data/test.ndjson",
                cache_control="public, max-age=30",
                metadata={"env": "prod"}
            )
            assert result is True
            assert mock_blob.cache_control == "public, max-age=30"
            assert mock_blob.metadata == {"env": "prod"}


class TestReadNdjsonUncompressed:

    @pytest.mark.asyncio
    async def test_read_uncompressed_ndjson(self):
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"), patch("cpls.gcs.WRITE_TO_DISK", False):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()

            ndjson = '{"a": 1}\n{"b": 2}'
            mock_blob = MagicMock()
            mock_blob.generation = 1
            mock_blob.download_as_string.return_value = ndjson
            client.bucket.blob.return_value = mock_blob

            result = await client.read_ndjson("data/test.ndjson")
            assert result == [{"a": 1}, {"b": 2}]


class TestGCSClientEdgeCases:

    def test_bucket_init_failure_is_silent(self):
        """When client.bucket() raises, the warning is printed but no exception propagates."""
        with patch("cpls.gcs.storage") as mock_storage, \
             patch("cpls.gcs.Path"), \
             patch("cpls.gcs.WRITE_TO_DISK", False), \
             patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("GOOGLE_CREDENTIALS", None)
            mock_storage.Client.return_value.bucket.side_effect = Exception("Bucket not found")
            from cpls.gcs import GCSClient
            client = GCSClient("nonexistent-bucket")
            # No attribute 'bucket' set, but no exception raised either
            assert client.client is not None

    @pytest.mark.asyncio
    async def test_get_blob_delegates_to_bucket(self):
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"), patch("cpls.gcs.WRITE_TO_DISK", False):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            mock_blob = MagicMock()
            client.bucket = MagicMock()
            client.bucket.blob.return_value = mock_blob

            result = await client.get_blob("my/path.json.gz")
            assert result is mock_blob
            client.bucket.blob.assert_called_once_with("my/path.json.gz")

    @pytest.mark.asyncio
    async def test_list_blobs_delegates_to_bucket(self):
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"), patch("cpls.gcs.WRITE_TO_DISK", False):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            mock_iter = iter([MagicMock(name="b1"), MagicMock(name="b2")])
            client.bucket = MagicMock()
            client.bucket.list_blobs.return_value = mock_iter

            result = await client.list_blobs("data/prefix/")
            client.bucket.list_blobs.assert_called_once_with(prefix="data/prefix/")
            assert result is mock_iter

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_upload_ndjson_raises_on_exception(self):
        """upload_ndjson raises Exception on failure."""
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()
            client.bucket.blob.side_effect = Exception("bucket error")

            with pytest.raises(Exception, match="Failed to upload NDJSON"):
                await client.upload_ndjson([{"a": 1}], "test.ndjson.gz")

    @pytest.mark.asyncio
    async def test_read_ndjson_raises_when_no_client(self):
        """read_ndjson raises when client is None."""
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"), patch("cpls.gcs.WRITE_TO_DISK", False):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = None

            with pytest.raises(Exception, match="GCS client not available"):
                await client.read_ndjson("test.ndjson.gz")

    @pytest.mark.asyncio
    async def test_read_ndjson_raises_on_download_failure(self):
        """read_ndjson re-raises download errors."""
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"), patch("cpls.gcs.WRITE_TO_DISK", False):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            mock_blob = MagicMock()
            mock_blob.reload.side_effect = Exception("network error")
            client.bucket = MagicMock()
            client.bucket.blob.return_value = mock_blob

            with pytest.raises(Exception, match="Failed to read NDJSON"):
                await client.read_ndjson("test.ndjson.gz")

    @pytest.mark.asyncio
    @patch("cpls.gcs.ENVIRONMENT", "prod")
    @patch("cpls.gcs.WRITE_TO_DISK", False)
    async def test_safe_upload_catches_upload_job_result_exception(self):
        """safe_upload_job_result swallows exceptions raised by upload_job_result."""
        from cpls.jobs import Job, JobStatus
        job = Job(id="j1", type="scheduled", payload={},
                  status=JobStatus.COMPLETED, created_at=datetime(2024, 1, 1))

        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"):
            from cpls.gcs import GCSClient
            client = GCSClient.__new__(GCSClient)
            client.client = MagicMock()
            client.bucket = MagicMock()

            with patch.object(client, 'upload_job_result', new_callable=AsyncMock) as mock_upload:
                mock_upload.side_effect = Exception("forced upload_job_result failure")
                await client.safe_upload_job_result(job)  # should not raise
