import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import httpx

from cpls.blockcache import BlockCacheClient, BlockNotFound


class TestReturnTs:

    @pytest.fixture
    def client(self):
        return BlockCacheClient("http://test", "test-key", http_client=AsyncMock())

    def test_returns_timestamp(self, client):
        assert client.return_ts({"ts": 1700000000}) == 1700000000

    def test_block_not_found(self, client):
        with pytest.raises(BlockNotFound):
            client.return_ts({"msg": "block not found"})

    def test_unhandled_response_raises(self, client):
        with pytest.raises(Exception, match="Unhandled client response"):
            client.return_ts({"msg": "something else"})

    def test_empty_ts_falls_through(self, client):
        # ts is None → falls to msg check
        with pytest.raises(Exception, match="Unhandled client response"):
            client.return_ts({"ts": None, "msg": "unknown"})


class TestHeaders:

    def test_headers_include_api_key(self):
        client = BlockCacheClient("http://test", "my-api-key", http_client=AsyncMock())
        assert client.headers() == {"alchemy-api-key": "my-api-key"}


class TestClearLru:

    def test_clears_ens_cache(self):
        client = BlockCacheClient("http://test", "key", http_client=AsyncMock())
        client.cached_ens = {"some_key": "some_value"}
        client.clear_lru()
        assert client.cached_ens == {}


class TestGetBlocktime:

    @pytest.fixture
    def client(self):
        return BlockCacheClient("http://test", "key", http_client=AsyncMock())

    @pytest.mark.asyncio
    async def test_returns_exact_when_found(self, client):
        client.get_exact_blocktime = AsyncMock(return_value=1700000000)
        result = await client.get_blocktime(10, 12345)
        assert result == 1700000000
        client.get_exact_blocktime.assert_called_once_with(10, 12345)

    @pytest.mark.asyncio
    async def test_falls_back_to_estimated(self, client):
        client.get_exact_blocktime = AsyncMock(side_effect=BlockNotFound())
        client.get_estimated_blocktime = AsyncMock(return_value=1700000100)
        result = await client.get_blocktime(10, 99999)
        assert result == 1700000100
        client.get_estimated_blocktime.assert_called_once_with(10, 99999)


class TestGetExactBlocktime:

    @pytest.mark.asyncio
    async def test_calls_correct_url(self):
        mock_http = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ts": 1700000000}
        mock_http.get = AsyncMock(return_value=mock_resp)

        client = BlockCacheClient("http://blockcache", "key", http_client=mock_http)
        result = await client.get_exact_blocktime(10, 500)
        assert result == 1700000000
        mock_http.get.assert_called_once_with(
            "http://blockcache/exact_blocktime/10/500",
            headers={"alchemy-api-key": "key"}
        )


class TestGetEnsLru:

    @pytest.mark.asyncio
    async def test_cache_hit(self):
        client = BlockCacheClient("http://test", "key", http_client=AsyncMock())
        client.cached_ens = {"['0xabc', 1]": {"name": "test.eth"}}
        result = await client.get_ens_lru("0xabc", chain_id=1)
        assert result == {"name": "test.eth"}

    @pytest.mark.asyncio
    async def test_cache_miss_fetches_and_stores(self):
        client = BlockCacheClient("http://test", "key", http_client=AsyncMock())
        client.get_ens = AsyncMock(return_value={"name": "vitalik.eth"})
        result = await client.get_ens_lru("0xd8da", chain_id=1)
        assert result == {"name": "vitalik.eth"}
        assert "['0xd8da', 1]" in client.cached_ens


class TestContractCallEncoded:

    @pytest.mark.asyncio
    async def test_no_params_method(self):
        client = BlockCacheClient("http://test", "key", http_client=AsyncMock())
        client.contract_call = AsyncMock(return_value={"result": "0x1234"})

        result = await client.contract_call_encoded(
            10, "0xcontract", 100, "votableSupply()", []
        )
        assert result == {"result": "0x1234"}
        client.contract_call.assert_called_once_with(
            10, "0xcontract", "votableSupply()", 100, ""
        )

    @pytest.mark.asyncio
    async def test_with_params(self):
        client = BlockCacheClient("http://test", "key", http_client=AsyncMock())
        client.contract_call = AsyncMock(return_value={"result": "0xabcd"})

        result = await client.contract_call_encoded(
            10, "0xcontract", 100, "votableSupply(uint256)", [12345]
        )
        assert result == {"result": "0xabcd"}
        # Verify data param is non-empty hex
        call_args = client.contract_call.call_args
        assert call_args[0][4] != ""  # data should be non-empty


class TestVotableSupplyAtBlock:

    @pytest.mark.asyncio
    async def test_returns_int_from_hex(self):
        client = BlockCacheClient("http://test", "key", http_client=AsyncMock())
        client.contract_call_encoded = AsyncMock(
            return_value={"result": "0x0000000000000000000000000000000000000000000000000000000000000064"}
        )
        result = await client.votable_supply_at_block(10, "0xcontract", 100)
        assert result == 100  # 0x64 = 100

    @pytest.mark.asyncio
    async def test_with_oracle_uses_max_block(self):
        client = BlockCacheClient("http://test", "key", http_client=AsyncMock())
        client.contract_call_encoded = AsyncMock(
            return_value={"result": "0x64"}
        )
        # block_number < BLOCK_ON_JAN_18_2024 (114968612), chain_id=10 triggers OP logic
        await client.votable_supply_at_block_with_oracle(10, "0xcontract", 100)
        # Should use the max of [114968612, 100] = 114968612
        call_args = client.contract_call_encoded.call_args
        assert call_args[0][2] == 114968612  # as_of_block_number

    @pytest.mark.asyncio
    async def test_with_oracle_invalid_result_raises(self):
        client = BlockCacheClient("http://test", "key", http_client=AsyncMock())
        client.contract_call_encoded = AsyncMock(return_value={"result": "0x"})
        with pytest.raises(ValueError):
            await client.votable_supply_at_block_with_oracle(10, "0xcontract", 100)

    @pytest.mark.asyncio
    async def test_oracle_high_block_uses_block_number_directly(self):
        """When block_number > BLOCK_ON_JAN_18_2024, as_of_block_number == block_number."""
        client = BlockCacheClient("http://test", "key", http_client=AsyncMock())
        client.contract_call_encoded = AsyncMock(return_value={"result": "0x64"})
        high_block = 200000000  # > BLOCK_ON_JAN_18_2024 (114968612)
        await client.votable_supply_at_block_with_oracle(10, "0xcontract", high_block)
        call_args = client.contract_call_encoded.call_args
        assert call_args[0][2] == high_block  # as_of_block_number == block_number


class TestGetEstimatedBlocktime:

    @pytest.mark.asyncio
    async def test_calls_correct_url(self):
        mock_http = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ts": 1700000200}
        mock_http.get = AsyncMock(return_value=mock_resp)

        client = BlockCacheClient("http://blockcache", "key", http_client=mock_http)
        result = await client.get_estimated_blocktime(10, 999)
        assert result == 1700000200
        mock_http.get.assert_called_once_with(
            "http://blockcache/estimated_blocktime/10/999",
            headers={"alchemy-api-key": "key"}
        )


class TestLastBlockBeforeTimestamp:

    @pytest.mark.asyncio
    async def test_returns_block_number(self):
        mock_http = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"block_number": 18000000}
        mock_http.get = AsyncMock(return_value=mock_resp)

        client = BlockCacheClient("http://blockcache", "key", http_client=mock_http)
        result = await client.last_block_before_timestamp(10, 1700000000)
        assert result == 18000000
        mock_http.get.assert_called_once_with(
            "http://blockcache/last_block_before_timestamp/10/1700000000",
            headers={"alchemy-api-key": "key"}
        )


class TestGetEns:

    @pytest.mark.asyncio
    async def test_returns_data_when_found(self):
        mock_http = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"name": "vitalik.eth"}
        mock_http.get = AsyncMock(return_value=mock_resp)

        client = BlockCacheClient("http://blockcache", "key", http_client=mock_http)
        result = await client.get_ens("0xd8da", chain_id=1)
        assert result == {"name": "vitalik.eth"}

    @pytest.mark.asyncio
    async def test_returns_none_on_404(self):
        mock_http = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_http.get = AsyncMock(return_value=mock_resp)

        client = BlockCacheClient("http://blockcache", "key", http_client=mock_http)
        result = await client.get_ens("0xunknown", chain_id=1)
        assert result is None


class TestGetDecodedEas:

    @pytest.mark.asyncio
    async def test_returns_data_on_200(self):
        mock_http = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"decoded": "data"}
        mock_http.get = AsyncMock(return_value=mock_resp)

        client = BlockCacheClient("http://blockcache", "key", http_client=mock_http)
        result = await client.get_decoded_eas(1, "0xAttestation")
        assert result == {"decoded": "data"}
        mock_http.get.assert_called_once_with(
            "http://blockcache/decoded_eas/1/attestation/0xAttestation",
            headers={"alchemy-api-key": "key"}
        )

    @pytest.mark.asyncio
    async def test_returns_none_on_non_200(self):
        mock_http = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_http.get = AsyncMock(return_value=mock_resp)

        client = BlockCacheClient("http://blockcache", "key", http_client=mock_http)
        result = await client.get_decoded_eas(1, "0xBad")
        assert result is None


class TestContractCall:

    @pytest.mark.asyncio
    async def test_posts_correct_payload(self):
        mock_http = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"result": "0x1"}
        mock_resp.raise_for_status = MagicMock()
        mock_http.post = AsyncMock(return_value=mock_resp)

        client = BlockCacheClient("http://blockcache", "key", http_client=mock_http)
        result = await client.contract_call(10, "0xContract", "state(uint256)", 500, "abc123")

        assert result == {"result": "0x1"}
        call_kwargs = mock_http.post.call_args[1]
        assert call_kwargs["json"]["block_number"] == 500
        assert call_kwargs["json"]["data"] == "abc123"
        assert call_kwargs["json"]["method_signature"] == "state(uint256)"


class TestGetTransactionByIndex:

    @pytest.mark.asyncio
    async def test_returns_transaction_data(self):
        mock_http = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"hash": "0xabc", "index": 0}
        mock_resp.raise_for_status = MagicMock()
        mock_http.get = AsyncMock(return_value=mock_resp)

        client = BlockCacheClient("http://blockcache", "key", http_client=mock_http)
        result = await client.get_transaction_by_index(10, 500, 0)
        assert result == {"hash": "0xabc", "index": 0}
        mock_http.get.assert_called_once_with(
            "http://blockcache/transaction/10/500/0",
            headers={"alchemy-api-key": "key"}
        )
