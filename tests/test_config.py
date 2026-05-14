from unittest.mock import patch
from cpls.config import get_proposal_check_api_url


class TestGetProposalCheckApiUrl:

    @patch("cpls.config.ENVIRONMENT", "prod")
    @patch("cpls.config.PROPOSAL_CHECK_API_URL", "/api/check")
    def test_prod_syndicate(self):
        result = get_proposal_check_api_url("syndicate")
        assert result == "https://www.syndicatecollective.org/api/check"

    @patch("cpls.config.ENVIRONMENT", "dev")
    @patch("cpls.config.PROPOSAL_CHECK_API_URL", "/api/check")
    def test_dev_syndicate(self):
        result = get_proposal_check_api_url("syndicate")
        assert "syndicate" in result
        assert result.endswith("/api/check")

    @patch("cpls.config.ENVIRONMENT", "prod")
    @patch("cpls.config.PROPOSAL_CHECK_API_URL", "/api/check")
    def test_unknown_dao_returns_empty(self):
        result = get_proposal_check_api_url("unknown_dao")
        assert result == ""

    @patch("cpls.config.ENVIRONMENT", "prod")
    @patch("cpls.config.PROPOSAL_CHECK_API_URL", "")
    def test_empty_api_url_returns_empty(self):
        result = get_proposal_check_api_url("syndicate")
        assert result == ""

    @patch("cpls.config.ENVIRONMENT", "prod")
    @patch("cpls.config.PROPOSAL_CHECK_API_URL", "/api/check")
    def test_prod_towns(self):
        result = get_proposal_check_api_url("towns")
        assert result == "https://www.townslodge.com/api/check"
