"""
Tests for proposal lifecycle stage determination logic.

These tests verify the lifecycle stage calculation in EASOoDaoSync
by testing the conditional logic directly with pre-built proposal dicts.
"""

import pytest


class TestOoDaoLifecycleDetermination:
    """
    Tests the lifecycle stage logic from sync_eas_oodao.py lines 687-800.
    We replicate the logic here to test it in isolation without needing
    the full refresh_list flow.
    """

    @staticmethod
    def determine_lifecycle(proposal, curts, startts, endts, proposal_type_name):
        """
        Extracted lifecycle determination logic from EASOoDaoSync.refresh_list().
        """
        if "delete_event" in proposal:
            return "CANCELLED"
        elif curts < startts:
            return "PENDING"
        elif startts <= curts < endts:
            return "ACTIVE"
        elif curts >= endts:
            if proposal_type_name == "UNSET":
                return "EXPIRED"
            elif proposal_type_name == "OPTIMISTIC":
                outcome_data = proposal["outcome"]["token-holders"]
                for_votes = int(outcome_data.get("1", 0))
                against_votes = int(outcome_data.get("0", 0))
                abstain_votes = int(outcome_data.get("2", 0))
                total_votes = for_votes + against_votes + abstain_votes
                passing_quorum = (proposal["proposal_type"]["quorum"] / 10000) * int(
                    proposal["total_voting_power_at_start"]
                )
                quorum_check = total_votes >= passing_quorum

                if not quorum_check:
                    return "SUCCEEDED"  # Optimistic passes by default
                else:
                    threshold = proposal["proposal_type"].get("threshold", 0)
                    threshold_value = (threshold / 10000) * int(
                        proposal["total_voting_power_at_start"]
                    )
                    if against_votes > threshold_value:
                        return "DEFEATED"
                    else:
                        return "SUCCEEDED"

            elif proposal_type_name == "STANDARD":
                passing_quorum = (proposal["proposal_type"]["quorum"] / 10000) * int(
                    proposal["total_voting_power_at_start"]
                )
                passing_approval_threshold = (
                    proposal["proposal_type"]["approval_threshold"] / 10000
                ) * int(proposal["total_voting_power_at_start"])

                total_weight = sum(
                    int(w) for w in proposal["outcome"]["token-holders"].values()
                )
                quorum_check = total_weight >= passing_quorum
                approval_check = (
                    int(proposal["outcome"]["token-holders"].get("1", 0))
                    >= passing_approval_threshold
                )

                if quorum_check and approval_check:
                    return "PASSED"
                else:
                    return "DEFEATED"
            else:
                raise ValueError(f"Unhandled type: {proposal_type_name}")
        raise ValueError("Unhandled time state")

    # --- CANCELLED ---

    def test_cancelled_takes_precedence(self):
        proposal = {
            "delete_event": {"uid": "0x123"},
            "outcome": {"token-holders": {}},
        }
        result = self.determine_lifecycle(proposal, curts=9999, startts=100, endts=200, proposal_type_name="STANDARD")
        assert result == "CANCELLED"

    # --- PENDING ---

    def test_pending_before_start(self):
        proposal = {"outcome": {"token-holders": {}}}
        result = self.determine_lifecycle(proposal, curts=50, startts=100, endts=200, proposal_type_name="STANDARD")
        assert result == "PENDING"

    # --- ACTIVE ---

    def test_active_during_voting(self):
        proposal = {"outcome": {"token-holders": {}}}
        result = self.determine_lifecycle(proposal, curts=150, startts=100, endts=200, proposal_type_name="STANDARD")
        assert result == "ACTIVE"

    def test_active_at_exact_start(self):
        proposal = {"outcome": {"token-holders": {}}}
        result = self.determine_lifecycle(proposal, curts=100, startts=100, endts=200, proposal_type_name="STANDARD")
        assert result == "ACTIVE"

    # --- UNSET / EXPIRED ---

    def test_unset_type_expired_after_end(self):
        proposal = {"outcome": {"token-holders": {}}}
        result = self.determine_lifecycle(proposal, curts=300, startts=100, endts=200, proposal_type_name="UNSET")
        assert result == "EXPIRED"

    # --- OPTIMISTIC ---

    def test_optimistic_succeeds_when_quorum_not_met(self):
        proposal = {
            "outcome": {"token-holders": {"0": "0", "1": "100", "2": "0"}},
            "proposal_type": {"quorum": 5000, "threshold": 5000},  # 50% quorum
            "total_voting_power_at_start": "10000",
        }
        # 100 votes < 5000 quorum → passes by default
        result = self.determine_lifecycle(proposal, curts=300, startts=100, endts=200, proposal_type_name="OPTIMISTIC")
        assert result == "SUCCEEDED"

    def test_optimistic_defeated_when_against_exceeds_threshold(self):
        proposal = {
            "outcome": {"token-holders": {"0": "6000", "1": "1000", "2": "1000"}},
            "proposal_type": {"quorum": 5000, "threshold": 5000},  # 50% threshold
            "total_voting_power_at_start": "10000",
        }
        # Total 8000 >= 5000 quorum, against 6000 > 5000 threshold → defeated
        result = self.determine_lifecycle(proposal, curts=300, startts=100, endts=200, proposal_type_name="OPTIMISTIC")
        assert result == "DEFEATED"

    def test_optimistic_succeeds_when_against_below_threshold(self):
        proposal = {
            "outcome": {"token-holders": {"0": "1000", "1": "5000", "2": "2000"}},
            "proposal_type": {"quorum": 5000, "threshold": 5000},
            "total_voting_power_at_start": "10000",
        }
        # Total 8000 >= 5000, against 1000 < 5000 → succeeds
        result = self.determine_lifecycle(proposal, curts=300, startts=100, endts=200, proposal_type_name="OPTIMISTIC")
        assert result == "SUCCEEDED"

    # --- STANDARD ---

    def test_standard_passed_with_quorum_and_approval(self):
        proposal = {
            "outcome": {"token-holders": {"0": "1000", "1": "6000", "2": "500"}},
            "proposal_type": {"quorum": 5000, "approval_threshold": 5000},
            "total_voting_power_at_start": "10000",
        }
        # Total 7500 >= 5000 quorum, for 6000 >= 5000 approval → PASSED
        result = self.determine_lifecycle(proposal, curts=300, startts=100, endts=200, proposal_type_name="STANDARD")
        assert result == "PASSED"

    def test_standard_defeated_quorum_not_met(self):
        proposal = {
            "outcome": {"token-holders": {"0": "100", "1": "200"}},
            "proposal_type": {"quorum": 5000, "approval_threshold": 5000},
            "total_voting_power_at_start": "10000",
        }
        # Total 300 < 5000 quorum → DEFEATED
        result = self.determine_lifecycle(proposal, curts=300, startts=100, endts=200, proposal_type_name="STANDARD")
        assert result == "DEFEATED"

    def test_standard_defeated_approval_not_met(self):
        proposal = {
            "outcome": {"token-holders": {"0": "4000", "1": "4000", "2": "1000"}},
            "proposal_type": {"quorum": 5000, "approval_threshold": 5000},
            "total_voting_power_at_start": "10000",
        }
        # Total 9000 >= 5000 quorum, but for 4000 < 5000 approval → DEFEATED
        result = self.determine_lifecycle(proposal, curts=300, startts=100, endts=200, proposal_type_name="STANDARD")
        assert result == "DEFEATED"

    def test_standard_edge_case_exact_threshold(self):
        proposal = {
            "outcome": {"token-holders": {"0": "0", "1": "5000", "2": "0"}},
            "proposal_type": {"quorum": 5000, "approval_threshold": 5000},
            "total_voting_power_at_start": "10000",
        }
        # Total 5000 == 5000 quorum, for 5000 == 5000 approval → PASSED
        result = self.determine_lifecycle(proposal, curts=300, startts=100, endts=200, proposal_type_name="STANDARD")
        assert result == "PASSED"
