from cpls.title_processor import (
    extract_title,
    remove_bold,
    remove_italics,
    get_title_from_proposal_description,
)


class TestExtractTitle:

    def test_markdown_h1(self):
        assert extract_title("# My Proposal") == "My Proposal"

    def test_markdown_h2(self):
        assert extract_title("## Sub Heading") == "Sub Heading"

    def test_markdown_h3(self):
        assert extract_title("### Deep Heading") == "Deep Heading"

    def test_markdown_h1_with_leading_whitespace(self):
        assert extract_title("  # Indented Title") == "Indented Title"

    def test_underlined_header_equals(self):
        assert extract_title("My Title\n=====") == "My Title"

    def test_underlined_header_dashes(self):
        assert extract_title("My Title\n-----") == "My Title"

    def test_fallback_first_line(self):
        assert extract_title("Just a plain line") == "Just a plain line"

    def test_empty_string(self):
        assert extract_title("") is None

    def test_none_input(self):
        assert extract_title(None) is None

    def test_whitespace_only(self):
        # Falls through all regex, returns None
        result = extract_title("   \n   ")
        assert result is None or result.strip() == ""

    def test_newline_only_returns_none(self):
        # "\n" body: truthy, no header, text regex can't match → line 24 return None
        assert extract_title("\n") is None

    def test_multiline_picks_first_header(self):
        body = "# First\n## Second\nParagraph"
        assert extract_title(body) == "First"


class TestRemoveBold:

    def test_removes_double_asterisks(self):
        assert remove_bold("**bold text**") == "bold text"

    def test_none_input(self):
        assert remove_bold(None) is None

    def test_no_bold(self):
        assert remove_bold("plain text") == "plain text"


class TestRemoveItalics:

    def test_removes_double_underscore(self):
        assert remove_italics("__italic__") == "italic"

    def test_none_input(self):
        assert remove_italics(None) is None

    def test_no_italics(self):
        assert remove_italics("plain text") == "plain text"


class TestGetTitleFromProposalDescription:

    def test_basic_markdown_header(self):
        assert get_title_from_proposal_description("# My Proposal") == "My Proposal"

    def test_escaped_newlines(self):
        desc = "# Title Here\\nSome body text"
        assert get_title_from_proposal_description(desc) == "Title Here"

    def test_wrapped_quotes(self):
        desc = "'# Quoted Title'"
        assert get_title_from_proposal_description(desc) == "Quoted Title"

    def test_bold_in_title(self):
        desc = "# **Bold Title**"
        assert get_title_from_proposal_description(desc) == "Bold Title"

    def test_italic_in_title(self):
        desc = "# __Italic Title__"
        assert get_title_from_proposal_description(desc) == "Italic Title"

    def test_empty_returns_untitled(self):
        assert get_title_from_proposal_description("") == "Untitled"

    def test_whitespace_returns_untitled(self):
        assert get_title_from_proposal_description("   ") == "Untitled"

    def test_default_arg_returns_untitled(self):
        assert get_title_from_proposal_description() == "Untitled"
