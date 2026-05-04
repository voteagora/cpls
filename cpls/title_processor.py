import re


def strip_leading_undefined(description: str | None) -> str:
    if not description:
        return description or ""
    return re.sub(r"^undefined[\s\r\n]*", "", description)


def extract_title(body: str | None) -> str | None:
    if not body:
        return None

    # Match markdown headers like "# Title", "## Title", etc.
    hash_result = re.match(r"^\s*#{1,6}\s+([^\n]+)", body)
    if hash_result:
        return hash_result.group(1)

    # Match underlined headers like:
    # Title
    # -----
    equal_result = re.match(r"^\s*([^\n]+)\n(={3,25}|-{3,25})", body)
    if equal_result:
        return equal_result.group(1)

    # Fallback: just take the first line of text
    text_result = re.match(r"^\s*([^\n]+)\s*", body)
    if text_result:
        return text_result.group(1)

    return None


def remove_bold(text: str | None) -> str | None:
    return re.sub(r"\*\*", "", text) if text else text


def remove_italics(text: str | None) -> str | None:
    return re.sub(r"__", "", text) if text else text


def get_title_from_proposal_description(description: str = "") -> str:
    # Normalize escaped newlines and remove wrapping quotes
    normalized_description = re.sub(r"\\n", "\n", description)
    normalized_description = re.sub(r"(^['\"]|['\"]$)", "", normalized_description)

    title = extract_title(normalized_description)
    title = remove_bold(remove_italics(title))

    return title.strip() if title and title.strip() else "Untitled"