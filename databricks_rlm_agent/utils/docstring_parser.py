"""Docstring parser for RLM delegation blobs.

This module parses the delegation blob format used by delegate_code_results:

Format:
    '''<instruction>'''
    <code>

Or:
    \"\"\"<instruction>\"\"\"
    <code>

The instruction is extracted as sublm_instruction, and the remaining code
is extracted as agent_code.

Example:
    >>> blob = '''
    ... '''Analyze the query results and identify top vendors by spend.'''
    ... import pandas as pd
    ... df = spark.sql("SELECT * FROM vendors").toPandas()
    ... print(df.head())
    ... '''
    >>> parsed = parse_delegation_blob(blob)
    >>> parsed.sublm_instruction
    'Analyze the query results and identify top vendors by spend.'
    >>> 'import pandas' in parsed.agent_code
    True
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


class DelegationBlobParseError(ValueError):
    """Raised when a delegation blob cannot be parsed."""

    def __init__(self, message: str, blob: str):
        self.blob = blob
        super().__init__(message)


@dataclass
class ParsedDelegationBlob:
    """Result of parsing a delegation blob.

    Attributes:
        sublm_instruction: The instruction text for the sub-LM (may be None).
        agent_code: The Python code to execute.
        has_instruction: Whether an instruction was found.
        raw_blob: The original unparsed blob.
    """

    sublm_instruction: Optional[str]
    agent_code: str
    has_instruction: bool
    raw_blob: str

    @property
    def is_valid(self) -> bool:
        """Check if the parsed blob is valid (has code)."""
        return bool(self.agent_code and self.agent_code.strip())


# Regex patterns for extracting instruction from docstring
# Matches triple single quotes or triple double quotes at the start
_TRIPLE_SINGLE_PATTERN = re.compile(
    r"^\s*'''(.*?)'''\s*\n?",
    re.DOTALL,
)
_TRIPLE_DOUBLE_PATTERN = re.compile(
    r'^\s*"""(.*?)"""\s*\n?',
    re.DOTALL,
)

# Pattern to detect if blob starts with a docstring
_DOCSTRING_START_PATTERN = re.compile(r'^\s*(\'\'\'|""")')


def parse_delegation_blob(blob: str) -> ParsedDelegationBlob:
    """Parse a delegation blob into instruction and code components.

    The blob format expects an optional docstring instruction followed by
    Python code:

        '''<instruction text>'''
        <python code>

    Or using double quotes:

        \"\"\"<instruction text>\"\"\"
        <python code>

    If no docstring is found at the start, the entire blob is treated as code
    with no instruction.

    Args:
        blob: The raw delegation blob string.

    Returns:
        ParsedDelegationBlob with extracted instruction and code.

    Raises:
        DelegationBlobParseError: If the blob has a malformed docstring.
    """
    if not blob or not blob.strip():
        return ParsedDelegationBlob(
            sublm_instruction=None,
            agent_code="",
            has_instruction=False,
            raw_blob=blob,
        )

    # Normalize the blob (strip leading/trailing whitespace)
    normalized = blob.strip()

    # Check if the blob starts with a docstring
    if not _DOCSTRING_START_PATTERN.match(normalized):
        # No docstring, entire blob is code
        return ParsedDelegationBlob(
            sublm_instruction=None,
            agent_code=normalized,
            has_instruction=False,
            raw_blob=blob,
        )

    # Try to extract instruction from triple single quotes
    match = _TRIPLE_SINGLE_PATTERN.match(normalized)
    if match:
        instruction = match.group(1).strip()
        code = normalized[match.end():].strip()
        return ParsedDelegationBlob(
            sublm_instruction=instruction if instruction else None,
            agent_code=code,
            has_instruction=bool(instruction),
            raw_blob=blob,
        )

    # Try to extract instruction from triple double quotes
    match = _TRIPLE_DOUBLE_PATTERN.match(normalized)
    if match:
        instruction = match.group(1).strip()
        code = normalized[match.end():].strip()
        return ParsedDelegationBlob(
            sublm_instruction=instruction if instruction else None,
            agent_code=code,
            has_instruction=bool(instruction),
            raw_blob=blob,
        )

    # Blob starts with a docstring but we couldn't parse it (malformed)
    raise DelegationBlobParseError(
        "Delegation blob appears to start with a docstring but the closing "
        "quotes could not be found. Ensure the instruction is properly wrapped "
        "in matching triple quotes (''' or \"\"\").",
        blob=blob,
    )


def validate_delegation_blob_format(blob: str) -> tuple[bool, Optional[str]]:
    """Validate that a delegation blob has the correct format.

    This is a convenience function for validation plugins.

    Args:
        blob: The raw delegation blob string.

    Returns:
        Tuple of (is_valid, error_message). If valid, error_message is None.
    """
    if not blob or not blob.strip():
        return False, "Delegation blob is empty"

    try:
        parsed = parse_delegation_blob(blob)
        if not parsed.is_valid:
            return False, "Delegation blob contains no executable code"
        return True, None
    except DelegationBlobParseError as e:
        return False, str(e)


def extract_instruction_and_code(blob: str) -> tuple[Optional[str], str]:
    """Extract instruction and code from a delegation blob.

    This is a convenience function that returns just the key components.

    Args:
        blob: The raw delegation blob string.

    Returns:
        Tuple of (instruction, code). Instruction may be None.
    """
    parsed = parse_delegation_blob(blob)
    return parsed.sublm_instruction, parsed.agent_code
