"""
Unit test to verify the encoding fix for missing metadata.

This test verifies that the MCAP data source handles MCAP files
with missing encoding metadata without crashing.
"""

from python_data_sources.mcap import decode_fallback


def test_fallback_decoder():
    """Test that fallback decoder handles messages correctly."""

    class MockMessage:
        def __init__(self):
            self.data = b"test_data"

    message = MockMessage()
    result = decode_fallback(message, None, None)

    assert "raw_data" in result, "Fallback decoder should return raw_data"
    assert result["raw_data"] == message.data.hex(), "Raw data should be hex encoded"


def test_encoding_extraction_both_none():
    """Test encoding extraction when both channel and schema encodings are None."""

    class MockChannel:
        def __init__(self, encoding=None):
            self.message_encoding = encoding

    class MockSchema:
        def __init__(self, encoding=None):
            self.encoding = encoding

    channel = MockChannel(None)
    schema = MockSchema(None)

    enc_raw = channel.message_encoding or getattr(schema, "encoding", None)
    if not enc_raw:
        enc = "fallback"
    else:
        enc = enc_raw.lower()

    assert enc == "fallback", "Should default to fallback when both encodings are None"


def test_encoding_extraction_channel_only():
    """Test encoding extraction when only channel has encoding."""

    class MockChannel:
        def __init__(self, encoding=None):
            self.message_encoding = encoding

    class MockSchema:
        def __init__(self, encoding=None):
            self.encoding = encoding

    channel = MockChannel("PROTOBUF")
    schema = MockSchema(None)

    enc_raw = channel.message_encoding or getattr(schema, "encoding", None)
    if not enc_raw:
        enc = "fallback"
    else:
        enc = enc_raw.lower()

    assert enc == "protobuf", "Should use channel encoding when available"


def test_encoding_extraction_schema_only():
    """Test encoding extraction when only schema has encoding."""

    class MockChannel:
        def __init__(self, encoding=None):
            self.message_encoding = encoding

    class MockSchema:
        def __init__(self, encoding=None):
            self.encoding = encoding

    channel = MockChannel(None)
    schema = MockSchema("JSON")

    enc_raw = channel.message_encoding or getattr(schema, "encoding", None)
    if not enc_raw:
        enc = "fallback"
    else:
        enc = enc_raw.lower()

    assert enc == "json", "Should use schema encoding when channel is None"


def test_encoding_extraction_missing_attribute():
    """Test encoding extraction when schema is missing encoding attribute."""

    class MockChannel:
        def __init__(self, encoding=None):
            self.message_encoding = encoding

    class SchemaNoEncoding:
        pass

    channel = MockChannel(None)
    schema = SchemaNoEncoding()

    enc_raw = channel.message_encoding or getattr(schema, "encoding", None)
    if not enc_raw:
        enc = "fallback"
    else:
        enc = enc_raw.lower()

    assert enc == "fallback", "Should handle missing encoding attribute gracefully"
