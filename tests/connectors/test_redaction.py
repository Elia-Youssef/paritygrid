"""Adversarial redaction tests proving raw secrets never leave the boundary.

Every fixture here quotes a real registered secret through the channel a
leak would actually use — exception text, structured detail fields,
payload fragments, header maps, and nested documents — and asserts the
raw value is absent from the result. The final gate
(:func:`assert_public_text_is_safe`) must raise when any raw value would
escape.
"""

from typing import cast

import pytest

from paritygrid.adapters.connectors import (
    ConnectorPermanentError,
    ConnectorRateLimitedError,
    ConnectorRetryableError,
    build_public_detail,
    redact_exception,
    redact_headers,
    redact_payload_fragment,
    redact_text,
    redact_value,
)
from paritygrid.adapters.connectors.redaction import (
    REDACTION_PLACEHOLDER,
    RedactionError,
    SecretMaterial,
    assert_public_text_is_safe,
)

pytestmark = pytest.mark.anyio

_SECRET = "tok_A7b9C2x4-secret-value"
_SECOND_SECRET = "hunter2 password"
_SECRETS = SecretMaterial((_SECRET, _SECOND_SECRET))


def assert_no_secret(text: str) -> None:
    """Fail when any registered raw secret survives in public text."""
    assert _SECRET not in text, f"raw secret leaked: {text!r}"
    assert _SECOND_SECRET not in text, f"raw second secret leaked: {text!r}"


class TestRedactText:
    def test_registered_values_are_replaced_everywhere(self) -> None:
        leak = f"request failed after Authorization: Bearer {_SECRET} and password={_SECOND_SECRET}"
        redacted = redact_text(leak, _SECRETS)
        assert _SECRET not in redacted
        assert _SECOND_SECRET not in redacted
        assert REDACTION_PLACEHOLDER in redacted

    def test_bearer_pattern_masks_unregistered_tokens(self) -> None:
        redacted = redact_text("Authorization: Bearer tok_not_registered_at_all")
        assert "tok_not_registered_at_all" not in redacted
        assert REDACTION_PLACEHOLDER in redacted

    def test_assignment_patterns_mask_unregistered_secrets(self) -> None:
        redacted = redact_text("upstream said api_key=sk_live_99887766 and token: t_555")
        assert "sk_live_99887766" not in redacted
        assert "t_555" not in redacted

    def test_credential_urls_are_masked(self) -> None:
        redacted = redact_text("connecting to http://admin:hunter2@localhost:9000/api")
        assert "hunter2" not in redacted
        assert redacted == f"connecting to http://{REDACTION_PLACEHOLDER}@localhost:9000/api"

    def test_marker_shapes_are_masked_without_registration(self) -> None:
        redacted = redact_text("response mentioned token: abc123def end")
        assert "abc123def" not in redacted

    def test_longest_secret_wins_over_contained_secret(self) -> None:
        secrets = SecretMaterial(("short-secret", "short-secret-with-suffix"))
        redacted = redact_text("value short-secret-with-suffix here", secrets)
        assert "short-secret" not in redacted
        assert "with-suffix" not in redacted

    def test_output_is_length_bounded(self) -> None:
        redacted = redact_text("x" * 10_000)
        assert len(redacted) <= 512

    def test_unicode_and_control_content_is_preserved_bounded(self) -> None:
        redacted = redact_text("青β " * 200)
        assert len(redacted) <= 512


class TestRedactHeaders:
    def test_sensitive_headers_are_replaced_wholesale(self) -> None:
        redacted = redact_headers(
            {"Authorization": f"Bearer {_SECRET}", "X-Api-Key": "sk_123", "Retry-After": "3"},
            _SECRETS,
        )
        assert redacted["Authorization"] == REDACTION_PLACEHOLDER
        assert redacted["X-Api-Key"] == REDACTION_PLACEHOLDER
        assert redacted["Retry-After"] == "3"

    def test_header_values_quoting_secrets_are_redacted(self) -> None:
        redacted = redact_headers({"X-Trace": f"saw {_SECRET} mid-flight"}, _SECRETS)
        assert _SECRET not in redacted["X-Trace"]


class TestRedactValue:
    def test_sensitive_keys_are_replaced_recursively(self) -> None:
        document = {
            "outer": {
                "password": _SECOND_SECRET,
                "nested": [{"token": _SECRET}, {"safe": "kept"}],
            }
        }
        redacted_document = cast("dict[str, object]", redact_value(document, _SECRETS))
        assert_no_secret(str(redacted_document))
        outer = cast("dict[str, object]", redacted_document["outer"])
        assert outer["password"] == REDACTION_PLACEHOLDER  # type: ignore[index,union-attr]
        nested = outer["nested"]  # type: ignore[index,union-attr]
        assert nested[0]["token"] == REDACTION_PLACEHOLDER  # type: ignore[index]
        assert nested[1]["safe"] == "kept"  # type: ignore[index]

    def test_non_string_scalars_pass_through(self) -> None:
        redacted = redact_value({"count": 5, "flag": True, "none": None})
        assert redacted == {"count": 5, "flag": True, "none": None}


class TestFragments:
    def test_fragment_is_bounded_and_sanitized(self) -> None:
        fragment = redact_payload_fragment(f'{{"error":"{_SECRET}"}}' + "x" * 500, _SECRETS)
        assert _SECRET not in fragment
        assert len(fragment) <= 131

    def test_control_characters_are_replaced(self) -> None:
        fragment = redact_payload_fragment("line\x00with\x1bcontrol")
        assert "\x00" not in fragment
        assert "\x1b" not in fragment

    def test_undecodable_bytes_are_replaced_not_raised(self) -> None:
        fragment = redact_payload_fragment(b"\xff\xfe broken")
        assert isinstance(fragment, str)


class TestExceptions:
    def test_exception_chain_is_flattened_and_redacted(self) -> None:
        try:
            try:
                raise ValueError(f"inner quoted {_SECRET}")
            except ValueError as inner:
                raise RuntimeError("outer failure") from inner
        except RuntimeError as error:
            flattened = redact_exception(error, _SECRETS)
        assert _SECRET not in flattened
        assert "RuntimeError" in flattened
        assert "ValueError" in flattened

    def test_cyclic_chains_terminate(self) -> None:
        try:
            raise ValueError("first")
        except ValueError as first:
            try:
                raise ValueError("second") from first
            except ValueError as second:
                first.__cause__ = second  # type: ignore[assignment]
                flattened = redact_exception(second)
        assert "second" in flattened

    def test_exception_output_is_bounded(self) -> None:
        error = ValueError("x" * 5_000)
        assert len(redact_exception(error)) <= 512


class TestPublicDetailComposer:
    def test_detail_composes_redacted_fields(self) -> None:
        detail = build_public_detail(
            f"upstream failed near {_SECRET}",
            fragment=f"body mentions {_SECRET}",
            details={"url": f"http://user:{_SECOND_SECRET}@host", "status": 503},
            secrets=_SECRETS,
        )
        assert_no_secret(detail)
        assert "503" in detail

    def test_detail_requires_a_summary(self) -> None:
        with pytest.raises(RedactionError):
            build_public_detail("")


class TestFailClosedGate:
    def test_gate_raises_when_a_secret_would_escape(self) -> None:
        with pytest.raises(RedactionError):
            assert_public_text_is_safe(f"quote {_SECRET}", _SECRETS)

    def test_gate_passes_clean_text(self) -> None:
        assert_public_text_is_safe("clean summary with status 503", _SECRETS)

    def test_gate_rejects_non_text(self) -> None:
        with pytest.raises(RedactionError):
            assert_public_text_is_safe(42)  # type: ignore[arg-type]


class TestConnectorErrorRedaction:
    def test_error_message_quoting_a_secret_fails_at_construction(self) -> None:
        with pytest.raises(RedactionError):
            ConnectorRetryableError(f"connection lost using {_SECRET}", secrets=_SECRETS)

    def test_error_detail_quoting_a_secret_fails_at_construction(self) -> None:
        with pytest.raises(RedactionError):
            ConnectorPermanentError(
                "visible summary",
                detail=f"fragment includes {_SECRET}",
                secrets=_SECRETS,
            )

    def test_constructed_errors_never_carry_secrets(self) -> None:
        error = ConnectorRateLimitedError("throttled by upstream", secrets=_SECRETS)
        assert _SECRET not in str(error)
        assert _SECRET not in error.detail
        assert _SECRET not in repr(error)

    def test_public_details_mask_unregistered_secret_shapes(self) -> None:
        # The marker layer must redact shapes even when the exact value
        # was never registered with the connector.
        detail = build_public_detail(
            "upstream rejected the call", details={"hint": "bearer tok_unregistered_value"}
        )
        assert "tok_unregistered_value" not in detail
        assert REDACTION_PLACEHOLDER in detail


class TestSecretMaterial:
    def test_combine_preserves_order_and_deduplicates_values(self) -> None:
        combined = SecretMaterial.combine(
            SecretMaterial((_SECRET,)),
            SecretMaterial((_SECOND_SECRET, _SECRET)),
        )
        assert combined.fingerprints() == _SECRETS.fingerprints()

    def test_repr_and_str_never_expose_values(self) -> None:
        assert _SECRET not in repr(_SECRETS)
        assert _SECRET not in str(_SECRETS)
        assert _SECOND_SECRET not in repr(_SECRETS)

    def test_fingerprints_are_stable_and_distinct(self) -> None:
        again = SecretMaterial((_SECRET, _SECOND_SECRET))
        assert _SECRETS.fingerprints() == again.fingerprints()
        assert len(set(_SECRETS.fingerprints())) == 2

    def test_invalid_values_are_rejected(self) -> None:
        with pytest.raises(RedactionError):
            SecretMaterial(("",))  # type: ignore[arg-type]
        with pytest.raises(RedactionError):
            SecretMaterial(("x" * 257,))
        with pytest.raises(RedactionError):
            SecretMaterial(("dup", "dup"))

    def test_empty_material_is_allowed(self) -> None:
        empty = SecretMaterial.empty()
        assert len(empty) == 0
        assert redact_text("plain text", empty) == "plain text"
