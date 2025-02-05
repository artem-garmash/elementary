from datetime import datetime
from pathlib import Path

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from tests.unit.alerts.alert_messages.test_alert_utils import (
    build_base_model_alert_model,
    get_alert_message_body,
    get_mock_report_link,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.mark.parametrize(
    "status,has_link,has_message,has_tags,has_owners,has_path,has_suppression_interval",
    [
        ("fail", True, True, True, True, True, True),
        ("fail", False, False, False, False, False, False),
        ("warn", True, False, True, False, True, False),
        ("warn", False, True, False, True, False, True),
        ("error", True, True, False, True, True, False),
        ("error", False, True, True, False, False, True),
        (None, True, False, True, False, False, True),
        (None, False, False, False, True, True, False),
        ("fail", True, False, True, True, False, True),
        ("warn", False, True, True, False, True, False),
    ],
)
def test_get_snapshot_alert_message_body(
    monkeypatch,
    status: str,
    has_link: bool,
    has_message: bool,
    has_tags: bool,
    has_owners: bool,
    has_path: bool,
    has_suppression_interval: bool,
):
    path = "models/test_snapshot.sql" if has_path else ""
    snapshot_alert_model = build_base_model_alert_model(
        status=status,
        tags=["tag1", "tag2"] if has_tags else None,
        owners=["owner1", "owner2"] if has_owners else None,
        path=path,
        materialization="snapshot",  # Always snapshot for this test
        full_refresh=False,
        detected_at=datetime(2025, 2, 3, 13, 21, 7),
        alias="test_snapshot",
        message=("Test message" if has_message else None),
        suppression_interval=24 if has_suppression_interval else None,
    )

    monkeypatch.setattr(
        snapshot_alert_model, "get_report_link", lambda: get_mock_report_link(has_link)
    )

    message_body = get_alert_message_body(snapshot_alert_model)
    adaptive_card_filename = (
        f"adaptive_card_snapshot_alert"
        f"_status-{status}"
        f"_link-{has_link}"
        f"_message-{has_message}"
        f"_tags-{has_tags}"
        f"_owners-{has_owners}"
        f"_path-{has_path}"
        f"_suppression-{has_suppression_interval}.json"
    )
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)
