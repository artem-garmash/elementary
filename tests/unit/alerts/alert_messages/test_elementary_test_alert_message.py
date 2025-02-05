from pathlib import Path

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from tests.unit.alerts.alert_messages.test_alert_utils import (
    build_base_test_alert_model,
    get_alert_message_body,
    get_mock_report_link,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def build_elementary_test_alert_model(*args, **kwargs):
    kwargs.pop("test_type", None)  # Remove test_type if present
    test_type = (
        "anomaly_detection" if kwargs.pop("has_anomaly", False) else "schema_change"
    )
    return build_base_test_alert_model(*args, test_type=test_type, **kwargs)


@pytest.mark.parametrize(
    "status,has_link,has_description,has_tags,has_owners,has_table,has_error,has_sample,has_anomaly",
    [
        (None, False, False, False, False, False, False, False, False),
        ("fail", True, True, True, True, True, True, True, True),
        ("fail", False, False, False, False, False, False, False, False),
        ("warn", True, False, True, False, True, False, True, False),
        ("warn", False, True, False, True, False, True, False, True),
        ("error", True, True, False, False, False, True, False, True),
        ("error", False, True, True, False, True, False, True, False),
        (None, True, False, True, False, True, False, True, True),
        (None, False, True, False, True, False, True, False, True),
    ],
)
def test_get_elementary_test_alert_message_body(
    monkeypatch,
    status: str,
    has_link: bool,
    has_description: bool,
    has_tags: bool,
    has_owners: bool,
    has_table: bool,
    has_error: bool,
    has_sample: bool,
    has_anomaly: bool,
):
    test_alert_model = build_elementary_test_alert_model(
        status=status,
        table_name=None if not has_table else "test_table",
        tags=["tag1", "tag2"] if has_tags else None,
        owners=["owner1", "owner2"] if has_owners else None,
        test_description="Test description" if has_description else None,
        error_message="Test error message" if has_error else None,
        test_rows_sample=(
            {"column1": "value1", "column2": "value2"} if has_sample else None
        ),
        test_results_query="SELECT * FROM test" if has_sample else None,
        test_params={"param1": "value1"} if has_sample else None,
        has_anomaly=has_anomaly,
        other={"anomalous_value": 42} if has_anomaly else None,
    )

    monkeypatch.setattr(
        test_alert_model, "get_report_link", lambda: get_mock_report_link(has_link)
    )

    message_body = get_alert_message_body(test_alert_model)
    adaptive_card_filename = (
        f"adaptive_card_elementary_test_alert"
        f"_status-{status}"
        f"_link-{has_link}"
        f"_description-{has_description}"
        f"_tags-{has_tags}"
        f"_owners-{has_owners}"
        f"_table-{has_table}"
        f"_error-{has_error}"
        f"_sample-{has_sample}"
        f"_anomaly-{has_anomaly}.json"
    )
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)
