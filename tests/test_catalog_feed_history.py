"""The catalog's reliability, continuous-coverage and validation-report
endpoints."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st

from aiomobilitydatabase.client import (
    MobilityDatabaseClient,
    _one,
    _Repeated,
    encode_params,
)
from aiomobilitydatabase.exceptions import MobilityDatabaseConnectionError
from aiomobilitydatabase.models import (
    CoverageWindowSource,
    CriterionStatus,
    NoticeSeverity,
    SealCriterion,
    SealStatus,
    ServiceDateWindow,
)

from tests.fixtures import (
    CONTINUOUS_COVERAGE_RESPONSE,
    RELIABILITY_REPORT,
    TOKEN_RESPONSE,
    VALIDATION_REPORTS_RESPONSE,
)
from tests.mock_server import MockApi, RecordedRequest


def _last(mock_api: MockApi, path: str) -> RecordedRequest:
    return next(r for r in mock_api.requests if r.path == path)


async def test_get_gtfs_feed_reliability(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    mock_api.get("/v1/gtfs_feeds/mdb-123/reliability", payload=RELIABILITY_REPORT)
    report = await client.get_gtfs_feed_reliability("mdb-123")
    assert report.feed_id == "mdb-123"
    assert report.seal_status is SealStatus.NOT_GRANTED
    assert report.has_seal is False
    assert report.probation_ends_at == datetime(2027, 2, 1, tzinfo=UTC)
    assert [c.criterion for c in report.criteria] == list(SealCriterion)
    available = report.criteria[2]
    assert available.status is CriterionStatus.FAIL
    assert available.first_failure_at == datetime(2026, 7, 20, tzinfo=UTC)
    assert _last(mock_api, "/v1/gtfs_feeds/mdb-123/reliability").query == {}


async def test_get_gtfs_feed_continuous_coverage(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    path = "/v1/gtfs_feeds/mdb-123/continuous_coverage"
    mock_api.get(path, payload=CONTINUOUS_COVERAGE_RESPONSE)
    coverage = await client.get_gtfs_feed_continuous_coverage(
        "mdb-123",
        downloaded_after=datetime(2026, 1, 1, tzinfo=UTC),
        limit=2,
    )
    latest, older = coverage.items
    assert latest.is_latest and not older.is_latest
    assert latest.coverage_window == ServiceDateWindow(
        date(2026, 9, 1), date(2027, 3, 31), 212
    )
    assert latest.coverage_window_source is CoverageWindowSource.FEED_INFO
    assert older.feed_info_window is None and older.feed_info_matches is None
    assert [f.present for f in latest.files] == [True, False, True]
    assert coverage.latest_state is not None
    assert coverage.latest_state.newer == latest
    assert coverage.latest_failure is None
    assert _last(mock_api, path).query == {
        "downloaded_after": "2026-01-01T00:00:00+00:00",
        "limit": "2",
    }


async def test_validation_report_severities_are_sent_repeated(
    mock_api: MockApi, client: MobilityDatabaseClient
) -> None:
    """The spec's severity filter takes the parameter repeated ("Repeat the
    parameter for more than one"), not comma-joined as status is."""
    mock_api.post("/v1/tokens", payload=TOKEN_RESPONSE)
    path = "/v1/gtfs_feeds/mdb-123/validation_reports"
    mock_api.get(path, payload=VALIDATION_REPORTS_RESPONSE)
    reports = await client.get_gtfs_feed_validation_reports(
        "mdb-123",
        min_errors=1,
        severities=[NoticeSeverity.ERROR, NoticeSeverity.WARNING],
    )
    assert reports.latest == reports.items[0]
    assert [n.severity for n in reports.items[0].notices] == [
        NoticeSeverity.ERROR,
        NoticeSeverity.WARNING,
    ]
    assert _last(mock_api, path).query_items == [
        ("min_errors", "1"),
        ("severity", "ERROR"),
        ("severity", "WARNING"),
    ]


@given(
    severities=st.lists(st.sampled_from(list(NoticeSeverity)), max_size=5),
    statuses=st.lists(st.sampled_from(["a", "b"]), max_size=3),
)
def test_a_repeated_filter_encodes_item_by_item(
    severities: list[NoticeSeverity], statuses: list[str]
) -> None:
    """A _Repeated filter keeps each value apart (for the request to repeat
    the parameter), in order; a plain list still joins with commas; either,
    empty, is no filter."""
    encoded = encode_params({"severity": _Repeated(severities), "status": statuses})
    assert encoded.get("severity") == (
        tuple(s.value for s in severities) if severities else None
    )
    assert encoded.get("status") == (",".join(statuses) if statuses else None)


@given(
    text=st.one_of(
        st.dates().map(date.isoformat),
        st.dates().map(lambda d: d.strftime("%Y%m%d")),
        st.from_regex(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", fullmatch=True),
        st.text(max_size=12),
    )
)
def test_a_service_date_is_a_full_date_or_an_invalid_payload(text: str) -> None:
    """``start``/``end`` are RFC 3339 full-dates: anything else -- the
    basic format fromisoformat would take, a date no calendar has -- is an
    invalid payload, never a guess."""
    try:
        expected: date | None = date.fromisoformat(text)
    except ValueError:
        expected = None
    if expected is not None and (len(text) != 10 or text[4] != "-"):
        expected = None
    payload = {"start": text, "end": "2027-01-01"}
    if expected is None:
        with pytest.raises(MobilityDatabaseConnectionError):
            _one(ServiceDateWindow, payload)
    else:
        assert _one(ServiceDateWindow, payload).start == expected
