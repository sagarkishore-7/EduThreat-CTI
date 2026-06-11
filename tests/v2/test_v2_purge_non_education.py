"""The keyword-junk purge must dry-run by default and cascade-delete on confirm."""

from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.services.data_quality import V2DataQualityService


def test_purge_dry_run_counts_without_deleting():
    svc = V2DataQualityService()
    session = Mock()
    ids_result = Mock()
    ids_result.scalars.return_value.all.return_value = [uuid4(), uuid4()]
    count_result = Mock()
    count_result.scalar_one.return_value = 100
    session.execute.side_effect = [ids_result, count_result]

    report = svc.purge_non_education_incidents(session, confirm=False)

    assert report["junk_candidates"] == 2
    assert report["source_incidents_before"] == 100
    assert report["deleted"] == 0
    assert report["confirmed"] is False
    # only the two SELECTs ran — no DELETE, no flush
    assert session.execute.call_count == 2
    session.flush.assert_not_called()


def test_purge_confirm_deletes_and_reports_after_count():
    svc = V2DataQualityService()
    session = Mock()
    junk = [uuid4(), uuid4(), uuid4()]
    ids_result = Mock()
    ids_result.scalars.return_value.all.return_value = junk
    before = Mock()
    before.scalar_one.return_value = 100
    delete_result = Mock()
    delete_result.rowcount = 3
    after = Mock()
    after.scalar_one.return_value = 97
    session.execute.side_effect = [ids_result, before, delete_result, after]

    report = svc.purge_non_education_incidents(session, confirm=True)

    assert report["deleted"] == 3
    assert report["source_incidents_after"] == 97
    session.flush.assert_called_once()


def test_purge_confirm_with_no_junk_is_noop():
    svc = V2DataQualityService()
    session = Mock()
    ids_result = Mock()
    ids_result.scalars.return_value.all.return_value = []
    count_result = Mock()
    count_result.scalar_one.return_value = 100
    session.execute.side_effect = [ids_result, count_result]

    report = svc.purge_non_education_incidents(session, confirm=True)

    assert report["junk_candidates"] == 0
    assert report["deleted"] == 0
    session.flush.assert_not_called()
