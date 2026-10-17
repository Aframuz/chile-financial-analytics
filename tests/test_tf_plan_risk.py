import json

import pytest

from scripts.ci.tf_plan_risk import assess, main


def resource_change(
    address,
    actions,
    before=None,
    after=None,
    after_unknown=None,
    importing=None,
):
    resource_type = address.split(".")[0]
    change = {
        "actions": actions,
        "before": before,
        "after": after,
        "after_unknown": after_unknown or {},
    }
    if importing is not None:
        change["importing"] = importing

    return {"address": address, "type": resource_type, "change": change}


def plan(*changes, output_changes=None):
    return {"resource_changes": list(changes), "output_changes": output_changes or {}}


def test_empty_plan_is_none():
    assert assess(plan()).risk == "none"


def test_import_without_changes_is_none():
    change = resource_change(
        "google_bigquery_dataset.analytics",
        ["no-op"],
        before={"dataset_id": "analytics_dev"},
        after={"dataset_id": "analytics_dev"},
        importing={"id": "projects/p/datasets/analytics_dev"},
    )

    assert assess(plan(change)).risk == "none"


def test_creating_a_dataset_is_low():
    change = resource_change(
        "google_bigquery_dataset.staging",
        ["create"],
        after={"dataset_id": "staging"},
    )

    assert assess(plan(change)).risk == "low"


def test_label_and_description_updates_are_low():
    change = resource_change(
        "google_bigquery_dataset.analytics",
        ["update"],
        before={"labels": {"layer": "a"}, "description": "old", "location": "x"},
        after={"labels": {"layer": "b"}, "description": "new", "location": "x"},
    )

    assert assess(plan(change)).risk == "low"


@pytest.mark.parametrize(
    "actions, reason",
    [
        (["delete"], "deletes"),
        (["delete", "create"], "replaces"),
        (["create", "delete"], "replaces"),
    ],
)
def test_deletes_and_replacements_are_high(actions, reason):
    change = resource_change("google_bigquery_dataset.analytics", actions)

    assessment = assess(plan(change))

    assert assessment.risk == "high"
    assert reason in assessment.reasons[0]


def test_updating_data_affecting_attributes_is_high():
    # Shortening table expiration deletes data
    change = resource_change(
        "google_bigquery_dataset.analytics",
        ["update"],
        before={"default_table_expiration_ms": None},
        after={"default_table_expiration_ms": 3600000},
    )

    assessment = assess(plan(change))

    assert assessment.risk == "high"
    assert "default_table_expiration_ms" in assessment.reasons[0]


def test_attributes_unknown_until_apply_count_as_changed():
    change = resource_change(
        "google_bigquery_dataset.analytics",
        ["update"],
        before={"labels": {}},
        after={"labels": {}},
        after_unknown={"access": True},
    )

    assert assess(plan(change)).risk == "high"


@pytest.mark.parametrize(
    "address",
    [
        "google_project_iam_member.bigquery_job_user",
        "google_bigquery_dataset_iam_member.dbt_viewer",
        "google_service_account.ci",
        "google_iam_workload_identity_pool_provider.github",
    ],
)
def test_identity_and_access_changes_are_high_even_when_created(address):
    assert assess(plan(resource_change(address, ["create"]))).risk == "high"


def test_creating_unlisted_resource_type_is_high():
    change = resource_change("google_storage_bucket.exports", ["create"])

    assert assess(plan(change)).risk == "high"


def test_forget_from_removed_block_is_high():
    change = resource_change("google_bigquery_dataset.legacy", ["forget"])

    assert assess(plan(change)).risk == "high"


def test_one_high_change_makes_the_plan_high():
    low = resource_change("google_bigquery_dataset.staging", ["create"])
    high = resource_change("google_bigquery_dataset.analytics", ["delete"])

    assessment = assess(plan(low, high))

    assert assessment.risk == "high"
    assert len(assessment.changes) == 2


def test_output_only_changes_are_low():
    assessment = assess(plan(output_changes={"x": {"actions": ["create"]}}))

    assert assessment.risk == "low"


def test_main_writes_github_output(tmp_path, monkeypatch, capsys):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(plan(resource_change("google_bigquery_dataset.d", ["delete"])))
    )
    github_output = tmp_path / "github_output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(github_output))

    assert main(["tf_plan_risk.py", str(plan_path)]) == 0

    assert github_output.read_text() == "risk=high\n"
    assert "Risk: `high`" in capsys.readouterr().out
