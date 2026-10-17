"""Classify a Terraform plan as none / low / high risk.

    terraform show -json plan.tfplan > plan.json
    python scripts/ci/tf_plan_risk.py plan.json

Prints a Markdown summary and, inside GitHub Actions, writes `risk=<level>`
to $GITHUB_OUTPUT. terraform-apply.yml applies `low` plans automatically
and sends `high` plans to an environment with required reviewers.

The rules are an allowlist: a change is low risk only if it is known to be
additive or cosmetic. Anything else (deletes, replacements, IAM, identity,
unknown resource types or attributes) is high risk.
"""

import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any

# Creating these adds capacity without touching existing data or access.
LOW_RISK_CREATE_TYPES = {
    "google_bigquery_dataset",
    "google_bigquery_table",
    "google_project_service",
}

# In-place updates that only touch these attributes are cosmetic.
# terraform_labels / effective_labels follow `labels` and provider
# default_labels.
LOW_RISK_UPDATE_ATTRIBUTES = {
    "labels",
    "terraform_labels",
    "effective_labels",
    "description",
    "friendly_name",
    "display_name",
}

# Who can do what: always reviewed, whatever the action.
HIGH_RISK_TYPE_MARKERS = (
    "_iam_",
    "google_service_account",
    "google_iam_workload_identity_pool",
    "google_kms_",
    "google_secret_manager_",
    "google_organization_",
    "google_folder_",
)


@dataclass
class Assessment:
    risk: str = "none"
    reasons: list[str] = field(default_factory=list)
    changes: list[tuple[str, str]] = field(default_factory=list)


def changed_attributes(before: Any, after: Any) -> set[str]:
    """Top-level attributes that differ between before and after.

    Attributes unknown until apply (after_unknown) are treated as changed
    by the caller.
    """

    before = before or {}
    after = after or {}

    return {
        key
        for key in before.keys() | after.keys()
        if before.get(key) != after.get(key)
    }


def classify_change(resource_change: dict) -> tuple[str, str | None]:
    """(risk, reason) for one entry of the plan's resource_changes."""

    address = resource_change["address"]
    resource_type = resource_change["type"]
    change = resource_change["change"]
    actions = change["actions"]

    if actions in (["no-op"], ["read"]):
        # Importing an existing resource only writes state
        return "none", None

    if "delete" in actions:
        verb = "replaces" if "create" in actions else "deletes"
        return "high", f"{address}: {verb} a resource"

    if any(marker in resource_type for marker in HIGH_RISK_TYPE_MARKERS):
        return "high", f"{address}: changes identity or access ({resource_type})"

    if actions == ["create"]:
        if resource_type in LOW_RISK_CREATE_TYPES:
            return "low", None
        return "high", f"{address}: creates {resource_type}, not on the low-risk list"

    if actions == ["update"]:
        attributes = changed_attributes(change.get("before"), change.get("after"))
        attributes |= set(change.get("after_unknown") or {})
        risky = sorted(attributes - LOW_RISK_UPDATE_ATTRIBUTES)
        if risky:
            return "high", f"{address}: updates {', '.join(risky)}"
        return "low", None

    # e.g. ["forget"] from a `removed` block: Terraform stops managing it
    return "high", f"{address}: unrecognised actions {actions}"


def assess(plan: dict) -> Assessment:
    assessment = Assessment()
    levels = {"none": 0, "low": 1, "high": 2}

    for resource_change in plan.get("resource_changes", []):
        risk, reason = classify_change(resource_change)

        if risk != "none":
            assessment.changes.append(
                (resource_change["address"], "/".join(resource_change["change"]["actions"]))
            )
        if reason:
            assessment.reasons.append(reason)
        if levels[risk] > levels[assessment.risk]:
            assessment.risk = risk

    # Output-only plans still need an apply to update state
    if assessment.risk == "none" and plan.get("output_changes"):
        if any(
            change["actions"] != ["no-op"]
            for change in plan["output_changes"].values()
        ):
            assessment.risk = "low"

    return assessment


def render_markdown(assessment: Assessment) -> str:
    lines = [f"**Risk: `{assessment.risk}`**", ""]

    if assessment.risk == "none":
        lines.append("No infrastructure changes.")
    elif assessment.risk == "low":
        lines.append("Additive or cosmetic changes only: applied automatically after merge.")
    else:
        lines.append("Needs approval in the `infra-approval` environment after merge:")
        lines.append("")
        lines.extend(f"- {reason}" for reason in assessment.reasons)

    if assessment.changes:
        lines += ["", "| Resource | Actions |", "|---|---|"]
        lines += [f"| `{address}` | {actions} |" for address, actions in assessment.changes]

    return "\n".join(lines)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2

    with open(argv[1]) as plan_file:
        assessment = assess(json.load(plan_file))

    print(render_markdown(assessment))

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as output:
            output.write(f"risk={assessment.risk}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
