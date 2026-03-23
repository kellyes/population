from typing import cast
from functools import partial
import requests
from prefect import Flow, State, get_client
from prefect.client.schemas import FlowRun
from prefect.settings import PREFECT_API_URL
from toolz import curry
from flows.EU.flows_config import USERS

__all__ = ["teams_notifier"]

from prefect_monitoring.monito_config import (
    COLORS,
    WEBHOOK,
    WEBHOOK_MONITO,
    WEBHOOK_US,
    DEFAULT_DEVS,
    MONITO_DEVS,
)

# Teams MessageCard theme colors mapped from state names.
# COLORS from monito_config are hex strings (e.g. "#FF0000"); Teams expects
# hex WITHOUT the leading "#".
def _teams_color(state_name: str) -> str:
    color = COLORS.get(state_name, "#808080")
    return color.lstrip("#")


async def send_teams_alert(
    flow: Flow = None,
    flow_run: FlowRun = None,
    state: State = None,
    users: dict = {},
    webhook: str = "",
) -> None:
    async with get_client() as client:
        deployment = await client.read_deployment(deployment_id=flow_run.deployment_id)
        deployment_name: str = deployment.name

    webhook = (
        WEBHOOK_MONITO
        if "Monitoring_Grafana" in flow.name
        else (WEBHOOK_US if "US" in flow.name else WEBHOOK)
    )
    users = (
        MONITO_DEVS
        if "Monitoring_Grafana" in flow.name
        else (DEFAULT_DEVS if "US" in flow.name else USERS)
    )

    teams_notifier(
        flow_name=flow.name,
        state=state,
        flow_run=flow_run,
        dep_name=deployment_name,
        users=users,
        webhook=webhook,
    )


def tag_users(users: dict) -> str:
    """
    Format a mention string for Teams.

    Teams incoming webhooks do not support @mention by user ID.
    Users dict values are expected to be display names or emails.

    Args:
        users: dict mapping developer names to their Teams display name / email.

    Returns:
        Human-readable string listing the people to notify.
    """
    suffix = "Can you please have a look?"
    if not users:
        return suffix
    names = ", ".join(users.keys())
    return f"{names} — {suffix}"


def teams_message_formatter(
    state: State = None,
    flow_run: FlowRun = None,
    deployment_name: str = "",
    title: str = "",
    users: dict = None,
) -> dict:
    """
    Build a Teams MessageCard payload.

    References:
        https://learn.microsoft.com/en-us/outlook/actionable-messages/message-card-reference
    """
    # Determine the error / state message value
    if isinstance(state.result, Exception):
        message_value = repr(state.result)
    else:
        message_value = cast(str, state.message) or ""

    # Build the Prefect UI deep-link
    url = (
        f"{PREFECT_API_URL.value()}/flow-runs/flow-run/{flow_run.id}"
        .replace("api.prefect.cloud/api/accounts", "app.prefect.cloud/account")
        .replace("workspaces", "workspace")
    )

    facts = [
        {"name": "Deployment", "value": deployment_name},
        {"name": "State",      "value": state.name},
        {"name": "Flow run",   "value": flow_run.name},
    ]
    if message_value:
        facts.append({"name": "Message", "value": message_value})

    payload = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "themeColor": _teams_color(state.name),
        "summary": f"{flow_run.name} is now in a {state.name} state",
        "sections": [
            {
                "activityTitle": f"**{title} Flow** — {state.name}",
                "activitySubtitle": tag_users(users),
                "activityImage": (
                    "https://emoji.slack-edge.com/TAN3D79AL/prefect/2497370f58500a5a.png"
                ),
                "facts": facts,
                "markdown": True,
            }
        ],
        "potentialAction": [
            {
                "@type": "OpenUri",
                "name": "View in Prefect",
                "targets": [{"os": "default", "uri": url}],
            }
        ],
    }
    return payload


@curry
def teams_notifier(
    state: State,
    flow_run: FlowRun,
    proxies: dict = None,
    flow_name: str = "",
    dep_name: str = "",
    users: dict = USERS,
    webhook: str = WEBHOOK,
) -> None:
    payload = teams_message_formatter(
        state=state,
        flow_run=flow_run,
        deployment_name=dep_name,
        title=flow_name,
        users=users,
    )
    r = requests.post(webhook, json=payload, proxies=proxies)
    if not r.ok:
        raise ValueError(f"Teams notification for {flow_name!r} failed: {r.status_code} {r.text}")
