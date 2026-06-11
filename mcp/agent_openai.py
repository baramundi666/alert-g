#!/usr/bin/env python3
"""
Minimal Alert-G MCP Agent using OpenAI Responses API only.

Usage:
  python agent.py                  # interactive chat
  python agent.py -q "..."         # single query
  python agent.py --triage         # investigate active alerts
  python agent.py --list-tools     # list MCP tools exposed by Grafana
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from openai import OpenAI
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table

load_dotenv()
console = Console()

GRAFANA_URL = os.getenv("GRAFANA_URL", "https://alertg.grafana.net")
GRAFANA_TOKEN = os.getenv("GRAFANA_SERVICE_ACCOUNT_TOKEN", "")
MCP_RUNNER = os.getenv("MCP_RUNNER", "uvx").lower()  # "uvx" | "docker"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")

LOKI_DS = os.getenv("LOKI_DATASOURCE_UID", "grafanacloud-logs")
TEMPO_DS = os.getenv("TEMPO_DATASOURCE_UID", "grafanacloud-traces")

TOOL_RESULT_MAX_CHARS = int(os.getenv("TOOL_RESULT_MAX_CHARS", "4000"))
MAX_TOOL_ROUNDS = int(os.getenv("MAX_TOOL_ROUNDS", "8"))

TOOL_KEYWORDS = {"alert", "loki", "log", "tempo", "trace"}
TOOL_BLOCKLIST: set[str] = set()

def build_system_prompt(loki_labels: str = "") -> str:
    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    hour_ago_iso = (now_utc - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    labels_section = f"\nKnown Loki labels:\n{loki_labels}\n" if loki_labels else ""

    return f"""You are an SRE assistant for Alert-G on Grafana Cloud ({GRAFANA_URL}).

Current time UTC: {now_iso}
Default time range:
  startRfc3339 = "{hour_ago_iso}"
  endRfc3339   = "{now_iso}"

Use Grafana MCP tools to inspect alerts, Loki logs and Tempo traces.
Do not invent data. If a tool returns no results, say that clearly.

Loki datasourceUid: {LOKI_DS}
Tempo datasourceUid: {TEMPO_DS}
{labels_section}
Loki / Kubernetes guidance:
- LogQL should start with a stream selector, e.g. {{job=~".*kube-apiserver.*"}}.
- If you are unsure about labels, first call list_loki_label_names and then list_loki_label_values.
- For Kubernetes components such as kube-apiserver, kubelet, coredns, controller-manager or scheduler, try labels like:
  container, pod, job, component, app, app_kubernetes_io_name, k8s_container_name, k8s_pod_name, service_name, namespace.
- Never ask the user for the Loki datasource UID. Use {LOKI_DS}.
- Do not stop after checking only one label. If exact label discovery fails, search broadly for the component name.
- For error searches, look for: error, failed, failure, forbidden, unauthorized, denied, timeout, panic, exception, unavailable.
- Keep queries narrow enough to avoid huge log payloads.

Investigation style:
1. Use tools to gather evidence.
2. Group recurring errors/patterns.
3. Explain likely causes.
4. Suggest concrete next checks or remediation steps.
"""


# MCP tool helpers
async def call_tool(session: ClientSession, name: str, args: dict) -> str:
    """Execute one MCP tool call and return text for the model."""
    console.print(f"  [dim]🔧 {name}({json.dumps(args, ensure_ascii=False)})[/dim]")
    try:
        result = await session.call_tool(name, args)
        text = result.content[0].text if result.content else "{}"
    except Exception as exc:
        text = json.dumps({"error": str(exc)}, ensure_ascii=False)

    if len(text) > TOOL_RESULT_MAX_CHARS:
        text = text[:TOOL_RESULT_MAX_CHARS] + f"\n... [truncated {len(text) - TOOL_RESULT_MAX_CHARS} chars]"
    return text


def clean_schema(schema: dict | None) -> dict:
    """Keep a simple JSON Schema subset suitable for OpenAI function tools."""
    if not schema:
        return {"type": "object", "properties": {}}

    allowed = {
        "type", "properties", "required", "items", "enum", "description",
        "format", "nullable", "anyOf", "oneOf", "additionalProperties",
    }

    def clean(node):
        if isinstance(node, list):
            return [clean(x) for x in node]
        if not isinstance(node, dict):
            return node

        out = {}
        for key, value in node.items():
            if key not in allowed:
                continue
            if key == "properties" and isinstance(value, dict):
                out[key] = {prop: clean(prop_schema) for prop, prop_schema in value.items()}
            elif key in {"items", "anyOf", "oneOf", "additionalProperties"}:
                out[key] = clean(value)
            else:
                out[key] = value
        return out

    cleaned = clean(schema)
    if "type" not in cleaned and "properties" in cleaned:
        cleaned["type"] = "object"
    return cleaned or {"type": "object", "properties": {}}


def build_openai_tools(mcp_tools) -> list[dict]:
    """Expose only alert/Loki/Tempo-related MCP tools to OpenAI."""
    tools = []
    for tool in mcp_tools:
        name_lower = tool.name.lower()
        if tool.name in TOOL_BLOCKLIST:
            continue
        if not any(keyword in name_lower for keyword in TOOL_KEYWORDS):
            continue

        tools.append({
            "type": "function",
            "name": tool.name,
            "description": (tool.description or f"Grafana MCP tool: {tool.name}")[:300],
            "parameters": clean_schema(tool.inputSchema),
            "strict": False,
        })

    console.print(
        f"  [dim]OpenAI tools selected ({len(tools)}): "
        + ", ".join(t["name"] for t in tools)
        + "[/dim]"
    )
    return tools


async def fetch_loki_labels(session: ClientSession) -> str:
    """Fetch label names once to help the model write better Loki queries."""
    try:
        result = await session.call_tool("list_loki_label_names", {"datasourceUid": LOKI_DS})
        text = result.content[0].text if result.content else ""
        try:
            labels = json.loads(text)
            if isinstance(labels, list):
                return ", ".join(str(label) for label in labels[:60])
        except Exception:
            pass
        return text[:500]
    except Exception as exc:
        return f"label fetch failed: {exc}"


# OpenAI Responses API agent loop
def get_function_calls(response) -> list:
    """Return all function_call output items from a Responses API response."""
    return [item for item in (response.output or []) if getattr(item, "type", None) == "function_call"]


async def run_openai_agent(session: ClientSession, query: str, loki_labels: str = "") -> str:
    client = OpenAI(api_key=OPENAI_API_KEY)

    mcp_tools = (await session.list_tools()).tools
    tools = build_openai_tools(mcp_tools)
    if not tools:
        return "No matching MCP tools found. Try --list-tools or adjust TOOL_KEYWORDS."

    input_items = [{"role": "user", "content": query}]
    instructions = build_system_prompt(loki_labels)

    for _ in range(MAX_TOOL_ROUNDS):
        response = client.responses.create(
            model=OPENAI_MODEL,
            instructions=instructions,
            input=input_items,
            tools=tools,
            store=False,
            max_output_tokens=1600,
        )

        function_calls = get_function_calls(response)
        if not function_calls:
            return response.output_text or ""

        input_items += response.output

        for tool_call in function_calls:
            name = tool_call.name
            try:
                args = json.loads(tool_call.arguments or "{}")
            except json.JSONDecodeError:
                args = {}

            result_text = await call_tool(session, name, args)
            input_items.append({
                "type": "function_call_output",
                "call_id": tool_call.call_id,
                "output": result_text,
            })

    return "Reached max tool rounds. Try a narrower question or increase MAX_TOOL_ROUNDS."


async def auto_triage(session: ClientSession, loki_labels: str = "") -> None:
    console.print(Panel("[bold yellow]Auto-triage mode[/bold yellow] — investigating active alerts"))
    result = await run_openai_agent(
        session,
        "List all currently firing or pending alerts. For each alert, investigate the root cause using relevant Loki logs and Tempo traces. Return sections: ## Alert Summary, ## Root Cause Analysis, ## Recommended Actions.",
        loki_labels=loki_labels,
    )
    console.print(Markdown(result or "_No response_"))


async def repl(session: ClientSession, loki_labels: str = "") -> None:
    console.print("[dim]Type 'exit' to quit.[/dim]\n")
    while True:
        try:
            query = console.input("[bold cyan]You:[/bold cyan] ").strip()
        except (KeyboardInterrupt, EOFError):
            break

        if not query:
            continue
        if query.lower() in {"exit", "quit", "q"}:
            break

        console.print("[yellow]Thinking...[/yellow]")
        result = await run_openai_agent(session, query, loki_labels=loki_labels)
        console.print("[bold green]Assistant:[/bold green]")
        console.print(Markdown(result or "_No response_"))
        console.print()


# Entry point
async def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal OpenAI + Grafana MCP Agent")
    parser.add_argument("--query", "-q", help="Run a single query and exit")
    parser.add_argument("--triage", "-t", action="store_true", help="Auto-triage active alerts")
    parser.add_argument("--list-tools", "-l", action="store_true", help="List all MCP tools and exit")
    args = parser.parse_args()

    if not GRAFANA_TOKEN:
        console.print("[bold red]ERROR:[/bold red] GRAFANA_SERVICE_ACCOUNT_TOKEN is not set in .env")
        sys.exit(1)
    if not OPENAI_API_KEY:
        console.print("[bold red]ERROR:[/bold red] OPENAI_API_KEY is not set in .env")
        sys.exit(1)

    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_row("[cyan]Grafana[/cyan]", GRAFANA_URL)
    table.add_row("[cyan]MCP runner[/cyan]", MCP_RUNNER.upper())
    table.add_row("[cyan]LLM[/cyan]", f"OpenAI / {OPENAI_MODEL}")
    table.add_row("[cyan]Loki DS[/cyan]", LOKI_DS)
    table.add_row("[cyan]Tempo DS[/cyan]", TEMPO_DS)
    console.print(Panel(table, title="[bold green]Alert-G OpenAI MCP Agent[/bold green]", border_style="green"))

    mcp_env = {
        **os.environ,
        "GRAFANA_URL": GRAFANA_URL,
        "GRAFANA_SERVICE_ACCOUNT_TOKEN": GRAFANA_TOKEN,
    }

    if MCP_RUNNER == "docker":
        server_params = StdioServerParameters(
            command="docker",
            args=[
                "run", "--rm", "-i",
                "-e", f"GRAFANA_URL={GRAFANA_URL}",
                "-e", f"GRAFANA_SERVICE_ACCOUNT_TOKEN={GRAFANA_TOKEN}",
                "grafana/mcp-grafana",
                "-t", "stdio",
            ],
        )
    else:
        server_params = StdioServerParameters(
            command="uvx",
            args=["mcp-grafana"],
            env=mcp_env,
        )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = (await session.list_tools()).tools
            selected_tools = build_openai_tools(tools)
            loki_labels = await fetch_loki_labels(session)
            console.print(f"[green]✓ MCP connected — {len(selected_tools)} tools active (from {len(tools)} total)[/green]")

            if args.list_tools:
                console.print("\n[bold]All MCP tools:[/bold]")
                for tool in tools:
                    console.print(f"  [cyan]{tool.name}[/cyan] — {(tool.description or '')[:100]}")
                return

            if args.triage:
                await auto_triage(session, loki_labels=loki_labels)
            elif args.query:
                result = await run_openai_agent(session, args.query, loki_labels=loki_labels)
                console.print(Markdown(result or "_No response_"))
            else:
                await repl(session, loki_labels=loki_labels)


if __name__ == "__main__":
    asyncio.run(main())
