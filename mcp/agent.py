#!/usr/bin/env python3
"""
Alert-G MCP Agent
Connects Grafana Cloud (Loki logs + Tempo traces + Prometheus + Alerts)
to a free LLM (Groq cloud or Ollama local) via the official Grafana MCP server.

Usage:
  python agent.py                  # interactive chat
  python agent.py --query "..."    # single query
  python agent.py --triage         # auto-triage active alerts
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table

load_dotenv()

console = Console()

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
GRAFANA_URL   = os.getenv("GRAFANA_URL", "https://alertg.grafana.net")
GRAFANA_TOKEN = os.getenv("GRAFANA_SERVICE_ACCOUNT_TOKEN", "") or os.getenv("GRAFANA_API_KEY", "")
MCP_RUNNER    = os.getenv("MCP_RUNNER", "uvx").lower()       # "uvx" | "docker"
LLM_PROVIDER  = os.getenv("LLM_PROVIDER", "groq").lower()   # "groq" | "ollama"
GROQ_API_KEY  = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL    = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
OLLAMA_URL    = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL  = os.getenv("OLLAMA_MODEL", "llama3.2")

# Datasource UIDs from your Grafana Cloud instance
LOKI_DS   = os.getenv("LOKI_DATASOURCE_UID",   "grafanacloud-logs")
TEMPO_DS  = os.getenv("TEMPO_DATASOURCE_UID",  "grafanacloud-traces")
PROM_DS   = os.getenv("PROM_DATASOURCE_UID",   "grafanacloud-prom")

# ─────────────────────────────────────────────
# System prompt — built fresh per query so the timestamp is always current
# ─────────────────────────────────────────────
def build_system_prompt(loki_labels: str = "") -> str:
    now_utc  = datetime.now(timezone.utc)
    now_iso  = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    hour_ago = now_utc.replace(hour=now_utc.hour - 1) if now_utc.hour > 0 else now_utc
    hour_ago_iso = hour_ago.strftime("%Y-%m-%dT%H:%M:%SZ")

    labels_section = (
        f"\nKnown Loki labels in this instance:\n{loki_labels}\n"
        if loki_labels else ""
    )

    return f"""You are an SRE assistant for Alert-G on Grafana Cloud ({GRAFANA_URL}).
Monitored app: Online Boutique (Kubernetes microservices: frontend, cartservice, checkoutservice, paymentservice, shippingservice, productcatalogservice, currencyservice, emailservice, recommendationservice, adservice).

CURRENT TIME (UTC): {now_iso}
Use this for all time ranges. A good default window is the last hour:
  startRfc3339 = "{hour_ago_iso}"
  endRfc3339   = "{now_iso}"
NEVER use hardcoded historical dates like 2024-01-01. Always use the dynamic variables above.

{labels_section}
You have access to three categories of tools — alerts, logs (Loki), and traces (Tempo).
Do NOT call Prometheus or dashboard tools; they are not available.

── Loki (logs) ──────────────────────────────────────────────────────
datasourceUid: {LOKI_DS}
LogQL MUST start with a stream selector in curly braces. 

The application logs structured JSON. Note that severity is logged as `severity` (e.g., "error", "info"), NOT `level`. Keys may contain dots (e.g., `http.req.path`).

Effective LogQL patterns for this app:
  # Target a specific service and error text
  {{service_name="frontend"}} |= "error" |= "/cart/checkout"
  
  # Parse JSON to filter by specific endpoint paths or attributes
  {{app="frontend"}} | json | severity="error"
  {{service_name="checkoutservice"}} | json | error =~ ".*failed to charge card.*"

BEFORE writing a LogQL query with labels you're unsure about, call list_loki_label_names first to discover real label names, then call list_loki_label_values to get valid values for a given label.

── Tempo (traces) ───────────────────────────────────────────────────
datasourceUid: {TEMPO_DS}
Use tempo_traceql-search with a TraceQL expression. When logs reveal a specific request ID (`http.req.id`) or session ID, use TraceQL to map the entire downstream distributed trace across microservices.

Examples:
  {{span.http.status_code >= 500}}
  {{resource.service.name="paymentservice" && duration > 500ms}}
  # Searching by custom log attributes passed into the trace context
  {{span.http.req.id="9f0a2791-0314-4b0b-9c2d-5bd1ec4549ad"}}

── Investigation playbook ───────────────────────────────────────────
1. List active alerts to identify which service is currently firing or degraded.
2. If unsure of Loki stream labels, verify them via list_loki_label_names and list_loki_label_values.
3. Query Loki logs for the target service. Look for structured fields like `severity="error"`, specific endpoint paths (like `/cart/checkout`), and downstream RPC error messages.
4. Extract unique identifiers from the log (such as `http.req.id` or `session`) and use Tempo TraceQL to locate the exact failed span and trace the root cause down the dependency chain (e.g., frontend -> checkoutservice -> paymentservice).
5. Summarise using clear Markdown:
   - **Root Cause**: Specific error mechanical failure or logic rejection (e.g., Card type Visa Electron rejected by paymentservice).
   - **Affected Services**: Entrypoint and blast radius.
   - **Recommended Actions**: Upstream validation fixes, configuration changes, or code alterations.
"""

# ─────────────────────────────────────────────
# Tool execution helper (shared)
# ─────────────────────────────────────────────
async def call_tool(session: ClientSession, name: str, args: dict) -> str:
    console.print(f"  [dim]🔧 {name}({json.dumps(args, ensure_ascii=False)})[/dim]")
    try:
        result = await session.call_tool(name, args)
        text = result.content[0].text if result.content else "{}"
    except Exception as exc:
        return f'{{"error": "{exc}"}}'

    # Truncate to prevent context explosion on large Loki/Prometheus responses
    if len(text) > TOOL_RESULT_MAX_CHARS:
        text = text[:TOOL_RESULT_MAX_CHARS] + f"\n... [truncated {len(text) - TOOL_RESULT_MAX_CHARS} chars]"
    return text


# ─────────────────────────────────────────────
# Tool filtering — keeps token count inside Groq free-tier limits.
# The full server has 59 tools; we only need ~12 for SRE work.
# ─────────────────────────────────────────────

# Keywords used to select tools from whatever the MCP server actually exposes.
# This is robust to name changes (e.g. "query_loki_logs" vs "queryLokiLogs").
TOOL_KEYWORDS = {
    "alert", "loki", "log", "tempo", "trace",
}

# Hard-block tools that return huge payloads we don't need
TOOL_BLOCKLIST: set[str] = set()

# Maximum characters to keep from a single tool result.
# Loki/Prometheus can return megabytes; this caps each at ~2 KB.
TOOL_RESULT_MAX_CHARS = 2_500


def _slim_schema(schema: dict | None) -> dict:
    """
    Strip verbose 'description' fields from every property in a JSON Schema.
    Keeps 'type', 'enum', 'items', 'properties', 'required' — the minimum
    a model needs to call the tool correctly.
    Typically cuts tool schema tokens by 60-70%.
    """
    if not schema:
        return {"type": "object", "properties": {}}

    def _clean(node: dict) -> dict:
        out: dict = {}
        for k, v in node.items():
            if k == "description":
                continue
            if k == "properties" and isinstance(v, dict):
                out[k] = {pk: _clean(pv) for pk, pv in v.items()}
            elif k in ("items", "additionalProperties") and isinstance(v, dict):
                out[k] = _clean(v)
            else:
                out[k] = v
        return out

    return _clean(schema)


def build_tool_list(mcp_tools) -> list:
    """
    Select tools whose name contains any of TOOL_KEYWORDS (case-insensitive),
    excluding anything in TOOL_BLOCKLIST, and slim their schemas.
    Prints a summary so you can see exactly what was chosen.
    """
    chosen, skipped = [], []
    for t in mcp_tools:
        name_lower = t.name.lower()
        if t.name in TOOL_BLOCKLIST:
            skipped.append(t.name)
            continue
        if any(kw in name_lower for kw in TOOL_KEYWORDS):
            chosen.append({
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": (t.description or "")[:120],
                    "parameters": _slim_schema(t.inputSchema),
                },
            })
        else:
            skipped.append(t.name)

    console.print(
        f"  [dim]Tools selected ({len(chosen)}): "
        + ", ".join(c["function"]["name"] for c in chosen)
        + "[/dim]"
    )
    return chosen


# ─────────────────────────────────────────────
# Groq agent loop
# ─────────────────────────────────────────────
async def run_groq(session: ClientSession, messages: list) -> str:
    try:
        from groq import Groq
    except ImportError:
        console.print("[red]groq package not installed. Run: pip install groq[/red]")
        sys.exit(1)

    client = Groq(api_key=GROQ_API_KEY)

    # Build filtered + slimmed tool list once; reuse for every iteration
    mcp_tools = (await session.list_tools()).tools
    groq_tools = build_tool_list(mcp_tools)

    if not groq_tools:
        return "No matching tools found. Check TOOL_KEYWORDS or run with --list-tools."

    # Agentic loop (max 6 rounds — each adds messages so keep it tight)
    for round_num in range(6):
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=messages,
            tools=groq_tools,
            tool_choice="auto",
            max_tokens=1024,
        )
        msg = response.choices[0].message

        if not msg.tool_calls:
            return msg.content or ""

        # Append assistant turn — content may be None when tool_calls are present
        messages.append({
            "role": "assistant",
            "content": msg.content or "",   # Groq requires non-null content
            "tool_calls": [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in msg.tool_calls
            ],
        })

        # Execute each tool call and append results
        for tc in msg.tool_calls:
            args = json.loads(tc.function.arguments or "{}")
            result_text = await call_tool(session, tc.function.name, args)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result_text,
            })

    return "Reached max iterations — partial results above."


# ─────────────────────────────────────────────
# Ollama agent loop
# ─────────────────────────────────────────────
async def run_ollama(session: ClientSession, messages: list) -> str:
    try:
        import ollama
    except ImportError:
        console.print("[red]ollama package not installed. Run: pip install ollama[/red]")
        sys.exit(1)

    client = ollama.Client(host=OLLAMA_URL)

    mcp_tools = (await session.list_tools()).tools
    ollama_tools = build_tool_list(mcp_tools)

    for _ in range(10):
        response = client.chat(
            model=OLLAMA_MODEL,
            messages=messages,
            tools=ollama_tools,
        )
        msg = response.message

        if not msg.tool_calls:
            return msg.content or ""

        messages.append({"role": "assistant", "content": msg.content or ""})

        for tc in msg.tool_calls:
            args = dict(tc.function.arguments) if tc.function.arguments else {}
            result_text = await call_tool(session, tc.function.name, args)
            messages.append({"role": "tool", "content": result_text})

    return "Max iterations reached."


# ─────────────────────────────────────────────
# Pre-fetch Loki label names to ground the model's LogQL generation
# ─────────────────────────────────────────────
async def fetch_loki_labels(session: ClientSession) -> str:
    """Call list_loki_label_names and return a compact string for the system prompt."""
    try:
        result = await session.call_tool(
            "list_loki_label_names",
            {"datasourceUid": LOKI_DS},
        )
        text = result.content[0].text if result.content else ""
        # Parse JSON array if possible, otherwise return raw (truncated)
        try:
            names = json.loads(text)
            if isinstance(names, list):
                return "Labels: " + ", ".join(str(n) for n in names[:40])
        except Exception:
            pass
        return text[:300]
    except Exception as exc:
        return f"(label fetch failed: {exc})"


# ─────────────────────────────────────────────
# Single query dispatcher
# ─────────────────────────────────────────────
async def ask(session: ClientSession, query: str, loki_labels: str = "") -> str:
    messages = [
        {"role": "system", "content": build_system_prompt(loki_labels)},
        {"role": "user",   "content": query},
    ]
    if LLM_PROVIDER == "ollama":
        return await run_ollama(session, messages)
    else:
        return await run_groq(session, messages)


# ─────────────────────────────────────────────
# Auto-triage mode
# ─────────────────────────────────────────────
async def auto_triage(session: ClientSession, loki_labels: str = ""):
    console.print(Panel("[bold yellow]Auto-triage mode[/bold yellow] — investigating all active alerts…"))
    response = await ask(
        session,
        "List all currently firing or pending alerts. For each alert, investigate the root cause "
        "by querying relevant Loki logs and Tempo traces. Return a structured Markdown report with "
        "sections: ## Alert Summary, ## Root Cause Analysis, ## Recommended Actions.",
        loki_labels=loki_labels,
    )
    console.print(Markdown(response))


# ─────────────────────────────────────────────
# Interactive REPL
# ─────────────────────────────────────────────
async def repl(session: ClientSession, loki_labels: str = ""):
    # Rebuild the system prompt each turn so timestamps stay fresh
    console.print("[dim]Type 'exit' to quit, 'clear' to reset conversation.[/dim]\n")
    history: list[dict] = []

    while True:
        try:
            user_input = console.input("[bold cyan]You:[/bold cyan] ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not user_input:
            continue
        if user_input.lower() in ("exit", "quit", "q"):
            break
        if user_input.lower() == "clear":
            history = []
            console.print("[green]Conversation cleared.[/green]")
            continue

        history.append({"role": "user", "content": user_input})
        console.print("[yellow]Thinking…[/yellow]\n")

        if LLM_PROVIDER == "ollama":
            response = await run_ollama(session, history)
        else:
            response = await run_groq(session, history)

        history.append({"role": "assistant", "content": response})
        console.print("[bold green]Assistant:[/bold green]")
        console.print(Markdown(response or "_No response_"))
        console.print()


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="Alert-G MCP Agent")
    parser.add_argument("--query",      "-q", help="Run a single query and exit")
    parser.add_argument("--triage",     "-t", action="store_true",
                        help="Auto-triage all active alerts")
    parser.add_argument("--list-tools", "-l", action="store_true",
                        help="Print all tool names the MCP server exposes, then exit")
    args = parser.parse_args()

    # Validate config
    if not GRAFANA_TOKEN:
        console.print("[bold red]ERROR:[/bold red] GRAFANA_SERVICE_ACCOUNT_TOKEN is not set. See .env.example")
        sys.exit(1)
    if LLM_PROVIDER == "groq" and not GROQ_API_KEY:
        console.print("[bold red]ERROR:[/bold red] GROQ_API_KEY is not set. See .env.example")
        sys.exit(1)

    # Print header
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_row("[cyan]Grafana[/cyan]",  GRAFANA_URL)
    table.add_row("[cyan]MCP runner[/cyan]", MCP_RUNNER.upper())
    table.add_row("[cyan]LLM[/cyan]",      f"{LLM_PROVIDER.upper()} / {GROQ_MODEL if LLM_PROVIDER == 'groq' else OLLAMA_MODEL}")
    table.add_row("[cyan]Loki DS[/cyan]",  LOKI_DS)
    table.add_row("[cyan]Tempo DS[/cyan]", TEMPO_DS)
    table.add_row("[cyan]Prom DS[/cyan]",  PROM_DS)
    console.print(Panel(table, title="[bold green]Alert-G MCP Agent[/bold green]", border_style="green"))

    # Launch Grafana MCP server as a subprocess
    # Option A (default): uvx  — pip install uv  then: uvx mcp-grafana
    # Option B:           docker — docker run grafana/mcp-grafana -t stdio
    mcp_env = {
        **os.environ,
        "GRAFANA_URL":                   GRAFANA_URL,
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
        # uvx (default) — install uv: https://docs.astral.sh/uv/
        server_params = StdioServerParameters(
            command="uvx",
            args=["mcp-grafana"],
            env=mcp_env,
        )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = (await session.list_tools()).tools
            active = build_tool_list(tools)
            console.print(f"[green]✓ MCP connected — {len(active)} tools active (from {len(tools)} total)[/green]")

            if args.list_tools:
                console.print("\n[bold]All tools exposed by the MCP server:[/bold]")
                for t in tools:
                    console.print(f"  [cyan]{t.name}[/cyan] — {(t.description or '')[:80]}")
                console.print(f"\n[dim]Total: {len(tools)}. Add names to TOOL_KEYWORDS to include them.[/dim]")
            elif args.triage:
                await auto_triage(session)
            elif args.query:
                result = await ask(session, args.query)
                console.print(Markdown(result))
            else:
                await repl(session)


if __name__ == "__main__":
    asyncio.run(main())