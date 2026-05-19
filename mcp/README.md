# Alert-G MCP Setup

Connects **Grafana Cloud** (Loki logs, Tempo traces, Alerts)
to a **free LLM** via the official [Grafana MCP server](https://github.com/grafana/mcp-grafana).

## Quick start

### 1. Grafana Service Account token

1. Grafana → **Administration → Users and access → Service accounts**
2. **Add service account** — name `mcp-agent`, role **Viewer**
3. **Add service account token → Generate** — copy the `glsa_…` string

### 2. Free LLM
**Groq** (free cloud, no GPU, recommended):
Sign up at https://console.groq.com → API Keys → Create → copy `gsk_…`

### 3. Configure

```bash
cp env.example .env
# Fill in:
#   GRAFANA_SERVICE_ACCOUNT_TOKEN=glsa_...
#   GROQ_API_KEY=gsk_...         
#   MCP_RUNNER=uvx              
#   LLM_PROVIDER=groq             
```

### 4. Install Python deps & run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install uv

python agent.py                  # interactive chat
python agent.py --triage         # auto-investigate all active alerts
python agent.py -q "Why is cartservice throwing 5xx errors?"
```


## Useful queries

```
What services are currently alerting?
Show me the last 20 error logs from checkoutservice
Are there slow traces (>2s) from paymentservice in the last 30 minutes?
What is the p99 latency of the frontend service right now?
Summarise all firing alerts and suggest remediation steps
```