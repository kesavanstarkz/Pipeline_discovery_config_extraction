# 🔍 Fabric Pipeline Discovery Agent

Auto-discovers Azure Fabric pipeline configurations — ingestion types, file structures, schemas — and exports them as portable ZIP packages anyone can import.

---

## What It Does

| Feature | Detail |
|--------|---------|
| **SSO Login** | Azure MSAL popup — no passwords, uses your Microsoft account |
| **Ingestion Type Detection** | Identifies API, S3/ADLS, SFTP, EventHub, SQL, CosmosDB, SharePoint |
| **File Type Detection** | CSV, JSON, XML, Parquet, Avro, PDF, Excel, Delta |
| **File Structure Detection** | Columns, data types, mandatory/optional, format, relationships, nested structures |
| **Trigger / Schedule Detection** | Scheduled, Event-driven, Manual |
| **Load Type Detection** | Delta (incremental), Merge, Truncate |
| **One-Click Export** | Portable ZIP: pipeline definition + metadata + schema + README |
| **Bulk Export** | Export ALL pipelines in a workspace at once |
| **Offline Analysis** | Paste any pipeline JSON and analyze without Fabric connection |

---

## Project Structure

```
fabric_agent/
├── main.py              # FastAPI backend — discovery + export engine
├── index.html           # Frontend — MSAL SSO + pipeline dashboard
├── requirements.txt     # Python dependencies
├── .env.example         # Environment variable template
└── README.md
```

---

## Setup

### 1. Azure App Registration

Go to [portal.azure.com](https://portal.azure.com) → **Azure Active Directory** → **App Registrations** → **New Registration**:

- **Name**: `FabricDiscoveryAgent`
- **Redirect URI**: `http://localhost:3000` (Single Page Application)
- **API Permissions** (add all):
  - `https://api.fabric.microsoft.com/Workspace.Read.All`
  - `https://api.fabric.microsoft.com/Item.Read.All`
  - `https://api.fabric.microsoft.com/Pipeline.Read.All`
  - `https://analysis.windows.net/powerbi/api/Workspace.Read.All`
- Click **Grant Admin Consent**

Copy your **Client ID** and **Tenant ID**.

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your Azure values
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
```

Update `index.html` — find these two lines and replace:
```javascript
clientId: "YOUR_CLIENT_ID",        // ← your App Registration Client ID
authority: "https://login.microsoftonline.com/YOUR_TENANT_ID",
```

### 3. Install & Run Backend

```bash
pip install -r requirements.txt

# With .env file
uvicorn main:app --reload --port 8000

# Or with env vars inline
AZURE_TENANT_ID=xxx AZURE_CLIENT_ID=yyy uvicorn main:app --reload
```

### 4. Open Frontend

```bash
# Serve index.html (needs to be served, not opened as file, for MSAL to work)
python -m http.server 3000
# Open http://localhost:3000
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Health check |
| `GET` | `/auth/config` | Returns MSAL config for frontend |
| `POST` | `/workspaces` | List all accessible workspaces |
| `POST` | `/workspaces/{id}/pipelines` | List pipelines in workspace |
| `POST` | `/discover` | **Main discovery** — analyze all pipelines |
| `POST` | `/export` | Export single pipeline as ZIP |
| `POST` | `/export/bulk` | Export all pipelines as ZIP |
| `POST` | `/analyze/schema` | Offline: analyze pasted pipeline JSON |

### Example: Discover Pipelines

```bash
curl -X POST http://localhost:8000/discover \
  -H "Content-Type: application/json" \
  -d '{
    "workspace_id": "your-workspace-id",
    "access_token": "your-fabric-token"
  }'
```

### Example: Export All Pipelines

```bash
curl -X POST http://localhost:8000/export/bulk \
  -H "Content-Type: application/json" \
  -d '{
    "workspace_id": "your-workspace-id",
    "access_token": "your-fabric-token"
  }' \
  --output pipelines_export.zip
```

---

## Export ZIP Structure

When you export, you get a ZIP like this:

```
fabric_pipelines_export_20240430_120000.zip
├── index.json                        ← master manifest
├── HOW_TO_IMPORT.md                  ← import guide
├── pipeline_1_Sales_Ingestion/
│   ├── pipeline_definition.json      ← Fabric-importable definition
│   ├── metadata.json                 ← ingestion config
│   ├── schema.json                   ← column definitions
│   └── README.md
└── pipeline_2_Customer_Load/
    ├── pipeline_definition.json
    ├── metadata.json
    ├── schema.json
    └── README.md
```

**To import**: Go to Fabric → your workspace → New → Data Pipeline → Import → upload `pipeline_definition.json`. Done.

---

## Discovery Agent Logic

```
Pipeline JSON
      │
      ▼
┌─────────────────────────────┐
│   Ingestion Type Detection   │  Scans for: rest/http/api → API
│                             │              s3/adls/blob  → S3
│                             │              sftp/ftp      → SFTP
│                             │              eventhub/kafka→ EventHub
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│    File Type Detection       │  Detects: .csv/.json/.parquet etc.
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│  File Structure Extraction   │  Columns, types, mandatory,
│                             │  formats, relationships, order
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│  Trigger / Schedule / Load   │  scheduled/event/manual
│  Type Detection              │  Delta/Merge/Truncate
└──────────────┬──────────────┘
               │
               ▼
        PipelineConfig
        (stored + exported)
```

---

## Import Someone Else's Export

1. Unzip the package
2. In Fabric → workspace → **New → Data Pipeline → Import**
3. Upload `pipeline_definition.json`
4. Update credentials/linked services
5. Review `schema.json` for expected column structure
6. Run!

---

## Notes

- The `getDefinition` API is used first (most complete). Falls back to regular `GET` if unavailable.
- Schema extraction is best-effort — Fabric doesn't always expose full schema in the REST API. For richer schemas, connect to the Lakehouse/Warehouse directly.
- Tokens are never stored server-side — they flow from the frontend per request.
"# Pipeline_discovery_config_extraction" 
