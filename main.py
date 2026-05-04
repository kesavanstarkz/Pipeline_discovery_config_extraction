"""
Fabric Pipeline Discovery Agent - FastAPI Backend
Connects to Microsoft Fabric via Azure SSO (MSAL)
"""

from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import httpx
import json
import os
import zipfile
import tempfile
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Fabric Pipeline Discovery Agent",
    description="Auto-discovers and exports Azure Fabric pipeline configs",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer(auto_error=False)

# ─── Config ───────────────────────────────────────────────────────────────────

AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
AZURE_REDIRECT_URI = os.getenv("AZURE_REDIRECT_URI")
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"

# ─── Models ───────────────────────────────────────────────────────────────────

class TokenRequest(BaseModel):
    access_token: str

class DiscoveryRequest(BaseModel):
    workspace_id: str
    access_token: str
    pipeline_ids: Optional[List[str]] = None  # None = discover all

class ExportRequest(BaseModel):
    workspace_id: str
    pipeline_id: str
    access_token: str
    include_schema: bool = True
    include_metadata: bool = True

class IngestionMetadata(BaseModel):
    source: str
    ingestion_type: str          # API / S3 / SFTP / ADF / EventHub etc.
    ingestion_frequency: Optional[str] = None
    arrival_time: Optional[str] = None
    trigger_type: Optional[str] = None   # scheduled / event / manual
    file_name_pattern: Optional[str] = None
    ingestion_process: Optional[str] = None
    load_type: Optional[str] = None      # Delta / Merge / Truncate
    processing_steps: Optional[List[str]] = None

class ColumnSchema(BaseModel):
    name: str
    data_type: str
    is_mandatory: bool
    format: Optional[str] = None        # Date, Timestamp, email etc.
    relationships: Optional[str] = None # if A present, B is null etc.
    order: int
    max_length: Optional[int] = None
    allowed_values: Optional[List[str]] = None
    is_nested: bool = False             # for JSON/XML

class FileStructure(BaseModel):
    file_type: str                      # CSV / JSON / XML / Parquet / Avro / PDF
    columns: List[ColumnSchema]
    file_size_bytes: Optional[int] = None
    record_count: Optional[int] = None
    nested_structure: Optional[Dict] = None

class PipelineConfig(BaseModel):
    pipeline_id: str
    pipeline_name: str
    workspace_id: str
    workspace_name: str
    ingestion_metadata: IngestionMetadata
    file_structures: List[FileStructure]
    raw_activities: Optional[List[Dict]] = None
    raw_properties: Optional[Dict] = None
    discovered_at: str
    schema_version: str = "1.0"

# ─── Fabric API Client ────────────────────────────────────────────────────────

class FabricClient:
    def __init__(self, access_token: str):
        self.token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    async def get(self, url: str) -> Dict:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, headers=self.headers)
            if resp.status_code == 401:
                raise HTTPException(status_code=401, detail="Token expired or invalid. Please re-authenticate.")
            if resp.status_code == 403:
                raise HTTPException(status_code=403, detail="Insufficient permissions for this Fabric resource.")
            resp.raise_for_status()
            return resp.json()

    async def post(self, url: str, body: Dict) -> Dict:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, headers=self.headers, json=body)
            if resp.status_code == 401:
                raise HTTPException(status_code=401, detail="Token expired or invalid. Please re-authenticate.")
            if resp.status_code == 403:
                raise HTTPException(status_code=403, detail="Insufficient scopes. Export requires 'DataPipeline.ReadWrite.All' or 'Item.ReadWrite.All'.")
            resp.raise_for_status()
            return resp.json()

    async def list_workspaces(self) -> List[Dict]:
        data = await self.get(f"{FABRIC_API_BASE}/workspaces")
        return data.get("value", [])

    async def list_pipelines(self, workspace_id: str) -> List[Dict]:
        data = await self.get(f"{FABRIC_API_BASE}/workspaces/{workspace_id}/dataPipelines")
        return data.get("value", [])

    async def get_pipeline(self, workspace_id: str, pipeline_id: str) -> Dict:
        return await self.get(
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/dataPipelines/{pipeline_id}"
        )

    async def export_pipeline(self, workspace_id: str, pipeline_id: str) -> Dict:
        """
        Uses Fabric's generic Item API to get the full export-ready definition.
        Format 'Fabric' ensures parameter references are preserved.
        """
        return await self.post(
            f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/getDefinition",
            {"format": "Fabric"}
        )

    async def get_workspace_info(self, workspace_id: str) -> Dict:
        return await self.get(f"{FABRIC_API_BASE}/workspaces/{workspace_id}")

# ─── Data Profiler ────────────────────────────────────────────────────────────

class DataProfiler:
    """
    Profiles sample data to extract schema, data types, and nullability.
    """
    @staticmethod
    def analyze_sample_data(sample_data: List[Dict]) -> tuple[List[ColumnSchema], Dict]:
        if not sample_data:
            return [], {}
        
        columns = {}
        nested_structure = {}
        for row in sample_data:
            for key, val in row.items():
                if key not in columns:
                    columns[key] = {"type": type(val).__name__, "has_null": False, "values": set(), "is_nested": False}
                
                if val is None:
                    columns[key]["has_null"] = True
                elif isinstance(val, (dict, list)):
                    columns[key]["is_nested"] = True
                    if key not in nested_structure:
                        nested_structure[key] = type(val).__name__
                
                # Collect sample values for low cardinality fields (e.g., categorical)
                if val is not None and not isinstance(val, (dict, list)):
                    if len(columns[key]["values"]) < 10:
                        columns[key]["values"].add(str(val))

        schema = []
        for i, (col_name, meta) in enumerate(columns.items()):
            dtype = "String"
            if meta["is_nested"]:
                dtype = "JSON/Complex"
            elif meta["type"] == "int":
                dtype = "Integer"
            elif meta["type"] == "float":
                dtype = "Float"
            elif meta["type"] == "bool":
                dtype = "Boolean"

            allowed_values = list(meta["values"]) if len(meta["values"]) < 10 and meta["type"] == "str" else None
            
            schema.append(ColumnSchema(
                name=col_name,
                data_type=dtype,
                is_mandatory=not meta["has_null"],
                order=i,
                allowed_values=allowed_values,
                is_nested=meta["is_nested"]
            ))
        return schema, nested_structure

# ─── Discovery Engine ─────────────────────────────────────────────────────────

class DiscoveryEngine:
    """
    Core agent that analyzes pipeline definitions and extracts
    ingestion metadata + file structure information
    """

    INGESTION_TYPE_KEYWORDS = {
        "API": ["rest", "http", "api", "webhook", "endpoint"],
        "S3": ["s3", "amazon", "aws", "blob", "adls", "datalake"],
        "SFTP": ["sftp", "ftp", "ssh", "file transfer"],
        "EventHub": ["eventhub", "event hub", "kafka", "streaming", "stream"],
        "SharePoint": ["sharepoint", "onedrive"],
        "SQL": ["sql", "database", "db", "jdbc", "odbc"],
        "CosmosDB": ["cosmos", "cosmosdb", "mongodb"],
    }

    FILE_TYPE_PATTERNS = {
        "CSV": [".csv", "text/csv", "delimited", "comma"],
        "JSON": [".json", "application/json", "json"],
        "XML": [".xml", "text/xml", "application/xml"],
        "Parquet": [".parquet", "parquet"],
        "Avro": [".avro", "avro"],
        "PDF": [".pdf", "application/pdf"],
        "Excel": [".xlsx", ".xls", "excel"],
        "Delta": ["delta", "delta lake"],
    }

    TRIGGER_TYPES = {
        "scheduled": ["schedule", "cron", "tumbling", "recurrence"],
        "event": ["event", "trigger", "blob", "message", "arrival"],
        "manual": ["manual", "on-demand", "adhoc"],
    }

    LOAD_TYPES = {
        "Delta": ["delta", "incremental", "upsert", "merge"],
        "Merge": ["merge", "scd"],
        "Truncate": ["truncate", "full load", "overwrite"],
    }

    def _extract_decoded_content(self, pipeline_def: Dict) -> Dict:
        """Extract actual pipeline JSON from Base64 definition parts if present"""
        if "definition" in pipeline_def and "parts" in pipeline_def["definition"]:
            for part in pipeline_def["definition"]["parts"]:
                if part.get("path") == "pipeline-content.json":
                    import base64
                    payload = part.get("payload", "")
                    try:
                        return json.loads(base64.b64decode(payload).decode('utf-8'))
                    except Exception as e:
                        logger.error(f"Failed to decode pipeline-content.json: {e}")
        return pipeline_def.get("properties", pipeline_def)

    def detect_ingestion_type(self, pipeline_def: Dict) -> str:
        raw_str = json.dumps(pipeline_def).lower()
        for ing_type, keywords in self.INGESTION_TYPE_KEYWORDS.items():
            if any(k in raw_str for k in keywords):
                return ing_type
        return "Not Available"

    def detect_file_types(self, pipeline_def: Dict) -> List[str]:
        raw_str = json.dumps(pipeline_def).lower()
        detected = []
        for ftype, patterns in self.FILE_TYPE_PATTERNS.items():
            if any(p.lower() in raw_str for p in patterns):
                detected.append(ftype)
        return detected if detected else ["Not Available"]

    def detect_trigger_type(self, pipeline_def: Dict) -> str:
        raw_str = json.dumps(pipeline_def).lower()
        for ttype, keywords in self.TRIGGER_TYPES.items():
            if any(k in raw_str for k in keywords):
                return ttype
        return "Not Available"

    def detect_load_type(self, pipeline_def: Dict) -> str:
        raw_str = json.dumps(pipeline_def).lower()
        for ltype, keywords in self.LOAD_TYPES.items():
            if any(k in raw_str for k in keywords):
                return ltype
        return "Not Available"

    def extract_source(self, actual_content: Dict) -> str:
        """Extract source system name from pipeline activities"""
        activities = actual_content.get("properties", actual_content).get("activities", actual_content.get("activities", []))
        for act in activities:
            src = act.get("typeProperties", {}).get("source", {})
            if src:
                linked = src.get("linkedServiceName", {})
                if isinstance(linked, dict):
                    return linked.get("referenceName", "Not Available")
                if isinstance(linked, str):
                    return linked
        return "Not Available"

    def extract_columns(self, actual_content: Dict, file_type: str) -> List[ColumnSchema]:
        """
        Extract column/schema info from pipeline dataset definitions
        """
        columns = []
        activities = actual_content.get("properties", actual_content).get("activities", actual_content.get("activities", []))

        for i, act in enumerate(activities):
            tp = act.get("typeProperties", {})
            translator = tp.get("translator", {})
            mappings = translator.get("columnMappings", [])
            
            if isinstance(mappings, list):
                for j, col in enumerate(mappings):
                    if isinstance(col, dict):
                        name = col.get("source", {}).get("name", f"column_{j}")
                        dtype = col.get("source", {}).get("type", "String")
                    else:
                        name = str(col)
                        dtype = "String"

                    # Infer format and mandatory status
                    fmt = None
                    if "date" in name.lower() or "time" in name.lower(): fmt = "Timestamp"
                    elif "email" in name.lower(): fmt = "Email"
                    
                    columns.append(ColumnSchema(
                        name=name,
                        data_type=dtype,
                        is_mandatory=True,
                        format=fmt,
                        order=j,
                        is_nested=file_type in ["JSON", "XML"]
                    ))

        if not columns:
            columns = [
                ColumnSchema(name="*", data_type="Dynamic",
                             is_mandatory=False, order=0,
                             is_nested=file_type in ["JSON", "XML"])
            ]
        return columns

    def extract_file_name_pattern(self, actual_content: Dict) -> str:
        raw_str = json.dumps(actual_content)
        import re
        patterns = re.findall(r'"fileName":\s*"([^"]+)"', raw_str)
        return patterns[0] if patterns else "Not Available"

    def extract_schedule(self, pipeline_def: Dict) -> Dict:
        """Extracts frequency and arrival times from schedule triggers or .schedules part"""
        schedule = "Not Available"
        arrival = "Not Available"
        
        # Check .schedules part if it's a getDefinition response
        if "definition" in pipeline_def and "parts" in pipeline_def["definition"]:
            for part in pipeline_def["definition"]["parts"]:
                if part.get("path") == ".schedules":
                    import base64
                    try:
                        payload = base64.b64decode(part.get("payload", "")).decode('utf-8')
                        sched_json = json.loads(payload)
                        schedules = sched_json.get("schedules", [])
                        if schedules:
                            config = schedules[0].get("configuration", {})
                            stype = config.get("type", "Manual")
                            times = config.get("times", ["00:00"])
                            schedule = f"Every {stype}"
                            arrival = f"{stype} at {', '.join(times)}"
                            return {"schedule": schedule, "arrival": arrival}
                    except:
                        pass
        
        # Fallback for raw JSON
        raw_str = json.dumps(pipeline_def).lower()
        import re
        freq = re.findall(r'"frequency":\s*"([^"]+)"', raw_str, re.IGNORECASE)
        interval = re.findall(r'"interval":\s*(\d+)', raw_str, re.IGNORECASE)
        
        if freq and interval:
            schedule = f"Every {interval[0]} {freq[0]}"
            # Mock arrival time based on frequency
            if "day" in freq[0]: arrival = "Daily at 12:00 AM UTC"
            elif "week" in freq[0]: arrival = "Monday at 8:00 AM UTC"
            
        return {"schedule": schedule, "arrival": arrival}

    def extract_processing_steps(self, actual_content: Dict) -> List[str]:
        """Extract ordered processing steps from pipeline activities"""
        activities = actual_content.get("properties", actual_content).get("activities", actual_content.get("activities", []))
        steps = []
        for act in activities:
            name = act.get("name", "UnknownStep")
            act_type = act.get("type", "UnknownType")
            steps.append(f"{name} ({act_type})")
        return steps if steps else ["Not Available"]

    def analyze_pipeline(self, pipeline_def: Dict, workspace_name: str, p_name: str = None, p_id: str = None) -> PipelineConfig:
        actual_content = self._extract_decoded_content(pipeline_def)
        props = pipeline_def.get("properties", pipeline_def)
        pipeline_id = p_id or pipeline_def.get("id", str(uuid.uuid4()))
        pipeline_name = p_name or pipeline_def.get("displayName", pipeline_def.get("name", "Unknown"))
        workspace_id = pipeline_def.get("workspaceId", "")

        ingestion_type = self.detect_ingestion_type(actual_content)
        file_types = self.detect_file_types(actual_content)
        trigger_type = self.detect_trigger_type(actual_content)
        load_type = self.detect_load_type(actual_content)
        source = self.extract_source(actual_content)
        file_pattern = self.extract_file_name_pattern(actual_content)
        schedule_data = self.extract_schedule(pipeline_def)
        schedule = schedule_data["schedule"]
        arrival_time = schedule_data["arrival"]
        processing_steps = self.extract_processing_steps(actual_content)

        ingestion_meta = IngestionMetadata(
            source=source,
            ingestion_type=ingestion_type,
            ingestion_frequency=schedule,
            arrival_time=arrival_time,
            trigger_type=trigger_type,
            file_name_pattern=file_pattern,
            load_type=load_type,
            ingestion_process=props.get("name", pipeline_name),
            processing_steps=processing_steps,
        )

        file_structures = []
        for ftype in file_types:
            cols = self.extract_columns(pipeline_def, ftype)
            file_structures.append(FileStructure(
                file_type=ftype,
                columns=cols,
                is_nested=ftype in ["JSON", "XML"],
            ))

        return PipelineConfig(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            ingestion_metadata=ingestion_meta,
            file_structures=file_structures,
            raw_activities=props.get("activities", []),
            raw_properties=props,
            discovered_at=datetime.utcnow().isoformat(),
        )

class ExportEngine:
    """
    Step 5-11: Build a Deep-Inspection Export Package
    Inlines all dependencies and matches the Git Integration format.
    """
    def _apply_dynamic_mapping(self, obj: Any) -> Any:
        """Replace hardcoded workspace and artifact IDs with dynamic mapping placeholders"""
        if isinstance(obj, dict):
            mapped_obj = {}
            for k, v in obj.items():
                if k == "workspaceId" and isinstance(v, str) and len(v) > 20:
                    mapped_obj[k] = "<workspace_id_placeholder>"
                elif k == "artifactId" and isinstance(v, str) and len(v) > 20:
                    mapped_obj[k] = "<artifact_id_placeholder>"
                else:
                    mapped_obj[k] = self._apply_dynamic_mapping(v)
            return mapped_obj
        elif isinstance(obj, list):
            return [self._apply_dynamic_mapping(item) for item in obj]
        return obj

    def create_deep_export(
        self, metadata_list: List[Dict], raw_definitions: List[Dict]
    ) -> str:
        """
        Builds a Fabric UI Export format package using live definition data.
        - API Ingestion.json: Full pipeline JSON from getDefinition
        - manifest.json: Metadata from get_pipeline
        """
        tmp_dir = tempfile.mkdtemp()
        zip_path = os.path.join(tmp_dir, f"fabric_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip")

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for i, raw in enumerate(raw_definitions):
                meta = metadata_list[i] if i < len(metadata_list) else {}
                p_name = meta.get("displayName", "Unknown")
                
                # 1. Generate the Full Pipeline JSON (<PipelineDisplayName>.json)
                ui_content = self._construct_ui_deployment_template(raw)
                
                # 2. Generate manifest.json from LIVE metadata
                manifest = self._construct_ui_manifest(meta, raw)
                
                # Prefix for bulk export
                prefix = f"{p_name}/" if len(raw_definitions) > 1 else ""
                
                zf.writestr(f"{prefix}{p_name}.json", json.dumps(ui_content, indent=2))
                zf.writestr(f"{prefix}manifest.json", json.dumps(manifest, indent=2))

        return zip_path

    def _construct_ui_deployment_template(self, raw_definition: Dict) -> Dict:
        """
        Extracts the full pipeline JSON exactly as returned in getDefinition.
        """
        import base64
        if "definition" in raw_definition and "parts" in raw_definition["definition"]:
            for part in raw_definition["definition"]["parts"]:
                if part.get("path") == "pipeline-content.json":
                    try:
                        payload = part.get("payload", "")
                        # Save the decoded JSON exactly as returned
                        return json.loads(base64.b64decode(payload).decode('utf-8'))
                    except Exception as e:
                        logger.error(f"Failed to decode pipeline-content.json: {e}")
        return {}

    def _construct_ui_manifest(self, metadata: Dict, raw_definition: Dict) -> Dict:
        """
        Constructs manifest.json by extracting metadata directly from the live API responses.
        """
        import base64
        logical_id = "00000000-0000-0000-0000-000000000000"
        
        # Extract logicalId from .platform part if available
        if "definition" in raw_definition and "parts" in raw_definition["definition"]:
            for part in raw_definition["definition"]["parts"]:
                if part.get("path") == ".platform":
                    try:
                        payload = base64.b64decode(part.get("payload", "")).decode('utf-8')
                        platform_json = json.loads(payload)
                        logical_id = platform_json.get("config", {}).get("logicalId", logical_id)
                        break
                    except Exception:
                        pass

        return {
            "displayName": metadata.get("displayName", "Unknown"),
            "description": metadata.get("description") or "",
            "type": "DataPipeline",
            "logicalId": logical_id,
            "version": "1.0"
        }



    def _extract_dependencies(self, raw: Dict) -> Dict:
        """Step 2: Recursive dependency extraction"""
        deps = {"datasets": set(), "linkedServices": set(), "pipelines": set()}
        def search(obj):
            if isinstance(obj, dict):
                if "dataset" in obj and isinstance(obj["dataset"], dict):
                    name = obj["dataset"].get("referenceName")
                    if name: deps["datasets"].add(name)
                if "linkedServiceName" in obj:
                    ls = obj["linkedServiceName"]
                    name = ls.get("referenceName") if isinstance(ls, dict) else ls
                    if name: deps["linkedServices"].add(name)
                if "type" in obj and obj["type"] == "ExecutePipeline":
                    p_name = obj.get("typeProperties", {}).get("pipeline", {}).get("referenceName")
                    if p_name: deps["pipelines"].add(p_name)
                for v in obj.values(): search(v)
            elif isinstance(obj, list):
                for item in obj: search(item)
        search(raw)
        return {k: list(v) for k, v in deps.items()}

    def _generate_import_guide(self) -> str:
        return """# Import Logic
To rebuild this workspace, follow this order:
1. **linkedServices/**
2. **datasets/**
3. **dataflows/** (if any)
4. **pipelines/**

Use the Fabric REST API:
- POST `/linkedServices`
- POST `/datasets`
- POST `/dataPipelines`
"""

    def _generate_readme(self, config: PipelineConfig) -> str:
        ing = config.ingestion_metadata
        return f"""# Pipeline: {config.pipeline_name}

## Overview
- **Pipeline ID**: {config.pipeline_id}
- **Workspace**: {config.workspace_name}
- **Discovered**: {config.discovered_at}

## Ingestion Configuration
| Property | Value |
|----------|-------|
| Source | {ing.source} |
| Ingestion Type | {ing.ingestion_type} |
| Trigger Type | {ing.trigger_type} |
| Load Type | {ing.load_type} |
| Schedule | {ing.ingestion_frequency or 'N/A'} |
| File Pattern | {ing.file_name_pattern or 'N/A'} |

## File Structures
{self._format_schema_table(config.file_structures)}

## How to Import
See `HOW_TO_IMPORT.md` in the root of this ZIP.
"""

    def _format_schema_table(self, structures: List[FileStructure]) -> str:
        out = ""
        for fs in structures:
            out += f"\n### {fs.file_type}\n"
            out += "| Column | Type | Mandatory | Format | Order |\n"
            out += "|--------|------|-----------|--------|-------|\n"
            for col in fs.columns:
                out += f"| {col.name} | {col.data_type} | {'Yes' if col.is_mandatory else 'No'} | {col.format or '-'} | {col.order} |\n"
        return out

    def _import_guide(self) -> str:
        return """# How to Import Pipelines into Microsoft Fabric

## Method 1: Fabric UI Import
1. Open Microsoft Fabric (app.fabric.microsoft.com)
2. Navigate to your target Workspace
3. Click **+ New** → **Data Pipeline**
4. Select **Import** and upload `pipeline_definition.json`
5. Update linked services / credentials as needed

## Method 2: Fabric REST API
```bash
curl -X POST \\
  "https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/dataPipelines/import" \\
  -H "Authorization: Bearer {YOUR_TOKEN}" \\
  -H "Content-Type: application/json" \\
  -d @pipeline_definition.json
```

## Method 3: Azure DevOps / CI-CD
Place `pipeline_definition.json` in your repo and use the Fabric REST API
in your pipeline YAML to auto-deploy on merge.

## After Import
- Review `metadata.json` for ingestion settings
- Review `schema.json` to validate column mappings
- Update connection strings / credentials in Fabric Settings
"""


# ─── Routes ───────────────────────────────────────────────────────────────────

engine = DiscoveryEngine()
export_engine = ExportEngine()


@app.get("/auth/config")
async def auth_config():
    """Returns MSAL config for frontend"""
    return {
        "clientId": AZURE_CLIENT_ID,
        "tenantId": AZURE_TENANT_ID,
        "authority": f"https://login.microsoftonline.com/{AZURE_TENANT_ID}",
        "redirectUri": AZURE_REDIRECT_URI
    }

@app.get("/")
async def serve_ui():
    """Serves the frontend UI"""
    return FileResponse("index.html")

@app.get("/login")
async def login():
    """Redirects user to Azure login page"""
    scope = "https://api.fabric.microsoft.com/DataPipeline.ReadWrite.All https://api.fabric.microsoft.com/Item.ReadWrite.All offline_access"
    auth_url = (
        f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/authorize"
        f"?client_id={AZURE_CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={AZURE_REDIRECT_URI}"
        f"&response_mode=query"
        f"&scope={scope}"
    )
    return RedirectResponse(auth_url)

@app.get("/getAToken")
async def auth_callback(code: str):
    """Handles Azure callback and exchanges code for token"""
    token_url = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": AZURE_CLIENT_ID,
        "client_secret": AZURE_CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": AZURE_REDIRECT_URI,
        "scope": "https://api.fabric.microsoft.com/DataPipeline.ReadWrite.All https://api.fabric.microsoft.com/Item.ReadWrite.All"
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(token_url, data=data)
        if resp.status_code != 200:
            # Fallback: if token exchange fails, just go back to home
            return RedirectResponse("/")
        
        token_data = resp.json()
        token = token_data.get("access_token")
        # Redirect back to home with token in fragment
        return RedirectResponse(f"/#access_token={token}")

@app.get("/{path:path}")
async def static_proxy(path: str):
    """Serves other static files (like msal.min.js)"""
    if os.path.exists(path) and os.path.isfile(path):
        return FileResponse(path)
    return FileResponse("index.html")

@app.post("/workspaces")
async def list_workspaces(req: TokenRequest):
    """List all Fabric workspaces accessible to the user"""
    client = FabricClient(req.access_token)
    workspaces = await client.list_workspaces()
    return {"workspaces": workspaces, "count": len(workspaces)}

@app.post("/workspaces/{workspace_id}/pipelines")
async def list_pipelines(workspace_id: str, req: TokenRequest):
    """List all data pipelines in a workspace"""
    client = FabricClient(req.access_token)
    pipelines = await client.list_pipelines(workspace_id)
    return {"pipelines": pipelines, "count": len(pipelines)}

@app.post("/discover")
async def discover_pipelines(req: DiscoveryRequest):
    """
    Main Discovery Agent endpoint.
    Fetches pipeline definitions and runs the full analysis:
    - Ingestion type detection
    - File type detection
    - File structure / schema extraction
    - Trigger, schedule, load type detection
    """
    client = FabricClient(req.access_token)

    # Get workspace info
    try:
        ws_info = await client.get_workspace_info(req.workspace_id)
        ws_name = ws_info.get("displayName", req.workspace_id)
    except Exception:
        ws_name = req.workspace_id

    # Discover pipelines
    pipeline_meta_map = {}
    if req.pipeline_ids:
        pipeline_ids = req.pipeline_ids
        # Fetch names for specific IDs
        for pid in pipeline_ids:
            try:
                m = await client.get_pipeline(req.workspace_id, pid)
                pipeline_meta_map[pid] = m.get("displayName", "Unknown")
            except:
                pipeline_meta_map[pid] = "Unknown"
    else:
        pipelines = await client.list_pipelines(req.workspace_id)
        pipeline_ids = [p["id"] for p in pipelines]
        pipeline_meta_map = {p["id"]: p.get("displayName", "Unknown") for p in pipelines}

    configs = []
    errors = []
    all_deps = {"datasets": set(), "linkedServices": set()}

    for pid in pipeline_ids:
        try:
            p_name = pipeline_meta_map.get(pid, "Unknown")
            try:
                pipe_def = await client.export_pipeline(req.workspace_id, pid)
            except Exception:
                pipe_def = await client.get_pipeline(req.workspace_id, pid)

            config = engine.analyze_pipeline(pipe_def, ws_name, p_name=p_name, p_id=pid)
            configs.append(config.dict())
            
            # Aggregate dependencies for summary
            deps = export_engine._extract_dependencies(pipe_def)
            all_deps["datasets"].update(deps.get("datasets", []))
            all_deps["linkedServices"].update(deps.get("linkedServices", []))
        except Exception as e:
            errors.append({"pipeline_id": pid, "error": str(e)})

    return {
        "workspace_id": req.workspace_id,
        "workspace_name": ws_name,
        "configs": configs,
        "summary": {
            "pipeline_count": len(configs),
            "dataset_count": len(all_deps["datasets"]),
            "linked_service_count": len(all_deps["linkedServices"]),
            "total_resources": len(configs) + len(all_deps["datasets"]) + len(all_deps["linkedServices"])
        },
        "errors": errors
    }

@app.post("/export")
async def export_pipeline(req: ExportRequest):
    """
    Export a single pipeline as a portable ZIP package.
    The ZIP can be imported into any Fabric workspace — no rebuild needed.
    """
    client = FabricClient(req.access_token)

    try:
        ws_info = await client.get_workspace_info(req.workspace_id)
        ws_name = ws_info.get("displayName", req.workspace_id)
    except Exception:
        ws_name = req.workspace_id

    # 1. Validate pipeline ID against current workspace (Metadata Check)
    try:
        # Instead of listing all (which might be paged), we check the specific ID metadata
        # If this fails with 404, the pipeline is invalid or unavailable.
        try:
            pipe_meta = await client.get_pipeline(req.workspace_id, req.pipeline_id)
        except HTTPException as he:
            if he.status_code == 404:
                raise HTTPException(
                    status_code=400, 
                    detail="Selected pipeline is no longer available in workspace"
                )
            raise he
            
        # 2. Fetch Definition
        pipe_def = await client.export_pipeline(req.workspace_id, req.pipeline_id)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Export validation or fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    # 3. Use metadata for the pipeline name
    pipeline_name = pipe_meta.get("displayName") or pipe_meta.get("name") or "Unknown"

    # 2. Extract verbatim parts (Pipeline Definition, Manifest, and SVG)
    import base64
    parts_map = {}
    exported_svg = ""
    manifest_obj = {}

    if "definition" in pipe_def and "parts" in pipe_def["definition"]:
        for part in pipe_def["definition"]["parts"]:
            path = part.get("path", "")
            try:
                payload_raw = part.get("payload", "")
                payload = base64.b64decode(payload_raw).decode('utf-8')
                parts_map[path] = payload
                
                # Capture SVG if found in any part
                if path.lower().endswith(".svg") or "image" in path.lower():
                    exported_svg = payload
                
                # Capture Manifest if found
                if path == "manifest.json":
                    try:
                        manifest_obj = json.loads(payload)
                    except:
                        pass
            except:
                continue

    # Identify core pipeline file
    # Fabric items usually have a main content file. For pipelines it's pipeline-content.json
    pipeline_json_str = parts_map.get("pipeline-content.json", "{}")
    pipeline_json = json.loads(pipeline_json_str)

    # 3. Use metadata for the pipeline name
    pipeline_name = pipe_meta.get("displayName") or pipe_meta.get("name") or "Unknown"

    # 4. Auto-detect all [parameters('...')] references (Aggressive Search)
    import re
    # Matches [parameters('name')], [ parameters( "name" ) ], etc.
    param_pattern = r"\[\s*parameters\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\]"
    detected_params = re.findall(param_pattern, pipeline_json_str)
    
    # Initialize parameters block
    final_parameters = pipeline_json.get("parameters", {})
    for p_name in set(detected_params):
        if p_name not in final_parameters:
            final_parameters[p_name] = {"type": "string"}

    # 5. Construct the Full ARM Deployment Template (api_ingestion.json)
    # This matches the native Fabric Home -> Export behavior
    arm_template = {
        "$schema": "http://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#",
        "contentVersion": "1.0.0.0",
        "parameters": final_parameters,
        "variables": pipeline_json.get("variables", {}),
        "resources": [
            {
                "name": pipeline_name,
                "type": "pipelines",
                "apiVersion": "2018-06-01",
                "properties": pipeline_json.get("properties", pipeline_json),
                "dependsOn": []
            }
        ]
    }

    # 6. Explicitly construct/preserve manifest.json
    if not manifest_obj:
        manifest_obj = {
            "name": pipeline_name,
            "image": exported_svg or "" 
        }
    else:
        # Prioritize the image we found in parts if manifest.json was incomplete
        if not manifest_obj.get("image") and exported_svg:
            manifest_obj["image"] = exported_svg
        if "name" not in manifest_obj:
            manifest_obj["name"] = pipeline_name

    # 7. Write files to disk as requested
    try:
        with open("api_ingestion.json", "w", encoding="utf-8") as f:
            json.dump(arm_template, f, indent=2)
        with open("manifest.json", "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to write files to disk: {e}")

    # 7. Generate the ZIP for download
    # We use a custom zip creation here to ensure exact filenames
    tmp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(tmp_dir, f"{pipeline_name}_export.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("api_ingestion.json", json.dumps(arm_template, indent=2))
        zf.writestr("manifest.json", json.dumps(manifest_obj, indent=2))

    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=f"{pipeline_name}_export.zip"
    )

@app.post("/export/bulk")
async def export_all_pipelines(req: DiscoveryRequest):
    """
    Export ALL pipelines in a workspace as one ZIP package.
    Perfect for workspace migration or sharing with a team.
    """
    client = FabricClient(req.access_token)

    try:
        ws_info = await client.get_workspace_info(req.workspace_id)
        ws_name = ws_info.get("displayName", req.workspace_id)
    except Exception:
        ws_name = req.workspace_id

    if req.pipeline_ids:
        pipeline_ids = req.pipeline_ids
    else:
        pipelines = await client.list_pipelines(req.workspace_id)
        pipeline_ids = [p["id"] for p in pipelines]

    metadata_list = []
    raw_defs = []

    for pid in pipeline_ids:
        try:
            # Fetch both metadata and definition
            pipe_meta = await client.get_pipeline(req.workspace_id, pid)
            pipe_def = await client.export_pipeline(req.workspace_id, pid)

            metadata_list.append(pipe_meta)
            raw_defs.append(pipe_def)
        except Exception as e:
            logger.error(f"Skipping pipeline {pid}: {e}")

    if not raw_defs:
        raise HTTPException(status_code=404, detail="No pipelines found or accessible.")

    zip_path = export_engine.create_deep_export(metadata_list, raw_defs)

    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=f"bulk_deep_export_{ws_name}.zip"
    )

@app.post("/analyze/schema")
async def analyze_schema(payload: Dict[str, Any]):
    """
    Analyze a pipeline definition JSON directly (paste mode).
    Useful for offline analysis without Fabric connection.
    If 'sample_data' is provided, it profiles the data to generate schema.
    """
    pipeline_def = payload.get("pipeline_definition", {})
    ws_name = payload.get("workspace_name", "Local")
    sample_data = payload.get("sample_data")

    config = engine.analyze_pipeline(pipeline_def, ws_name)
    
    if sample_data and isinstance(sample_data, list):
        data_schema, nested_struct = DataProfiler.analyze_sample_data(sample_data)
        if data_schema:
            # Append profiling results to file_structures
            config.file_structures.append(FileStructure(
                file_type="ProfiledData",
                columns=data_schema,
                record_count=len(sample_data),
                nested_structure=nested_struct if nested_struct else None
            ))

    return config.dict()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
