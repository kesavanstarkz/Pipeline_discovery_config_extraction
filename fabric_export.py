#!/usr/bin/env python3
"""
Fabric Pipeline Exporter
========================
Exports Microsoft Fabric Data Pipelines using the bulkExportDefinitions API.
Reconstructs the ZIP format exactly as it is downloaded from the Fabric UI.
"""

import os
import time
import json
import base64
import zipfile
import argparse
import logging
from typing import Dict, Any, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── Configuration & Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

def get_session() -> requests.Session:
    """Create a requests Session with exponential backoff for 429 and 500s."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def poll_operation(session: requests.Session, location_url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """Poll the Long Running Operation (LRO) until Succeeded or Failed."""
    logger.info(f"Polling operation status...")
    while True:
        resp = session.get(location_url, headers=headers)
        resp.raise_for_status()
        
        # Depending on the API, status is either in headers or JSON body.
        # Fabric LRO usually returns 200 with JSON {"status": "Succeeded", ...}
        # or 202 Accepted while running.
        if resp.status_code == 200:
            data = resp.json()
            status = data.get("status", "Unknown")
            logger.info(f"Operation status: {status}")
            
            if status == "Succeeded":
                # For bulkExportDefinitions, when Succeeded, we call the /result endpoint
                return data
            elif status in ("Failed", "Canceled"):
                raise Exception(f"Operation failed: {json.dumps(data)}")
        
        # If 202, wait and poll again. Read Retry-After if available.
        retry_after = int(resp.headers.get("Retry-After", 5))
        time.sleep(retry_after)

def fetch_result(session: requests.Session, result_url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """Fetch the final result after the LRO succeeds."""
    logger.info(f"Fetching operation result...")
    resp = session.get(result_url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def transform_structure(parts: List[Dict[str, Any]], pipeline_name: str) -> Dict[str, bytes]:
    """
    Decode parts and transform them to match the Fabric UI format.
    - pipeline-content.json -> pipeline.json
    - item.metadata.json -> manifest.json (fixed schema)
    """
    files: Dict[str, bytes] = {}
    
    for part in parts:
        path = part.get("path", "")
        payload_b64 = part.get("payload", "")
        
        try:
            content = base64.b64decode(payload_b64)
        except Exception as e:
            logger.error(f"Failed to decode base64 for {path}: {e}")
            continue

        if path.endswith("pipeline-content.json"):
            # Rename to pipeline.json
            files["pipeline.json"] = content
            
        elif path.endswith("item.metadata.json"):
            # Transform to manifest.json
            try:
                metadata = json.loads(content)
                # Fabric UI manifest format
                manifest = {
                    "name": pipeline_name,
                    "type": "DataPipeline",
                    "properties": metadata
                }
                # If there are any other specific top-level properties to retain,
                # you can copy them dynamically here if needed.
                files["manifest.json"] = json.dumps(manifest, indent=2).encode('utf-8')
            except Exception as e:
                logger.error(f"Failed to process metadata json: {e}")
                files["manifest.json"] = content # fallback to original if parsing fails
        else:
            # Preserve any other files like .platform
            files[path] = content
            
    return files

def create_zip(files: Dict[str, bytes], output_zip_path: str):
    """Write the reconstructed files into a ZIP file."""
    os.makedirs(os.path.dirname(os.path.abspath(output_zip_path)), exist_ok=True)
    
    with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filepath, content in files.items():
            zf.writestr(filepath, content)
            
    logger.info(f"Successfully created ZIP package: {output_zip_path}")

def export_pipeline(
    session: requests.Session, 
    workspace_id: str, 
    pipeline_id: str, 
    pipeline_name: str,
    access_token: str, 
    output_zip_path: str
):
    """Execute the full export flow for a single pipeline."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # 1. Trigger bulkExportDefinitions
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/bulkExportDefinitions?beta=true"
    payload = {
        "items": [
            {
                "id": pipeline_id
            }
        ]
    }
    
    logger.info(f"Triggering export for pipeline {pipeline_id}...")
    resp = session.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    
    # 2. Extract Location header for LRO polling
    location_url = resp.headers.get("Location")
    if not location_url:
        raise Exception("Location header missing from export response. Cannot poll LRO.")
    
    # 3. Poll LRO
    poll_data = poll_operation(session, location_url, headers)
    
    # 4. Get Result
    # Usually, poll_data contains a "result" field or we hit the {location_url}/result endpoint
    # Fabric pattern: GET {operationId}/result
    result_url = f"{location_url}/result"
    result_data = fetch_result(session, result_url, headers)
    
    # 5. Process API response
    items = result_data.get("items", [])
    if not items:
        # Fallback if result is structured differently
        logger.warning("No 'items' found in result. Assuming root is the item definition.")
        items = [result_data]
        
    for item in items:
        target_id = item.get("id")
        if target_id == pipeline_id or not target_id:
            definition = item.get("definition", {})
            parts = definition.get("parts", [])
            
            if not parts:
                raise Exception("No parts found in the exported definition.")
                
            # 6. Transform structure
            files = transform_structure(parts, pipeline_name)
            
            # 7. Package output
            create_zip(files, output_zip_path)
            return

    raise Exception(f"Pipeline {pipeline_id} not found in the exported result payload.")

def main():
    parser = argparse.ArgumentParser(description="Export a Fabric Data Pipeline to a Fabric UI compatible ZIP format.")
    parser.add_argument("--workspace-id", required=True, help="Target Fabric Workspace ID")
    parser.add_argument("--pipeline-id", required=True, help="Target Pipeline ID")
    parser.add_argument("--pipeline-name", required=True, help="Name of the pipeline (used for manifest and default zip name)")
    parser.add_argument("--token", required=True, help="Azure AD Bearer Token")
    parser.add_argument("--output", help="Output ZIP file path (default: <pipeline-name>_export.zip)")
    
    args = parser.parse_args()
    
    output_path = args.output
    if not output_path:
        output_path = f"{args.pipeline_name.replace(' ', '_')}_export.zip"
        
    session = get_session()
    
    try:
        export_pipeline(
            session=session,
            workspace_id=args.workspace_id,
            pipeline_id=args.pipeline_id,
            pipeline_name=args.pipeline_name,
            access_token=args.token,
            output_zip_path=output_path
        )
    except Exception as e:
        logger.error(f"Export failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()
