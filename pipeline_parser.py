"""
pipeline_parser.py
==================
Dynamically parses any Fabric pipeline ZIP (manifest.json + pipeline<name>.json)
and remaps all IDs for deployment to a new workspace.
"""

import io
import json
import re
import zipfile
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# ZIP Parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_pipeline_zip(raw_bytes: bytes) -> dict:
    """
    Given raw bytes of a Fabric pipeline ZIP, returns a dict with:
    - pipeline_name        : str
    - activities           : list of {name, type, depends_on}
    - variables            : dict
    - linked_services      : list of unique linked service names
    - connections          : list of unique external connection IDs
    - notebook_ids         : list of notebook IDs referenced
    - workspace_ids        : list of workspace IDs found
    - artifact_ids         : list of artifact IDs found
    - parameters           : dict (from pipeline JSON parameters section)
    - manifest             : dict (full manifest.json)
    - raw_definition       : dict (the pipeline properties block, ready to POST)
    """
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
        names = zf.namelist()

        manifest_path = _find_file(names, "manifest.json")
        pipeline_path = _find_pipeline_json(names)

        if not manifest_path:
            raise ValueError("manifest.json not found in ZIP")
        if not pipeline_path:
            raise ValueError("Pipeline JSON file not found in ZIP")

        manifest = json.loads(zf.read(manifest_path))
        pipeline_json = json.loads(zf.read(pipeline_path))

    # The pipeline JSON follows ARM template structure:
    # resources[0].name, resources[0].properties
    resource = _get_resource(pipeline_json)
    props = resource.get("properties", {})
    activities = props.get("activities", [])

    # Flatten all activities (including nested ForEach / IfCondition children)
    all_activities = _flatten_activities(activities)

    # Extract referenced IDs
    raw_str = json.dumps(pipeline_json)
    workspace_ids = _extract_guids_by_key(pipeline_json, "workspaceId")
    artifact_ids = _extract_guids_by_key(pipeline_json, "artifactId")
    notebook_ids = _extract_guids_by_key(pipeline_json, "notebookId")
    connection_ids = _extract_connection_ids(pipeline_json)
    linked_services = _extract_linked_service_names(all_activities)

    # Build the definition to post: just the properties block
    raw_definition = {
        "properties": props
    }

    return {
        "pipeline_name": resource.get("name", "Imported Pipeline"),
        "activities": [
            {
                "name": a.get("name"),
                "type": a.get("type"),
                "depends_on": [d.get("activity") for d in a.get("dependsOn", [])],
            }
            for a in all_activities
        ],
        "variables": props.get("variables", {}),
        "parameters": pipeline_json.get("parameters", {}),
        "linked_services": list(linked_services),
        "connections": list(connection_ids),
        "notebook_ids": list(notebook_ids),
        "workspace_ids": list(workspace_ids),
        "artifact_ids": list(artifact_ids),
        "manifest": manifest,
        "raw_definition": raw_definition,
    }


# ─────────────────────────────────────────────────────────────────────────────
# ID Remapper
# ─────────────────────────────────────────────────────────────────────────────

def remap_pipeline(raw_definition: dict, id_mappings: dict[str, str]) -> dict:
    """
    Replaces every occurrence of old IDs → new IDs throughout the entire
    pipeline definition JSON (does a full string replace, handles all nested
    structures: workspace IDs, artifact IDs, connection IDs, notebook IDs, etc.)

    id_mappings: { "old-id": "new-id", ... }
    """
    if not id_mappings:
        return raw_definition

    # Serialise → string replace → deserialise
    raw_str = json.dumps(raw_definition)

    for old_id, new_id in id_mappings.items():
        if old_id and new_id:
            raw_str = raw_str.replace(old_id, new_id)

    return json.loads(raw_str)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _find_file(names: list[str], filename: str) -> str | None:
    for n in names:
        if n.endswith(filename):
            return n
    return None


def _find_pipeline_json(names: list[str]) -> str | None:
    """Find the main pipeline JSON (not manifest.json)."""
    for n in names:
        if n.endswith(".json") and "manifest" not in n.lower():
            return n
    return None


def _get_resource(pipeline_json: dict) -> dict:
    """
    Supports two formats:
    1. ARM-style: { resources: [ { name, type, properties } ] }
    2. Direct:    { name, properties }
    """
    if "resources" in pipeline_json:
        resources = pipeline_json["resources"]
        if resources:
            return resources[0]
    if "properties" in pipeline_json:
        return pipeline_json
    raise ValueError("Cannot find pipeline resource in JSON — unexpected format")


def _flatten_activities(activities: list, result: list | None = None) -> list:
    """Recursively collect all activities including nested ones."""
    if result is None:
        result = []
    for act in activities:
        result.append(act)
        tp = act.get("typeProperties", {})
        # ForEach
        for child in tp.get("activities", []):
            _flatten_activities([child], result)
        # IfCondition
        for child in tp.get("ifTrueActivities", []):
            _flatten_activities([child], result)
        for child in tp.get("ifFalseActivities", []):
            _flatten_activities([child], result)
        # Switch
        for case in tp.get("cases", []):
            _flatten_activities(case.get("activities", []), result)
        for child in tp.get("defaultActivities", []):
            _flatten_activities([child], result)
    return result


def _extract_guids_by_key(obj: Any, key: str, found: set | None = None) -> set:
    """Walk the JSON tree and collect all values for a given key."""
    if found is None:
        found = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key and isinstance(v, str) and v:
                found.add(v)
            else:
                _extract_guids_by_key(v, key, found)
    elif isinstance(obj, list):
        for item in obj:
            _extract_guids_by_key(item, key, found)
    return found


def _extract_connection_ids(obj: Any, found: set | None = None) -> set:
    """
    Find connection IDs from externalReferences.connection and
    ARM-style parameter references like [parameters('some-connection-id')]
    """
    if found is None:
        found = set()
    if isinstance(obj, dict):
        # Direct externalReferences.connection
        if "externalReferences" in obj:
            conn = obj["externalReferences"].get("connection")
            if conn:
                # Strip ARM parameter wrapper if present
                clean = _strip_arm_param(conn)
                found.add(clean)
        for v in obj.values():
            _extract_connection_ids(v, found)
    elif isinstance(obj, list):
        for item in obj:
            _extract_connection_ids(item, found)
    return found


def _extract_linked_service_names(activities: list) -> set:
    """Collect all linkedService names referenced across activities."""
    names = set()

    def _walk(obj):
        if isinstance(obj, dict):
            if "linkedService" in obj and isinstance(obj["linkedService"], dict):
                name = obj["linkedService"].get("name")
                if name:
                    names.add(name)
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    for act in activities:
        _walk(act)
    return names


def _strip_arm_param(value: str) -> str:
    """Convert [parameters('some-id')] → some-id"""
    match = re.match(r"\[parameters\('(.+?)'\)\]", value)
    if match:
        return match.group(1)
    return value