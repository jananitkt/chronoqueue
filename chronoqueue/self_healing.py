"""
ChronoQueue Self-Healing DLQ Engine
Diagnoses failures, suggests root-cause fixes, provides a dry-run sandbox, and executes repaired redrives.
"""

from __future__ import annotations
import json
import re
import time
from typing import Any, Dict, List, Optional
from .models import DiagnosisReport, Message
from .storage import Storage
from .engine import QueueEngine


class SelfHealingDLQEngine:
    def __init__(self, storage: Storage, engine: QueueEngine):
        self.storage = storage
        self.engine = engine

    def diagnose_dlq(self, dlq_name: str) -> List[Dict[str, Any]]:
        """
        Scans all messages in the DLQ, clusters them by failure cause, and proposes automatic patch recipes.
        """
        messages = self.storage.list_queue_messages(dlq_name, limit=500)
        if not messages:
            return []

        clusters: Dict[str, List[Message]] = {
            "SCHEMA_MISSING_KEY": [],
            "MALFORMED_JSON": [],
            "TYPE_MISMATCH": [],
            "DOWNSTREAM_TIMEOUT": [],
            "RATE_LIMITED": [],
            "AUTH_EXPIRED": [],
            "UNKNOWN": []
        }

        detected_missing_keys: Dict[str, int] = {}
        detected_casing_keys: Dict[str, str] = {}

        for msg in messages:
            reason = (msg.dead_letter_reason or "").lower()
            err_ctx = msg.error_context or {}
            stack = str(err_ctx.get("stack_trace", "")).lower()
            combined_err = f"{reason} {stack} {err_ctx.get('error', '')}"

            # Check JSON Validity
            parsed_json = None
            try:
                parsed_json = json.loads(msg.body)
            except Exception:
                clusters["MALFORMED_JSON"].append(msg)
                continue

            # Check Missing Key / KeyError
            key_match = re.search(r"keyerror:\s*['\"]([^'\"]+)['\"]", combined_err, re.IGNORECASE)
            if key_match:
                missing_key = key_match.group(1)
                detected_missing_keys[missing_key] = detected_missing_keys.get(missing_key, 0) + 1

                # Check if there's a camelCase or snake_case variant in parsed_json
                if isinstance(parsed_json, dict):
                    for existing_k in parsed_json.keys():
                        if existing_k.replace("_", "").lower() == missing_key.replace("_", "").lower() and existing_k != missing_key:
                            detected_casing_keys[existing_k] = missing_key

                clusters["SCHEMA_MISSING_KEY"].append(msg)
                continue

            # Check Type Mismatch
            if "typeerror" in combined_err or "valueerror" in combined_err:
                clusters["TYPE_MISMATCH"].append(msg)
            elif "timeout" in combined_err or "timed out" in combined_err or "504" in combined_err:
                clusters["DOWNSTREAM_TIMEOUT"].append(msg)
            elif "429" in combined_err or "rate limit" in combined_err or "throttled" in combined_err:
                clusters["RATE_LIMITED"].append(msg)
            elif "401" in combined_err or "unauthorized" in combined_err or "token expired" in combined_err:
                clusters["AUTH_EXPIRED"].append(msg)
            else:
                clusters["UNKNOWN"].append(msg)

        reports = []
        for category, items in clusters.items():
            if not items:
                continue

            sample = items[0]
            sample_body_json = {}
            try:
                sample_body_json = json.loads(sample.body)
            except Exception:
                sample_body_json = {"raw": sample.body}

            patch_recipe: Dict[str, Any] = {}
            desc = ""
            confidence = 0.5

            if category == "SCHEMA_MISSING_KEY":
                if detected_casing_keys:
                    patch_recipe["rename_keys"] = detected_casing_keys
                    desc = f"Key naming mismatch detected (e.g., {list(detected_casing_keys.keys())[0]} -> {list(detected_casing_keys.values())[0]})."
                    confidence = 0.95
                elif detected_missing_keys:
                    top_key = max(detected_missing_keys, key=detected_missing_keys.get)
                    patch_recipe["set_defaults"] = {top_key: "default_val"}
                    desc = f"Missing required attribute '{top_key}'. Proposing default fallback injection."
                    confidence = 0.85

            elif category == "DOWNSTREAM_TIMEOUT" or category == "RATE_LIMITED":
                desc = "Transient downstream infrastructure failure. No payload patch required; safe for redrive."
                confidence = 0.90
                patch_recipe["clean_retry"] = True

            elif category == "MALFORMED_JSON":
                desc = "Invalid JSON syntax. Proposing auto-sanitization (strip BOM/trailing commas)."
                patch_recipe["sanitize_json"] = True
                confidence = 0.70

            elif category == "AUTH_EXPIRED":
                desc = "Authentication expired during invocation. Ensure tokens are refreshed prior to redrive."
                confidence = 0.85
                patch_recipe["refresh_auth_header"] = True

            else:
                desc = f"Uncategorized failure for {len(items)} message(s)."

            reports.append({
                "category": category,
                "description": desc,
                "affected_count": len(items),
                "confidence": confidence,
                "sample_message_ids": [m.message_id for m in items[:5]],
                "sample_payload": sample_body_json,
                "sample_error": sample.error_context or {"reason": sample.dead_letter_reason},
                "suggested_patch": patch_recipe
            })

        return reports

    def dry_run_patch(self, dlq_name: str, patch_recipe: Dict[str, Any], limit: int = 5) -> List[Dict[str, Any]]:
        """
        Executes a dry-run transformation on DLQ messages and returns side-by-side diff previews.
        """
        messages = self.storage.list_queue_messages(dlq_name, limit=limit)
        previews = []

        for msg in messages:
            original_body = msg.body
            patched_body, error = self._apply_patch(original_body, patch_recipe)
            previews.append({
                "message_id": msg.message_id,
                "original_body": original_body,
                "patched_body": patched_body,
                "success": error is None,
                "error": error
            })

        return previews

    def redrive_with_patch(
        self,
        dlq_name: str,
        target_queue: str,
        patch_recipe: Dict[str, Any],
        selected_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Transforms DLQ messages using the patch recipe, republishes them to target_queue,
        and marks/purges them from DLQ.
        """
        messages = self.storage.list_queue_messages(dlq_name, limit=1000)
        redriven = []
        failed = []

        for msg in messages:
            if selected_ids and msg.message_id not in selected_ids:
                continue

            patched_body, err = self._apply_patch(msg.body, patch_recipe)
            if err:
                failed.append({"message_id": msg.message_id, "error": err})
                continue

            # Publish repaired message to target queue
            new_attrs = dict(msg.message_attributes)
            new_attrs["x-chrono-redriven-from"] = {"DataType": "String", "StringValue": dlq_name}
            new_attrs["x-chrono-patch-applied"] = {"DataType": "String", "StringValue": json.dumps(patch_recipe)}

            new_msg = self.engine.send_message(
                queue_name=target_queue,
                body=patched_body,
                message_attributes=new_attrs,
                replayed_from_id=msg.message_id
            )

            # Mark old DLQ message as deleted
            msg.status = "deleted"
            self.storage.update_message(msg)

            redriven.append({
                "old_message_id": msg.message_id,
                "new_message_id": new_msg.message_id
            })

        return {
            "source_dlq": dlq_name,
            "target_queue": target_queue,
            "total_requested": len(messages) if not selected_ids else len(selected_ids),
            "successfully_redriven": len(redriven),
            "failed_count": len(failed),
            "redriven_items": redriven[:50],
            "failures": failed[:10],
            "timestamp": time.time()
        }

    def _apply_patch(self, raw_body: str, recipe: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        try:
            # Handle JSON sanitization if requested
            cleaned = raw_body.strip()
            if recipe.get("sanitize_json"):
                # Clean trailing commas
                cleaned = re.sub(r",\s*([\]}])", r"\1", cleaned)

            data = json.loads(cleaned)

            if isinstance(data, dict):
                # Apply key renaming
                for old_key, new_key in recipe.get("rename_keys", {}).items():
                    if old_key in data:
                        data[new_key] = data.pop(old_key)

                # Apply default injections
                for def_key, def_val in recipe.get("set_defaults", {}).items():
                    if def_key not in data:
                        data[def_key] = def_val

                # Apply type casting
                for cast_key, target_type in recipe.get("cast_types", {}).items():
                    if cast_key in data:
                        if target_type == "int":
                            data[cast_key] = int(data[cast_key])
                        elif target_type == "float":
                            data[cast_key] = float(data[cast_key])
                        elif target_type == "str":
                            data[cast_key] = str(data[cast_key])

            return json.dumps(data), None
        except Exception as e:
            return raw_body, str(e)
