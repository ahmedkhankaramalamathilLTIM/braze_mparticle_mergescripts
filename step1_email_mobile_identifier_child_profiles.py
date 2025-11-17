#!/usr/bin/env python3

import os
import csv
import json
import time
import glob
import logging
from typing import Optional, Tuple, List

import requests

# ---------------------------------------
# CONFIGURATION
# ---------------------------------------
CHUNKS_DIR = "output/childprofiles/chunk"
INPUT_GLOB = os.path.join(CHUNKS_DIR, "chunk_part*.csv") # for all files use -> os.path.join(CHUNKS_DIR, "chunk_part*.csv")

LOG_DIR = "logs"
SUMMARY_DIR = "summaries"
RESULT_SUMMARY = "results_summary.csv"

IDENTITY_BASE_URL = "https://identity.mparticle.com/v1"
EVENTS_BASE_URL = "https://s2s.mparticle.com/v2/events"

MPARTICLE_API_KEY = os.getenv("MPARTICLE_API_KEY", "") # set STG, PROD Keys here
MPARTICLE_API_SECRET = os.getenv("MPARTICLE_API_SECRET", "") # set STG,PROD Secret here

ENVIRONMENT = "production" #change production for production run
DRY_RUN = False # change it to False for Real time run
REQUEST_TIMEOUT = 20
RATE_LIMIT_DELAY = 0.2
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0

EMAIL_HEADER = "Email_Address"
MPID_HEADER = "External_ID"
PHONE_HEADER = "Phone_Number"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(SUMMARY_DIR, exist_ok=True)

# ---------------------------------------
# Logging
# ---------------------------------------
master_log = logging.getLogger("master")
master_log.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")

fh = logging.FileHandler(os.path.join(LOG_DIR, "mparticle_process.log"))
fh.setFormatter(fmt)
master_log.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(fmt)
master_log.addHandler(ch)


# ---------------------------------------
# Helpers
# ---------------------------------------
def make_auth():
    if MPARTICLE_API_KEY and MPARTICLE_API_SECRET:
        return (MPARTICLE_API_KEY, MPARTICLE_API_SECRET)
    return None


def retryable_post(url: str, auth, json_payload: dict, dry_run=False):
    if dry_run:
        return None, 0, None

    backoff = INITIAL_BACKOFF
    attempt = 0
    last_err = None

    while attempt < MAX_RETRIES:
        attempt += 1
        try:
            resp = requests.post(url, auth=auth, json=json_payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 200 and resp.status_code < 300:
                return resp, attempt, None
            elif resp.status_code == 400:
                if(resp.json().get("Errors", [{}])[0].get("message") == "MpId doesn't exist"):
                    json_payload["environment"] = "development"
                    resp, attempt, last_err = retryable_post(url, auth, json_payload, dry_run)
                elif(resp.json().get("Errors", [{}])[0].get("message") == "ToModifyIdentities is empty."):
                   for ident in json_payload["identity_changes"]:
                    if ident["identity_type"] == "mobile_number":
                        ident["old_value"] = None
                    resp, attempt, last_err = retryable_post(url, auth, json_payload, dry_run)
               

            last_err = f"Status {resp.status_code}: {resp.text}"

            if resp.status_code >= 500 or resp.status_code == 429:
                time.sleep(backoff)
                backoff *= 2
                continue

            return resp, attempt, last_err

        except requests.exceptions.RequestException as e:
            last_err = str(e)
            time.sleep(backoff)
            backoff *= 2

    return None, attempt, last_err

def build_modify_payload(email, phone, environment):
    # Normalize blanks → None
    email = email.strip() if email and email.strip() != "" else None
    phone = phone.strip() if phone and phone.strip() != "" else None
    changes = []
    if email is None and phone is None:
        return {
            "environment": environment,
            "identity_changes": []
        }
    # Add email only if not None
    if email is not None:
        changes.append({
            "old_value": email,
            "new_value": None,
            "identity_type": "email"
        })

    # Add phone only if not None
    if phone is not None:
        changes.append({
            "old_value": phone,
            "new_value": None,
            "identity_type": "mobile_number"
        })

    return {
        "environment": environment,
        "identity_changes": changes
    }


def build_events_payload(mpid):
    return {
        "mpid": mpid,
        "schema_version": 2,
        "environment": ENVIRONMENT,
        "events": [
            {
                "event_type": "custom_event",
                "data": {
                    "event_name": "ProfileToKeep",
                    "custom_event_type": "other",
                    "user_attribute_name": "ProfileToKeep",
                    "new": "false",
                    "is_new_attribute": "false"
                }
            }
        ],
        "user_attributes": {
            "ProfileToKeep": "false",
            "$mobile": None
        },
        "user_identities": {
            "email": None,
            "mobile_number": None
        }
    }


# ---------------------------------------
# Process single file SEQUENTIALLY
# ---------------------------------------
def process_file(file_path: str, dry_run=True, rate_limit=RATE_LIMIT_DELAY) -> str:
    filename = os.path.basename(file_path)
    filestem = os.path.splitext(filename)[0]

    log_path = os.path.join(LOG_DIR, f"{filestem}.log")
    summary_path = os.path.join(SUMMARY_DIR, f"{filestem}_summary.csv")

    logger = logging.getLogger(f"log_{filestem}")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh2 = logging.FileHandler(log_path)
        fh2.setFormatter(fmt)
        logger.addHandler(fh2)

    logger.info(f"START file: {file_path}, dry_run={dry_run}")

    auth = make_auth()
    results = []

    with open(file_path, newline="",encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        row_num = 0

        for row in reader:
            row_num += 1
            email = row.get(EMAIL_HEADER, "").strip() or None
            mpid = row.get(MPID_HEADER, "").strip() or None
            phone = row.get(PHONE_HEADER, "").strip() or None

            if not mpid:
                msg = "SKIPPED: Missing MPID"
                logger.warning(f"Row {row_num}: {msg}")
                results.append({
                    "mpid": "",
                    "email": email or "",
                    "phone": phone or "",
                    "modify_status": "skipped",
                    "events_status": "skipped",
                    "retries": 0,
                    "message": msg
                })
                continue

            logger.info(f"Row {row_num} → MPID={mpid}, email={email}, phone={phone}")

            # ---------------------------
            # Step 1 — Modify API
            # ---------------------------
            modify_url = f"{IDENTITY_BASE_URL}/{mpid}/modify"
            modify_payload = build_modify_payload(email, phone, ENVIRONMENT)

            if dry_run:
                logger.info(f"[DRY-RUN] Modify → {modify_payload}")
                modify_status = "dry-run"
                modify_attempts = 0
                modify_err = None
            else:
                resp, attempts, err = retryable_post(modify_url, auth, modify_payload)
                modify_attempts = attempts
                modify_err = err

                if resp is not None:
                    modify_status = str(resp.status_code)
                else:
                    modify_status = "failed"

            # ---------------------------
            # Step 2 — Events API (only if Modify succeeded)
            # ---------------------------
            events_status = ""
            events_attempts = 0
            events_err = None

            if dry_run:
                logger.info(f"[DRY-RUN] Events → MPID={mpid}")
                events_status = "dry-run"
            else:
                if str(modify_status).startswith("2"):  # modify success
                    events_payload = build_events_payload(mpid)
                    resp2, attempts2, err2 = retryable_post(EVENTS_BASE_URL, auth, events_payload)
                    events_attempts = attempts2
                    events_err = err2

                    if resp2 is not None:
                        events_status = str(resp2.status_code)
                    else:
                        events_status = "failed"
                else:
                    events_status = "skipped_modify_failed"

            # Collect results
            results.append({
                "mpid": mpid,
                "email": email or "",
                "phone": phone or "",
                "modify_status": modify_status,
                "events_status": events_status,
                "retries": modify_attempts + events_attempts,
                "message": (modify_err or events_err or "")
            })

            time.sleep(rate_limit)

    # Write per-file summary
    with open(summary_path, "w", newline="", encoding="utf-8") as sf:
        writer = csv.DictWriter(sf, fieldnames=["mpid","email","phone","modify_status","events_status","retries","message"])
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    logger.info(f"END file: {file_path}")
    return summary_path


# ---------------------------------------
# Combine summaries
# ---------------------------------------
def combine_summaries(files: List[str]):
    combined = []
    for f in files:
        with open(f, newline="", encoding="utf-8") as sf:
            reader = csv.DictReader(sf)
            combined.extend(reader)

    if not combined:
        master_log.info("No summary rows to combine.")
        return

    with open(RESULT_SUMMARY, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=combined[0].keys())
        writer.writeheader()
        for r in combined:
            writer.writerow(r)

    master_log.info(f"Final summary written to {RESULT_SUMMARY}")


# ---------------------------------------
# MAIN (sequential)
# ---------------------------------------
def main():
  
    dry_run = DRY_RUN
    rate_limit = RATE_LIMIT_DELAY

    master_log.info("=== START STEP1 For EMAIL/Mobile IDENTIFIER  CHILD PROFILES RUN (NO PARALLEL) ===")
    master_log.info(f"Chunks dir: {CHUNKS_DIR}")
    master_log.info(f"dry_run={dry_run}")

    files = sorted(glob.glob(INPUT_GLOB))
    if not files:
        master_log.error(f"No files found in {CHUNKS_DIR}")
        return

    summary_files = []

    for f in files:
        master_log.info(f"Processing: {f}")
        summary_path = process_file(f, dry_run=dry_run, rate_limit=rate_limit)
        summary_files.append(summary_path)

    combine_summaries(summary_files)
    master_log.info("=== STEP1 For EMAIL/Mobile IDENTIFIER  CHILD PROFILES RUN COMPLETE ===")


if __name__ == "__main__":
    main()
