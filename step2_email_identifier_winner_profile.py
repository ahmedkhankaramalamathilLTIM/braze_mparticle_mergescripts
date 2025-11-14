import csv
import json
import requests
import time
import logging
import os
from glob import glob

# Configuration
BULK_API_URL = "https://s2s.mparticle.com/v2/bulkevents"
API_KEY = os.getenv("MPARTICLE_STG_API_KEY")
API_SECRET = os.getenv("MPARTICLE_STG_API_SECRET")
BATCH_SIZE = 100
ENVIRONMENT = "development" # "development" or "production"
INPUT_DIR = "output"  # directory with chunked CSV files
LOG_FILE = "logs_bulkevents_winnerprofile.csv"
DRY_RUN = True  #Set to False to send real requests

EMAIL_HEADER = "Email_Address"
MPID_HEADER = "External_ID"
PHONE_HEADER = "Phone_Number"

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_csv(input_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def chunkify(data, size):
    for i in range(0, len(data), size):
        yield data[i:i + size]


def write_log(log_data):
    file_exists = os.path.isfile(LOG_FILE)
    with open(LOG_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["mpid", "email", "Status Code", "Response"])
        writer.writerows(log_data)


def send_batch(batch, retries=3, backoff=1):
    events_payload = []
    log_rows = []

    for row in batch:
        mpid = row.get(MPID_HEADER)
        email = row.get(EMAIL_HEADER)

        user_event = {
            "events": [
                {
                    "event_type": "custom_event",
                    "data": {
                        "event_name": "ProfileToKeep",
                        "custom_event_type": "other",
                        "user_attribute_name": "ProfileToKeep",
                        "email": email,
                        "new": "true",
                        "is_new_attribute": "true"
                    }
                }
            ],
            "user_attributes": {
                "ProfileToKeep": "true",
                "email":email
            },
            "user_identities": {
                "email": email
            },
            "mpid": mpid,
            "schema_version": 2,
            "environment": ENVIRONMENT
        }
        events_payload.append(user_event)

    if DRY_RUN:
        logging.info(f"[DRY RUN] Would send batch of {len(events_payload)} events.")
        for row in batch:
            log_rows.append([row.get(MPID_HEADER), row.get(EMAIL_HEADER), "DRY_RUN", "Simulated"])
        return log_rows

    headers = {
        "Content-Type": "application/json"
    }

    for attempt in range(retries):
        try:
            response = requests.post(
                BULK_API_URL,
                headers=headers,
                auth=(API_KEY, API_SECRET),
                json=events_payload
            )
            status = response.status_code
            success = status == 202
            for row in batch:
                log_rows.append([row.get(MPID_HEADER), row.get(EMAIL_HEADER), status, "Success" if success else response.text])
            logging.info(f"Batch sent: {len(batch)} users, Status: {status}")
            return log_rows
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
            else:
                logging.error(f"Batch failed permanently after {retries} attempts: {str(e)}")
                for row in batch:
                    log_rows.append([row.get(MPID_HEADER), row.get(EMAIL_HEADER), "Error", str(e)])
                return log_rows


def main():
    chunk_files = sorted(glob(os.path.join(INPUT_DIR, "*.csv")))
    logging.info(f"Found {len(chunk_files)} chunk files to process.")

    for file_path in chunk_files:
        logging.info(f"Processing file: {file_path}")
        try:
            all_data = read_csv(file_path)
            logging.info(f"  Records found: {len(all_data)}")
            for batch in chunkify(all_data, BATCH_SIZE):
                logs = send_batch(batch)
                write_log(logs)
                time.sleep(0.5)
        except Exception as e:
            logging.error(f"Failed to process file {file_path}: {str(e)}")


if __name__ == "__main__":
    main()
