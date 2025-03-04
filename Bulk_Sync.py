#!/usr/bin/env python3
"""
Sync objects from SOURCE_BUCKET (region A) to DEST_BUCKET (region B), using server-side copy.
Auth: Uses OCI CLI profile for authentication.
"""

import os
import json
import time
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import oci

# -----------------------------
# USER CONFIGURATION
# -----------------------------
# Set to the profile name in ~/.oci/config. Defaults to 'DEFAULT'.
PROFILE_NAME = "DEFAULT"

# Source
SOURCE_NAMESPACE = ""
SOURCE_BUCKET = ""
SOURCE_REGION = ""
SOURCE_PREFIX = None

# Destination
DEST_NAMESPACE = ""
DEST_BUCKET = ""
DEST_REGION = ""
DEST_PREFIX = None

# Parallelism and retries
MAX_WORKERS = 50
MAX_RETRIES = 5

# Local JSON state file
STATE_FILE = "sync_state.json"

# -----------------------------
# OCI AUTH & CLIENTS
# -----------------------------
def create_os_client(region):
    """
    Returns an ObjectStorageClient authenticated via OCI CLI profile.
    """
    try:
        config = oci.config.from_file(profile_name=PROFILE_NAME)
        config["region"] = region  # Ensure correct region
        client = oci.object_storage.ObjectStorageClient(config)
        return client
    except Exception as e:
        print(f"ERROR creating ObjectStorageClient for {region}: {e}")
        raise

source_client = create_os_client(SOURCE_REGION)
dest_client = create_os_client(DEST_REGION)

# -----------------------------
# STATE MANAGEMENT
# -----------------------------
lock = threading.Lock()

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    tmp_file = STATE_FILE + ".tmp"
    with open(tmp_file, "w") as f:
        json.dump(state, f, indent=2)
    os.rename(tmp_file, STATE_FILE)

state = load_state()

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def object_in_destination(object_name):
    """Check if 'object_name' exists in the destination bucket."""
    dest_obj_name = (DEST_PREFIX or "") + object_name if DEST_PREFIX else object_name
    try:
        dest_client.head_object(
            namespace_name=DEST_NAMESPACE,
            bucket_name=DEST_BUCKET,
            object_name=dest_obj_name
        )
        return True  # If head_object succeeds, it exists
    except oci.exceptions.ServiceError as e:
        if e.status == 404:
            return False
        else:
            raise  # Some other error => re-raise

def copy_object_server_side(object_name):
    """Issue a server-side copy_object from source to destination."""
    dest_obj_name = (DEST_PREFIX or "") + object_name if DEST_PREFIX else object_name

    copy_details = oci.object_storage.models.CopyObjectDetails(
        source_object_name=object_name,
        destination_namespace=DEST_NAMESPACE,
        destination_bucket=DEST_BUCKET,
        destination_region=DEST_REGION,
        destination_object_name=dest_obj_name
    )

    response = source_client.copy_object(
        namespace_name=SOURCE_NAMESPACE,
        bucket_name=SOURCE_BUCKET,
        copy_object_details=copy_details
    )
    return response

def process_object(object_name):
    """
    Worker function: checks if object in dest, if not, tries copy.
    Tracks status in local 'state' dict with a lock.
    """
    with lock:
        obj_state = state.get(object_name, {})
        # If "DONE" or too many "FAILED" retries, skip
        if obj_state.get("status") == "DONE":
            return f"SKIP (already DONE): {object_name}"
        if obj_state.get("status") == "FAILED" and obj_state.get("retries", 0) >= MAX_RETRIES:
            return f"SKIP (max retries reached): {object_name}"

    # Check if object already in destination
    try:
        if object_in_destination(object_name):
            with lock:
                state[object_name] = {"status": "DONE", "retries": 0}
            return f"SKIP (dest exists): {object_name}"
    except Exception as e:
        with lock:
            obj_state = state.get(object_name, {})
            retries = obj_state.get("retries", 0) + 1
            state[object_name] = {"status": "FAILED", "retries": retries, "error": str(e)}
        return f"HEAD FAIL {object_name}: {e}"

    # Not in destination => attempt server-side copy
    for attempt in range(MAX_RETRIES):
        try:
            copy_object_server_side(object_name)
            # success => mark DONE
            with lock:
                state[object_name] = {"status": "DONE", "retries": attempt}
            return f"COPIED {object_name}"
        except Exception as e:
            err_str = f"[attempt {attempt+1}/{MAX_RETRIES}] copy failed for {object_name}: {e}"
            time.sleep(1.0 + attempt)  # simple backoff
            if attempt == MAX_RETRIES - 1:
                # final fail => mark FAILED
                with lock:
                    old_state = state.get(object_name, {})
                    retries = old_state.get("retries", 0) + 1
                    state[object_name] = {"status": "FAILED", "retries": retries, "error": str(e)}
                return f"FAILED {object_name}: {e}"

    return f"FAILED-UNKNOWN {object_name}"  # shouldn't get here

# -----------------------------
# MAIN SYNC LOGIC
# -----------------------------
def main():
    # 1) List source objects
    print(f"Listing objects in source bucket '{SOURCE_BUCKET}' (region={SOURCE_REGION}) ...")

    all_objects = []
    next_start = None
    while True:
        list_response = source_client.list_objects(
            namespace_name=SOURCE_NAMESPACE,
            bucket_name=SOURCE_BUCKET,
            prefix=SOURCE_PREFIX,
            start=next_start,
            limit=1000
        )
        for obj in list_response.data.objects:
            all_objects.append(obj.name)
        if list_response.data.next_start_with:
            next_start = list_response.data.next_start_with
        else:
            break

    print(f"Found {len(all_objects)} total objects in source. Beginning sync...")

    # 2) Parallel sync
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {}
        for obj_name in all_objects:
            future = executor.submit(process_object, obj_name)
            future_map[future] = obj_name

        try:
            for future in as_completed(future_map):
                obj_name = future_map[future]
                try:
                    result_str = future.result()
                    print(result_str)  # log each result
                except Exception as e:
                    tb = traceback.format_exc()
                    print(f"UNHANDLED ERROR for {obj_name}: {e}\n{tb}")
        finally:
            # Always save state if we break early
            with lock:
                save_state(state)

    elapsed = time.time() - start_time

    # 3) Save final state & summarize
    with lock:
        save_state(state)

    done_count = sum(1 for s in state.values() if s["status"] == "DONE")
    failed_count = sum(1 for s in state.values() if s["status"] == "FAILED")
    print(f"Sync complete in {elapsed:.1f}s. DONE={done_count}, FAILED={failed_count} (see {STATE_FILE}).")

if __name__ == "__main__":
    main()
