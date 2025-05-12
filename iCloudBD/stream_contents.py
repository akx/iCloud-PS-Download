import json
import random
import time

import requests

from iCloudBD.utils import do_batch


def get_stream_contents(stream_id, mme_host="p13-sharedstreams.icloud.com"):
    """Gets available assets"""
    base_url = f"https://{mme_host}/{stream_id}/sharedstreams/"
    url = f"{base_url}webstream"
    print(f"Getting photo list from {url}...")
    r = requests.post(url, data=json.dumps({"streamCtag": None}))
    stream_data = r.json()

    if "X-Apple-MMe-Host" in stream_data:
        mme_host = stream_data["X-Apple-MMe-Host"]
        print(f"iCloud says we should try again at {mme_host}")
        return get_stream_contents(stream_id, mme_host=mme_host)

    guids = [item["photoGuid"] for item in stream_data["photos"]]
    print(f"{len(guids)} items in stream.")
    chunk = 20
    batches = list(do_batch(guids, batch_size=chunk))
    locations = {}
    items = {}
    for i, batch in enumerate(batches, 1):
        url = f"{base_url}webasseturls"
        print(f"Getting photo URLs ({int(i)}/{len(batches)})...")
        r = requests.post(url, data=json.dumps({"photoGuids": list(batch)}))
        batch_data = r.json()
        locations.update(batch_data.get("locations", {}))
        items.update(batch_data.get("items", {}))

        # Sleep for a while to avoid 509 throttling errors
        time.sleep(random.uniform(0.5, 1.2))

    return {
        "id": stream_id,
        "stream_data": stream_data,
        "locations": locations,
        "items": items,
    }


def get_stream_id(url):
    if "#" in url:
        stream_id = url.split("#").pop()
    else:
        stream_id = url
    if not stream_id.isalnum():
        raise ValueError(f"stream ID should be alphanumeric (got {stream_id})")
    return stream_id
