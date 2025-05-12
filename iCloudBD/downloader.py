import os.path
import time

import requests

parallel_requests_session = None


def subprocess_initializer():
    global parallel_requests_session
    parallel_requests_session = requests.session()


def download_item(item, sess=None):
    if not sess:
        sess = parallel_requests_session

    os.makedirs(os.path.dirname(item.file_name), exist_ok=True)
    if os.path.exists(item.file_name):
        print(f"Already exists: {item.file_name}")
        return False

    print(
        f"Downloading photo {item.template_namespace['photo_guid']} "
        f"derivative {item.template_namespace['derivative_id']} "
        f"to {item.file_name} ({item.derivative['fileSize']} bytes)",
    )
    r = sess.get(item.url, stream=True)
    r.raise_for_status()

    temp_name = f"{item.file_name}.tmp-{time.time()}"
    try:
        with open(temp_name, "wb") as f:
            for chunk in r.iter_content(chunk_size=512 * 1024):
                if chunk:
                    f.write(chunk)
        # Renames should be atomic
        os.rename(temp_name, item.file_name)
    finally:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
    return r


def perform_download(download_items, parallel=0):
    if parallel > 1:
        import multiprocessing

        with multiprocessing.Pool(
            processes=parallel, initializer=subprocess_initializer,
        ) as p:
            for result in p.imap_unordered(download_item, download_items, chunksize=10):
                pass
    else:
        with requests.session() as sess:
            for item in download_items:
                download_item(item=item, sess=sess)
