"""
Batch download script for wenku8.com text files.
Fully satisfies all given requirements, with anti-anti-spider enhancements.
"""

import os
import time
import logging
import threading
import random
from concurrent.futures import ThreadPoolExecutor, Future

import requests
from tqdm import tqdm

# ========== CONFIGURATION (Adjust according to network conditions) ==========
BASE_URL_TEMPLATE = "https://dl1.wenku8.com/txtutf8/{}/{}.txt"
DOWNLOAD_DIR = r"C:\Users\Downloads\bunko8-neko"       #Configure Download Directory

# ---------- Concurrency Control ----------
# Requirement 2: Download 2 files in parallel.
# If 403 errors occur frequently, set to 1 and increase REQUEST_DELAY.
MAX_WORKERS = 2          # Thread pool size = number of concurrent files

# ---------- Retry Strategy (Requirement 4) ----------
RETRY_TIMES = 5          # Max retry attempts per file
INITIAL_DELAY = 5        # Initial delay for exponential backoff (seconds)
MAX_DELAY = 60           # Maximum backoff delay (seconds)

# ---------- Anti-Anti-Spider Parameters ----------
# Random delay before each request to mimic human browsing (seconds)
REQUEST_DELAY = (1.5, 3.5)   # (min, max) – set >=1.5 to be safe
# Full browser headers to avoid being flagged as a script
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
    'Referer': 'https://www.wenku8.com/',      # Referer, checked by some sites
}
# Request timeout (connect timeout, read timeout)
TIMEOUT = (10, 30)
# ============================================================================

# Logging configuration (console + file)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
log_file = os.path.join(DOWNLOAD_DIR, "download.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Global variables
executor = None          # Thread pool (created in main())
global_pbar = None       # Global progress bar

def download_file(session, dir_num, file_id, save_dir, attempt, pending_set, lock, pending_event):
    """
    Download a single file with:
    - Browser-like headers
    - Random request delay
    - Resume support + file size check
    - Skip 404 (no retry)
    - Non-blocking exponential backoff retry
    - Retry tasks are also submitted to the same thread pool (concurrency control)
    """
    url = BASE_URL_TEMPLATE.format(dir_num, file_id)
    save_path = os.path.join(save_dir, f"{file_id}.txt")

    # ---------- Resume: skip if file exists and size > 0 ----------
    if os.path.exists(save_path):
        if os.path.getsize(save_path) > 0:
            logging.debug(f"Skipping existing file: {save_path}")
            global_pbar.update(1)
            return True
        else:
            logging.warning(f"Empty file detected, re-downloading: {save_path}")
            os.remove(save_path)

    # ---------- Random delay before request ----------
    delay = random.uniform(*REQUEST_DELAY)
    time.sleep(delay)

    try:
        resp = session.get(url, timeout=TIMEOUT)
        if resp.status_code == 200:
            with open(save_path, "wb") as f:
                f.write(resp.content)
            logging.debug(f"Success: {url} -> {save_path}")
            global_pbar.update(1)
            return True
        elif resp.status_code == 404:
            logging.info(f"File not found (404): {url}")
            global_pbar.update(1)
            return False
        else:
            logging.warning(f"HTTP {resp.status_code} - {url}")
    except Exception as e:
        logging.warning(f"Download error (attempt {attempt}/{RETRY_TIMES}): {url} - {e}")

    # ---------- Non-blocking retry with exponential backoff ----------
    if attempt < RETRY_TIMES:
        delay = min(INITIAL_DELAY * (2 ** (attempt - 1)), MAX_DELAY)
        logging.info(f"Retrying in {delay:.1f} seconds (attempt {attempt + 1}/{RETRY_TIMES}): {url}")

        def retry_task():
            """Submit retry task after delay (still limited by thread pool)"""
            fut = executor.submit(
                download_file,
                session,
                dir_num,
                file_id,
                save_dir,
                attempt + 1,
                pending_set,
                lock,
                pending_event
            )
            with lock:
                pending_set.add(fut)
            fut.add_done_callback(
                lambda f: future_done_callback(f, pending_set, lock, pending_event)
            )

        timer = threading.Timer(delay, retry_task)
        timer.daemon = True
        timer.start()
    else:
        logging.error(f"Failed after max retries: {url}")
        global_pbar.update(1)
        return False

def future_done_callback(fut, pending_set, lock, pending_event):
    """Callback when a Future completes: remove from pending set; if empty, set the event."""
    with lock:
        pending_set.discard(fut)
        if not pending_set:
            pending_event.set()

def download_directory(dir_num, id_start, id_end):
    """
    Download all files in one directory.
    - Strict directory order: wait until all tasks (including delayed retries) in this directory finish.
    - Concurrency is controlled by the global thread pool.
    """
    dir_save_path = os.path.join(DOWNLOAD_DIR, str(dir_num))
    os.makedirs(dir_save_path, exist_ok=True)

    file_ids = range(id_start, id_end + 1)
    total_files = len(file_ids)
    logging.info(f"Starting directory {dir_num}: {id_start}~{id_end}, total {total_files} files")

    # Track all pending Futures (initial tasks + retry tasks) in this directory
    pending_set = set()
    lock = threading.Lock()
    pending_event = threading.Event()

    # Use a separate Session per directory for connection reuse and custom headers
    with requests.Session() as session:
        session.headers.update(DEFAULT_HEADERS)

        # Submit initial download tasks
        for fid in file_ids:
            fut = executor.submit(
                download_file,
                session,
                dir_num,
                fid,
                dir_save_path,
                1,  # first attempt
                pending_set,
                lock,
                pending_event
            )
            with lock:
                pending_set.add(fut)
            fut.add_done_callback(
                lambda f: future_done_callback(f, pending_set, lock, pending_event)
            )

        # Wait for all tasks in this directory (including scheduled retries) to complete
        while True:
            with lock:
                if not pending_set:
                    break
            pending_event.wait(timeout=1)
            pending_event.clear()

    logging.info(f"Directory {dir_num} completed")

def main():
    global executor, global_pbar

    # Directory configuration (strict order 0→1→2→3→4)
    dirs = [
        (0, 1, 999),
        (1, 1000, 1999),
        (2, 2000, 2999),
        (3, 3000, 3999),
        (4, 4000, 4999)
    ]

    total_files = sum(end - start + 1 for _, start, end in dirs)
    logging.info(f"Total files: {total_files}, download directory: {DOWNLOAD_DIR}")
    logging.info(f"Concurrency: {MAX_WORKERS} threads, request delay: {REQUEST_DELAY}s, "
                 f"retry: {RETRY_TIMES} times, backoff: initial {INITIAL_DELAY}s, max {MAX_DELAY}s")

    # Global progress bar
    with tqdm(total=total_files, desc="Overall Progress", unit="file") as pbar:
        global_pbar = pbar

        # Create global thread pool
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
            executor = exec
            try:
                for dir_num, start, end in dirs:
                    download_directory(dir_num, start, end)
            except KeyboardInterrupt:
                logging.warning("User interrupted, waiting for current tasks to finish...")
                # Do not force exit; let the thread pool complete submitted tasks gracefully

    logging.info("All tasks finished.")

if __name__ == "__main__":
    print("=" * 60)
    print("Script started. If you encounter 403 errors, try the following:")
    print("1. Set MAX_WORKERS to 1")
    print("2. Increase REQUEST_DELAY, e.g. (3, 6)")
    print("3. Increase INITIAL_DELAY and MAX_DELAY")
    print("4. Change your IP (restart router or use proxy)")
    print("=" * 60)
    main()