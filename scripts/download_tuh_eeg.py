import os, time
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urljoin, urlparse
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

"""
Recursively crawl a directory listing page and download all files
while reproducing the directory tree locally.

Configuration: edit the variables in the CONFIG section below.
Authentication: HTTP Basic only.

Skips files that have already been downloaded; downloads can be cancelled
halfway and then restarted later, resuming from where it was cancelled off.
"""


# CONFIGURATION 
load_dotenv()
URL = os.getenv("URL")
USERNAME = os.getenv("USRNAME")
PASSWORD = os.getenv("PSSWORD")

DELAY = 0.1
SKIP_EXISTING = True
OUTPUT_DIR = "./../TUH-EEG"
DEFAULT_CHUNK = 1024 * 64


def make_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=(500,502,503,504))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))

    if USERNAME and PASSWORD:
        s.auth = (USERNAME, PASSWORD)
    return s

def sanitize_url(u):
    p = urlparse(u)
    return p._replace(query='', fragment='').geturl()

def rel_path_from_root(root_url, target_url):
    root_path = urlparse(root_url).path
    target_path = urlparse(target_url).path
    if not root_path.endswith('/'):
        root_path = root_path + '/'
    if target_path.startswith(root_path):
        rel = target_path[len(root_path):]
    else:
        rel = urlparse(target_url).netloc + target_path
    return rel.lstrip('/')

def download_file(session, file_url, local_path):
    try:
        # skip if exists and same size
        if os.path.exists(local_path) and SKIP_EXISTING:
            try:
                head = session.head(file_url, allow_redirects=True, timeout=30)
                if head.status_code == 200 and 'Content-Length' in head.headers:
                    expected = int(head.headers.get('Content-Length', -1))
                    if expected >= 0 and os.path.getsize(local_path) == expected:
                        print(f"SKIP (exists): {local_path}")
                        return
            except Exception:
                pass

        with session.get(file_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            tmp = local_path + ".part"
            dirpath = os.path.dirname(local_path) or OUTPUT_DIR
            os.makedirs(dirpath, exist_ok=True)
            # write stream to temporary file
            with open(tmp, 'wb') as f:
                for chunk in r.iter_content(chunk_size=DEFAULT_CHUNK):
                    if chunk:
                        f.write(chunk)
            os.replace(tmp, local_path)
            print(f"DOWNLOADED: {local_path}")
    except Exception as e:
        print(f"ERROR downloading {file_url} -> {e}")

def crawl_index(session, root_url, current_url, output_dir, visited):
    current_url = sanitize_url(current_url)
    if current_url in visited:
        return
    visited.add(current_url)
    try:
        r = session.get(current_url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"ERROR fetching {current_url} -> {e}")
        return

    soup = BeautifulSoup(r.text, "html.parser")
    links = soup.find_all("a")
    for a in links:
        href = a.get('href')
        if not href:
            continue
        text = (a.text or "").strip()
        # skip parent links
        if text.lower().startswith('parent') or href in ('../', '/'):
            continue
        # ignore anchors/mailto/javascript
        if href.startswith('#') or href.startswith('mailto:') or href.lower().startswith('javascript:'):
            continue

        full = urljoin(current_url, href)
        full = sanitize_url(full)

        # decide if directory
        if href.endswith('/') or full.endswith('/'):
            rel = rel_path_from_root(root_url, full)
            local_dir = os.path.join(output_dir, rel)
            os.makedirs(local_dir, exist_ok=True)
            time.sleep(DELAY)
            crawl_index(session, root_url, full, output_dir, visited)
        else:
            rel = rel_path_from_root(root_url, full)
            local_file = os.path.join(output_dir, rel)
            if os.path.isdir(local_file):
                local_file = local_file + ".file"
            download_file(session, full, local_file)
            time.sleep(DELAY)

def main():
    root = URL if URL.endswith('/') else URL + '/'
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    session = make_session()
    visited = set()
    crawl_index(session, root, root, OUTPUT_DIR, visited)

if __name__ == "__main__":
    main()
