import time
import random
import logging
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


class BlockedException(Exception):
    pass

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

_rate_lock      = threading.Lock()
_last_request_t = 0.0
MIN_INTERVAL    = 1.2   

def _global_rate_wait():
    global _last_request_t
    with _rate_lock:
        now     = time.time()
        elapsed = now - _last_request_t
        wait    = MIN_INTERVAL - elapsed
        if wait > 0:
            time.sleep(wait)
        _last_request_t = time.time()


class BaseScraper:
    def __init__(self, base_url: str = "https://www.vesselfinder.com", delay: float = 2.0):
        self.base_url = base_url
        self.delay    = delay
        self.session  = self._build_session()

    def _build_session(self) -> requests.Session:
        s = requests.Session()
        s.mount("https://", HTTPAdapter(max_retries=Retry(
            total=3,
            backoff_factor=2.0,           
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )))
        self._set_headers(s)
        return s

    def _set_headers(self, s=None):
        (s or self.session).headers.update({
            "User-Agent":      random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer":         "https://www.google.com/",
            "Connection":      "keep-alive",
        })

    def get_page(self, url: str, with_delay: bool = False) -> BeautifulSoup | None:
        if with_delay:
            time.sleep(self.delay + random.uniform(0.5, 1.5))

        _global_rate_wait()
        self._set_headers()
        
        try:
            r = self.session.get(url, timeout=15)
            
            # --- Fail Fast ---
            if r.status_code in [403, 429]:
                logger.error(f"BLOCKED! {r.status_code}. Failing fast.")
                raise BlockedException(f"Site blocked us with {r.status_code}")
            # ------------------------

            if r.status_code == 200:
                return BeautifulSoup(r.content, "html.parser")
                
            logger.warning(f"HTTP {r.status_code}: {url}")
            return None
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout: {url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error {url}: {e}")
            return None

    def parse_size(self, text: str) -> tuple[str | None, str | None]:
        if "/" in text:
            parts = text.replace("m", "").split("/")
            if len(parts) == 2:
                return parts[0].strip() or None, parts[1].strip() or None
        return None, None