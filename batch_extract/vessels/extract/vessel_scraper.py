import re
import time
import random
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from bs4 import BeautifulSoup
from base_scraper import BaseScraper

logger = logging.getLogger(__name__)

VESSEL_TYPES = {"4": "Cargo", "6": "Tanker"}

OUTPUT_COLUMNS = [
    "name", "type", "year_built", "gross_tonnage", "deadweight",
    "length(m)", "beam(m)", "detail_link",
    "departure_date", "last_port_country", "last_port_name",
    "arrival_date", "destination_port_country", "destination_port_name",
    "destination_port_lat", "destination_port_lon",
    "reported_status", "report_date",
]


class VesselFinderScraper(BaseScraper):

    BASE_URL = "https://www.vesselfinder.com"

    def __init__(self, delay: float = 2.0, test_mode: bool = False,
                 test_limit: int = 5, max_workers: int = 3):
        super().__init__(self.BASE_URL, delay)
        self.test_mode   = test_mode
        self.test_limit  = test_limit
        self.max_workers = max_workers
        self._ports_cache: dict = {}
        self._cache_lock  = threading.Lock()

    # ── Pagination ────────────────────────────────────────────────────────────
    def _total_pages(self, soup: BeautifulSoup) -> int:
        nav = soup.find("nav", class_="pagination")
        if nav:
            span = nav.find("span")
            if span:
                try:
                    return int(span.text.strip().split("/")[-1])
                except (ValueError, IndexError):
                    pass
        m = re.search(r"[Pp]age\s*1\s*[/of]+\s*(\d+)", soup.get_text())
        if m:
            return int(m.group(1))
        return 1

    # ── List page ─────────────────────────────────────────────────────────────
    def _parse_list_page(self, soup: BeautifulSoup, vessel_type: str) -> list[dict]:
        table = soup.find("table")
        if not table:
            logger.warning("No <table> on list page — site structure may have changed")
            return []

        vessels = []
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
            link_tag = cells[0].find("a", href=True)
            if not link_tag:
                continue

            href  = link_tag["href"]
            url   = href if href.startswith("http") else self.BASE_URL + href
            parts = [p.strip() for p in cells[0].get_text(separator="\n").split("\n") if p.strip()]
            length, beam = self.parse_size(cells[4].get_text().strip())

            vessels.append({
                "name":          parts[0] if parts else None,
                "type":          parts[1] if len(parts) > 1 else vessel_type,
                "year_built":    cells[1].get_text().strip() or None,
                "gross_tonnage": cells[2].get_text().strip() or None,
                "deadweight":    cells[3].get_text().strip() or None,
                "length(m)":     length,
                "beam(m)":       beam,
                "detail_link":   url,
            })
        return vessels

    # ── Detail page (runs in thread) ──────────────────────────────────────────
    def _scrape_detail(self, vessel: dict) -> dict:
    
        soup = self.get_page(vessel["detail_link"], with_delay=False)
        if not soup:
            return {**vessel, **self._empty_details()}
        try:
            return {
                **vessel,
                **self._parse_last_port(soup),
                **self._parse_destination(soup),
                **self._parse_status(soup),
                "report_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            }
        except Exception as e:
            logger.error(f"Parse error {vessel['detail_link']}: {e}")
            return {**vessel, **self._empty_details()}

    # ── Last port — RAW text ──────────────────────────────────────────────────
    def _parse_last_port(self, soup: BeautifulSoup) -> dict:
        div = soup.find("div", class_="vi__r1 vi__stp")
        if not div:
            return {"last_port_name": None, "last_port_country": None, "departure_date": None}
        name, country = self._port_name_country(div)
        val = div.find("div", class_="_value")
        return {
            "last_port_name":    name,
            "last_port_country": country,
            "departure_date":    val.get_text(strip=True) if val else None,  # RAW
        }

    # ── Destination — RAW text + RAW coordinates ──────────────────────────────
    def _parse_destination(self, soup: BeautifulSoup) -> dict:
        div = soup.find("div", class_="vi__r1 vi__sbt")
        empty = {
            "destination_port_name": None, "destination_port_country": None,
            "destination_port_lat":  None, "destination_port_lon":     None,
            "arrival_date":          None,
        }
        if not div:
            return empty

        name, country = self._port_name_country(div)
        val  = div.find("div", class_="_value")
        lat, lon = None, None

        link = div.find("a", class_="_npNa")
        if link and link.get("href"):
            ep = link["href"]
            with self._cache_lock:
                cached = self._ports_cache.get(ep)
            if cached:
                lat, lon = cached
            else:
                lat, lon = self._get_port_coords(ep)
                with self._cache_lock:
                    self._ports_cache[ep] = (lat, lon)

        return {
            "destination_port_name":    name,
            "destination_port_country": country,
            "destination_port_lat":     lat,   # RAW: "49.29N"
            "destination_port_lon":     lon,   # RAW: "123.10W"
            "arrival_date":             val.get_text(strip=True) if val else None,  # RAW
        }

    # ── Status ────────────────────────────────────────────────────────────────
    def _parse_status(self, soup: BeautifulSoup) -> dict:
        tbl = soup.find("table")
        if not tbl:
            return {"reported_status": None}
        m = re.search(
            r"Navigation Status\n(.*)\n",
            tbl.get_text(separator="\n", strip=True),
        )
        return {"reported_status": m.group(1).strip() if m else None}

    # ── Port helpers ──────────────────────────────────────────────────────────
    def _port_name_country(self, div) -> tuple[str | None, str | None]:
        link = div.find("a", class_="_npNa")
        if link:
            parts = [p.strip() for p in link.get_text().split(",")]
            return parts[0] or None, (parts[1] if len(parts) > 1 else None)
        return None, None

    def _get_port_coords(self, endpoint: str) -> tuple[str | None, str | None]:
        soup = self.get_page(f"{self.BASE_URL}{endpoint}", with_delay=False)
        if not soup:
            return None, None
        p = soup.find("p", class_="text1")
        if not p:
            return None, None
        parts = p.get_text().strip().split("\n")[0].split(",")
        if len(parts) >= 2:
            return parts[0].split()[-1].strip(), parts[-1].strip().rstrip(".")
        return None, None

    def _empty_details(self) -> dict:
        return {
            "last_port_name": None,           "last_port_country": None,
            "departure_date": None,           "destination_port_name": None,
            "destination_port_country": None, "destination_port_lat": None,
            "destination_port_lon": None,     "arrival_date": None,
            "reported_status": None,
            "report_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }

    # ── Main collect ──────────────────────────────────────────────────────────
    def collect(self) -> list[dict]:
        basic: list[dict] = []

        # Phase 1: 
        for v_type, v_name in VESSEL_TYPES.items():
            logger.info(f"{'='*55}")
            logger.info(f"  Type: {v_name}  (type={v_type})")
            logger.info(f"{'='*55}")

            first = self.get_page(
                f"{self.BASE_URL}/vessels?type={v_type}&flag=EG&page=1",
                with_delay=False,
            )
            if not first:
                logger.error(f"Cannot load list page for {v_name} — skipping")
                continue

            pages = self._total_pages(first)
            logger.info(f"  Pages: {pages}")

            for pg in range(1, pages + 1):
                soup = first if pg == 1 else self.get_page(
                    f"{self.BASE_URL}/vessels?type={v_type}&flag=EG&page={pg}",
                    with_delay=True,
                )
                if not soup:
                    logger.warning(f"  Skip page {pg}")
                    continue

                found = self._parse_list_page(soup, v_name)
                logger.info(f"  Page {pg}/{pages} → {len(found)} vessels")
                basic.extend(found)

                if self.test_mode and len(basic) >= self.test_limit:
                    basic = basic[:self.test_limit]
                    logger.info(f"  TEST MODE: capped at {self.test_limit}")
                    break

            if self.test_mode and len(basic) >= self.test_limit:
                break

        logger.info(f"\nPhase 1 done → {len(basic)} vessels")
        if not basic:
            return []

        # Phase 2:3
        total    = len(basic)
        est_mins = (total * 1.2) / 60
        logger.info(f"Phase 2: {total} vessels | {self.max_workers} threads | ~{est_mins:.1f} min estimated")
        results  = [None] * total
        done_cnt = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(self._scrape_detail, v): i for i, v in enumerate(basic)}
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    logger.error(f"Thread error idx={idx}: {e}")
                    results[idx] = {**basic[idx], **self._empty_details()}
                done_cnt += 1
                if done_cnt % 5 == 0 or done_cnt == total:
                    pct = done_cnt / total * 100
                    logger.info(f"  [{done_cnt:>3}/{total}]  {pct:.0f}%")

        final = [r for r in results if r is not None]
        logger.info(f"Phase 2 done → {len(final)} vessels with full details")
        return final