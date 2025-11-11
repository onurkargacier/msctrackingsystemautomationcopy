import base64
import unicodedata
import requests
import re
import os
import asyncio
from typing import Dict, Any, List
from playwright.async_api import async_playwright

# ---- eşleştirme yardımcıları ----
def normalize(s: str) -> str:
    """Diacritics removal + lowercase."""
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()

def _norm_desc(s: str) -> str:
    """Lower + diacritics kaldır + harf/rakam dışını tek boşluğa indir."""
    s = normalize(s or "")
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s

POD_ETA_ALIASES = {
    "pod eta", "pod eta date", "eta pod",
    "pod estimated time of arrival", "pod estimated arrival",
    "pod eta at pod"
}

IMPORT_TO_CONSIGNEE_ALIASES = {
    "import to consignee", "import to consignee date", "import consignee"
}

DEBUG_EVENTS = os.environ.get("DEBUG_EVENTS", "0") == "1"

# ---- ana fonksiyonlar ----
async def get_eta_etd(bl: str, browser, sem):
    """
    Döndürür:
      {
        "konşimento": BL,
        "ETA (Date)": ...,
        "Kaynak": ...,
        "ETD": ...,
        "log": [ "mesaj1", "mesaj2", ... ]
      }
    """
    async with sem:
        page = await browser.new_page()
        page.set_default_navigation_timeout(120000)
        page.set_default_timeout(15000)

        # Gereksiz medya isteklerini iptal et
        await page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2,mp4,webm}", lambda r: r.abort())

        eta = "Bilinmiyor"
        kaynak = "Bilinmiyor"
        etd = "Bilinmiyor"
        logs: List[str] = []

        try:
            # 1️⃣ Sayfaya git
            param = f"trackingNumber={bl}&trackingMode=0"
            b64 = base64.b64encode(param.encode()).decode()
            url = f"https://www.msc.com/en/track-a-shipment?params={b64}"
            await page.goto(url, wait_until="domcontentloaded")

            # 2️⃣ Cookie + token al
            cookies = await page.context.cookies()
            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
            token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")
            await page.close()

            # 3️⃣ Pagination döngüsü
            api_url = "https://www.msc.com/api/feature/tools/TrackingInfo"
            payload = {"trackingNumber": bl, "trackingMode": "0"}
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Cookie": cookie_str,
                "Origin": "https://www.msc.com",
                "Referer": url,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
                "__RequestVerificationToken": token,
            }

            all_containers = []
            page_number = 1
            while True:
                payload["pageNumber"] = page_number
                resp = requests.post(api_url, json=payload, headers=headers, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                bills = (data or {}).get("Data", {}).get("BillOfLadings", [])
                if not bills:
                    if page_number == 1:
                        logs.append("BillOfLadings boş veya gelmedi.")
                    break

                bill = bills[0]
                containers = bill.get("ContainersInfo", []) or []
                all_containers.extend(containers)

                # MSC API bazen “NextPageNumber” veya “PagingToken” döndürür
                next_page = (data or {}).get("Data", {}).get("NextPageNumber")
                if not next_page or next_page == page_number:
                    break
                page_number += 1
                await asyncio.sleep(1.0)

            if not all_containers:
                logs.append("Hiç konteyner verisi alınamadı (tüm sayfalar tarandı).")
                raise ValueError("ContainersInfo boş.")

            # --- ETD (Export Loaded on Vessel) ---
            export_events = [
                (ev or {}).get("Date")
                for c in all_containers
                for ev in (c.get("Events") or [])
                if _norm_desc(ev.get("Description")) == "export loaded on vessel"
            ]
            if export_events:
                etd = export_events[-1]
            else:
                logs.append("ETD için 'export loaded on vessel' event'i bulunamadı.")

            # --- ETA ---
            event_etas = []
            for c in all_containers:
                for ev in (c.get("Events") or []):
                    if _norm_desc(ev.get("Description")) in POD_ETA_ALIASES:
                        event_etas.append(ev.get("Date"))

            if event_etas:
                eta, kaynak = event_etas[0], "POD ETA"
            else:
                general = bills[0].get("GeneralTrackingInfo", {}) if bills else {}
                if general.get("FinalPodEtaDate"):
                    eta, kaynak = general["FinalPodEtaDate"], "Final POD ETA"
                else:
                    container_etas = [c.get("PodEtaDate") for c in all_containers if c.get("PodEtaDate")]
                    if container_etas:
                        eta, kaynak = container_etas[0], "Container POD ETA"
                    else:
                        import_events = []
                        for c in all_containers:
                            for ev in (c.get("Events") or []):
                                if _norm_desc(ev.get("Description")) in IMPORT_TO_CONSIGNEE_ALIASES:
                                    import_events.append(ev.get("Date"))
                        if import_events:
                            eta, kaynak = import_events[0], "Import to consignee"
                        else:
                            logs.append("ETA için uygun event/alan bulunamadı.")

            if DEBUG_EVENTS:
                try:
                    first = all_containers[0] if all_containers else {}
                    descs = [
                        ((ev.get("Description", "") or "").strip(), (ev.get("Date", "") or ""))
                        for ev in (first.get("Events") or [])
                    ]
                    logs.append(f"İlk konteyner event'leri (ilk 10): {descs[:10]}")
                except Exception:
                    pass

        except Exception as e:
            logs.append(f"Hata: {e}")
            print(f"[{bl}] ⚠️ Hata: {e}")

        print(f"[{bl}] → ETA: {eta} ({kaynak}), ETD: {etd}")
        return {
            "konşimento": bl,
            "ETA (Date)": eta,
            "Kaynak": kaynak,
            "ETD": etd,
            "log": logs,
        }

async def init_browser():
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True)
    return browser, pw
