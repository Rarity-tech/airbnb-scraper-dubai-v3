from playwright.sync_api import sync_playwright
import csv, re, time, random, urllib.parse, os

# ========= CONFIG =========
SEARCH_URL_BASE = "https://www.airbnb.fr/s/Marina-Walk--Dubai/homes?refinement_paths%5B%5D=%2Fhomes&acp_id=10bf84a9-12d7-4468-983f-e0ab703f2501&date_picker_type=calendar&source=structured_search_input_header&search_type=autocomplete_click&flexible_trip_lengths%5B%5D=one_week&price_filter_input_type=2&price_filter_num_nights=5&channel=EXPLORE&monthly_start_date=2025-11-01&monthly_length=3&monthly_end_date=2026-02-01&place_id=ChIJX47nvlBrXz4RVW5QiQ0Xvjw&location_bb=QcixMUJcmTxByKuqQlyWeQ%3D%3D"
MAX_NEW_LISTINGS_PER_RUN = 100
TIME_LIMIT_MIN = 30
ITEMS_PER_PAGE = 20
MAX_OFFSET = 4000
SECTION_OFFSETS = [0,1,2,3,4,5]
OUTPUT_RUN = "airbnb_listings_run.csv"
OUTPUT_MASTER = "airbnb_listings_master.csv"
# =========================

RE_LICENSE_PRIMARY = re.compile(r"\b([A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6})\b", re.IGNORECASE)
RE_LICENSE_FALLBACK = re.compile(r"(?:Registration(?:\s*No\.|\s*Number)?|Permit|License|Licence|DTCM)[^\n\r]*?([A-Z0-9][A-Z0-9\-\/]{3,40})", re.IGNORECASE)
RE_HOST_RATING = re.compile(r"([0-5]\.\d{1,2})\s*(?:out of 5|·|/5|rating|reviews)", re.IGNORECASE)

def now(): return time.monotonic()
def minutes_elapsed(s, e): return (e - s) / 60.0
def pause(a=0.7, b=1.1): time.sleep(random.uniform(a, b))
def clean(s): return (s or "").replace("\xa0"," ").strip()

def build_page_url(base, items_offset, section_offset):
    p = urllib.parse.urlparse(base)
    q = dict(urllib.parse.parse_qsl(p.query, keep_blank_values=True))
    q["items_offset"] = str(items_offset); q["section_offset"] = str(section_offset)
    return urllib.parse.urlunparse(p._replace(query=urllib.parse.urlencode(q)))

def load_seen_urls(path):
    seen = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                u = (row.get("url_annonce") or "").strip()
                if u: seen.add(u)
    return seen

def extract_license(page):
    try:
        c = page.query_selector("div[data-testid='listing-permit-license-number']")
        if c:
            spans = c.query_selector_all("span")
            if spans and len(spans) >= 2:
                val = clean(spans[-1].inner_text())
                m = RE_LICENSE_PRIMARY.search(val) or RE_LICENSE_FALLBACK.search(val)
                return (m.group(1) if m else val).upper()
    except: pass
    for sel in ["div:has-text('Permit number')","div:has-text('Dubai Tourism permit number')",
                "div:has-text('Registration')","div:has-text('License')","div:has-text('Licence')",
                "div:has-text('DTCM')","section[aria-labelledby*='About this space']","div[data-section-id='DESCRIPTION_DEFAULT']"]:
        try:
            el = page.query_selector(sel)
            if el:
                txt = clean(el.inner_text())
                m = RE_LICENSE_PRIMARY.search(txt) or RE_LICENSE_FALLBACK.search(txt)
                if m: return m.group(1).upper()
        except: pass
    try:
        body = clean(page.inner_text("body"))
        m = RE_LICENSE_PRIMARY.search(body) or RE_LICENSE_FALLBACK.search(body)
        if m: return m.group(1).upper()
    except: pass
    return ""

def scrape_host_profile(context, host_url, start_time):
    note, nb_listings, joined = "", "", ""
    p = context.new_page(); p.goto(host_url, timeout=90000); pause()
    for _ in range(10):
        if minutes_elapsed(start_time, now()) >= TIME_LIMIT_MIN: break
        p.evaluate("window.scrollBy(0, document.body.scrollHeight)"); pause(0.25,0.45)
    try:
        body = p.inner_text("body"); m = RE_HOST_RATING.search(body)
        if m: note = m.group(1)
    except: pass
    try:
        cards = p.query_selector_all("a[href*='/rooms/']")
        nb_listings = str(len({(c.get_attribute('href') or '').split('?')[0] for c in cards}))
    except: pass
    try:
        j = p.query_selector("span:has-text('Joined')") or p.query_selector("div:has-text('Joined')")
        if j: joined = clean(j.inner_text())
    except: pass
    p.close(); return note, nb_listings, joined

def collect_urls_from_page(page, exclude_set):
    out, seen_local = [], set()
    try:
        page.wait_for_selector("a[href*='/rooms/']", timeout=30000)
        for a in page.query_selector_all("a[href*='/rooms/']"):
            href = (a.get_attribute("href") or "").strip()
            if not href: continue
            if href.startswith("/"): href = "https://www.airbnb.com"+href
            href = href.split("?")[0]
            if "/rooms/" in href and href not in exclude_set and href not in seen_local:
                seen_local.add(href); out.append(href)
    except: pass
    return out

if __name__ == "__main__":
    if "airbnb." not in SEARCH_URL_BASE: raise SystemExit("Mets ton URL dans SEARCH_URL_BASE.")
    start_time = now()

    seen_global = load_seen_urls(OUTPUT_MASTER)
    print(f"[init] URLs déjà en master: {len(seen_global)}")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36",
            locale="en-US",
        )
        context.route("**/*", lambda r: r.abort() if r.request.resource_type in ["image","media","font"] else r.continue_())
        page = context.new_page()

        new_urls = []
        items_offset = 0
        while (items_offset <= MAX_OFFSET 
               and len(new_urls) < MAX_NEW_LISTINGS_PER_RUN 
               and minutes_elapsed(start_time, now()) < TIME_LIMIT_MIN):
            found_here = False
            for so in [0,1,2,3,4,5]:
                if minutes_elapsed(start_time, now()) >= TIME_LIMIT_MIN: break
                url = build_page_url(SEARCH_URL_BASE, items_offset, so)
                print(f"[search] offset={items_offset} section={so}")
                try:
                    page.goto(url, timeout=120000); pause()
                    urls = collect_urls_from_page(page, exclude_set=seen_global)
                    if urls:
                        for u in urls:
                            if len(new_urls) >= MAX_NEW_LISTINGS_PER_RUN: break
                            if u not in seen_global:
                                new_urls.append(u); seen_global.add(u)
                        print(f"  +{len(urls)} candidates → nouvelles cumulées={len(new_urls)}")
                        found_here = True; break
                except Exception as e:
                    print("  (skip)", e)
            items_offset += 20
            if not found_here: print("  rien de nouveau, on avance…")

        print(f"[collect] nouvelles URLs à visiter: {len(new_urls)}")
        rows = []

        for i, url in enumerate(new_urls, 1):
            if minutes_elapsed(start_time, now()) >= TIME_LIMIT_MIN:
                print("[stop] limite de temps atteinte"); break
            try:
                page.goto(url, timeout=120000); pause()
                try:
                    btn = page.query_selector("button:has-text('Accept')") or page.query_selector("button:has-text('OK')")
                    if btn: btn.click()
                except: pass
                titre = ""
                try:
                    h1 = page.query_selector("h1"); titre = clean(h1.inner_text() if h1 else "")
                except: pass
                code = extract_license(page)
                nom_hote, host_url = "", ""
                try:
                    hl = page.query_selector("a[href*='/users/show/']")
                    if hl:
                        host_url = hl.get_attribute("href") or ""
                        if host_url.startswith("/"): host_url = "https://www.airbnb.com"+host_url
                        nom_hote = clean(hl.inner_text() or "")
                except: pass
                note, nb, joined = "", "", ""
                if host_url and minutes_elapsed(start_time, now()) < TIME_LIMIT_MIN:
                    note, nb, joined = scrape_host_profile(context, host_url, start_time)
                rows.append({
                    "url_annonce": url,
                    "titre_annonce": titre,
                    "code_licence": code,
                    "nom_hote": nom_hote,
                    "url_profil_hote": host_url,
                    "note_globale_hote": note,
                    "nb_annonces_hote": nb,
                    "date_inscription_hote": joined,
                })
                print(f"[{i}/{len(new_urls)}] {titre} | licence:{code} | note:{note}")
            except Exception as e:
                print(f"[{i}] ERREUR {url}: {e}")

        # --- ÉCRIRE TOUJOURS LE CSV DU RUN (même vide, avec en-tête) ---
        header = ["url_annonce","titre_annonce","code_licence","nom_hote","url_profil_hote","note_globale_hote","nb_annonces_hote","date_inscription_hote"]
        with open(OUTPUT_RUN, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=header); w.writeheader()
            for r in rows: w.writerow(r)
        print("[ok] CSV run écrit (peut être vide):", OUTPUT_RUN)

        # MASTER sans doublons
        master_rows = []
        if os.path.exists(OUTPUT_MASTER):
            with open(OUTPUT_MASTER, "r", encoding="utf-8-sig", newline="") as f:
                master_rows.extend(list(csv.DictReader(f)))
        existing = { (r.get("url_annonce") or "").strip() for r in master_rows }
        for r in rows:
            u = (r.get("url_annonce") or "").strip()
            if u and u not in existing:
                master_rows.append(r); existing.add(u)
        if master_rows:
            with open(OUTPUT_MASTER, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=header); w.writeheader()
                for r in master_rows: w.writerow(r)
            print("[ok] CSV master mis à jour:", OUTPUT_MASTER)

        context.close(); browser.close()
        print(f"[fin] durée: {minutes_elapsed(start_time, now()):.1f} min")
