# scrape_airbnb_v3.py
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import csv, re, time, random, urllib.parse, os, sys

# ========= CONFIG =========
SEARCH_URL_BASE = "https://www.airbnb.fr/s/Marina-Walk--Dubai/homes?refinement_paths%5B%5D=%2Fhomes&date_picker_type=calendar&source=structured_search_input_header&search_type=autocomplete_click"

MAX_NEW_LISTINGS = 100
TIME_LIMIT_MIN = 28
MAX_PAGES = 20

OUTPUT_RUN = "airbnb_listings_run.csv"
OUTPUT_MASTER = "airbnb_listings_master.csv"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

RE_LICENSE = re.compile(r"\b([A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6})\b", re.I)

class ScraperState:
    def __init__(self):
        self.start = time.monotonic()
        self.seen_urls = set()
        self.scraped = []
        
    def elapsed_min(self):
        return (time.monotonic() - self.start) / 60.0
    
    def should_stop(self):
        return self.elapsed_min() >= TIME_LIMIT_MIN or len(self.scraped) >= MAX_NEW_LISTINGS

def load_master_urls():
    urls = set()
    if os.path.exists(OUTPUT_MASTER):
        try:
            with open(OUTPUT_MASTER, "r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    u = row.get("url_annonce", "").strip()
                    if u: urls.add(u)
        except Exception as e:
            print(f"⚠️ Error loading master: {e}")
    return urls

def create_browser(pw):
    return pw.chromium.launch(
        headless=True,
        args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
    )

def create_context(browser):
    ctx = browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        locale="en-US",
        viewport={'width': 1920, 'height': 1080}
    )
    
    ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    """)
    
    ctx.route("**/*", lambda r: (
        r.abort() if r.request.resource_type in ["image", "media", "font"] 
        else r.continue_()
    ))
    
    return ctx

def build_url(base, offset):
    p = urllib.parse.urlparse(base)
    q = dict(urllib.parse.parse_qsl(p.query))
    q["items_offset"] = str(offset)
    q["section_offset"] = "0"
    return urllib.parse.urlunparse(p._replace(query=urllib.parse.urlencode(q)))

def extract_license(page):
    """Extraction rapide de la licence"""
    selectors = [
        "div[data-testid='listing-permit-license-number'] span",
        "div:has-text('Permit')",
        "div:has-text('DTCM')",
    ]
    
    for sel in selectors:
        try:
            els = page.query_selector_all(sel)
            for el in els:
                text = el.inner_text().strip()
                match = RE_LICENSE.search(text)
                if match: 
                    return match.group(1).upper()
        except: 
            continue
    return ""

def scrape_listing(url, context, state):
    """Scrape une annonce"""
    listing = {
        "url_annonce": url,
        "titre_annonce": "",
        "code_licence": "",
        "nom_hote": "",
        "url_profil_hote": ""
    }
    
    page = None
    try:
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=40000)
        
        # Accept cookies
        try:
            btn = page.wait_for_selector("button:has-text('Accept')", timeout=2000)
            if btn: btn.click()
        except: pass
        
        # Titre
        try:
            h1 = page.wait_for_selector("h1", timeout=5000)
            if h1: listing["titre_annonce"] = h1.inner_text().strip()
        except: pass
        
        # Licence
        listing["code_licence"] = extract_license(page)
        
        # Hôte
        try:
            host = page.query_selector("a[href*='/users/show/']")
            if host:
                listing["nom_hote"] = host.inner_text().strip()
                href = host.get_attribute("href") or ""
                if href.startswith("/"): href = "https://www.airbnb.com" + href
                listing["url_profil_hote"] = href
        except: pass
        
        print(f"✓ {listing['titre_annonce'][:60]} | {listing['code_licence']}")
        
    except Exception as e:
        print(f"✗ {url}: {e}")
    finally:
        if page: page.close()
    
    return listing

def collect_urls(context, base_url, state):
    """Collecte les URLs de listings"""
    urls = []
    page = context.new_page()
    
    for page_num in range(MAX_PAGES):
        if state.should_stop(): break
        
        offset = page_num * 20
        url = build_url(base_url, offset)
        
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            
            # Attendre les liens
            page.wait_for_selector("a[href*='/rooms/']", timeout=10000)
            links = page.query_selector_all("a[href*='/rooms/']")
            
            found = 0
            for link in links:
                href = link.get_attribute("href")
                if href:
                    clean_url = href.split("?")[0]
                    if clean_url.startswith("/"): 
                        clean_url = "https://www.airbnb.com" + clean_url
                    
                    if clean_url not in state.seen_urls:
                        urls.append(clean_url)
                        state.seen_urls.add(clean_url)
                        found += 1
                        
                        if len(urls) >= MAX_NEW_LISTINGS:
                            break
            
            print(f"[page {page_num}] offset={offset} → +{found} new URLs (total: {len(urls)})")
            
            if found == 0:
                print("No new listings found, stopping search")
                break
            
            time.sleep(random.uniform(0.5, 1.2))
            
        except Exception as e:
            print(f"⚠️ Error on page {page_num}: {e}")
            break
    
    page.close()
    return urls

def save_csvs(listings):
    """Sauvegarde CSV run et master"""
    header = ["url_annonce", "titre_annonce", "code_licence", "nom_hote", "url_profil_hote"]
    
    # CSV du run (toujours créé, même vide)
    with open(OUTPUT_RUN, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(listings)
    print(f"✓ {OUTPUT_RUN}: {len(listings)} listings")
    
    # Master (fusion)
    master = {}
    
    # Charger ancien master
    if os.path.exists(OUTPUT_MASTER):
        with open(OUTPUT_MASTER, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                url = row.get("url_annonce", "").strip()
                if url: master[url] = row
    
    # Ajouter nouveaux
    for listing in listings:
        url = listing["url_annonce"].strip()
        if url: master[url] = listing
    
    # Écrire master
    with open(OUTPUT_MASTER, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(master.values())
    print(f"✓ {OUTPUT_MASTER}: {len(master)} total listings")

def main():
    state = ScraperState()
    
    # Charger URLs existantes
    master_urls = load_master_urls()
    state.seen_urls.update(master_urls)
    print(f"📚 Master contains {len(master_urls)} URLs\n")
    
    try:
        with sync_playwright() as pw:
            browser = create_browser(pw)
            context = create_context(browser)
            
            # Phase 1: Collecter URLs
            print("=== PHASE 1: Collecting URLs ===")
            new_urls = collect_urls(context, SEARCH_URL_BASE, state)
            print(f"\n✓ Found {len(new_urls)} new URLs\n")
            
            # Phase 2: Scraper
            print("=== PHASE 2: Scraping listings ===")
            for i, url in enumerate(new_urls, 1):
                if state.should_stop():
                    print(f"\n⏱️ Time/quota limit reached")
                    break
                
                listing = scrape_listing(url, context, state)
                state.scraped.append(listing)
                
                print(f"[{i}/{len(new_urls)}] {state.elapsed_min():.1f}min elapsed")
            
            context.close()
            browser.close()
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    
    # Sauvegarde finale
    print(f"\n=== SAVING RESULTS ===")
    save_csvs(state.scraped)
    
    print(f"\n✅ Done in {state.elapsed_min():.1f}min | {len(state.scraped)} listings scraped")
    return 0

if __name__ == "__main__":
    sys.exit(main())
