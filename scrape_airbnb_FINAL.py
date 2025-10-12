from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import csv, re, time, random, urllib.parse, os, sys

# ========= CONFIG =========
SEARCH_URL_BASE = "https://www.airbnb.fr/s/Downtown-Dubai--Dubai--United-Arab-Emirates/homes"

MAX_NEW_LISTINGS = 100
TIME_LIMIT_MIN = 28
MAX_PAGES = 30

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
            print(f"⚠️  Erreur master: {e}")
    return urls

def create_browser(pw):
    return pw.chromium.launch(
        headless=True,
        args=[
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
        ]
    )

def create_context(browser):
    ctx = browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        locale="fr-FR",
        viewport={'width': 1920, 'height': 1080},
        extra_http_headers={
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
        }
    )
    
    ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        window.chrome = {runtime: {}};
    """)
    
    ctx.route("**/*", lambda r: (
        r.abort() if r.request.resource_type in ["image", "media"] 
        else r.continue_()
    ))
    
    return ctx

def build_url(base, offset):
    return f"{base}?items_offset={offset}&section_offset=0"

def extract_license(page):
    selectors = [
        'div[data-testid="listing-permit-license-number"] span',
        'div:has-text("Permit")',
        'div:has-text("DTCM")',
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
        
        try:
            btn = page.wait_for_selector(
                "button:has-text('Accept'), button:has-text('Accepter')", 
                timeout=3000
            )
            if btn: btn.click()
        except: pass
        
        time.sleep(1)
        
        try:
            h1 = page.wait_for_selector("h1", timeout=5000)
            if h1: listing["titre_annonce"] = h1.inner_text().strip()
        except: pass
        
        listing["code_licence"] = extract_license(page)
        
        try:
            host = page.query_selector("a[href*='/users/show/']")
            if host:
                listing["nom_hote"] = host.inner_text().strip()
                href = host.get_attribute("href") or ""
                if href.startswith("/"): href = "https://www.airbnb.com" + href
                listing["url_profil_hote"] = href
        except: pass
        
        print(f"✓ {listing['titre_annonce'][:60] or 'Sans titre'} | {listing['code_licence'] or 'N/A'}")
        
    except Exception as e:
        print(f"✗ {url}: {e}")
    finally:
        if page: page.close()
    
    return listing

def collect_urls(context, base_url, state):
    urls = []
    page = context.new_page()
    
    for page_num in range(MAX_PAGES):
        if state.should_stop(): break
        
        offset = page_num * 18
        url = build_url(base_url, offset)
        
        print(f"\n🔍 Page {page_num} (offset={offset})")
        
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=35000)
            time.sleep(2)
            
            # Méthode 1: Liens directs
            links1 = set()
            try:
                page.wait_for_selector('a[href*="/rooms/"]', timeout=10000)
                for a in page.query_selector_all('a[href*="/rooms/"]'):
                    href = a.get_attribute("href")
                    if href and '/rooms/' in href:
                        clean = href.split("?")[0]
                        if clean.startswith("/"): 
                            clean = "https://www.airbnb.fr" + clean
                        links1.add(clean)
                print(f"  Méthode 1: {len(links1)} URLs")
            except:
                print("  Méthode 1 échouée")
            
            # Méthode 2: Meta tags
            links2 = set()
            try:
                for meta in page.query_selector_all('meta[itemprop="url"]'):
                    content = meta.get_attribute("content")
                    if content and '/rooms/' in content:
                        if not content.startswith("http"):
                            content = "https://www.airbnb.fr/rooms/" + content.split("/rooms/")[-1]
                        links2.add(content.split("?")[0])
                print(f"  Méthode 2: {len(links2)} URLs")
            except:
                print("  Méthode 2 échouée")
            
            # Méthode 3: Regex HTML
            links3 = set()
            try:
                html = page.content()
                for room_id in re.findall(r'/rooms/(\d{10,20})', html):
                    links3.add(f"https://www.airbnb.fr/rooms/{room_id}")
                print(f"  Méthode 3: {len(links3)} URLs")
            except:
                print("  Méthode 3 échouée")
            
            all_links = links1 | links2 | links3
            print(f"  📊 Total: {len(all_links)} URLs")
            
            found = 0
            for clean_url in all_links:
                if clean_url not in state.seen_urls:
                    urls.append(clean_url)
                    state.seen_urls.add(clean_url)
                    found += 1
                    
                    if len(urls) >= MAX_NEW_LISTINGS:
                        break
            
            print(f"  ✅ +{found} nouvelles (collecté: {len(urls)})")
            
            if found == 0:
                print("  ⚠️  Aucune nouvelle URL, arrêt")
                break
            
            time.sleep(random.uniform(1.5, 2.5))
            
        except Exception as e:
            print(f"  ❌ Erreur: {e}")
            continue
    
    page.close()
    return urls

def save_csvs(listings):
    header = ["url_annonce", "titre_annonce", "code_licence", "nom_hote", "url_profil_hote"]
    
    with open(OUTPUT_RUN, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(listings)
    print(f"✓ {OUTPUT_RUN}: {len(listings)} listings")
    
    master = {}
    
    if os.path.exists(OUTPUT_MASTER):
        with open(OUTPUT_MASTER, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                url = row.get("url_annonce", "").strip()
                if url: master[url] = row
    
    for listing in listings:
        url = listing["url_annonce"].strip()
        if url: master[url] = listing
    
    with open(OUTPUT_MASTER, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(master.values())
    print(f"✓ {OUTPUT_MASTER}: {len(master)} total")

def main():
    state = ScraperState()
    
    master_urls = load_master_urls()
    state.seen_urls.update(master_urls)
    print(f"📚 Master: {len(master_urls)} URLs\n")
    
    try:
        with sync_playwright() as pw:
            browser = create_browser(pw)
            context = create_context(browser)
            
            print("=== PHASE 1: Collecte URLs ===")
            new_urls = collect_urls(context, SEARCH_URL_BASE, state)
            print(f"\n✓ {len(new_urls)} URLs trouvées\n")
            
            if len(new_urls) == 0:
                print("⚠️  AUCUNE URL trouvée !")
            
            print("=== PHASE 2: Scraping ===")
            for i, url in enumerate(new_urls, 1):
                if state.should_stop():
                    print(f"\n⏱️  Limite atteinte")
                    break
                
                listing = scrape_listing(url, context, state)
                state.scraped.append(listing)
                
                print(f"[{i}/{len(new_urls)}] {state.elapsed_min():.1f}min")
                time.sleep(random.uniform(0.8, 1.5))
            
            context.close()
            browser.close()
            
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n=== SAUVEGARDE ===")
    save_csvs(state.scraped)
    
    print(f"\n✅ Terminé: {state.elapsed_min():.1f}min | {len(state.scraped)} listings")
    return 0

if __name__ == "__main__":
    sys.exit(main())
