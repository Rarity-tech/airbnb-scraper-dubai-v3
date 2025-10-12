from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import csv, re, time, random, urllib.parse, os, json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from typing import Set, List, Dict
import hashlib

# ========= CONFIG =========
SEARCH_URL_BASE = "https://www.airbnb.fr/s/Marina-Walk--Dubai/homes?refinement_paths%5B%5D=%2Fhomes&acp_id=10bf84a9-12d7-4468-983f-e0ab703f2501&date_picker_type=calendar&source=structured_search_input_header&search_type=autocomplete_click&flexible_trip_lengths%5B%5D=one_week&price_filter_input_type=2&price_filter_num_nights=5&channel=EXPLORE&monthly_start_date=2025-11-01&monthly_length=3&monthly_end_date=2026-02-01&place_id=ChIJX47nvlBrXz4RVW5QiQ0Xvjw&location_bb=QcixMUJcmTxByKuqQlyWeQ%3D%3D"

MAX_NEW_LISTINGS_PER_RUN = 100
TIME_LIMIT_MIN = 28  # Marge de sécurité pour GitHub Actions
MAX_WORKERS = 3  # Parallélisation
CHECKPOINT_EVERY = 20  # Sauvegarder tous les 20 listings

OUTPUT_RUN = "airbnb_listings_run.csv"
OUTPUT_MASTER = "airbnb_listings_master.csv"
CHECKPOINT_FILE = "checkpoint.json"

# User-Agents rotatifs
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# Regex optimisées
RE_LICENSE = re.compile(r"\b([A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6})\b", re.I)
RE_RATING = re.compile(r"([0-5]\.\d{1,2})", re.I)
# =========================

@dataclass
class Listing:
    url_annonce: str
    titre_annonce: str = ""
    code_licence: str = ""
    nom_hote: str = ""
    url_profil_hote: str = ""
    note_globale_hote: str = ""
    nb_annonces_hote: str = ""
    date_inscription_hote: str = ""

class ScraperState:
    def __init__(self):
        self.start_time = time.monotonic()
        self.seen_urls: Set[str] = set()
        self.scraped_data: List[Listing] = []
        self.processed_count = 0
        
    def elapsed_min(self) -> float:
        return (time.monotonic() - self.start_time) / 60.0
    
    def should_stop(self) -> bool:
        return (self.elapsed_min() >= TIME_LIMIT_MIN or 
                len(self.scraped_data) >= MAX_NEW_LISTINGS_PER_RUN)
    
    def save_checkpoint(self):
        """Sauvegarde intermédiaire pour ne pas perdre de données"""
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'seen_urls': list(self.seen_urls),
                'scraped_data': [asdict(l) for l in self.scraped_data],
                'processed_count': self.processed_count
            }, f)
    
    def load_checkpoint(self):
        """Reprendre après interruption"""
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r') as f:
                    data = json.load(f)
                self.seen_urls = set(data['seen_urls'])
                self.scraped_data = [Listing(**l) for l in data['scraped_data']]
                self.processed_count = data['processed_count']
                print(f"[checkpoint] Reprise: {len(self.scraped_data)} listings déjà scrapés")
            except: pass

def load_master_urls(path: str) -> Set[str]:
    """Charge les URLs du master CSV"""
    urls = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                url = (row.get("url_annonce") or "").strip()
                if url: urls.add(url)
    return urls

def create_stealth_context(playwright):
    """Context avec anti-détection"""
    browser = playwright.chromium.launch(
        headless=True,
        args=[
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-setuid-sandbox',
        ]
    )
    
    context = browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        locale="en-US",
        timezone_id="America/New_York",
        viewport={'width': 1920, 'height': 1080},
        extra_http_headers={
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    )
    
    # Anti-détection JavaScript
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        window.chrome = {runtime: {}};
    """)
    
    # Bloquer ressources inutiles
    context.route("**/*", lambda route: (
        route.abort() if route.request.resource_type in ["image", "media", "font", "stylesheet"]
        else route.continue_()
    ))
    
    return browser, context

def smart_wait(page, selector: str, timeout: int = 15000):
    """Wait intelligent avec retry"""
    try:
        page.wait_for_selector(selector, timeout=timeout, state="visible")
        return True
    except:
        return False

def extract_license_fast(page) -> str:
    """Extraction licence optimisée"""
    # Sélecteurs prioritaires
    selectors = [
        "div[data-testid='listing-permit-license-number'] span:last-child",
        "div:has-text('Permit number')",
        "div:has-text('DTCM')",
    ]
    
    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if el:
                text = el.inner_text().strip()
                match = RE_LICENSE.search(text)
                if match: return match.group(1).upper()
        except: continue
    
    return ""

def scrape_single_listing(url: str, context, state: ScraperState) -> Listing:
    """Scrape une annonce (optimisé)"""
    listing = Listing(url_annonce=url)
    
    try:
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        
        # Accept cookies si nécessaire
        try:
            btn = page.wait_for_selector(
                "button:has-text('Accept'), button:has-text('OK')", 
                timeout=3000
            )
            if btn: btn.click()
        except: pass
        
        # Titre
        if smart_wait(page, "h1", timeout=5000):
            listing.titre_annonce = page.locator("h1").first.inner_text().strip()
        
        # Licence
        listing.code_licence = extract_license_fast(page)
        
        # Info hôte
        try:
            host_link = page.query_selector("a[href*='/users/show/']")
            if host_link:
                listing.nom_hote = host_link.inner_text().strip()
                listing.url_profil_hote = host_link.get_attribute("href") or ""
                if listing.url_profil_hote.startswith("/"):
                    listing.url_profil_hote = "https://www.airbnb.com" + listing.url_profil_hote
        except: pass
        
        page.close()
        print(f"✓ {listing.titre_annonce[:50]} | {listing.code_licence}")
        
    except Exception as e:
        print(f"✗ Erreur {url}: {e}")
    
    return listing

def collect_listing_urls(context, base_url: str, state: ScraperState) -> List[str]:
    """Collecte URLs des listings (optimisé)"""
    urls = []
    page = context.new_page()
    
    for offset in range(0, 500, 20):  # Limiter la profondeur
        if state.should_stop(): break
        
        search_url = build_page_url(base_url, offset, 0)
        
        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            
            if smart_wait(page, "a[href*='/rooms/']", timeout=10000):
                links = page.query_selector_all("a[href*='/rooms/']")
                
                for link in links:
                    href = link.get_attribute("href")
                    if href:
                        clean_url = href.split("?")[0]
                        if clean_url.startswith("/"): 
                            clean_url = "https://www.airbnb.com" + clean_url
                        
                        if clean_url not in state.seen_urls:
                            urls.append(clean_url)
                            state.seen_urls.add(clean_url)
                            
                            if len(urls) >= MAX_NEW_LISTINGS_PER_RUN:
                                break
                
                print(f"[collect] offset={offset} → +{len(links)} URLs | total={len(urls)}")
                time.sleep(random.uniform(0.5, 1.0))
            else:
                break  # Plus de résultats
                
        except Exception as e:
            print(f"[collect] Erreur offset {offset}: {e}")
            continue
    
    page.close()
    return urls

def build_page_url(base, items_offset, section_offset):
    p = urllib.parse.urlparse(base)
    q = dict(urllib.parse.parse_qsl(p.query, keep_blank_values=True))
    q["items_offset"] = str(items_offset)
    q["section_offset"] = str(section_offset)
    return urllib.parse.urlunparse(p._replace(query=urllib.parse.urlencode(q)))

def save_csvs(state: ScraperState):
    """Sauvegarde les CSV"""
    header = list(Listing.__annotations__.keys())
    
    # CSV du run
    with open(OUTPUT_RUN, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for listing in state.scraped_data:
            writer.writerow(asdict(listing))
    print(f"[save] {OUTPUT_RUN}: {len(state.scraped_data)} lignes")
    
    # CSV master (fusion sans doublons)
    master_data = {}
    
    # Charger l'ancien master
    if os.path.exists(OUTPUT_MASTER):
        with open(OUTPUT_MASTER, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                url = row.get("url_annonce", "").strip()
                if url: master_data[url] = row
    
    # Ajouter nouvelles données
    for listing in state.scraped_data:
        url = listing.url_annonce.strip()
        if url: master_data[url] = asdict(listing)
    
    # Écrire master
    with open(OUTPUT_MASTER, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in master_data.values():
            writer.writerow(row)
    print(f"[save] {OUTPUT_MASTER}: {len(master_data)} lignes totales")

def main():
    state = ScraperState()
    state.load_checkpoint()
    
    # Charger URLs déjà dans master
    master_urls = load_master_urls(OUTPUT_MASTER)
    state.seen_urls.update(master_urls)
    print(f"[init] URLs en master: {len(master_urls)}")
    
    with sync_playwright() as pw:
        browser, context = create_stealth_context(pw)
        
        try:
            # Phase 1: Collecter les URLs
            print("\n=== PHASE 1: Collecte des URLs ===")
            new_urls = collect_listing_urls(context, SEARCH_URL_BASE, state)
            print(f"[collect] {len(new_urls)} nouvelles URLs à scraper\n")
            
            # Phase 2: Scraper en parallèle (avec limite)
            print("=== PHASE 2: Scraping des annonces ===")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                
                for url in new_urls[:MAX_NEW_LISTINGS_PER_RUN]:
                    if state.should_stop(): break
                    
                    # Créer un nouveau context pour chaque worker
                    _, worker_ctx = create_stealth_context(pw)
                    future = executor.submit(scrape_single_listing, url, worker_ctx, state)
                    futures.append((future, worker_ctx))
                
                for i, (future, ctx) in enumerate(futures, 1):
                    if state.should_stop(): 
                        print("[stop] Limite temps/quota atteinte")
                        break
                    
                    try:
                        listing = future.result(timeout=60)
                        state.scraped_data.append(listing)
                        state.processed_count += 1
                        
                        # Checkpoint régulier
                        if state.processed_count % CHECKPOINT_EVERY == 0:
                            state.save_checkpoint()
                            print(f"[checkpoint] Sauvegarde intermédiaire: {state.processed_count}")
                        
                    except Exception as e:
                        print(f"[worker] Erreur: {e}")
                    finally:
                        ctx.close()
                    
                    print(f"[progress] {i}/{len(futures)} | temps: {state.elapsed_min():.1f}min")
            
        finally:
            context.close()
            browser.close()
    
    # Sauvegarde finale
    save_csvs(state)
    
    # Nettoyer checkpoint
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
    
    print(f"\n[✓] Terminé en {state.elapsed_min():.1f}min | {len(state.scraped_data)} listings scrapés")

if __name__ == "__main__":
    main()
