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

# Regex pour licence Dubai (format: ABC-DEF-123456)
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
    """Charge les URLs déjà dans le master CSV"""
    urls = set()
    if os.path.exists(OUTPUT_MASTER):
        try:
            with open(OUTPUT_MASTER, "r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    u = row.get("url_annonce", "").strip()
                    if u: urls.add(u)
        except Exception as e:
            print(f"⚠️  Erreur chargement master: {e}")
    return urls

def create_browser(pw):
    """Lance Chromium avec options anti-détection"""
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
    """Context avec anti-détection renforcé"""
    ctx = browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        locale="fr-FR",
        viewport={'width': 1920, 'height': 1080},
        extra_http_headers={
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    )
    
    # Anti-détection JavaScript
    ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        Object.defineProperty(navigator, 'languages', {get: () => ['fr-FR', 'fr', 'en']});
        window.chrome = {runtime: {}};
        delete navigator.__proto__.webdriver;
    """)
    
    # Bloquer seulement images/media lourdes
    ctx.route("**/*", lambda r: (
        r.abort() if r.request.resource_type in ["image", "media"] 
        else r.continue_()
    ))
    
    return ctx

def build_url(base, offset):
    """Construit URL de pagination"""
    # Airbnb utilise cursor-based pagination
    return f"{base}?items_offset={offset}&section_offset=0"

def extract_license(page):
    """Extraction RAPIDE de la licence Dubai"""
    # Sélecteurs prioritaires basés sur le vrai HTML
    selectors = [
        'div[data-testid="listing-permit-license-number"] span',
        'div:has-text("Permit")',
        'div:has-text("License")',
        'div:has-text("DTCM")',
        'span:has-text("Registration")',
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
    """Scrape UNE annonce (optimisé)"""
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
            btn = page.wait_for_selector(
                "button:has-text('Accept'), button:has-text('OK'), button:has-text('Accepter')", 
                timeout=3000
            )
            if btn: btn.click()
        except: pass
        
        # Attendre chargement
        time.sleep(1)
        
        # Titre (plusieurs tentatives)
        try:
            # Méthode 1: h1 standard
            h1 = page.wait_for_selector("h1", timeout=5000)
            if h1: listing["titre_annonce"] = h1.inner_text().strip()
        except:
            try:
                # Méthode 2: data-testid
                title = page.query_selector("[data-testid='listing-title']")
                if title: listing["titre_annonce"] = title.inner_text().strip()
            except: pass
        
        # Licence
        listing["code_licence"] = extract_license(page)
        
        # Hôte (lien profil)
        try:
            # Chercher lien vers profil hôte
            host_selectors = [
                "a[href*='/users/show/']",
                "a[href*='/user/show/']",
                "a[aria-label*='Profil']",
            ]
            
            for sel in host_selectors:
                host = page.query_selector(sel)
                if host:
                    listing["nom_hote"] = host.inner_text().strip()
                    href = host.get_attribute("href") or ""
                    if href:
                        if href.startswith("/"): 
                            href = "https://www.airbnb.com" + href
                        listing["url_profil_hote"] = href
                        break
        except: pass
        
        print(f"✓ {listing['titre_annonce'][:60] or 'Sans titre'} | {listing['code_licence'] or 'Pas de licence'}")
        
    except Exception as e:
        print(f"✗ {url}: {e}")
    finally:
        if page: page.close()
    
    return listing

def collect_urls(context, base_url, state):
    """Collecte les URLs de listings (MÉTHODE MULTIPLE)"""
    urls = []
    page = context.new_page()
    
    for page_num in range(MAX_PAGES):
        if state.should_stop(): break
        
        offset = page_num * 18  # Airbnb utilise 18 items par page
        url = build_url(base_url, offset)
        
        print(f"\n🔍 Page {page_num} (offset={offset})")
        
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=35000)
            
            # Attendre chargement
            time.sleep(2)
            
            # === MÉTHODE 1: Liens directs ===
            links_method1 = set()
            try:
                page.wait_for_selector('a[href*="/rooms/"]', timeout=10000)
                for a in page.query_selector_all('a[href*="/rooms/"]'):
                    href = a.get_attribute("href")
                    if href and '/rooms/' in href:
                        clean_url = href.split("?")[0]
                        if clean_url.startswith("/"): 
                            clean_url = "https://www.airbnb.fr" + clean_url
                        links_method1.add(clean_url)
                print(f"  Méthode 1 (liens): {len(links_method1)} URLs")
            except:
                print("  Méthode 1 échouée")
            
            # === MÉTHODE 2: Meta tags itemprop ===
            links_method2 = set()
            try:
                metas = page.query_selector_all('meta[itemprop="url"]')
                for meta in metas:
                    content = meta.get_attribute("content")
                    if content and '/rooms/' in content:
                        if not content.startswith("http"):
                            content = "https://www.airbnb.fr/rooms/" + content.split("/rooms/")[-1]
                        links_method2.add(content.split("?")[0])
                print(f"  Méthode 2 (meta): {len(links_method2)} URLs")
            except:
                print("  Méthode 2 échouée")
            
            # === MÉTHODE 3: Regex dans HTML brut ===
            links_method3 = set()
            try:
                html = page.content()
                # Chercher pattern: /rooms/CHIFFRES
                matches = re.findall(r'/rooms/(\d{10,20})', html)
                for room_id in matches:
                    links_method3.add(f"https://www.airbnb.fr/rooms/{room_id}")
                print(f"  Méthode 3 (regex): {len(links_method3)} URLs")
            except:
                print("  Méthode 3 échouée")
            
            # === FUSION DES MÉTHODES ===
            all_links = links_method1 | links_method2 | links_method3
            print(f"  📊 TOTAL unique: {len(all_links)} URLs")
            
            # Ajouter les nouvelles URLs
            found = 0
            for clean_url in all_links:
                if clean_url not in state.seen_urls:
                    urls.append(clean_url)
                    state.seen_urls.add(clean_url)
                    found += 1
                    
                    if len(urls) >= MAX_NEW_LISTINGS:
                        break
            
            print(f"  ✅ +{found} nouvelles (total collecté: {len(urls)})")
            
            # Si aucune nouvelle URL, on arrête
            if found == 0:
                print("  ⚠️  Aucune nouvelle URL, fin de la collecte")
                break
            
            # Pause aléatoire
            time.sleep(random.uniform(1.5, 2.5))
            
        except Exception as e:
            print(f"  ❌ Erreur page {page_num}: {e}")
            continue
    
    page.close()
    return urls

def save_csvs(listings):
    """Sauvegarde CSV run et master"""
    header = ["url_annonce", "titre_annonce", "code_licence", "nom_hote", "url_profil_hote"]
    
    # CSV du run (toujours créé)
    with open(OUTPUT_RUN, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(listings)
    print(f"✓ {OUTPUT_RUN}: {len(listings)} listings")
    
    # Master (fusion)
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
    print(f"✓ {OUTPUT_MASTER}: {len(master)} total listings")

def main():
    state = ScraperState()
    
    master_urls = load_master_urls()
    state.seen_urls.update(master_urls)
    print(f"📚 Master contient {len(master_urls)} URLs\n")
    
    try:
        with sync_playwright() as pw:
            browser = create_browser(pw)
            context = create_context(browser)
            
            print("=== PHASE 1: Collecte des URLs ===")
            new_urls = collect_urls(context, SEARCH_URL_BASE, state)
            print(f"\n✓ Trouvé {len(new_urls)} nouvelles URLs\n")
            
            if len(new_urls) == 0:
                print("⚠️  AUCUNE URL trouvée ! Vérifier la connexion ou les sélecteurs.")
            
            print("=== PHASE 2: Scraping des listings ===")
            for i, url in enumerate(new_urls, 1):
                if state.should_stop():
                    print(f"\n⏱️  Limite temps/quota atteinte")
                    break
                
                listing = scrape_listing(url, context, state)
                state.scraped.append(listing)
                
                print(f"[{i}/{len(new_urls)}] {state.elapsed_min():.1f}min écoulées")
                
                # Pause entre chaque scrape
                time.sleep(random.uniform(0.8, 1.5))
            
            context.close()
            browser.close()
            
    except Exception as e:
        print(f"❌ Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n=== SAUVEGARDE ===")
    save_csvs(state.scraped)
    
    print(f"\n✅ Terminé en {state.elapsed_min():.1f}min | {len(state.scraped)} listings scrapés")
    return 0

if __name__ == "__main__":
    sys.exit(main())
