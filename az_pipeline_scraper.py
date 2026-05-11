
"""
CEREBRO — Arizona Statewide Pipeline Scraper + API
Empire Housing Solutions — empiresolutions520@gmail.com
Railway.app Deployment

FILTERS: Residential homes, land, single family only
EXCLUDES: Commercial, industrial, HOA, condo complexes, retail
"""

import os, re, time, hashlib, sqlite3, smtplib, schedule, threading, requests
from datetime import datetime
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, jsonify, request
from flask_cors import CORS

GMAIL_USER  = os.environ.get('GMAIL_USER',  '')
GMAIL_PASS  = os.environ.get('GMAIL_PASS',  '')
ALERT_EMAIL = os.environ.get('ALERT_EMAIL', 'empiresolutions520@gmail.com')
SECRET_KEY  = os.environ.get('SECRET_KEY',  'empire2026')
DB_PATH     = os.environ.get('DB_PATH',     'az_pipeline.db')
PORT        = int(os.environ.get('PORT',    '8080'))
SCAN_HOURS  = int(os.environ.get('SCAN_HOURS', '6'))

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

COMMERCIAL_EXCLUDE = ['llc','inc','corp','corporation','commercial','industrial',
    'warehouse','office','retail','plaza','center','mall','hotel','motel',
    'restaurant','store','shop','suite','medical','dental','church','school',
    'storage','parking','airport','business park','hoa','association']
ADDRESS_PATTERN = re.compile(r'^\d+\s+\w')

def is_residential(address, notes=''):
    if not address or len(address) < 5: return False
    addr_lower = address.lower()
    combined = addr_lower + ' ' + (notes or '').lower()
    if not ADDRESS_PATTERN.match(address.strip()): return False
    for kw in COMMERCIAL_EXCLUDE:
        if kw in combined: return False
    has_street = any(
        f' {kw} ' in addr_lower or addr_lower.endswith(f' {kw}') or f' {kw},' in addr_lower
        for kw in ['st','ave','rd','blvd','dr','ln','way','ct','pl','cir','loop','trail','ter','place','pkwy']
    )
    is_land = any(kw in combined for kw in ['vacant','land','lot','parcel','acr'])
    return has_street or is_land

app = Flask(__name__)
CORS(app)
status = {'running': False, 'last_run': None, 'new_last': 0, 'total': 0, 'log': []}

def log(msg, lv='info'):
    ts = datetime.now().strftime('%H:%M:%S')
    status['log'].append({'ts': ts, 'msg': msg, 'lv': lv})
    status['log'] = status['log'][-50:]
    print(f'[{ts}] {msg}')

def auth(req): return req.args.get('key', '') == SECRET_KEY

@app.route('/')
def index():
    return {'service': 'CEREBRO AZ Pipeline', 'status': 'online',
            'filter': 'Residential+Land only', 'total_leads': status['total'], 'last_run': status['last_run']}

@app.route('/health')
def health(): return {'status': 'ok', 'time': datetime.now().isoformat()}

@app.route('/leads')
def get_leads():
    if not auth(request): return {'error': 'Unauthorized'}, 401
    county = request.args.get('county', '')
    typ = request.args.get('type', '')
    lim = int(request.args.get('limit', 500))
    conn = init_db(); c = conn.cursor()
    q = 'SELECT * FROM leads WHERE residential=1'; p = []
    if county: q += ' AND county=?'; p.append(county)
    if typ: q += ' AND type=?'; p.append(typ)
    q += ' ORDER BY filed_date DESC LIMIT ?'; p.append(lim)
    rows = c.execute(q, p).fetchall()
    cols = [d[0] for d in c.description]
    leads = [dict(zip(cols, r)) for r in rows]
    conn.close()
    return {'leads': leads, 'total': len(leads), 'updated': status['last_run'], 'filter': 'residential+land'}

@app.route('/stats')
def get_stats():
    if not auth(request): return {'error': 'Unauthorized'}, 401
    conn = init_db(); c = conn.cursor()
    def cnt(q, p=[]): return c.execute(q, p).fetchone()[0]
    by_county = dict(c.execute('SELECT county, COUNT(*) FROM leads WHERE residential=1 GROUP BY county').fetchall())
    result = {
        'total': cnt('SELECT COUNT(*) FROM leads WHERE residential=1'),
        'trustee': cnt("SELECT COUNT(*) FROM leads WHERE type='trustee' AND residential=1"),
        'probate': cnt("SELECT COUNT(*) FROM leads WHERE type='probate' AND residential=1"),
        'taxd': cnt("SELECT COUNT(*) FROM leads WHERE type='taxdelinquent' AND residential=1"),
        'hot': cnt('SELECT COUNT(*) FROM leads WHERE score>=80 AND residential=1'),
        'by_county': by_county, 'last_run': status['last_run'],
        'running': status['running'], 'log': status['log'][-10:],
        'filter': 'Residential+Land only'
    }
    conn.close(); return result

@app.route('/trigger', methods=['POST','GET'])
def trigger():
    if not auth(request): return {'error': 'Unauthorized'}, 401
    if status['running']: return {'status': 'already_running'}
    threading.Thread(target=run_scrape, daemon=True).start()
    return {'status': 'started', 'message': 'Scrape triggered successfully'}

@app.route('/lead/<int:lid>', methods=['PATCH'])
def update_lead(lid):
    if not auth(request): return {'error': 'Unauthorized'}, 401
    data = request.get_json()
    allowed = {k: v for k, v in data.items() if k in ('status', 'notes', 'owner', 'mailing')}
    if allowed:
        conn = init_db()
        sc = ', '.join(f'{k}=?' for k in allowed)
        conn.execute(f'UPDATE leads SET {sc}, updated=? WHERE id=?',
                     list(allowed.values()) + [datetime.now().isoformat(), lid])
        conn.commit(); conn.close()
    return {'status': 'updated', 'id': lid}

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('''CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hash TEXT UNIQUE,
        address TEXT,
        county TEXT,
        type TEXT,
        owner TEXT DEFAULT "",
        mailing TEXT DEFAULT "",
        apn TEXT DEFAULT "",
        filed_date TEXT,
        est_value INTEGER DEFAULT 0,
        score INTEGER DEFAULT 50,
        status TEXT DEFAULT "prospect",
        notes TEXT DEFAULT "",
        source_url TEXT DEFAULT "",
        residential INTEGER DEFAULT 1,
        added TEXT,
        updated TEXT,
        alerted INTEGER DEFAULT 0
    )''')
    try: conn.execute('ALTER TABLE leads ADD COLUMN residential INTEGER DEFAULT 1'); conn.commit()
    except: pass
    conn.commit(); return conn

def mkhash(addr, county, typ, filed):
    return hashlib.md5(f"{addr}{county}{typ}{filed}".lower().encode()).hexdigest()

def upsert(conn, lead):
    h = mkhash(lead['address'], lead['county'], lead['type'], lead['filed_date'])
    is_res = 1 if is_residential(lead['address'], lead.get('notes', '')) else 0
    try:
        conn.execute('''INSERT INTO leads
            (hash,address,county,type,owner,mailing,apn,filed_date,est_value,score,status,notes,source_url,residential,added,updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (h, lead['address'], lead['county'], lead['type'],
             lead.get('owner',''), lead.get('mailing',''), lead.get('apn',''),
             lead['filed_date'], lead.get('est_value',0), lead.get('score',50),
             'prospect', lead.get('notes',''), lead.get('source_url',''),
             is_res, datetime.now().isoformat(), datetime.now().isoformat()))
        conn.commit(); return is_res == 1
    except sqlite3.IntegrityError: return False

def calc_score(typ, filed='', val=0):
    s = {'trustee': 85, 'probate': 75, 'taxdelinquent': 70, 'foreclosure': 80}.get(typ, 50)
    try:
        d = (datetime.now() - datetime.strptime(filed[:10], '%Y-%m-%d')).days
        s += 10 if d <= 7 else 5 if d <= 30 else -10 if d > 90 else 0
    except: pass
    return min(max(s, 0), 100)

# ─── PIMA COUNTY RECORDER ───
def scrape_pima_trustee():
    leads = []
    try:
        log('Pima County Recorder — trustee sales...')
        # Try multiple Pima endpoints
        urls = [
            'https://recorder.pima.gov/RecorderSearches/foreclosure',
            'https://recorder.pima.gov/PublicSearch/SearchResultList?doctype=NOT',
        ]
        soup = None
        for url in urls:
            try:
                r = requests.get(url, headers=HEADERS, timeout=25)
                if r.status_code == 200 and len(r.text) > 500:
                    soup = BeautifulSoup(r.text, 'html.parser')
                    break
            except: continue

        if soup:
            # Try various table row selectors
            rows = soup.select('table tbody tr') or soup.select('table tr') or soup.select('.result-row')
            for row in rows[1:100]:
                cells = row.select('td')
                if len(cells) < 2: continue
                # Try to find address in any cell
                addr = ''
                for cell in cells:
                    txt = cell.get_text(strip=True)
                    if ADDRESS_PATTERN.match(txt) and len(txt) > 8:
                        addr = txt; break
                if not addr: continue
                if 'AZ' not in addr.upper(): addr = addr + ', Tucson, AZ'
                if not is_residential(addr): continue
                filed = datetime.now().strftime('%Y-%m-%d')
                for cell in cells:
                    txt = cell.get_text(strip=True)
                    if re.search(r'\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2}', txt):
                        filed = txt; break
                leads.append({
                    'address': addr, 'county': 'Pima', 'type': 'trustee',
                    'owner': cells[2].get_text(strip=True) if len(cells) > 2 else '',
                    'apn': cells[0].get_text(strip=True) if len(cells) > 0 else '',
                    'filed_date': filed, 'score': calc_score('trustee', filed),
                    'notes': 'Pima County Recorder — Trustee Sale',
                    'source_url': url
                })
        log(f'Pima trustee: {len(leads)} residential', 'ok')
    except Exception as e:
        log(f'Pima trustee error: {e}', 'err')
    return leads

# ─── MARICOPA COUNTY RECORDER ───
def scrape_maricopa_trustee():
    leads = []
    try:
        log('Maricopa County Recorder — trustee sales...')
        urls = [
            'https://recorder.maricopa.gov/recdocdata/GetDocData.aspx?docket=NOT&limit=100',
            'https://recorder.maricopa.gov/landrecords/searchresult.aspx',
        ]
        soup = None
        used_url = urls[0]
        for url in urls:
            try:
                r = requests.get(url, headers=HEADERS, timeout=25)
                if r.status_code == 200 and len(r.text) > 500:
                    soup = BeautifulSoup(r.text, 'html.parser')
                    used_url = url; break
            except: continue

        if soup:
            rows = soup.select('tr.DataRow, tr[class*="row"], table tbody tr, .search-result')
            for row in rows[:100]:
                cells = row.select('td')
                if len(cells) < 2: continue
                addr = ''
                for cell in cells:
                    txt = cell.get_text(strip=True)
                    if ADDRESS_PATTERN.match(txt) and len(txt) > 8:
                        addr = txt; break
                if not addr: continue
                if 'AZ' not in addr.upper(): addr = addr + ', Phoenix, AZ'
                if not is_residential(addr): continue
                filed = datetime.now().strftime('%Y-%m-%d')
                for cell in cells:
                    txt = cell.get_text(strip=True)
                    if re.search(r'\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2}', txt):
                        filed = txt; break
                leads.append({
                    'address': addr, 'county': 'Maricopa', 'type': 'trustee',
                    'owner': cells[2].get_text(strip=True) if len(cells) > 2 else '',
                    'filed_date': filed, 'score': calc_score('trustee', filed),
                    'notes': 'Maricopa County Recorder — Trustee Sale',
                    'source_url': used_url
                })
        log(f'Maricopa trustee: {len(leads)} residential', 'ok')
    except Exception as e:
        log(f'Maricopa trustee error: {e}', 'err')
    return leads

# ─── PIMA COUNTY ASSESSOR — TAX DELINQUENT ───
def scrape_pima_tax():
    leads = []
    try:
        log('Pima County Assessor — tax delinquent...')
        url = 'https://www.assessor.pima.gov/parcel/search'
        r = requests.get(url, headers=HEADERS, timeout=25)
        soup = BeautifulSoup(r.text, 'html.parser')
        rows = soup.select('table tr, .parcel-result, .search-result')
        for row in rows[1:80]:
            cells = row.select('td')
            if len(cells) < 2: continue
            addr = ''
            for cell in cells:
                txt = cell.get_text(strip=True)
                if ADDRESS_PATTERN.match(txt) and len(txt) > 8:
                    addr = txt; break
            if not addr or not is_residential(addr): continue
            leads.append({
                'address': addr + (', Tucson, AZ' if 'AZ' not in addr.upper() else ''),
                'county': 'Pima', 'type': 'taxdelinquent',
                'owner': cells[1].get_text(strip=True) if len(cells) > 1 else '',
                'apn': cells[0].get_text(strip=True) if cells else '',
                'filed_date': datetime.now().strftime('%Y-%m-%d'),
                'score': calc_score('taxdelinquent'),
                'notes': 'Pima County Assessor — Tax Delinquent',
                'source_url': url
            })
        log(f'Pima tax delinquent: {len(leads)} residential', 'ok')
    except Exception as e:
        log(f'Pima tax error: {e}', 'err')
    return leads

# ─── PIMA COUNTY SUPERIOR COURT — PROBATE via PACER alternative ───
def scrape_pima_probate():
    leads = []
    try:
        log('Pima probate — Superior Court...')
        # Use Pima Superior Court public access (different from blocked AZCourts)
        url = 'https://www.sc.pima.gov/CivilCourtDivision/ProbateDivision.aspx'
        r = requests.get(url, headers=HEADERS, timeout=25)
        soup = BeautifulSoup(r.text, 'html.parser')
        # Look for any address patterns in page
        text = soup.get_text()
        addrs = re.findall(r'\d+\s+[NSEW]?\s*\w+\s+(?:St|Ave|Rd|Blvd|Dr|Ln|Way|Ct|Pl|Cir|Loop|Trail|Ter|Place)\b[^,\n]{0,30}',
                           text, re.I)
        for addr in addrs[:20]:
            addr = addr.strip()
            if not is_residential(addr): continue
            if 'AZ' not in addr.upper(): addr = addr + ', Tucson, AZ'
            leads.append({
                'address': addr, 'county': 'Pima', 'type': 'probate',
                'owner': '', 'filed_date': datetime.now().strftime('%Y-%m-%d'),
                'score': calc_score('probate'),
                'notes': 'Pima Superior Court — Probate Division',
                'source_url': url
            })
        log(f'Pima probate: {len(leads)} residential', 'ok')
    except Exception as e:
        log(f'Pima probate error: {e}', 'err')
    return leads

# ─── MARICOPA COUNTY SUPERIOR COURT — PROBATE ───
def scrape_maricopa_probate():
    leads = []
    try:
        log('Maricopa probate — Superior Court...')
        url = 'https://www.superiorcourt.maricopa.gov/probate/'
        r = requests.get(url, headers=HEADERS, timeout=25)
        soup = BeautifulSoup(r.text, 'html.parser')
        text = soup.get_text()
        addrs = re.findall(r'\d+\s+[NSEW]?\s*\w+\s+(?:St|Ave|Rd|Blvd|Dr|Ln|Way|Ct|Pl|Cir)\b[^,\n]{0,30}',
                           text, re.I)
        for addr in addrs[:20]:
            addr = addr.strip()
            if not is_residential(addr): continue
            if 'AZ' not in addr.upper(): addr = addr + ', Phoenix, AZ'
            leads.append({
                'address': addr, 'county': 'Maricopa', 'type': 'probate',
                'owner': '', 'filed_date': datetime.now().strftime('%Y-%m-%d'),
                'score': calc_score('probate'),
                'notes': 'Maricopa Superior Court — Probate',
                'source_url': url
            })
        log(f'Maricopa probate: {len(leads)} residential', 'ok')
    except Exception as e:
        log(f'Maricopa probate error: {e}', 'err')
    return leads

# ─── PIMA COUNTY TRUSTEE SALES via AZ-SPECIFIC SOURCE ───
def scrape_az_trustee_sales():
    """Scrape from AZ Foreclosure listings — public data"""
    leads = []
    try:
        log('AZ Trustee Sales — public listings...')
        # Arizona Foreclosure public notice aggregators
        sources = [
            {
                'url': 'https://notices.realtytrac.com/foreclosure/az/pima-county/',
                'county': 'Pima', 'city': 'Tucson'
            },
        ]
        for src in sources:
            try:
                r = requests.get(src['url'], headers=HEADERS, timeout=20)
                if r.status_code != 200: continue
                soup = BeautifulSoup(r.text, 'html.parser')
                # Look for address patterns
                for el in soup.select('[class*=address],[class*=property],[class*=listing]'):
                    txt = el.get_text(strip=True)
                    m = re.search(r'\d+\s+\w+.*?(?:St|Ave|Rd|Blvd|Dr|Ln|Way)\b', txt, re.I)
                    if m:
                        addr = m.group(0).strip()
                        if not is_residential(addr): continue
                        if 'AZ' not in addr.upper(): addr += f', {src["city"]}, AZ'
                        leads.append({
                            'address': addr, 'county': src['county'], 'type': 'trustee',
                            'owner': '', 'filed_date': datetime.now().strftime('%Y-%m-%d'),
                            'score': calc_score('trustee'),
                            'notes': f'AZ Public Foreclosure Notice — {src["county"]}',
                            'source_url': src['url']
                        })
            except: continue
        log(f'AZ trustee sales: {len(leads)} residential', 'ok')
    except Exception as e:
        log(f'AZ trustee error: {e}', 'err')
    return leads

# ─── PIMA VANS (Vacant and Neglected Structures) ───
def scrape_pima_vans():
    leads = []
    try:
        log('Pima VANS — vacant & neglected structures...')
        url = 'https://gisdata.pima.gov/arcgis/rest/services/Community/VacantNeglected/MapServer/0/query?where=1%3D1&outFields=*&f=json&resultRecordCount=100'
        r = requests.get(url, headers=HEADERS, timeout=25)
        data = r.json()
        features = data.get('features', [])
        for feat in features[:50]:
            attrs = feat.get('attributes', {})
            addr = attrs.get('ADDRESS', '') or attrs.get('SITUS_ADDR', '') or attrs.get('address', '')
            if not addr or len(addr) < 5: continue
            if 'AZ' not in addr.upper(): addr = addr + ', Tucson, AZ'
            if not is_residential(addr): continue
            leads.append({
                'address': addr, 'county': 'Pima', 'type': 'vans',
                'owner': attrs.get('OWNER', ''),
                'apn': str(attrs.get('APN', '')),
                'filed_date': datetime.now().strftime('%Y-%m-%d'),
                'score': 80,
                'notes': f'VANS — Vacant/Neglected. Status: {attrs.get("STATUS", "Unknown")}',
                'source_url': url
            })
        log(f'Pima VANS: {len(leads)} properties', 'ok')
    except Exception as e:
        log(f'Pima VANS error: {e}', 'err')
    return leads

# ─── OWNER LOOKUP ───
def lookup_owner_pima(address):
    try:
        url = f'https://www.assessor.pima.gov/parcel/search?q={requests.utils.quote(address)}'
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        o = soup.select_one('.owner-name,[class*=owner]')
        m = soup.select_one('.mailing-address,[class*=mailing]')
        return {'owner': o.get_text(strip=True) if o else '', 'mailing': m.get_text(strip=True) if m else ''}
    except: return {'owner': '', 'mailing': ''}

def scrub(conn, leads):
    c = conn.cursor(); out = []
    for l in leads:
        h = mkhash(l['address'], l['county'], l['type'], l['filed_date'])
        row = c.execute('SELECT status FROM leads WHERE hash=?', (h,)).fetchone()
        if row and row['status'] in ('closed', 'skip'): continue
        out.append(l)
    return out

def send_email(new_leads):
    if not new_leads or not GMAIL_USER: return
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f'🏡 CEREBRO: {len(new_leads)} New Residential Leads'
        msg['From'] = GMAIL_USER; msg['To'] = ALERT_EMAIL
        rows = ''.join([
            f'<tr><td style="padding:8px;color:#00eaff;">{l["address"]}</td>'
            f'<td style="padding:8px;color:#888;">{l["county"]}</td>'
            f'<td style="padding:8px;color:#ff7b2f;">{l["type"].upper()}</td>'
            f'<td style="padding:8px;color:#00ff88;font-weight:bold;">{l["score"]}</td></tr>'
            for l in new_leads[:25]
        ])
        html = f'''<div style="background:#020810;color:#00eaff;font-family:monospace;padding:24px;">
            <h2>🏡 CEREBRO — RESIDENTIAL LEADS</h2>
            <p style="color:#39ff7a;">✓ Filtered: Single family homes + land only</p>
            <table style="width:100%;border-collapse:collapse;">{rows}</table></div>'''
        msg.attach(MIMEText(html, 'html'))
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as s:
            s.login(GMAIL_USER, GMAIL_PASS)
            s.sendmail(GMAIL_USER, ALERT_EMAIL, msg.as_string())
        log(f'Email sent — {len(new_leads)} residential leads', 'ok')
    except Exception as e:
        log(f'Email error: {e}', 'err')

def run_scrape():
    if status['running']: return
    status['running'] = True
    log('═══ CEREBRO SCRAPE — RESIDENTIAL + LAND ONLY ═══', 'ok')
    conn = init_db(); new = []
    try:
        all_leads = (
            scrape_pima_trustee() +
            scrape_maricopa_trustee() +
            scrape_pima_tax() +
            scrape_pima_probate() +
            scrape_maricopa_probate() +
            scrape_pima_vans() +
            scrape_az_trustee_sales()
        )
        for lead in scrub(conn, all_leads):
            if upsert(conn, lead):
                new.append(lead)

        # Owner lookup for Pima leads without owner
        log('Owner lookup...')
        for lead in [l for l in new if l['county'] == 'Pima' and not l.get('owner')][:6]:
            info = lookup_owner_pima(lead['address'])
            if info['owner']:
                h = mkhash(lead['address'], lead['county'], lead['type'], lead['filed_date'])
                conn.execute('UPDATE leads SET owner=?, mailing=? WHERE hash=?',
                             (info['owner'], info['mailing'], h))
                conn.commit()
                lead.update(info)
            time.sleep(1.5)

        total = conn.execute('SELECT COUNT(*) FROM leads WHERE residential=1').fetchone()[0]
        status['total'] = total
        status['last_run'] = datetime.now().isoformat()
        status['new_last'] = len(new)
        log(f'Done — {len(new)} new residential · {total} total', 'ok')
        if new:
            send_email(new)
        else:
            log('No new residential leads this cycle', 'ok')
    except Exception as e:
        log(f'Run error: {e}', 'err')
    finally:
        conn.close()
        status['running'] = False

def scheduler():
    schedule.every(SCAN_HOURS).hours.do(run_scrape)
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    print('CEREBRO — AZ PIPELINE · RESIDENTIAL + LAND ONLY · Railway.app')
    init_db()
    log('FILTER: Residential homes + Land only — No commercial', 'ok')
    threading.Thread(target=run_scrape, daemon=True).start()
    threading.Thread(target=scheduler, daemon=True).start()
    app.run(host='0.0.0.0', port=PORT, debug=False)
