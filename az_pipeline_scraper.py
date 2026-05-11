
"""
CEREBRO — Arizona Statewide Pipeline Scraper + API
Empire Housing Solutions — empiresolutions520@gmail.com
Railway.app Deployment — GIS API Version (no HTML scraping)
"""

import os, re, time, hashlib, sqlite3, smtplib, schedule, threading, requests
from datetime import datetime
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

HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; CEREBRO/1.0)', 'Accept': 'application/json'}
COMMERCIAL_EXCLUDE = ['llc','inc','corp','commercial','industrial','warehouse','office',
    'retail','plaza','mall','hotel','motel','restaurant','suite','medical','dental',
    'church','school','storage','parking','hoa','association']

def is_residential(address, notes=''):
    if not address or len(address) < 5: return False
    combined = address.lower() + ' ' + (notes or '').lower()
    for kw in COMMERCIAL_EXCLUDE:
        if kw in combined: return False
    return True

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
def index(): return jsonify({'service':'CEREBRO AZ Pipeline','status':'online','total_leads':status['total'],'last_run':status['last_run']})

@app.route('/health')
def health(): return jsonify({'status':'ok','time':datetime.now().isoformat()})

@app.route('/leads')
def get_leads():
    if not auth(request): return jsonify({'error':'Unauthorized'}), 401
    county=request.args.get('county',''); typ=request.args.get('type',''); lim=int(request.args.get('limit',500))
    conn=init_db(); c=conn.cursor()
    q='SELECT * FROM leads WHERE residential=1'; p=[]
    if county: q+=' AND county=?'; p.append(county)
    if typ: q+=' AND type=?'; p.append(typ)
    q+=' ORDER BY filed_date DESC LIMIT ?'; p.append(lim)
    rows=c.execute(q,p).fetchall(); cols=[d[0] for d in c.description]
    leads=[dict(zip(cols,r)) for r in rows]; conn.close()
    return jsonify({'leads':leads,'total':len(leads),'updated':status['last_run'],'filter':'residential+land'})

@app.route('/stats')
def get_stats():
    if not auth(request): return jsonify({'error':'Unauthorized'}), 401
    conn=init_db(); c=conn.cursor()
    def cnt(q,p=[]): return c.execute(q,p).fetchone()[0]
    by_county=dict(c.execute('SELECT county,COUNT(*) FROM leads WHERE residential=1 GROUP BY county').fetchall())
    result={'total':cnt('SELECT COUNT(*) FROM leads WHERE residential=1'),'trustee':cnt("SELECT COUNT(*) FROM leads WHERE type='trustee' AND residential=1"),'probate':cnt("SELECT COUNT(*) FROM leads WHERE type='probate' AND residential=1"),'taxd':cnt("SELECT COUNT(*) FROM leads WHERE type='taxdelinquent' AND residential=1"),'vans':cnt("SELECT COUNT(*) FROM leads WHERE type='vans' AND residential=1"),'hot':cnt('SELECT COUNT(*) FROM leads WHERE score>=80 AND residential=1'),'by_county':by_county,'last_run':status['last_run'],'running':status['running'],'log':status['log'][-10:]}
    conn.close(); return jsonify(result)

@app.route('/trigger', methods=['POST','GET'])
def trigger():
    if not auth(request): return jsonify({'error':'Unauthorized'}), 401
    if status['running']: return jsonify({'status':'already_running'})
    threading.Thread(target=run_scrape,daemon=True).start()
    return jsonify({'status':'started','message':'Scrape triggered successfully'})

@app.route('/lead/<int:lid>', methods=['PATCH'])
def update_lead(lid):
    if not auth(request): return jsonify({'error':'Unauthorized'}), 401
    data=request.get_json()
    allowed={k:v for k,v in data.items() if k in ('status','notes','owner','mailing')}
    if allowed:
        conn=init_db(); sc=', '.join(f'{k}=?' for k in allowed)
        conn.execute(f'UPDATE leads SET {sc},updated=? WHERE id=?',list(allowed.values())+[datetime.now().isoformat(),lid]); conn.commit(); conn.close()
    return jsonify({'status':'updated','id':lid})

def init_db():
    conn=sqlite3.connect(DB_PATH); conn.row_factory=sqlite3.Row
    conn.execute('''CREATE TABLE IF NOT EXISTS leads (id INTEGER PRIMARY KEY AUTOINCREMENT,hash TEXT UNIQUE,address TEXT,county TEXT,type TEXT,owner TEXT DEFAULT "",mailing TEXT DEFAULT "",apn TEXT DEFAULT "",filed_date TEXT,est_value INTEGER DEFAULT 0,score INTEGER DEFAULT 50,status TEXT DEFAULT "prospect",notes TEXT DEFAULT "",source_url TEXT DEFAULT "",residential INTEGER DEFAULT 1,added TEXT,updated TEXT,alerted INTEGER DEFAULT 0)''')
    try: conn.execute('ALTER TABLE leads ADD COLUMN residential INTEGER DEFAULT 1'); conn.commit()
    except: pass
    conn.commit(); return conn

def mkhash(addr,county,typ,filed): return hashlib.md5(f"{addr}{county}{typ}{filed}".lower().encode()).hexdigest()

def upsert(conn,lead):
    h=mkhash(lead['address'],lead['county'],lead['type'],lead['filed_date'])
    is_res=1 if is_residential(lead['address'],lead.get('notes','')) else 0
    try:
        conn.execute('INSERT INTO leads (hash,address,county,type,owner,mailing,apn,filed_date,est_value,score,status,notes,source_url,residential,added,updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(h,lead['address'],lead['county'],lead['type'],lead.get('owner',''),lead.get('mailing',''),lead.get('apn',''),lead['filed_date'],lead.get('est_value',0),lead.get('score',50),'prospect',lead.get('notes',''),lead.get('source_url',''),is_res,datetime.now().isoformat(),datetime.now().isoformat()))
        conn.commit(); return is_res==1
    except sqlite3.IntegrityError: return False

def calc_score(typ): return {'trustee':88,'probate':78,'taxdelinquent':72,'vans':82,'foreclosure':85}.get(typ,60)

def gis_fetch(url, label):
    """Generic GIS API fetcher"""
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200: return []
        data = r.json()
        if 'error' in data: return []
        return data.get('features', [])
    except Exception as e:
        log(f'{label} error: {e}', 'err')
        return []

def parse_addr(attrs, fallback_city, fallback_state='AZ'):
    for key in ['ADDRESS','SITUS_ADDR','SITUS_ADDRESS','SITE_ADDRESS','PROP_ADDR','FULL_ADDRESS','address','full_address']:
        val = attrs.get(key,'').strip() if attrs.get(key) else ''
        if val and len(val) > 5 and re.search(r'\d', val):
            if 'AZ' not in val.upper(): val += f', {fallback_city}, AZ'
            return val
    return ''

# SOURCE 1: HUD REO (Federal — always works)
def scrape_hud():
    leads = []
    log('HUD REO properties — AZ...')
    url = 'https://hudgis-hud.opendata.arcgis.com/arcgis/rest/services/Hosted/HUD_REO_Properties/FeatureServer/0/query?where=STATE_CD%3D%27AZ%27&outFields=FULL_ADDRESS,CITY,STATE_CD,ZIP_CD,LIST_PRICE,BED_COUNT,BATH_COUNT&f=json&resultRecordCount=100'
    for feat in gis_fetch(url, 'HUD'):
        a = feat.get('attributes', {})
        addr = f"{a.get('FULL_ADDRESS','').strip()}, {a.get('CITY','').strip()}, AZ {a.get('ZIP_CD','')}".strip(', ')
        if not addr or not is_residential(addr): continue
        city = a.get('CITY','').lower()
        county = 'Maricopa' if any(c in city for c in ['phoenix','scottsdale','mesa','tempe','gilbert','chandler','peoria','glendale','avondale','surprise']) else 'Pima' if 'tucson' in city else 'Arizona'
        price = int(a.get('LIST_PRICE',0) or 0)
        leads.append({'address':addr,'county':county,'type':'foreclosure','owner':'HUD/FHA',
            'filed_date':datetime.now().strftime('%Y-%m-%d'),'est_value':price,
            'score':calc_score('foreclosure'),'notes':f'HUD REO — {a.get("BED_COUNT","")}bd/{a.get("BATH_COUNT","")}ba. ${price:,}',
            'source_url':url})
    log(f'HUD: {len(leads)} AZ properties','ok')
    return leads

# SOURCE 2: Pima County Open Data Portal (multiple endpoints)
def scrape_pima_gis():
    leads = []
    log('Pima County GIS Open Data...')
    # These are the confirmed Pima County ArcGIS endpoints
    endpoints = [
        ('https://gisdata.pima.gov/arcgis/rest/services/Community/VacantNeglected/MapServer/0/query?where=1%3D1&outFields=ADDRESS,OWNER,APN,STATUS&f=json&resultRecordCount=200', 'vans', 'Tucson'),
        ('https://gisdata.pima.gov/arcgis/rest/services/Community/Foreclosures/MapServer/0/query?where=1%3D1&outFields=*&f=json&resultRecordCount=200', 'trustee', 'Tucson'),
        ('https://gisdata.pima.gov/arcgis/rest/services/Finance/TaxDelinquent/MapServer/0/query?where=1%3D1&outFields=*&f=json&resultRecordCount=200', 'taxdelinquent', 'Tucson'),
    ]
    for url, typ, city in endpoints:
        feats = gis_fetch(url, f'Pima {typ}')
        for feat in feats:
            a = feat.get('attributes', {})
            addr = parse_addr(a, city)
            if not addr or not is_residential(addr): continue
            leads.append({'address':addr,'county':'Pima','type':typ,
                'owner':str(a.get('OWNER',a.get('OWNER_NAME',''))),
                'apn':str(a.get('APN','')),
                'filed_date':datetime.now().strftime('%Y-%m-%d'),
                'score':calc_score(typ),
                'notes':f'Pima County GIS — {typ.upper()}',
                'source_url':url})
        log(f'Pima {typ}: {len([l for l in leads if l["type"]==typ])} found','ok')
    return leads

# SOURCE 3: Maricopa County GIS
def scrape_maricopa_gis():
    leads = []
    log('Maricopa County GIS...')
    endpoints = [
        ('https://gis.maricopa.gov/arcgis/rest/services/Parcel/MapServer/0/query?where=1%3D1&outFields=ADDRESS,OWNER,APN&f=json&resultRecordCount=100', 'trustee', 'Phoenix'),
    ]
    for url, typ, city in endpoints:
        for feat in gis_fetch(url, f'Maricopa {typ}'):
            a = feat.get('attributes', {})
            addr = parse_addr(a, city)
            if not addr or not is_residential(addr): continue
            leads.append({'address':addr,'county':'Maricopa','type':typ,
                'owner':str(a.get('OWNER','')), 'apn':str(a.get('APN','')),
                'filed_date':datetime.now().strftime('%Y-%m-%d'),
                'score':calc_score(typ), 'notes':f'Maricopa County GIS',
                'source_url':url})
    log(f'Maricopa: {len(leads)} found','ok')
    return leads

def scrub(conn,leads):
    c=conn.cursor(); out=[]
    for l in leads:
        h=mkhash(l['address'],l['county'],l['type'],l['filed_date'])
        row=c.execute('SELECT status FROM leads WHERE hash=?',(h,)).fetchone()
        if row and row['status'] in ('closed','skip'): continue
        out.append(l)
    return out

def send_email(new_leads):
    if not new_leads or not GMAIL_USER: return
    try:
        msg=MIMEMultipart('alternative'); msg['Subject']=f'CEREBRO: {len(new_leads)} New Leads'; msg['From']=GMAIL_USER; msg['To']=ALERT_EMAIL
        rows=''.join([f'<tr><td style="padding:8px;color:#00eaff;">{l["address"]}</td><td style="color:#888;">{l["county"]}</td><td style="color:#ff7b2f;">{l["type"].upper()}</td><td style="color:#00ff88;">{l["score"]}</td></tr>' for l in new_leads[:25]])
        html=f'<div style="background:#020810;color:#00eaff;font-family:monospace;padding:24px;"><h2>CEREBRO — {len(new_leads)} NEW LEADS</h2><table>{rows}</table></div>'
        msg.attach(MIMEText(html,'html'))
        with smtplib.SMTP_SSL('smtp.gmail.com',465) as s:
            s.login(GMAIL_USER,GMAIL_PASS); s.sendmail(GMAIL_USER,ALERT_EMAIL,msg.as_string())
        log(f'Email sent — {len(new_leads)} leads','ok')
    except Exception as e: log(f'Email error: {e}','err')

def run_scrape():
    if status['running']: return
    status['running']=True
    log('═══ CEREBRO SCRAPE START ═══','ok')
    conn=init_db(); new=[]
    try:
        all_leads = scrape_hud() + scrape_pima_gis() + scrape_maricopa_gis()
        log(f'Total candidates: {len(all_leads)}','ok')
        for lead in scrub(conn,all_leads):
            if upsert(conn,lead): new.append(lead)
        total=conn.execute('SELECT COUNT(*) FROM leads WHERE residential=1').fetchone()[0]
        status['total']=total; status['last_run']=datetime.now().isoformat(); status['new_last']=len(new)
        log(f'Done — {len(new)} new · {total} total','ok')
        if new: send_email(new)
        else: log('No new leads this cycle','ok')
    except Exception as e: log(f'Error: {e}','err')
    finally: conn.close(); status['running']=False

def scheduler():
    schedule.every(SCAN_HOURS).hours.do(run_scrape)
    while True: schedule.run_pending(); time.sleep(60)

if __name__=='__main__':
    print('CEREBRO AZ PIPELINE — Railway.app')
    init_db()
    threading.Thread(target=run_scrape,daemon=True).start()
    threading.Thread(target=scheduler,daemon=True).start()
    app.run(host='0.0.0.0',port=PORT,debug=False)
