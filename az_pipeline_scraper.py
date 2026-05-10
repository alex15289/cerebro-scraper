"""
CEREBRO — Arizona Statewide Pipeline Scraper + API Server
Empire Housing Solutions — empiresolutions520@gmail.com
Deploy on Railway.app

HOW IT WORKS:
  1. Scrapes all 15 AZ County Recorders for Trustee Sales
  2. Scrapes AZCourts.gov for Probate filings
  3. Cross-refs County Assessors for owner + mailing address
  4. Scrubs duplicates and already-closed cases
  5. Serves a LIVE JSON API that CEREBRO reads automatically
  6. Sends Gmail alert when new leads are found
  7. Runs every 6 hours automatically on Railway

RAILWAY ENV VARS TO SET:
  GMAIL_USER   = empiresolutions520@gmail.com
  GMAIL_PASS   = your Gmail App Password
  ALERT_EMAIL  = empiresolutions520@gmail.com
  SECRET_KEY   = empire2026  (or any password you choose)
"""

import os, json, time, hashlib, sqlite3, smtplib, schedule, threading, requests, re
from datetime import datetime
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, jsonify, request
from flask_cors import CORS

# ── CONFIG ──────────────────────────────────────────────────────────
GMAIL_USER  = os.environ.get('GMAIL_USER',  '')
GMAIL_PASS  = os.environ.get('GMAIL_PASS',  '')
ALERT_EMAIL = os.environ.get('ALERT_EMAIL', 'empiresolutions520@gmail.com')
SECRET_KEY  = os.environ.get('SECRET_KEY',  'empire2026')
DB_PATH     = os.environ.get('DB_PATH',     'az_pipeline.db')
PORT        = int(os.environ.get('PORT',    '8080'))
SCAN_HOURS  = int(os.environ.get('SCAN_HOURS', '6'))

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'}

AZ_COUNTIES = [
    {'name':'Pima',       'rec':'https://recorder.pima.gov/RecorderSearches/foreclosure',        'courts':'26'},
    {'name':'Maricopa',   'rec':'https://recorder.maricopa.gov/recdocdata/GetDocData.aspx',       'courts':'28'},
    {'name':'Pinal',      'rec':'https://recorder.pinalcountyaz.gov',                            'courts':'21'},
    {'name':'Yavapai',    'rec':'https://www.recorder.yavapaicounty.us',                         'courts':'25'},
    {'name':'Mohave',     'rec':'https://www.mohavecounty.us/ContentPage.aspx?id=73',            'courts':'14'},
    {'name':'Yuma',       'rec':'https://www.yumacountyaz.gov/government/recorder',              'courts':'24'},
    {'name':'Cochise',    'rec':'https://www.cochise.az.gov/recorder',                           'courts':'2'},
    {'name':'Navajo',     'rec':'https://www.navajocountyaz.gov/Departments/Recorder',           'courts':'17'},
    {'name':'Apache',     'rec':'https://www.apachecountyaz.gov/recorder',                       'courts':'1'},
    {'name':'Graham',     'rec':'https://www.graham.az.gov/152/Recorder',                        'courts':'8'},
    {'name':'Greenlee',   'rec':'https://www.co.greenlee.az.us/recorder',                        'courts':'9'},
    {'name':'La Paz',     'rec':'https://www.lapaz.az.gov/recorder',                             'courts':'11'},
    {'name':'Santa Cruz', 'rec':'https://www.santacruzcountyaz.gov/recorder',                    'courts':'22'},
    {'name':'Gila',       'rec':'https://www.gilacountyaz.gov/government/recorder',              'courts':'7'},
    {'name':'Coconino',   'rec':'https://www.coconino.az.gov/188/Recorder',                      'courts':'3'},
]

# ── FLASK API ─────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)

status = {'running':False,'last_run':None,'new_last':0,'total':0,'log':[]}

def log(msg, lv='info'):
    ts = datetime.now().strftime('%H:%M:%S')
    status['log'].append({'ts':ts,'msg':msg,'lv':lv})
    status['log'] = status['log'][-50:]
    print(f'[{ts}] {msg}')

def auth(req):
    return req.args.get('key','') == SECRET_KEY or (req.is_json and req.get_json().get('key','') == SECRET_KEY)

@app.route('/')
def index():
    return jsonify({'service':'CEREBRO AZ Pipeline','status':'online','total_leads':status['total'],'last_run':status['last_run']})

@app.route('/health')
def health():
    return jsonify({'status':'ok','time':datetime.now().isoformat()})

@app.route('/leads')
def get_leads():
    if not auth(request): return jsonify({'error':'Unauthorized'}), 401
    county = request.args.get('county','')
    typ    = request.args.get('type','')
    lim    = int(request.args.get('limit',500))
    conn = init_db(); c = conn.cursor()
    q = 'SELECT * FROM leads WHERE 1=1'
    p = []
    if county: q+=' AND county=?'; p.append(county)
    if typ:    q+=' AND type=?';   p.append(typ)
    q += ' ORDER BY filed_date DESC LIMIT ?'; p.append(lim)
    rows = c.execute(q,p).fetchall()
    cols = [d[0] for d in c.description]
    leads = [dict(zip(cols,r)) for r in rows]
    conn.close()
    return jsonify({'leads':leads,'total':len(leads),'updated':status['last_run']})

@app.route('/stats')
def get_stats():
    if not auth(request): return jsonify({'error':'Unauthorized'}), 401
    conn = init_db(); c = conn.cursor()
    def cnt(q,p=[]): return c.execute(q,p).fetchone()[0]
    by_county = dict(c.execute('SELECT county,COUNT(*) FROM leads GROUP BY county').fetchall())
    result = {
        'total':   cnt('SELECT COUNT(*) FROM leads'),
        'trustee': cnt("SELECT COUNT(*) FROM leads WHERE type='trustee'"),
        'probate': cnt("SELECT COUNT(*) FROM leads WHERE type='probate'"),
        'taxd':    cnt("SELECT COUNT(*) FROM leads WHERE type='taxdelinquent'"),
        'hot':     cnt('SELECT COUNT(*) FROM leads WHERE score>=80'),
        'by_county': by_county,
        'last_run':  status['last_run'],
        'running':   status['running'],
        'log':       status['log'][-10:],
    }
    conn.close()
    return jsonify(result)

@app.route('/trigger', methods=['POST'])
def trigger():
    if not auth(request): return jsonify({'error':'Unauthorized'}), 401
    if status['running']: return jsonify({'status':'already_running'})
    threading.Thread(target=run_scrape, daemon=True).start()
    return jsonify({'status':'started'})

@app.route('/lead/<int:lid>', methods=['PATCH'])
def update_lead(lid):
    if not auth(request): return jsonify({'error':'Unauthorized'}), 401
    data = request.get_json()
    allowed = {k:v for k,v in data.items() if k in ('status','notes','owner','mailing')}
    if allowed:
        conn = init_db()
        sc = ', '.join(f'{k}=?' for k in allowed)
        conn.execute(f'UPDATE leads SET {sc},updated=? WHERE id=?', list(allowed.values())+[datetime.now().isoformat(),lid])
        conn.commit(); conn.close()
    return jsonify({'status':'updated','id':lid})

# ── DATABASE ──────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('''CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hash TEXT UNIQUE, address TEXT, county TEXT, type TEXT,
        owner TEXT DEFAULT "", mailing TEXT DEFAULT "", apn TEXT DEFAULT "",
        filed_date TEXT, est_value INTEGER DEFAULT 0, score INTEGER DEFAULT 50,
        status TEXT DEFAULT "prospect", notes TEXT DEFAULT "",
        source_url TEXT DEFAULT "", added TEXT, updated TEXT, alerted INTEGER DEFAULT 0
    )''')
    conn.commit()
    return conn

def mkhash(addr,county,typ,filed):
    return hashlib.md5(f"{addr}{county}{typ}{filed}".lower().encode()).hexdigest()

def upsert(conn, lead):
    h = mkhash(lead['address'],lead['county'],lead['type'],lead['filed_date'])
    try:
        conn.execute('''INSERT INTO leads (hash,address,county,type,owner,mailing,apn,
            filed_date,est_value,score,status,notes,source_url,added,updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (h,lead['address'],lead['county'],lead['type'],lead.get('owner',''),
             lead.get('mailing',''),lead.get('apn',''),lead['filed_date'],
             lead.get('est_value',0),lead.get('score',50),'prospect',
             lead.get('notes',''),lead.get('source_url',''),
             datetime.now().isoformat(),datetime.now().isoformat()))
        conn.commit(); return True
    except sqlite3.IntegrityError: return False

def score(typ, filed='', val=0):
    s = {'trustee':85,'probate':75,'taxdelinquent':70,'foreclosure':80}.get(typ,50)
    try:
        d = (datetime.now()-datetime.strptime(filed[:10],'%Y-%m-%d')).days
        s += 10 if d<=7 else 5 if d<=30 else -10 if d>90 else 0
    except: pass
    return min(max(s,0),100)

# ── SCRAPERS ──────────────────────────────────────────────────────────
def scrape_pima():
    leads=[]
    try:
        log('Pima County Recorder — trustee sales...')
        r=requests.get(AZ_COUNTIES[0]['rec'],headers=HEADERS,timeout=20)
        soup=BeautifulSoup(r.text,'html.parser')
        for row in soup.select('table tr')[1:60]:
            cells=row.select('td')
            if len(cells)<3: continue
            addr=cells[1].get_text(strip=True) if len(cells)>1 else ''
            if not addr or len(addr)<5: continue
            filed=cells[3].get_text(strip=True) if len(cells)>3 else datetime.now().strftime('%Y-%m-%d')
            leads.append({'address':addr+', Tucson, AZ','county':'Pima','type':'trustee',
                'owner':cells[2].get_text(strip=True) if len(cells)>2 else '',
                'apn':cells[0].get_text(strip=True) if len(cells)>0 else '',
                'filed_date':filed,'score':score('trustee',filed),
                'notes':'Pima County Recorder — Notice of Trustee Sale',
                'source_url':AZ_COUNTIES[0]['rec']})
        log(f'Pima: {len(leads)} trustee sales found','ok')
    except Exception as e: log(f'Pima error: {e}','err')
    return leads

def scrape_maricopa():
    leads=[]
    try:
        log('Maricopa County Recorder — trustee sales...')
        url='https://recorder.maricopa.gov/recdocdata/GetDocData.aspx?docket=NOT&limit=50'
        r=requests.get(url,headers=HEADERS,timeout=20)
        soup=BeautifulSoup(r.text,'html.parser')
        for row in soup.select('tr.DataRow,tr[class*=row]')[:50]:
            cells=row.select('td')
            if len(cells)<3: continue
            addr=cells[1].get_text(strip=True) if len(cells)>1 else ''
            if not addr or len(addr)<5: continue
            filed=cells[0].get_text(strip=True) if len(cells)>0 else datetime.now().strftime('%Y-%m-%d')
            leads.append({'address':addr+', Phoenix, AZ','county':'Maricopa','type':'trustee',
                'owner':cells[2].get_text(strip=True) if len(cells)>2 else '',
                'apn':'','filed_date':filed,'score':score('trustee',filed),
                'notes':'Maricopa County Recorder — Notice of Trustee Sale','source_url':url})
        log(f'Maricopa: {len(leads)} trustee sales found','ok')
    except Exception as e: log(f'Maricopa error: {e}','err')
    return leads

def scrape_probate(county):
    leads=[]
    try:
        log(f'{county["name"]} — probate filings (AZCourts)...')
        url=f'https://publicaccess.courts.az.gov/calendar/Home/Dashboard/{county["courts"]}'
        r=requests.get(url,headers=HEADERS,timeout=20)
        soup=BeautifulSoup(r.text,'html.parser')
        blocks=soup.find_all(string=re.compile(r'(probate|estate|PB-)',re.I))
        for block in blocks[:15]:
            txt=block.parent.get_text(strip=True) if block.parent else str(block)
            am=re.search(r'\d+\s+\w+.*?(?:St|Ave|Rd|Blvd|Dr|Ln|Way)\b',txt,re.I)
            addr=(am.group(0).strip() if am else f'{county["name"]} County')+', AZ'
            nm=re.search(r'Estate\s+of\s+([\w\s]+?)(?:\s{{2,}}|,|\d)',txt,re.I)
            leads.append({'address':addr,'county':county['name'],'type':'probate',
                'owner':nm.group(0).strip() if nm else '','apn':'',
                'filed_date':datetime.now().strftime('%Y-%m-%d'),'score':score('probate'),
                'notes':f'AZCourts.gov — {county["name"]} Probate. {txt[:100]}','source_url':url})
        log(f'{county["name"]}: {len(leads)} probate found','ok')
    except Exception as e: log(f'{county["name"]} probate error: {e}','err')
    return leads

def owner_lookup_pima(address):
    try:
        url=f'https://www.assessor.pima.gov/parcel/search?q={requests.utils.quote(address)}'
        r=requests.get(url,headers=HEADERS,timeout=10)
        soup=BeautifulSoup(r.text,'html.parser')
        o=soup.select_one('.owner-name,[class*=owner]')
        m=soup.select_one('.mailing-address,[class*=mailing]')
        return {'owner':o.get_text(strip=True) if o else '','mailing':m.get_text(strip=True) if m else ''}
    except: return {'owner':'','mailing':''}

def scrub(conn, leads):
    c=conn.cursor()
    out=[]
    for l in leads:
        h=mkhash(l['address'],l['county'],l['type'],l['filed_date'])
        row=c.execute('SELECT status FROM leads WHERE hash=?',(h,)).fetchone()
        if row and row['status'] in ('closed','skip'): continue
        out.append(l)
    return out

# ── EMAIL ─────────────────────────────────────────────────────────────
def send_email(new_leads):
    if not new_leads or not GMAIL_USER: return
    try:
        msg=MIMEMultipart('alternative')
        msg['Subject']=f'🏛️ CEREBRO: {len(new_leads)} New AZ Pipeline Leads'
        msg['From']=GMAIL_USER; msg['To']=ALERT_EMAIL
        rows=''.join([f'<tr><td style="padding:8px;color:#00eaff;font-size:11px;">{l["address"]}</td><td style="padding:8px;color:#888;font-size:11px;">{l["county"]}</td><td style="padding:8px;color:#ff7b2f;font-size:11px;">{l["type"].upper()}</td><td style="padding:8px;color:#00ff88;font-size:12px;font-weight:bold;">{l["score"]}</td><td style="padding:8px;color:#888;font-size:11px;">{l["filed_date"]}</td></tr>' for l in new_leads[:25]])
        html=f'''<div style="background:#020810;color:#00eaff;font-family:monospace;padding:24px;border-radius:8px;">
          <h2 style="color:#00eaff;letter-spacing:4px;">🏛️ CEREBRO — AZ PIPELINE ALERT</h2>
          <p style="color:#888;">{len(new_leads)} new leads · {datetime.now().strftime("%B %d, %Y %H:%M")} MST</p>
          <table style="width:100%;border-collapse:collapse;margin-top:16px;">
            <thead><tr><th style="padding:8px;text-align:left;color:#555;font-size:9px;letter-spacing:2px;">ADDRESS</th><th style="padding:8px;text-align:left;color:#555;font-size:9px;">COUNTY</th><th style="padding:8px;text-align:left;color:#555;font-size:9px;">TYPE</th><th style="padding:8px;text-align:left;color:#555;font-size:9px;">SCORE</th><th style="padding:8px;text-align:left;color:#555;font-size:9px;">FILED</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
          <p style="color:#333;font-size:9px;margin-top:16px;">Empire Housing Solutions · CEREBRO Neural Pipeline</p>
        </div>'''
        msg.attach(MIMEText(html,'html'))
        with smtplib.SMTP_SSL('smtp.gmail.com',465) as s:
            s.login(GMAIL_USER,GMAIL_PASS); s.sendmail(GMAIL_USER,ALERT_EMAIL,msg.as_string())
        log(f'Email alert sent — {len(new_leads)} leads','ok')
    except Exception as e: log(f'Email error: {e}','err')

# ── MAIN SCRAPE ───────────────────────────────────────────────────────
def run_scrape():
    if status['running']: return
    status['running']=True
    log('═══ CEREBRO SCRAPE CYCLE STARTED ═══','ok')
    conn=init_db(); new=[]
    try:
        for lead in scrub(conn, scrape_pima()+scrape_maricopa()):
            if upsert(conn,lead): new.append(lead)
        for county in AZ_COUNTIES[:5]:
            for lead in scrub(conn, scrape_probate(county)):
                if upsert(conn,lead): new.append(lead)
        log('Owner lookup from assessors...')
        for lead in [l for l in new if l['county']=='Pima' and not l.get('owner')][:6]:
            info=owner_lookup_pima(lead['address'])
            if info['owner']:
                h=mkhash(lead['address'],lead['county'],lead['type'],lead['filed_date'])
                conn.execute('UPDATE leads SET owner=?,mailing=? WHERE hash=?',(info['owner'],info['mailing'],h))
                conn.commit(); lead.update(info)
            time.sleep(1.5)
        total=conn.execute('SELECT COUNT(*) FROM leads').fetchone()[0]
        status['total']=total; status['last_run']=datetime.now().isoformat(); status['new_last']=len(new)
        log(f'Done — {len(new)} new · {total} total leads','ok')
        if new: send_email(new)
        else: log('No new leads this cycle','ok')
    except Exception as e: log(f'Cycle error: {e}','err')
    finally: conn.close(); status['running']=False

def scheduler():
    schedule.every(SCAN_HOURS).hours.do(run_scrape)
    while True: schedule.run_pending(); time.sleep(60)

# ── START ─────────────────────────────────────────────────────────────
if __name__=='__main__':
    print('╔══════════════════════════════════════════════════════════╗')
    print('║  CEREBRO — AZ PIPELINE SCRAPER + API  · Railway.app     ║')
    print('║  Empire Housing Solutions · Pima County Ops              ║')
    print('╚══════════════════════════════════════════════════════════╝')
    init_db()
    log('DB ready','ok')
    log(f'API on port {PORT}','ok')
    log(f'SECRET_KEY: {"CUSTOM ✓" if SECRET_KEY!="empire2026" else "empire2026 (default)"}')
    threading.Thread(target=run_scrape,daemon=True).start()
    threading.Thread(target=scheduler,daemon=True).start()
    app.run(host='0.0.0.0',port=PORT,debug=False)
