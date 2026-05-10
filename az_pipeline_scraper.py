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

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'}

COMMERCIAL_EXCLUDE = ['llc','inc','corp','corporation','commercial','industrial','warehouse','office','retail','plaza','center','mall','hotel','motel','restaurant','store','shop','suite','medical','dental','church','school','storage','parking','airport','business park']
ADDRESS_PATTERN = re.compile(r'^\d+\s+\w')

def is_residential(address, notes=''):
    if not address or len(address) < 5: return False
    addr_lower = address.lower()
    combined = addr_lower + ' ' + (notes or '').lower()
    if not ADDRESS_PATTERN.match(address.strip()): return False
    for kw in COMMERCIAL_EXCLUDE:
        if kw in combined: return False
    has_street = any(f' {kw} ' in addr_lower or addr_lower.endswith(f' {kw}') or f' {kw},' in addr_lower for kw in ['st','ave','rd','blvd','dr','ln','way','ct','pl','cir','loop','trail','ter','place'])
    is_land = any(kw in combined for kw in ['vacant','land','lot','parcel','acr'])
    return has_street or is_land

app = Flask(__name__)
CORS(app)
status = {'running':False,'last_run':None,'new_last':0,'total':0,'log':[]}

def log(msg, lv='info'):
    ts = datetime.now().strftime('%H:%M:%S')
    status['log'].append({'ts':ts,'msg':msg,'lv':lv})
    status['log'] = status['log'][-50:]
    print(f'[{ts}] {msg}')

def auth(req): return req.args.get('key','') == SECRET_KEY

@app.route('/')
def index(): return {'service':'CEREBRO AZ Pipeline','status':'online','filter':'Residential+Land only','total_leads':status['total'],'last_run':status['last_run']}

@app.route('/health')
def health(): return {'status':'ok','time':datetime.now().isoformat()}

@app.route('/leads')
def get_leads():
    if not auth(request): return {'error':'Unauthorized'},401
    county=request.args.get('county',''); typ=request.args.get('type',''); lim=int(request.args.get('limit',500))
    conn=init_db(); c=conn.cursor()
    q='SELECT * FROM leads WHERE residential=1'; p=[]
    if county: q+=' AND county=?'; p.append(county)
    if typ: q+=' AND type=?'; p.append(typ)
    q+=' ORDER BY filed_date DESC LIMIT ?'; p.append(lim)
    rows=c.execute(q,p).fetchall(); cols=[d[0] for d in c.description]
    leads=[dict(zip(cols,r)) for r in rows]; conn.close()
    return {'leads':leads,'total':len(leads),'updated':status['last_run'],'filter':'residential+land'}

@app.route('/stats')
def get_stats():
    if not auth(request): return {'error':'Unauthorized'},401
    conn=init_db(); c=conn.cursor()
    def cnt(q,p=[]): return c.execute(q,p).fetchone()[0]
    by_county=dict(c.execute('SELECT county,COUNT(*) FROM leads WHERE residential=1 GROUP BY county').fetchall())
    result={'total':cnt('SELECT COUNT(*) FROM leads WHERE residential=1'),'trustee':cnt("SELECT COUNT(*) FROM leads WHERE type='trustee' AND residential=1"),'probate':cnt("SELECT COUNT(*) FROM leads WHERE type='probate' AND residential=1"),'taxd':cnt("SELECT COUNT(*) FROM leads WHERE type='taxdelinquent' AND residential=1"),'hot':cnt('SELECT COUNT(*) FROM leads WHERE score>=80 AND residential=1'),'by_county':by_county,'last_run':status['last_run'],'running':status['running'],'log':status['log'][-10:],'filter':'Residential+Land only'}
    conn.close(); return result

@app.route('/trigger', methods=['POST'])
def trigger():
    if not auth(request): return {'error':'Unauthorized'},401
    if status['running']: return {'status':'already_running'}
    threading.Thread(target=run_scrape,daemon=True).start()
    return {'status':'started'}

@app.route('/lead/<int:lid>', methods=['PATCH'])
def update_lead(lid):
    if not auth(request): return {'error':'Unauthorized'},401
    data=request.get_json()
    allowed={k:v for k,v in data.items() if k in ('status','notes','owner','mailing')}
    if allowed:
        conn=init_db(); sc=', '.join(f'{k}=?' for k in allowed)
        conn.execute(f'UPDATE leads SET {sc},updated=? WHERE id=?',list(allowed.values())+[datetime.now().isoformat(),lid]); conn.commit(); conn.close()
    return {'status':'updated','id':lid}

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

def score(typ,filed='',val=0):
    s={'trustee':85,'probate':75,'taxdelinquent':70,'foreclosure':80}.get(typ,50)
    try:
        d=(datetime.now()-datetime.strptime(filed[:10],'%Y-%m-%d')).days
        s+=10 if d<=7 else 5 if d<=30 else -10 if d>90 else 0
    except: pass
    return min(max(s,0),100)

def scrape_pima_trustee():
    leads=[]
    try:
        log('Pima County Recorder — residential trustee sales...')
        url='https://recorder.pima.gov/RecorderSearches/foreclosure'
        r=requests.get(url,headers=HEADERS,timeout=20); soup=BeautifulSoup(r.text,'html.parser')
        for row in soup.select('table tr')[1:80]:
            cells=row.select('td')
            if len(cells)<3: continue
            addr=cells[1].get_text(strip=True) if len(cells)>1 else ''
            if not addr or len(addr)<5: continue
            if 'AZ' not in addr: addr=addr+', Tucson, AZ'
            if not is_residential(addr): continue
            filed=cells[3].get_text(strip=True) if len(cells)>3 else datetime.now().strftime('%Y-%m-%d')
            leads.append({'address':addr,'county':'Pima','type':'trustee','owner':cells[2].get_text(strip=True) if len(cells)>2 else '','apn':cells[0].get_text(strip=True) if len(cells)>0 else '','filed_date':filed,'score':score('trustee',filed),'notes':'Pima County Recorder — Trustee Sale. Residential.','source_url':url})
        log(f'Pima: {len(leads)} residential trustee sales','ok')
    except Exception as e: log(f'Pima error: {e}','err')
    return leads

def scrape_maricopa_trustee():
    leads=[]
    try:
        log('Maricopa County Recorder — residential trustee sales...')
        url='https://recorder.maricopa.gov/recdocdata/GetDocData.aspx?docket=NOT&limit=100'
        r=requests.get(url,headers=HEADERS,timeout=20); soup=BeautifulSoup(r.text,'html.parser')
        for row in soup.select('tr.DataRow,tr[class*=row]')[:80]:
            cells=row.select('td')
            if len(cells)<3: continue
            addr=cells[1].get_text(strip=True) if len(cells)>1 else ''
            if not addr or len(addr)<5: continue
            if 'AZ' not in addr: addr=addr+', Phoenix, AZ'
            if not is_residential(addr): continue
            filed=cells[0].get_text(strip=True) if len(cells)>0 else datetime.now().strftime('%Y-%m-%d')
            leads.append({'address':addr,'county':'Maricopa','type':'trustee','owner':cells[2].get_text(strip=True) if len(cells)>2 else '','apn':'','filed_date':filed,'score':score('trustee',filed),'notes':'Maricopa County Recorder — Trustee Sale. Residential.','source_url':url})
        log(f'Maricopa: {len(leads)} residential trustee sales','ok')
    except Exception as e: log(f'Maricopa error: {e}','err')
    return leads

def scrape_probate(county):
    leads=[]
    try:
        log(f'{county["name"]} probate — residential filter...')
        url=f'https://publicaccess.courts.az.gov/calendar/Home/Dashboard/{county["courts"]}'
        r=requests.get(url,headers=HEADERS,timeout=20); soup=BeautifulSoup(r.text,'html.parser')
        blocks=soup.find_all(string=re.compile(r'(probate|estate|PB-)',re.I))
        for block in blocks[:20]:
            txt=block.parent.get_text(strip=True) if block.parent else str(block)
            am=re.search(r'\d+\s+\w+.*?(?:St|Ave|Rd|Blvd|Dr|Ln|Way|Ct|Pl)\b',txt,re.I)
            addr=(am.group(0).strip()+', AZ' if am else f'{county["name"]} County, AZ')
            if not is_residential(addr,txt): continue
            nm=re.search(r'Estate\s+of\s+([\w\s]+?)(?:\s{2,}|,|\d)',txt,re.I)
            leads.append({'address':addr,'county':county['name'],'type':'probate','owner':nm.group(0).strip() if nm else '','apn':'','filed_date':datetime.now().strftime('%Y-%m-%d'),'score':score('probate'),'notes':f'AZCourts.gov probate — {county["name"]}. Residential. {txt[:100]}','source_url':url})
        log(f'{county["name"]} probate: {len(leads)} residential','ok')
    except Exception as e: log(f'{county["name"]} probate error: {e}','err')
    return leads

def lookup_owner_pima(address):
    try:
        url=f'https://www.assessor.pima.gov/parcel/search?q={requests.utils.quote(address)}'
        r=requests.get(url,headers=HEADERS,timeout=10); soup=BeautifulSoup(r.text,'html.parser')
        o=soup.select_one('.owner-name,[class*=owner]'); m=soup.select_one('.mailing-address,[class*=mailing]')
        return {'owner':o.get_text(strip=True) if o else '','mailing':m.get_text(strip=True) if m else ''}
    except: return {'owner':'','mailing':''}

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
        msg=MIMEMultipart('alternative'); msg['Subject']=f'🏡 CEREBRO: {len(new_leads)} New Residential Leads'; msg['From']=GMAIL_USER; msg['To']=ALERT_EMAIL
        rows=''.join([f'<tr><td style="padding:8px;color:#00eaff;">{l["address"]}</td><td style="padding:8px;color:#888;">{l["county"]}</td><td style="padding:8px;color:#ff7b2f;">{l["type"].upper()}</td><td style="padding:8px;color:#00ff88;font-weight:bold;">{l["score"]}</td></tr>' for l in new_leads[:25]])
        html=f'<div style="background:#020810;color:#00eaff;font-family:monospace;padding:24px;"><h2>🏡 CEREBRO — RESIDENTIAL LEADS</h2><p style="color:#39ff7a;">✓ Filtered: Single family homes + land only</p><table style="width:100%;border-collapse:collapse;">{rows}</table></div>'
        msg.attach(MIMEText(html,'html'))
        with smtplib.SMTP_SSL('smtp.gmail.com',465) as s:
            s.login(GMAIL_USER,GMAIL_PASS); s.sendmail(GMAIL_USER,ALERT_EMAIL,msg.as_string())
        log(f'Email sent — {len(new_leads)} residential leads','ok')
    except Exception as e: log(f'Email error: {e}','err')

AZ_COUNTIES=[{'name':'Pima','courts':'26'},{'name':'Maricopa','courts':'28'},{'name':'Pinal','courts':'21'},{'name':'Yavapai','courts':'25'},{'name':'Mohave','courts':'14'}]

def run_scrape():
    if status['running']: return
    status['running']=True
    log('═══ CEREBRO SCRAPE — RESIDENTIAL + LAND ONLY ═══','ok')
    conn=init_db(); new=[]
    try:
        for lead in scrub(conn,scrape_pima_trustee()+scrape_maricopa_trustee()):
            if upsert(conn,lead): new.append(lead)
        for county in AZ_COUNTIES:
            for lead in scrub(conn,scrape_probate(county)):
                if upsert(conn,lead): new.append(lead)
        log('Owner lookup...')
        for lead in [l for l in new if l['county']=='Pima' and not l.get('owner')][:6]:
            info=lookup_owner_pima(lead['address'])
            if info['owner']:
                h=mkhash(lead['address'],lead['county'],lead['type'],lead['filed_date'])
                conn.execute('UPDATE leads SET owner=?,mailing=? WHERE hash=?',(info['owner'],info['mailing'],h)); conn.commit(); lead.update(info)
            time.sleep(1.5)
        total=conn.execute('SELECT COUNT(*) FROM leads WHERE residential=1').fetchone()[0]
        status['total']=total; status['last_run']=datetime.now().isoformat(); status['new_last']=len(new)
        log(f'Done — {len(new)} new residential · {total} total','ok')
        if new: send_email(new)
        else: log('No new residential leads this cycle','ok')
    except Exception as e: log(f'Error: {e}','err')
    finally: conn.close(); status['running']=False

def scheduler():
    schedule.every(SCAN_HOURS).hours.do(run_scrape)
    while True: schedule.run_pending(); time.sleep(60)

if __name__=='__main__':
    print('CEREBRO — AZ PIPELINE · RESIDENTIAL + LAND ONLY · Railway.app')
    init_db()
    log('FILTER: Residential homes + Land only — No commercial','ok')
    threading.Thread(target=run_scrape,daemon=True).start()
    threading.Thread(target=scheduler,daemon=True).start()
    app.run(host='0.0.0.0',port=PORT,debug=False)
