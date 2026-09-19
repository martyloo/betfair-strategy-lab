from __future__ import annotations
import bz2,hashlib,hmac,json,math,os,re,secrets,threading,time,uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass,asdict
from datetime import date,datetime,timezone,timedelta
from pathlib import Path
import boto3,pyarrow as pa,pyarrow.parquet as pq,requests
from botocore.exceptions import ClientError
from fastapi import FastAPI,HTTPException,Request,Depends,Response
from fastapi.responses import HTMLResponse,RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel,Field
from starlette.middleware.sessions import SessionMiddleware

ENGINE="public-web-v3.4-csv-history"; ROOT=Path(os.getenv("BETFAIR_WEB_CACHE",Path.home()/".betfair-public-web-cache"));ROOT.mkdir(parents=True,exist_ok=True)
WORKERS=max(1,int(os.getenv("BACKTEST_WORKERS","4"))); MAX_BETS=max(100,int(os.getenv("MAX_BETS_RETURNED","5000")))
POOL=ThreadPoolExecutor(max_workers=WORKERS,thread_name_prefix="backtest"); LOCK=threading.Lock(); JOBS={}
API_BASE="https://historicdata.betfair.com/api/"
ADMIN_WORKERS=max(1,int(os.getenv("ADMIN_INGEST_WORKERS","2"))); ADMIN_POOL=ThreadPoolExecutor(max_workers=ADMIN_WORKERS,thread_name_prefix="ingest"); ADMIN_JOBS={}; ADMIN_LOCK=threading.Lock()
COUNTRIES=["GB","IE","US","AU","NZ","FR","DE","IT","ZA","AE","HK","SG","SE","NO","DK","ES","NL","BE","CA","CL","AR","BR","JP"]
BANDS=[(1.01,2),(2,3),(3,5),(5,10),(10,20),(20,30),(30,50),(50,100),(100,200),(200,500),(500,1001)]

@dataclass
class Runner:
    selection_id:int; name:str; bsp:float; winner:bool
    adjustment_factor:float|None=None; sort_priority:int|None=None; status:str=""; ltp:float|None=None
@dataclass
class Market:
    market_id:str; market_time:str; event_name:str; country:str; runners:list[Runner]
    venue:str=""; bet_delay:int|None=None; betting_type:str=""; market_base_rate:float|None=None
    number_of_winners:int|None=None; in_play_enabled:bool|None=None; cross_matching:bool|None=None
    discount_allowed:bool|None=None; persistence_enabled:bool|None=None
    race_code:str=""; distance:str=""; handicap_status:str=""; race_category:str=""; race_grade:str=""
@dataclass
class Bet:
    market_id:str;market_time:str;event_name:str;country:str;horse:str;bsp:float;bet_type:str;won:bool;stake:float;liability:float;gross:float;commission:float;net:float

class R2:
    def __init__(self):
        e=os.getenv("R2_ENDPOINT","").strip();a=os.getenv("R2_ACCESS_KEY_ID","").strip();s=os.getenv("R2_SECRET_ACCESS_KEY","").strip();self.bucket=os.getenv("R2_BUCKET","betfair-historical-data").strip()
        if not all([e,a,s,self.bucket]):raise RuntimeError("R2 environment variables are not fully configured.")
        self.s3=boto3.client("s3",endpoint_url=e,aws_access_key_id=a,aws_secret_access_key=s,region_name="auto")
    def test(self):self.s3.head_bucket(Bucket=self.bucket)
    def list(self,prefix):
        out=[];token=None
        while True:
            kw={"Bucket":self.bucket,"Prefix":prefix,"MaxKeys":1000}
            if token:kw["ContinuationToken"]=token
            r=self.s3.list_objects_v2(**kw);out += [x["Key"] for x in r.get("Contents",[]) if x["Key"].endswith(".parquet")]
            if not r.get("IsTruncated"):break
            token=r.get("NextContinuationToken")
            if not token:break
        return out
    def list_any(self,prefix,suffix=".parquet"):
        out=[];token=None
        while True:
            kw={"Bucket":self.bucket,"Prefix":prefix,"MaxKeys":1000}
            if token:kw["ContinuationToken"]=token
            z=self.s3.list_objects_v2(**kw);out += [x["Key"] for x in z.get("Contents",[]) if x["Key"].endswith(suffix)]
            if not z.get("IsTruncated"):break
            token=z.get("NextContinuationToken")
            if not token:break
        return out
    def getj(self,key):
        try:return json.loads(self.s3.get_object(Bucket=self.bucket,Key=key)["Body"].read())
        except ClientError as e:
            if str(e.response.get("Error",{}).get("Code","")) in ("404","NoSuchKey","NotFound"):return None
            raise
    def putj(self,key,v):self.s3.put_object(Bucket=self.bucket,Key=key,Body=json.dumps(v,separators=(",",":"),default=str).encode(),ContentType="application/json")
    def exists(self,key):
        try:self.s3.head_object(Bucket=self.bucket,Key=key);return True
        except ClientError as e:
            if str(e.response.get('Error',{}).get('Code','')) in ('404','NoSuchKey','NotFound'):return False
            raise
    def upload(self,src,key):self.s3.upload_file(str(src),self.bucket,key)
    def download(self,key,dest):
        dest.parent.mkdir(parents=True,exist_ok=True);tmp=dest.parent/(dest.name+".r2part")
        if tmp.exists():tmp.unlink()
        self.s3.download_file(self.bucket,key,str(tmp))
        if not tmp.exists():raise RuntimeError("R2 temporary download file missing.")
        tmp.replace(dest)


class BetfairHistoricAPI:
    def __init__(self):
        token=os.getenv("BETFAIR_SESSION_TOKEN","").strip()
        if not token: raise RuntimeError("BETFAIR_SESSION_TOKEN is not configured on the server.")
        self.session=requests.Session()
        self.session.headers.update({"ssoid":token,"Content-Type":"application/json","User-Agent":"BetfairStrategyLabAdmin/1.0"})
    def post(self,method,payload):
        r=self.session.post(API_BASE+method,data=json.dumps(payload),timeout=90);r.raise_for_status();return r.json()
    def list_files(self,payload):return self.post("DownloadListOfFiles",payload)
    def download(self,remote,dest):
        dest.parent.mkdir(parents=True,exist_ok=True);tmp=dest.parent/(dest.name+".part")
        if tmp.exists():tmp.unlink()
        with self.session.get(API_BASE+"DownloadFile",params={"filePath":remote},stream=True,timeout=(30,240)) as r:
            r.raise_for_status()
            with open(tmp,"wb") as f:
                for chunk in r.iter_content(262144):
                    if chunk:f.write(chunk)
        tmp.replace(dest)

class AdminIngestRequest(BaseModel):
    from_date:date;to_date:date;countries:list[str]=Field(min_length=1);plan:str="Basic Plan"

def source_id(remote):return hashlib.sha256(str(remote).encode()).hexdigest()[:24]
def raw_key(remote,plan):
    return f"raw/horse-racing/win/{slug(plan)}/{hashlib.sha256(str(remote).encode()).hexdigest()[:16]}-{Path(str(remote)).name}"
def index_key(remote,plan):return f"processed-index/horse-racing/win/{slug(plan)}/{source_id(remote)}.json"
def safe_float(v):
    try:
        x=float(v);return x if math.isfinite(x) else None
    except:return None
def parse_raw(path):
    latest=None;mid=path.stem;opener=bz2.open if path.suffix.lower()==".bz2" else open
    with opener(path,"rt",encoding="utf-8",errors="ignore") as f:
        for line in f:
            try:msg=json.loads(line)
            except:continue
            for mc in msg.get("mc",[]) if isinstance(msg,dict) else []:
                if mc.get("id"):mid=str(mc["id"])
                if isinstance(mc.get("marketDefinition"),dict):latest=mc["marketDefinition"]
    if not latest or str(latest.get("status","")).upper()!="CLOSED" or str(latest.get("marketType","")).upper()!="WIN":return None
    runners=[];winners=0
    for x in latest.get("runners",[]):
        status=str(x.get("status","")).upper()
        if status=="REMOVED":continue
        bsp=safe_float(x.get("bsp"));sid=x.get("id")
        if bsp is None or sid is None or not 1.01<=bsp<=1000:continue
        won=status=="WINNER";winners+=int(won);runners.append(Runner(int(sid),str(x.get("name") or f"Selection {sid}"),bsp,won))
    if len(runners)<2 or winners!=1:return None
    return Market(mid,str(latest.get("marketTime") or latest.get("openDate") or ""),str(latest.get("eventName") or latest.get("name") or ""),str(latest.get("countryCode") or "").upper(),runners)
def parquet_key(m,plan,sid):
    try:d=datetime.fromisoformat(m.market_time.replace("Z","+00:00"))
    except:d=datetime(1970,1,1,tzinfo=timezone.utc)
    return f"processed/horse-racing/win/{slug(plan)}/year={d.year:04d}/month={d.month:02d}/country={m.country or 'XX'}/{sid}-{m.market_id.replace('.','_')}.parquet"
def write_market(m,path,sid,plan):
    n=len(m.runners);path.parent.mkdir(parents=True,exist_ok=True)
    pq.write_table(pa.table({"schema_version":[1]*n,"source_id":[sid]*n,"plan":[plan]*n,"market_id":[m.market_id]*n,"market_time":[m.market_time]*n,"event_name":[m.event_name]*n,"country":[m.country]*n,"runner_count":[n]*n,"selection_id":[x.selection_id for x in m.runners],"horse":[x.name for x in m.runners],"bsp":[x.bsp for x in m.runners],"winner":[x.winner for x in m.runners]}),path,compression="zstd")
def betfair_payload(q):
    return {"sport":"Horse Racing","plan":q.plan,"fromDay":q.from_date.day,"fromMonth":q.from_date.month,"fromYear":q.from_date.year,"toDay":q.to_date.day,"toMonth":q.to_date.month,"toYear":q.to_date.year,"eventId":None,"eventName":None,"marketTypesCollection":["WIN"],"countriesCollection":q.countries,"fileTypeCollection":["M"]}
def admin_update(r,j,**changes):
    with ADMIN_LOCK:
        x=ADMIN_JOBS.get(j,{"job_id":j});x.update(changes);x["updated_at"]=datetime.now(timezone.utc).isoformat();ADMIN_JOBS[j]=x.copy()
    try:r.putj(f"admin-jobs/{j}.json",x)
    except:pass
def availability(q):
    r=R2();total=0;by={}
    for y,m in months(q.from_date,q.to_date):
        for c in sorted(set(x.upper() for x in q.countries)):
            n=len(r.list(f"processed/horse-racing/win/{slug(q.plan)}/year={y:04d}/month={m:02d}/country={c}/"));total+=n;by[f"{y:04d}-{m:02d}-{c}"]=n
    return {"processed_markets":total,"partitions":by}
def ingest(j,qd):
    r=R2();q=AdminIngestRequest(**qd);start=time.time()
    try:
        admin_update(r,j,status="running",progress=1,message="Requesting Betfair historical file list…")
        api=BetfairHistoricAPI();remotes=api.list_files(betfair_payload(q))
        if not isinstance(remotes,list):raise RuntimeError("Unexpected Betfair DownloadListOfFiles response.")
        st={"requested":len(remotes),"already_processed":0,"raw_r2_hits":0,"downloaded":0,"raw_uploaded":0,"parquet_created":0,"parquet_uploaded":0,"skipped":0,"failed":0}
        admin_update(r,j,status="running",progress=3,message=f"Betfair returned {len(remotes):,} market files.",stats=st)
        for i,remote in enumerate(remotes,1):
            try:
                sid=source_id(remote);idx=index_key(remote,q.plan);existing=r.getj(idx)
                if existing and existing.get("parquet_key") and r.exists(existing["parquet_key"]):st["already_processed"]+=1;continue
                raw=ROOT/"admin-raw"/slug(q.plan)/(sid+"-"+Path(str(remote)).name);rk0=raw_key(remote,q.plan)
                if raw.exists() and raw.stat().st_size:pass
                elif r.exists(rk0):r.download(rk0,raw);st["raw_r2_hits"]+=1
                else:api.download(remote,raw);st["downloaded"]+=1;r.upload(raw,rk0);st["raw_uploaded"]+=1
                m=parse_raw(raw)
                if not m:st["skipped"]+=1;continue
                pp=ROOT/"admin-parquet"/slug(q.plan)/(sid+".parquet");write_market(m,pp,sid,q.plan);st["parquet_created"]+=1
                pk=parquet_key(m,q.plan,sid)
                if not r.exists(pk):r.upload(pp,pk);st["parquet_uploaded"]+=1
                r.putj(idx,{"schema_version":1,"source_id":sid,"market_id":m.market_id,"market_time":m.market_time,"country":m.country,"parquet_key":pk})
            except Exception:st["failed"]+=1
            if i%max(1,len(remotes)//100)==0 or i==len(remotes):
                admin_update(r,j,status="running",progress=min(99,3+int(i/max(1,len(remotes))*96)),message=f"Processed {i:,} of {len(remotes):,} files…",stats=st)
        admin_update(r,j,status="complete",progress=100,message="Historical database update complete.",stats=st,elapsed_seconds=round(time.time()-start,2))
    except Exception as e:admin_update(r,j,status="failed",progress=100,message=str(e))

def require_admin(request:Request):
    if not request.session.get("admin"):raise HTTPException(401,"Admin authentication required.")
    return True

class Req(BaseModel):
    from_date:date;to_date:date;countries:list[str]=Field(min_length=1);plan:str="Basic Plan";strategy:str="Lay longest outsider";nth:int=Field(2,ge=1,le=100)
    min_odds:float=Field(1.01,ge=1.01,le=1000);max_odds:float=Field(1000,ge=1.01,le=1000);min_runners:int=Field(2,ge=2);max_runners:int=Field(0,ge=0)
    stake_mode:str="Fixed stake";amount:float=Field(1.0,gt=0);commission:float=Field(2.0,ge=0,le=100)
    venues:list[str]=Field(default_factory=list);days_of_week:list[int]=Field(default_factory=list);months_of_year:list[int]=Field(default_factory=list);time_from:str|None=None;time_to:str|None=None
    fav_min_bsp:float|None=None;fav_max_bsp:float|None=None;second_min_bsp:float|None=None;second_max_bsp:float|None=None
    fav_gap_min:float|None=None;fav_gap_max:float|None=None;overround_min:float|None=None;overround_max:float|None=None
    number_of_winners:int|None=None;in_play_enabled:bool|None=None;bet_delay:int|None=None;betting_type:str|None=None
    market_base_rate_min:float|None=None;market_base_rate_max:float|None=None;cross_matching:bool|None=None;discount_allowed:bool|None=None;persistence_enabled:bool|None=None
    race_codes:list[str]=Field(default_factory=list);distances:list[str]=Field(default_factory=list);handicap_status:str|None=None
    race_categories:list[str]=Field(default_factory=list);race_grades:list[str]=Field(default_factory=list)
    selected_ltp_min:float|None=None;selected_ltp_max:float|None=None;selected_adjustment_min:float|None=None;selected_adjustment_max:float|None=None
    selected_sort_priority_min:int|None=None;selected_sort_priority_max:int|None=None

def slug(s):return s.lower().replace(" ","-")
def months(a,b):
    y,m=a.year,a.month
    while (y,m)<=(b.year,b.month):
        yield y,m;m+=1
        if m==13:y+=1;m=1
def norm(q):
    d=q.model_dump(mode="json");d["countries"]=sorted(set(x.upper() for x in d["countries"]));d["engine_version"]=ENGINE;return d
def hsh(q):return hashlib.sha256(json.dumps(norm(q),sort_keys=True,separators=(",",":")).encode()).hexdigest()
def rk(h):return f"results/horse-racing/win/{ENGINE}/{h}.json"
def jk(j):return f"jobs/horse-racing/win/{ENGINE}/{j}.json"
def run_key(run_id):return f"public-runs/horse-racing/win/{run_id}.json"
def save_run(r,run_id,**changes):
    old=r.getj(run_key(run_id)) or {"run_id":run_id,"created_at":datetime.now(timezone.utc).isoformat()}
    old.update(changes);old["updated_at"]=datetime.now(timezone.utc).isoformat();r.putj(run_key(run_id),old);return old
def local(key):
    h=hashlib.sha256(key.encode()).hexdigest();return ROOT/"parquet"/h[:2]/f"{h}.parquet"
def _col(d,name,default=None):
    x=d.get(name);return x[0] if x else default
def readm(path):
    d=pq.read_table(path).to_pydict()
    if not d.get("market_id"):return None
    rs=[]
    for i in range(len(d["selection_id"])):
        def at(name,default=None):
            x=d.get(name);return x[i] if x and i<len(x) else default
        rs.append(Runner(int(at("selection_id")),str(at("horse","")),float(at("bsp")),bool(at("winner",False)),safe_float(at("adjustment_factor")),int(at("sort_priority")) if at("sort_priority") is not None else None,str(at("runner_status","")),safe_float(at("ltp"))))
    return Market(str(_col(d,"market_id","")),str(_col(d,"market_time","")),str(_col(d,"event_name","")),str(_col(d,"country","")).upper(),rs,str(_col(d,"venue","") or ""),_col(d,"bet_delay"),str(_col(d,"betting_type","") or ""),safe_float(_col(d,"market_base_rate")),_col(d,"number_of_winners"),_col(d,"in_play_enabled"),_col(d,"cross_matching"),_col(d,"discount_allowed"),_col(d,"persistence_enabled"),
                  str(_col(d,"race_code","") or ""),str(_col(d,"distance","") or ""),str(_col(d,"handicap_status","") or ""),str(_col(d,"race_category","") or ""),str(_col(d,"race_grade","") or ""))
def mdate(m):
    try:return datetime.fromisoformat(m.market_time.replace("Z","+00:00")).date()
    except:return None
def pick(m,s,n):
    a=sorted(m.runners,key=lambda x:x.bsp)
    if s in ("Back favourite","Lay favourite"):return a[0]
    if s in ("Back longest outsider","Lay longest outsider"):return a[-1]
    return a[n-1] if n<=len(a) else None
def settle(m,r,q):
    rate=q.commission/100;lay=q.strategy.startswith("Lay")
    if lay:
        if q.stake_mode=="Fixed liability":li=q.amount;st=li/(r.bsp-1)
        else:st=q.amount;li=(r.bsp-1)*st
        if r.winner:g,co,ne,won=-li,0,-li,False
        else:g=st;co=g*rate;ne=g-co;won=True
        typ="LAY"
    else:
        st=li=q.amount
        if r.winner:g=(r.bsp-1)*st;co=g*rate;ne=g-co;won=True
        else:g,co,ne,won=-st,0,-st,False
        typ="BACK"
    return Bet(m.market_id,m.market_time,m.event_name,m.country,r.name,r.bsp,typ,won,st,li,g,co,ne)
def stats(a):
    if not a:return {"bets":0,"wins":0,"losses":0,"strike":0,"gross":0,"commission":0,"net":0,"stake_roi":0,"liability_roi":0,"avg_bsp":0,"max_drawdown":0,"equity":[]}
    n=len(a);wins=sum(x.won for x in a);g=sum(x.gross for x in a);co=sum(x.commission for x in a);ne=sum(x.net for x in a);st=sum(x.stake for x in a);li=sum(x.liability for x in a)
    run=peak=dd=0;eq=[]
    for x in a:run+=x.net;eq.append(round(run,4));peak=max(peak,run);dd=max(dd,peak-run)
    return {"bets":n,"wins":wins,"losses":n-wins,"strike":wins/n*100,"gross":g,"commission":co,"net":ne,"stake_roi":ne/st*100 if st else 0,"liability_roi":ne/li*100 if li else 0,"avg_bsp":sum(x.bsp for x in a)/n,"max_drawdown":dd,"equity":eq}
def band(p):
    for lo,hi in BANDS:
        if lo<=p<hi:return f"{lo:g}–{hi:g}" if hi<1001 else f"{lo:g}+"
    return "Other"
def upd(r,j,**c):
    with LOCK:
        x=JOBS.get(j,{"job_id":j});x.update(c);x["updated_at"]=datetime.now(timezone.utc).isoformat();JOBS[j]=x.copy()
    try:r.putj(jk(j),x)
    except:pass
def discover(r,q):
    enriched=set();legacy=set()
    for y,m in months(q.from_date,q.to_date):
        for c in sorted(set(x.upper() for x in q.countries)):
            enriched.update(r.list_any(f"processed-enriched/horse-racing/win/{slug(q.plan)}/year={y:04d}/month={m:02d}/country={c}/"))
            legacy.update(r.list(f"processed/horse-racing/win/{slug(q.plan)}/year={y:04d}/month={m:02d}/country={c}/"))
    return sorted(enriched) if enriched else sorted(legacy)
def between(v,lo,hi):
    if lo is None and hi is None:return True
    if v is None:return False
    return (lo is None or v>=lo) and (hi is None or v<=hi)
def market_dt(m):
    try:return datetime.fromisoformat(m.market_time.replace("Z","+00:00"))
    except:return None
def market_filters(m,q):
    dt=market_dt(m)
    if q.venues and m.venue.strip().lower() not in {x.strip().lower() for x in q.venues}:return False
    if q.days_of_week and (not dt or dt.weekday() not in q.days_of_week):return False
    if q.months_of_year and (not dt or dt.month not in q.months_of_year):return False
    if dt and (q.time_from or q.time_to):
        hhmm=dt.strftime("%H:%M")
        if q.time_from and hhmm<q.time_from:return False
        if q.time_to and hhmm>q.time_to:return False
    a=sorted(m.runners,key=lambda x:x.bsp)
    if len(a)<2:return False
    fav,second=a[0],a[1];gap=second.bsp-fav.bsp;overround=sum(1/x.bsp for x in a)*100
    if not between(fav.bsp,q.fav_min_bsp,q.fav_max_bsp) or not between(second.bsp,q.second_min_bsp,q.second_max_bsp):return False
    if not between(gap,q.fav_gap_min,q.fav_gap_max) or not between(overround,q.overround_min,q.overround_max):return False
    if q.number_of_winners is not None and m.number_of_winners!=q.number_of_winners:return False
    if q.in_play_enabled is not None and m.in_play_enabled!=q.in_play_enabled:return False
    if q.bet_delay is not None and m.bet_delay!=q.bet_delay:return False
    if q.betting_type and m.betting_type.upper()!=q.betting_type.upper():return False
    if not between(m.market_base_rate,q.market_base_rate_min,q.market_base_rate_max):return False
    if q.cross_matching is not None and m.cross_matching!=q.cross_matching:return False
    if q.discount_allowed is not None and m.discount_allowed!=q.discount_allowed:return False
    if q.persistence_enabled is not None and m.persistence_enabled!=q.persistence_enabled:return False
    if q.race_codes and m.race_code not in q.race_codes:return False
    if q.distances and m.distance not in q.distances:return False
    if q.handicap_status and m.handicap_status!=q.handicap_status:return False
    if q.race_categories and m.race_category not in q.race_categories:return False
    if q.race_grades and m.race_grade not in q.race_grades:return False
    return True
def runner_filters(r,q):
    return between(r.ltp,q.selected_ltp_min,q.selected_ltp_max) and between(r.adjustment_factor,q.selected_adjustment_min,q.selected_adjustment_max) and between(r.sort_priority,q.selected_sort_priority_min,q.selected_sort_priority_max)
def work(j,qd,h,run_id):
    r=R2();q=Req(**qd);t=time.time()
    try:
        upd(r,j,status="running",progress=2,message="Finding processed Parquet partitions in R2…");keys=discover(r,q)
        if not keys:raise RuntimeError("No processed Parquet data found in R2 for this date/country/plan selection.")
        upd(r,j,status="running",progress=5,message=f"Found {len(keys):,} processed markets.",markets_found=len(keys))
        bets=[];skip=0;cs=set(x.upper() for x in q.countries)
        for i,key in enumerate(keys,1):
            try:
                lp=local(key)
                if not lp.exists() or not lp.stat().st_size:r.download(key,lp)
                m=readm(lp);d=mdate(m) if m else None
                if not m or not d or not(q.from_date<=d<=q.to_date) or m.country not in cs:continue
                n=len(m.runners)
                if n<q.min_runners or(q.max_runners and n>q.max_runners):continue
                if not market_filters(m,q):continue
                rr=pick(m,q.strategy,q.nth)
                if rr and runner_filters(rr,q) and q.min_odds<=rr.bsp<=q.max_odds:bets.append(settle(m,rr,q))
            except:skip+=1
            if i%max(1,len(keys)//20)==0:upd(r,j,status="running",progress=min(95,5+int(i/len(keys)*90)),message=f"Processed {i:,} of {len(keys):,} markets…")
        bets.sort(key=lambda x:x.market_time);s=stats(bets);groups={}
        for b in bets:groups.setdefault(band(b.bsp),[]).append(b)
        bands=[]
        for lo,hi in BANDS:
            n=f"{lo:g}–{hi:g}" if hi<1001 else f"{lo:g}+"
            if n in groups:
                z=stats(groups[n]);bands.append({"band":n,"bets":z["bets"],"wins":z["wins"],"strike":z["strike"],"gross":z["gross"],"net":z["net"],"roi":z["stake_roi"]})
        graph_points=[]; cumulative=0.0
        for b in bets:
            cumulative+=b.net
            graph_points.append({"market_time":b.market_time,"event_name":b.event_name,"horse":b.horse,"bsp":round(b.bsp,4),"bet_type":b.bet_type,"bet_net":round(b.net,4),"cumulative":round(cumulative,4)})
        out={"engine_version":ENGINE,"cache_hash":h,"request":norm(q),"stats":s,"bands":bands,"graph_points":graph_points,"bets":[asdict(x) for x in bets[:MAX_BETS]],"bets_truncated":len(bets)>MAX_BETS,"total_bets":len(bets),"markets_found":len(keys),"skipped":skip,"elapsed_seconds":round(time.time()-t,3)}
        r.putj(rk(h),out)
        save_run(r,run_id,status="complete",job_id=j,result_hash=h,engine_version=ENGINE,request=out["request"],
                 roi=round(s["stake_roi"],6),net=round(s["net"],6),bets=s["bets"],strike=round(s["strike"],6),
                 strategy=q.strategy,from_date=str(q.from_date),to_date=str(q.to_date),countries=q.countries,cached=False,
                 elapsed_seconds=out["elapsed_seconds"])
        upd(r,j,status="complete",progress=100,message="Backtest complete.",result_hash=h,run_id=run_id,cached=False,elapsed_seconds=out["elapsed_seconds"])
    except Exception as e:
        try: save_run(r,run_id,status="failed",job_id=j,result_hash=h,error=str(e))
        except: pass
        upd(r,j,status="failed",progress=100,message=str(e))

app=FastAPI(title="Betfair Strategy Lab");app.add_middleware(SessionMiddleware,secret_key=os.getenv("ADMIN_SESSION_SECRET",secrets.token_hex(32)),same_site="lax",https_only=os.getenv("COOKIE_SECURE","0")=="1");BASE=Path(__file__).parent
app.mount("/static",StaticFiles(directory=BASE/"static"),name="static");templates=Jinja2Templates(directory=BASE/"templates")
@app.get("/",response_class=HTMLResponse)
def home(request:Request):return templates.TemplateResponse(request=request,name="index.html",context={"countries":COUNTRIES})

@app.get("/api/filter-options")
def filter_options(plan:str="Basic Plan",country:str="GB"):
    r=R2(); base=f"processed-enriched/horse-racing/win/{slug(plan)}/"
    keys=r.list_any(base)
    venues=set();distances=set();codes=set();cats=set();grades=set()
    # metadata only: read columns from enriched files; cap is deliberately generous
    for k in keys:
        if f"/country={country.upper()}/" not in k: continue
        try:
            path=local(k); r.download(k,path) if not path.exists() else None
            d=pq.read_table(path,columns=["venue","distance","race_code","race_category","race_grade"]).to_pydict()
            for field,target in [("venue",venues),("distance",distances),("race_code",codes),("race_category",cats),("race_grade",grades)]:
                for v in d.get(field,[]) or []:
                    if v: target.add(str(v))
        except Exception: continue
    def dkey(x):
        m=re.match(r"(?:(\d+)m)?(?:(\d+)f)?",x)
        return (int(m.group(1) or 0)*8+int(m.group(2) or 0)) if m else 9999
    # Distance remains usable before a full enrichment has completed. These are
    # normal GB racing increments; values discovered in the user's data are merged in.
    fallback_distances={"5f","6f","7f","1m","1m1f","1m2f","1m3f","1m4f","1m5f","1m6f","1m7f",
                        "2m","2m1f","2m2f","2m3f","2m4f","2m5f","2m6f","2m7f","3m","3m1f",
                        "3m2f","3m3f","3m4f","3m5f","3m6f"}
    distances.update(fallback_distances)
    return {"venues":sorted(venues),"distances":sorted(distances,key=dkey),"race_codes":sorted(codes),
            "race_categories":sorted(cats),"race_grades":sorted(grades)}

@app.get("/api/health")
def health():
    try:r=R2();r.test();return {"ok":True,"r2":True,"workers":WORKERS,"engine":ENGINE}
    except Exception as e:return {"ok":False,"r2":False,"workers":WORKERS,"error":str(e)}
@app.post("/api/jobs",status_code=202)
def create(q:Req):
    if q.from_date>q.to_date:raise HTTPException(400,"From date must be before To date.")
    if q.min_odds>q.max_odds:raise HTTPException(400,"Minimum BSP cannot exceed maximum BSP.")
    q.countries=sorted(set(x.upper().strip() for x in q.countries if x.strip()))
    r=R2();h=hsh(q);cached=r.getj(rk(h));j=uuid.uuid4().hex;run_id=uuid.uuid4().hex
    base=dict(status="queued",job_id=j,result_hash=h,engine_version=ENGINE,request=norm(q),strategy=q.strategy,
              from_date=str(q.from_date),to_date=str(q.to_date),countries=q.countries)
    if cached:
        st=cached.get("stats",{})
        save_run(r,run_id,**base,status="complete",roi=round(float(st.get("stake_roi",0)),6),
                 net=round(float(st.get("net",0)),6),bets=int(st.get("bets",0)),strike=round(float(st.get("strike",0)),6),cached=True)
        upd(r,j,status="complete",progress=100,message="Loaded from persistent result cache.",result_hash=h,run_id=run_id,cached=True,elapsed_seconds=0)
        return {"job_id":j,"run_id":run_id,"status":"complete","cached":True}
    save_run(r,run_id,**base,cached=False)
    upd(r,j,status="queued",progress=0,message="Backtest queued.",result_hash=h,run_id=run_id,cached=False)
    POOL.submit(work,j,q.model_dump(mode="json"),h,run_id)
    return {"job_id":j,"run_id":run_id,"status":"queued","cached":False}

@app.get("/api/jobs/{j}")
def job(j:str):
    with LOCK:x=JOBS.get(j)
    if x:return x
    x=R2().getj(jk(j))
    if x:return x
    raise HTTPException(404,"Job not found.")
@app.get("/api/jobs/{j}/result")
def result(j:str):
    r=R2()
    with LOCK:x=JOBS.get(j)
    if not x:x=r.getj(jk(j))
    if not x:raise HTTPException(404,"Job not found.")
    if x.get("status")!="complete":raise HTTPException(409,f"Job is {x.get('status','unknown')}.")
    z=r.getj(rk(x["result_hash"]))
    if not z:raise HTTPException(404,"Result cache entry not found.")
    return z

@app.get("/api/public-runs")
def public_runs(limit:int=200):
    r=R2();keys=r.list_any("public-runs/horse-racing/win/",suffix=".json");rows=[]
    for k in keys:
        try:
            x=r.getj(k)
            if x and x.get("status")=="complete": rows.append(x)
        except: pass
    rows.sort(key=lambda x:(float(x.get("roi",0)),x.get("created_at","")),reverse=True)
    return {"runs":rows[:max(1,min(limit,1000))],"total":len(rows)}

@app.get("/api/public-runs/{run_id}/result")
def public_run_result(run_id:str):
    r=R2();x=r.getj(run_key(run_id))
    if not x:raise HTTPException(404,"Saved run not found.")
    if x.get("status")!="complete":raise HTTPException(409,"Saved run is not complete.")
    z=r.getj(rk(x.get("result_hash","")))
    if not z:raise HTTPException(404,"Saved result data not found.")
    return {"run":x,"result":z}

@app.get("/admin",response_class=HTMLResponse)
def admin_page(request:Request):
    if not request.session.get("admin"):return templates.TemplateResponse(request=request,name="admin_login.html",context={})
    return templates.TemplateResponse(request=request,name="admin.html",context={"countries":COUNTRIES,"auto_enabled":os.getenv("AUTO_UPDATE_ENABLED","0")=="1"})

@app.post("/api/admin/login")
async def admin_login(request:Request):
    body=await request.json();expected=os.getenv("ADMIN_PASSWORD","")
    if not expected:raise HTTPException(503,"ADMIN_PASSWORD is not configured on the server.")
    if not hmac.compare_digest(str(body.get("password","")),expected):raise HTTPException(401,"Incorrect admin password.")
    request.session["admin"]=True;return {"ok":True}

@app.post("/api/admin/logout")
def admin_logout(request:Request):
    request.session.clear();return {"ok":True}

@app.get("/api/admin/status")
def admin_status(_:bool=Depends(require_admin)):
    r2ok=bfok=False;errors=[]
    try:R2().test();r2ok=True
    except Exception as e:errors.append("R2: "+str(e))
    try:BetfairHistoricAPI();bfok=True
    except Exception as e:errors.append("Betfair: "+str(e))
    return {"r2":r2ok,"betfair_credentials":bfok,"auto_update_enabled":os.getenv("AUTO_UPDATE_ENABLED","0")=="1","errors":errors}

@app.post("/api/admin/availability")
def admin_availability(q:AdminIngestRequest,_:bool=Depends(require_admin)):
    return availability(q)

@app.post("/api/admin/jobs",status_code=202)
def admin_create_job(q:AdminIngestRequest,_:bool=Depends(require_admin)):
    if q.from_date>q.to_date:raise HTTPException(400,"From date must be before To date.")
    q.countries=sorted(set(x.upper().strip() for x in q.countries if x.strip()))
    j=uuid.uuid4().hex;r=R2();admin_update(r,j,status="queued",progress=0,message="Historical ingestion queued.",stats={})
    ADMIN_POOL.submit(ingest,j,q.model_dump(mode="json"));return {"job_id":j}

@app.get("/api/admin/jobs/{j}")
def admin_job(j:str,_:bool=Depends(require_admin)):
    with ADMIN_LOCK:x=ADMIN_JOBS.get(j)
    if x:return x
    x=R2().getj(f"admin-jobs/{j}.json")
    if x:return x
    raise HTTPException(404,"Admin job not found.")

def automatic_update_once():
    if os.getenv("AUTO_UPDATE_ENABLED","0")!="1":return
    countries=[x.strip().upper() for x in os.getenv("AUTO_UPDATE_COUNTRIES","GB,IE").split(",") if x.strip()]
    plan=os.getenv("AUTO_UPDATE_PLAN","Basic Plan");days=max(2,int(os.getenv("AUTO_UPDATE_LOOKBACK_DAYS","7")))
    end=date.today()-timedelta(days=1);start=end-timedelta(days=days)
    q=AdminIngestRequest(from_date=start,to_date=end,countries=countries,plan=plan);j="auto-"+datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    ADMIN_POOL.submit(ingest,j,q.model_dump(mode="json"))

@app.on_event("startup")
def start_auto_updater():
    if os.getenv("AUTO_UPDATE_ENABLED","0")!="1":return
    def loop():
        time.sleep(10)
        while True:
            try:automatic_update_once()
            except Exception:pass
            time.sleep(max(3600,int(os.getenv("AUTO_UPDATE_INTERVAL_HOURS","24"))*3600))
    threading.Thread(target=loop,daemon=True,name="auto-updater").start()
