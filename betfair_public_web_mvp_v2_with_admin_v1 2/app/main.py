from __future__ import annotations
import bz2,hashlib,hmac,json,math,os,secrets,threading,time,uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass,asdict
from collections import Counter
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

ENGINE="public-web-v2.1"; ROOT=Path(os.getenv("BETFAIR_WEB_CACHE",Path.home()/".betfair-public-web-cache"));ROOT.mkdir(parents=True,exist_ok=True)
WORKERS=max(1,int(os.getenv("BACKTEST_WORKERS","4"))); MAX_BETS=max(100,int(os.getenv("MAX_BETS_RETURNED","5000")))
POOL=ThreadPoolExecutor(max_workers=WORKERS,thread_name_prefix="backtest"); LOCK=threading.Lock(); JOBS={}
API_BASE="https://historicdata.betfair.com/api/"
ADMIN_WORKERS=max(1,int(os.getenv("ADMIN_INGEST_WORKERS","2"))); ADMIN_POOL=ThreadPoolExecutor(max_workers=ADMIN_WORKERS,thread_name_prefix="ingest"); ADMIN_JOBS={}; ADMIN_LOCK=threading.Lock()
COUNTRIES=["GB","IE","US","AU","NZ","FR","DE","IT","ZA","AE","HK","SG","SE","NO","DK","ES","NL","BE","CA","CL","AR","BR","JP"]
BANDS=[(1.01,2),(2,3),(3,5),(5,10),(10,20),(20,30),(30,50),(50,100),(100,200),(200,500),(500,1001)]

@dataclass
class Runner: selection_id:int; name:str; bsp:float; winner:bool
@dataclass
class Market: market_id:str; market_time:str; event_name:str; country:str; runners:list[Runner]
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


class FieldInspectRequest(BaseModel):
    plan:str="Basic Plan"
    sample_size:int=Field(default=500,ge=10,le=5000)

def inspector_present(v): return v is not None and v != "" and v != [] and v != {}
def inspector_flatten(obj,prefix="",depth=0):
    out=[]
    if not isinstance(obj,dict) or depth>4:return out
    for k,v in obj.items():
        name=f"{prefix}.{k}" if prefix else str(k);out.append((name,v))
        if isinstance(v,dict):out.extend(inspector_flatten(v,name,depth+1))
    return out
def inspector_grade(p):
    return "RELIABLE" if p>=99 else "MOSTLY" if p>=90 else "SPARSE" if p>=10 else "RARE"
def inspector_merge(a,b):
    for k,v in b.items():a[k]+=v
def inspector_rows(seen,nonempty,denom):
    z=[]
    for f in seen:
        p=round(100*nonempty[f]/max(1,denom),2)
        z.append({"field":f,"present":nonempty[f],"opportunities":denom,"percent":p,"reliability":inspector_grade(p)})
    return sorted(z,key=lambda x:(-x["percent"],x["field"]))
def inspect_one_raw(path):
    ms=Counter();mn=Counter();rs=Counter();rn=Counter();ps=Counter();pn=Counter();mdn=rdn=pdn=0;countries=Counter()
    opener=bz2.open if path.suffix.lower()==".bz2" else open
    with opener(path,"rt",encoding="utf-8",errors="ignore") as f:
        for line in f:
            try:msg=json.loads(line)
            except:continue
            for mc in msg.get("mc",[]) if isinstance(msg,dict) else []:
                md=mc.get("marketDefinition")
                if isinstance(md,dict):
                    mdn+=1
                    if md.get("countryCode"):countries[str(md["countryCode"]).upper()]+=1
                    for k,v in inspector_flatten(md):
                        ms[k]+=1;mn[k]+=int(inspector_present(v))
                    for rr in md.get("runners",[]) if isinstance(md.get("runners"),list) else []:
                        if not isinstance(rr,dict):continue
                        rdn+=1
                        for k,v in inspector_flatten(rr):
                            rs[k]+=1;rn[k]+=int(inspector_present(v))
                for rc in mc.get("rc",[]) if isinstance(mc.get("rc"),list) else []:
                    if not isinstance(rc,dict):continue
                    pdn+=1
                    for k,v in inspector_flatten(rc):
                        ps[k]+=1;pn[k]+=int(inspector_present(v))
    return ms,mn,rs,rn,ps,pn,mdn,rdn,pdn,countries
def run_field_inspection(j,qd):
    r=R2();q=FieldInspectRequest(**qd);start=time.time()
    try:
        prefix=f"raw/horse-racing/win/{slug(q.plan)}/";admin_update(r,j,status="running",progress=1,message="Listing raw R2 files…")
        keys=sorted(k for k in r.list(prefix) if k.lower().endswith((".bz2",".json",".jsonl",".txt")))
        if not keys:raise RuntimeError(f"No raw historical files found under {prefix}")
        n=min(q.sample_size,len(keys));sample=keys if n==len(keys) else [keys[min(len(keys)-1,int(i*len(keys)/n))] for i in range(n)]
        ms=Counter();mn=Counter();rs=Counter();rn=Counter();ps=Counter();pn=Counter();countries=Counter();mdn=rdn=pdn=failed=0
        cache=ROOT/"field-inspector"
        for i,key in enumerate(sample,1):
            try:
                ext=Path(key).suffix;local=cache/(hashlib.sha256(key.encode()).hexdigest()+ext)
                if not local.exists():r.download(key,local)
                x=inspect_one_raw(local)
                for a,b in ((ms,x[0]),(mn,x[1]),(rs,x[2]),(rn,x[3]),(ps,x[4]),(pn,x[5]),(countries,x[9])):inspector_merge(a,b)
                mdn+=x[6];rdn+=x[7];pdn+=x[8]
            except Exception:failed+=1
            if i%max(1,n//100)==0 or i==n:
                admin_update(r,j,status="running",progress=min(99,2+int(i/n*97)),message=f"Inspected {i:,} of {n:,} raw files…",stats={"bucket_raw_files":len(keys),"sampled":i,"failed":failed,"market_definitions":mdn,"runner_definitions":rdn,"price_updates":pdn})
        report={"report_version":1,"generated_at":datetime.now(timezone.utc).isoformat(),"plan":q.plan,"raw_prefix":prefix,"bucket_raw_files":len(keys),"files_sampled":n,"files_failed":failed,"market_definition_observations":mdn,"runner_definition_observations":rdn,"price_update_observations":pdn,"observed_countries":dict(countries),"thresholds":{"RELIABLE":">=99%","MOSTLY":"90-98.99%","SPARSE":"10-89.99%","RARE":"<10%"},"market_fields":inspector_rows(ms,mn,mdn),"runner_fields":inspector_rows(rs,rn,rdn),"price_fields":inspector_rows(ps,pn,pdn)}
        rk=f"field-reports/horse-racing/win/{slug(q.plan)}/{j}.json";r.putj(rk,report)
        admin_update(r,j,status="complete",progress=100,message="Historical field inspection complete.",stats={"bucket_raw_files":len(keys),"sampled":n,"failed":failed,"market_definitions":mdn,"runner_definitions":rdn,"price_updates":pdn},report_key=rk,report=report,elapsed_seconds=round(time.time()-start,2))
    except Exception as e:admin_update(r,j,status="failed",progress=100,message=str(e))

def require_admin(request:Request):
    if not request.session.get("admin"):raise HTTPException(401,"Admin authentication required.")
    return True

class Req(BaseModel):
    from_date:date;to_date:date;countries:list[str]=Field(min_length=1);plan:str="Basic Plan";strategy:str="Lay longest outsider";nth:int=Field(2,ge=1,le=100)
    min_odds:float=Field(1.01,ge=1.01,le=1000);max_odds:float=Field(1000,ge=1.01,le=1000);min_runners:int=Field(2,ge=2);max_runners:int=Field(0,ge=0)
    stake_mode:str="Fixed stake";amount:float=Field(1.0,gt=0);commission:float=Field(2.0,ge=0,le=100)

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
def local(key):
    h=hashlib.sha256(key.encode()).hexdigest();return ROOT/"parquet"/h[:2]/f"{h}.parquet"
def readm(path):
    d=pq.read_table(path).to_pydict()
    if not d.get("market_id"):return None
    rs=[Runner(int(a),str(b),float(c),bool(w)) for a,b,c,w in zip(d["selection_id"],d["horse"],d["bsp"],d["winner"])]
    return Market(str(d["market_id"][0]),str(d["market_time"][0]),str(d["event_name"][0]),str(d["country"][0]).upper(),rs)
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
    keys=set()
    for y,m in months(q.from_date,q.to_date):
        for c in sorted(set(x.upper() for x in q.countries)):
            keys.update(r.list(f"processed/horse-racing/win/{slug(q.plan)}/year={y:04d}/month={m:02d}/country={c}/"))
    return sorted(keys)
def work(j,qd,h):
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
                rr=pick(m,q.strategy,q.nth)
                if rr and q.min_odds<=rr.bsp<=q.max_odds:bets.append(settle(m,rr,q))
            except:skip+=1
            if i%max(1,len(keys)//20)==0:upd(r,j,status="running",progress=min(95,5+int(i/len(keys)*90)),message=f"Processed {i:,} of {len(keys):,} markets…")
        bets.sort(key=lambda x:x.market_time);s=stats(bets);groups={}
        for b in bets:groups.setdefault(band(b.bsp),[]).append(b)
        bands=[]
        for lo,hi in BANDS:
            n=f"{lo:g}–{hi:g}" if hi<1001 else f"{lo:g}+"
            if n in groups:
                z=stats(groups[n]);bands.append({"band":n,"bets":z["bets"],"wins":z["wins"],"strike":z["strike"],"gross":z["gross"],"net":z["net"],"roi":z["stake_roi"]})
        out={"engine_version":ENGINE,"cache_hash":h,"request":norm(q),"stats":s,"bands":bands,"bets":[asdict(x) for x in bets[:MAX_BETS]],"bets_truncated":len(bets)>MAX_BETS,"total_bets":len(bets),"markets_found":len(keys),"skipped":skip,"elapsed_seconds":round(time.time()-t,3)}
        r.putj(rk(h),out);upd(r,j,status="complete",progress=100,message="Backtest complete.",result_hash=h,cached=False,elapsed_seconds=out["elapsed_seconds"])
    except Exception as e:upd(r,j,status="failed",progress=100,message=str(e))

app=FastAPI(title="Betfair Strategy Lab");app.add_middleware(SessionMiddleware,secret_key=os.getenv("ADMIN_SESSION_SECRET",secrets.token_hex(32)),same_site="lax",https_only=os.getenv("COOKIE_SECURE","0")=="1");BASE=Path(__file__).parent
app.mount("/static",StaticFiles(directory=BASE/"static"),name="static");templates=Jinja2Templates(directory=BASE/"templates")
@app.get("/",response_class=HTMLResponse)
def home(request:Request):return templates.TemplateResponse(request=request,name="index.html",context={"countries":COUNTRIES})
@app.get("/api/health")
def health():
    try:r=R2();r.test();return {"ok":True,"r2":True,"workers":WORKERS,"engine":ENGINE}
    except Exception as e:return {"ok":False,"r2":False,"workers":WORKERS,"error":str(e)}
@app.post("/api/jobs",status_code=202)
def create(q:Req):
    if q.from_date>q.to_date:raise HTTPException(400,"From date must be before To date.")
    if q.min_odds>q.max_odds:raise HTTPException(400,"Minimum BSP cannot exceed maximum BSP.")
    q.countries=sorted(set(x.upper().strip() for x in q.countries if x.strip()));r=R2();h=hsh(q);cached=r.getj(rk(h));j=uuid.uuid4().hex
    if cached:upd(r,j,status="complete",progress=100,message="Loaded from persistent result cache.",result_hash=h,cached=True,elapsed_seconds=0);return {"job_id":j,"status":"complete","cached":True}
    upd(r,j,status="queued",progress=0,message="Backtest queued.",result_hash=h,cached=False);POOL.submit(work,j,q.model_dump(mode="json"),h);return {"job_id":j,"status":"queued","cached":False}
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

@app.post("/api/admin/field-inspector/jobs",status_code=202)
def create_field_inspector_job(q:FieldInspectRequest,_:bool=Depends(require_admin)):
    j="inspect-"+uuid.uuid4().hex;r=R2();admin_update(r,j,status="queued",progress=0,message="Historical field inspection queued.",stats={});ADMIN_POOL.submit(run_field_inspection,j,q.model_dump(mode="json"));return {"job_id":j}
@app.get("/api/admin/field-inspector/jobs/{j}")
def get_field_inspector_job(j:str,_:bool=Depends(require_admin)):
    with ADMIN_LOCK:x=ADMIN_JOBS.get(j)
    if x:return x
    x=R2().getj(f"admin-jobs/{j}.json")
    if x:return x
    raise HTTPException(404,"Inspector job not found.")
