#!/usr/bin/env python3
import argparse,bz2,gzip,hashlib,json,math,os,tempfile
from datetime import datetime,timezone
from pathlib import Path
import boto3,pyarrow as pa,pyarrow.parquet as pq
def slug(s):
 import re;return re.sub(r"[^a-z0-9]+","-",s.lower()).strip("-")
def sf(v):
 try:
  x=float(v);return x if math.isfinite(x) else None
 except:return None
def client():
 need=["R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY","R2_ENDPOINT","R2_BUCKET"];miss=[x for x in need if not os.getenv(x)]
 if miss:raise SystemExit("Missing: "+", ".join(miss))
 return boto3.client("s3",endpoint_url=os.environ["R2_ENDPOINT"],aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],region_name="auto")
def listing(s3,b,p):
 token=None
 while True:
  kw={"Bucket":b,"Prefix":p,"MaxKeys":1000}
  if token:kw["ContinuationToken"]=token
  z=s3.list_objects_v2(**kw)
  for o in z.get("Contents",[]):yield o["Key"]
  if not z.get("IsTruncated"):break
  token=z.get("NextContinuationToken")
def op(p):
 return bz2.open(p,"rt",encoding="utf8",errors="ignore") if p.suffix.lower()==".bz2" else gzip.open(p,"rt",encoding="utf8",errors="ignore") if p.suffix.lower()==".gz" else open(p,"rt",encoding="utf8",errors="ignore")
def parse(p):
 latest=None;mid=p.stem;ltp={}
 with op(p) as f:
  for line in f:
   try:msg=json.loads(line)
   except:continue
   for mc in msg.get("mc",[]) if isinstance(msg,dict) else []:
    if mc.get("id"):mid=str(mc["id"])
    if isinstance(mc.get("marketDefinition"),dict):latest=mc["marketDefinition"]
    for rc in mc.get("rc",[]) or []:
     if isinstance(rc,dict) and rc.get("id") is not None and sf(rc.get("ltp")) is not None:ltp[int(rc["id"])]=sf(rc["ltp"])
 if not latest or str(latest.get("status","")).upper()!="CLOSED" or str(latest.get("marketType","")).upper()!="WIN":return None
 rr=[];wins=0
 for x in latest.get("runners",[]) or []:
  status=str(x.get("status","")).upper()
  if status=="REMOVED":continue
  bsp=sf(x.get("bsp"));sid=x.get("id")
  if sid is None or bsp is None or not 1.01<=bsp<=1000:continue
  win=status=="WINNER";wins+=int(win);rr.append({"selection_id":int(sid),"horse":str(x.get("name") or f"Selection {sid}"),"bsp":bsp,"winner":win,"adjustment_factor":sf(x.get("adjustmentFactor")),"sort_priority":x.get("sortPriority"),"runner_status":status,"ltp":ltp.get(int(sid))})
 if len(rr)<2 or wins!=1:return None
 return {"market_id":mid,"market_time":str(latest.get("marketTime") or latest.get("openDate") or ""),"event_name":str(latest.get("eventName") or latest.get("name") or ""),"country":str(latest.get("countryCode") or "").upper(),"venue":str(latest.get("venue") or ""),"bet_delay":latest.get("betDelay"),"betting_type":str(latest.get("bettingType") or ""),"market_base_rate":sf(latest.get("marketBaseRate")),"number_of_winners":latest.get("numberOfWinners"),"in_play_enabled":latest.get("inPlay"),"cross_matching":latest.get("crossMatching"),"discount_allowed":latest.get("discountAllowed"),"persistence_enabled":latest.get("persistenceEnabled"),"runners":rr}
def key(m,plan,src):
 try:d=datetime.fromisoformat(m["market_time"].replace("Z","+00:00"))
 except:d=datetime(1970,1,1,tzinfo=timezone.utc)
 return f"processed-enriched/horse-racing/win/{slug(plan)}/year={d.year:04d}/month={d.month:02d}/country={m['country'] or 'XX'}/{hashlib.sha256(src.encode()).hexdigest()[:24]}-{m['market_id'].replace('.','_')}.parquet"
def write(m,p,plan,src):
 r=m["runners"];n=len(r);cols={"schema_version":[2]*n,"source_id":[hashlib.sha256(src.encode()).hexdigest()[:24]]*n,"plan":[plan]*n,"market_id":[m["market_id"]]*n,"market_time":[m["market_time"]]*n,"event_name":[m["event_name"]]*n,"country":[m["country"]]*n,"venue":[m["venue"]]*n,"runner_count":[n]*n,"bet_delay":[m["bet_delay"]]*n,"betting_type":[m["betting_type"]]*n,"market_base_rate":[m["market_base_rate"]]*n,"number_of_winners":[m["number_of_winners"]]*n,"in_play_enabled":[m["in_play_enabled"]]*n,"cross_matching":[m["cross_matching"]]*n,"discount_allowed":[m["discount_allowed"]]*n,"persistence_enabled":[m["persistence_enabled"]]*n}
 for k in ("selection_id","horse","bsp","winner","adjustment_factor","sort_priority","runner_status","ltp"):cols[k]=[x.get(k) for x in r]
 pq.write_table(pa.table(cols),p,compression="zstd")
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--plan",default="Basic Plan");ap.add_argument("--limit",type=int,default=0);ap.add_argument("--overwrite",action="store_true");a=ap.parse_args()
 s3=client();b=os.environ["R2_BUCKET"];prefix=f"raw/horse-racing/win/{slug(a.plan)}/";raw=[k for k in listing(s3,b,prefix) if k.lower().endswith((".bz2",".gz",".json",".jsonl",".txt"))]
 if a.limit:raw=raw[:a.limit]
 print(f"Raw files to process: {len(raw):,}");ok=skip=bad=0
 with tempfile.TemporaryDirectory(prefix="betfair-enrich-") as td:
  td=Path(td)
  for i,k in enumerate(raw,1):
   try:
    lp=td/(hashlib.sha256(k.encode()).hexdigest()+Path(k).suffix);s3.download_file(b,k,str(lp));m=parse(lp)
    if not m:bad+=1;continue
    target=key(m,a.plan,k);exists=False
    try:s3.head_object(Bucket=b,Key=target);exists=True
    except:pass
    if exists and not a.overwrite:skip+=1
    else:
     pp=td/"out.parquet";write(m,pp,a.plan,k);s3.upload_file(str(pp),b,target);ok+=1
   except Exception as e:bad+=1;print(f"\nFAILED {k}: {e}")
   if i==1 or i==len(raw) or i%max(1,len(raw)//100)==0:print(f"\r{i:,}/{len(raw):,} enriched={ok:,} existing={skip:,} skipped/failed={bad:,}",end="",flush=True)
 print("\nDone.")
if __name__=="__main__":main()
