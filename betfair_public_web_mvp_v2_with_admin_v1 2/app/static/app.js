const $=x=>document.getElementById(x),v=x=>$(x).value;let timer;
const cs=()=>[...document.querySelectorAll('.countries input:checked')].map(x=>x.value);
let t=new Date(),f=new Date();f.setMonth(f.getMonth()-1);$('to').value=t.toISOString().slice(0,10);$('from').value=f.toISOString().slice(0,10);
async function jf(url,opt={}){let r=await fetch(url,opt),txt=await r.text(),j;try{j=JSON.parse(txt)}catch{throw Error(txt||`HTTP ${r.status}`)}if(!r.ok)throw Error(j.detail||j.error||`HTTP ${r.status}`);return j}
fetch('/api/health').then(r=>r.json()).then(x=>$('health').textContent=x.r2?`● R2 online · ${x.workers} workers`:'● Storage unavailable');
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{document.querySelectorAll('.tab,.panel').forEach(x=>x.classList.remove('active'));b.classList.add('active');$(b.dataset.tab).classList.add('active')});
function body(){return{from_date:v('from'),to_date:v('to'),countries:cs(),plan:v('plan'),strategy:v('strategy'),nth:+v('nth'),min_odds:+v('minOdds'),max_odds:+v('maxOdds'),min_runners:+v('minRunners'),max_runners:+v('maxRunners'),stake_mode:v('stakeMode'),amount:+v('amount'),commission:+v('commission')}}
$('run').onclick=async()=>{if(!cs().length)return alert('Select at least one country.');try{$('run').disabled=true;$('cached').classList.add('hide');$('job').classList.remove('hide');$('jobmsg').textContent='Submitting backtest…';let j=await jf('/api/jobs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body())});$('jobid').textContent='Job '+j.job_id;poll(j.job_id)}catch(e){fail(e.message)}};
async function poll(id){clearTimeout(timer);try{let j=await jf('/api/jobs/'+id);$('jobmsg').textContent=j.message||j.status;$('pct').textContent=(j.progress||0)+'%';$('bar').style.width=(j.progress||0)+'%';if(j.status==='complete'){render(await jf('/api/jobs/'+id+'/result'),j);$('run').disabled=false;return}if(j.status==='failed'){fail(j.message);return}timer=setTimeout(()=>poll(id),900)}catch(e){fail(e.message)}}
function fail(m){$('jobmsg').innerHTML='<span class="error">'+esc(m)+'</span>';$('title').textContent='Backtest could not complete';$('sub').textContent=m;$('run').disabled=false}
const money=x=>'£'+Number(x||0).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}),esc=s=>String(s).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[m]));
function render(r,j){let s=r.stats;$('title').textContent='Backtest complete';$('sub').textContent=`${r.request.from_date} → ${r.request.to_date} · ${r.request.countries.join(', ')} · ${r.request.strategy}`;$('cached').classList.toggle('hide',!j.cached);$('betsM').textContent=s.bets.toLocaleString();$('strikeM').textContent=s.strike.toFixed(2)+'%';$('netM').textContent=money(s.net);$('roiM').textContent=s.stake_roi.toFixed(2)+'%';$('ddM').textContent=money(s.max_drawdown);$('bspM').textContent=s.avg_bsp.toFixed(2);$('bandrows').innerHTML=r.bands.map(x=>`<tr><td>${x.band}</td><td>${x.bets}</td><td>${x.wins}</td><td>${x.strike.toFixed(2)}%</td><td>${money(x.gross)}</td><td>${money(x.net)}</td><td>${x.roi.toFixed(2)}%</td></tr>`).join('');$('betrows').innerHTML=r.bets.map(x=>`<tr><td>${esc(x.market_time)}</td><td>${esc(x.country)}</td><td>${esc(x.event_name)}</td><td>${esc(x.horse)}</td><td>${x.bsp.toFixed(2)}</td><td>${x.bet_type}</td><td>${x.won?'WIN':'LOSS'}</td><td>${money(x.net)}</td></tr>`).join('');$('trunc').textContent=r.bets_truncated?`Showing first ${r.bets.length.toLocaleString()} of ${r.total_bets.toLocaleString()} bets.`:'';$('detailgrid').innerHTML=[['Job ID',j.job_id],['Result cache',j.cached?'Persistent cache hit':'New calculation'],['Markets found',r.markets_found],['Markets skipped',r.skipped],['Elapsed',r.elapsed_seconds+' sec'],['Engine',r.engine_version]].map(x=>`<div><span>${x[0]}</span><b>${esc(x[1])}</b></div>`).join('');draw(r.graph_points||[],s.equity)}
let chartState={points:[],plot:[],left:76,right:22,top:22,bottom:58};
function fmtDate(v,full=false){let d=new Date(v);if(isNaN(d))return v||'';return d.toLocaleString(undefined,full?{day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'}:{day:'2-digit',month:'short',year:'2-digit'})}
function niceStep(range,target=5){let rough=Math.max(range/target,.01),p=Math.pow(10,Math.floor(Math.log10(rough))),n=rough/p;return (n<=1?1:n<=2?2:n<=5?5:10)*p}
function draw(points,legacy=[]){
 let c=$('chart'),ctx=c.getContext('2d'),d=devicePixelRatio||1,w=c.clientWidth,h=c.clientHeight;
 c.width=w*d;c.height=h*d;ctx.setTransform(d,0,0,d,0,0);ctx.clearRect(0,0,w,h);
 let data=points.length?points:legacy.map((v,i)=>({market_time:String(i+1),cumulative:v,event_name:'',horse:'',bsp:0,bet_net:0}));
 $('empty').style.display=data.length?'none':'block';if(!data.length)return;
 let L=chartState.left,R=chartState.right,T=chartState.top,B=chartState.bottom,pw=Math.max(10,w-L-R),ph=Math.max(10,h-T-B);
 let vals=[0,...data.map(x=>Number(x.cumulative)||0)],mn=Math.min(...vals),mx=Math.max(...vals),pad=(mx-mn)*.08||1;mn-=pad;mx+=pad;
 let step=niceStep(mx-mn),y0=Math.floor(mn/step)*step,y1=Math.ceil(mx/step)*step;
 ctx.font='12px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif';ctx.textBaseline='middle';
 ctx.strokeStyle='#e6ebf0';ctx.fillStyle='#718096';ctx.lineWidth=1;
 for(let yv=y0;yv<=y1+step*.1;yv+=step){let y=T+ph*(y1-yv)/(y1-y0);ctx.beginPath();ctx.moveTo(L,y);ctx.lineTo(w-R,y);ctx.stroke();ctx.textAlign='right';ctx.fillText(money(yv),L-10,y)}
 ctx.save();ctx.translate(18,T+ph/2);ctx.rotate(-Math.PI/2);ctx.textAlign='center';ctx.fillStyle='#52606d';ctx.fillText('Cumulative profit / loss (£)',0,0);ctx.restore();
 let n=data.length,ticks=Math.min(6,n),idxs=[...new Set(Array.from({length:ticks},(_,k)=>Math.round(k*(n-1)/Math.max(1,ticks-1))))];
 ctx.textAlign='center';ctx.textBaseline='top';ctx.fillStyle='#718096';
 idxs.forEach(i=>{let x=L+pw*i/Math.max(1,n-1);ctx.strokeStyle='#e6ebf0';ctx.beginPath();ctx.moveTo(x,T);ctx.lineTo(x,T+ph);ctx.stroke();ctx.fillText(fmtDate(data[i].market_time),x,T+ph+9)});
 ctx.fillStyle='#52606d';ctx.fillText('Race date / time',L+pw/2,h-18);
 chartState.plot=data.map((p,i)=>({x:L+pw*i/Math.max(1,n-1),y:T+ph*(y1-(Number(p.cumulative)||0))/(y1-y0),p}));
 ctx.strokeStyle='#2463eb';ctx.lineWidth=2;ctx.beginPath();chartState.plot.forEach((q,i)=>i?ctx.lineTo(q.x,q.y):ctx.moveTo(q.x,q.y));ctx.stroke();
 chartState.points=data;
}
function chartHover(ev){
 let c=$('chart');if(!chartState.plot.length)return;let r=c.getBoundingClientRect(),mx=ev.clientX-r.left;
 let q=chartState.plot.reduce((a,b)=>Math.abs(b.x-mx)<Math.abs(a.x-mx)?b:a),p=q.p;
 let tip=$('charttip');if(!tip){tip=document.createElement('div');tip.id='charttip';tip.className='charttip';c.parentElement.appendChild(tip)}
 tip.innerHTML=`<b>${esc(fmtDate(p.market_time,true))}</b><span>${esc(p.event_name||'')}</span><span>${esc(p.horse||'')} · BSP ${Number(p.bsp||0).toFixed(2)} · ${esc(p.bet_type||'')}</span><span>Bet P/L: <b>${money(p.bet_net)}</b></span><span>Cumulative P/L: <b>${money(p.cumulative)}</b></span>`;
 tip.style.display='block';let x=Math.min(Math.max(q.x+14,8),r.width-230),y=Math.max(8,q.y-78);tip.style.left=x+'px';tip.style.top=y+'px';
}
$('chart').addEventListener('mousemove',chartHover);$('chart').addEventListener('mouseleave',()=>{let t=$('charttip');if(t)t.style.display='none'});
window.addEventListener('resize',()=>{if(chartState.points.length)draw(chartState.points)});
