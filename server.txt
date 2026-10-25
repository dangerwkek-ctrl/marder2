const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const{WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000,DB=process.env.DB_FILE||path.join(__dirname,'users.json');
let U=Object.create(null);try{Object.assign(U,JSON.parse(fs.readFileSync(DB,'utf8')))}catch(e){if(e.code!=='ENOENT')try{fs.copyFileSync(DB,DB+'.bad')}catch(_){}}
let EP='';try{EP=fs.readFileSync(DB+'.ep','utf8')}catch(_){}if(!EP){EP=crypto.randomBytes(6).toString('hex');try{fs.writeFileSync(DB+'.ep',EP)}catch(_){}}
const UR=process.env.UPSTASH_REDIS_REST_URL,UT=process.env.UPSTASH_REDIS_REST_TOKEN;
const rest=async cmd=>{const r=await fetch(UR,{method:'POST',headers:{Authorization:'Bearer '+UT,'content-type':'application/json'},body:JSON.stringify(cmd)});const j=await r.json();if(j.error)throw new Error(j.error);return j.result};
let rst=0,last='';const pushRemote=async()=>{if(!UR)return;const d=JSON.stringify({u:U,ep:EP});if(d===last)return;try{await rest(['SET','mmdb',d]);last=d}catch(e){console.log('remote save failed:',e.message)}};
async function loadRemote(){if(!UR)return;try{const j=await rest(['GET','mmdb']);if(j){const o=JSON.parse(j);Object.assign(U,o.u||{});if(o.ep)EP=o.ep;last=j;console.log('db loaded from remote:',Object.keys(U).length,'users')}else await pushRemote()}catch(e){console.log('remote load failed:',e.message)}}
let st=0;const flush=()=>{try{fs.writeFileSync(DB+'.tmp',JSON.stringify(U));fs.renameSync(DB+'.tmp',DB)}catch(_){}clearTimeout(rst);rst=setTimeout(pushRemote,UR?1500:0)},save=()=>{clearTimeout(st);st=setTimeout(flush,300)};
for(const sg of['SIGTERM','SIGINT'])process.on(sg,()=>{try{fs.writeFileSync(DB+'.tmp',JSON.stringify(U));fs.renameSync(DB+'.tmp',DB)}catch(_){}pushRemote().finally(()=>process.exit(0));setTimeout(()=>process.exit(0),4000)});
const key=n=>n.toLowerCase(),hash=(p,s)=>crypto.scryptSync(p,s,32).toString('hex'),rm=(a,x)=>{const i=a.indexOf(x);if(i>=0)a.splice(i,1)};
const server=http.createServer((q,r)=>{if(q.url==='/health'){r.end('ok');return}
fs.readFile(path.join(__dirname,'index.html'),(e,d)=>{if(e){r.writeHead(404);r.end('index.html not found');return}r.writeHead(200,{'content-type':'text/html; charset=utf-8'});r.end(d)})});
const wss=new WebSocketServer({server,maxPayload:20000}),on=new Map();
const send=(w,o)=>{if(w.readyState===1)w.send(JSON.stringify(o))},isOn=k=>on.has(k)&&on.get(k).size>0,toUser=(k,o)=>{for(const w of on.get(k)||[])send(w,o)};
const snap=k=>{const u=U[k],nm=x=>U[x]?U[x].name:x;return{t:'friends',friends:u.fr.map(x=>({name:nm(x),online:isOn(x),av:U[x]&&U[x].av,...rinfo(x)})),incoming:u.inc.map(nm),outgoing:u.out.map(nm)}};
const rinfo=k=>{for(const w of on.get(k)||[]){const r=w.room;if(r)return{rs:r.phase,rid:r.m.length<MAXH?r.id:undefined}}return{}};
const push=k=>{if(U[k])toUser(k,snap(k))};
function prof(ws,name){const k=typeof name==='string'?key(name.trim()):'',o=U[k],me=U[ws.k];
if(!o)return send(ws,{t:'profile',found:0,q:String(name||'').slice(0,16)});
const rel=k===ws.k?'self':me.fr.includes(k)?'friend':me.out.includes(k)?'sent':me.inc.includes(k)?'received':'none';
send(ws,{t:'profile',found:1,name:o.name,online:isOn(k),rel,friends:o.fr.length,av:o.av||null,pf:o.pf||null})}
function befriend(a,b){for(const[x,y]of[[a,b],[b,a]]){rm(U[x].inc,y);rm(U[x].out,y);if(!U[x].fr.includes(y))U[x].fr.push(y)}}
function enter(ws,k,t){ws.k=k;if(!on.has(k))on.set(k,new Set());const first=!on.get(k).size;on.get(k).add(ws);
send(ws,{t:'auth',ok:1,name:U[k].name,token:t,ep:EP});send(ws,snap(k));if(first)U[k].fr.forEach(push)}
function auth(ws,m){const fail=(err,resume)=>{send(ws,{t:'auth',ok:0,err,resume});if(++ws.bad>8)ws.close()};
if(m.t==='resume'){if(typeof m.token!=='string')return fail('',1);const k=Object.keys(U).find(x=>U[x].tok.includes(m.token));return k?enter(ws,k,m.token):fail('',1)}
if(m.t!=='reg'&&m.t!=='login')return;
const n=String(m.name||'').trim(),p=String(m.pass||''),k=key(n);
if(!/^[\p{L}\d_]{3,16}$/u.test(n))return fail('Ник: 3–16 символов, буквы/цифры/_');
if(p.length<4||p.length>64)return fail('Пароль: от 4 символов');
if(m.t==='reg'){if(U[k])return fail('Ник уже занят');const salt=crypto.randomBytes(16).toString('hex');U[k]={name:n,salt,hash:hash(p,salt),tok:[],fr:[],inc:[],out:[]}}
else{const u=U[k];if(!u||!crypto.timingSafeEqual(Buffer.from(hash(p,u.salt)),Buffer.from(u.hash)))return fail('Неверный ник или пароль')}
const t=crypto.randomBytes(24).toString('hex');U[k].tok=[...U[k].tok.slice(-4),t];save();enter(ws,k,t)}

const rooms=new Map();let rid=1;const MAXH=8,hn=w=>U[w.k]?U[w.k].name:'?';
const rchk=r=>{if(r.wait&&r.m.length>=2){r.wait=0;r.end=Date.now()+20000}};
const rbots=r=>{const n=Math.max(0,10-r.m.length);if(r.bots.length>n)r.bots.length=n;while(r.bots.length<n)r.bots.push((Math.random()*4)|0)};
const rcount=r=>{const v=[0,0,0,0];for(const x of r.votes.values())v[x]++;for(const b of r.bots)v[b]++;return v};
const rstate=(r,w)=>({t:'room',id:r.id,host:hn(r.host),phase:r.phase,left:r.wait?-1:Math.max(0,r.end-Date.now()),wait:r.wait?1:0,avs:r.m.map(x=>x.av&&x.av.a||null),pv:r.m.map(w=>r.votes.has(w)?r.votes.get(w):-1),players:r.m.map(hn),bots:10-r.m.length,votes:rcount(r),my:r.votes.has(w)?r.votes.get(w):-1});
const rbc=r=>{for(const w of r.m)send(w,rstate(r,w))};
function rgo(r){const v=rcount(r),mx=Math.max(...v),c=v.map((x,i)=>x===mx?i:-1).filter(i=>i>=0),map=c[(Math.random()*c.length)|0];r.phase='play';const names=r.m.map(hn);for(const w of r.m)send(w,{t:'rstart',map,names,avs:r.m.map(x=>x.av||null)});rfr(r)}
function rleave(w){const r=w.room;if(!r)return;const mem=[...r.m];rleave0(w);mem.forEach(x=>U[x.k]&&U[x.k].fr.forEach(push))}
const rfr=r=>r.m.forEach(x=>U[x.k]&&U[x.k].fr.forEach(push));
function rleave0(w){const r=w.room;if(!r)return;w.room=null;const i=r.m.indexOf(w);if(i>=0)r.m.splice(i,1);r.votes.delete(w);
if(w===r.host){if(r.phase==='lobby'&&r.m.length){r.host=r.m[0];rbots(r);rbc(r);return}for(const x of r.m){x.room=null;send(x,{t:'rend',why:'Хост вышел из игры'})}rooms.delete(r.id);return}
if(r.phase==='play')send(r.host,{t:'gleave',name:hn(w)});else{rbots(r);rbc(r)}}

const trades=new Map();let tid=1;const okId=v=>typeof v==='string'&&/^[a-z0-9_]{2,20}$/.test(v),NOTR=['knife','pistol'],myown=w=>(U[w.k]&&U[w.k].pf&&U[w.k].pf.own)||['knife','pistol'];
function tstate(t){for(const[w,o,x]of[[t.a,t.oa,t.ob],[t.b,t.ob,t.oa]])send(w,{t:'tr',ev:'st',id:t.id,mine:o.item,theirs:x.item,mok:o.ok?1:0,tok:x.ok?1:0})}
function tclose(ws){for(const t of[...trades.values()])if(t.a===ws||t.b===ws){trades.delete(t.id);for(const w of[t.a,t.b])if(w&&w!==ws)send(w,{t:'tr',ev:'cancel',id:t.id,why:'Игрок вышел'})}}
wss.on('connection',ws=>{ws.k=null;ws.bad=0;ws.alive=true;ws.on('pong',()=>ws.alive=true);
ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch(_){return}if(!m||typeof m.t!=='string')return;
if(!ws.k)return auth(ws,m);const me=U[ws.k];if(!me)return;
if(m.t==='rav'){if(m.a&&typeof m.a==='object'&&JSON.stringify(m.a).length<500)ws.av=m.a;return}
if(m.t==='rcreate'){rleave(ws);const r={id:String(rid++),host:ws,m:[ws],phase:'lobby',votes:new Map(),bots:[],end:Date.now()+20000,inv:new Set()};ws.room=r;rooms.set(r.id,r);rbots(r);rbc(r);return rfr(r)}
if(m.t==='rjoin'){const r=rooms.get(String(m.room));if(!r||ws.room===r||r.m.length>=MAXH)return send(ws,{t:'err',err:'Лобби недоступно'});if(!(r.pub||r.inv.has(ws.k)||r.m.some(x=>U[x.k]&&U[x.k].fr.includes(ws.k))))return send(ws,{t:'err',err:'Нет доступа к лобби'});
rleave(ws);ws.room=r;r.m.push(ws);if(r.phase==='lobby'){rchk(r);rbots(r);r.end=Math.max(r.end,Date.now()+12000);rbc(r)}else{send(ws,{t:'rlate',host:hn(r.host),id:r.id});send(r.host,{t:'gjoin',name:hn(ws)})}return rfr(r)}
if(m.t==='rmatch'){rleave(ws);const n=Date.now();let r=null;for(const x of rooms.values())if(x.pub&&x.phase==='lobby'&&x.m.length<MAXH&&x.end-n>1500&&(!r||x.m.length>r.m.length))r=x;if(!r){r={id:String(rid++),host:ws,m:[],phase:'lobby',votes:new Map(),bots:[],end:Date.now()+20000,wait:0,inv:new Set(),pub:1};rooms.set(r.id,r)}ws.room=r;r.m.push(ws);rchk(r);rbots(r);r.t0=r.t0||n;r.end=Math.min(Math.max(r.end,n+8000),r.t0+45000);rbc(r);return rfr(r)}
if(m.t==='myav'){const a=m.a;if(a&&typeof a==='object'){const f=(v,x)=>(v|0)>=0&&(v|0)<=x?v|0:0;me.av={h:f(a.h,13),t:f(a.t,13),l:f(a.l,13),hat:f(a.hat,7),f:f(a.f,5),cl:f(a.cl,5)};save();me.fr.forEach(push)}return}
if(m.t==='rvote'){const r=ws.room;const v=m.map|0;if(r&&r.phase==='lobby'&&v>=-1&&v<4){v<0?r.votes.delete(ws):r.votes.set(ws,v);rbc(r)}return}
if(m.t==='rstart'){const r=ws.room;if(r&&r.host===ws&&r.phase==='lobby'&&(r.m.length===1||!r.pub))rgo(r);return}
if(m.t==='rleave')return rleave(ws);
if(m.t==='rinv'){const r=ws.room;if(!r||r.phase!=='lobby'||typeof m.name!=='string')return;const k=key(m.name.trim());if(!me.fr.includes(k)||!isOn(k))return;r.inv.add(k);toUser(k,{t:'rinv',from:me.name,room:r.id});return}
if(m.t==='g'){const r=ws.room;if(!r||!m.d||typeof m.d!=='object')return;if(r.phase==='lobby'){if(m.d.k==='lp')for(const w of r.m)if(w!==ws)send(w,{t:'g',d:m.d,from:hn(ws)});return}if(r.phase!=='play')return;if(ws===r.host){for(const w of r.m)if(w!==ws&&(!m.to||hn(w)===m.to))send(w,{t:'g',d:m.d})}else send(r.host,{t:'g',d:m.d,from:hn(ws)});return}
if(m.t==='find')return prof(ws,m.name);
if(m.t==='myprof'){const a=Array.isArray(m.own)?m.own.filter(okId).slice(0,40):[];const np={own:[...new Set(a)],k:okId(m.k)?m.k:'knife',g:okId(m.g)?m.g:'pistol',c:Math.min(1e9,Math.max(0,m.c|0))};if(JSON.stringify(np)!==JSON.stringify(me.pf)){me.pf=np;save()}return}
if(m.t==='chat'){const txt=String(m.text||'').replace(/\s+/g,' ').trim().slice(0,120),n=Date.now();if(!txt||n-(ws.ct||0)<600)return;ws.ct=n;
if(m.to){const k=key(String(m.to).trim());if(!U[k]||!me.fr.includes(k))return send(ws,{t:'err',err:'Личные сообщения только друзьям'});if(!isOn(k))return send(ws,{t:'err',err:'Друг не в сети'});toUser(k,{t:'chat',from:me.name,text:txt,dm:1});send(ws,{t:'chat',from:me.name,to:U[k].name,text:txt,dm:1});return}
const r=ws.room;if(!r)return send(ws,{t:'err',err:'Общий чат работает в лобби и матче. Другу: @ник текст'});for(const w of r.m)send(w,{t:'chat',from:me.name,text:txt});return}
if(m.t==='tinv'){const k=key(String(m.name||'').trim());const inR=ws.room&&ws.room.m.some(w=>w!==ws&&w.k===k);if(!U[k]||k===ws.k||!(inR||(me.fr.includes(k)&&isOn(k))))return send(ws,{t:'err',err:'Игрок недоступен для обмена'});const id=String(tid++);trades.set(id,{id,a:ws,b:null,oa:{item:'',ok:0},ob:{item:'',ok:0},to:k});toUser(k,{t:'tr',ev:'inv',id,from:me.name});send(ws,{t:'tr',ev:'wait',id});return}
if(m.t==='tjoin'){const t=trades.get(String(m.id));if(!t||t.b||t.to!==ws.k)return;t.b=ws;send(t.a,{t:'tr',ev:'open',id:t.id,with:hn(ws)});send(ws,{t:'tr',ev:'open',id:t.id,with:hn(t.a)});tstate(t);return}
if(m.t==='tdec'||m.t==='tcancel'){const t=trades.get(String(m.id));if(!t||(t.a!==ws&&t.b!==ws&&t.to!==ws.k))return;trades.delete(t.id);for(const w of[t.a,t.b])if(w&&w!==ws)send(w,{t:'tr',ev:'cancel',id:t.id,why:hn(ws)+' отменил(а) обмен'});return}
if(m.t==='tset'||m.t==='tok'){const t=trades.get(String(m.id));if(!t||!t.b)return;const mine=t.a===ws?t.oa:t.b===ws?t.ob:null;if(!mine)return;const ow=t.a===ws?t.b:t.a;
if(m.t==='tset'){const it=m.item;if(it!==''){if(!okId(it)||NOTR.includes(it)||!myown(ws).includes(it))return send(ws,{t:'err',err:'Этот предмет нельзя обменять'});if(myown(ow).includes(it))return send(ws,{t:'err',err:'У игрока уже есть такое оружие'})}mine.item=it;t.oa.ok=t.ob.ok=0;return tstate(t)}
if(!t.oa.item||!t.ob.item)return;mine.ok=1;if(!(t.oa.ok&&t.ob.ok))return tstate(t);
const A=U[t.a.k],B=U[t.b.k];if(!myown(t.a).includes(t.oa.item)||!myown(t.b).includes(t.ob.item)||myown(t.a).includes(t.ob.item)||myown(t.b).includes(t.oa.item)){trades.delete(t.id);for(const w of[t.a,t.b])send(w,{t:'tr',ev:'cancel',id:t.id,why:'Обмен не удался: список оружия изменился'});return}
A.pf.own=A.pf.own.filter(x=>x!==t.oa.item).concat(t.ob.item);B.pf.own=B.pf.own.filter(x=>x!==t.ob.item).concat(t.oa.item);trades.delete(t.id);save();
send(t.a,{t:'tr',ev:'done',id:t.id,give:t.oa.item,get:t.ob.item});send(t.b,{t:'tr',ev:'done',id:t.id,give:t.ob.item,get:t.oa.item});return}

if(typeof m.name!=='string')return;const k=key(m.name.trim()),o=U[k];if(!o||k===ws.k)return;
if(m.t==='req'){if(me.fr.includes(k))return;if(me.inc.includes(k)){befriend(ws.k,k);toUser(k,{t:'accepted',by:me.name})}
else if(!me.out.includes(k)){if(o.inc.length>=50)return send(ws,{t:'err',err:'У игрока слишком много заявок'});me.out.push(k);o.inc.push(ws.k);toUser(k,{t:'reqin',from:me.name})}}
else if(m.t==='acc'){if(!me.inc.includes(k))return;befriend(ws.k,k);toUser(k,{t:'accepted',by:me.name})}
else if(m.t==='dec'){rm(me.inc,k);rm(o.out,ws.k)}
else if(m.t==='rem'){rm(me.fr,k);rm(o.fr,ws.k)}
else return;
save();push(ws.k);push(k);prof(ws,o.name)});
ws.on('close',()=>{rleave(ws);tclose(ws);if(!ws.k)return;const s=on.get(ws.k);if(s){s.delete(ws);if(!s.size){on.delete(ws.k);U[ws.k]&&U[ws.k].fr.forEach(push)}}})});
setInterval(()=>{for(const w of wss.clients){if(!w.alive){w.terminate();continue}w.alive=false;w.ping()}},25000);
setInterval(()=>{const n=Date.now();for(const r of[...rooms.values()])if(r.phase==='lobby'&&n>=r.end)rgo(r)},500);
setInterval(()=>{for(const r of rooms.values())if(r.phase==='lobby'){if(r.bots.length&&Math.random()<.45)r.bots[(Math.random()*r.bots.length)|0]=(Math.random()*4)|0;rbc(r)}},1500);
loadRemote().finally(()=>server.listen(PORT,()=>console.log('Server on '+PORT)));
