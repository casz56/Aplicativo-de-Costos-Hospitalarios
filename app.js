
function stripAccents(s){
  return (s ?? "").toString().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}
function normText(s){
  return stripAccents(s).toLowerCase();
}
/* v3 - Contenedor histórico (IndexedDB) para acumular 2020–2033 */
const store = {
  route: "menu",
  filters: { centroCostoText: "", vigencia: [], mes: [], uf: [] },
  session: { costosMes: [] },
  canonical: { costosMes: [] },
  vault: { loaded: false, files: [] }
};

const MONTHS = ["enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"];

// --- AutoSize global (Chart.js) ---
if (window.Chart) {
  Chart.defaults.responsive = true;
  Chart.defaults.font = Chart.defaults.font || {};
  Chart.defaults.font.size = 10;
  Chart.defaults.maintainAspectRatio = false;   // permite que el canvas use el alto del contenedor
  Chart.defaults.animation = false;             // más fluido al redimensionar
  Chart.defaults.plugins.legend.labels.boxWidth = 12;
  Chart.defaults.plugins.legend.labels.boxHeight = 12;
  Chart.defaults.plugins.legend.labels.font = Chart.defaults.plugins.legend.labels.font || {};
  Chart.defaults.plugins.legend.labels.font.size = 10;
}

// Observa cambios de tamaño y fuerza resize de gráficos
const __resizeObserver = new ResizeObserver(() => {
  if (!window.__charts) return;
  for (const k of Object.keys(window.__charts)) {
    try { window.__charts[k].resize(); } catch(e) {}
  }
});
window.addEventListener("load", () => {
  const root = document.querySelector(".content") || document.body;
  __resizeObserver.observe(root);
});

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

// --- Logo Base64 para export PDF ---
async function loadPdfLogo(){
  try{
    const resp = await fetch("assets/logo-huhmp-blanco.png", { cache: "no-store" });
    if(!resp.ok) return;
    const blob = await resp.blob();
    const b64 = await new Promise((resolve)=>{
      const fr = new FileReader();
      fr.onload = ()=> resolve(fr.result);
      fr.readAsDataURL(blob);
    });
    // jsPDF acepta DataURL directamente
    window.PDF_LOGO_BASE64 = b64;
  }catch(e){ /* ignore */ }
}
window.addEventListener("load", () => { loadPdfLogo(); });


/* ---------- IndexedDB ---------- */
const DB_NAME = "visor_costos_db";
const DB_VERSION = 1;
const STORE_ROWS = "costos_mes";
const STORE_FILES = "sources";

function openDB(){
  return new Promise((resolve, reject)=>{
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = ()=>{
      const db = req.result;
      if(!db.objectStoreNames.contains(STORE_ROWS)){
        const os = db.createObjectStore(STORE_ROWS, { keyPath: "id" });
        os.createIndex("vigencia", "vigencia", { unique:false });
        os.createIndex("cc", "cc", { unique:false });
      }
      if(!db.objectStoreNames.contains(STORE_FILES)){
        db.createObjectStore(STORE_FILES, { keyPath: "id" });
      }
    };
    req.onsuccess = ()=> resolve(req.result);
    req.onerror = ()=> reject(req.error);
  });
}
function tx(db, storeName, mode="readonly"){ return db.transaction(storeName, mode).objectStore(storeName); }

function makeRowId(r){
  return [r.vigencia||"", r.mes||"", r.cc||"", r.centro||"", r.uf||"Sin UF"].join("||").toLowerCase();
}

async function vaultLoad(){
  const db = await openDB();
  const rows = await new Promise((resolve)=>{
    const out=[]; const req = tx(db, STORE_ROWS).openCursor();
    req.onsuccess = (e)=>{ const cur=e.target.result; if(cur){ out.push(cur.value); cur.continue(); } else resolve(out); };
    req.onerror = ()=> resolve([]);
  });
  const files = await new Promise((resolve)=>{
    const out=[]; const req = tx(db, STORE_FILES).openCursor();
    req.onsuccess = (e)=>{ const cur=e.target.result; if(cur){ out.push(cur.value); cur.continue(); } else resolve(out); };
    req.onerror = ()=> resolve([]);
  });
  store.canonical.costosMes = rows.map(r=>({ ...r }));
  store.vault.files = files.sort((a,b)=>(b.createdAt||"").localeCompare(a.createdAt||""));
  store.vault.loaded = true;
  updateVaultUI();
  populateFilterCatalogs();
  render();
}

async function vaultSaveRows(rows, sourceMeta){
  if(!rows.length) return { inserted:0, updated:0 };
  const db = await openDB();

  const map = new Map(store.canonical.costosMes.map(r=>[r.id, r]));
  let inserted=0, updated=0;

  await new Promise((resolve, reject)=>{
    const tr = db.transaction(STORE_ROWS, "readwrite");
    const os = tr.objectStore(STORE_ROWS);
    for(const r0 of rows){
      const r = { ...r0 };
      r.id = r.id || makeRowId(r);
      if(map.has(r.id)) updated++; else inserted++;
      map.set(r.id, r);
      os.put(r);
    }
    tr.oncomplete = resolve;
    tr.onerror = ()=> reject(tr.error);
  });

  if(sourceMeta){
    const meta = {
      id: sourceMeta.id || crypto.randomUUID(),
      filename: sourceMeta.filename || "Archivo",
      detectedType: sourceMeta.detectedType || "auto",
      createdAt: sourceMeta.createdAt || new Date().toISOString(),
      rows: rows.length,
      years: Array.from(new Set(rows.map(r=>String(r.vigencia||"")))).sort()
    };
    await new Promise((resolve, reject)=>{
      const tr = db.transaction(STORE_FILES, "readwrite");
      tr.objectStore(STORE_FILES).put(meta);
      tr.oncomplete = resolve;
      tr.onerror = ()=> reject(tr.error);
    });
  }

  store.canonical.costosMes = Array.from(map.values());
  store.vault.loaded = true;
  updateVaultUI();
  populateFilterCatalogs();
  render();
  return { inserted, updated };
}

async function vaultClear(){
  const db = await openDB();
  await new Promise((resolve, reject)=>{
    const tr = db.transaction([STORE_ROWS, STORE_FILES], "readwrite");
    tr.objectStore(STORE_ROWS).clear();
    tr.objectStore(STORE_FILES).clear();
    tr.oncomplete = resolve;
    tr.onerror = ()=> reject(tr.error);
  });
  store.canonical.costosMes = [];
  store.vault.files = [];
  store.vault.loaded = false;
  updateVaultUI();
  populateFilterCatalogs();
  render();
}

async function vaultExportJSON(){
  const payload = { exportedAt:new Date().toISOString(), version:"v3", rows:store.canonical.costosMes, sources:store.vault.files };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type:"application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "visor_costos_historico.json";
  document.body.appendChild(a); a.click(); a.remove();
}

async function vaultImportJSON(file){
  const payload = JSON.parse(await file.text());
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  const sources = Array.isArray(payload.sources) ? payload.sources : [];
  const db = await openDB();
  await new Promise((resolve, reject)=>{
    const tr = db.transaction([STORE_ROWS, STORE_FILES], "readwrite");
    const osR = tr.objectStore(STORE_ROWS);
    const osF = tr.objectStore(STORE_FILES);
    for(const r of rows){
      const rr = { ...r };
      rr.id = rr.id || makeRowId(rr);
      osR.put(rr);
    }
    for(const s of sources){
      osF.put({ ...s, id: s.id || crypto.randomUUID() });
    }
    tr.oncomplete = resolve;
    tr.onerror = ()=> reject(tr.error);
  });
  await vaultLoad();
}

/* ---------- UI helpers ---------- */
function formatCOP(value){ return Number(value||0).toLocaleString("es-CO",{style:"currency",currency:"COP",maximumFractionDigits:0}); }
function pct(value){ if(value===null||value===undefined||isNaN(Number(value))) return ""; return (Number(value)*100).toFixed(2)+"%"; }
function escapeHtml(s){ return String(s||"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;"); }

function setRoute(route){
  store.route = route;
  $$(".tab").forEach(b => b.classList.toggle("is-active", b.dataset.route === route));
  $$(".view").forEach(v => v.classList.toggle("is-active", v.dataset.view === route));
  // Mostrar el panel de histórico SOLO en el menú
  const menuOnlyPanel = document.getElementById("menuOnlyPanel");
  if (menuOnlyPanel) menuOnlyPanel.classList.toggle("is-hidden", route !== "menu");
  render();
}

function readMultiSelectValues(selectEl){ return Array.from(selectEl.selectedOptions).map(o => o.value); }
function clearSelect(selectEl){ Array.from(selectEl.options).forEach(o => o.selected = false); }

function resetFilters(){
  store.filters = { centroCostoText:"", vigencia:[], mes:[], uf:[] };
  $("#fCentroCosto").value = "";
  clearSelect($("#fVigencia")); clearSelect($("#fMes")); clearSelect($("#fUF"));
  populateFilterCatalogs();
  render();
}

function uniqueSorted(arr){
  return Array.from(new Set(arr.filter(v => v !== null && v !== undefined && v !== "")))
    .map(v => String(v))
    .sort((a,b)=> a.localeCompare(b, "es"));
}
function fillSelect(selectEl, values){
  selectEl.innerHTML = "";
  values.forEach(v => { const opt=document.createElement("option"); opt.value=v; opt.textContent=v; selectEl.appendChild(opt); });
}

function updateVaultUI(){
  $("#vaultStatus").textContent = store.vault.loaded ? "Cargado" : "No cargado";
  $("#vaultRows").textContent = String(store.canonical.costosMes.length);
  const years = uniqueSorted(store.canonical.costosMes.map(r=>r.vigencia)).sort((a,b)=>Number(a)-Number(b));
  $("#vaultYears").textContent = years.length ? years.join(", ") : "—";
  const filesDiv = $("#vaultFiles");
  filesDiv.innerHTML = store.vault.files.length
    ? store.vault.files.slice(0,30).map(f=>`• ${escapeHtml(f.filename)} (${escapeHtml(f.detectedType)}) • filas: ${f.rows} • ${escapeHtml((f.years||[]).join(","))}`).join("<br/>")
    : "—";
}

/* ---------- dataset + filtros ---------- */
function datasetActiveRows(){ return store.canonical.costosMes; }
function populateFilterCatalogs(){
  const rows = datasetActiveRows();
  fillSelect($("#fVigencia"), uniqueSorted(rows.map(r=>r.vigencia)).sort((a,b)=>Number(a)-Number(b)));
  fillSelect($("#fMes"), uniqueSorted(rows.map(r=>r.mes)));
  fillSelect($("#fUF"), uniqueSorted(rows.map(r=>r.uf||"Sin UF")));
}
function matchCentro(r, text){
  if(!text) return true;
  const t = text.toLowerCase();
  return String(r.cc||"").toLowerCase().includes(t) || String(r.centro||"").toLowerCase().includes(t);
}
function applyFilters(rows){
  const f = store.filters;
  return rows.filter(r=>{
    const okCentro = matchCentro(r, f.centroCostoText);
    const okVig = !f.vigencia.length || f.vigencia.includes(String(r.vigencia));
    const okMes = !f.mes.length || f.mes.includes(String(r.mes));
    const uf = r.uf || "Sin UF";
    const okUf  = !f.uf.length  || f.uf.includes(String(uf));
    return okCentro && okVig && okMes && okUf;
  });
}
function sum(rows, key){ return rows.reduce((acc,r)=> acc + Number(r[key]||0), 0); }

function destroyChart(id){
  if(window.__charts && window.__charts[id]){ window.__charts[id].destroy(); delete window.__charts[id]; }
}
function setChart(id, chart){ window.__charts = window.__charts || {}; window.__charts[id]=chart; }
function monthIndex(m){ return MONTHS.indexOf(String(m||"").toLowerCase()); }

/* ---------- render módulos (reutiliza lógica v2) ---------- */
function renderResultados(){
  const rows = applyFilters(datasetActiveRows());
  const fact = sum(rows,"facturado"), costo=sum(rows,"costo_total"), util=sum(rows,"utilidad");
  $("#kpiFacturacion").textContent = formatCOP(fact);
  $("#kpiCosto").textContent = formatCOP(costo);
  $("#kpiUtilidad").textContent = formatCOP(util);

  const byMes=new Map();
  for(const r of rows){
    const k=r.mes; if(!byMes.has(k)) byMes.set(k,{mes:k,fact:0,costo:0,util:0});
    const x=byMes.get(k); x.fact+=Number(r.facturado||0); x.costo+=Number(r.costo_total||0); x.util+=Number(r.utilidad||0);
  }
  const series=Array.from(byMes.values()).sort((a,b)=>monthIndex(a.mes)-monthIndex(b.mes));

  destroyChart("chartResultadoMes");
  setChart("chartResultadoMes", new Chart($("#chartResultadoMes"),{
    type:"bar",
    data:{labels:series.map(s=>s.mes),datasets:[
      {label:"Facturado",data:series.map(s=>s.fact)},
      {label:"Costo total",data:series.map(s=>s.costo)},
      {label:"Utilidad",data:series.map(s=>s.util)},
    ]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"top"}},scales:{y:{ticks:{font:{size:10},callback:(v)=>formatCOP(v)}}}}
  }));

  destroyChart("chartCostoVsUtilidad");
  setChart("chartCostoVsUtilidad", new Chart($("#chartCostoVsUtilidad"),{
    type:"doughnut",
    data:{labels:["Costo total","Utilidad"],datasets:[{data:[costo,util]}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"right"}}}
  }));

  const directos=sum(rows,"directos"), indirectos=sum(rows,"indirectos");
  destroyChart("chartDirInd");
  setChart("chartDirInd", new Chart($("#chartDirInd"),{
    type:"doughnut",
    data:{labels:["Directos","Indirectos"],datasets:[{data:[directos,indirectos]}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"right"}}}
  }));

  const gg=sum(rows,"gastos_generales"), mo=sum(rows,"mano_obra"), af=sum(rows,"activos_fijos"), disp=sum(rows,"dispensacion"), cons=sum(rows,"consumo");
  destroyChart("chartClases");
  setChart("chartClases", new Chart($("#chartClases"),{
    type:"pie",
    data:{labels:["Gastos Generales","Mano de Obra","Activos Fijos","Dispensación","Consumo"],datasets:[{data:[gg,mo,af,disp,cons]}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"right"}}}
  }));

  const byCC=new Map();
  for(const r of rows){
    const k=`${r.cc}||${r.centro}||${r.uf||"Sin UF"}`;
    if(!byCC.has(k)) byCC.set(k,{cc:r.cc,centro:r.centro,uf:r.uf||"Sin UF",fact:0,costo:0,util:0,sosVals:[]});
    const x=byCC.get(k);
    x.fact+=Number(r.facturado||0); x.costo+=Number(r.costo_total||0); x.util+=Number(r.utilidad||0);
    if(r.sos!==null&&r.sos!==undefined&&!isNaN(Number(r.sos))) x.sosVals.push(Number(r.sos));
  }
  const tbl=Array.from(byCC.values()).sort((a,b)=>b.costo-a.costo).slice(0,50);
  const tbody=$("#tblResultados tbody"); tbody.innerHTML="";
  for(const r of tbl){
    const sosAvg=r.sosVals.length?(r.sosVals.reduce((a,b)=>a+b,0)/r.sosVals.length):null;
    const tr=document.createElement("tr");
    tr.innerHTML=`<td>${r.cc??""}</td><td>${escapeHtml(r.centro??"")}</td><td>${escapeHtml(r.uf??"")}</td>
      <td>${formatCOP(r.fact)}</td><td>${formatCOP(r.costo)}</td><td>${formatCOP(r.util)}</td><td>${sosAvg===null?"":pct(sosAvg)}</td>`;
    tbody.appendChild(tr);
  }
}

function renderUF(){
  const rows=applyFilters(datasetActiveRows());
  $("#kpiUfTotal").textContent=formatCOP(sum(rows,"costo_total"));
  $("#kpiUfDirecto").textContent=formatCOP(sum(rows,"directos"));
  $("#kpiUfIndirecto").textContent=formatCOP(sum(rows,"indirectos"));

  const byUF=new Map();
  for(const r of rows){
    const k=r.uf||"Sin UF";
    if(!byUF.has(k)) byUF.set(k,{uf:k,total:0,directos:0,indirectos:0});
    const x=byUF.get(k); x.total+=Number(r.costo_total||0); x.directos+=Number(r.directos||0); x.indirectos+=Number(r.indirectos||0);
  }
  const ufList=Array.from(byUF.values()).sort((a,b)=>b.total-a.total);
  const top=ufList.slice(0,12);

  destroyChart("chartUfPart");
  setChart("chartUfPart", new Chart($("#chartUfPart"),{
    type:"bar",
    data:{labels:top.map(x=>x.uf),datasets:[{label:"Costo total",data:top.map(x=>x.total)}]},
    options:{indexAxis:"y",responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"top"}},scales:{x:{ticks:{font:{size:10},callback:(v)=>formatCOP(v)}}}}
  }));

  const byMes=new Map();
  for(const r of rows){ const k=r.mes; byMes.set(k,(byMes.get(k)||0)+Number(r.costo_total||0)); }
  const series=Array.from(byMes.entries()).map(([mes,total])=>({mes,total})).sort((a,b)=>monthIndex(a.mes)-monthIndex(b.mes));
  destroyChart("chartUfMes");
  setChart("chartUfMes", new Chart($("#chartUfMes"),{
    type:"bar",
    data:{labels:series.map(s=>s.mes),datasets:[{label:"Costo total",data:series.map(s=>s.total)}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"top"}},scales:{y:{ticks:{font:{size:10},callback:(v)=>formatCOP(v)}}}}
  }));

  const tbody=$("#tblUF tbody"); tbody.innerHTML="";
  ufList.slice(0,50).forEach(x=>{
    const tr=document.createElement("tr");
    tr.innerHTML=`<td>${escapeHtml(x.uf)}</td><td>${formatCOP(x.total)}</td><td>${formatCOP(x.directos)}</td><td>${formatCOP(x.indirectos)}</td>`;
    tbody.appendChild(tr);
  });
}

function renderComparativo(){
  const container=$("#cmpContainer"); 
  container.innerHTML="";

  const all=applyFilters(datasetActiveRows());
  const years=uniqueSorted(all.map(r=>r.vigencia)).sort((a,b)=>Number(a)-Number(b));

  if(!years.length){
    container.innerHTML=`<div class="muted" style="padding:10px 12px">No hay vigencias cargadas para comparar.</div>`;
    return;
  }

  for(const y of years){
    const rows=all.filter(r=>String(r.vigencia)===String(y));

    // KPIs
    const fact=sum(rows,"facturado");
    const costo=sum(rows,"costo_total");
    const util=sum(rows,"utilidad");

    // Datos tipo Power BI (tortas)
    const gg=sum(rows,"gastos_generales");
    const mo=sum(rows,"mano_obra");
    const af=sum(rows,"activos_fijos");
    const disp=sum(rows,"dispensacion");
    const cons=sum(rows,"consumo");

    const directos=sum(rows,"directos");
    const indirectos=sum(rows,"indirectos");

    // Donut costo vs utilidad (si utilidad negativa: se representa en 0 y se anota)
    const utilForChart = util >= 0 ? util : 0;
    const utilNegNote = util < 0 ? `<span class="cmp-note">* Utilidad negativa: ${formatCOP(util)}</span>` : ``;

    const idDonut=`cmpDonut_${y}`;
    const idClase=`cmpClase_${y}`;
    const idTipo=`cmpTipo_${y}`;

    const card=document.createElement("div"); 
    card.className="cmp-card";
    card.innerHTML=`
      <div class="cmp-card__head">
        <span>Vigencia ${y}</span>
        <span style="opacity:.9">Comparativo (tortas)</span>
      </div>

      <div class="cmp-card__body">
        <div class="cmp-kpis">
          <div class="cmp-kpi"><div class="l">Facturado</div><div class="v">${formatCOP(fact)}</div></div>
          <div class="cmp-kpi"><div class="l">Costo total</div><div class="v">${formatCOP(costo)}</div></div>
          <div class="cmp-kpi"><div class="l">Utilidad</div><div class="v">${formatCOP(util)}</div></div>
        </div>

        <div class="cmp-charts">
          <div class="cmp-chartbox">
            <div class="cmp-chartbox__title">E. R costo total vs utilidad</div>
            <div class="cmp-chartbox__canvas"><canvas id="${idDonut}"></canvas></div>
            ${utilNegNote}
          </div>

          <div class="cmp-chartbox">
            <div class="cmp-chartbox__title">E. resultado por clase de costo</div>
            <div class="cmp-chartbox__canvas"><canvas id="${idClase}"></canvas></div>
          </div>

          <div class="cmp-chartbox">
            <div class="cmp-chartbox__title">E. resultado por tipo de costo</div>
            <div class="cmp-chartbox__canvas"><canvas id="${idTipo}"></canvas></div>
          </div>
        </div>
      </div>
    `;
    container.appendChild(card);

    // Charts
    destroyChart(idDonut);
    setChart(idDonut, new Chart($("#"+idDonut),{
      type:"doughnut",
      data:{labels:["Costo total","Utilidad"],datasets:[{data:[costo, utilForChart]}]},
      options:{
        responsive:true,maintainAspectRatio:false,
        plugins:{legend:{position:"right"}}
      }
    }));

    destroyChart(idClase);
    setChart(idClase, new Chart($("#"+idClase),{
      type:"pie",
      data:{
        labels:["Gastos Generales","Mano de Obra","Activos Fijos","Dispensación","Consumo"],
        datasets:[{data:[gg,mo,af,disp,cons]}]
      },
      options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"right"}}}
    }));

    destroyChart(idTipo);
    setChart(idTipo, new Chart($("#"+idTipo),{
      type:"doughnut",
      data:{labels:["Directos","Indirectos"],datasets:[{data:[directos,indirectos]}]},
      options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"right"}}}
    }));
  }
}


function renderIndicadores(){
  const rows=applyFilters(datasetActiveRows()).filter(r=>r.sos!==null&&r.sos!==undefined&&!isNaN(Number(r.sos)));
  if(!rows.length){
    $("#kpiSosAvg").textContent="0%"; $("#kpiSosMin").textContent="0%"; $("#kpiSosMax").textContent="0%";
    destroyChart("chartSosMes"); destroyChart("chartSosDist"); $("#tblSos tbody").innerHTML=""; return;
  }
  const sosArr=rows.map(r=>Number(r.sos));
  const avg=sosArr.reduce((a,b)=>a+b,0)/sosArr.length;
  $("#kpiSosAvg").textContent=pct(avg);
  $("#kpiSosMin").textContent=pct(Math.min(...sosArr));
  $("#kpiSosMax").textContent=pct(Math.max(...sosArr));

  const byMes=new Map();
  for(const r of rows){ const k=r.mes; if(!byMes.has(k)) byMes.set(k,[]); byMes.get(k).push(Number(r.sos)); }
  const series=Array.from(byMes.entries()).map(([mes,vals])=>({mes,sos:vals.reduce((x,y)=>x+y,0)/vals.length}))
    .sort((a,b)=>monthIndex(a.mes)-monthIndex(b.mes));
  destroyChart("chartSosMes");
  setChart("chartSosMes", new Chart($("#chartSosMes"),{
    type:"line",
    data:{labels:series.map(s=>s.mes),datasets:[{label:"% Sos",data:series.map(s=>s.sos),tension:.2}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"top"}},scales:{y:{ticks:{font:{size:10},callback:(v)=>pct(v)}}}}
  }));

  const bins=[-1,-0.2,0,0.1,0.2,0.3,1], labels=["<-20%","-20% a 0%","0% a 10%","10% a 20%","20% a 30%"," >30%"];
  const counts=new Array(labels.length).fill(0);
  for(const s of sosArr){
    for(let i=0;i<bins.length-1;i++){
      if(s>=bins[i]&&s<bins[i+1]){counts[i]++;break;}
      if(i===bins.length-2&&s>=bins[i+1]) counts[counts.length-1]++;
    }
  }
  destroyChart("chartSosDist");
  setChart("chartSosDist", new Chart($("#chartSosDist"),{type:"bar",data:{labels,datasets:[{label:"Registros",data:counts}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"top"}}}}));

  const byCC=new Map();
  for(const r of rows){
    const k=`${r.cc}||${r.centro}||${r.uf||"Sin UF"}`;
    if(!byCC.has(k)) byCC.set(k,{cc:r.cc,centro:r.centro,uf:r.uf||"Sin UF",sosVals:[],fact:0,costo:0,util:0});
    const x=byCC.get(k); x.sosVals.push(Number(r.sos)); x.fact+=Number(r.facturado||0); x.costo+=Number(r.costo_total||0); x.util+=Number(r.utilidad||0);
  }
  const list=Array.from(byCC.values()).map(x=>({ ...x, sosAvg:x.sosVals.reduce((a,b)=>a+b,0)/x.sosVals.length }))
    .sort((a,b)=>a.sosAvg-b.sosAvg).slice(0,50);
  const tbody=$("#tblSos tbody"); tbody.innerHTML="";
  list.forEach(r=>{
    const tr=document.createElement("tr");
    tr.innerHTML=`<td>${r.cc??""}</td><td>${escapeHtml(r.centro??"")}</td><td>${escapeHtml(r.uf??"")}</td>
      <td>${pct(r.sosAvg)}</td><td>${formatCOP(r.fact)}</td><td>${formatCOP(r.costo)}</td><td>${formatCOP(r.util)}</td>`;
    tbody.appendChild(tr);
  });
}

function render(){
  $("#rowsSession").textContent = String(store.session.costosMes.length);
  updateVaultUI();
  if(store.route==="resultados") renderResultados();
  if(store.route==="uf") renderUF();
  if(store.route==="comparativo") renderComparativo();
  if(store.route==="indicadores") renderIndicadores();
}

/* ---------- Loader Excel: autodetección (reusa v2) ---------- */
function toNumber(v){ const n=Number(v); return isNaN(n)?0:n; }
function normalizeMonthName(m){
  const s=String(m||"").trim().toLowerCase();
  for(const name of MONTHS){
    if(s.startsWith(name.slice(0,3))) return name;
    if(s===name) return name;
  }
  return s;
}
function parseReport_rptCostListResultOperation(wb){
  const ws=wb.Sheets[wb.SheetNames[0]];
  const rows=XLSX.utils.sheet_to_json(ws,{header:1,defval:null,raw:true});
  const out=[]; let currentCentro="", currentCC="", currentYear="", col={};
  const get=(arr,idx)=>(idx===undefined||idx===null)?null:arr[idx];

  for(const r of rows){
    const c0=get(r,0);
    if(typeof c0==="string"&&normText(c0).includes("centros de produccion")){
      const centroTxt=get(r,4);
      currentCentro=centroTxt?String(centroTxt).trim():"";
      const m=currentCentro.match(/^\s*(\d+)\s*[-–]/);
      currentCC=m?m[1]:"";
      continue;
    }
    if(typeof c0==="string"&&normText(c0).startsWith("mes")){
      const m=String(c0).match(/(\d{4})/);
      currentYear=m?m[1]:"";
      col={};
      for(let j=0;j<r.length;j++){
        const v=r[j];
        if(typeof v==="string") col[v.trim().toLowerCase()]=j;
      }
      continue;
    }
    if(typeof c0==="string"){
      const name=normalizeMonthName(c0);
      if(MONTHS.includes(name)){
        const gg=toNumber(get(r,col["gastos generales"]));
        const mo=toNumber(get(r,col["mano de obra"]));
        const af=toNumber(get(r,col["activos fijos"]));
        const disp=toNumber(get(r,col["dispensacion"]));
        const cons=toNumber(get(r,col["consumo"]));
        const primaria=toNumber(get(r,col["primaria"]));
        const administrativo=toNumber(get(r,col["administrativo"]));
        const logistico=toNumber(get(r,col["logistico"]));
        const total=toNumber(get(r,col["total"]));
        const facturado=toNumber(get(r,col["facturado"]));
        const utilidad=toNumber(get(r,col["utilidad"]));
        let idx=null;
        const perc=Object.entries(col).filter(([k,_])=>k==="%").map(([_,v])=>v).sort((a,b)=>a-b);
        if(perc.length) idx=perc[perc.length-1];
        if(idx===null) idx=r.length-1;
        const v=get(r,idx); const sos=isNaN(Number(v))?null:Number(v);

        const row={vigencia:currentYear||"", mes:name, uf:"Sin UF", cc:currentCC, centro:currentCentro,
          gastos_generales:gg, mano_obra:mo, activos_fijos:af, dispensacion:disp, consumo:cons,
          directos:primaria, indirectos:administrativo+logistico, costo_total:total, facturado, utilidad, sos};
        row.id=makeRowId(row);
        out.push(row);
      }
    }
  }
  return out;
}

async function detectAndParseFile(file){
  const buf=await file.arrayBuffer();
  const wb=XLSX.read(buf,{type:"array"});
  const ws0=wb.Sheets[wb.SheetNames[0]];
  const preview=XLSX.utils.sheet_to_json(ws0,{header:1,defval:null,raw:true}).slice(0,20);
  const flat=preview.flat().filter(v=>typeof v==="string").join(" ").toLowerCase();
  const flatNorm=normText(flat);
  const isRpt=flatNorm.includes("fecha impresion")||flatNorm.includes("centros de produccion");
  if(isRpt) return { detectedType:"rptCostListResultOperation", rows:parseReport_rptCostListResultOperation(wb) };

  if(wb.SheetNames.includes("COSTOS")){
    const rows=XLSX.utils.sheet_to_json(wb.Sheets["COSTOS"],{defval:null});
    const mapped=rows.map(r=>{
      const row={vigencia:String(r["VIGENCIA"]??""), mes:normalizeMonthName(r["Mes"]??r["MES"]??""), uf:r["Unidad Funcional"]??r["UF"]??"Sin UF",
        cc:r["cc."]??r["C.C."]??"", centro:r["Centro de Costos"]??r["NOMBRE"]??"",
        gastos_generales:toNumber(r["Gastos Generales"]??r["GASTOS GENERALES"]), mano_obra:toNumber(r["Mano de Obra"]??r["MANO DE OBRA"]),
        activos_fijos:toNumber(r["Activos Fijos"]??r["ACTIVOS FIJOS"]), dispensacion:toNumber(r["Dispensación"]??r["DISPENSACIÓN"]),
        consumo:toNumber(r["Consumo"]??r["CONSUMO"]), directos:toNumber(r["Directos"]??r["COSTOS DIRECTOS"]),
        indirectos:toNumber(r["Indirectos"]??r["COSTOS INDIRECTOS"]), costo_total:toNumber(r["Costo total"]??r["COSTO TOTAL"]),
        facturado:toNumber(r["Facturado"]??r["VALOR FACTURADO"]), utilidad:toNumber(r["Utilidad"]??r["EXCEDENTE "]),
        sos:(r["% Sos"]===null||r["% Sos"]===undefined)?null:Number(r["% Sos"])};
      row.id=makeRowId(row); return row;
    }).filter(x=>x.mes);
    return { detectedType:"EstructuraAnterior:COSTOS", rows:mapped };
  }

  return { detectedType:"NoReconocido", rows:[] };
}

async function loadFilesToSession(files){
  $("#dataStatus").textContent="Cargando...";
  store.session.costosMes=[];
  try{
    for(const file of files){
      const parsed=await detectAndParseFile(file);
      if(parsed.rows && parsed.rows.length){
        parsed.rows.forEach(r=>r.__source=file.name);
        store.session.costosMes=store.session.costosMes.concat(parsed.rows);
      }
    }
  }catch(err){
    console.error(err);
    $("#dataStatus").textContent="Error al cargar archivos";
    alert("Error al cargar el archivo. Ver consola para más detalle.");
    return;
  }

  if(store.session.costosMes.length){
    $("#dataStatus").textContent=`Datos cargados en sesión: ${store.session.costosMes.length}`;
    // ✅ Activar automáticamente los datos cargados (equivalente a 'Solo sesión')
    store.canonical.costosMes = store.session.costosMes.map(r=>({ ...r, id:r.id||makeRowId(r) }));
    // No tocamos histórico/vault aquí: solo refrescamos catálogos y vista
    populateFilterCatalogs();
    render();
  }else{
    $("#dataStatus").textContent="Sin datos válidos";
    render();
  }
}

async function saveSessionToVault(){
  if(!store.session.costosMes.length){ alert("No hay datos en sesión para guardar."); return; }
  const bySource=new Map();
  for(const r of store.session.costosMes){
    const k=r.__source||"Archivo";
    if(!bySource.has(k)) bySource.set(k,[]);
    bySource.get(k).push(r);
  }
  let totalInserted=0,totalUpdated=0;
  for(const [filename, rows] of bySource.entries()){
    const meta={ filename, detectedType:"auto", createdAt:new Date().toISOString(), id:crypto.randomUUID() };
    const res=await vaultSaveRows(rows, meta);
    totalInserted+=res.inserted; totalUpdated+=res.updated;
  }
  alert(`Guardado en histórico ✅\nInsertados: ${totalInserted}\nActualizados: ${totalUpdated}`);
  store.session.costosMes=[];
  $("#dataStatus").textContent="Sesión guardada y limpiada";
  render();
}

function useSessionOnly(){
  store.canonical.costosMes = store.session.costosMes.map(r=>({ ...r, id:r.id||makeRowId(r) }));
  store.vault.loaded=false; store.vault.files=[];
  updateVaultUI(); populateFilterCatalogs(); render();
  alert("Modo SOLO SESIÓN activado (no guardado).");
}

/* ---------- Export ---------- */
function currentFiltersText(){
  const f=store.filters; const parts=[];
  if(f.centroCostoText) parts.push(`CC: ${f.centroCostoText}`);
  if(f.vigencia.length) parts.push(`Vigencia: ${f.vigencia.join(", ")}`);
  if(f.mes.length) parts.push(`Mes: ${f.mes.join(", ")}`);
  if(f.uf.length) parts.push(`UF: ${f.uf.join(", ")}`);
  return parts.length?parts.join(" | "):"Sin filtros";
}
function exportCurrentViewToPDF(){
  const { jsPDF } = window.jspdf;
  const doc = new jsPDF({ orientation:"landscape", unit:"mm", format:"a4" });

  // ---- Helpers (fecha/hora + títulos) ----
  const pad2 = (n)=> String(n).padStart(2,"0");
  const fmtGenerated = ()=>{
    const d = new Date();
    // Formato: dd/mm/yyyy, h:mm:ss a. m./p. m.
    let hh = d.getHours();
    const ampm = hh >= 12 ? "p. m." : "a. m.";
    hh = hh % 12; if (hh === 0) hh = 12;
    return `${pad2(d.getDate())}/${pad2(d.getMonth()+1)}/${d.getFullYear()}, ${hh}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())} ${ampm}`;
  };

  const viewTitles = {
    "menu": "REPORTE VISOR DE COSTOS",
    "resultados": "ESTADO DE RESULTADOS DE COSTOS",
    "uf": "COSTOS POR UNIDAD FUNCIONAL",
    "comparativo": "COMPARATIVO POR VIGENCIAS",
    "indicadores": "INDICADORES DE SOSTENIBILIDAD"
  };
  const titleText = viewTitles[store.route] || "REPORTE VISOR DE COSTOS";
  const subtitleText = "ESE Hospital Universitario Hernando Moncaleano Perdomo";
  const codeText = "GD-SGI-M-005";
  const vigText = (store.filters.vigencia && store.filters.vigencia.length) ? store.filters.vigencia.join(", ") : "—";
  const generatedAt = fmtGenerated();

  // ---- Header/Footer estilo institucional (guía visual) ----
  const drawHeader = ()=>{
    const w = doc.internal.pageSize.getWidth();
    const bannerX = 10, bannerY = 8, bannerW = w - 20, bannerH = 26;

    // Fondo banner (azul institucional)
    doc.setFillColor(42,42,116);
    // Rectángulo con esquinas redondeadas leves (estilo del ejemplo)
    doc.roundedRect(bannerX, bannerY, bannerW, bannerH, 6, 6, "F");

    // Textura suave (diagonales sutiles) - MUY tenue
    doc.setDrawColor(60, 60, 150);
    doc.setLineWidth(0.2);
    for (let x = bannerX-40; x < bannerX + bannerW + 40; x += 10){
      doc.line(x, bannerY + bannerH, x + 22, bannerY); // diagonal
    }

    // Logo (si existe PDF_LOGO_BASE64)
    try{
      if (window.PDF_LOGO_BASE64){
        // x,y,w,h
        doc.addImage(window.PDF_LOGO_BASE64, "PNG", bannerX+6, bannerY+5, 26, 16);
      }
    }catch(e){ /* ignore */ }

    // Títulos
    doc.setTextColor(255,255,255);
    doc.setFont("helvetica","bold");
    doc.setFontSize(18);
    doc.text(titleText, bannerX + 38, bannerY + 12);

    doc.setFont("helvetica","normal");
    doc.setFontSize(11);
    doc.text(subtitleText, bannerX + 38, bannerY + 19);

    // "Pill" código/vigencia
    const pillText = `${codeText}  |  Vigencia ${vigText}`;
    doc.setFillColor(30,30,88);
    doc.roundedRect(bannerX + 38, bannerY + 20.2, 72, 5.6, 3, 3, "F");
    doc.setFont("helvetica","bold");
    doc.setFontSize(9.5);
    doc.text(pillText, bannerX + 41, bannerY + 24.2);

    // Badge "Modo Offline"
    doc.setFillColor(24,24,68);
    const badgeW = 34;
    doc.roundedRect(bannerX + bannerW - badgeW - 8, bannerY + 18.8, badgeW, 6.2, 3, 3, "F");
    doc.setFont("helvetica","bold");
    doc.setFontSize(9.5);
    doc.text("Modo Offline", bannerX + bannerW - badgeW - 8 + badgeW/2, bannerY + 23.2, {align:"center"});
  };

  const drawFooter = (pageNo, totalExp)=>{
    const w = doc.internal.pageSize.getWidth();
    const h = doc.internal.pageSize.getHeight();
    doc.setDrawColor(180);
    doc.setLineWidth(0.3);
    doc.line(10, h-14, w-10, h-14);

    doc.setFont("helvetica","normal");
    doc.setFontSize(9.5);
    doc.setTextColor(90);
    doc.text(`Generado: ${generatedAt}`, 10, h-8);
    doc.text(`Página ${pageNo} de ${totalExp}`, w-10, h-8, {align:"right"});
  };

  // ---- Contenido (tabla) ----
  // Margen superior después del banner
  const startY = 38;
  const head = [[ "Vigencia","Mes","UF","C.C.","Centro","Facturado","Costo","Utilidad","% Sos" ]];
  const rows = applyFilters(datasetActiveRows());

  const body = rows.slice(0, 1200).map(r=>[
    r.vigencia ?? "",
    r.mes ?? "",
    r.uf ?? "",
    r.cc ?? "",
    r.centro ?? "",
    Number(r.facturado||0),
    Number(r.costo||0),
    Number(r.utilidad||0),
    (r.sos===null || r.sos===undefined || r.sos==="") ? "" : Number(r.sos)
  ]);

  // Dibuja tabla con autotable
  doc.autoTable({
    head,
    body,
    startY,
    styles:{ fontSize:8, cellPadding:2 },
    headStyles:{ fillColor:[42,42,116], textColor:255, fontStyle:"bold" },
    alternateRowStyles:{ fillColor:[245,246,252] },
    columnStyles:{
      5:{ halign:"right" }, 6:{ halign:"right" }, 7:{ halign:"right" }, 8:{ halign:"right" }
    },
    margin:{ left:10, right:10, top:startY, bottom:16 }
  });

  // ---- Paginación con total real ----
  const totalPagesExp = "{total_pages_count_string}";
  const pageCount = doc.getNumberOfPages();

  for (let i=1; i<=pageCount; i++){
    doc.setPage(i);
    drawHeader();
    drawFooter(i, totalPagesExp);
  }
  if (typeof doc.putTotalPages === "function"){
    doc.putTotalPages(totalPagesExp);
  }

  const safeRoute = (store.route||"visor").replace(/[^a-z0-9_-]/gi,"_");
  doc.save(`visor_costos_${safeRoute}.pdf`);
}
function exportCurrentViewToXLSX(){
  const rows=applyFilters(datasetActiveRows());
  const ws=XLSX.utils.json_to_sheet(rows);
  const wb=XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Export");
  XLSX.writeFile(wb, `visor_costos_${store.route}.xlsx`);
}

/* ---------- Events ---------- */
function wireEvents(){
  $$(".tab").forEach(btn => btn.addEventListener("click", () => setRoute(btn.dataset.route)));
  $$(".menu-tile").forEach(btn => btn.addEventListener("click", () => setRoute(btn.dataset.route)));

  $("#fCentroCosto").addEventListener("input",(e)=>{ store.filters.centroCostoText=e.target.value||""; render(); });
  $("#fVigencia").addEventListener("change",(e)=>{ store.filters.vigencia=readMultiSelectValues(e.target); render(); });
  $("#fMes").addEventListener("change",(e)=>{ store.filters.mes=readMultiSelectValues(e.target); render(); });
  $("#fUF").addEventListener("change",(e)=>{ store.filters.uf=readMultiSelectValues(e.target); render(); });

  $("#btnReset").addEventListener("click", resetFilters);

  $("#fileInput").addEventListener("change", async (e)=>{
    const files = Array.from(e.target.files || []);
    if(!files.length) return;
    await loadFilesToSession(files);
  });

  $("#btnSaveToVault").addEventListener("click", saveSessionToVault);
  $("#btnUseSessionOnly").addEventListener("click", useSessionOnly);

  $("#btnVaultLoad").addEventListener("click", vaultLoad);
  $("#btnVaultExport").addEventListener("click", vaultExportJSON);
  $("#vaultImportInput").addEventListener("change", async (e)=>{
    const f=e.target.files?.[0]; if(!f) return;
    await vaultImportJSON(f); e.target.value="";
  });
  $("#btnVaultClear").addEventListener("click", async ()=>{
    const ok=confirm("¿Seguro que deseas borrar TODO el histórico guardado en este navegador?");
    if(ok) await vaultClear();
  });

  $("#btnExportPDF").addEventListener("click", exportCurrentViewToPDF);
  $("#btnExportXLSX").addEventListener("click", exportCurrentViewToXLSX);
}

wireEvents();
setRoute("menu");
vaultLoad().catch(()=>{});
