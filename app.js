
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


// --- Catálogo CC → UF / Centro (merge institucional) ---
const CC_CATALOG = {
  "133": { uf: "300 - SERVICIOS AMBULATORIOS", centro: "133 - PROGRAMA MADRE CANGURO" },
  "201": { uf: "200 - URGENCIAS", centro: "201 - URGENCIAS, CONSULTAS Y PROCEDIMIENTOS" },
  "202": { uf: "200 - URGENCIAS", centro: "202 - URGENCIAS OBSERVACION ADULTOS" },
  "203": { uf: "200 - URGENCIAS", centro: "203 - URGENCIAS OBSERVACION PEDIATRIA" },
  "301": { uf: "300 - SERVICIOS AMBULATORIOS", centro: "301 - CONSULTA EXTERNA" },
  "401": { uf: "400 - HOSPITALIZACIÓN", centro: "401 - HOSPITALIZACION CIRUGIA" },
  "402": { uf: "400 - HOSPITALIZACIÓN", centro: "402 - HOSPITALIZACION GINECOBSTETRICIA" },
  "403": { uf: "400 - HOSPITALIZACIÓN", centro: "403 - HOSPITALIZACION MEDICINA INTERNA" },
  "404": { uf: "400 - HOSPITALIZACIÓN", centro: "404 - HOSPITALIZACION NEUROCIRUGIA" },
  "406": { uf: "400 -1 - UCI", centro: "406 - CUIDADO INTENSIVO NEONATAL" },
  "407": { uf: "400 - HOSPITALIZACIÓN", centro: "407 - HOSPITALIZACION PEDIATRIA" },
  "408": { uf: "400 - HOSPITALIZACIÓN", centro: "408 - HOSPITALIZACION SALUD MENTAL" },
  "409": { uf: "400 - HOSPITALIZACIÓN", centro: "409 - HOSPITALIZACION SEPTIMO PISO - VIP" },
  "410": { uf: "400 -1 - UCI", centro: "410 - CUIDADO INTENSIVO ADULTO" },
  "411": { uf: "400 -1 - UCI", centro: "411 - CUIDADO INTENSIVO PEDIATRICO" },
  "415": { uf: "400 -1 - UCI", centro: "415 - CUIDADO INTENSIVO OBSTETRICO" },
  "416": { uf: "400 - HOSPITALIZACIÓN", centro: "416 - HOSPITALIZACION ONCOLOGIA" },
  "501": { uf: "500 - QUIRÓFANOS", centro: "501 - QUIROFANOS" },
  "502": { uf: "500 - QUIRÓFANOS", centro: "502 - SALA DE PARTOS" },
  "503": { uf: "500 - QUIRÓFANOS", centro: "503 - UNIDAD DE TRASPLANTES" },
  "601": { uf: "600 - APOYO DIAGNOSTICO", centro: "601 - CARDIOLOGIA" },
  "602": { uf: "600 - APOYO DIAGNOSTICO", centro: "602 - IMAGENOLOGIA" },
  "603": { uf: "600 - APOYO DIAGNOSTICO", centro: "603 - LABORATORIO CLINICO" },
  "604": { uf: "600 - APOYO DIAGNOSTICO", centro: "604 - NEUMOLOGIA" },
  "605": { uf: "600 - APOYO DIAGNOSTICO", centro: "605 - ANATOMIA PATOLOGICA" },
  "606": { uf: "600 - APOYO DIAGNOSTICO", centro: "606 - ENDOSCOPIAS" },
  "607": { uf: "700 - APOYO TERAPÉUTICO", centro: "607 - CARDIOVASCULAR" },
  "608": { uf: "600 - APOYO DIAGNOSTICO", centro: "608 - RESONANCIA MAGNETICA NUCLEAR (RMN)" },
  "701": { uf: "700 - APOYO TERAPÉUTICO", centro: "701 - BANCO DE SANGRE" },
  "703": { uf: "600 - APOYO DIAGNOSTICO", centro: "703 - NEUROFISIOLOGIA" },
  "902": { uf: "700 - APOYO TERAPÉUTICO", centro: "902 - UNIDAD RENAL" },
  "903": { uf: "700 - APOYO TERAPÉUTICO", centro: "903 - UNIDAD CANCEROLOGIA" },
  "904": { uf: "SERVICIOS CONEXOS", centro: "904 - TRANSPORTE AMBULANCIA" },
  "913": { uf: "SERVICIOS CONEXOS", centro: "913 - INGRESOS NO OPERATIVOS" },
};

function ccKeyFromAny(v){
  if(v===null || v===undefined) return "";
  const s = String(v).trim();
  if(!s) return "";
  const m = s.match(/\d{3,4}/);
  return m ? m[0] : "";
}
function enrichRowWithCatalog(row){
  try{
    const k = ccKeyFromAny(row.cc) || ccKeyFromAny(row.centro);
    if(!k) return row;
    const ref = CC_CATALOG[k];
    if(!ref) return row;
    if(!row.uf || String(row.uf).trim()==="" || String(row.uf).trim().toLowerCase()==="sin uf"){
      row.uf = ref.uf;
    }
    if(!row.centro || String(row.centro).trim()===""){
      row.centro = ref.centro;
    }
    // normaliza CC como número string
    if(!row.cc || String(row.cc).trim()===""){
      row.cc = k;
    }
    return row;
  }catch(_){ return row; }
}
function ufFromCC(cc, fallback="Sin UF"){
  const k = ccKeyFromAny(cc);
  const ref = k ? CC_CATALOG[k] : null;
  return ref ? ref.uf : fallback;
}
function centroFromCC(cc, fallback=""){
  const k = ccKeyFromAny(cc);
  const ref = k ? CC_CATALOG[k] : null;
  return ref ? ref.centro : fallback;
}


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


// ---------- Respaldo en localStorage (fallback persistente) ----------
// Útil cuando IndexedDB está restringido (políticas corporativas / perfiles bloqueados / orígenes especiales)
const LS_VAULT_BACKUP_KEY = "HUHMP_COSTOS_VAULT_BACKUP_V1";

function vaultBackupWrite(){
  try{
    const payload = {
      savedAt: new Date().toISOString(),
      version: "v1",
      rows: (store && store.canonical && store.canonical.costosMes) ? store.canonical.costosMes : [],
      sources: (store && store.vault && store.vault.files) ? store.vault.files : []
    };
    localStorage.setItem(LS_VAULT_BACKUP_KEY, JSON.stringify(payload));
    return true;
  }catch(e){
    console.warn("[VaultBackup] No se pudo escribir en localStorage", e);
    return false;
  }
}
function vaultBackupRead(){
  try{
    const raw = localStorage.getItem(LS_VAULT_BACKUP_KEY);
    if(!raw) return null;
    const parsed = JSON.parse(raw);
    if(parsed && Array.isArray(parsed.rows)) return parsed;
    return null;
  }catch(e){
    console.warn("[VaultBackup] No se pudo leer localStorage", e);
    return null;
  }
}
function vaultBackupClear(){
  try{ localStorage.removeItem(LS_VAULT_BACKUP_KEY); }catch(_){}
}
// ---------- /Respaldo en localStorage ----------


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
  let db = null;
  try{
    db = await openDB();
  }catch(err){
    console.warn('[Vault] IndexedDB no disponible, usando respaldo localStorage', err);
    const backup = vaultBackupRead();
    const rows = (backup && Array.isArray(backup.rows)) ? backup.rows.map(r=>enrichRowWithCatalog({ ...r })) : [];
    const files = (backup && Array.isArray(backup.sources)) ? backup.sources.slice().sort((a,b)=>(b.createdAt||'').localeCompare(a.createdAt||'')) : [];
    store.canonical.costosMes = rows;
    store.vault.files = files;
    store.vault.loaded = rows.length>0;
    updateVaultUI();
    populateFilterCatalogs();
    render();
    return;
  }
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
// Si el histórico quedó vacío pero existe respaldo localStorage, úsalo como fuente.
const backup = vaultBackupRead();
if((!rows || !rows.length) && backup && Array.isArray(backup.rows) && backup.rows.length){
  rows.splice(0, rows.length, ...backup.rows);
}
if((!files || !files.length) && backup && Array.isArray(backup.sources) && backup.sources.length){
  files.splice(0, files.length, ...backup.sources);
}
store.canonical.costosMes = rows.map(r=>enrichRowWithCatalog({ ...r }));

  store.vault.files = files.sort((a,b)=>(b.createdAt||"").localeCompare(a.createdAt||""));
  store.vault.loaded = true;
  vaultBackupWrite();
  updateVaultUI();
  populateFilterCatalogs();
  render();
}

async function vaultSaveRows(rows, sourceMeta){
  // Guarda en IndexedDB cuando esté disponible.
  // Si IndexedDB está bloqueado (muy común en ejecuciones file:// o políticas corporativas),
  // hacemos persistencia completa usando localStorage (vaultBackup*) como fuente de verdad.
  if(!rows.length) return { inserted:0, updated:0, mode:"noop" };

  const map = new Map((store.canonical.costosMes||[]).map(r=>[r.id, r]));
  let inserted=0, updated=0;

  // Pre-merge en memoria (sirve tanto para IDB como para fallback)
  const merged = rows.map(r0=>{
    const r = enrichRowWithCatalog({ ...r0 });
    r.id = r.id || makeRowId(r);
    if(map.has(r.id)) updated++; else inserted++;
    map.set(r.id, r);
    return r;
  });

  // Intento IndexedDB
  let db = null;
  try{ db = await openDB(); }catch(err){
    console.warn('[Vault] No se pudo abrir IndexedDB. Persistiendo en localStorage.', err);
  }

  if(db){
    await new Promise((resolve, reject)=>{
      const tr = db.transaction(STORE_ROWS, "readwrite");
      const os = tr.objectStore(STORE_ROWS);
      for(const r of merged){
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
  }else{
    // Fallback: mantenemos un catálogo de "sources" simple
    if(sourceMeta){
      const meta = {
        id: sourceMeta.id || (crypto?.randomUUID ? crypto.randomUUID() : String(Date.now())),
        filename: sourceMeta.filename || "Archivo",
        detectedType: sourceMeta.detectedType || "auto",
        createdAt: sourceMeta.createdAt || new Date().toISOString(),
        rows: rows.length,
        years: Array.from(new Set(rows.map(r=>String(r.vigencia||"")))).sort()
      };
      store.vault.files = [meta, ...(store.vault.files||[])].slice(0,200);
    }
  }

  // Actualiza estado in-memory y persiste backup SIEMPRE
  store.canonical.costosMes = Array.from(map.values());
  store.vault.loaded = true;
  vaultBackupWrite();
  updateVaultUI();
  populateFilterCatalogs();
  render();
  return { inserted, updated, mode: db ? "indexeddb" : "localstorage" };
}

async function vaultClear(){
  // Intenta borrar IndexedDB; si falla, igual limpia el respaldo localStorage
  let db = null;
  try{ db = await openDB(); }catch(err){
    console.warn('[Vault] No se pudo abrir IndexedDB para borrar. Limpiando respaldo localStorage.', err);
  }
  if(db){
    await new Promise((resolve, reject)=>{
      const tr = db.transaction([STORE_ROWS, STORE_FILES], "readwrite");
      tr.objectStore(STORE_ROWS).clear();
      tr.objectStore(STORE_FILES).clear();
      tr.oncomplete = resolve;
      tr.onerror = ()=> reject(tr.error);
    });
  }
  store.canonical.costosMes = [];
  store.vault.files = [];
  store.vault.loaded = false;
  vaultBackupClear();
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
    x.mo+=Number(r.mano_obra||0);
    x.gg+=Number(r.gastos_generales||0);
    x.disp+=Number(r.dispensacion||0);
    x.cons+=Number(r.consumo||0);
    x.af+=Number(r.activos_fijos||0);
    x.adm+=Number(r.administrativo||0);
    x.log+=Number(r.logistico||0);
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
    if(!byCC.has(k)) byCC.set(k,{cc:r.cc,centro:r.centro,uf:r.uf||"Sin UF",fact:0,costo:0,util:0,sosVals:[],mo:0,gg:0,disp:0,cons:0,af:0,adm:0,log:0});
    const x=byCC.get(k);
    x.fact+=Number(r.facturado||0); x.costo+=Number(r.costo_total||0); x.util+=Number(r.utilidad||0);
    x.mo+=Number(r.mano_obra||0);
    x.gg+=Number(r.gastos_generales||0);
    x.disp+=Number(r.dispensacion||0);
    x.cons+=Number(r.consumo||0);
    x.af+=Number(r.activos_fijos||0);
    x.adm+=Number(r.administrativo||0);
    x.log+=Number(r.logistico||0);
    if(r.sos!==null&&r.sos!==undefined&&!isNaN(Number(r.sos))) x.sosVals.push(Number(r.sos));
  }
  const tbl=Array.from(byCC.values()).sort((a,b)=>b.costo-a.costo).slice(0,50);

// ---- Tabla E.RESULTADOS: estructura por clases + % Sostenibilidad + % Margen ----
const table=$("#tblResultados");
if(table){
  // Encabezado: exactamente como estructura solicitada
  const thead=table.querySelector("thead") || table.createTHead();
  thead.innerHTML="";
  const trh=document.createElement("tr");
  const headers=[
    "Centro de Costos",
    "Gastos Generales",
    "Consumo",
    "Activos Fijos",
    "Dispensación",
    "Administrativo",
    "Logístico",
    "Costo Total",
    "Facturación",
    "Utilidad",
    "% Sostenibilidad",
    "% Margen"
  ];
  for(const h of headers){
    const th=document.createElement("th");
    th.textContent=h;
    th.style.textAlign = (h==="Centro de Costos" ? "left" : "right");
    trh.appendChild(th);
  }
  thead.appendChild(trh);

  const tbody=table.querySelector("tbody") || table.createTBody();
  tbody.innerHTML="";

  for(const r of tbl){
    // Recalcular con base en la estructura financiera solicitada (evita columnas en blanco / inconsistencias)
    const gg  = Number(r.gg||0);
    const cons= Number(r.cons||0);
    const af  = Number(r.af||0);
    const disp= Number(r.disp||0);
    const adm = Number(r.adm||0);
    const log = Number(r.log||0);

    const costoTotal = gg + cons + af + disp + adm + log;

    const facturacion = Number(r.fact||0);
    const utilidad = facturacion - costoTotal;

    // Formulas solicitadas:
    const sostenibilidad = facturacion ? (utilidad / facturacion) : 0; // utilidad / facturación
    const margen = costoTotal ? (utilidad / costoTotal) : 0;          // utilidad / costo total

    const tr=document.createElement("tr");
    const ccLabel = (r.cc??"") ? `${r.cc} - ${r.centro||""}` : (r.centro||"");

    const cells=[
      {v: escapeHtml(ccLabel), align:"left", bold:true},
      {v: formatCOP(gg), align:"right"},
      {v: formatCOP(cons), align:"right"},
      {v: formatCOP(af), align:"right"},
      {v: formatCOP(disp), align:"right"},
      {v: formatCOP(adm), align:"right"},
      {v: formatCOP(log), align:"right"},
      {v: formatCOP(costoTotal), align:"right", bold:true},
      {v: formatCOP(facturacion), align:"right"},
      {v: formatCOP(utilidad), align:"right", util:true},
      {v: pct(sostenibilidad), align:"right", pct:true, val:sostenibilidad},
      {v: pct(margen), align:"right", pct:true, val:margen},
    ];

    tr.innerHTML = cells.map(c=>{
      const style=[];
      style.push(`text-align:${c.align}`);
      if(c.bold) style.push("font-weight:800");
      let extra="";
      if(c.util){
        const color = utilidad<0 ? "#EC268F" : (utilidad>0 ? "#008041" : "#0f172a");
        extra += `color:${color};font-weight:900;`;
      }
      if(c.pct){
        const v=Number(c.val||0);
        const color = v<0 ? "#EC268F" : (v>0 ? "#008041" : "#0f172a");
        extra += `color:${color};font-weight:800;`;
      }
      style.push(extra);
      return `<td style="${"".concat(...style)}">${c.v}</td>`;
    }).join("");

    tbody.appendChild(tr);
  }
}
// ---- /Tabla E.RESULTADOS ----
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
    x.mo+=Number(r.mano_obra||0);
    x.gg+=Number(r.gastos_generales||0);
    x.disp+=Number(r.dispensacion||0);
    x.cons+=Number(r.consumo||0);
    x.af+=Number(r.activos_fijos||0);
    x.adm+=Number(r.administrativo||0);
    x.log+=Number(r.logistico||0);
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

function normKeyHU(s){
  const str = String(s||"").trim().toLowerCase();
  // remove accents
  const noAcc = str.normalize("NFD").replace(/[\u0300-\u036f]/g,"");
  return noAcc.replace(/[^a-z0-9]+/g," ").trim().replace(/\s+/g," ");
}

function toNumber(v){
  if(v===null||v===undefined) return 0;
  if(typeof v==="number"){
    return isFinite(v)?v:0;
  }
  // Handle strings like "$ 32.873.651,12" or "32.873.651,12" or "(1.234,56)"
  let s=String(v).trim();
  if(!s) return 0;
  // negative parentheses
  let neg=false;
  if(s.startsWith("(") && s.endsWith(")")){ neg=true; s=s.slice(1,-1); }
  // remove currency symbols and spaces
  s=s.replace(/[^0-9,.-]/g,"");
  // If it looks like Colombian format (thousands '.' and decimal ',')
  // remove thousands separators and convert decimal comma to dot
  const hasComma=s.includes(","); 
  const hasDot=s.includes(".");
  if(hasComma){
    // remove all dots as thousand separators
    s=s.replace(/\./g,"");
    // replace last comma with dot (decimal)
    const lastComma=s.lastIndexOf(",");
    s=s.slice(0,lastComma).replace(/,/g,"") + "." + s.slice(lastComma+1);
  }else{
    // no comma: if multiple dots, keep last as decimal, remove others
    const parts=s.split(".");
    if(parts.length>2){
      const dec=parts.pop();
      s=parts.join("") + "." + dec;
    }
  }
  let n=parseFloat(s);
  if(isNaN(n)) n=0;
  if(neg) n=-n;
  return n;
}
function normalizeMonthName(m){
  const s=String(m||"").trim().toLowerCase();
  for(const name of MONTHS){
    if(s.startsWith(name.slice(0,3))) return name;
    if(s===name) return name;
  }
  return s;
}

function pickColKey(col, keys){
  for (const k of keys){
    const kk = normKeyHU(k);
    if (col[kk] !== undefined) return kk;
  }
  // fallback: try raw keys as-is after normalization to handle edge cases
  return normKeyHU(keys[0]);
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
        if(typeof v==="string") col[normKeyHU(v)]=j;
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
        const administrativo=toNumber(get(r, col[pickColKey(col, ["administrativo","administracion","adm"])]));
        const logistico=toNumber(get(r, col[pickColKey(col, ["logistico","logistica"])]));
        const total=toNumber(get(r,col["total"]));
        const facturado=toNumber(get(r,col["facturado"]));
        const utilidad=toNumber(get(r,col["utilidad"]));
        let idx=null;
        const perc=Object.entries(col).filter(([k,_])=>k==="%").map(([_,v])=>v).sort((a,b)=>a-b);
        if(perc.length) idx=perc[perc.length-1];
        if(idx===null) idx=r.length-1;
        const v=get(r,idx); const sos=isNaN(Number(v))?null:Number(v);

        // Incluimos explícitamente Administrativo y Logístico para que el layout E (Resultados)
        // pueda mostrarlos por columna (antes se estaban perdiendo y quedaban en $0).
        const row={vigencia:currentYear||"", mes:name, uf: ufFromCC(currentCC, "Sin UF"), cc:currentCC, centro: (currentCentro||centroFromCC(currentCC,"")),
          gastos_generales:gg, mano_obra:mo, activos_fijos:af, dispensacion:disp, consumo:cons,
          administrativo, logistico,
          directos:primaria, indirectos:administrativo+logistico, costo_total:total, facturado, utilidad, sos};
        row.id=makeRowId(row);
        out.push(enrichRowWithCatalog(row));
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



// ===== FAB AYUDA (Manual + Paso a paso) =====
(function(){
  function initHelpFab(){
    const fab = document.getElementById("fabHelp");
    const modal = document.getElementById("helpModal");
    if(!fab || !modal) return false;

    const open = () => { modal.classList.remove("hidden"); document.body.style.overflow = "hidden"; };
    const close = () => { modal.classList.add("hidden"); document.body.style.overflow = ""; };

    // Evitar doble binding
    if(fab.dataset.bound === "1") return true;
    fab.dataset.bound = "1";

    fab.addEventListener("click", open);
    modal.querySelectorAll("[data-help-close]").forEach(el => el.addEventListener("click", close));
    document.addEventListener("keydown", (e)=>{ if(e.key === "Escape" && !modal.classList.contains("hidden")) close(); });

    // Tabs
    const tabs = Array.from(modal.querySelectorAll("[data-help-tab]"));
    const panes = Array.from(modal.querySelectorAll("[data-help-pane]"));

    function setTab(name){
      tabs.forEach(t=>{
        const active = t.getAttribute("data-help-tab") === name;
        t.classList.toggle("is-active", active);
        t.setAttribute("aria-selected", active ? "true" : "false");
      });
      panes.forEach(p=>{
        const show = p.getAttribute("data-help-pane") === name;
        p.classList.toggle("hidden", !show);
      });
    }
    tabs.forEach(t => t.addEventListener("click", ()=> setTab(t.getAttribute("data-help-tab"))));
    setTab("manual");
    return true;
  }

  function boot(){
    if(initHelpFab()) return;
    // Reintento por si el HTML se inyecta después
    let tries = 0;
    const t = setInterval(()=>{
      tries++;
      if(initHelpFab() || tries >= 20) clearInterval(t);
    }, 150);
  }

  if(document.readyState === "loading"){
    document.addEventListener("DOMContentLoaded", boot);
  }else{
    boot();
  }
})();
// ===== /FAB AYUDA =====



// =================== HISTÓRICO PERSISTENTE (HUHMP) ===================
const HIST_KEY__HUHMP_COSTOS = "HUHMP_COSTOS_HISTORICO_V2";

/** Intenta obtener la data canónica actual (la que alimenta gráficos/tablas). */
function getCanonicalForSave(){
  // 1) variables comunes
  const candidates = [
    window.__canonicalData,
    window.canonicalData,
    window.canonData,
    window.dataCanonica,
    window.canonica,
    window.DATA_CANONICA,
  ];
  for(const c of candidates){
    if(Array.isArray(c) && c.length) return c;
  }
  // 2) state objects (si existen)
  try{
    if(window.state){
      if(Array.isArray(window.state.canonica) && window.state.canonica.length) return window.state.canonica;
      if(Array.isArray(window.state.canonical) && window.state.canonical.length) return window.state.canonical;
      if(Array.isArray(window.state.rowsCanonicas) && window.state.rowsCanonicas.length) return window.state.rowsCanonicas;
      if(Array.isArray(window.state.sessionCanon) && window.state.sessionCanon.length) return window.state.sessionCanon;
    }
  }catch(_){}
  // 3) fallback: data en sesión (si manejas un contenedor)
  try{
    if(window.__session && Array.isArray(window.__session.rows) && window.__session.rows.length) return window.__session.rows;
  }catch(_){}
  return null;
}

function historicoSave(rows){
  if(!Array.isArray(rows) || !rows.length) return false;
  try{
    const payload = { savedAt: new Date().toISOString(), rows };
    localStorage.setItem(HIST_KEY__HUHMP_COSTOS, JSON.stringify(payload));
    return true;
  }catch(e){
    console.error("[Historico] No se pudo guardar en localStorage", e);
    return false;
  }
}

function historicoLoad(){
  try{
    const raw = localStorage.getItem(HIST_KEY__HUHMP_COSTOS);
    if(!raw) return null;
    const parsed = JSON.parse(raw);
    if(parsed && Array.isArray(parsed.rows) && parsed.rows.length) return parsed;
    return null;
  }catch(e){
    console.error("[Historico] No se pudo leer histórico", e);
    return null;
  }
}

function historicoClear(){
  try{ localStorage.removeItem(HIST_KEY__HUHMP_COSTOS); }catch(_){}
}

function updateHistoricoUI(meta){
  // actualiza textos si existen
  const elEstado = document.getElementById("histEstado") || document.getElementById("estadoHistorico");
  const elRegs  = document.getElementById("histRegistros") || document.getElementById("registrosHistoricos");
  const elVigs  = document.getElementById("histVigencias") || document.getElementById("vigenciasHistorico");
  if(elEstado) elEstado.textContent = meta?.loaded ? "Cargado" : "No cargado";
  if(elRegs) elRegs.textContent = String(meta?.count ?? 0);

  // extrae vigencias si están en los datos
  if(elVigs && meta?.rows){
    const set = new Set();
    for(const r of meta.rows){
      const v = r.vigencia ?? r.Vigencia ?? r.Año ?? r.anio ?? r.year;
      if(v!==undefined && v!==null && String(v).trim()!=="") set.add(String(v));
    }
    elVigs.textContent = set.size ? Array.from(set).sort().join(", ") : "—";
  }
}

function applyLoadedHistoricoRows(rows){
  // conecta con el motor del visor sin romper la arquitectura
  window.__canonicalData = rows; // variable "neutral"
  if(window.state){
    if(Array.isArray(window.state.canonica)) window.state.canonica = rows;
    if(Array.isArray(window.state.canonical)) window.state.canonical = rows;
  }
  // dispara render si existe
  if(typeof applyFiltersAndRender === "function") { applyFiltersAndRender(); return; }
  if(typeof renderAll === "function") { renderAll(); return; }
  if(typeof render === "function") { render(); return; }
}

function bindHistoricoButtons(){
  const btnGuardar = document.getElementById("btnGuardarHistorico") 
                  || document.getElementById("btnSaveHistorico")
                  || document.querySelector("[data-action='guardar-historico']");
  const btnCargar  = document.getElementById("btnCargarHistorico")
                  || document.getElementById("btnLoadHistorico")
                  || document.querySelector("[data-action='cargar-historico']");
  const btnBorrar  = document.getElementById("btnBorrarHistorico")
                  || document.getElementById("btnClearHistorico")
                  || document.querySelector("[data-action='borrar-historico']");

  if(btnGuardar){
    btnGuardar.addEventListener("click", ()=>{
      const rows = getCanonicalForSave();
      if(!rows){
        alert("No hay datos canónicos para guardar. Primero carga archivos y verifica que se procesen.");
        return;
      }
      const ok = historicoSave(rows);
      updateHistoricoUI({loaded:false, count: rows.length, rows});
      if(ok) alert("Histórico guardado correctamente. Al volver a abrir el aplicativo se cargará automáticamente.");
      else alert("No fue posible guardar el histórico (revisa permisos de almacenamiento del navegador).");
    });
  }

  if(btnCargar){
    btnCargar.addEventListener("click", ()=>{
      const payload = historicoLoad();
      if(!payload){ alert("No hay histórico guardado."); updateHistoricoUI({loaded:false, count:0}); return; }
      applyLoadedHistoricoRows(payload.rows);
      updateHistoricoUI({loaded:true, count: payload.rows.length, rows: payload.rows});
    });
  }

  if(btnBorrar){
    btnBorrar.addEventListener("click", ()=>{
      if(!confirm("¿Deseas borrar el histórico guardado en este navegador?")) return;
      historicoClear();
      updateHistoricoUI({loaded:false, count:0});
      alert("Histórico borrado.");
    });
  }
}

document.addEventListener("DOMContentLoaded", ()=>{
  bindHistoricoButtons();

  // Autocarga al abrir
  const payload = historicoLoad();
  if(payload && Array.isArray(payload.rows) && payload.rows.length){
    applyLoadedHistoricoRows(payload.rows);
    updateHistoricoUI({loaded:true, count: payload.rows.length, rows: payload.rows});
  }else{
    // intenta pintar info si ya había algo en pantalla
    updateHistoricoUI({loaded:false, count:0});
  }
});
// =================== /HISTÓRICO PERSISTENTE (HUHMP) ===================



// ===============================
// Scroll horizontal con la rueda del mouse en tablas
// (evita texto "remontado" usando barra inferior)
// ===============================
window.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.table-wrap').forEach(el => {
    el.addEventListener('wheel', (evt) => {
      if (evt.deltaY !== 0 && el.scrollWidth > el.clientWidth) {
        evt.preventDefault();
        el.scrollLeft += evt.deltaY;
      }
    }, { passive: false });
  });
});

