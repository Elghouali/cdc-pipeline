/* =============================================================
   CDC Pipeline Dashboard — logique frontend
   ============================================================= */

// ─── Configuration ──────────────────────────────────────────
// Quand le dashboard est servi par Flask, on tape sur la meme origine.
// On peut surcharger via ?api=http://autre-host:5000 dans l'URL.
const params = new URLSearchParams(window.location.search);
const API_BASE = params.get("api") || "";
const REFRESH_MS = 15000;

// ─── Helpers ────────────────────────────────────────────────
async function fetchJson(path) {
  try {
    const r = await fetch(API_BASE + path, { cache: "no-store" });
    return await r.json();
  } catch (err) {
    console.error("fetch fail", path, err);
    return null;
  }
}

function fmtInt(n) {
  if (n === null || n === undefined) return "—";
  return new Intl.NumberFormat("fr-FR").format(n);
}

function fmtMoney(n) {
  if (n === null || n === undefined) return "—";
  return new Intl.NumberFormat("fr-FR", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(n);
}

function fmtSec(n) {
  if (n === null || n === undefined) return "—";
  return n.toFixed(2) + " s";
}

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function clearTable(id) {
  document.querySelector(`#${id} tbody`).innerHTML = "";
}

function appendRow(tableId, cells) {
  const tr = document.createElement("tr");
  cells.forEach((c) => {
    const td = document.createElement("td");
    td.innerHTML = c.html ?? String(c);
    if (c.num) td.classList.add("num");
    tr.appendChild(td);
  });
  document.querySelector(`#${tableId} tbody`).appendChild(tr);
}

// ─── Chart.js theme ─────────────────────────────────────────
Chart.defaults.color = "#94a3b8";
Chart.defaults.borderColor = "#334155";
Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, Segoe UI";

const PALETTE = [
  "#6366f1", "#06b6d4", "#10b981", "#f59e0b",
  "#ef4444", "#8b5cf6", "#ec4899", "#84cc16",
];

// Conserve les instances pour pouvoir les detruire avant un re-render
const charts = {};
function renderChart(id, config) {
  if (charts[id]) charts[id].destroy();
  const ctx = document.getElementById(id);
  if (!ctx) return;
  charts[id] = new Chart(ctx, config);
}

// ─── Onglets ────────────────────────────────────────────────
document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".tab-panel").forEach((p) => p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById(btn.dataset.tab).classList.add("active");
  });
});

// ─── Charge : resume global (vue d'ensemble) ────────────────
async function loadSummary() {
  const s = await fetchJson("/api/summary");
  if (!s) return;
  setText("kpi-cdc", fmtInt(s.total_cdc_events));
  setText("kpi-orders", fmtInt(s.total_orders));
  setText("kpi-customers", fmtInt(s.total_customers));
  setText("kpi-revenue", fmtMoney(s.total_revenue));
  setText("kpi-latency", fmtSec(s.avg_latency_sec));
}

// ─── Charge : etat services ─────────────────────────────────
async function loadHealth() {
  const h = await fetchJson("/api/health");
  if (!h) return;
  setText("kpi-services", `${h.summary.up} / ${h.summary.total}`);
  setText("last-update", "Mis à jour à " + h.summary.checked_at);

  const grid = document.getElementById("services-list");
  grid.innerHTML = "";
  h.services.forEach((s) => {
    const div = document.createElement("div");
    div.className = "service-item " + (s.up ? "up" : "down");
    div.innerHTML = `
      <div>
        <span class="name">${s.name}</span>
        <span class="role">${s.role}</span>
      </div>
      <span class="badge ${s.up ? "up" : "down"}">${s.up ? "Opérationnel" : "Hors-ligne"}</span>
    `;
    grid.appendChild(div);
  });
}

// ─── Charge : connecteurs Debezium ──────────────────────────
async function loadConnectors() {
  const d = await fetchJson("/api/connectors");
  const box = document.getElementById("connectors-box");
  if (!d) { box.innerHTML = '<p class="muted">Indisponible</p>'; return; }
  if (d.error) { box.innerHTML = `<p class="muted">${d.error}</p>`; return; }
  if (!d.connectors.length) {
    box.innerHTML = '<p class="muted">Aucun connecteur enregistré (lancer scripts/register_connector.sh)</p>';
    return;
  }
  box.innerHTML = "";
  d.connectors.forEach((c) => {
    const stateClass = c.state === "RUNNING" ? "running"
                     : c.state === "FAILED"  ? "failed"
                     : "unknown";
    const tasksHtml = (c.tasks || []).map(
      (t) => `<span class="badge ${t.state === "RUNNING" ? "running" : "failed"}">tâche #${t.id} : ${t.state}</span>`
    ).join(" ");
    const item = document.createElement("div");
    item.className = "service-item " + (c.state === "RUNNING" ? "up" : "down");
    item.innerHTML = `
      <div>
        <span class="name">${c.name}</span>
        <span class="role">Type : ${c.type || "—"}</span>
      </div>
      <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap; justify-content:flex-end;">
        <span class="badge ${stateClass}">${c.state}</span>
        ${tasksHtml}
      </div>
    `;
    box.appendChild(item);
  });
}

// ─── Charge : Spark ─────────────────────────────────────────
async function loadSpark() {
  const d = await fetchJson("/api/spark");
  const box = document.getElementById("spark-box");
  if (!d) { box.innerHTML = '<p class="muted">Indisponible</p>'; return; }
  if (d.error) { box.innerHTML = `<p class="muted">${d.error}</p>`; return; }
  if (!d.active_apps.length) {
    box.innerHTML = `<p class="muted">${d.workers || 0} worker(s) — aucune application Spark active.</p>`;
    return;
  }
  let html = `<p class="muted">${d.workers} worker(s) — ${d.active_apps.length} application(s) active(s) — ${d.completed_apps} terminées</p>`;
  d.active_apps.forEach((a) => {
    html += `
      <div class="service-item up">
        <div>
          <span class="name">${a.name}</span>
          <span class="role">${a.cores} cœur(s) · ${a.memory_per_executor_mb} Mo/exec · démarré ${a.submit_date || ""}</span>
        </div>
        <span class="badge running">${a.state}</span>
      </div>`;
  });
  box.innerHTML = html;
}

// ─── Charge : liens rapides ─────────────────────────────────
async function loadLinks() {
  const links = await fetchJson("/api/links");
  if (!links) return;
  const grid = document.getElementById("links-grid");
  grid.innerHTML = "";
  links.forEach((l) => {
    const a = document.createElement("a");
    a.href = l.url;
    a.target = "_blank";
    a.rel = "noopener";
    a.textContent = l.name;
    grid.appendChild(a);
  });
}

// ─── Charge : info / stack ──────────────────────────────────
async function loadInfo() {
  const info = await fetchJson("/api/info");
  if (!info) return;
  const stackEl = document.getElementById("stack-list");
  stackEl.innerHTML = "";
  info.stack.forEach((t) => {
    const tag = document.createElement("span");
    tag.className = "tag";
    tag.textContent = t;
    stackEl.appendChild(tag);
  });
}

// ─── KPI : CDC counts (bar + pie + table) ───────────────────
async function loadCdcEvents() {
  const r = await fetchJson("/api/kpi/cdc_event_counts");
  const tbody = "table-cdc-events";
  clearTable(tbody);
  if (!r || !r.data || !r.data.length) {
    appendRow(tbody, [{ html: '<em>Aucune donnée — la pipeline n\'a pas encore produit d\'évènements.</em>' }, "", "", "", "", ""]);
    return;
  }
  let totalIns = 0, totalUpd = 0, totalDel = 0, totalSnap = 0;
  r.data.forEach((row) => {
    appendRow(tbody, [
      row.source_table,
      { html: fmtInt(row.total_events), num: true },
      { html: fmtInt(row.inserts), num: true },
      { html: fmtInt(row.updates), num: true },
      { html: fmtInt(row.deletes), num: true },
      { html: fmtInt(row.snapshots), num: true },
    ]);
    totalIns  += +row.inserts || 0;
    totalUpd  += +row.updates || 0;
    totalDel  += +row.deletes || 0;
    totalSnap += +row.snapshots || 0;
  });

  // Bar chart : volume par table
  renderChart("chart-cdc-bar", {
    type: "bar",
    data: {
      labels: r.data.map((d) => d.source_table),
      datasets: [{
        label: "Évènements CDC",
        data: r.data.map((d) => d.total_events),
        backgroundColor: PALETTE[0],
        borderRadius: 6,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true } },
    },
  });

  // Pie : ventilation par operation
  renderChart("chart-cdc-pie", {
    type: "doughnut",
    data: {
      labels: ["Insertions (c)", "Updates (u)", "Deletes (d)", "Snapshots (r)"],
      datasets: [{
        data: [totalIns, totalUpd, totalDel, totalSnap],
        backgroundColor: [PALETTE[2], PALETTE[0], PALETTE[4], PALETTE[3]],
        borderColor: "#1e293b",
        borderWidth: 2,
      }],
    },
    options: { responsive: true, maintainAspectRatio: false },
  });
}

// ─── KPI : latency table ────────────────────────────────────
async function loadLatency() {
  const r = await fetchJson("/api/kpi/latency");
  clearTable("table-latency");
  if (!r || !r.data || !r.data.length) {
    appendRow("table-latency", [{ html: "<em>Aucune donnée</em>" }, "", "", "", ""]);
    return;
  }
  r.data.forEach((row) => {
    appendRow("table-latency", [
      row.source_table,
      { html: fmtInt(row.events), num: true },
      { html: fmtSec(row.avg_latency_sec), num: true },
      { html: fmtSec(row.max_latency_sec), num: true },
      { html: fmtSec(row.p95_latency_sec), num: true },
    ]);
  });
}

// ─── KPI : ventes par canal ─────────────────────────────────
async function loadSales() {
  const r = await fetchJson("/api/kpi/sales_by_channel");
  if (!r || !r.data || !r.data.length) {
    renderChart("chart-sales", { type: "bar", data: { labels: [], datasets: [] }});
    return;
  }
  // Pivot : mois en X, canal -> serie
  const months = [...new Set(r.data.map((d) => d.sale_month))].sort();
  const channels = [...new Set(r.data.map((d) => d.channel))];
  const datasets = channels.map((ch, i) => ({
    label: ch,
    data: months.map((m) => {
      const row = r.data.find((d) => d.sale_month === m && d.channel === ch);
      return row ? +row.gross_revenue : 0;
    }),
    backgroundColor: PALETTE[i % PALETTE.length],
    borderRadius: 4,
  }));
  renderChart("chart-sales", {
    type: "bar",
    data: { labels: months, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true } },
    },
  });
}

// ─── KPI : paiements (doughnut) ─────────────────────────────
async function loadPayments() {
  const r = await fetchJson("/api/kpi/payment_methods");
  if (!r || !r.data || !r.data.length) {
    renderChart("chart-payments", { type: "doughnut", data: { labels: [], datasets: [] }});
    return;
  }
  const byMethod = {};
  r.data.forEach((d) => {
    byMethod[d.method] = (byMethod[d.method] || 0) + (+d.total_collected || 0);
  });
  const labels = Object.keys(byMethod);
  renderChart("chart-payments", {
    type: "doughnut",
    data: {
      labels,
      datasets: [{
        data: labels.map((l) => byMethod[l]),
        backgroundColor: labels.map((_, i) => PALETTE[i % PALETTE.length]),
        borderColor: "#1e293b",
        borderWidth: 2,
      }],
    },
    options: { responsive: true, maintainAspectRatio: false },
  });
}

// ─── KPI : annulations / retours ────────────────────────────
async function loadCancelReturn() {
  const r = await fetchJson("/api/kpi/cancel_return_rates");
  if (!r || !r.data || !r.data.length) {
    renderChart("chart-cancel-return", { type: "line", data: { labels: [], datasets: [] }});
    return;
  }
  const months = r.data.map((d) => d.order_month).reverse();
  renderChart("chart-cancel-return", {
    type: "line",
    data: {
      labels: months,
      datasets: [
        {
          label: "Taux d'annulation (%)",
          data: r.data.map((d) => +d.cancellation_rate_pct || 0).reverse(),
          borderColor: PALETTE[4],
          backgroundColor: "rgba(239,68,68,0.1)",
          tension: 0.3,
          fill: true,
        },
        {
          label: "Taux de retour (%)",
          data: r.data.map((d) => +d.return_rate_pct || 0).reverse(),
          borderColor: PALETTE[3],
          backgroundColor: "rgba(245,158,11,0.1)",
          tension: 0.3,
          fill: true,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: { y: { beginAtZero: true } },
    },
  });
}

// ─── KPI : segment x categorie ──────────────────────────────
async function loadSegmentTable() {
  const r = await fetchJson("/api/kpi/revenue_segment_category");
  clearTable("table-segment");
  if (!r || !r.data || !r.data.length) {
    appendRow("table-segment", [{ html: "<em>Aucune donnée</em>" }, "", "", "", ""]);
    return;
  }
  r.data.forEach((row) => {
    appendRow("table-segment", [
      row.customer_segment || "—",
      row.product_category || "—",
      { html: fmtInt(row.total_orders), num: true },
      { html: fmtMoney(row.gross_revenue), num: true },
      { html: fmtMoney(row.net_revenue), num: true },
    ]);
  });
}

// ─── KPI : top clients ──────────────────────────────────────
async function loadTopCustomers() {
  const r = await fetchJson("/api/kpi/top_customers");
  clearTable("table-top-customers");
  if (!r || !r.data || !r.data.length) {
    appendRow("table-top-customers", [{ html: "<em>Aucune donnée</em>" }, "", "", "", "", "", "", ""]);
    return;
  }
  r.data.forEach((row, i) => {
    appendRow("table-top-customers", [
      i + 1,
      row.name,
      row.city,
      row.segment,
      { html: fmtInt(row.total_orders), num: true },
      { html: fmtMoney(row.lifetime_value), num: true },
      { html: fmtMoney(row.avg_basket), num: true },
      row.last_order_date || "—",
    ]);
  });
}

// ─── Base source : tables Postgres ──────────────────────────
let currentPgTable = null;

async function loadPostgresTables() {
  const r = await fetchJson("/api/postgres/tables");
  if (!r) return;
  const cards = document.getElementById("pg-summary-cards");
  const tabs = document.getElementById("pg-tabs");
  cards.innerHTML = "";
  tabs.innerHTML = "";

  r.tables.forEach((t, i) => {
    // Petite carte resumee
    const card = document.createElement("div");
    card.className = "kpi-card";
    card.innerHTML = `
      <div class="kpi-label">${t.name}</div>
      <div class="kpi-value">${t.ok ? fmtInt(t.row_count) : "—"}</div>
      <div class="kpi-hint">${t.ok ? t.columns.length + " colonnes" : "indisponible"}</div>
    `;
    cards.appendChild(card);

    // Bouton-onglet pour le tableau detaille
    const tab = document.createElement("span");
    tab.className = "tag";
    tab.textContent = t.name;
    tab.dataset.table = t.name;
    if (!t.ok) tab.style.opacity = "0.4";
    tab.addEventListener("click", () => loadPostgresRows(t.name));
    tabs.appendChild(tab);
  });

  // Charge la premiere table par defaut
  if (!currentPgTable && r.tables.length) {
    const first = r.tables.find((t) => t.ok) || r.tables[0];
    loadPostgresRows(first.name);
  } else if (currentPgTable) {
    // Re-active le bon onglet apres refresh
    document.querySelectorAll("#pg-tabs .tag").forEach((b) => {
      if (b.dataset.table === currentPgTable) b.classList.add("active");
    });
  }
}

async function loadPostgresRows(table) {
  currentPgTable = table;
  document.querySelectorAll("#pg-tabs .tag").forEach((b) => {
    b.classList.toggle("active", b.dataset.table === table);
  });
  document.getElementById("pg-table-title").textContent = "Table : " + table;
  document.getElementById("pg-table-meta").textContent = "Chargement…";

  const r = await fetchJson("/api/postgres/table/" + table);
  const headerRow = document.querySelector("#pg-table thead tr");
  const tbody = document.querySelector("#pg-table tbody");
  headerRow.innerHTML = "";
  tbody.innerHTML = "";

  if (!r || r.error) {
    document.getElementById("pg-table-meta").textContent =
      "Erreur : " + (r?.error || "inaccessible");
    return;
  }
  document.getElementById("pg-table-meta").textContent =
    `${r.count} lignes affichées (max 50, triées par id DESC)`;

  r.columns.forEach((c) => {
    const th = document.createElement("th");
    th.textContent = c;
    headerRow.appendChild(th);
  });

  r.rows.forEach((row) => {
    const tr = document.createElement("tr");
    r.columns.forEach((c) => {
      const td = document.createElement("td");
      const v = row[c];
      td.textContent = v === null ? "—" : String(v);
      if (typeof v === "number") td.classList.add("num");
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

// ─── Scripts & actions ──────────────────────────────────────
function logAction(text, cls = "info") {
  const log = document.getElementById("action-log");
  const time = new Date().toLocaleTimeString();
  const line = document.createElement("span");
  line.className = "log-line " + cls;
  line.textContent = `[${time}] ${text}\n`;
  if (log.textContent === "En attente d'une action…") log.textContent = "";
  log.appendChild(line);
  log.scrollTop = log.scrollHeight;
}

async function runAction(id, btn) {
  if (btn) { btn.disabled = true; btn.textContent = "En cours…"; }
  logAction(`▶ Lancement : ${id}`, "info");
  try {
    const r = await fetch(API_BASE + "/api/actions/run/" + id, { method: "POST" });
    const data = await r.json();
    if (data.ok) {
      const summary = JSON.stringify(data.result);
      logAction(`✓ Succès — ${summary}`, "ok");
    } else {
      logAction(`✗ Échec — ${data.error || "?"}`, "err");
    }
  } catch (e) {
    logAction(`✗ Erreur réseau : ${e}`, "err");
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "Exécuter"; }
  }
}

async function loadActionsList() {
  const r = await fetchJson("/api/actions/list");
  if (!r) return;
  const grid = document.getElementById("actions-grid");
  grid.innerHTML = "";
  r.actions.forEach((a) => {
    const card = document.createElement("div");
    card.className = "action-card";
    card.innerHTML = `
      <h4>${a.label}</h4>
      <p>${a.description}</p>
      <button class="btn-action">Exécuter</button>
    `;
    card.querySelector("button").addEventListener("click", (ev) => runAction(a.id, ev.target));
    grid.appendChild(card);
  });
}

document.getElementById("clear-log").addEventListener("click", () => {
  document.getElementById("action-log").textContent = "En attente d'une action…";
});

// Charge la liste d'actions une seule fois
loadActionsList();

// ─── Orchestration ──────────────────────────────────────────
async function refreshAll() {
  await Promise.all([
    loadSummary(),
    loadHealth(),
    loadLinks(),
    loadInfo(),
    loadConnectors(),
    loadSpark(),
    loadCdcEvents(),
    loadLatency(),
    loadSales(),
    loadPayments(),
    loadCancelReturn(),
    loadSegmentTable(),
    loadTopCustomers(),
    loadPostgresTables(),
  ]);
}

document.getElementById("refresh-btn").addEventListener("click", refreshAll);

// Premier chargement + auto-refresh
refreshAll();
setInterval(refreshAll, REFRESH_MS);
