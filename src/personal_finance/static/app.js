const money = new Intl.NumberFormat("en-CA", {
  style: "currency",
  currency: "CAD",
  maximumFractionDigits: 2,
});

const num = new Intl.NumberFormat("en-CA", { maximumFractionDigits: 2 });

const colors = [
  "#1f6feb",
  "#198754",
  "#c27c0e",
  "#c2415b",
  "#6f42c1",
  "#0f7f8c",
  "#8f5e15",
  "#536471",
  "#4d7c0f",
  "#b45309",
  "#0f766e",
  "#7c3aed",
];

let dashboard = null;
let selectedCategory = null;

function $(id) {
  return document.getElementById(id);
}

function asNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function fmtMoney(value) {
  return money.format(asNumber(value));
}

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const payload = await res.json();
      detail = payload.detail || detail;
    } catch (_) {
      /* ignore */
    }
    throw new Error(detail);
  }
  return res.json();
}

function rowsToTable(rows, columns, { rowClick } = {}) {
  if (!rows || rows.length === 0) return `<div class="empty">No rows to show.</div>`;
  const body = rows
    .map((row, idx) => {
      const cells = columns
        .map((col) => {
          const raw = typeof col.value === "function" ? col.value(row) : row[col.key];
          const value = col.money ? fmtMoney(raw) : col.number ? num.format(asNumber(raw)) : raw;
          return `<td class="${col.align === "right" ? "num" : ""}">${esc(value)}</td>`;
        })
        .join("");
      const attrs = rowClick ? ` tabindex="0" data-row-index="${idx}"` : "";
      return `<tr${attrs}>${cells}</tr>`;
    })
    .join("");
  const head = columns.map((col) => `<th class="${col.align === "right" ? "num" : ""}">${esc(col.label)}</th>`).join("");
  return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function attachRowClicks(container, rows, callback) {
  if (!container || !callback) return;
  container.querySelectorAll("tbody tr").forEach((tr) => {
    tr.addEventListener("click", () => callback(rows[Number(tr.dataset.rowIndex)]));
    tr.addEventListener("keydown", (event) => {
      if (event.key === "Enter") callback(rows[Number(tr.dataset.rowIndex)]);
    });
  });
}

function pivotCategories(rows) {
  const months = [...new Set(rows.map((r) => String(r.statement_month)))].sort();
  const categories = [...new Set(rows.map((r) => String(r.category)))];
  const lookup = new Map(rows.map((r) => [`${r.category}|||${r.statement_month}`, asNumber(r.expense)]));
  return { months, categories, lookup };
}

function setKpis(data) {
  const ov = data.overview || {};
  $("kpi-total-spend").textContent = fmtMoney(ov.total_spend);
  $("kpi-credit").textContent = fmtMoney(ov.credit_card_expense);
  $("kpi-external-spend").textContent = fmtMoney(ov.external_spend_excluding_credit_line_principal);
  $("kpi-loc-principal").textContent = fmtMoney(ov.credit_line_principal_payments);
  $("kpi-lg").textContent = fmtMoney(ov.lg_payroll);
  $("kpi-el").textContent = fmtMoney(ov.el_payroll);
  $("kpi-months").textContent = `Months: ${ov.months_included || "not available"}`;
  $("kpi-income-diff").textContent = `${fmtMoney(ov.payroll_external_diff)} external cash-in not matched to LG/EL payroll`;
  $("audit-middle-gaps").textContent = ov.middle_gap_count ?? 0;
  $("audit-failed").textContent = ov.failed_reconciliation_count ?? 0;
  $("audit-partial").textContent = ov.partial_or_missing_count ?? 0;
  $("audit-accounts").textContent = ov.account_count ?? 0;
  $("audit-duplicates").textContent = `${ov.exact_duplicate_file_count ?? 0} / ${ov.ignored_duplicate_file_count ?? 0}`;
}

function renderCategoryChart(data) {
  const rows = (data.spend?.summary || []).slice().sort((a, b) => asNumber(a.total_expense) - asNumber(b.total_expense));
  const trace = {
    type: "bar",
    orientation: "h",
    x: rows.map((row) => asNumber(row.total_expense)),
    y: rows.map((row) => row.category),
    marker: { color: rows.map((_, i) => colors[i % colors.length]) },
    customdata: rows.map((row) => row.category),
    hovertemplate: "%{y}<br>%{x:$,.2f}<extra></extra>",
  };
  Plotly.newPlot(
    "category-chart",
    [trace],
    {
      margin: { l: 180, r: 20, t: 10, b: 36 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      xaxis: { tickprefix: "$", gridcolor: "rgba(15,23,42,.09)" },
      yaxis: { fixedrange: true },
      font: { family: "Inter, sans-serif", color: "#334155" },
    },
    { responsive: true, displaylogo: false }
  );
  $("category-chart").on("plotly_click", (event) => {
    const point = event.points?.[0];
    if (!point) return;
    showCategoryDetails(point.customdata);
  });
}

function renderIncomeChart(data) {
  const rows = (data.income?.check_by_month || []).filter((r) => r.statement_month !== "TOTAL");
  const months = rows.map((r) => r.statement_month);
  Plotly.newPlot(
    "income-chart",
    [
      { type: "bar", name: "External cash-in", x: months, y: rows.map((r) => asNumber(r.external_cash_in)), marker: { color: "#0f766e" } },
      { type: "bar", name: "LG payroll", x: months, y: rows.map((r) => asNumber(r.lg_payroll)), marker: { color: "#1f6feb" } },
      { type: "bar", name: "EL payroll", x: months, y: rows.map((r) => asNumber(r.el_unitytech_payroll)), marker: { color: "#c27c0e" } },
    ],
    {
      barmode: "group",
      margin: { l: 54, r: 12, t: 8, b: 36 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      yaxis: { tickprefix: "$", gridcolor: "rgba(15,23,42,.09)" },
      xaxis: { type: "category" },
      legend: { orientation: "h", y: -0.24 },
      font: { family: "Inter, sans-serif", color: "#334155" },
    },
    { responsive: true, displaylogo: false }
  );
}

function renderIncomeTable(data) {
  const rows = data.income?.check_by_month || [];
  $("income-table").innerHTML = rowsToTable(rows, [
    { key: "statement_month", label: "Month" },
    { key: "external_cash_in", label: "External in", money: true, align: "right" },
    { key: "lg_payroll", label: "LG", money: true, align: "right" },
    { key: "el_unitytech_payroll", label: "EL", money: true, align: "right" },
    { key: "lg_plus_el_payroll", label: "LG + EL", money: true, align: "right" },
    { key: "excluded_internal_cash_in", label: "Excluded internal", money: true, align: "right" },
    { key: "payroll_vs_external_cash_in_diff", label: "Unmatched external", money: true, align: "right" },
  ]);
}

function renderSpendSplitTable(data) {
  const ov = data.overview || {};
  const rows = [
    {
      line: "External spend",
      amount: ov.external_spend_excluding_credit_line_principal,
      notes: "Own-account transfers and LOC principal excluded",
    },
    {
      line: "Credit-line principal",
      amount: ov.credit_line_principal_payments,
      notes: "Shown separately from survival spend",
    },
    {
      line: "Credit cards",
      amount: ov.credit_card_expense,
      notes: "Visa and MasterCard purchase/debit activity",
    },
    {
      line: "Debit bank spend",
      amount: ov.debit_bank_expense,
      notes: "Chequing debit outflows after own transfers removed",
    },
  ];
  $("spend-split-table").innerHTML = rowsToTable(rows, [
    { key: "line", label: "Line" },
    { key: "amount", label: "Amount", money: true, align: "right" },
    { key: "notes", label: "Treatment" },
  ]);
}

function renderCategoryTable(data) {
  const rows = data.spend?.summary || [];
  const container = $("category-table");
  container.innerHTML = rowsToTable(rows, [
    { key: "category", label: "Category" },
    { key: "total_expense", label: "Total", money: true, align: "right" },
    { key: "share_of_total", label: "Share %", number: true, align: "right" },
    { key: "transaction_count", label: "Tx", number: true, align: "right" },
  ], { rowClick: true });
  attachRowClicks(container, rows, (row) => showCategoryDetails(row.category));
}

function renderMonthCategoryTable(data) {
  const rows = data.spend?.by_month || [];
  const { months, categories, lookup } = pivotCategories(rows);
  const totals = new Map();
  categories.forEach((cat) => {
    totals.set(cat, months.reduce((sum, month) => sum + (lookup.get(`${cat}|||${month}`) || 0), 0));
  });
  const sorted = categories.sort((a, b) => (totals.get(b) || 0) - (totals.get(a) || 0));
  const tableRows = sorted.map((category) => {
    const row = { category, total: totals.get(category) || 0 };
    months.forEach((month) => {
      row[month] = lookup.get(`${category}|||${month}`) || 0;
    });
    return row;
  });
  const columns = [
    { key: "category", label: "Category" },
    ...months.map((month) => ({ key: month, label: month, money: true, align: "right" })),
    { key: "total", label: "Total", money: true, align: "right" },
  ];
  const container = $("month-category-table");
  container.innerHTML = rowsToTable(tableRows, columns, { rowClick: true });
  attachRowClicks(container, tableRows, (row) => showCategoryDetails(row.category));
}

function renderAuditTable(data, mode = "failed-reconciliation") {
  const audit = data.audit || {};
  let rows = audit.failed_reconciliation || [];
  let columns = [
    { key: "filename", label: "Statement" },
    { key: "account_key", label: "Account" },
    { key: "statement_month", label: "Month" },
    { key: "diff_in", label: "Diff in", money: true, align: "right" },
    { key: "diff_out", label: "Diff out", money: true, align: "right" },
    { key: "notes", label: "Notes" },
  ];
  if (mode === "middle-gaps") {
    rows = audit.middle_gaps || [];
    columns = [
      { key: "account_key", label: "Account" },
      { key: "available_months", label: "Available" },
      { key: "middle_missing_months", label: "Middle missing" },
      { key: "trailing_missing_months", label: "Trailing missing" },
    ];
  } else if (mode === "partial-missing") {
    rows = audit.partial_or_missing || [];
    columns = [
      { key: "account_key", label: "Account" },
      { key: "month", label: "Month" },
      { key: "status", label: "Status" },
      { key: "filenames", label: "File" },
      { key: "notes", label: "Notes" },
    ];
  } else if (mode === "accounts") {
    rows = audit.account_summary || [];
    columns = [
      { key: "account_key", label: "Account" },
      { key: "available_months", label: "Available" },
      { key: "middle_missing_count", label: "Middle gaps", number: true, align: "right" },
      { key: "months_partial", label: "Partial", number: true, align: "right" },
      { key: "months_missing", label: "Missing", number: true, align: "right" },
    ];
  } else if (mode === "duplicates") {
    rows = [...(audit.exact_duplicate_files || []), ...(audit.ignored_duplicate_files || [])];
    columns = [
      { key: "filename", label: "File" },
      { key: "logical_statement_key", label: "Logical statement" },
      { key: "ignored_duplicate_copy", label: "Ignored" },
      { key: "same_logical_statement_count", label: "Same logical", number: true, align: "right" },
      { key: "same_content_file_count", label: "Same content", number: true, align: "right" },
      { key: "sha256", label: "SHA-256" },
    ];
  }
  const container = $("audit-table");
  container.innerHTML = rowsToTable(rows, columns, { rowClick: true });
  attachRowClicks(container, rows, (row) => showAuditDetails(row));
}

function showAuditDetails(row) {
  const entries = Object.entries(row || {}).map(([field, value]) => ({ field, value }));
  $("detail-title").textContent = "Audit row";
  $("detail-subtitle").textContent = row?.filename || row?.account_key || "Selected audit detail";
  $("detail-table").innerHTML = rowsToTable(entries, [
    { key: "field", label: "Field" },
    { key: "value", label: "Value" },
  ]);
}

function showCategoryDetails(category, month = null) {
  selectedCategory = category;
  const rows = (dashboard.spend?.transactions || [])
    .filter((row) => row.category === category)
    .filter((row) => (month ? row.statement_month === month : true))
    .sort((a, b) => asNumber(b.amount) - asNumber(a.amount));
  const total = rows.reduce((sum, row) => sum + asNumber(row.amount), 0);
  $("detail-title").textContent = category;
  $("detail-subtitle").textContent = `${month || "All included months"} · ${rows.length} transactions · ${fmtMoney(total)}`;
  $("detail-table").innerHTML = rowsToTable(rows, [
    { key: "statement_month", label: "Month" },
    { key: "transaction_date", label: "Date" },
    { key: "spend_source", label: "Source" },
    { key: "account_key", label: "Account" },
    { key: "description", label: "Description" },
    { key: "amount", label: "Amount", money: true, align: "right" },
  ]);
}

function clearDetails() {
  selectedCategory = null;
  $("detail-title").textContent = "Details";
  $("detail-subtitle").textContent = "Select a category, audit status, or chart segment.";
  $("detail-table").innerHTML = `<div class="empty">No selection yet.</div>`;
}

function render(data) {
  dashboard = data;
  setKpis(data);
  renderCategoryChart(data);
  renderIncomeChart(data);
  renderIncomeTable(data);
  renderSpendSplitTable(data);
  renderMonthCategoryTable(data);
  renderCategoryTable(data);
  renderAuditTable(data);
  clearDetails();
}

async function loadDashboard() {
  try {
    render(await fetchJson("/api/audit-dashboard"));
  } catch (error) {
    $("detail-title").textContent = "Audit reports unavailable";
    $("detail-subtitle").textContent = error.message;
    $("detail-table").innerHTML = `<div class="empty">Use Refresh audit to regenerate the reports.</div>`;
  }
}

async function refreshAudit() {
  const btn = $("reload-btn");
  btn.disabled = true;
  btn.textContent = "Refreshing...";
  $("process-status").textContent = "Processing uploaded statements...";
  try {
    render(await fetchJson("/api/audit-refresh", { method: "POST" }));
    $("process-status").textContent = "Processed files and refreshed audit.";
  } catch (error) {
    $("detail-title").textContent = "Refresh failed";
    $("detail-subtitle").textContent = error.message;
    $("process-status").textContent = "Processing failed.";
  } finally {
    btn.disabled = false;
    btn.textContent = "Process files";
  }
}

async function uploadStatements(event) {
  const files = event.target.files;
  if (!files || files.length === 0) return;
  const btn = $("reload-btn");
  btn.disabled = true;
  btn.textContent = "Uploading...";
  $("process-status").textContent = `Uploading ${files.length} file${files.length === 1 ? "" : "s"}...`;
  const body = new FormData();
  [...files].forEach((file) => body.append("files", file));
  try {
    await fetchJson("/api/audit-upload", { method: "POST", body });
    btn.textContent = "Refreshing...";
    $("process-status").textContent = "Upload complete. Processing files...";
    render(await fetchJson("/api/audit-refresh", { method: "POST" }));
    $("process-status").textContent = "Uploaded and processed files.";
  } catch (error) {
    $("detail-title").textContent = "Upload failed";
    $("detail-subtitle").textContent = error.message;
    $("process-status").textContent = "Upload failed.";
  } finally {
    btn.disabled = false;
    btn.textContent = "Process files";
    event.target.value = "";
  }
}

document.addEventListener("DOMContentLoaded", () => {
  $("reload-btn").addEventListener("click", refreshAudit);
  $("file-input").addEventListener("change", uploadStatements);
  $("clear-detail-btn").addEventListener("click", clearDetails);
  document.querySelectorAll(".audit-pill").forEach((button) => {
    button.addEventListener("click", () => {
      renderAuditTable(dashboard, button.dataset.detail);
      $("detail-title").textContent = button.querySelector("span")?.textContent || "Audit";
      $("detail-subtitle").textContent = "Audit table updated.";
      $("detail-table").innerHTML = `<div class="empty">Use the audit table on the left.</div>`;
    });
  });
  loadDashboard();
});
