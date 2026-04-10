const currency = new Intl.NumberFormat("en-CA", {
  style: "currency",
  currency: "CAD",
  maximumFractionDigits: 2,
});

const PALETTE = {
  primary: "#26a65b",
  primarySoft: "rgba(38, 166, 91, 0.15)",
  secondary: "#1d8f4d",
  slate: "#5a6b7a",
  coral: "#e85d4c",
  amber: "#e6a23c",
  blue: "#3d7ea6",
  purple: "#7b68a6",
  ink: "#1a252e",
  muted: "#6b7c8c",
  grid: "rgba(61, 78, 92, 0.12)",
  plotBg: "#fafbfc",
};

const CHART_COLORS = [
  PALETTE.primary,
  PALETTE.blue,
  PALETTE.amber,
  PALETTE.coral,
  PALETTE.purple,
  "#2d9cdb",
  "#8bc34a",
  PALETTE.slate,
];

const plotlyConfig = {
  responsive: true,
  displayModeBar: true,
  displaylogo: false,
  modeBarButtonsToRemove: ["lasso2d", "select2d"],
};

const state = {
  summary: null,
  ruleConfig: null,
  currentJobId: null,
  pollHandle: null,
  /** "extract" after upload, or "reload" from header — controls completion UI */
  processJobContext: null,
  pendingSavedFileCount: 0,
  rulesToolbar: {
    search: "",
    filterCategory: "",
    filterNecessity: "",
    filterBeneficiary: "",
  },
};

const EXCLUDE_STORAGE_KEY = "pf-exclude-v1";
const EXTRACTION_PRESET_STORAGE_KEY = "pf-extraction-preset-v1";
const DASH_CHART_ORDER_KEY = "pf-chart-order-v1";

let dashboardDebouncer = null;
/** Shown after the next loadDashboard completes (chart drill-down feedback). */
let pendingDrilldownHint = null;
let pendingDrilldownScroll = false;
/** Substring filter for /api/summary `q` (e.g. merchant bar click); cleared when sidebar filters change. */
let summaryTextSearch = "";

function defaultExcludeState() {
  return { categories: [], necessities: [], beneficiaries: [] };
}

function loadExcludeState() {
  try {
    const raw = localStorage.getItem(EXCLUDE_STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      return {
        categories: Array.isArray(parsed.categories) ? parsed.categories : [],
        necessities: Array.isArray(parsed.necessities) ? parsed.necessities : [],
        beneficiaries: Array.isArray(parsed.beneficiaries) ? parsed.beneficiaries : [],
      };
    }
  } catch (_) {
    /* ignore */
  }
  return defaultExcludeState();
}

function saveExcludeState(payload) {
  try {
    localStorage.setItem(EXCLUDE_STORAGE_KEY, JSON.stringify(payload));
  } catch (_) {
    /* ignore */
  }
}

let excludeState = loadExcludeState();

function normalizeSummary(raw) {
  if (!raw || typeof raw !== "object") return null;
  const arr = (v) => (Array.isArray(v) ? v : []);
  return {
    ...raw,
    meta: raw.meta && typeof raw.meta === "object" ? raw.meta : {},
    overview: raw.overview && typeof raw.overview === "object" ? raw.overview : {},
    filters: {
      owners: arr(raw.filters?.owners),
      accounts: arr(raw.filters?.accounts),
      categories: arr(raw.filters?.categories),
      months: arr(raw.filters?.months),
      necessities: arr(raw.filters?.necessities),
      beneficiaries: arr(raw.filters?.beneficiaries),
    },
    monthly_expenses: arr(raw.monthly_expenses),
    daily_expenses: arr(raw.daily_expenses),
    category_breakdown: arr(raw.category_breakdown),
    necessity_breakdown: arr(raw.necessity_breakdown),
    beneficiary_breakdown: arr(raw.beneficiary_breakdown),
    monthly_category_breakdown: arr(raw.monthly_category_breakdown),
    monthly_necessity_breakdown: arr(raw.monthly_necessity_breakdown),
    monthly_beneficiary_breakdown: arr(raw.monthly_beneficiary_breakdown),
    merchant_breakdown: arr(raw.merchant_breakdown),
    owner_breakdown: arr(raw.owner_breakdown),
    account_breakdown: arr(raw.account_breakdown),
    owner_beneficiary_breakdown: arr(raw.owner_beneficiary_breakdown),
    flow_breakdown: arr(raw.flow_breakdown),
    waterfall_breakdown: arr(raw.waterfall_breakdown),
    weekday_breakdown: arr(raw.weekday_breakdown),
    treemap_breakdown: arr(raw.treemap_breakdown),
    sunburst_breakdown: arr(raw.sunburst_breakdown),
    statement_breakdown: arr(raw.statement_breakdown),
    sankey:
      raw.sankey && typeof raw.sankey === "object"
        ? { nodes: arr(raw.sankey.nodes), links: arr(raw.sankey.links) }
        : { nodes: [], links: [] },
    matched_transfers: arr(raw.matched_transfers),
    unmatched_internal: arr(raw.unmatched_internal),
    recent_transactions: arr(raw.recent_transactions),
    filter_dimensions:
      raw.filter_dimensions && typeof raw.filter_dimensions === "object"
        ? {
            categories: arr(raw.filter_dimensions.categories),
            necessities: arr(raw.filter_dimensions.necessities),
            beneficiaries: arr(raw.filter_dimensions.beneficiaries),
          }
        : { categories: [], necessities: [], beneficiaries: [] },
    internal_review:
      raw.internal_review && typeof raw.internal_review === "object"
        ? {
            stats: raw.internal_review.stats && typeof raw.internal_review.stats === "object" ? raw.internal_review.stats : {},
            caption: String(raw.internal_review.caption || ""),
            matched_transfers: arr(raw.internal_review.matched_transfers),
            unmatched_internal: arr(raw.internal_review.unmatched_internal),
            other_uncategorized: arr(raw.internal_review.other_uncategorized),
          }
        : {
            stats: {},
            caption: "",
            matched_transfers: [],
            unmatched_internal: [],
            other_uncategorized: [],
          },
    taxonomy:
      raw.taxonomy && typeof raw.taxonomy === "object"
        ? {
            categories: arr(raw.taxonomy.categories),
            necessities: arr(raw.taxonomy.necessities),
            beneficiaries: arr(raw.taxonomy.beneficiaries),
          }
        : { categories: [], necessities: [], beneficiaries: [] },
    category_review: arr(raw.category_review),
  };
}

const ICON_UPLOAD = `<svg class="upload-ic" viewBox="0 0 24 24" width="22" height="22" aria-hidden="true"><path fill="currentColor" d="M9 16h6v-6h4l-7-7-7 7h4v6zm-4 2h14v2H5v-2z"/></svg>`;
const ICON_CHECK = `<svg class="upload-ic upload-ic--ok" viewBox="0 0 24 24" width="22" height="22" aria-hidden="true"><path fill="currentColor" d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>`;

let activityHintTimer = null;

function showErrorBanner(message) {
  const el = document.getElementById("status-banner");
  if (!el) return;
  el.hidden = false;
  el.textContent = message;
}

function hideErrorBanner() {
  const el = document.getElementById("status-banner");
  if (!el) return;
  el.hidden = true;
  el.textContent = "";
}

function showActivityHint(message, extraClass = "", durationMs = 4000) {
  const el = document.getElementById("activity-hint");
  if (!el || !message) return;
  if (activityHintTimer) clearTimeout(activityHintTimer);
  el.hidden = false;
  el.textContent = message;
  el.className = `activity-hint${extraClass ? ` ${extraClass}` : ""}`;
  activityHintTimer = setTimeout(() => {
    el.hidden = true;
    el.textContent = "";
    activityHintTimer = null;
  }, durationMs);
}

function hideActivityHint() {
  if (activityHintTimer) clearTimeout(activityHintTimer);
  activityHintTimer = null;
  const el = document.getElementById("activity-hint");
  if (el) {
    el.hidden = true;
    el.textContent = "";
  }
}

function setProgressVisual(visible, progress, text, pctText, detailText) {
  const wrap = document.getElementById("upload-progress-wrap");
  const fill = document.getElementById("progress-fill");
  const label = document.getElementById("upload-progress-text");
  const pct = document.getElementById("upload-progress-pct");
  const detailEl = document.getElementById("upload-progress-detail");
  if (!wrap || !fill) return;
  wrap.hidden = !visible;
  const w = visible ? Math.max(0, Math.min(100, (progress || 0) * 100)) : 0;
  fill.style.width = `${w}%`;
  if (label && text != null) label.textContent = text;
  if (pct) pct.textContent = visible && pctText != null && pctText !== "" ? pctText : "";
  if (detailEl) {
    const d = detailText != null && String(detailText).trim() !== "" ? String(detailText).trim() : "";
    detailEl.textContent = d;
    detailEl.hidden = !visible || !d;
  }
}

function resetUploadProgress() {
  setProgressVisual(false, 0, "Processing…", "", "");
}

function renderUploadQueued(fileCount) {
  const fb = document.getElementById("upload-feedback");
  if (!fb) return;
  fb.hidden = false;
  fb.innerHTML = `
    <div class="upload-feedback-inner upload-feedback--queued">
      <span class="upload-ic-wrap">${ICON_UPLOAD}</span>
      <div>
        <div class="upload-report-head" style="margin-bottom:2px">
          <strong>${fileCount} file${fileCount === 1 ? "" : "s"} selected</strong>
        </div>
        <p class="upload-feedback-sub">Uploading to your machine…</p>
      </div>
    </div>
  `;
}

function renderUploadSavedAwaitingExtract(savedNames) {
  const fb = document.getElementById("upload-feedback");
  if (!fb) return;
  const n = savedNames.length;
  const list =
    n <= 3
      ? savedNames.map((s) => escapeHtml(s)).join(", ")
      : `${n} files (e.g. ${escapeHtml(savedNames[0])}…)`;
  fb.hidden = false;
  fb.innerHTML = `
    <div class="upload-feedback-inner upload-feedback--queued">
      <span class="upload-ic-wrap">${ICON_UPLOAD}</span>
      <div style="flex:1;min-width:0">
        <div class="upload-report-head" style="margin-bottom:2px">
          <strong>Saved locally</strong>
        </div>
        <p class="upload-feedback-sub">${escapeHtml(String(n))} file${n === 1 ? "" : "s"}: ${list}. Choose <strong>Fast</strong> or <strong>Slow (full)</strong> above, then extract — the bar shows each model as it runs.</p>
        <button type="button" class="mint-btn mint-btn-primary mint-btn--sm upload-process-btn" data-action="process-statements">Extract transactions</button>
      </div>
    </div>
  `;
}

function renderUploadComplete(fileCount, summary) {
  const fb = document.getElementById("upload-feedback");
  if (!fb) return;
  const ov = summary.overview || {};
  const meta = summary.meta || {};
  const warnings = Array.isArray(meta.warnings) ? meta.warnings : [];
  const warnBlock =
    warnings.length > 0
      ? `<div class="upload-report-warn" role="status"><strong>Heads up:</strong> ${warnings.map((w) => escapeHtml(w)).join(" · ")}</div>`
      : "";
  fb.hidden = false;
  fb.innerHTML = `
    <div class="upload-feedback-inner upload-feedback--done">
      ${ICON_CHECK}
      <div style="flex:1;min-width:0">
        <div class="upload-report-head">
          <strong>Import complete</strong>
        </div>
        <p class="upload-feedback-sub">${fileCount} file${fileCount === 1 ? "" : "s"} merged into the dataset</p>
        <div class="upload-report-stats">
          <div class="upload-stat"><span class="upload-stat-val">${Number(ov.transaction_count ?? 0).toLocaleString()}</span><span class="upload-stat-lbl">Transactions</span></div>
          <div class="upload-stat"><span class="upload-stat-val">${Number(ov.statement_count ?? 0).toLocaleString()}</span><span class="upload-stat-lbl">Statements</span></div>
          <div class="upload-stat"><span class="upload-stat-val">${Number(ov.account_count ?? 0).toLocaleString()}</span><span class="upload-stat-lbl">Accounts</span></div>
          <div class="upload-stat"><span class="upload-stat-val">${Number(ov.owner_count ?? 0).toLocaleString()}</span><span class="upload-stat-lbl">Owners</span></div>
          <div class="upload-stat"><span class="upload-stat-val">${formatCurrency(ov.expense_total)}</span><span class="upload-stat-lbl">Spend (filtered)</span></div>
          <div class="upload-stat"><span class="upload-stat-val" style="font-size:0.875rem">${escapeHtml(ov.date_start || "—")} → ${escapeHtml(ov.date_end || "—")}</span><span class="upload-stat-lbl">Date range</span></div>
        </div>
        ${warnBlock}
      </div>
    </div>
  `;
}

function formatCurrency(value) {
  return currency.format(Number(value || 0));
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function optionMarkup(value, selectedValue, label = value) {
  const selected = value === selectedValue ? "selected" : "";
  return `<option value="${escapeHtml(value)}" ${selected}>${escapeHtml(label)}</option>`;
}

function filterValue(id) {
  return document.getElementById(id)?.value || "";
}

/** Calendar month dropdown values (match API month key YYYY-MM). */
const CAL_MONTH_OPTIONS = [
  ["01", "January"],
  ["02", "February"],
  ["03", "March"],
  ["04", "April"],
  ["05", "May"],
  ["06", "June"],
  ["07", "July"],
  ["08", "August"],
  ["09", "September"],
  ["10", "October"],
  ["11", "November"],
  ["12", "December"],
];

function updateCalendarMonthDisabledState() {
  const yearEl = document.getElementById("year-filter");
  const calEl = document.getElementById("calendar-month-filter");
  if (!yearEl || !calEl) return;
  const hasYear = Boolean(yearEl.value);
  calEl.disabled = !hasYear;
  if (!hasYear) calEl.value = "";
}

let periodUrlApplied = false;

/** Apply ?month=YYYY-MM or ?year=YYYY once after period dropdowns exist. Returns true if URL changed scope (caller may refetch). */
function applyPeriodSelectionFromUrlOnce() {
  if (periodUrlApplied) return false;
  periodUrlApplied = true;
  const p = new URLSearchParams(window.location.search);
  const ym = p.get("month");
  const yr = p.get("year");
  const yearEl = document.getElementById("year-filter");
  const calEl = document.getElementById("calendar-month-filter");
  if (!yearEl || !calEl) return false;
  let changed = false;
  if (ym && /^\d{4}-\d{2}$/.test(ym)) {
    const [y, m] = ym.split("-");
    const mm = m.length === 1 ? `0${m}` : m.slice(0, 2);
    if ([...yearEl.options].some((o) => o.value === y)) {
      yearEl.value = y;
      changed = true;
    }
    updateCalendarMonthDisabledState();
    if ([...calEl.options].some((o) => o.value === mm)) {
      calEl.value = mm;
      changed = true;
    }
  } else if (yr && /^\d{4}$/.test(yr)) {
    if ([...yearEl.options].some((o) => o.value === yr)) {
      yearEl.value = yr;
      calEl.value = "";
      changed = true;
    }
  }
  updateCalendarMonthDisabledState();
  return changed;
}

function populatePeriodFilters(summary) {
  const yearEl = document.getElementById("year-filter");
  const calEl = document.getElementById("calendar-month-filter");
  if (!yearEl || !calEl) return;
  const monthKeys = summary.filters?.months || [];
  const years = [
    ...new Set(monthKeys.map((k) => String(k).slice(0, 4)).filter((y) => /^\d{4}$/.test(y))),
  ].sort((a, b) => b.localeCompare(a));
  const prevY = yearEl.value;
  const prevM = calEl.value;
  yearEl.innerHTML = [`<option value="">All years</option>`, ...years.map((y) => optionMarkup(y, prevY, y))].join("");
  calEl.innerHTML = [
    `<option value="">All months</option>`,
    ...CAL_MONTH_OPTIONS.map(([val, lab]) => optionMarkup(val, prevM, lab)),
  ].join("");
  if (years.includes(prevY)) yearEl.value = prevY;
  else yearEl.value = "";
  const mOk = prevM && CAL_MONTH_OPTIONS.some(([v]) => v === prevM);
  if (yearEl.value && mOk) calEl.value = prevM;
  else if (!yearEl.value) calEl.value = "";
  updateCalendarMonthDisabledState();
  updatePeriodNudgeState();
}

function updatePeriodNudgeState() {
  const prev = document.getElementById("period-prev-month");
  const next = document.getElementById("period-next-month");
  const yearEl = document.getElementById("year-filter");
  const calEl = document.getElementById("calendar-month-filter");
  const on = Boolean(yearEl?.value && calEl && !calEl.disabled && calEl.value);
  if (prev) prev.disabled = !on;
  if (next) next.disabled = !on;
}

/** Debounced refresh so rapid filter changes (e.g. month compare) hit the API once. */
function scheduleDashboardRefresh() {
  if (dashboardDebouncer) clearTimeout(dashboardDebouncer);
  dashboardDebouncer = setTimeout(() => {
    dashboardDebouncer = null;
    loadDashboard().catch((err) => showErrorBanner(err.message));
  }, 260);
}

function shiftCalendarMonth(delta) {
  const yearEl = document.getElementById("year-filter");
  const calEl = document.getElementById("calendar-month-filter");
  if (!yearEl?.value || !calEl?.value || calEl.disabled) return;
  const y = parseInt(yearEl.value, 10);
  const m = parseInt(calEl.value, 10) - 1;
  const d = new Date(Date.UTC(y, m + delta, 1));
  const ny = d.getUTCFullYear();
  const nm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const years = [...yearEl.options].map((o) => o.value).filter(Boolean);
  if (!years.includes(String(ny))) {
    showActivityHint("No data for that year in the list — choose another year first.", "", 3800);
    return;
  }
  yearEl.value = String(ny);
  updateCalendarMonthDisabledState();
  calEl.value = nm;
  updatePeriodNudgeState();
  scheduleDashboardRefresh();
}

function buildScopeDescription() {
  const parts = [];
  const y = filterValue("year-filter");
  const cm = filterValue("calendar-month-filter");
  if (y && cm) {
    const lab = CAL_MONTH_OPTIONS.find(([v]) => v === cm)?.[1] || cm;
    parts.push(`${lab} ${y}`);
  } else if (y) {
    parts.push(`Year ${y} · all months`);
  } else {
    parts.push("All dates in your data");
  }
  const dims = [
    ["owner-filter", "Owner"],
    ["account-filter", "Account"],
    ["category-filter", "Category"],
    ["necessity-filter", "Need"],
    ["beneficiary-filter", "Beneficiary"],
  ];
  dims.forEach(([id, label]) => {
    const v = filterValue(id);
    if (v) parts.push(`${label}: ${v}`);
  });
  const qs = summaryTextSearch.trim();
  if (qs) parts.push(`Search: ${qs}`);
  if (document.getElementById("internal-toggle")?.checked) {
    parts.push("Internal transfers included in spend charts");
  }
  const exParts = [];
  if (excludeState.categories.length) exParts.push(`${excludeState.categories.length} categories`);
  if (excludeState.necessities.length) exParts.push(`${excludeState.necessities.length} need levels`);
  if (excludeState.beneficiaries.length) exParts.push(`${excludeState.beneficiaries.length} beneficiaries`);
  if (exParts.length) parts.push(`Hidden from totals: ${exParts.join(", ")}`);
  return parts.join(" · ");
}

function updateScopeContextBanner() {
  const el = document.getElementById("dash-scope-live");
  if (!el) return;
  el.textContent = buildScopeDescription();
  el.hidden = false;
}

function currentQueryParams() {
  const params = new URLSearchParams();
  const mapping = [
    ["owner-filter", "owner"],
    ["account-filter", "account"],
    ["category-filter", "category"],
    ["necessity-filter", "necessity"],
    ["beneficiary-filter", "beneficiary"],
  ];

  mapping.forEach(([id, key]) => {
    const value = filterValue(id);
    if (value) params.set(key, value);
  });

  const y = filterValue("year-filter");
  const cm = filterValue("calendar-month-filter");
  if (y && cm) {
    const mm = cm.length === 1 ? `0${cm}` : cm;
    params.set("month", `${y}-${mm}`);
  } else if (y) {
    params.set("year", y);
  }

  const intToggle = document.getElementById("internal-toggle");
  if (intToggle && intToggle.checked) {
    params.set("include_internal", "true");
  }

  excludeState.categories.forEach((c) => params.append("exclude_category", c));
  excludeState.necessities.forEach((n) => params.append("exclude_necessity", n));
  excludeState.beneficiaries.forEach((b) => params.append("exclude_beneficiary", b));

  const q = summaryTextSearch.trim();
  if (q) params.set("q", q);

  return params;
}

/** @returns {boolean} true if URL forced a new scope and caller should refetch summary once */
function populateFilters(summary) {
  const configs = [
    ["owner-filter", "owners", "All owners"],
    ["account-filter", "accounts", "All accounts"],
    ["category-filter", "categories", "All categories"],
    ["necessity-filter", "necessities", "All need levels"],
    ["beneficiary-filter", "beneficiaries", "All beneficiaries"],
  ];

  configs.forEach(([id, key, label]) => {
    const select = document.getElementById(id);
    if (!select) return;
    const currentValue = select.value;
    const items = summary.filters[key] || [];
    select.innerHTML = [`<option value="">${label}</option>`, ...items.map((item) => optionMarkup(item, currentValue))].join("");
  });
  populatePeriodFilters(summary);
  renderExcludeLists(summary);
  return applyPeriodSelectionFromUrlOnce();
}

function renderExcludeLists(summary) {
  const fd = summary.filter_dimensions || { categories: [], necessities: [], beneficiaries: [] };
  const cats = fd.categories.length ? fd.categories : summary.filters.categories;
  const nec = fd.necessities.length ? fd.necessities : summary.filters.necessities;
  const ben = fd.beneficiaries.length ? fd.beneficiaries : summary.filters.beneficiaries;

  const renderCheckList = (containerId, items, key) => {
    const el = document.getElementById(containerId);
    if (!el) return;
    const selected = new Set(excludeState[key] || []);
    el.innerHTML = items
      .filter(Boolean)
      .map(
        (item) => `
      <label class="exclude-row">
        <input type="checkbox" data-exclude-group="${key}" value="${escapeHtml(item)}" ${selected.has(item) ? "checked" : ""} />
        <span class="exclude-row-label">${escapeHtml(item)}</span>
      </label>
    `,
      )
      .join("");
    if (!items.length) {
      el.innerHTML = `<div class="empty-state exclude-empty">Nothing to list yet.</div>`;
    }
  };

  renderCheckList("exclude-categories-list", cats, "categories");
  renderCheckList("exclude-necessities-list", nec, "necessities");
  renderCheckList("exclude-beneficiaries-list", ben, "beneficiaries");
}

function refreshExcludeStateFromDom() {
  excludeState = {
    categories: [...document.querySelectorAll('#exclude-categories-list input[type="checkbox"]:checked')].map((i) => i.value),
    necessities: [...document.querySelectorAll('#exclude-necessities-list input[type="checkbox"]:checked')].map((i) => i.value),
    beneficiaries: [...document.querySelectorAll('#exclude-beneficiaries-list input[type="checkbox"]:checked')].map((i) => i.value),
  };
  saveExcludeState(excludeState);
}

function renderInternalReview(summary) {
  const ir = summary.internal_review || {};
  const cap = document.getElementById("internal-review-caption");
  if (cap) cap.textContent = ir.caption || "";

  const st = ir.stats || {};
  const statsEl = document.getElementById("internal-review-stats");
  if (statsEl) {
    const cells = [
      ["Paired rows", st.rows_paired],
      ["Keyword internal", st.rows_keyword],
      ["Unmatched candidates", st.rows_unmatched_candidate],
      ["Transfer pair groups", st.pair_count],
      ["Heuristic pairs (amount + timing)", st.pairs_amount_timing],
    ];
    statsEl.innerHTML = `<div class="ir-stat-grid">${cells
      .map(
        ([k, v]) =>
          `<div class="ir-stat"><span class="ir-stat-k">${escapeHtml(k)}</span><span class="ir-stat-v">${Number(v ?? 0).toLocaleString()}</span></div>`,
      )
      .join("")}</div>`;
  }

  renderTable(
    "internal-review-matched",
    ir.matched_transfers || [],
    ["Amount", "Leg A", "Leg B", "When", "Rule / tag"],
    (row) => `
      <tr>
        <td><span class="pill">${formatCurrency(row.amount)}</span></td>
        <td><strong>${escapeHtml(row.account_left)}</strong><br /><span class="ir-desc">${escapeHtml(row.description_left)}</span></td>
        <td><strong>${escapeHtml(row.account_right)}</strong><br /><span class="ir-desc">${escapeHtml(row.description_right)}</span></td>
        <td class="ir-dates">${escapeHtml(row.date_left || "—")}<br />${escapeHtml(row.date_right || "—")}</td>
        <td class="ir-tags"><span class="ir-tag">${escapeHtml(row.tag_left || "—")}</span><br /><span class="ir-tag">${escapeHtml(row.tag_right || "—")}</span></td>
      </tr>
    `,
    "data-table-wrap internal-review-tbl",
  );

  renderTable(
    "internal-review-unmatched",
    ir.unmatched_internal || [],
    ["Date", "Owner", "Account", "Description", "Cash flow"],
    (row) => `
      <tr>
        <td>${escapeHtml(row.transaction_date || "—")}</td>
        <td>${escapeHtml(row.owner)}</td>
        <td>${escapeHtml(row.account_label)}</td>
        <td>${escapeHtml(row.description)}</td>
        <td>${formatCurrency(row.value)}</td>
      </tr>
    `,
    "data-table-wrap internal-review-tbl",
  );

  renderTable(
    "internal-review-other",
    ir.other_uncategorized || [],
    ["Date", "Account", "Merchant", "Description", "Amount"],
    (row) => `
      <tr>
        <td>${escapeHtml(row.transaction_date || "—")}</td>
        <td>${escapeHtml(row.account_label)}</td>
        <td>${escapeHtml(row.merchant)}</td>
        <td>${escapeHtml(row.description)}</td>
        <td>${formatCurrency(row.amount)}</td>
      </tr>
    `,
    "data-table-wrap internal-review-tbl",
  );
}

function sparklineSvg(values, w = 128, h = 40) {
  const nums = values.map((v) => Number(v || 0));
  if (!nums.length) return "";
  const max = Math.max(...nums, 1);
  const min = Math.min(...nums, 0);
  const range = max - min || 1;
  const step = nums.length > 1 ? w / (nums.length - 1) : w;
  const pts = nums
    .map((v, i) => {
      const x = i * step;
      const y = h - ((v - min) / range) * (h - 4) - 2;
      return `${x},${y}`;
    })
    .join(" ");
  return `<svg class="kpi-spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" aria-hidden="true"><polyline fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" points="${pts}" /></svg>`;
}

function renderOverview(summary) {
  const overview = summary.overview || {};
  const tc = overview.transaction_count ?? 0;
  const matched = overview.matched_internal_pairs ?? 0;
  const unmatched = overview.unmatched_internal_rows ?? 0;
  const topCat = overview.top_category;
  const topCatAmt = overview.top_category_amount;
  const monthlyVals = summary.monthly_expenses.map((r) => Number(r.value || 0));
  const spark = sparklineSvg(monthlyVals.slice(-10));

  const eq = overview.extraction_quality || {};
  const modeRaw = String(eq.mode || "when_empty").toLowerCase();
  const modeText =
    modeRaw === "always"
      ? "Multi-model on every PDF — results merged for accuracy"
      : modeRaw === "never"
        ? "Optional OCR only when you enable backends"
        : "Multi-model when embedded PDF text returns no rows";
  const backends = Array.isArray(eq.backends) ? eq.backends.filter(Boolean) : [];
  const backendsStr = backends.length ? backends.join(" · ") : "Native text only (install MinerU + ocr extra for VLM pipeline)";
  const conf =
    eq.avg_confidence != null && !Number.isNaN(Number(eq.avg_confidence)) ? Math.round(Number(eq.avg_confidence) * 100) : null;
  const extractionStrip = `
    <article class="metric metric--extraction" aria-label="Statement extraction consensus">
      <div class="extraction-strip">
        <div class="extraction-strip-head">
          <span class="extraction-strip-title">Statement extraction &amp; model consensus</span>
          <span class="extraction-strip-mode">${escapeHtml(modeText)}</span>
        </div>
        <div class="extraction-strip-stats">
          <div class="extraction-strip-stat"><span class="es-k">OCR backends</span><span class="es-v es-v--wrap">${escapeHtml(backendsStr)}</span></div>
          <div class="extraction-strip-stat"><span class="es-k">Avg agreement</span><span class="es-v">${conf != null ? `${conf}%` : "—"}</span></div>
          <div class="extraction-strip-stat"><span class="es-k">Files merged</span><span class="es-v">${Number(eq.files_with_consensus_merge ?? 0)}</span></div>
          <div class="extraction-strip-stat"><span class="es-k">Low-confidence rows</span><span class="es-v es-v--warn">${Number(eq.low_confidence_transactions ?? 0)}</span></div>
          <div class="extraction-strip-stat"><span class="es-k">Description disagreements</span><span class="es-v">${Number(eq.description_disagreements ?? 0)}</span></div>
          <div class="extraction-strip-stat"><span class="es-k">Statements in report</span><span class="es-v">${Number(eq.files_analyzed ?? 0)}</span></div>
        </div>
        <p class="extraction-strip-foot">Parallel parses are aligned on date, amount, and a normalized description fingerprint; the merged row keeps the majority description (longest tie-break). On Apple Silicon, <span class="es-code">vlm-mlx-engine</span> runs a local MLX VLM (no cloud). Tune with <span class="es-code">PF_OCR_ENSEMBLE</span> and <span class="es-code">PF_OCR_BACKENDS</span>; see README “VLMs on Mac”.</p>
      </div>
    </article>
  `;

  const avgTx = overview.avg_expense_transaction ?? 0;
  const heroSubtitle =
    topCat && topCatAmt != null
      ? `Largest share: <strong>${escapeHtml(topCat)}</strong> (${formatCurrency(topCatAmt)}) · avg expense ${formatCurrency(avgTx)}`
      : `${tc} visible transactions · avg ${formatCurrency(avgTx)}`;

  const hero = `
    <article class="metric metric--hero">
      <div class="metric-hero-top">
        <div class="metric-hero-copy">
          <div class="metric-label">Total spend (filtered)</div>
          <div class="metric-value metric-value--xl">${formatCurrency(overview.expense_total)}</div>
          <div class="metric-subtitle metric-subtitle--hero">${heroSubtitle}</div>
        </div>
        <div class="metric-hero-spark" title="Recent months trend">${spark || ""}</div>
      </div>
    </article>
  `;

  const rest = [
    ["Cash in", formatCurrency(overview.cash_in_total), `${overview.account_count ?? 0} accounts`],
    ["Internal / transfers", formatCurrency(overview.internal_total), `${matched} matched · ${unmatched} unmatched`],
    ["Transactions", String(tc), `${overview.statement_count ?? 0} statements`],
    ["People & scope", `${overview.owner_count ?? 0} owners`, `${overview.date_start || "—"} → ${overview.date_end || "—"}`],
  ]
    .map(
      ([label, value, subtitle]) => `
        <article class="metric metric--compact">
          <div class="metric-label">${label}</div>
          <div class="metric-value">${value}</div>
          <div class="metric-subtitle">${subtitle}</div>
        </article>
      `,
    )
    .join("");

  document.getElementById("overview-cards").innerHTML = extractionStrip + hero + rest;
}

function baseLayout(extra = {}) {
  return {
    paper_bgcolor: "rgba(255,255,255,0)",
    plot_bgcolor: PALETTE.plotBg,
    margin: { l: 48, r: 20, t: 14, b: 40 },
    font: { family: "Inter, system-ui, sans-serif", color: PALETTE.ink, size: 11 },
    xaxis: { showgrid: false, zeroline: false, tickfont: { size: 10, color: PALETTE.muted } },
    yaxis: { gridcolor: PALETTE.grid, zeroline: false, tickfont: { size: 10, color: PALETTE.muted } },
    ...extra,
  };
}

function emptyChart(targetId) {
  Plotly.react(
    targetId,
    [],
    baseLayout({
      annotations: [
        {
          text: "No data in this view",
          xref: "paper",
          yref: "paper",
          x: 0.5,
          y: 0.5,
          showarrow: false,
          font: { family: "Inter, system-ui, sans-serif", size: 13, color: PALETTE.muted },
        },
      ],
    }),
    plotlyConfig,
  );
}

const PLOTLY_DRILLDOWN_CHART_IDS = [
  "chart-category-donut",
  "category-chart",
  "necessity-chart",
  "beneficiary-chart",
  "stacked-chart",
  "merchant-chart",
  "owner-chart",
  "account-chart",
  "chart-treemap",
];

function purgePlotlyDrilldownHandlers() {
  if (typeof Plotly === "undefined") return;
  PLOTLY_DRILLDOWN_CHART_IDS.forEach((id) => {
    const gd = document.getElementById(id);
    if (gd && typeof gd.removeAllListeners === "function") {
      gd.removeAllListeners("plotly_click");
    }
  });
}

function applySidebarSelectValue(selectId, value) {
  const v = String(value ?? "").trim();
  if (!v) return false;
  const sel = document.getElementById(selectId);
  if (!sel) return false;
  const has = [...sel.options].some((o) => o.value === v);
  if (!has) return false;
  summaryTextSearch = "";
  sel.value = v;
  return true;
}

function drilldownFromSidebarSelect(selectId, value, noun) {
  const v = String(value ?? "").trim();
  if (!v) return;
  if (!applySidebarSelectValue(selectId, v)) {
    showActivityHint(`No matching ${noun} filter for “${v}”.`, "", 4200);
    return;
  }
  pendingDrilldownHint = `${noun} “${v}” — recent transactions below.`;
  pendingDrilldownScroll = true;
  scheduleDashboardRefresh();
}

function drilldownFromMerchantChart(merchantLabel) {
  const q = String(merchantLabel ?? "").trim();
  if (!q || q === "—") return;
  summaryTextSearch = q;
  pendingDrilldownHint = `Text filter (merchant) “${q}” — recent transactions below. Change any sidebar filter to clear it.`;
  pendingDrilldownScroll = true;
  scheduleDashboardRefresh();
}

function wirePlotlyChartDrilldown(hasSpendData) {
  if (typeof Plotly === "undefined") return;
  purgePlotlyDrilldownHandlers();
  if (!hasSpendData) return;

  document.getElementById("chart-category-donut")?.on?.("plotly_click", (ev) => {
    const lab = ev?.points?.[0]?.label;
    if (lab != null) drilldownFromSidebarSelect("category-filter", lab, "Category");
  });

  document.getElementById("category-chart")?.on?.("plotly_click", (ev) => {
    const lab = ev?.points?.[0]?.y;
    if (lab != null) drilldownFromSidebarSelect("category-filter", lab, "Category");
  });

  document.getElementById("necessity-chart")?.on?.("plotly_click", (ev) => {
    const lab = ev?.points?.[0]?.y;
    if (lab != null) drilldownFromSidebarSelect("necessity-filter", lab, "Need level");
  });

  document.getElementById("beneficiary-chart")?.on?.("plotly_click", (ev) => {
    const lab = ev?.points?.[0]?.y;
    if (lab != null) drilldownFromSidebarSelect("beneficiary-filter", lab, "Beneficiary");
  });

  document.getElementById("stacked-chart")?.on?.("plotly_click", (ev) => {
    const pt = ev?.points?.[0];
    const name = pt?.fullData?.name ?? pt?.data?.name;
    if (name != null) drilldownFromSidebarSelect("category-filter", name, "Category");
  });

  document.getElementById("merchant-chart")?.on?.("plotly_click", (ev) => {
    const lab = ev?.points?.[0]?.y;
    if (lab != null) drilldownFromMerchantChart(lab);
  });

  document.getElementById("owner-chart")?.on?.("plotly_click", (ev) => {
    const lab = ev?.points?.[0]?.x;
    if (lab != null) drilldownFromSidebarSelect("owner-filter", lab, "Owner");
  });

  document.getElementById("account-chart")?.on?.("plotly_click", (ev) => {
    const lab = ev?.points?.[0]?.y;
    if (lab != null) drilldownFromSidebarSelect("account-filter", lab, "Account");
  });

  document.getElementById("chart-treemap")?.on?.("plotly_click", (ev) => {
    const pt = ev?.points?.[0];
    const lab = pt?.label;
    const parent = pt?.parent;
    if (lab == null) return;
    if (!parent) {
      drilldownFromSidebarSelect("category-filter", lab, "Category");
    } else {
      drilldownFromMerchantChart(lab);
    }
  });
}

function sortBreakdownDesc(rows) {
  return [...rows].sort((a, b) => Number(b.value || 0) - Number(a.value || 0));
}

function yMarginForLabels(labels) {
  const longest = labels.reduce((m, l) => Math.max(m, String(l).length), 8);
  return Math.min(240, 28 + longest * 7);
}

function buildTreemapHierarchy(rows) {
  if (!rows.length) return null;
  const ids = [];
  const labels = [];
  const parents = [];
  const values = [];
  const catSums = {};
  rows.forEach((r) => {
    const c = String(r.category || "Other");
    catSums[c] = (catSums[c] || 0) + Number(r.value || 0);
  });
  Object.entries(catSums).forEach(([cat, v]) => {
    const id = `cat:${cat}`;
    ids.push(id);
    labels.push(cat);
    parents.push("");
    values.push(v);
  });
  rows.forEach((r, i) => {
    const cat = String(r.category || "Other");
    ids.push(`m:${i}`);
    labels.push(String(r.merchant || "—"));
    parents.push(`cat:${cat}`);
    values.push(Number(r.value || 0));
  });
  return { ids, labels, parents, values };
}

function buildSunburstHierarchy(rows) {
  if (!rows.length) return null;
  const ids = [];
  const labels = [];
  const parents = [];
  const values = [];
  const ROOT = "sb:root";
  const total = rows.reduce((s, r) => s + Number(r.value || 0), 0);
  ids.push(ROOT);
  labels.push("All");
  parents.push("");
  values.push(total);

  const benSums = {};
  rows.forEach((r) => {
    const b = String(r.beneficiary || "—");
    benSums[b] = (benSums[b] || 0) + Number(r.value || 0);
  });
  Object.entries(benSums).forEach(([ben, v]) => {
    const id = `sb:b:${ben}`;
    ids.push(id);
    labels.push(ben);
    parents.push(ROOT);
    values.push(v);
  });

  const catSums = {};
  rows.forEach((r) => {
    const b = String(r.beneficiary || "—");
    const c = String(r.category || "—");
    const k = `${b}|||${c}`;
    catSums[k] = (catSums[k] || 0) + Number(r.value || 0);
  });
  Object.entries(catSums).forEach(([k, v]) => {
    const [ben, cat] = k.split("|||");
    const id = `sb:bc:${ben}::${cat}`;
    ids.push(id);
    labels.push(cat);
    parents.push(`sb:b:${ben}`);
    values.push(v);
  });

  rows.forEach((r, i) => {
    const b = String(r.beneficiary || "—");
    const c = String(r.category || "—");
    const pid = `sb:bc:${b}::${c}`;
    ids.push(`sb:l:${i}`);
    labels.push(String(r.merchant || "—"));
    parents.push(pid);
    values.push(Number(r.value || 0));
  });

  return { ids, labels, parents, values };
}

function renderCharts(summary) {
  const chartIds = [
    "monthly-chart",
    "chart-category-donut",
    "necessity-chart",
    "beneficiary-chart",
    "necessity-monthly-chart",
    "beneficiary-monthly-chart",
    "owner-beneficiary-chart",
    "category-chart",
    "flow-chart",
    "stacked-chart",
    "waterfall-chart",
    "owner-chart",
    "account-chart",
    "merchant-chart",
    "daily-chart",
    "chart-weekday",
    "chart-treemap",
    "chart-sunburst",
    "chart-sankey",
  ];

  if (!summary.monthly_expenses.length) {
    chartIds.forEach(emptyChart);
    scheduleDashChartResize();
    wirePlotlyChartDrilldown(false);
    return;
  }

  Plotly.react(
    "monthly-chart",
    [
      {
        x: summary.monthly_expenses.map((row) => row.label),
        y: summary.monthly_expenses.map((row) => row.value),
        type: "scatter",
        mode: "lines+markers",
        fill: "tozeroy",
        fillcolor: "rgba(38, 166, 91, 0.14)",
        line: { color: PALETTE.primary, width: 3, shape: "spline" },
        marker: { size: 9, color: PALETTE.secondary, line: { width: 1.5, color: "#fff" } },
        hovertemplate: "%{x}<br>%{y:$,.2f}<extra></extra>",
      },
    ],
    baseLayout(),
    plotlyConfig,
  );

  const donutCats = sortBreakdownDesc(summary.category_breakdown).slice(0, 12);
  if (!donutCats.length) {
    emptyChart("chart-category-donut");
  } else {
    Plotly.react(
      "chart-category-donut",
      [
        {
          type: "pie",
          labels: donutCats.map((row) => row.label),
          values: donutCats.map((row) => row.value),
          hole: 0.55,
          sort: false,
          direction: "clockwise",
          marker: {
            colors: donutCats.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]),
            line: { width: 1, color: "#fff" },
          },
          textinfo: "label+percent",
          textposition: "auto",
          insidetextfont: { size: 9 },
          hovertemplate: "<b>%{label}</b><br>%{value:$,.2f}<br>%{percent}<extra></extra>",
        },
      ],
      baseLayout({ showlegend: false, margin: { l: 12, r: 12, t: 8, b: 8 } }),
      plotlyConfig,
    );
  }

  const necSorted = sortBreakdownDesc(summary.necessity_breakdown);
  Plotly.react(
    "necessity-chart",
    [
      {
        type: "bar",
        orientation: "h",
        y: necSorted.map((row) => row.label),
        x: necSorted.map((row) => row.value),
        marker: { color: necSorted.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]) },
        hovertemplate: "%{y}<br>%{x:$,.2f}<extra></extra>",
      },
    ],
    baseLayout({ margin: { l: yMarginForLabels(necSorted.map((r) => r.label)), r: 20, t: 14, b: 40 } }),
    plotlyConfig,
  );

  const benSorted = sortBreakdownDesc(summary.beneficiary_breakdown);
  Plotly.react(
    "beneficiary-chart",
    [
      {
        type: "bar",
        orientation: "h",
        y: benSorted.map((row) => row.label),
        x: benSorted.map((row) => row.value),
        marker: { color: benSorted.map((_, i) => CHART_COLORS[(i + 2) % CHART_COLORS.length]) },
        hovertemplate: "%{y}<br>%{x:$,.2f}<extra></extra>",
      },
    ],
    baseLayout({ margin: { l: yMarginForLabels(benSorted.map((r) => r.label)), r: 20, t: 14, b: 40 } }),
    plotlyConfig,
  );

  const necessityNames = [...new Set(summary.monthly_necessity_breakdown.map((row) => row.necessity))];
  const necessityMonths = [...new Set(summary.monthly_necessity_breakdown.map((row) => row.month))];
  if (!necessityNames.length || !necessityMonths.length) {
    emptyChart("necessity-monthly-chart");
  } else {
    Plotly.react(
      "necessity-monthly-chart",
      necessityNames.map((name, index) => ({
        x: necessityMonths,
        y: necessityMonths.map(
          (month) => summary.monthly_necessity_breakdown.find((row) => row.month === month && row.necessity === name)?.value || 0,
        ),
        type: "bar",
        name,
        marker: { color: CHART_COLORS[index % CHART_COLORS.length] },
      })),
      baseLayout({ barmode: "stack", legend: { orientation: "h", y: 1.08, font: { size: 10 } } }),
      plotlyConfig,
    );
  }

  const beneficiaryNames = [...new Set(summary.monthly_beneficiary_breakdown.map((row) => row.beneficiary))];
  const beneficiaryMonths = [...new Set(summary.monthly_beneficiary_breakdown.map((row) => row.month))];
  if (!beneficiaryNames.length || !beneficiaryMonths.length) {
    emptyChart("beneficiary-monthly-chart");
  } else {
    Plotly.react(
      "beneficiary-monthly-chart",
      beneficiaryNames.map((name, index) => ({
        x: beneficiaryMonths,
        y: beneficiaryMonths.map(
          (month) => summary.monthly_beneficiary_breakdown.find((row) => row.month === month && row.beneficiary === name)?.value || 0,
        ),
        type: "scatter",
        mode: "lines",
        stackgroup: "one",
        name,
        line: { width: 2.5, shape: "spline", color: CHART_COLORS[(index + 1) % CHART_COLORS.length] },
      })),
      baseLayout({ legend: { orientation: "h", y: 1.08, font: { size: 10 } } }),
      plotlyConfig,
    );
  }

  const owners = [...new Set(summary.owner_beneficiary_breakdown.map((row) => row.owner))];
  const beneficiaries = [...new Set(summary.owner_beneficiary_breakdown.map((row) => row.beneficiary))];
  if (!owners.length || !beneficiaries.length) {
    emptyChart("owner-beneficiary-chart");
  } else {
    const z = owners.map((owner) =>
      beneficiaries.map(
        (beneficiary) => summary.owner_beneficiary_breakdown.find((row) => row.owner === owner && row.beneficiary === beneficiary)?.value || 0,
      ),
    );
    Plotly.react(
      "owner-beneficiary-chart",
      [
        {
          type: "heatmap",
          x: beneficiaries,
          y: owners,
          z,
          colorscale: [
            [0, "#f0f7f3"],
            [0.35, "#c5e6d0"],
            [0.65, "#6bc48a"],
            [1, "#1d8f4d"],
          ],
          hovertemplate: "%{y} → %{x}<br>%{z:$,.2f}<extra></extra>",
        },
      ],
      baseLayout(),
      plotlyConfig,
    );
  }

  const catSorted = sortBreakdownDesc(summary.category_breakdown).slice(0, 14);
  Plotly.react(
    "category-chart",
    [
      {
        type: "bar",
        orientation: "h",
        y: catSorted.map((row) => row.label),
        x: catSorted.map((row) => row.value),
        marker: { color: catSorted.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]) },
        hovertemplate: "%{y}<br>%{x:$,.2f}<extra></extra>",
      },
    ],
    baseLayout({ margin: { l: yMarginForLabels(catSorted.map((r) => r.label)), r: 20, t: 14, b: 40 } }),
    plotlyConfig,
  );

  const flowLabels = summary.flow_breakdown.map((row) => row.label);
  Plotly.react(
    "flow-chart",
    [
      {
        x: flowLabels,
        y: summary.flow_breakdown.map((row) => Math.abs(row.value)),
        type: "bar",
        marker: { color: flowLabels.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]) },
        hovertemplate: "%{x}<br>%{y:$,.2f}<extra></extra>",
      },
    ],
    baseLayout(),
    plotlyConfig,
  );

  const categories = [...new Set(summary.monthly_category_breakdown.map((row) => row.category))];
  const months = [...new Set(summary.monthly_category_breakdown.map((row) => row.month))];
  const topCats = categories.slice(0, 8);
  if (!topCats.length || !months.length) {
    emptyChart("stacked-chart");
  } else {
    Plotly.react(
      "stacked-chart",
      topCats.map((category, index) => ({
        x: months,
        y: months.map(
          (month) => summary.monthly_category_breakdown.find((row) => row.month === month && row.category === category)?.value || 0,
        ),
        type: "scatter",
        mode: "lines",
        stackgroup: "one",
        name: category,
        line: { width: 2.2, color: CHART_COLORS[index % CHART_COLORS.length] },
      })),
      baseLayout({ legend: { orientation: "h", y: 1.08, font: { size: 10 } } }),
      plotlyConfig,
    );
  }

  if (!summary.waterfall_breakdown.length) {
    emptyChart("waterfall-chart");
  } else {
    Plotly.react(
      "waterfall-chart",
      [
        {
          type: "waterfall",
          x: summary.waterfall_breakdown.map((row) => row.label),
          y: summary.waterfall_breakdown.map((row) => row.value),
          measure: summary.waterfall_breakdown.map(() => "relative"),
          connector: { line: { color: PALETTE.grid } },
          increasing: { marker: { color: PALETTE.primary } },
          decreasing: { marker: { color: PALETTE.coral } },
        },
      ],
      baseLayout(),
      plotlyConfig,
    );
  }

  Plotly.react(
    "owner-chart",
    [
      {
        x: summary.owner_breakdown.map((row) => row.label),
        y: summary.owner_breakdown.map((row) => row.value),
        type: "bar",
        marker: { color: PALETTE.blue },
        hovertemplate: "%{x}<br>%{y:$,.2f}<extra></extra>",
      },
    ],
    baseLayout(),
    plotlyConfig,
  );

  const acctSorted = sortBreakdownDesc(summary.account_breakdown);
  Plotly.react(
    "account-chart",
    [
      {
        type: "bar",
        orientation: "h",
        y: acctSorted.map((row) => row.label),
        x: acctSorted.map((row) => row.value),
        marker: { color: acctSorted.map((_, i) => CHART_COLORS[(i + 3) % CHART_COLORS.length]) },
        hovertemplate: "%{y}<br>%{x:$,.2f}<extra></extra>",
      },
    ],
    baseLayout({ margin: { l: yMarginForLabels(acctSorted.map((r) => r.label)), r: 20, t: 14, b: 40 } }),
    plotlyConfig,
  );

  const merch = [...summary.merchant_breakdown].reverse();
  Plotly.react(
    "merchant-chart",
    [
      {
        x: merch.map((row) => row.value),
        y: merch.map((row) => row.label),
        type: "bar",
        orientation: "h",
        marker: { color: PALETTE.primary },
        hovertemplate: "%{y}<br>%{x:$,.2f}<extra></extra>",
      },
    ],
    baseLayout({ margin: { l: yMarginForLabels(merch.map((r) => r.label)), r: 20, t: 14, b: 40 } }),
    plotlyConfig,
  );

  Plotly.react(
    "daily-chart",
    [
      {
        x: summary.daily_expenses.map((row) => row.label || row.date),
        y: summary.daily_expenses.map((row) => row.value),
        type: "scatter",
        mode: "lines",
        fill: "tozeroy",
        fillcolor: PALETTE.primarySoft,
        line: { color: PALETTE.primary, width: 2.8, shape: "spline" },
        hovertemplate: "%{x}<br>%{y:$,.2f}<extra></extra>",
      },
    ],
    baseLayout(),
    plotlyConfig,
  );

  const wdOrder = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
  const wdMap = Object.fromEntries((summary.weekday_breakdown || []).map((r) => [r.label, Number(r.value || 0)]));
  const wdLabels = wdOrder.filter((d) => d in wdMap);
  const wdVals = wdLabels.map((d) => wdMap[d]);
  if (!wdLabels.length) {
    emptyChart("chart-weekday");
  } else {
    Plotly.react(
      "chart-weekday",
      [
        {
          type: "bar",
          x: wdLabels,
          y: wdVals,
          marker: { color: wdLabels.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]) },
          hovertemplate: "%{x}<br>%{y:$,.2f}<extra></extra>",
        },
      ],
      baseLayout(),
      plotlyConfig,
    );
  }

  const tm = buildTreemapHierarchy(summary.treemap_breakdown);
  if (!tm) {
    emptyChart("chart-treemap");
  } else {
    Plotly.react(
      "chart-treemap",
      [
        {
          type: "treemap",
          ids: tm.ids,
          labels: tm.labels,
          parents: tm.parents,
          values: tm.values,
          branchvalues: "total",
          textfont: { family: "Inter, system-ui, sans-serif", size: 11 },
          pathbar: { thickness: 18, textfont: { size: 10 } },
          hovertemplate: "<b>%{label}</b><br>%{value:$,.2f}<extra></extra>",
        },
      ],
      baseLayout({ margin: { l: 0, r: 0, t: 0, b: 0 } }),
      plotlyConfig,
    );
  }

  const sb = buildSunburstHierarchy(summary.sunburst_breakdown);
  if (!sb) {
    emptyChart("chart-sunburst");
  } else {
    Plotly.react(
      "chart-sunburst",
      [
        {
          type: "sunburst",
          ids: sb.ids,
          labels: sb.labels,
          parents: sb.parents,
          values: sb.values,
          branchvalues: "total",
          insidetextorientation: "auto",
          hovertemplate: "<b>%{label}</b><br>%{value:$,.2f}<extra></extra>",
        },
      ],
      baseLayout({ margin: { l: 4, r: 4, t: 4, b: 4 } }),
      plotlyConfig,
    );
  }

  const sk = summary.sankey;
  if (!sk.links.length || !sk.nodes.length) {
    emptyChart("chart-sankey");
  } else {
    Plotly.react(
      "chart-sankey",
      [
        {
          type: "sankey",
          arrangement: "snap",
          node: {
            pad: 12,
            thickness: 14,
            line: { color: PALETTE.grid, width: 0.4 },
            label: sk.nodes,
            color: sk.nodes.map((_, i) => `rgba(61, 126, 166, ${0.2 + (i % 6) * 0.1})`),
          },
          link: {
            source: sk.links.map((l) => l.source),
            target: sk.links.map((l) => l.target),
            value: sk.links.map((l) => l.value),
            color: sk.links.map(() => "rgba(38, 166, 91, 0.18)"),
          },
        },
      ],
      baseLayout({ margin: { l: 4, r: 4, t: 8, b: 8 }, font: { size: 10 } }),
      plotlyConfig,
    );
  }

  scheduleDashChartResize();
  wirePlotlyChartDrilldown(true);
}

function taxonomySelectOptions(values, selected) {
  return values
    .map((v) => `<option value="${escapeHtml(v)}"${v === selected ? " selected" : ""}>${escapeHtml(v)}</option>`)
    .join("");
}

function categoryReviewBadgeLabel(source) {
  const s = String(source || "auto");
  const labels = {
    auto: "Auto — no keyword",
    rule: "Keyword rule",
    override: "Your correction",
    internal_pair: "Internal (paired)",
    internal_keyword: "Internal (text)",
  };
  return labels[s] || s;
}

function suggestRuleKeyword(description) {
  const t = String(description || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
  if (!t) return "";
  return t.length > 48 ? t.slice(0, 48) : t;
}

function renderCategoryReviewPanel(summary) {
  const list = document.getElementById("category-review-list");
  const toolbar = document.getElementById("category-review-toolbar");
  if (!list) return;
  const rows = summary.category_review || [];
  const tax = summary.taxonomy || { categories: [], necessities: [], beneficiaries: [] };
  const cats = tax.categories.length ? tax.categories : [];
  const necs = tax.necessities.length ? tax.necessities : [];
  const bens = tax.beneficiaries.length ? tax.beneficiaries : [];

  if (toolbar) {
    toolbar.removeAttribute("aria-hidden");
    toolbar.innerHTML =
      rows.length === 0
        ? `<p class="cat-review-toolbar-text">No rows in the review queue for this scope.</p>`
        : `<p class="cat-review-toolbar-text"><strong>${rows.length}</strong> transaction${rows.length === 1 ? "" : "s"} in the queue for the current filters.</p>`;
  }

  if (!rows.length) {
    list.innerHTML = `
      <div class="cat-review-empty">
        <p class="cat-review-empty-title">Queue is clear</p>
        <p class="cat-review-empty-sub">Try another month or owner, or clear sidebar exclusions. Rows land here when they are <strong>Other</strong>, carry an <strong>unknown merchant</strong>, or are <strong>expenses with no keyword rule</strong>.</p>
      </div>`;
    return;
  }

  const badgeModifiers = new Set(["auto", "rule", "override", "internal_pair", "internal_keyword"]);

  list.innerHTML = rows
    .map((row) => {
      const tx = escapeHtml(row.tx_key || "");
      const src = String(row.category_source || "auto");
      const badgeMod = badgeModifiers.has(src) ? src : "misc";
      const sug = suggestRuleKeyword(row.description);
      return `
    <div class="cat-review-row" data-tx-key="${tx}">
      <div class="cat-review-row-top">
        <span class="cat-review-badge cat-review-badge--${badgeMod}" title="Source">${escapeHtml(categoryReviewBadgeLabel(src))}</span>
        <span class="cat-review-date">${escapeHtml(row.transaction_date || "—")}</span>
        <span class="cat-review-amt">${formatCurrency(row.expense_amount || 0)}</span>
      </div>
      <div class="cat-review-desc">
        <span class="cat-review-merchant">${escapeHtml(row.merchant || "—")}</span>
        <span class="cat-review-desc-text">${escapeHtml(row.description || "")}</span>
      </div>
      <div class="cat-review-meta">${escapeHtml(row.account_label || "")} · ${escapeHtml(row.owner || "")}</div>
      <div class="cat-review-controls">
        <label class="cat-review-field">
          <span>Category</span>
          <select class="cat-review-select" data-field="category">${taxonomySelectOptions(cats, row.category)}</select>
        </label>
        <label class="cat-review-field">
          <span>Need level</span>
          <select class="cat-review-select" data-field="necessity">${taxonomySelectOptions(necs, row.necessity)}</select>
        </label>
        <label class="cat-review-field">
          <span>Beneficiary</span>
          <select class="cat-review-select" data-field="beneficiary">${taxonomySelectOptions(bens, row.beneficiary)}</select>
        </label>
      </div>
      <div class="cat-review-rule">
        <label class="cat-review-rule-label">
          <input type="checkbox" data-field="add_rule" />
          <span>Save keyword rule for similar lines</span>
        </label>
        <input type="text" class="cat-review-keyword-input" data-field="add_rule_keyword" value="${escapeHtml(sug)}" placeholder="Keyword from description…" autocomplete="off" />
      </div>
    </div>`;
    })
    .join("");
}

async function saveCategoryReview() {
  const dirty = document.querySelectorAll(".cat-review-row--dirty");
  if (!dirty.length) {
    showActivityHint("Change a category or keyword field, then save.");
    return;
  }
  hideErrorBanner();
  const items = [];
  dirty.forEach((row) => {
    const tx_key = row.getAttribute("data-tx-key");
    if (!tx_key) return;
    const cat = row.querySelector('[data-field="category"]')?.value;
    const nec = row.querySelector('[data-field="necessity"]')?.value;
    const ben = row.querySelector('[data-field="beneficiary"]')?.value;
    const addRule = row.querySelector('[data-field="add_rule"]')?.checked;
    const kw = row.querySelector('[data-field="add_rule_keyword"]')?.value?.trim() || "";
    items.push({
      tx_key,
      category: cat,
      necessity: nec,
      beneficiary: ben,
      add_rule: !!(addRule && kw.length >= 2),
      add_rule_keyword: kw,
    });
  });
  const response = await fetch("/api/transaction-corrections", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ items }),
  });
  if (!response.ok) throw new Error("Could not save categorization.");
  const payload = await response.json();
  const normalized = normalizeSummary(payload.summary);
  if (!normalized) {
    showErrorBanner("Server returned an invalid summary.");
    return;
  }
  state.summary = normalized;
  populateFilters(normalized);
  renderOverview(normalized);
  renderCharts(normalized);
  renderTables(normalized);
  renderRulesTable();
  showActivityHint(
    payload.rules_inserted
      ? `Saved — ${payload.rules_inserted} new keyword rule(s) and corrections applied.`
      : "Corrections saved — dataset rebuilt.",
    "activity-hint--ok",
  );
}

function bindCategoryReview() {
  const list = document.getElementById("category-review-list");
  if (list) {
    list.addEventListener("change", (e) => {
      const row = e.target.closest(".cat-review-row");
      if (row) row.classList.add("cat-review-row--dirty");
    });
    list.addEventListener("input", (e) => {
      if (e.target && e.target.matches && e.target.matches('[data-field="add_rule_keyword"]')) {
        e.target.closest(".cat-review-row")?.classList.add("cat-review-row--dirty");
      }
    });
  }
  document.getElementById("category-review-save")?.addEventListener("click", () => {
    saveCategoryReview().catch((error) => showErrorBanner(error.message));
  });
}

function renderTable(containerId, rows, headers, rowMapper, className = "") {
  const container = document.getElementById(containerId);
  if (!rows.length) {
    container.innerHTML = `<div class="empty-state">No rows to show.</div>`;
    return;
  }

  const headerRow = headers.map((header) => `<th>${header}</th>`).join("");
  const bodyRows = rows.map(rowMapper).join("");
  container.innerHTML = `<div class="${className}"><table><thead><tr>${headerRow}</tr></thead><tbody>${bodyRows}</tbody></table></div>`;
}

function renderTables(summary) {
  renderTable(
    "matched-transfers",
    summary.matched_transfers,
    ["Amount", "Left Side", "Right Side", "Dates", "Tag"],
    (row) => `
      <tr>
        <td><span class="pill">${formatCurrency(row.amount)}</span></td>
        <td><strong>${escapeHtml(row.account_left)}</strong><br />${escapeHtml(row.description_left)}</td>
        <td><strong>${escapeHtml(row.account_right)}</strong><br />${escapeHtml(row.description_right)}</td>
        <td>${escapeHtml(row.date_left || "—")}<br />${escapeHtml(row.date_right || "—")}</td>
        <td class="ir-tags"><span class="ir-tag">${escapeHtml(row.tag_left || "—")}</span><br /><span class="ir-tag">${escapeHtml(row.tag_right || "—")}</span></td>
      </tr>
    `,
  );

  renderTable(
    "unmatched-internal",
    summary.unmatched_internal,
    ["Date", "Owner", "Account", "Description", "Cash Flow"],
    (row) => `
      <tr>
        <td>${escapeHtml(row.transaction_date || "—")}</td>
        <td>${escapeHtml(row.owner)}</td>
        <td>${escapeHtml(row.account_label)}</td>
        <td>${escapeHtml(row.description)}</td>
        <td>${formatCurrency(row.value)}</td>
      </tr>
    `,
  );

  renderTable(
    "recent-transactions",
    summary.recent_transactions,
    ["Date", "Owner", "Beneficiary", "Merchant", "Category", "Need", "Expense", "Status"],
    (row) => `
      <tr>
        <td>${escapeHtml(row.transaction_date || "—")}</td>
        <td>${escapeHtml(row.owner)}</td>
        <td>${escapeHtml(row.beneficiary)}</td>
        <td><strong>${escapeHtml(row.merchant)}</strong><br />${escapeHtml(row.description)}</td>
        <td>${escapeHtml(row.category)}</td>
        <td>${escapeHtml(row.necessity)}</td>
        <td>${formatCurrency(row.expense_amount)}</td>
        <td>${escapeHtml(row.internal_match_status)}</td>
      </tr>
    `,
    "data-table-wrap",
  );

  renderInternalReview(summary);
  renderCategoryReviewPanel(summary);
}

function rulesFilterSelectOptions(options, selectedVal, emptyLabel) {
  const emptySel = !selectedVal ? "selected" : "";
  return (
    `<option value="" ${emptySel}>${escapeHtml(emptyLabel)}</option>` +
    options.map((o) => optionMarkup(o, selectedVal)).join("")
  );
}

function rulesBulkSelect(id, options, label) {
  return `
    <label class="rules-bulk-label">
      <span>${label}</span>
      <select id="${id}" class="rules-bulk-select">
        <option value="">${escapeHtml("No change")}</option>
        ${options.map((o) => `<option value="${escapeHtml(o)}">${escapeHtml(o)}</option>`).join("")}
      </select>
    </label>
  `;
}

function persistRulesToolbarFromDom() {
  if (!document.getElementById("rules-search")) return;
  state.rulesToolbar.search = document.getElementById("rules-search").value || "";
  state.rulesToolbar.filterCategory = filterValue("rules-filter-category");
  state.rulesToolbar.filterNecessity = filterValue("rules-filter-necessity");
  state.rulesToolbar.filterBeneficiary = filterValue("rules-filter-beneficiary");
}

function applyRulesFilters() {
  const searchEl = document.getElementById("rules-search");
  const search = (searchEl?.value || "").trim().toLowerCase();
  const fc = filterValue("rules-filter-category");
  const fn = filterValue("rules-filter-necessity");
  const fb = filterValue("rules-filter-beneficiary");
  const tbody = document.querySelector("#rules-table tbody");
  if (!tbody) return;
  const rows = tbody.querySelectorAll("tr");
  let visible = 0;
  rows.forEach((row) => {
    const kw = row.querySelector('[data-field="keyword"]')?.value?.toLowerCase() || "";
    const cat = row.querySelector('[data-field="category"]')?.value || "";
    const nec = row.querySelector('[data-field="necessity"]')?.value || "";
    const ben = row.querySelector('[data-field="beneficiary"]')?.value || "";
    const textMatch =
      !search ||
      kw.includes(search) ||
      cat.toLowerCase().includes(search) ||
      nec.toLowerCase().includes(search) ||
      ben.toLowerCase().includes(search);
    const catOk = !fc || cat === fc;
    const necOk = !fn || nec === fn;
    const benOk = !fb || ben === fb;
    const show = textMatch && catOk && necOk && benOk;
    row.classList.toggle("rules-row--hidden", !show);
    if (show) visible += 1;
  });
  const countEl = document.getElementById("rules-visible-count");
  if (countEl) countEl.textContent = `${visible} / ${rows.length}`;
  persistRulesToolbarFromDom();
}

function bindRulesToolbar() {
  const search = document.getElementById("rules-search");
  if (search) {
    search.addEventListener("input", () => applyRulesFilters());
  }
  ["rules-filter-category", "rules-filter-necessity", "rules-filter-beneficiary"].forEach((id) => {
    document.getElementById(id)?.addEventListener("change", () => applyRulesFilters());
  });

  document.getElementById("rules-bulk-apply")?.addEventListener("click", () => {
    const bc = filterValue("rules-bulk-category");
    const bn = filterValue("rules-bulk-necessity");
    const bb = filterValue("rules-bulk-beneficiary");
    if (!bc && !bn && !bb) return;
    document.querySelectorAll("#rules-table tbody tr:not(.rules-row--hidden)").forEach((row) => {
      if (bc) row.querySelector('[data-field="category"]').value = bc;
      if (bn) row.querySelector('[data-field="necessity"]').value = bn;
      if (bb) row.querySelector('[data-field="beneficiary"]').value = bb;
    });
    ["rules-bulk-category", "rules-bulk-necessity", "rules-bulk-beneficiary"].forEach((bulkId) => {
      const el = document.getElementById(bulkId);
      if (el) el.value = "";
    });
    applyRulesFilters();
  });
}

function renderRulesTable() {
  const container = document.getElementById("rules-table");
  const config = state.ruleConfig;
  if (!config) {
    container.innerHTML = `<div class="empty-state">Loading rules…</div>`;
    return;
  }

  persistRulesToolbarFromDom();
  const rt = state.rulesToolbar;

  const rows = config.rules
    .map(
      (rule, index) => `
        <tr data-index="${index}">
          <td class="rules-col-match"><input type="text" data-field="keyword" value="${escapeHtml(rule.keyword)}" placeholder="keyword…" autocomplete="off" /></td>
          <td class="rules-col-cat">
            <select data-field="category">
              ${config.category_options.map((option) => optionMarkup(option, rule.category)).join("")}
            </select>
          </td>
          <td class="rules-col-need">
            <select data-field="necessity">
              ${config.necessity_options.map((option) => optionMarkup(option, rule.necessity)).join("")}
            </select>
          </td>
          <td class="rules-col-who">
            <select data-field="beneficiary">
              ${config.beneficiary_options.map((option) => optionMarkup(option, rule.beneficiary)).join("")}
            </select>
          </td>
          <td class="rules-col-del"><button type="button" class="rule-delete" data-delete-index="${index}" title="Remove row">×</button></td>
        </tr>
      `,
    )
    .join("");

  const filterCatOpts = rulesFilterSelectOptions(
    config.category_options,
    rt.filterCategory,
    "All categories",
  );
  const filterNecOpts = rulesFilterSelectOptions(
    config.necessity_options,
    rt.filterNecessity,
    "All need levels",
  );
  const filterBenOpts = rulesFilterSelectOptions(
    config.beneficiary_options,
    rt.filterBeneficiary,
    "All beneficiaries",
  );

  container.innerHTML = `
    <div class="rules-panel">
      <div class="rules-toolbar" role="search">
        <input
          type="search"
          id="rules-search"
          class="rules-search-input"
          placeholder="Filter by keyword, category, need, or beneficiary…"
          autocomplete="off"
          value="${escapeHtml(rt.search)}"
        />
        <div class="rules-toolbar-row">
          <label class="rules-filter-label">
            <span class="rules-filter-key">Category</span>
            <select id="rules-filter-category" class="rules-filter-select">${filterCatOpts}</select>
          </label>
          <label class="rules-filter-label">
            <span class="rules-filter-key">Need</span>
            <select id="rules-filter-necessity" class="rules-filter-select">${filterNecOpts}</select>
          </label>
          <label class="rules-filter-label">
            <span class="rules-filter-key">Who</span>
            <select id="rules-filter-beneficiary" class="rules-filter-select">${filterBenOpts}</select>
          </label>
          <span class="rules-visible-count" id="rules-visible-count" aria-live="polite">—</span>
        </div>
        <div class="rules-bulk-row">
          <span class="rules-bulk-title">Bulk on filtered rows</span>
          ${rulesBulkSelect("rules-bulk-category", config.category_options, "Category")}
          ${rulesBulkSelect("rules-bulk-necessity", config.necessity_options, "Need")}
          ${rulesBulkSelect("rules-bulk-beneficiary", config.beneficiary_options, "Beneficiary")}
          <button type="button" id="rules-bulk-apply" class="mint-btn mint-btn-ghost mint-btn--sm rules-bulk-apply">Apply</button>
        </div>
      </div>
      <div class="rules-table rules-table--dense">
        <table>
          <thead>
            <tr>
              <th class="rules-col-match">Match</th>
              <th class="rules-col-cat">Category</th>
              <th class="rules-col-need">Need</th>
              <th class="rules-col-who">Who</th>
              <th class="rules-col-del" aria-label="Remove"></th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </div>
  `;

  container.querySelectorAll("[data-delete-index]").forEach((button) => {
    button.addEventListener("click", () => {
      const index = Number(button.getAttribute("data-delete-index"));
      state.ruleConfig.rules.splice(index, 1);
      renderRulesTable();
    });
  });

  bindRulesToolbar();
  applyRulesFilters();
}

function collectRulesFromDom() {
  return [...document.querySelectorAll("#rules-table tbody tr")].map((row) => ({
    keyword: row.querySelector('[data-field="keyword"]').value.trim(),
    category: row.querySelector('[data-field="category"]').value,
    necessity: row.querySelector('[data-field="necessity"]').value,
    beneficiary: row.querySelector('[data-field="beneficiary"]').value,
  }));
}

async function loadRules() {
  const response = await fetch("/api/category-rules");
  if (!response.ok) {
    throw new Error("Unable to load category rules.");
  }
  state.ruleConfig = await response.json();
  renderRulesTable();
}

async function saveRules() {
  hideErrorBanner();
  const response = await fetch("/api/category-rules", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ rules: collectRulesFromDom() }),
  });

  if (!response.ok) {
    throw new Error("Unable to save category rules.");
  }

  const payload = await response.json();
  state.ruleConfig = {
    rules: payload.rules,
    category_options: payload.category_options,
    necessity_options: payload.necessity_options,
    beneficiary_options: payload.beneficiary_options,
  };
  const normalized = normalizeSummary(payload.summary);
  if (!normalized) {
    showErrorBanner("Server returned an invalid summary.");
    return;
  }
  state.summary = normalized;
  populateFilters(normalized);
  renderOverview(normalized);
  renderCharts(normalized);
  renderTables(normalized);
  updateScopeContextBanner();
  renderRulesTable();
  showActivityHint("Rules saved — dataset rebuilt.", "activity-hint--ok");
}

async function fetchSummary() {
  const response = await fetch(`/api/summary?${currentQueryParams().toString()}`);
  if (!response.ok) {
    throw new Error("Unable to load dashboard summary.");
  }
  return response.json();
}

async function loadDashboard() {
  hideErrorBanner();
  hideActivityHint();
  let summary = await fetchSummary();
  let normalized = normalizeSummary(summary);
  state.summary = normalized;
  let urlScoped = populateFilters(normalized);
  if (urlScoped) {
    summary = await fetchSummary();
    normalized = normalizeSummary(summary);
    state.summary = normalized;
    populateFilters(normalized);
  }
  renderOverview(normalized);
  renderCharts(normalized);
  renderTables(normalized);
  updateScopeContextBanner();
  if (pendingDrilldownHint) {
    showActivityHint(pendingDrilldownHint, "activity-hint--ok", 5200);
    pendingDrilldownHint = null;
  }
  if (pendingDrilldownScroll) {
    pendingDrilldownScroll = false;
    requestAnimationFrame(() => {
      document.getElementById("recent-transactions")?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }
}

function progressDetailFromJob(job) {
  if (!job) return "";
  const parts = [];
  if (job.current_file) parts.push(job.current_file);
  if (job.ocr_backend != null && job.ocr_backend_index != null && job.ocr_backends_total != null) {
    parts.push(`Model ${job.ocr_backend_index}/${job.ocr_backends_total}: ${job.ocr_backend}`);
  }
  return parts.join(" · ");
}

/** Stop polling; jobs live in server memory and vanish on restart — avoid infinite 404s. */
function stopJobPolling(message) {
  if (state.pollHandle) {
    clearInterval(state.pollHandle);
    state.pollHandle = null;
  }
  state.currentJobId = null;
  state.processJobContext = null;
  resetUploadProgress();
  if (message) showErrorBanner(message);
}

async function pollJob(jobId) {
  const response = await fetch(`/api/jobs/${jobId}`);
  if (response.status === 404) {
    stopJobPolling(
      "That upload/extraction job is no longer on the server (usually after the app restarted). Run upload or extraction again.",
    );
    return;
  }
  if (!response.ok) {
    throw new Error("Unable to read job status.");
  }

  const job = await response.json();
  const pct = `${Math.round((job.progress || 0) * 100)}%`;
  const headline =
    job.kind === "upload"
      ? job.stage === "saving"
        ? "Uploading…"
        : "Upload complete"
      : job.message && String(job.message).trim()
        ? String(job.message)
        : "Extracting statements…";
  setProgressVisual(true, job.progress || 0, headline, pct, progressDetailFromJob(job));

  if (job.status === "complete") {
    if (state.pollHandle) {
      clearInterval(state.pollHandle);
      state.pollHandle = null;
    }
    state.currentJobId = null;

    if (job.kind === "upload" && job.result && job.result.upload_only) {
      const savedFiles = Array.isArray(job.result.saved_files) ? job.result.saved_files : [];
      state.pendingSavedFileCount = savedFiles.length;
      resetUploadProgress();
      try {
        await loadDashboard();
        await loadRules();
        renderUploadSavedAwaitingExtract(savedFiles);
        hideErrorBanner();
      } catch (err) {
        showErrorBanner(err.message || "Could not refresh after upload.");
      }
      return;
    }

    if (job.kind === "process") {
      const ctx = state.processJobContext;
      state.processJobContext = null;
      resetUploadProgress();
      try {
        await loadDashboard();
        await loadRules();
        if (ctx === "reload") {
          showActivityHint("Dataset rebuilt from your statement files.", "activity-hint--ok");
        } else {
          const n = state.pendingSavedFileCount || 1;
          state.pendingSavedFileCount = 0;
          if (state.summary) renderUploadComplete(n, state.summary);
        }
        hideErrorBanner();
      } catch (err) {
        showErrorBanner(err.message || "Could not refresh dashboard after extraction.");
      }
      return;
    }

    resetUploadProgress();
    return;
  }

  if (job.status === "error") {
    stopJobPolling(null);
    showErrorBanner(job.error || "Processing failed.");
    return;
  }
}

function startPolling(jobId) {
  if (state.pollHandle) clearInterval(state.pollHandle);
  state.currentJobId = jobId;
  const onPollError = (error) => {
    stopJobPolling(error.message || "Job polling failed.");
  };
  state.pollHandle = setInterval(() => {
    pollJob(jobId).catch(onPollError);
  }, 600);
  pollJob(jobId).catch(onPollError);
}

function restoreExtractionPresetFromStorage() {
  try {
    const v = localStorage.getItem(EXTRACTION_PRESET_STORAGE_KEY);
    if (v !== "fast" && v !== "slow") return;
    const input = document.querySelector(`input[name="extraction-preset"][value="${v}"]`);
    if (input) input.checked = true;
  } catch (_) {
    /* ignore */
  }
}

function bindExtractionPresetPersistence() {
  document.getElementById("extraction-preset-block")?.addEventListener("change", (ev) => {
    const t = ev.target;
    if (!(t instanceof HTMLInputElement)) return;
    if (t.name !== "extraction-preset" || !t.value) return;
    try {
      localStorage.setItem(EXTRACTION_PRESET_STORAGE_KEY, t.value);
    } catch (_) {
      /* ignore */
    }
  });
}

function selectedExtractionPreset() {
  const el = document.querySelector('input[name="extraction-preset"]:checked');
  if (el && el.value) return el.value;
  try {
    const v = localStorage.getItem(EXTRACTION_PRESET_STORAGE_KEY);
    if (v === "fast" || v === "slow") return v;
  } catch (_) {
    /* ignore */
  }
  return "slow";
}

async function loadExtractionOptions() {
  const hint = document.getElementById("extraction-preset-hint");
  try {
    const response = await fetch("/api/extraction-options");
    if (!response.ok || !hint) return;
    const data = await response.json();
    const f = data.presets && data.presets.fast;
    const s = data.presets && data.presets.slow;
    if (!f || !s) return;
    const fList = Array.isArray(f.backends) ? f.backends.join(" + ") : "pipeline";
    const sList = Array.isArray(s.backends) ? s.backends.join(" + ") : "";
    hint.textContent = `Fast → only ${fList} (${f.ensemble_mode}), not your multi-model env list. Slow → ${sList || "(env)"} (${s.ensemble_mode}) from PF_OCR_BACKENDS.`;
  } catch {
    if (hint) hint.textContent = "";
  }
}

async function startProcessJob(context = "extract", endpoint = "/api/process-statements") {
  hideErrorBanner();
  hideActivityHint();
  state.processJobContext = context;
  setProgressVisual(true, 0.04, "Starting extraction…", "4%", "");
  const preset = selectedExtractionPreset();
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ preset }),
  });
  if (!response.ok) {
    state.processJobContext = null;
    resetUploadProgress();
    let detail = context === "reload" ? "Reload failed." : "Could not start extraction.";
    try {
      const errBody = await response.json();
      if (errBody.detail) detail = String(errBody.detail);
    } catch {
      /* ignore */
    }
    throw new Error(detail);
  }
  const payload = await response.json();
  if (!payload.job_id) {
    state.processJobContext = null;
    resetUploadProgress();
    throw new Error("Invalid server response.");
  }
  startPolling(payload.job_id);
}

async function uploadFiles(files) {
  if (!files.length) return;
  hideErrorBanner();
  hideActivityHint();
  renderUploadQueued(files.length);
  setProgressVisual(true, 0.06, "Uploading…", "");
  const formData = new FormData();
  [...files].forEach((file) => formData.append("files", file));

  const response = await fetch("/api/upload", {
    method: "POST",
    body: formData,
  });

  if (!response.ok) {
    resetUploadProgress();
    throw new Error("Upload failed.");
  }

  const payload = await response.json();
  startPolling(payload.job_id);
}

function bindUploadFeedbackActions() {
  document.getElementById("upload-feedback")?.addEventListener("click", (event) => {
    const btn = event.target && event.target.closest && event.target.closest("[data-action='process-statements']");
    if (!btn) return;
    startProcessJob("extract").catch((err) => showErrorBanner(err.message || "Extraction failed to start"));
  });
}

async function clearAppCache() {
  if (
    !confirm(
      "Clear cached uploads, MinerU OCR output, and processed transactions? Files in input_statements and your rules in data/settings are kept.",
    )
  ) {
    return;
  }
  const btn = document.getElementById("clear-cache-btn");
  if (btn) btn.disabled = true;
  try {
    const response = await fetch("/api/clear-cache", {
      method: "POST",
      headers: { Accept: "application/json" },
    });
    if (!response.ok) {
      let detail = "Could not clear cache.";
      try {
        const body = await response.json();
        if (body.detail) detail = String(body.detail);
      } catch {
        /* ignore */
      }
      throw new Error(detail);
    }
    const payload = await response.json();
    const n = Number(payload.cleared_items ?? 0);
    document.getElementById("upload-feedback")?.setAttribute("hidden", "");
    hideErrorBanner();
    await loadDashboard();
    showActivityHint(
      n > 0 ? `Cache cleared (${n} top-level item${n === 1 ? "" : "s"} removed).` : "Cache was already empty.",
      "activity-hint--ok",
    );
  } catch (err) {
    showErrorBanner(err.message || "Could not clear cache.");
  } finally {
    if (btn) btn.disabled = false;
  }
}

function bindUploader() {
  const dropzone = document.getElementById("upload-form");
  const fileInput = document.getElementById("file-input");
  const browseButton = document.getElementById("browse-button");

  document.getElementById("clear-cache-btn")?.addEventListener("click", () => {
    clearAppCache().catch((e) => showErrorBanner(e.message || "Clear cache failed."));
  });

  browseButton.addEventListener("click", () => fileInput.click());
  fileInput.addEventListener("change", async (event) => {
    try {
      await uploadFiles(event.target.files);
    } catch (error) {
      resetUploadProgress();
      showErrorBanner(error.message);
    } finally {
      fileInput.value = "";
    }
  });

  ["dragenter", "dragover"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (event) => {
      event.preventDefault();
      dropzone.classList.add("dragover");
    });
  });

  ["dragleave", "drop"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (event) => {
      event.preventDefault();
      dropzone.classList.remove("dragover");
    });
  });

  dropzone.addEventListener("drop", async (event) => {
    try {
      await uploadFiles(event.dataTransfer.files);
    } catch (error) {
      resetUploadProgress();
      showErrorBanner(error.message);
    }
  });
}

function bindFilters() {
  [
    "year-filter",
    "calendar-month-filter",
    "owner-filter",
    "account-filter",
    "category-filter",
    "necessity-filter",
    "beneficiary-filter",
    "internal-toggle",
  ].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("change", () => {
      summaryTextSearch = "";
      if (id === "year-filter") {
        updateCalendarMonthDisabledState();
        updatePeriodNudgeState();
      }
      scheduleDashboardRefresh();
    });
  });

  document.getElementById("period-prev-month")?.addEventListener("click", () => shiftCalendarMonth(-1));
  document.getElementById("period-next-month")?.addEventListener("click", () => shiftCalendarMonth(1));

  const sidebarBody = document.getElementById("dash-sidebar-body");
  if (sidebarBody) {
    sidebarBody.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement)) return;
      if (!target.matches('input[data-exclude-group][type="checkbox"]')) return;
      refreshExcludeStateFromDom();
      scheduleDashboardRefresh();
    });
  }

  updatePeriodNudgeState();
}

function bindDashboardExtras() {
  document.getElementById("reload-dashboard-btn")?.addEventListener("click", async () => {
    hideErrorBanner();
    try {
      await startProcessJob("reload", "/api/reload");
    } catch (err) {
      showErrorBanner(err.message || "Reload failed");
    }
  });

  document.getElementById("export-recent-csv")?.addEventListener("click", () => {
    const rows = state.summary?.recent_transactions;
    if (!rows?.length) {
      showActivityHint("Nothing to export in the current view.");
      return;
    }
    const cols = [
      "transaction_date",
      "owner",
      "account_label",
      "merchant",
      "description",
      "category",
      "necessity",
      "beneficiary",
      "flow_type",
      "amount",
      "expense_amount",
      "internal_match_status",
      "tx_key",
      "category_source",
    ].filter((c) => rows[0] && Object.prototype.hasOwnProperty.call(rows[0], c));
    const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
    const lines = [cols.join(",")].concat(rows.map((row) => cols.map((c) => esc(row[c])).join(",")));
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `recent-activity-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
    showActivityHint("CSV downloaded.", "activity-hint--ok", 3200);
  });
}

function bindRuleActions() {
  document.getElementById("add-rule-button").addEventListener("click", () => {
    if (!state.ruleConfig) return;
    state.ruleConfig.rules.push({
      keyword: "",
      category: "Other",
      necessity: "Auto",
      beneficiary: "Auto",
    });
    renderRulesTable();
  });

  document.getElementById("save-rules-button").addEventListener("click", () => {
    saveRules().catch((error) => showErrorBanner(error.message));
  });
}

/** Matches `index.html` chart-grid article sequence (for layout reset). */
const CHART_GRID_DEFAULT_ORDER = [
  "chart-monthly",
  "chart-category-donut",
  "chart-necessity",
  "chart-beneficiary",
  "chart-treemap",
  "chart-sankey",
  "chart-sunburst",
  "chart-weekday",
  "chart-necessity-monthly",
  "chart-beneficiary-monthly",
  "chart-owner-beneficiary",
  "chart-category",
  "chart-flow",
  "chart-stacked",
  "chart-waterfall",
  "chart-owner",
  "chart-account",
  "chart-merchant",
  "chart-daily",
];

function resetChartGridDomOrder() {
  const grid = document.getElementById("chart-grid");
  if (!grid) return;
  const articles = [...grid.querySelectorAll(":scope > article[data-dash-panel]")];
  if (!articles.length) return;
  const byId = new Map(articles.map((el) => [el.getAttribute("data-dash-panel"), el]));
  const frag = document.createDocumentFragment();
  CHART_GRID_DEFAULT_ORDER.forEach((id) => {
    const el = byId.get(id);
    if (el) frag.appendChild(el);
  });
  articles.forEach((el) => {
    const id = el.getAttribute("data-dash-panel");
    if (id && !CHART_GRID_DEFAULT_ORDER.includes(id)) frag.appendChild(el);
  });
  grid.appendChild(frag);
}

function restoreChartOrder() {
  const grid = document.getElementById("chart-grid");
  if (!grid) return;
  let order = [];
  try {
    order = JSON.parse(localStorage.getItem(DASH_CHART_ORDER_KEY) || "[]");
  } catch (_) {
    /* ignore */
  }
  if (!Array.isArray(order) || order.length < 2) return;
  const children = [...grid.querySelectorAll(":scope > article[data-dash-panel]")];
  if (!children.length) return;
  const byId = new Map(children.map((el) => [el.getAttribute("data-dash-panel"), el]));
  const seen = new Set();
  const frag = document.createDocumentFragment();
  order.forEach((id) => {
    const el = byId.get(id);
    if (el) {
      frag.appendChild(el);
      seen.add(id);
    }
  });
  children.forEach((el) => {
    const id = el.getAttribute("data-dash-panel");
    if (id && !seen.has(id)) frag.appendChild(el);
  });
  grid.appendChild(frag);
}

function persistChartOrder() {
  const grid = document.getElementById("chart-grid");
  if (!grid) return;
  const ids = [...grid.querySelectorAll(":scope > article[data-dash-panel]")].map((el) => el.getAttribute("data-dash-panel"));
  try {
    localStorage.setItem(DASH_CHART_ORDER_KEY, JSON.stringify(ids));
  } catch (_) {
    /* ignore */
  }
}

let dragChartArticle = null;

function installChartPanelChrome() {
  const grid = document.getElementById("chart-grid");
  if (!grid) return;
  grid.querySelectorAll(":scope > article[data-dash-panel]").forEach((article) => {
    if (article.querySelector(".panel-chrome")) return;
    const head = article.querySelector(".mint-card-head");
    if (!head) return;
    head.classList.add("mint-card-head--with-tools");
    if (!head.querySelector(".mint-card-head-text")) {
      const wrap = document.createElement("div");
      wrap.className = "mint-card-head-text";
      while (head.firstChild) {
        wrap.appendChild(head.firstChild);
      }
      head.appendChild(wrap);
    }
    const tools = document.createElement("div");
    tools.className = "panel-chrome";
    // Use <span draggable> — <button draggable> often fails to start a drag in Chrome / Safari.
    tools.innerHTML =
      '<span class="panel-chrome-drag" draggable="true" role="button" tabindex="0" aria-label="Drag to reorder" title="Drag to reorder"></span>' +
      '<button type="button" class="panel-chrome-hide" aria-label="Hide chart" title="Hide chart">×</button>';
    head.appendChild(tools);
  });
}

function bindChartGridChrome() {
  const grid = document.getElementById("chart-grid");
  if (!grid || grid.dataset.chromeBound === "1") return;
  grid.dataset.chromeBound = "1";

  grid.addEventListener("dragstart", (e) => {
    const grip = e.target.closest(".panel-chrome-drag");
    if (!grip) return;
    const article = grip.closest("article[data-dash-panel]");
    if (!article || !grid.contains(article)) return;
    dragChartArticle = article;
    article.classList.add("panel-dragging");
    e.dataTransfer.effectAllowed = "move";
    e.dataTransfer.setData("text/plain", article.getAttribute("data-dash-panel") || "");
  });

  grid.addEventListener("dragend", () => {
    if (dragChartArticle) dragChartArticle.classList.remove("panel-dragging");
    dragChartArticle = null;
  });

  grid.addEventListener("dragover", (e) => {
    e.preventDefault();
    if (e.dataTransfer) e.dataTransfer.dropEffect = "move";
  });

  grid.addEventListener("drop", (e) => {
    e.preventDefault();
    if (!dragChartArticle) return;

    const visibleArticles = () =>
      [...grid.querySelectorAll(":scope > article[data-dash-panel]")].filter(
        (a) => !a.classList.contains("dash-panel-hidden"),
      );

    let tgt = e.target.closest("article[data-dash-panel]");
    if (!tgt || !grid.contains(tgt)) {
      const stack = document.elementsFromPoint(e.clientX, e.clientY);
      const found = stack.find(
        (el) => el instanceof HTMLElement && el.matches("article[data-dash-panel]") && grid.contains(el),
      );
      tgt = found || null;
    }

    if (!tgt || tgt.classList.contains("dash-panel-hidden")) {
      const vis = visibleArticles().filter((a) => a !== dragChartArticle);
      const last = vis[vis.length - 1];
      if (last) grid.insertBefore(dragChartArticle, last.nextSibling);
      else grid.appendChild(dragChartArticle);
      persistChartOrder();
      scheduleDashChartResize();
      return;
    }

    if (tgt === dragChartArticle) return;

    const rect = tgt.getBoundingClientRect();
    const before = e.clientY < rect.top + rect.height / 2;
    if (before) grid.insertBefore(dragChartArticle, tgt);
    else grid.insertBefore(dragChartArticle, tgt.nextSibling);
    persistChartOrder();
    scheduleDashChartResize();
  });

  grid.addEventListener("click", (e) => {
    const btn = e.target.closest(".panel-chrome-hide");
    if (!btn) return;
    const article = btn.closest("[data-dash-panel]");
    const id = article?.getAttribute("data-dash-panel");
    if (!id) return;
    dashVisibility[id] = false;
    persistDashVisibility();
    syncDashToggleCheckboxes();
    applyDashPanelVisibility();
  });
}

const DASH_STORAGE_KEY = "pf-dash-layout-v1";

const DASH_PANEL_DEFS = [
  { id: "panel-overview", label: "Overview metrics" },
  { id: "panel-category-review", label: "Fix categories (review queue)" },
  { id: "chart-monthly", label: "Spending over time" },
  { id: "chart-category-donut", label: "Category mix (donut)" },
  { id: "chart-necessity", label: "Spending by need" },
  { id: "chart-beneficiary", label: "Spending by beneficiary" },
  { id: "chart-treemap", label: "Category × merchant treemap" },
  { id: "chart-sankey", label: "Owner → beneficiary → category flow" },
  { id: "chart-sunburst", label: "Beneficiary × category × merchant" },
  { id: "chart-weekday", label: "Spend by weekday" },
  { id: "chart-necessity-monthly", label: "Need level by month" },
  { id: "chart-beneficiary-monthly", label: "Beneficiary over time" },
  { id: "chart-owner-beneficiary", label: "Owner vs beneficiary" },
  { id: "chart-category", label: "Categories (bars)" },
  { id: "chart-flow", label: "Cash flow type" },
  { id: "chart-stacked", label: "Category trends" },
  { id: "chart-waterfall", label: "Top categories (waterfall)" },
  { id: "chart-owner", label: "By owner" },
  { id: "chart-account", label: "By account" },
  { id: "chart-merchant", label: "Top merchants" },
  { id: "chart-daily", label: "Daily spending" },
  { id: "panel-rules", label: "Keyword rules" },
  { id: "table-matched", label: "Matched transfers" },
  { id: "table-unmatched", label: "Unmatched transfers" },
  { id: "table-recent", label: "Recent activity" },
];

const DASH_PRESETS = {
  full: null,
  minimal: new Set(["panel-overview", "panel-category-review", "chart-monthly", "chart-category-donut", "chart-category", "table-recent"]),
  household: new Set([
    "panel-overview",
    "panel-category-review",
    "chart-necessity",
    "chart-beneficiary",
    "chart-necessity-monthly",
    "chart-beneficiary-monthly",
    "chart-owner-beneficiary",
    "table-recent",
  ]),
  reconcile: new Set(["panel-overview", "panel-category-review", "table-matched", "table-unmatched", "table-recent"]),
  trends: new Set([
    "panel-overview",
    "panel-category-review",
    "chart-monthly",
    "chart-category-donut",
    "chart-stacked",
    "chart-daily",
    "chart-merchant",
    "chart-category",
    "chart-waterfall",
    "chart-treemap",
    "chart-sunburst",
    "chart-sankey",
    "chart-weekday",
  ]),
};

let dashVisibility = {};

function dashDefaultVisibility() {
  return Object.fromEntries(DASH_PANEL_DEFS.map((p) => [p.id, true]));
}

function loadDashVisibility() {
  try {
    const raw = localStorage.getItem(DASH_STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      dashVisibility = { ...dashDefaultVisibility(), ...parsed };
      return;
    }
  } catch (_) {
    /* ignore */
  }
  dashVisibility = dashDefaultVisibility();
}

function persistDashVisibility() {
  try {
    localStorage.setItem(DASH_STORAGE_KEY, JSON.stringify(dashVisibility));
  } catch (_) {
    /* ignore */
  }
}

function applyDashPanelVisibility() {
  DASH_PANEL_DEFS.forEach(({ id }) => {
    const el = document.querySelector(`[data-dash-panel="${id}"]`);
    if (!el) return;
    const on = dashVisibility[id] !== false;
    el.classList.toggle("dash-panel-hidden", !on);
    el.setAttribute("aria-hidden", on ? "false" : "true");
  });

  document.querySelectorAll("[data-dash-section]").forEach((section) => {
    const panels = section.querySelectorAll("[data-dash-panel]");
    if (!panels.length) {
      section.classList.remove("dash-section-hidden");
      return;
    }
    const anyOn = [...panels].some((p) => {
      const pid = p.getAttribute("data-dash-panel");
      return dashVisibility[pid] !== false;
    });
    section.classList.toggle("dash-section-hidden", !anyOn);
  });

  scheduleDashChartResize();
}

function scheduleDashChartResize() {
  requestAnimationFrame(() => {
    if (typeof Plotly === "undefined") return;
    document.querySelectorAll("[data-dash-panel]:not(.dash-panel-hidden) .chart").forEach((div) => {
      if (!div.id || div.offsetParent === null) return;
      try {
        Plotly.Plots.resize(div);
      } catch (_) {
        /* plot may not exist yet */
      }
    });
  });
}

function syncDashToggleCheckboxes() {
  const list = document.getElementById("dash-toggle-list");
  if (!list) return;
  list.querySelectorAll("input[data-dash-target]").forEach((input) => {
    const id = input.getAttribute("data-dash-target");
    input.checked = dashVisibility[id] !== false;
  });
}

function buildDashToggleList() {
  const list = document.getElementById("dash-toggle-list");
  if (!list) return;
  list.innerHTML = DASH_PANEL_DEFS.map(
    ({ id, label }) => `
    <label class="dash-toggle-row">
      <input type="checkbox" data-dash-target="${id}" ${dashVisibility[id] !== false ? "checked" : ""} />
      <span>${label}</span>
    </label>
  `,
  ).join("");

  list.querySelectorAll("input[data-dash-target]").forEach((input) => {
    input.addEventListener("change", () => {
      const id = input.getAttribute("data-dash-target");
      dashVisibility[id] = input.checked;
      persistDashVisibility();
      applyDashPanelVisibility();
    });
  });
}

function applyDashPreset(name) {
  const preset = DASH_PRESETS[name];
  const ids = DASH_PANEL_DEFS.map((p) => p.id);
  if (name === "full" || preset == null) {
    ids.forEach((id) => {
      dashVisibility[id] = true;
    });
  } else {
    ids.forEach((id) => {
      dashVisibility[id] = preset.has(id);
    });
  }
  persistDashVisibility();
  syncDashToggleCheckboxes();
  applyDashPanelVisibility();
}

function bindDashLayout() {
  loadDashVisibility();
  restoreChartOrder();
  installChartPanelChrome();
  bindChartGridChrome();
  buildDashToggleList();
  applyDashPanelVisibility();

  document.querySelectorAll("[data-dash-preset]").forEach((btn) => {
    btn.addEventListener("click", () => applyDashPreset(btn.getAttribute("data-dash-preset")));
  });

  const showAll = document.getElementById("dash-show-all");
  if (showAll) showAll.addEventListener("click", () => applyDashPreset("full"));

  const reset = document.getElementById("dash-reset-layout");
  if (reset) {
    reset.addEventListener("click", () => {
      localStorage.removeItem(DASH_STORAGE_KEY);
      localStorage.removeItem(DASH_CHART_ORDER_KEY);
      resetChartGridDomOrder();
      loadDashVisibility();
      syncDashToggleCheckboxes();
      applyDashPanelVisibility();
      scheduleDashChartResize();
    });
  }

  const sideToggle = document.getElementById("dash-sidebar-toggle");
  const layout = document.querySelector(".mint-layout");
  const sidebarBody = document.getElementById("dash-sidebar-body");
  if (sideToggle && layout) {
    sideToggle.addEventListener("click", () => {
      const collapsed = layout.classList.toggle("dash-sidebar-collapsed");
      sideToggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
      sideToggle.textContent = collapsed ? "›" : "‹";
      sideToggle.setAttribute("title", collapsed ? "Expand sidebar" : "Collapse sidebar");
      if (sidebarBody) sidebarBody.setAttribute("aria-hidden", collapsed ? "true" : "false");
      requestAnimationFrame(() => scheduleDashChartResize());
    });
  }
}

window.addEventListener("DOMContentLoaded", async () => {
  bindDashLayout();
  restoreExtractionPresetFromStorage();
  bindExtractionPresetPersistence();
  bindUploader();
  bindUploadFeedbackActions();
  bindFilters();
  bindCategoryReview();
  bindDashboardExtras();
  bindRuleActions();
  resetUploadProgress();
  loadExtractionOptions();
  try {
    await Promise.all([loadDashboard(), loadRules()]);
    scheduleDashChartResize();
  } catch (error) {
    showErrorBanner(error.message);
  }
});
