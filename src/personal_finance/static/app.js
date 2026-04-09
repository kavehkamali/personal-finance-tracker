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
  rulesToolbar: {
    search: "",
    filterCategory: "",
    filterNecessity: "",
    filterBeneficiary: "",
  },
};

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
  };
}

function setStatus(message) {
  document.getElementById("status-banner").textContent = message;
}

function setProgress(progress, label, detail) {
  document.getElementById("progress-fill").style.width = `${Math.max(0, Math.min(100, progress * 100))}%`;
  document.getElementById("progress-label").textContent = label;
  document.getElementById("progress-meta").textContent = detail;
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

  const monthVal = filterValue("month-filter");
  if (monthVal) params.set("month", monthVal);

  if (document.getElementById("internal-toggle").checked) {
    params.set("include_internal", "true");
  }

  return params;
}

function populateFilters(summary) {
  const configs = [
    ["month-filter", "months", "All months"],
    ["owner-filter", "owners", "All owners"],
    ["account-filter", "accounts", "All accounts"],
    ["category-filter", "categories", "All categories"],
    ["necessity-filter", "necessities", "All need levels"],
    ["beneficiary-filter", "beneficiaries", "All beneficiaries"],
  ];

  configs.forEach(([id, key, label]) => {
    const select = document.getElementById(id);
    const currentValue = select.value;
    select.innerHTML = [`<option value="">${label}</option>`, ...summary.filters[key].map((item) => optionMarkup(item, currentValue))].join("");
  });
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

  document.getElementById("overview-cards").innerHTML = hero + rest;
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
    ["Amount", "Left Side", "Right Side", "Dates"],
    (row) => `
      <tr>
        <td><span class="pill">${formatCurrency(row.amount)}</span></td>
        <td><strong>${escapeHtml(row.account_left)}</strong><br />${escapeHtml(row.description_left)}</td>
        <td><strong>${escapeHtml(row.account_right)}</strong><br />${escapeHtml(row.description_right)}</td>
        <td>${escapeHtml(row.date_left || "—")}<br />${escapeHtml(row.date_right || "—")}</td>
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
  setStatus("Saving category rules and rebuilding dataset…");
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
    setStatus("Server returned an invalid summary.");
    return;
  }
  state.summary = normalized;
  populateFilters(normalized);
  renderOverview(normalized);
  renderCharts(normalized);
  renderTables(normalized);
  renderRulesTable();
  setStatus("Category rules saved.");
}

async function fetchSummary() {
  const response = await fetch(`/api/summary?${currentQueryParams().toString()}`);
  if (!response.ok) {
    throw new Error("Unable to load dashboard summary.");
  }
  return response.json();
}

async function loadDashboard() {
  setStatus("Refreshing dashboard…");
  const summary = await fetchSummary();
  const normalized = normalizeSummary(summary);
  state.summary = normalized;
  populateFilters(normalized);
  renderOverview(normalized);
  renderCharts(normalized);
  renderTables(normalized);
  const warnings = normalized.meta?.warnings || [];
  setStatus(warnings.length ? `Warnings: ${warnings.join(" | ")}` : "Data loaded successfully.");
}

async function pollJob(jobId) {
  const response = await fetch(`/api/jobs/${jobId}`);
  if (!response.ok) {
    throw new Error("Unable to read job status.");
  }

  const job = await response.json();
  const detail = job.total_files ? `${Math.round(job.progress * 100)}% · ${job.processed_files}/${job.total_files} files` : `${Math.round(job.progress * 100)}%`;
  setProgress(job.progress || 0, job.message || job.stage || "Processing", detail);
  setStatus(job.current_file ? `${job.message} — ${job.current_file}` : job.message);

  if (job.status === "complete") {
    clearInterval(state.pollHandle);
    state.pollHandle = null;
    state.currentJobId = null;
    const savedFiles = job.result && Array.isArray(job.result.saved_files) ? job.result.saved_files : [];
    setProgress(1, job.message || "Finished", "100%");
    try {
      await loadDashboard();
      await loadRules();
      let msg = "Processing complete.";
      if (savedFiles.length) msg += ` Imported: ${savedFiles.join(", ")}.`;
      setStatus(msg);
    } catch (err) {
      setStatus(err.message || "Could not refresh dashboard after upload.");
    }
    return;
  }

  if (job.status === "error") {
    clearInterval(state.pollHandle);
    state.pollHandle = null;
    state.currentJobId = null;
    setStatus(job.error || "Processing failed.");
    return;
  }
}

function startPolling(jobId) {
  if (state.pollHandle) clearInterval(state.pollHandle);
  state.currentJobId = jobId;
  state.pollHandle = setInterval(() => {
    pollJob(jobId).catch((error) => setStatus(error.message));
  }, 800);
  pollJob(jobId).catch((error) => setStatus(error.message));
}

async function uploadFiles(files) {
  if (!files.length) return;
  setStatus(`Uploading ${files.length} file(s)…`);
  setProgress(0.03, "Saving uploaded files", "0%");
  const formData = new FormData();
  [...files].forEach((file) => formData.append("files", file));

  const response = await fetch("/api/upload", {
    method: "POST",
    body: formData,
  });

  if (!response.ok) {
    throw new Error("Upload failed.");
  }

  const payload = await response.json();
  startPolling(payload.job_id);
}

function bindUploader() {
  const dropzone = document.getElementById("upload-form");
  const fileInput = document.getElementById("file-input");
  const browseButton = document.getElementById("browse-button");

  browseButton.addEventListener("click", () => fileInput.click());
  fileInput.addEventListener("change", async (event) => {
    try {
      await uploadFiles(event.target.files);
    } catch (error) {
      setStatus(error.message);
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
      setStatus(error.message);
    }
  });
}

function bindFilters() {
  ["month-filter", "owner-filter", "account-filter", "category-filter", "necessity-filter", "beneficiary-filter", "internal-toggle"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("change", () => {
      loadDashboard().catch((error) => setStatus(error.message));
    });
  });
}

function bindDashboardExtras() {
  document.getElementById("reload-dashboard-btn")?.addEventListener("click", async () => {
    setStatus("Rebuilding dataset from statements…");
    try {
      const res = await fetch("/api/reload", { method: "POST" });
      if (!res.ok) throw new Error("Reload failed");
      await loadDashboard();
      await loadRules();
      setStatus("Dataset rebuilt. Your filters were re-applied.");
    } catch (err) {
      setStatus(err.message || "Reload failed");
    }
  });

  document.getElementById("export-recent-csv")?.addEventListener("click", () => {
    const rows = state.summary?.recent_transactions;
    if (!rows?.length) {
      setStatus("No transactions to export.");
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
    ];
    const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
    const lines = [cols.join(",")].concat(rows.map((row) => cols.map((c) => esc(row[c])).join(",")));
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `recent-activity-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
    setStatus("Exported recent activity CSV.");
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
    saveRules().catch((error) => setStatus(error.message));
  });
}

const DASH_STORAGE_KEY = "pf-dash-layout-v1";

const DASH_PANEL_DEFS = [
  { id: "panel-overview", label: "Overview metrics" },
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
  minimal: new Set(["panel-overview", "chart-monthly", "chart-category-donut", "chart-category", "table-recent"]),
  household: new Set([
    "panel-overview",
    "chart-necessity",
    "chart-beneficiary",
    "chart-necessity-monthly",
    "chart-beneficiary-monthly",
    "chart-owner-beneficiary",
    "table-recent",
  ]),
  reconcile: new Set(["panel-overview", "table-matched", "table-unmatched", "table-recent"]),
  trends: new Set([
    "panel-overview",
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
      loadDashVisibility();
      syncDashToggleCheckboxes();
      applyDashPanelVisibility();
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
  bindUploader();
  bindFilters();
  bindDashboardExtras();
  bindRuleActions();
  setProgress(0, "Idle", "0%");
  try {
    await Promise.all([loadDashboard(), loadRules()]);
    scheduleDashChartResize();
  } catch (error) {
    setStatus(error.message);
  }
});
