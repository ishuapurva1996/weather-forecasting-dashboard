const PRESET_URL = "https://9106dc82.us2a.app.preset.io/superset/dashboard/8/?native_filters_key=dBFp1-5AII8";

const FILES = {
    kpis: "data/kpis.json",
    daily: "data/daily_weather.json",
    accuracy: "data/forecast_accuracy.json",
    revisions: "data/forecast_revisions.json",
    categories: "data/weather_categories.json",
    rolling: "data/rolling_weather.json",
};

const COLORS = {
    actual: "#277f83",
    forecast: "#c9782b",
    lowerBand: "rgba(201, 120, 43, 0.16)",
    upperBand: "rgba(201, 120, 43, 0.28)",
    sanJose: "#277f83",
    losAngeles: "#b75265",
    blue: "#366ea7",
    green: "#3a7f54",
    amber: "#c9782b",
    rose: "#b75265",
};

const PLOT_CONFIG = {
    responsive: true,
    displayModeBar: false,
};

const CHART_IDS = [
    "chart-forecast",
    "chart-city-comparison",
    "chart-accuracy",
    "chart-revisions",
    "chart-conditions",
    "chart-severity",
    "chart-rolling",
];

let DATA = {};
let selectedCity = "All";

function cssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function plotTheme() {
    return {
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { color: cssVar("--text"), family: "IBM Plex Sans, sans-serif" },
        xaxis: { gridcolor: cssVar("--grid"), zerolinecolor: cssVar("--grid") },
        yaxis: { gridcolor: cssVar("--grid"), zerolinecolor: cssVar("--grid") },
        margin: { l: 54, r: 24, t: 58, b: 54 },
    };
}

function formatTemp(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
    return `${Number(value).toFixed(1)}F`;
}

function formatPct(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
    return `${Number(value).toFixed(0)}%`;
}

function dateLabel(value) {
    if (!value) return "--";
    const date = new Date(`${value}T00:00:00`);
    return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function cityFiltered(rows, city = selectedCity) {
    if (!Array.isArray(rows)) return [];
    if (city === "All") return rows;
    return rows.filter((row) => row.city === city);
}

function latestByDate(rows, dateKey) {
    return [...rows]
        .filter((row) => row[dateKey])
        .sort((a, b) => String(a[dateKey]).localeCompare(String(b[dateKey])))
        .at(-1);
}

function average(values) {
    const clean = values.filter((value) => value !== null && value !== undefined && !Number.isNaN(Number(value)));
    if (!clean.length) return null;
    return clean.reduce((sum, value) => sum + Number(value), 0) / clean.length;
}

function summarizeKpis() {
    const kpiRows = selectedCity === "All" ? DATA.kpis?.by_city || [] : (DATA.kpis?.by_city || []).filter((row) => row.city === selectedCity);
    const latestForecast = average(kpiRows.map((row) => row.latest_forecast_temp_max));
    const latestActual = average(kpiRows.map((row) => row.latest_actual_temp_max));
    const mae = average(kpiRows.map((row) => row.mean_absolute_error));
    const hitRate = average(kpiRows.map((row) => row.interval_hit_rate_pct));
    const detail = selectedCity === "All" ? "Average across cities" : selectedCity;

    document.getElementById("kpi-forecast").textContent = formatTemp(latestForecast);
    document.getElementById("kpi-forecast-detail").textContent = detail;
    document.getElementById("kpi-actual").textContent = formatTemp(latestActual);
    document.getElementById("kpi-actual-detail").textContent = detail;
    document.getElementById("kpi-mae").textContent = formatTemp(mae);
    document.getElementById("kpi-hit-rate").textContent = formatPct(hitRate);
    document.getElementById("refresh-stamp").textContent = `Data refreshed: ${DATA.kpis?.generated_at || "sample data"}`;
}

function baseLayout(height = 390) {
    return {
        ...plotTheme(),
        height,
        hovermode: "x unified",
        legend: {
            orientation: "h",
            y: 1.16,
            x: 0,
            font: { color: cssVar("--text") },
        },
    };
}

function renderForecast() {
    const rows = cityFiltered(DATA.daily);
    const cities = selectedCity === "All" ? ["San Jose", "Los Angeles"] : [selectedCity];
    const traces = [];

    for (const city of cities) {
        const cityRows = rows.filter((row) => row.city === city).sort((a, b) => String(a.weather_date).localeCompare(String(b.weather_date)));
        const history = cityRows.filter((row) => row.record_type === "history");
        const forecast = cityRows.filter((row) => row.record_type === "forecast");
        const color = city === "San Jose" ? COLORS.sanJose : COLORS.losAngeles;

        traces.push({
            type: "scatter",
            mode: "lines",
            name: `${city} actual`,
            x: history.map((row) => row.weather_date),
            y: history.map((row) => row.actual_temp_max),
            line: { color, width: 3 },
            hovertemplate: "%{y:.1f}F<extra></extra>",
        });

        traces.push({
            type: "scatter",
            mode: "lines+markers",
            name: `${city} forecast`,
            x: forecast.map((row) => row.weather_date),
            y: forecast.map((row) => row.forecast_temp_max),
            line: { color: COLORS.forecast, width: 3, dash: city === "San Jose" ? "solid" : "dot" },
            marker: { size: 7 },
            hovertemplate: "%{y:.1f}F<extra></extra>",
        });

        if (selectedCity !== "All" && forecast.length) {
            traces.push({
                type: "scatter",
                mode: "lines",
                name: "Upper bound",
                x: forecast.map((row) => row.weather_date),
                y: forecast.map((row) => row.forecast_upper_bound),
                line: { color: "rgba(201, 120, 43, 0)", width: 0 },
                hoverinfo: "skip",
                showlegend: false,
            });
            traces.push({
                type: "scatter",
                mode: "lines",
                name: "95% interval",
                x: forecast.map((row) => row.weather_date),
                y: forecast.map((row) => row.forecast_lower_bound),
                fill: "tonexty",
                fillcolor: COLORS.upperBand,
                line: { color: "rgba(201, 120, 43, 0)", width: 0 },
                hoverinfo: "skip",
            });
        }
    }

    Plotly.react("chart-forecast", traces, {
        ...baseLayout(500),
        yaxis: { ...plotTheme().yaxis, title: "Temperature (F)" },
    }, PLOT_CONFIG);
}

function renderCityComparison() {
    const cityRows = DATA.kpis?.by_city || [];
    const cities = cityRows.map((row) => row.city);
    const actual = cityRows.map((row) => row.latest_actual_temp_max);
    const forecast = cityRows.map((row) => row.latest_forecast_temp_max);

    Plotly.react("chart-city-comparison", [
        { type: "bar", name: "Latest actual", x: cities, y: actual, marker: { color: COLORS.actual }, text: actual.map(formatTemp), textposition: "outside" },
        { type: "bar", name: "Latest forecast", x: cities, y: forecast, marker: { color: COLORS.forecast }, text: forecast.map(formatTemp), textposition: "outside" },
    ], {
        ...baseLayout(390),
        barmode: "group",
        hovermode: "closest",
        yaxis: { ...plotTheme().yaxis, title: "Temperature (F)" },
    }, PLOT_CONFIG);
}

function renderAccuracy() {
    const rows = cityFiltered(DATA.accuracy);
    const horizons = [...new Set(rows.map((row) => row.days_ahead))].sort((a, b) => a - b);
    const values = horizons.map((horizon) => average(rows.filter((row) => row.days_ahead === horizon).map((row) => row.absolute_error)));

    Plotly.react("chart-accuracy", [{
        type: "bar",
        x: horizons.map((horizon) => `${horizon} day`),
        y: values,
        marker: { color: values, colorscale: [[0, COLORS.green], [0.6, COLORS.amber], [1, COLORS.rose]], showscale: false },
        text: values.map(formatTemp),
        textposition: "outside",
        hovertemplate: "MAE %{y:.2f}F<extra></extra>",
    }], {
        ...baseLayout(390),
        hovermode: "closest",
        yaxis: { ...plotTheme().yaxis, title: "Mean absolute error (F)" },
    }, PLOT_CONFIG);
}

function renderRevisions() {
    const rows = cityFiltered(DATA.revisions).sort((a, b) => String(a.forecast_made_on).localeCompare(String(b.forecast_made_on)));
    const groups = new Map();
    for (const row of rows) {
        const key = `${row.city} -> ${dateLabel(row.forecast_for_date)}`;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(row);
    }

    const traces = [...groups.entries()].slice(0, 8).map(([key, group], index) => ({
        type: "scatter",
        mode: "lines+markers",
        name: key,
        x: group.map((row) => row.forecast_made_on),
        y: group.map((row) => row.predicted_temp_max),
        line: { width: 2, color: [COLORS.sanJose, COLORS.losAngeles, COLORS.amber, COLORS.blue, COLORS.green, COLORS.rose][index % 6] },
        hovertemplate: "%{y:.1f}F<extra></extra>",
    }));

    Plotly.react("chart-revisions", traces, {
        ...baseLayout(390),
        yaxis: { ...plotTheme().yaxis, title: "Predicted max (F)" },
    }, PLOT_CONFIG);
}

function renderConditions() {
    const rows = cityFiltered(DATA.categories);
    const counts = new Map();
    for (const row of rows) {
        const category = row.weather_category || "Uncategorized";
        counts.set(category, (counts.get(category) || 0) + 1);
    }

    Plotly.react("chart-conditions", [{
        type: "pie",
        labels: [...counts.keys()],
        values: [...counts.values()],
        hole: 0.45,
        marker: { colors: [COLORS.actual, COLORS.amber, COLORS.blue, COLORS.green, COLORS.rose] },
        textinfo: "label+percent",
        hovertemplate: "%{label}: %{value} days<extra></extra>",
    }], {
        ...baseLayout(390),
        hovermode: "closest",
        showlegend: false,
        margin: { l: 20, r: 20, t: 20, b: 20 },
    }, PLOT_CONFIG);
}

function renderSeverity() {
    const rows = cityFiltered(DATA.categories).sort((a, b) => String(a.weather_date).localeCompare(String(b.weather_date)));
    const cities = selectedCity === "All" ? ["San Jose", "Los Angeles"] : [selectedCity];
    const traces = cities.map((city) => {
        const group = rows.filter((row) => row.city === city);
        return {
            type: "scatter",
            mode: "lines",
            name: city,
            x: group.map((row) => row.weather_date),
            y: group.map((row) => row.severity_score),
            line: { color: city === "San Jose" ? COLORS.sanJose : COLORS.losAngeles, width: 3 },
            hovertemplate: "Severity %{y}<extra></extra>",
        };
    });

    Plotly.react("chart-severity", traces, {
        ...baseLayout(390),
        yaxis: { ...plotTheme().yaxis, title: "Severity score" },
    }, PLOT_CONFIG);
}

function renderRolling() {
    const rows = cityFiltered(DATA.rolling).sort((a, b) => String(a.weather_date).localeCompare(String(b.weather_date)));
    const cities = selectedCity === "All" ? ["San Jose", "Los Angeles"] : [selectedCity];
    const traces = [];

    for (const city of cities) {
        const group = rows.filter((row) => row.city === city);
        const suffix = selectedCity === "All" ? ` ${city}` : "";
        traces.push({
            type: "scatter",
            mode: "lines",
            name: `7-day max${suffix}`,
            x: group.map((row) => row.weather_date),
            y: group.map((row) => row.temp_max_7day),
            line: { color: COLORS.rose, width: 2, dash: city === "San Jose" ? "solid" : "dot" },
        });
        traces.push({
            type: "scatter",
            mode: "lines",
            name: `7-day mean${suffix}`,
            x: group.map((row) => row.weather_date),
            y: group.map((row) => row.temp_mean_7day_avg),
            line: { color: city === "San Jose" ? COLORS.sanJose : COLORS.losAngeles, width: 3 },
        });
        traces.push({
            type: "scatter",
            mode: "lines",
            name: `7-day min${suffix}`,
            x: group.map((row) => row.weather_date),
            y: group.map((row) => row.temp_min_7day),
            line: { color: COLORS.blue, width: 2, dash: city === "San Jose" ? "solid" : "dot" },
        });
    }

    Plotly.react("chart-rolling", traces, {
        ...baseLayout(500),
        yaxis: { ...plotTheme().yaxis, title: "Temperature (F)" },
    }, PLOT_CONFIG);
}

function renderAll() {
    summarizeKpis();
    renderForecast();
    renderCityComparison();
    renderAccuracy();
    renderRevisions();
    renderConditions();
    renderSeverity();
    renderRolling();
}

function initTheme() {
    const saved = localStorage.getItem("weather-dashboard-theme");
    if (saved) document.documentElement.dataset.theme = saved;

    document.getElementById("theme-toggle").addEventListener("click", () => {
        const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
        document.documentElement.dataset.theme = next;
        localStorage.setItem("weather-dashboard-theme", next);
        renderAll();
    });
}

function initCityFilter() {
    document.querySelectorAll("[data-city]").forEach((button) => {
        button.addEventListener("click", () => {
            selectedCity = button.dataset.city;
            document.querySelectorAll("[data-city]").forEach((el) => el.classList.toggle("active", el === button));
            renderAll();
        });
    });
}

async function loadJson(path) {
    const response = await fetch(path);
    if (!response.ok) throw new Error(`Failed to load ${path}`);
    return response.json();
}

async function init() {
    initTheme();
    initCityFilter();
    document.querySelector(".preset-link").href = PRESET_URL;

    try {
        const [kpis, daily, accuracy, revisions, categories, rolling] = await Promise.all([
            loadJson(FILES.kpis),
            loadJson(FILES.daily),
            loadJson(FILES.accuracy),
            loadJson(FILES.revisions),
            loadJson(FILES.categories),
            loadJson(FILES.rolling),
        ]);
        DATA = { kpis, daily, accuracy, revisions, categories, rolling };
        renderAll();
    } catch (error) {
        console.error(error);
        document.getElementById("dashboard-error").hidden = false;
        CHART_IDS.forEach((id) => {
            const el = document.getElementById(id);
            if (el) el.innerHTML = "";
        });
        document.getElementById("refresh-stamp").textContent = "Data failed to load";
    }
}

document.addEventListener("DOMContentLoaded", init);
