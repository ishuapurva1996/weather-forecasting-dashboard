const PRESET_URL = "https://9106dc82.us2a.app.preset.io/superset/dashboard/8/?native_filters_key=dBFp1-5AII8";

const FILES = {
    kpis: "data/kpis.json",
    daily: "data/daily_weather.json",
    accuracy: "data/forecast_accuracy.json",
    revisions: "data/forecast_revisions.json",
    categories: "data/weather_categories.json",
    rolling: "data/rolling_weather.json",
};

const CITY_LABELS = {
    "Los Angeles": "Los Angeles",
    "San Jose": "San Jose",
};

const CITY_SHORT = {
    "Los Angeles": "LA",
    "San Jose": "SJ",
};

const COLORS = {
    sanJose: "#6ed3b0",
    losAngeles: "#ffbf4d",
    sanJoseForecast: "#4b8990",
    losAngelesForecast: "#57c7df",
    error: "#2aa8bd",
    clear: "#4c568b",
    cloudy: "#2aa8bd",
    fog: "#ff7a45",
    rain: "#6a6a6a",
    drizzle: "#5ac887",
    fallback: "#8a8f98",
};

const CATEGORY_COLORS = {
    clear: COLORS.clear,
    clouds: COLORS.cloudy,
    cloudy: COLORS.cloudy,
    fog: COLORS.fog,
    rain: COLORS.rain,
    drizzle: COLORS.drizzle,
};

const CHART_IDS = [
    "spark-la-forecast",
    "spark-sj-forecast",
    "spark-la-error",
    "spark-sj-error",
    "chart-revisions-sj",
    "chart-revisions-la",
    "chart-conditions-sj",
    "chart-conditions-la",
    "chart-categories-sj",
    "chart-categories-la",
    "chart-rolling-min",
    "chart-rolling-mean",
    "chart-rolling-max",
    "chart-history-forecast",
];

const PLOT_CONFIG = {
    responsive: true,
    displayModeBar: false,
};

const MAX_STALE_DAYS = 3;

let DATA = {};
let selectedCity = "Los Angeles";

function cssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function plotTheme() {
    return {
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { color: cssVar("--text"), family: "IBM Plex Sans, sans-serif", size: 12 },
        xaxis: { gridcolor: cssVar("--grid"), zerolinecolor: cssVar("--grid") },
        yaxis: { gridcolor: cssVar("--grid"), zerolinecolor: cssVar("--grid") },
        margin: { l: 48, r: 18, t: 32, b: 44 },
    };
}

function formatTemp(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
    return `${Number(value).toFixed(2)}`;
}

function parseDate(value) {
    if (!value || value === "sample data") return null;
    const parsed = new Date(value.includes("T") ? value : `${value}T00:00:00`);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function dateLabel(value) {
    if (!value) return "--";
    const date = new Date(`${value}T00:00:00`);
    return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function daysBetween(later, earlier) {
    const laterCopy = new Date(later);
    const earlierCopy = new Date(earlier);
    const msPerDay = 24 * 60 * 60 * 1000;
    return Math.floor((laterCopy.setHours(0, 0, 0, 0) - earlierCopy.setHours(0, 0, 0, 0)) / msPerDay);
}

function rowsFor(rows, city) {
    return (rows || []).filter((row) => normalizeCity(row.city) === city);
}

function sortedByDate(rows, key) {
    return [...rows].sort((a, b) => String(a[key]).localeCompare(String(b[key])));
}

function normalizeCity(value) {
    return String(value || "").trim().replace(/^["']|["']$/g, "");
}

function average(values) {
    const clean = values.filter((value) => value !== null && value !== undefined && !Number.isNaN(Number(value)));
    if (!clean.length) return null;
    return clean.reduce((sum, value) => sum + Number(value), 0) / clean.length;
}

function cityColor(city, forecast = false) {
    if (city === "San Jose") return forecast ? COLORS.sanJoseForecast : COLORS.sanJose;
    return forecast ? COLORS.losAngelesForecast : COLORS.losAngeles;
}

function categoryKey(value) {
    return String(value || "uncategorized").toLowerCase();
}

function categoryLabel(value) {
    const key = categoryKey(value);
    if (key === "clouds") return "cloudy";
    return key;
}

function categoryColor(value) {
    return CATEGORY_COLORS[categoryKey(value)] || CATEGORY_COLORS[categoryLabel(value)] || COLORS.fallback;
}

function baseLayout(height = 330) {
    return {
        ...plotTheme(),
        height,
        hovermode: "x unified",
        legend: {
            orientation: "h",
            y: 1.16,
            x: 0,
            font: { color: cssVar("--muted"), size: 11 },
        },
    };
}

function sparkLayout(color) {
    return {
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        height: 82,
        margin: { l: 0, r: 0, t: 6, b: 0 },
        xaxis: { visible: false },
        yaxis: { visible: false },
        showlegend: false,
        hovermode: false,
        shapes: [],
        colorway: [color],
    };
}

function renderFreshness() {
    document.getElementById("refresh-stamp").textContent = `Data refreshed: ${DATA.kpis?.generated_at || "sample data"}`;

    const banner = document.getElementById("freshness-banner");
    const generatedAt = DATA.kpis?.generated_at;
    const generatedDate = parseDate(generatedAt);
    const actualDates = (DATA.kpis?.by_city || [])
        .map((row) => parseDate(row.latest_actual_date))
        .filter(Boolean)
        .sort((a, b) => a - b);
    const actualDate = actualDates.at(-1);

    banner.classList.remove("freshness-ok", "freshness-warning");

    if (!generatedDate || generatedAt === "sample data") {
        banner.textContent = "Sample data is loaded. Run the dashboard export workflow with Snowflake secrets before using this as the real live dashboard.";
        banner.classList.add("freshness-warning");
        banner.hidden = false;
        return;
    }

    if (!actualDate) {
        banner.textContent = "Data was exported from Snowflake, but no latest actual weather date was found in the KPI file.";
        banner.classList.add("freshness-warning");
        banner.hidden = false;
        return;
    }

    const staleDays = daysBetween(new Date(), actualDate);
    if (staleDays > MAX_STALE_DAYS) {
        banner.textContent = `Data was exported, but latest actual weather is ${dateLabel(actualDate.toISOString().slice(0, 10))}. Refresh the Airflow pipeline and deploy workflow.`;
        banner.classList.add("freshness-warning");
        banner.hidden = false;
        return;
    }

    banner.textContent = `Live Snowflake export is current. Latest actual weather: ${dateLabel(actualDate.toISOString().slice(0, 10))}.`;
    banner.classList.add("freshness-ok");
    banner.hidden = false;
}

function forecastRows(city) {
    return sortedByDate(rowsFor(DATA.daily, city).filter((row) => row.record_type === "forecast"), "weather_date");
}

function historyRows(city) {
    return sortedByDate(rowsFor(DATA.daily, city).filter((row) => row.record_type === "history"), "weather_date");
}

function accuracyRows(city) {
    return sortedByDate(rowsFor(DATA.accuracy, city), "forecast_for_date");
}

function renderSparkline(id, rows, xKey, yKey, color) {
    Plotly.react(id, [{
        type: "scatter",
        mode: "lines",
        x: rows.map((row) => row[xKey]),
        y: rows.map((row) => row[yKey]),
        line: { color, width: 2 },
        fill: "tozeroy",
        fillcolor: `${color}33`,
        hoverinfo: "skip",
    }], sparkLayout(color), PLOT_CONFIG);
}

function renderKpis() {
    const laForecastRows = forecastRows("Los Angeles");
    const sjForecastRows = forecastRows("San Jose");
    const laAccuracyRows = accuracyRows("Los Angeles");
    const sjAccuracyRows = accuracyRows("San Jose");

    document.getElementById("kpi-la-forecast").textContent = formatTemp(average(laForecastRows.map((row) => row.forecast_temp_max)));
    document.getElementById("kpi-sj-forecast").textContent = formatTemp(average(sjForecastRows.map((row) => row.forecast_temp_max)));
    document.getElementById("kpi-la-error").textContent = formatTemp(average(laAccuracyRows.map((row) => row.absolute_error)));
    document.getElementById("kpi-sj-error").textContent = formatTemp(average(sjAccuracyRows.map((row) => row.absolute_error)));

    renderSparkline("spark-la-forecast", laForecastRows, "weather_date", "forecast_temp_max", "#ff8a54");
    renderSparkline("spark-sj-forecast", sjForecastRows, "weather_date", "forecast_temp_max", COLORS.sanJose);
    renderSparkline("spark-la-error", laAccuracyRows, "forecast_for_date", "absolute_error", COLORS.error);
    renderSparkline("spark-sj-error", sjAccuracyRows, "forecast_for_date", "absolute_error", COLORS.error);
}

function renderRevisions(city, elementId) {
    const rows = sortedByDate(rowsFor(DATA.revisions, city), "forecast_for_date");
    const revisions = new Map();

    for (const row of rows) {
        const key = row.forecast_made_on || "unknown";
        if (!revisions.has(key)) revisions.set(key, []);
        revisions.get(key).push(row);
    }

    const palette = [COLORS.sanJose, COLORS.losAngeles, COLORS.error, "#ef6f79", "#b9a44c", "#59768a"];
    const traces = [...revisions.entries()].map(([madeOn, group], index) => ({
        type: "scatter",
        mode: "lines+markers",
        name: madeOn,
        x: group.map((row) => row.forecast_for_date),
        y: group.map((row) => row.predicted_temp_max),
        line: { color: palette[index % palette.length], width: 2 },
        marker: { size: 6 },
        hovertemplate: "%{y:.1f}<extra></extra>",
    }));

    Plotly.react(elementId, traces, {
        ...baseLayout(330),
        yaxis: { ...plotTheme().yaxis, title: "Forecast Temperature" },
        xaxis: { ...plotTheme().xaxis, title: "Forecast Date" },
    }, PLOT_CONFIG);
}

function renderConditions(city, elementId) {
    const counts = new Map();
    for (const row of rowsFor(DATA.categories, city)) {
        const label = categoryLabel(row.weather_category);
        counts.set(label, (counts.get(label) || 0) + 1);
    }

    const labels = [...counts.keys()];
    Plotly.react(elementId, [{
        type: "pie",
        labels,
        values: labels.map((label) => counts.get(label)),
        marker: { colors: labels.map(categoryColor) },
        textinfo: "label",
        sort: false,
        hovertemplate: "%{label}: %{value} days<extra></extra>",
    }], {
        ...baseLayout(330),
        showlegend: true,
        legend: { orientation: "h", y: 1.12, x: 0.4, font: { color: cssVar("--muted"), size: 11 } },
        margin: { l: 20, r: 20, t: 24, b: 20 },
    }, PLOT_CONFIG);
}

function renderCategoriesOverTime(city, elementId) {
    const rows = sortedByDate(rowsFor(DATA.categories, city), "weather_date");
    const dates = [...new Set(rows.map((row) => row.weather_date))];
    const categories = [...new Set(rows.map((row) => categoryLabel(row.weather_category)))];

    const traces = categories.map((category) => ({
        type: "bar",
        name: category,
        x: dates,
        y: dates.map((date) => rows.filter((row) => row.weather_date === date && categoryLabel(row.weather_category) === category).length),
        marker: { color: categoryColor(category) },
        hovertemplate: `${category}: %{y}<extra></extra>`,
    }));

    Plotly.react(elementId, traces, {
        ...baseLayout(330),
        barmode: "stack",
        yaxis: { ...plotTheme().yaxis, title: "Days" },
        legend: { orientation: "h", y: 1.15, x: 0.42, font: { color: cssVar("--muted"), size: 11 } },
    }, PLOT_CONFIG);
}

function renderRollingMetric(elementId, metricKey) {
    const rows = sortedByDate(DATA.rolling || [], "weather_date");
    const traces = ["Los Angeles", "San Jose"].map((city) => {
        const group = rowsFor(rows, city);
        const isFocused = city === selectedCity;
        return {
            type: "scatter",
            mode: "lines+markers",
            name: CITY_LABELS[city],
            x: group.map((row) => row.weather_date),
            y: group.map((row) => row[metricKey]),
            line: { color: cityColor(city), width: isFocused ? 3 : 2 },
            marker: { size: isFocused ? 5 : 4 },
            opacity: isFocused ? 1 : 0.65,
            hovertemplate: "%{y:.1f}<extra></extra>",
        };
    });

    Plotly.react(elementId, traces, {
        ...baseLayout(300),
        yaxis: { ...plotTheme().yaxis, title: "Temperature" },
    }, PLOT_CONFIG);
}

function renderHistoryForecast() {
    const traces = [];

    for (const city of ["San Jose", "Los Angeles"]) {
        const history = historyRows(city);
        const forecast = forecastRows(city);
        const isFocused = city === selectedCity;

        traces.push({
            type: "scatter",
            mode: "lines",
            name: `AVG(ACTUAL_TEMP_MAX), ${CITY_SHORT[city]}, history`,
            x: history.map((row) => row.weather_date),
            y: history.map((row) => row.actual_temp_max),
            line: { color: cityColor(city), width: isFocused ? 3 : 2 },
            opacity: isFocused ? 1 : 0.62,
            hovertemplate: "%{y:.1f}<extra></extra>",
        });

        traces.push({
            type: "scatter",
            mode: "lines",
            name: `AVG(FORECAST_TEMP_MAX), ${CITY_SHORT[city]}, forecast`,
            x: forecast.map((row) => row.weather_date),
            y: forecast.map((row) => row.forecast_temp_max),
            line: { color: cityColor(city, true), width: isFocused ? 3 : 2 },
            opacity: isFocused ? 1 : 0.62,
            hovertemplate: "%{y:.1f}<extra></extra>",
        });
    }

    Plotly.react("chart-history-forecast", traces, {
        ...baseLayout(420),
        yaxis: { ...plotTheme().yaxis, title: "Temperature" },
    }, PLOT_CONFIG);
}

function renderAll() {
    renderFreshness();
    renderKpis();
    renderRevisions("San Jose", "chart-revisions-sj");
    renderRevisions("Los Angeles", "chart-revisions-la");
    renderConditions("San Jose", "chart-conditions-sj");
    renderConditions("Los Angeles", "chart-conditions-la");
    renderCategoriesOverTime("San Jose", "chart-categories-sj");
    renderCategoriesOverTime("Los Angeles", "chart-categories-la");
    renderRollingMetric("chart-rolling-min", "temp_min_7day");
    renderRollingMetric("chart-rolling-mean", "temp_mean_7day_avg");
    renderRollingMetric("chart-rolling-max", "temp_max_7day");
    renderHistoryForecast();
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

function initCityFocus() {
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
    initCityFocus();
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
