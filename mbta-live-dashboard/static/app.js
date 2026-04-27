const EMPTY_GEOJSON = { type: "FeatureCollection", features: [] };
const REFRESH_MS = 5000;
const ALERT_REFRESH_MS = 15000;

const state = {
  map: null,
  vehicles: EMPTY_GEOJSON,
  routeShapes: EMPTY_GEOJSON,
  selectedRoute: "",
  selectedVehicleId: "",
  showFreshOnly: false,
  userMovedMap: false,
  didFitRoute: false,
  didFlyVehicle: false,
  refreshTimer: null,
  alertsTimer: null,
  popup: null,
};

const el = {
  routeSelect: document.querySelector("#route-select"),
  vehicleSelect: document.querySelector("#vehicle-select"),
  freshToggle: document.querySelector("#fresh-toggle"),
  refreshButton: document.querySelector("#refresh-button"),
  refreshStatus: document.querySelector("#refresh-status"),
  warning: document.querySelector("#warning-badge"),
  emptyState: document.querySelector("#empty-state"),
  vehicleFeedAge: document.querySelector("#vehicle-feed-age"),
  tripFeedAge: document.querySelector("#trip-feed-age"),
  alertsFeedAge: document.querySelector("#alerts-feed-age"),
  vehicleCard: document.querySelector("#vehicle-card"),
  vehicleNote: document.querySelector("#vehicle-note"),
  stopsList: document.querySelector("#stops-list"),
  alertsList: document.querySelector("#alerts-list"),
  routeStats: document.querySelector("#route-stats"),
  vehicleList: document.querySelector("#vehicle-list"),
};

function formatAge(seconds) {
  if (seconds === null || seconds === undefined) return "--";
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return `${minutes}m ${remainder}s`;
}

function formatSpeed(speed) {
  return speed === null || speed === undefined ? "--" : `${speed} mph`;
}

function formatStop(props) {
  if (props.stop_name) return props.stop_id ? `${props.stop_name} (${props.stop_id})` : props.stop_name;
  return props.stop_id || "--";
}

function formatTime(value) {
  if (!value) return "--";
  return new Date(value).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[char]);
}

async function getJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return response.json();
}

function showWarning(message) {
  if (!el.warning) return;
  el.warning.textContent = message;
  el.warning.hidden = !message;
}

function setEmptyState(message) {
  el.emptyState.textContent = message || "";
  el.emptyState.hidden = !message;
}

function visibleFeatures() {
  if (!state.showFreshOnly) return state.vehicles.features;
  return state.vehicles.features.filter((feature) => feature.properties.is_fresh);
}

function visibleVehiclesGeojson() {
  return { type: "FeatureCollection", features: visibleFeatures() };
}

function initMap() {
  if (typeof maplibregl === "undefined") {
    console.error("MapLibre GL JS did not load.");
    return;
  }

  state.map = new maplibregl.Map({
    container: "map",
    style: {
      version: 8,
      sources: {
        osm: {
          type: "raster",
          tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
          tileSize: 256,
          attribution: "© OpenStreetMap contributors",
        },
      },
      layers: [{ id: "osm", type: "raster", source: "osm" }],
    },
    center: [-71.0589, 42.3601],
    zoom: 11,
  });

  state.map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-left");

  state.map.on("load", () => {
    state.map.addSource("route-shapes", { type: "geojson", data: EMPTY_GEOJSON });
    state.map.addLayer({
      id: "route-shape-lines",
      type: "line",
      source: "route-shapes",
      paint: {
        "line-color": ["match", ["get", "direction_id"], "0", "#e33e2f", "1", "#1d6fb8", "#596877"],
        "line-width": ["interpolate", ["linear"], ["zoom"], 10, 4, 15, 7],
        "line-opacity": 0.72,
      },
      layout: {
        "line-cap": "round",
        "line-join": "round",
      },
    });
    state.map.addLayer({
      id: "route-shape-labels",
      type: "symbol",
      source: "route-shapes",
      layout: {
        "symbol-placement": "line",
        "text-field": ["get", "direction_label"],
        "text-size": 12,
        "text-rotation-alignment": "map",
        "text-pitch-alignment": "viewport",
        "text-allow-overlap": false,
      },
      paint: {
        "text-color": "#16212c",
        "text-halo-color": "#ffffff",
        "text-halo-width": 2,
      },
    });
    state.map.addSource("vehicles", { type: "geojson", data: EMPTY_GEOJSON });
    state.map.addLayer({
      id: "vehicle-circles",
      type: "circle",
      source: "vehicles",
      paint: {
        "circle-radius": ["case", ["==", ["get", "vehicle_id"], state.selectedVehicleId || ""], 12, 7],
        "circle-color": ["case", ["==", ["get", "vehicle_id"], state.selectedVehicleId || ""], "#e33e2f", "#1d6fb8"],
        "circle-stroke-color": "#ffffff",
        "circle-stroke-width": ["case", ["==", ["get", "vehicle_id"], state.selectedVehicleId || ""], 3, 2],
        "circle-opacity": ["case", ["get", "is_fresh"], 0.95, 0.48],
      },
    });
    state.map.addLayer({
      id: "vehicle-labels",
      type: "symbol",
      source: "vehicles",
      layout: {
        "text-field": ["case", ["==", ["get", "vehicle_id"], state.selectedVehicleId || ""], ["coalesce", ["get", "vehicle_label"], ["get", "vehicle_id"]], ""],
        "text-size": 12,
        "text-offset": [0, 1.6],
        "text-anchor": "top",
        "text-allow-overlap": true,
      },
      paint: {
        "text-color": "#16212c",
        "text-halo-color": "#ffffff",
        "text-halo-width": 2,
      },
    });

    state.map.on("mouseenter", "vehicle-circles", () => {
      state.map.getCanvas().style.cursor = "pointer";
    });
    state.map.on("mouseleave", "vehicle-circles", () => {
      state.map.getCanvas().style.cursor = "";
    });
    state.map.on("click", "vehicle-circles", (event) => {
      const feature = event.features[0];
      selectVehicle(feature.properties.vehicle_id, { fly: false });
      showPopup(feature);
    });

    updateVehicleSource();
    updateRouteShapeSource();
  });

  ["dragstart", "zoomstart", "rotatestart", "pitchstart"].forEach((eventName) => {
    state.map.on(eventName, (event) => {
      if (event.originalEvent) state.userMovedMap = true;
    });
  });
}

function updateSelectedPaint() {
  if (!state.map || !state.map.getLayer("vehicle-circles")) return;
  const selected = state.selectedVehicleId || "";
  state.map.setPaintProperty("vehicle-circles", "circle-radius", ["case", ["==", ["get", "vehicle_id"], selected], 12, 7]);
  state.map.setPaintProperty("vehicle-circles", "circle-color", ["case", ["==", ["get", "vehicle_id"], selected], "#e33e2f", "#1d6fb8"]);
  state.map.setPaintProperty("vehicle-circles", "circle-stroke-width", ["case", ["==", ["get", "vehicle_id"], selected], 3, 2]);
  state.map.setLayoutProperty("vehicle-labels", "text-field", [
    "case",
    ["==", ["get", "vehicle_id"], selected],
    ["coalesce", ["get", "vehicle_label"], ["get", "vehicle_id"]],
    "",
  ]);
}

function updateVehicleSource() {
  if (!state.map || !state.map.getSource("vehicles")) return;
  state.map.getSource("vehicles").setData(visibleVehiclesGeojson());
  updateSelectedPaint();
}

function updateRouteShapeSource() {
  if (!state.map || !state.map.getSource("route-shapes")) return;
  state.map.getSource("route-shapes").setData(state.routeShapes);
}

function fitToRouteContext(force = false) {
  if (!state.map) return;
  if (state.userMovedMap && !force) return;

  const bounds = new maplibregl.LngLatBounds();
  state.routeShapes.features.forEach((feature) => {
    feature.geometry.coordinates.forEach((coordinate) => bounds.extend(coordinate));
  });
  visibleFeatures().forEach((feature) => bounds.extend(feature.geometry.coordinates));
  if (bounds.isEmpty()) return;
  state.map.fitBounds(bounds, { padding: 70, maxZoom: 15, duration: 650 });
}

function flyToSelectedOnce() {
  if (!state.map || state.didFlyVehicle || state.userMovedMap) return;
  const feature = visibleFeatures().find((item) => item.properties.vehicle_id === state.selectedVehicleId);
  if (!feature) return;
  state.didFlyVehicle = true;
  state.map.flyTo({ center: feature.geometry.coordinates, zoom: Math.max(state.map.getZoom(), 14), duration: 650 });
}

function showPopup(feature) {
  const props = feature.properties;
  if (state.popup) state.popup.remove();
  state.popup = new maplibregl.Popup({ closeButton: true })
    .setLngLat(feature.geometry.coordinates)
    .setHTML(`
      <div class="popup-title">Route ${escapeHtml(props.route_id)} · ${escapeHtml(props.vehicle_label || props.vehicle_id)}</div>
      <div>${escapeHtml(props.status || "--")}</div>
      <div>${escapeHtml(formatStop(props))}</div>
      <div>${formatSpeed(props.speed_mph)} · ${formatAge(props.age_seconds)} old</div>
    `)
    .addTo(state.map);
}

async function loadRoutes() {
  const routes = await getJson("/api/live/routes");
  el.routeSelect.innerHTML = "";
  if (!routes.length) {
    setEmptyState("No live bus routes are currently visible in the MBTA vehicle feed.");
    return;
  }

  routes.forEach((route) => {
    const option = document.createElement("option");
    option.value = route.route_id;
    option.textContent = route.label;
    el.routeSelect.appendChild(option);
  });

  if (!state.selectedRoute || !routes.some((route) => route.route_id === state.selectedRoute)) {
    state.selectedRoute = routes[0].route_id;
  }
  el.routeSelect.value = state.selectedRoute;
  setEmptyState("");
}

async function refreshRouteShapes() {
  if (!state.selectedRoute) return;
  state.routeShapes = await getJson(`/api/live/route-shapes?route_id=${encodeURIComponent(state.selectedRoute)}`);
  updateRouteShapeSource();
}

async function refreshVehicles({ routeChanged = false } = {}) {
  if (!state.selectedRoute) return;
  const data = await getJson(`/api/live/vehicles?route_id=${encodeURIComponent(state.selectedRoute)}`);
  state.vehicles = data;
  updateVehicleSource();
  renderVehicleOptions();
  renderFleetSummary();

  if (!data.features.length) {
    setEmptyState(`No active buses are currently visible for route ${state.selectedRoute}.`);
    state.selectedVehicleId = "";
    renderVehicleCard(null);
    return;
  }

  const visible = visibleFeatures();
  if (!visible.length) {
    setEmptyState(`No fresh buses are currently visible for route ${state.selectedRoute}. Turn off the fresh-only filter to see older positions.`);
    state.selectedVehicleId = "";
    renderVehicleCard(null);
    renderVehicleOptions();
    renderFleetSummary();
    return;
  }

  setEmptyState("");
  const stillVisible = visible.some((feature) => feature.properties.vehicle_id === state.selectedVehicleId);
  if (!state.selectedVehicleId || !stillVisible) {
    const previous = state.selectedVehicleId;
    state.selectedVehicleId = visible[0].properties.vehicle_id;
    el.vehicleNote.textContent = previous ? "The previously selected bus is no longer visible; showing the freshest available bus." : "";
  }

  el.vehicleSelect.value = state.selectedVehicleId;
  updateSelectedPaint();
  renderFleetSummary();

  if (routeChanged) {
    state.didFitRoute = true;
    state.didFlyVehicle = false;
    fitToRouteContext(true);
  } else if (!state.didFitRoute) {
    state.didFitRoute = true;
    fitToRouteContext();
  }

  await refreshSelectedVehicle();
}

function renderVehicleOptions() {
  const current = state.selectedVehicleId;
  el.vehicleSelect.innerHTML = "";
  visibleFeatures().forEach((feature) => {
    const props = feature.properties;
    const option = document.createElement("option");
    option.value = props.vehicle_id;
    option.textContent = `${props.vehicle_label || props.vehicle_id} · ${props.direction_label || "Direction unknown"}`;
    el.vehicleSelect.appendChild(option);
  });
  el.vehicleSelect.disabled = !visibleFeatures().length;
  if (current) el.vehicleSelect.value = current;
}

function renderVehicleCard(props) {
  if (!props) {
    el.vehicleCard.innerHTML = "";
    el.vehicleNote.textContent = "";
    return;
  }
  const rows = [
    ["Route", props.route_id || "--"],
    ["Bus", props.vehicle_label || props.vehicle_id || "--"],
    ["Status", props.status || "--"],
    ["Current stop", formatStop(props)],
    ["Direction", props.direction_label || (props.direction_id ?? "--")],
  ];
  el.vehicleCard.innerHTML = rows.map(([key, value]) => `<dt>${escapeHtml(key)}</dt><dd>${escapeHtml(value)}</dd>`).join("");
}

function renderFleetSummary() {
  const rows = [...visibleFeatures()].sort((a, b) => (b.properties.updated_timestamp || 0) - (a.properties.updated_timestamp || 0));
  if (!rows.length) {
    el.routeStats.innerHTML = "";
    el.vehicleList.innerHTML = `<p class="empty-copy">${state.showFreshOnly ? "No fresh buses for this route." : "No active buses for this route."}</p>`;
    return;
  }

  const totalCount = state.vehicles.features.length;
  const freshCount = state.vehicles.features.filter((feature) => feature.properties.is_fresh).length;
  const avgAge = Math.round(rows.reduce((total, feature) => total + (feature.properties.age_seconds || 0), 0) / rows.length);
  el.routeStats.innerHTML = `
    <span><strong>${totalCount}</strong> active</span>
    <span><strong>${freshCount}</strong> fresh</span>
    ${state.showFreshOnly ? `<span><strong>${rows.length}</strong> shown</span>` : ""}
    <span><strong>${formatAge(avgAge)}</strong> avg age</span>
  `;

  el.vehicleList.innerHTML = rows
    .map((feature) => {
      const props = feature.properties;
      const selectedClass = props.vehicle_id === state.selectedVehicleId ? " is-selected" : "";
      const freshClass = props.is_fresh ? "is-fresh" : "is-stale";
      return `
        <button class="fleet-card${selectedClass}" type="button" data-vehicle-id="${escapeHtml(props.vehicle_id)}">
          <span class="fleet-card-main">
            <strong>${escapeHtml(props.vehicle_label || props.vehicle_id)}</strong>
            <span>${escapeHtml(formatStop(props))}</span>
            <small>${escapeHtml(props.direction_label || "Direction unknown")}</small>
          </span>
          <span class="fleet-card-meta">
            <span class="freshness ${freshClass}">${formatAge(props.age_seconds)}</span>
            <span>${formatSpeed(props.speed_mph)}</span>
            <span>${escapeHtml(props.status || "--")}</span>
          </span>
        </button>
      `;
    })
    .join("");
}

async function refreshSelectedVehicle() {
  if (!state.selectedVehicleId) return;
  const props = await getJson(`/api/live/vehicle/${encodeURIComponent(state.selectedVehicleId)}`);
  renderVehicleCard(props);
  await refreshStops();
  flyToSelectedOnce();
}

async function refreshStops() {
  if (!state.selectedVehicleId) return;
  const data = await getJson(`/api/live/vehicle/${encodeURIComponent(state.selectedVehicleId)}/upcoming-stops`);
  if (!data.stops || !data.stops.length) {
    el.stopsList.innerHTML = `<p class="empty-copy">${data.message || "Upcoming stops are unavailable for this bus right now."}</p>`;
    return;
  }
  el.stopsList.innerHTML = data.stops
    .map((stop) => `
      <div class="list-item">
        <strong>#${escapeHtml(stop.stop_sequence ?? "--")} · ${escapeHtml(stop.stop_id || "--")}</strong>
        ${stop.stop_name ? `<span>${escapeHtml(stop.stop_name)}</span>` : ""}
        <small>Arr ${formatTime(stop.arrival_time)} · Dep ${formatTime(stop.departure_time)}</small>
      </div>
    `)
    .join("");
}

async function refreshAlerts() {
  if (!state.selectedRoute) return;
  const alerts = await getJson(`/api/live/alerts?route_id=${encodeURIComponent(state.selectedRoute)}`);
  if (!alerts.length) {
    el.alertsList.innerHTML = `<p class="empty-copy">No active alerts for this route</p>`;
    return;
  }
  el.alertsList.innerHTML = alerts
    .slice(0, 6)
    .map((alert) => `
      <div class="list-item">
        <strong>${escapeHtml(alert.header || "Route alert")}</strong>
        <small>${escapeHtml(alert.effect || "")}${alert.cause ? ` · ${escapeHtml(alert.cause)}` : ""}</small>
        <p>${escapeHtml(alert.description || "")}</p>
      </div>
    `)
    .join("");
}

async function refreshMeta() {
  const meta = await getJson("/api/live/meta");
  if (el.vehicleFeedAge) el.vehicleFeedAge.textContent = formatAge(meta.vehicle_feed_age_seconds);
  if (el.tripFeedAge) el.tripFeedAge.textContent = formatAge(meta.trip_feed_age_seconds);
  if (el.alertsFeedAge) el.alertsFeedAge.textContent = formatAge(meta.alerts_feed_age_seconds);
}

async function refreshAll(options = {}) {
  showWarning("");
  if (el.refreshStatus) el.refreshStatus.textContent = "Refreshing...";
  try {
    await loadRoutes();
    if (options.routeChanged) await refreshRouteShapes();
    await Promise.all([refreshVehicles(options), refreshMeta()]);
    if (options.routeChanged) await refreshAlerts();
    if (el.refreshStatus) el.refreshStatus.textContent = "Auto-refresh every 5s";
  } catch (error) {
    console.error(error);
    showWarning("Live data refresh failed. Showing last known data.");
    if (el.refreshStatus) el.refreshStatus.textContent = "Auto-refresh every 5s";
  }
}

async function selectVehicle(vehicleId, { fly = true } = {}) {
  if (!vehicleId) return;
  state.selectedVehicleId = vehicleId;
  state.didFlyVehicle = !fly;
  el.vehicleSelect.value = vehicleId;
  updateSelectedPaint();
  renderFleetSummary();
  try {
    await refreshSelectedVehicle();
  } catch (error) {
    console.error(error);
    showWarning("Selected bus details are temporarily unavailable.");
  }
}

function bindEvents() {
  el.routeSelect.addEventListener("change", async () => {
    state.selectedRoute = el.routeSelect.value;
    state.selectedVehicleId = "";
    state.userMovedMap = false;
    state.didFitRoute = false;
    state.didFlyVehicle = false;
    await refreshAll({ routeChanged: true });
  });

  el.vehicleSelect.addEventListener("change", () => {
    selectVehicle(el.vehicleSelect.value, { fly: true });
  });

  if (el.freshToggle) {
    el.freshToggle.addEventListener("change", async () => {
      state.showFreshOnly = el.freshToggle.checked;
      state.didFlyVehicle = false;
      updateVehicleSource();
      renderVehicleOptions();
      renderFleetSummary();

      const visible = visibleFeatures();
      if (!visible.some((feature) => feature.properties.vehicle_id === state.selectedVehicleId)) {
        state.selectedVehicleId = visible[0]?.properties.vehicle_id || "";
      }

      if (state.selectedVehicleId) {
        setEmptyState("");
        await refreshSelectedVehicle();
      } else {
        setEmptyState(`No fresh buses are currently visible for route ${state.selectedRoute}. Turn off the fresh-only filter to see older positions.`);
        renderVehicleCard(null);
        el.stopsList.innerHTML = "";
      }
      updateVehicleSource();
      renderVehicleOptions();
      renderFleetSummary();
    });
  }

  el.refreshButton.addEventListener("click", () => {
    refreshAll();
    refreshAlerts();
  });

  el.vehicleList.addEventListener("click", (event) => {
    const card = event.target.closest("[data-vehicle-id]");
    if (card) selectVehicle(card.dataset.vehicleId, { fly: true });
  });
}

async function start() {
  try {
    initMap();
  } catch (error) {
    console.error(error);
  }
  bindEvents();
  await refreshAll({ routeChanged: true });
  await refreshAlerts();

  state.refreshTimer = window.setInterval(() => refreshAll(), REFRESH_MS);
  state.alertsTimer = window.setInterval(() => refreshAlerts().catch(() => showWarning("Alerts are temporarily unavailable.")), ALERT_REFRESH_MS);
}

start();
