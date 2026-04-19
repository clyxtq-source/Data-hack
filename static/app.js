const splashScreen = document.getElementById("splash-screen");
const appShell = document.getElementById("app-shell");
const form = document.getElementById("route-form");
const locateButton = document.getElementById("locate-btn");
const routeButton = document.getElementById("route-btn");
const originInput = document.getElementById("origin");
const destinationInput = document.getElementById("destination");
const originSuggestions = document.getElementById("origin-suggestions");
const destinationSuggestions = document.getElementById("destination-suggestions");
const resultsContainer = document.getElementById("results");
const statusText = document.getElementById("status-text");
const weatherBanner = document.getElementById("weather-banner");
const modeButtons = Array.from(document.querySelectorAll(".mode-card"));
const walkLimitField = document.getElementById("walk-limit-field");
const maxWalkInput = document.getElementById("max-walk-distance");
const LOCATION_CACHE_KEY = "sydney-traffic:last-location";
const LOCATION_CACHE_MAX_AGE_MS = 1000 * 60 * 60 * 12;
const LOCATION_DEADLINE_MS = 15000;
const SYDNEY_BOUNDS = {
  south: -34.35,
  north: -33.10,
  west: 150.52,
  east: 151.55,
};

const map = L.map("map", {
  zoomControl: true,
  preferCanvas: true,
}).setView([-33.8688, 151.2093], 10);

L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
}).addTo(map);

const appState = {
  activeProfile: "drive",
  userLocation: null,
  networkLayer: null,
  routeLayers: [],
  endpointLayers: [],
  locationLayers: [],
  transferLayers: [],
  hoverTooltip: null,
  gpsMarker: null,
  pendingRoutes: [],
  navigation: null,
  watchId: null,
  rerouting: false,
  locationRequestStarted: false,
};

function loadCachedLocation() {
  try {
    const raw = window.localStorage.getItem(LOCATION_CACHE_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

function saveCachedLocation(payload) {
  try {
    const stampedPayload = {
      ...payload,
      cached_at: Date.now(),
    };
    window.localStorage.setItem(LOCATION_CACHE_KEY, JSON.stringify(stampedPayload));
  } catch {
    // ignore
  }
}

function isCachedLocationFresh(payload) {
  if (!payload || !payload.cached_at) {
    return false;
  }
  return Date.now() - Number(payload.cached_at) <= LOCATION_CACHE_MAX_AGE_MS;
}

function isInsideSydney(lat, lng) {
  return (
    lat >= SYDNEY_BOUNDS.south &&
    lat <= SYDNEY_BOUNDS.north &&
    lng >= SYDNEY_BOUNDS.west &&
    lng <= SYDNEY_BOUNDS.east
  );
}

function setStatus(message) {
  statusText.textContent = message;
}

function clearLayerSet(layers) {
  layers.forEach((layer) => map.removeLayer(layer));
  layers.length = 0;
}

function clearRouteArtifacts() {
  clearLayerSet(appState.routeLayers);
  clearLayerSet(appState.endpointLayers);
  if (appState.hoverTooltip) {
    map.removeLayer(appState.hoverTooltip);
    appState.hoverTooltip = null;
  }
}

function clearWeatherBanner() {
  weatherBanner.innerHTML = "";
  weatherBanner.className = "weather-banner is-hidden";
}

function weatherIconMarkup(icon) {
  if (icon === "sun") {
    return '<span class="weather-icon" aria-hidden="true">&#9728;</span>';
  }
  if (icon === "rain") {
    return '<span class="weather-icon" aria-hidden="true">&#9730;</span>';
  }
  return '<span class="weather-icon" aria-hidden="true">&#9729;</span>';
}

function renderWeatherBanner(route) {
  const summary = route?.weather_summary;
  if (!summary || !summary.headline) {
    clearWeatherBanner();
    return;
  }

  const detailMarkup = Array.isArray(summary.details) && summary.details.length > 0
    ? `<div class="weather-details">${summary.details.map((detail) => `<div>${detail}</div>`).join("")}</div>`
    : "";

  weatherBanner.className = `weather-banner weather-${summary.icon || "mixed"}`;
  weatherBanner.innerHTML = `
    <div class="weather-main">
      ${weatherIconMarkup(summary.icon)}
      <div>
        <strong>${summary.headline}</strong>
        ${detailMarkup}
      </div>
    </div>
  `;
}

function clearLocationArtifacts() {
  clearLayerSet(appState.locationLayers);
  if (appState.gpsMarker) {
    map.removeLayer(appState.gpsMarker);
    appState.gpsMarker = null;
  }
}

function clearTransferArtifacts() {
  clearLayerSet(appState.transferLayers);
}

function setRouteLoading(isLoading) {
  routeButton.disabled = isLoading;
  routeButton.textContent = isLoading ? "Loading..." : "Show routes";
}

function setLocationLoading(isLoading) {
  locateButton.disabled = isLoading;
  locateButton.textContent = isLoading ? "Locating..." : "Use my location";
}

function showAppAfterSplash() {
  window.setTimeout(() => {
    splashScreen.classList.add("fade-out");
    window.setTimeout(() => {
      splashScreen.remove();
      appShell.classList.remove("is-hidden");
      appShell.classList.add("is-ready");
      map.invalidateSize();
    }, 1200);
  }, 1100);
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: "Request failed." }));
    throw new Error(error.detail || "Request failed.");
  }
  return response.json();
}

async function fetchNetwork() {
  return fetchJson("/api/network");
}

async function fetchRoute(origin, destination, profile, maxWalkDistanceKm = null) {
  return fetchJson("/api/route", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ origin, destination, profile, max_walk_distance_km: maxWalkDistanceKm }),
  });
}

async function snapLocation(lat, lng) {
  return fetchJson("/api/snap-location", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lat, lng }),
  });
}

async function reverseGeocode(lat, lng) {
  return fetchJson("/api/reverse-geocode", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lat, lng }),
  });
}

async function fetchSuggestions(query, mode) {
  const params = new URLSearchParams({ q: query, mode });
  if (appState.userLocation) {
    params.set("user_lat", String(appState.userLocation.lat));
    params.set("user_lng", String(appState.userLocation.lng));
  }
  return fetchJson(`/api/suggest?${params.toString()}`);
}

function drawNetworkOverview(data) {
  if (appState.networkLayer) {
    map.removeLayer(appState.networkLayer);
  }

  appState.networkLayer = L.geoJSON(data.geojson, {
    style: {
      color: "#7b8794",
      weight: 1,
      opacity: 0.22,
    },
    interactive: false,
  }).addTo(map);

  map.fitBounds(
    [
      [data.bounds.south, data.bounds.west],
      [data.bounds.north, data.bounds.east],
    ],
    { padding: [20, 20] }
  );
}

function drawEndpoints(origin, destination) {
  const originMarker = L.marker([origin.lat, origin.lng]).bindPopup(`Origin: ${origin.query}`);
  const destinationMarker = L.marker([destination.lat, destination.lng]).bindPopup(`Destination: ${destination.query}`);
  originMarker.addTo(map);
  destinationMarker.addTo(map);
  appState.endpointLayers.push(originMarker, destinationMarker);
}

function interpolateMetric(start, end, ratio, key) {
  return start[key] + (end[key] - start[key]) * ratio;
}

function findNearestTracePoint(route, latlng) {
  const cursorPoint = map.latLngToLayerPoint(latlng);
  let bestMatch = null;

  for (let index = 0; index < route.trace.length - 1; index += 1) {
    const start = route.trace[index];
    const end = route.trace[index + 1];
    const startPoint = map.latLngToLayerPoint([start.lat, start.lng]);
    const endPoint = map.latLngToLayerPoint([end.lat, end.lng]);
    const segment = endPoint.subtract(startPoint);
    const lengthSquared = segment.x * segment.x + segment.y * segment.y;
    const rawT =
      lengthSquared === 0
        ? 0
        : ((cursorPoint.x - startPoint.x) * segment.x + (cursorPoint.y - startPoint.y) * segment.y) / lengthSquared;
    const t = Math.max(0, Math.min(1, rawT));
    const projection = L.point(startPoint.x + segment.x * t, startPoint.y + segment.y * t);
    const distancePx = projection.distanceTo(cursorPoint);

    if (!bestMatch || distancePx < bestMatch.distancePx) {
      const cumulativeDistance = interpolateMetric(start, end, t, "distance_m");
      const cumulativeMinutes = interpolateMetric(start, end, t, "minutes");

      bestMatch = {
        distancePx,
        latlng: map.layerPointToLatLng(projection),
        elapsedDistanceKm: cumulativeDistance / 1000,
        elapsedMinutes: cumulativeMinutes,
        remainingDistanceKm: route.distance_km - cumulativeDistance / 1000,
        remainingMinutes: route.estimated_minutes - cumulativeMinutes,
      };
    }
  }

  return bestMatch;
}

function attachHoverInspector(polyline, route) {
  polyline.on("mousemove", (event) => {
    const snapshot = findNearestTracePoint(route, event.latlng);
    if (!snapshot) {
      return;
    }

    if (!appState.hoverTooltip) {
      appState.hoverTooltip = L.popup({
        closeButton: false,
        autoClose: false,
        closeOnClick: false,
        className: "hover-popup",
        offset: [0, -10],
      });
    }

    appState.hoverTooltip
      .setLatLng(snapshot.latlng)
      .setContent(`
        <div class="popup-card">
          <strong>${route.label}</strong><br />
          From origin: ${snapshot.elapsedDistanceKm.toFixed(2)} km, ${snapshot.elapsedMinutes.toFixed(2)} min<br />
          To destination: ${Math.max(snapshot.remainingDistanceKm, 0).toFixed(2)} km, ${Math.max(snapshot.remainingMinutes, 0).toFixed(2)} min
        </div>
      `)
      .openOn(map);
  });

  polyline.on("mouseout", () => {
    if (appState.hoverTooltip) {
      map.closePopup(appState.hoverTooltip);
    }
  });
}

function drawRoute(route, emphasize = true) {
  const polyline = L.polyline(route.polyline, {
    color: route.color,
    weight: emphasize ? 7 : 5,
    opacity: emphasize ? 0.88 : 0.54,
  }).addTo(map);

  attachHoverInspector(polyline, route);
  appState.routeLayers.push(polyline);
  return polyline;
}

function drawDetectedLocation(locationData) {
  clearLocationArtifacts();

  const userMarker = L.circleMarker([locationData.user_location.lat, locationData.user_location.lng], {
    radius: 7,
    color: "#0b7285",
    fillColor: "#15aabf",
    fillOpacity: 0.9,
    weight: 2,
  }).bindPopup(`Detected location: ${locationData.user_location.query}`);

  const snappedMarker = L.circleMarker([locationData.nearest_road_node.lat, locationData.nearest_road_node.lng], {
    radius: 7,
    color: "#2b8a3e",
    fillColor: "#51cf66",
    fillOpacity: 0.88,
    weight: 2,
  }).bindPopup(`Nearest road place: ${locationData.nearest_road_node.query}`);

  const connector = L.polyline(
    [
      [locationData.user_location.lat, locationData.user_location.lng],
      [locationData.nearest_road_node.lat, locationData.nearest_road_node.lng],
    ],
    {
      color: "#20c997",
      dashArray: "7 7",
      weight: 3,
      opacity: 0.78,
    }
  );

  userMarker.addTo(map);
  snappedMarker.addTo(map);
  connector.addTo(map);
  appState.locationLayers.push(userMarker, snappedMarker, connector);
}

function updateGpsMarker(lat, lng) {
  if (!appState.gpsMarker) {
    appState.gpsMarker = L.circleMarker([lat, lng], {
      radius: 8,
      color: "#ffffff",
      fillColor: "#111827",
      fillOpacity: 0.95,
      weight: 2,
    }).addTo(map);
    return;
  }
  appState.gpsMarker.setLatLng([lat, lng]);
}

function fillSuggestions(container, input, values) {
  container.innerHTML = "";
  if (values.length === 0) {
    container.classList.remove("is-open");
    return;
  }

  values.forEach((value) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "suggestion-item";
    button.textContent = value;
    button.addEventListener("click", () => {
      input.value = value;
      container.innerHTML = "";
      container.classList.remove("is-open");
      input.focus();
    });
    container.appendChild(button);
  });

  container.classList.add("is-open");
}

function debounce(fn, waitMs) {
  let timeoutId = null;
  return (...args) => {
    if (timeoutId) {
      window.clearTimeout(timeoutId);
    }
    timeoutId = window.setTimeout(() => fn(...args), waitMs);
  };
}

function bindSuggestionInput(input, container, mode) {
  const runLookup = debounce(async () => {
    const query = input.value.trim();
    if (query.length < 1) {
      container.innerHTML = "";
      container.classList.remove("is-open");
      return;
    }

    try {
      const payload = await fetchSuggestions(query, mode);
      fillSuggestions(container, input, payload.suggestions);
    } catch {
      container.innerHTML = "";
      container.classList.remove("is-open");
    }
  }, 120);

  input.addEventListener("input", runLookup);
  input.addEventListener("focus", runLookup);
  input.addEventListener("blur", () => {
    window.setTimeout(() => container.classList.remove("is-open"), 140);
  });
}

function setMode(profile) {
  appState.activeProfile = profile;
  modeButtons.forEach((button) => {
    button.classList.toggle("is-active", button.dataset.profile === profile);
  });
  walkLimitField.classList.toggle("is-hidden", profile !== "combined");
  routeButton.textContent =
    profile === "walk" ? "Start walking" :
    profile === "transit" ? "Show transit" :
    profile === "combined" ? "Show combined" :
    "Show routes";
}

function renderRouteChoices(data) {
  resultsContainer.innerHTML = "";
  if (data.routes[0]) {
    renderWeatherBanner(data.routes[0]);
  } else {
    clearWeatherBanner();
  }

  if (data.profile === "walk") {
    enterNavigationMode(data.routes[0], data.destination.query, data.profile, data.off_route_threshold_m);
    return;
  }

  const intro = document.createElement("div");
  intro.className = "route-choice-intro";
  intro.textContent =
    data.profile === "drive"
      ? "Choose one driving option to enter navigation mode."
      : "Choose one route to inspect the full step-by-step journey.";
  resultsContainer.appendChild(intro);

  data.routes.forEach((route) => {
    const card = document.createElement("button");
    card.type = "button";
    card.className = "route-choice-card";
    card.innerHTML = `
      <div class="result-header">
        <span class="route-dot" style="background:${route.color}"></span>
        <h3>${route.label}</h3>
      </div>
      <p>${route.distance_km} km</p>
      <p>${route.estimated_minutes} min</p>
    `;
    card.addEventListener("click", () => {
      if (data.profile === "drive") {
        enterNavigationMode(route, data.destination.query, data.profile, data.off_route_threshold_m);
        return;
      }
      openJourneyDetails(route, data.destination.query);
    });
    resultsContainer.appendChild(card);
  });
}

function openJourneyDetails(route, destinationQuery) {
  clearRouteArtifacts();
  clearTransferArtifacts();
  renderWeatherBanner(route);
  if (route.polyline && route.polyline.length > 1) {
    const polyline = drawRoute(route, true);
    map.fitBounds(polyline.getBounds(), { padding: [40, 40] });
  }

  drawEndpoints(
    { lat: route.polyline?.[0]?.[0] ?? appState.userLocation?.lat ?? -33.8688, lng: route.polyline?.[0]?.[1] ?? appState.userLocation?.lng ?? 151.2093, query: originInput.value || "Origin" },
    {
      lat: route.polyline?.[route.polyline.length - 1]?.[0] ?? -33.8688,
      lng: route.polyline?.[route.polyline.length - 1]?.[1] ?? 151.2093,
      query: destinationQuery,
    }
  );

  (route.details?.transfer_points || []).forEach((transfer) => {
    const marker = L.circleMarker([transfer.lat, transfer.lng], {
      radius: 7,
      color: "#7c2d12",
      fillColor: "#fb923c",
      fillOpacity: 0.95,
      weight: 2,
    }).bindPopup(`
      <div class="popup-card">
        <strong>${transfer.title}</strong><br />
        ${transfer.summary}<br />
        ${transfer.from || ""}${transfer.to ? ` -> ${transfer.to}` : ""}
      </div>
    `);
    marker.addTo(map);
    appState.transferLayers.push(marker);
  });

  const stepCards = (route.details?.steps || [])
    .map((step, index) => {
      const altLines =
        step.alternatives && step.alternatives.length > 1
          ? `<small class="alt-lines">Alternatives: ${step.alternatives.join(" / ")}</small>`
          : "";

      if (step.mode === "walk") {
        return `
          <details class="journey-step" ${index === 0 ? "open" : ""}>
            <summary>
              <span>Walk</span>
              <span>${step.duration_minutes} min</span>
            </summary>
            <div class="journey-step-body">
              <p>${step.from || "Current point"} to ${step.to || "next point"}</p>
              <small>${step.distance_km} km · ${step.duration_minutes} min</small>
            </div>
          </details>
        `;
      }

      return `
        <details class="journey-step" ${index === 0 ? "open" : ""}>
          <summary>
            <span>${step.line || step.mode}</span>
            <span>${step.duration_minutes} min</span>
          </summary>
          <div class="journey-step-body">
            <p>Board at ${step.from}${step.from_platform ? ` (${step.from_platform})` : ""}</p>
            <p>Alight at ${step.to}${step.to_platform ? ` (${step.to_platform})` : ""}</p>
            <small>Wait ${step.wait_minutes} min · Ride ${step.duration_minutes} min · ${step.stops_count} stops</small>
            ${altLines}
          </div>
        </details>
      `;
    })
    .join("");

  resultsContainer.innerHTML = `
    <article class="navigation-card">
      <div class="navigation-top">
        <div>
          <div class="nav-label">${route.label}</div>
          <h3>Journey details</h3>
        </div>
      </div>
      <div class="nav-metrics">
        <div class="metric-box">
          <span>Total time</span>
          <strong>${route.estimated_minutes.toFixed(2)} min</strong>
          <small>${route.distance_km.toFixed(2)} km</small>
        </div>
        <div class="metric-box">
          <span>Walk / Wait</span>
          <strong>${(route.walk_distance_km || 0).toFixed(2)} km walk</strong>
          <small>${(route.wait_minutes || 0).toFixed(2)} min wait</small>
        </div>
      </div>
      <div class="journey-steps">${stepCards}</div>
    </article>
  `;
  setStatus("Detailed public transport journey loaded.");
}

function renderNavigationPanel(route, snapshot, rerouting = false) {
  renderWeatherBanner(route);
  resultsContainer.innerHTML = `
    <article class="navigation-card">
      <div class="navigation-top">
        <div>
          <div class="nav-label">${route.label}</div>
          <h3>Navigation live</h3>
        </div>
        <button type="button" id="exit-nav-btn" class="ghost-button">Exit</button>
      </div>
      <div class="nav-metrics">
        <div class="metric-box">
          <span>To destination</span>
          <strong>${snapshot.remainingDistanceKm.toFixed(2)} km</strong>
          <small>${snapshot.remainingMinutes.toFixed(2)} min</small>
        </div>
        <div class="metric-box">
          <span>Covered</span>
          <strong>${snapshot.elapsedDistanceKm.toFixed(2)} km</strong>
          <small>${snapshot.elapsedMinutes.toFixed(2)} min</small>
        </div>
      </div>
      <p class="status-inline">${rerouting ? "Off route detected. Replanning..." : "Following selected route. Other routes are hidden."}</p>
    </article>
  `;

  document.getElementById("exit-nav-btn").addEventListener("click", exitNavigationMode);
}

function exitNavigationMode() {
  appState.navigation = null;
  clearTransferArtifacts();
  resultsContainer.innerHTML = "";
  clearWeatherBanner();
  setStatus("Navigation exited. You can show routes again.");
  if (appState.watchId !== null && navigator.geolocation) {
    navigator.geolocation.clearWatch(appState.watchId);
    appState.watchId = null;
  }
}

function startWatchTracking() {
  if (!navigator.geolocation || appState.watchId !== null) {
    return;
  }

  appState.watchId = navigator.geolocation.watchPosition(
    (position) => {
      const lat = position.coords.latitude;
      const lng = position.coords.longitude;
      appState.userLocation = { lat, lng };
      updateGpsMarker(lat, lng);

      if (!appState.navigation) {
        return;
      }

      const currentPoint = L.latLng(lat, lng);
      const snapshot = findNearestTracePoint(appState.navigation.route, currentPoint);
      if (!snapshot) {
        return;
      }

      renderNavigationPanel(appState.navigation.route, snapshot, false);

      if (snapshot.distancePx > 35 && !appState.rerouting) {
        const distanceMeters = map.distance(currentPoint, snapshot.latlng);
        if (distanceMeters > appState.navigation.offRouteThresholdMeters) {
          rerouteFromCurrentLocation(lat, lng);
        }
      }
    },
    () => {
      setStatus("Live navigation location updates are unavailable.");
    },
    {
      enableHighAccuracy: true,
      maximumAge: 5000,
      timeout: 8000,
    }
  );
}

async function rerouteFromCurrentLocation(lat, lng) {
  if (!appState.navigation || appState.rerouting) {
    return;
  }

  appState.rerouting = true;
  const currentRoute = appState.navigation.route;
  renderNavigationPanel(currentRoute, findNearestTracePoint(currentRoute, L.latLng(lat, lng)) || {
    remainingDistanceKm: currentRoute.distance_km,
    remainingMinutes: currentRoute.estimated_minutes,
    elapsedDistanceKm: 0,
    elapsedMinutes: 0,
  }, true);

  try {
    const data = await fetchRoute(`${lat},${lng}`, appState.navigation.destinationQuery, appState.navigation.profile);
    const nextRoute =
      appState.navigation.profile === "walk"
        ? data.routes[0]
        : data.routes.find((route) => route.label === appState.navigation.route.label) || data.routes[0];

    appState.navigation.route = nextRoute;
    appState.navigation.offRouteThresholdMeters = data.off_route_threshold_m;
    clearRouteArtifacts();
    const polyline = drawRoute(nextRoute, true);
    drawEndpoints({ lat, lng, query: "Current location" }, data.destination);
    map.fitBounds(polyline.getBounds(), { padding: [40, 40] });

    const snapshot = findNearestTracePoint(nextRoute, L.latLng(lat, lng));
    if (snapshot) {
      renderNavigationPanel(nextRoute, snapshot, false);
    }
    setStatus("Route updated from your current position.");
  } catch (error) {
    setStatus(error.message);
  } finally {
    appState.rerouting = false;
  }
}

function enterNavigationMode(route, destinationQuery, profile, offRouteThresholdMeters) {
  appState.navigation = {
    route,
    destinationQuery,
    profile,
    offRouteThresholdMeters,
  };

  clearRouteArtifacts();
  const polyline = drawRoute(route, true);
  drawEndpoints(
    { lat: route.trace[0].lat, lng: route.trace[0].lng, query: originInput.value || "Origin" },
    { lat: route.trace[route.trace.length - 1].lat, lng: route.trace[route.trace.length - 1].lng, query: destinationQuery }
  );
  map.fitBounds(polyline.getBounds(), { padding: [40, 40] });
  setStatus("Navigation mode started.");

  const seedSnapshot = {
    remainingDistanceKm: route.distance_km,
    remainingMinutes: route.estimated_minutes,
    elapsedDistanceKm: 0,
    elapsedMinutes: 0,
  };
  renderNavigationPanel(route, seedSnapshot, false);
  startWatchTracking();
}

function applyCachedLocation(cachedLocation) {
  if (!cachedLocation) {
    return;
  }

  appState.userLocation = {
    lat: cachedLocation.user_location.lat,
    lng: cachedLocation.user_location.lng,
  };
  originInput.value = cachedLocation.nearest_road_node.query;
  drawDetectedLocation(cachedLocation);
  setStatus(`Using recent place: ${cachedLocation.nearest_road_node.query}`);
}

async function applyApproximateSydneyLocation(messagePrefix = "Using an approximate Sydney location") {
  const center = map.getCenter();
  const locationData = await snapLocation(center.lat, center.lng);
  locationData.nearest_road_node.query = "Approximate Sydney location";
  originInput.value = locationData.nearest_road_node.query;
  appState.userLocation = {
    lat: locationData.user_location.lat,
    lng: locationData.user_location.lng,
  };
  drawDetectedLocation(locationData);
  saveCachedLocation(locationData);
  setStatus(`${messagePrefix}: ${locationData.nearest_road_node.query}`);
}

function requestUserLocation() {
  if (appState.locationRequestStarted) {
    return;
  }
  appState.locationRequestStarted = true;

  if (!navigator.geolocation) {
    applyApproximateSydneyLocation("Geolocation is unavailable, so the app used an approximate Sydney location")
      .catch(() => setStatus("Geolocation is unavailable and no approximate Sydney fallback could be prepared."))
      .finally(() => {
        setLocationLoading(false);
        appState.locationRequestStarted = false;
      });
    return;
  }

  setLocationLoading(true);
  const cachedLocation = loadCachedLocation();
  if (isCachedLocationFresh(cachedLocation)) {
    applyCachedLocation(cachedLocation);
    setStatus(`Using saved place first: ${cachedLocation.nearest_road_node.query}. Refreshing location...`);
  } else if (!originInput.value.trim()) {
    setStatus("Trying to detect your current place...");
  }

  let finished = false;

  const finish = () => {
    if (finished) {
      return false;
    }
    finished = true;
    setLocationLoading(false);
    appState.locationRequestStarted = false;
    return true;
  };

    const deadlineId = window.setTimeout(() => {
      if (!finish()) {
        return;
      }
      const fallbackLocation = cachedLocation || loadCachedLocation();
      if (fallbackLocation) {
        applyCachedLocation(fallbackLocation);
        setStatus(`Location resolved within 15 seconds using saved place: ${fallbackLocation.nearest_road_node.query}`);
        return;
      }
      applyApproximateSydneyLocation("Location resolved within 15 seconds using an approximate Sydney fallback")
        .catch(() => setStatus("Could not prepare an approximate Sydney fallback within 15 seconds."));
    }, LOCATION_DEADLINE_MS);

  navigator.geolocation.getCurrentPosition(
    async (position) => {
      const lat = position.coords.latitude;
      const lng = position.coords.longitude;
      if (!isInsideSydney(lat, lng)) {
        if (!finish()) {
          return;
        }
        window.clearTimeout(deadlineId);
        applyApproximateSydneyLocation("Detected position was outside Sydney, so an approximate Sydney location was used")
          .catch(() => setStatus("Detected position was outside Sydney."));
        return;
      }

      appState.userLocation = { lat, lng };

      try {
        const locationData = await snapLocation(lat, lng);
        originInput.value = locationData.nearest_road_node.coordinates;
        drawDetectedLocation(locationData);
        map.flyTo([lat, lng], 13, { duration: 1.1 });
        if (!finish()) {
          return;
        }
        window.clearTimeout(deadlineId);
        setStatus("Location found. Resolving nearby place name...");

        reverseGeocode(locationData.nearest_road_node.lat, locationData.nearest_road_node.lng)
          .then((nameData) => {
            originInput.value = nameData.query;
            locationData.nearest_road_node.query = nameData.query;
            saveCachedLocation(locationData);
            drawDetectedLocation(locationData);
            setStatus(`Current place detected: ${nameData.query}`);
          })
          .catch(() => {
            saveCachedLocation(locationData);
            setStatus("Location found within 10 seconds. Using nearby coordinates because place naming was slow.");
          });
      } catch (error) {
        if (!finish()) {
          return;
        }
        window.clearTimeout(deadlineId);
        if (isCachedLocationFresh(cachedLocation)) {
          setStatus(`Live location refresh failed. Using saved place: ${cachedLocation.nearest_road_node.query}`);
        } else {
          applyApproximateSydneyLocation("Live location refresh failed, so an approximate Sydney location was used")
            .catch(() => setStatus(error.message));
        }
      }
    },
    (error) => {
      if (!finish()) {
        return;
      }
      window.clearTimeout(deadlineId);
      if (cachedLocation) {
        applyCachedLocation(cachedLocation);
        setStatus(`Live location refresh failed. Using saved place: ${cachedLocation.nearest_road_node.query}`);
      } else {
        applyApproximateSydneyLocation(
          error.code === error.PERMISSION_DENIED
            ? "Location permission was denied, so an approximate Sydney location was used"
            : "Unable to detect your current place, so an approximate Sydney location was used"
        ).catch(() => {
          setStatus(error.code === error.PERMISSION_DENIED ? "Location permission was denied." : "Unable to detect your current place.");
        });
      }
    },
    {
      enableHighAccuracy: false,
      timeout: 9000,
      maximumAge: 21600000,
    }
  );
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  exitNavigationMode();
  clearRouteArtifacts();
  clearTransferArtifacts();
  setRouteLoading(true);
  setStatus(appState.activeProfile === "drive" ? "Calculating drive routes..." : "Calculating walking route...");
  resultsContainer.innerHTML = "";

  const formData = new FormData(form);
  const origin = String(formData.get("origin") || "");
  const destination = String(formData.get("destination") || "");
  const maxWalkDistanceKm = formData.get("max_walk_distance_km");

  try {
    const data = await fetchRoute(
      origin,
      destination,
      appState.activeProfile,
      appState.activeProfile === "combined" ? Number(maxWalkDistanceKm || 0) : null
    );
    appState.pendingRoutes = data.routes;

    const bounds = [];
    data.routes.forEach((route, index) => {
      const polyline = drawRoute(route, index === 0);
      bounds.push(...polyline.getLatLngs());
    });
    drawEndpoints(data.origin, data.destination);
    if (bounds.length > 0) {
      map.fitBounds(bounds, { padding: [40, 40] });
    }

    renderRouteChoices(data);
    const baseStatus =
      appState.activeProfile === "drive"
        ? "Choose a driving option to begin live navigation."
        : appState.activeProfile === "walk"
          ? "Walking navigation is now active."
          : "Choose a route to inspect the full trip steps.";
    const warningText = Array.isArray(data.warnings) && data.warnings.length > 0 ? ` ${data.warnings.join(" ")}` : "";
    setStatus(`${baseStatus}${warningText}`);
    } catch (error) {
      resultsContainer.innerHTML = `<p class="error">${error.message}</p>`;
      clearWeatherBanner();
      setStatus("Route calculation failed.");
    } finally {
    setRouteLoading(false);
  }
});

locateButton.addEventListener("click", requestUserLocation);
bindSuggestionInput(originInput, originSuggestions, "origin");
bindSuggestionInput(destinationInput, destinationSuggestions, "destination");

modeButtons.forEach((button) => {
  button.addEventListener("click", () => setMode(button.dataset.profile));
});

document.addEventListener("click", (event) => {
  if (!originSuggestions.contains(event.target) && event.target !== originInput) {
    originSuggestions.classList.remove("is-open");
  }
  if (!destinationSuggestions.contains(event.target) && event.target !== destinationInput) {
    destinationSuggestions.classList.remove("is-open");
  }
});

async function initializeApp() {
  showAppAfterSplash();
  setStatus("Preparing Sydney traffic map...");

  try {
    const network = await fetchNetwork();
    drawNetworkOverview(network);
    const cachedLocation = loadCachedLocation();
    applyCachedLocation(cachedLocation);
    setStatus(cachedLocation ? "Sydney traffic map is ready. Recent place loaded." : "Sydney traffic map is ready. Tap 'Use my location' to refresh your place.");
  } catch (error) {
    setStatus(error.message);
  }
}

initializeApp();
