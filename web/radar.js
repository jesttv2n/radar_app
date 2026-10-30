/* TV2 Nord nedbørsradar: <div data-tv2nord-radar data-src="…/data/"></div> + radar.css + radar.js */
(function () {
  "use strict";
  if (window.TV2NordRadar) return; // radar.js indlæst to gange

  var MAPLIBRE = "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl";
  var STYLE = "https://tiles.openfreemap.org/styles/positron";
  var DEFAULT_SRC = "https://services.tv2nord.dk/radar/data/";
  var BBOX = [7.6, 54.4, 15.4, 57.9];
  var ATTRIBUTION = "Radardata: DMI (CC BY 4.0)";
  var REFRESH_MS = 2 * 60 * 1000;
  var PLAY_MS = 700;
  var STALE_MIN = 30;
  var RAIN_DBZ = 10;
  var LEGEND = ["#a6e3ff", "#5ab4f0", "#2a7fd4", "#1f4fa8", "#2fae3c", "#f2e030", "#f5a623", "#e8452c", "#b81f5b", "#7a1fa2"];

  var FMT = new Intl.DateTimeFormat("da-DK", {
    timeZone: "Europe/Copenhagen", day: "numeric", month: "short",
    hour: "2-digit", minute: "2-digit", hourCycle: "h23"
  });

  // "25. okt. 02.30" i dansk tid
  function formatTime(iso) {
    var p = {};
    FMT.formatToParts(new Date(iso)).forEach(function (x) { p[x.type] = x.value; });
    return p.day + ". " + p.month + " " + p.hour + "." + p.minute;
  }

  function relative(lead) {
    if (lead === 0) return "nu";
    return (lead > 0 ? "+" : "−") + Math.abs(lead) + " min";
  }

  var maplibreReady, maplibreCss;
  function loadMaplibre() {
    if (window.maplibregl) return Promise.resolve(window.maplibregl);
    if (!maplibreCss) {
      maplibreCss = document.createElement("link");
      maplibreCss.rel = "stylesheet";
      maplibreCss.href = MAPLIBRE + ".css";
      document.head.appendChild(maplibreCss);
    }
    if (!maplibreReady) {
      maplibreReady = new Promise(function (resolve, reject) {
        var js = document.createElement("script");
        js.src = MAPLIBRE + ".js";
        js.onload = function () { resolve(window.maplibregl); };
        js.onerror = function (e) {
          maplibreReady = null; // næste forsøg henter igen
          js.remove();
          reject(e);
        };
        document.head.appendChild(js);
      });
    }
    return maplibreReady;
  }

  function el(tag, cls, parent) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (parent) parent.appendChild(e);
    return e;
  }

  var count = 0;

  function mount(root) {
    if (root.__tv2nordRadar) return;
    root.__tv2nordRadar = true;
    var id = "tv2nord-radar-" + (++count);
    var src = root.getAttribute("data-src") || DEFAULT_SRC;
    if (src.slice(-1) !== "/") src += "/";
    var base = new URL(src, location.href);

    root.classList.add("tv2nord-radar");
    root.setAttribute("role", "region");
    root.setAttribute("aria-label", "Nedbørsradar");

    var stage = el("div", "tv2nord-radar__stage", root);
    var mapEl = el("div", "tv2nord-radar__map", stage);
    mapEl.id = id + "-map";
    var badge = el("div", "tv2nord-radar__badge", stage);
    badge.textContent = "Prognose";
    var status = el("div", "tv2nord-radar__status", stage);
    status.id = id + "-status";
    status.setAttribute("role", "status");
    status.setAttribute("aria-live", "polite");

    var controls = el("div", "tv2nord-radar__controls", root);
    var play = el("button", "tv2nord-radar__play", controls);
    play.type = "button";
    play.id = id + "-play";
    play.disabled = true;
    var label = el("label", "tv2nord-radar__sr", controls);
    label.htmlFor = id + "-slider";
    label.textContent = "Tidspunkt";
    var slider = el("input", "tv2nord-radar__slider", controls);
    slider.type = "range";
    slider.id = id + "-slider";
    slider.min = 0;
    slider.max = 0;
    slider.step = 1;
    slider.disabled = true;
    var time = el("div", "tv2nord-radar__time", controls);
    time.id = id + "-time";
    time.setAttribute("aria-hidden", "true");

    var footer = el("div", "tv2nord-radar__footer", root);
    var legend = el("div", "tv2nord-radar__legend", footer);
    legend.setAttribute("role", "img");
    legend.setAttribute("aria-label", "Nedbør: let, moderat, kraftig");
    var bar = el("div", "tv2nord-radar__legend-bar", legend);
    bar.style.background = "linear-gradient(to right, " + LEGEND.join(", ") + ")";
    var words = el("div", "tv2nord-radar__legend-words", legend);
    ["let", "moderat", "kraftig"].forEach(function (w) { el("span", "", words).textContent = w; });
    el("div", "tv2nord-radar__attribution", footer).textContent = ATTRIBUTION;

    var manifest = null, idx = 0, timer = null, map = null, mapReady = false, preloaded = {};
    var fetchFailed = false, mapFailed = false, mapLoading = false, refreshTimer = null;

    function frameUrl(f) { return new URL(f.url, base).href; }

    function setPlaying(on) {
      if (on && !timer) {
        timer = setInterval(function () {
          if (!root.isConnected) return tick();
          show((idx + 1) % manifest.frames.length);
        }, PLAY_MS);
      } else if (!on && timer) {
        clearInterval(timer);
        timer = null;
      }
      play.setAttribute("aria-label", timer ? "Pause" : "Afspil");
      play.setAttribute("aria-pressed", timer ? "true" : "false");
      play.classList.toggle("is-playing", !!timer);
    }

    function setStatus(text, kind) {
      status.textContent = text || "";
      status.className = "tv2nord-radar__status" + (text ? " is-visible is-" + kind : "");
    }

    function updateStatus() {
      if (fetchFailed || mapFailed) return setStatus("Radaren er ikke tilgængelig lige nu", "error");
      if (!manifest) return setStatus("", "info");
      var age = Math.round((Date.now() - Date.parse(manifest.generated)) / 60000);
      if (age > STALE_MIN) return setStatus("Data er " + age + " min gamle", "warning");
      var dry = manifest.frames.every(function (f) { return !(f.max_dbz >= RAIN_DBZ); });
      setStatus(dry ? "Ingen nedbør i området" : "", "info");
    }

    function drawImage() {
      if (!mapReady || !manifest) return;
      var f = manifest.frames[idx];
      var source = map.getSource("tv2nord-radar");
      if (source) {
        source.updateImage({ url: frameUrl(f), coordinates: manifest.coordinates });
        return;
      }
      map.addSource("tv2nord-radar", { type: "image", url: frameUrl(f), coordinates: manifest.coordinates });
      var labels = map.getStyle().layers.filter(function (l) { return l.type === "symbol"; })[0];
      map.addLayer({
        id: "tv2nord-radar", type: "raster", source: "tv2nord-radar",
        paint: { "raster-opacity": 0.75, "raster-fade-duration": 0 }
      }, labels && labels.id);
    }

    function show(i) {
      idx = i;
      var f = manifest.frames[i];
      var text = formatTime(f.time) + " · " + relative(f.lead);
      slider.value = i;
      slider.setAttribute("aria-valuetext", (f.kind === "forecast" ? "Prognose " : "") + text);
      time.textContent = text;
      root.classList.toggle("is-forecast", f.kind === "forecast");
      drawImage();
    }

    function nowIndex(frames) {
      for (var i = frames.length - 1; i >= 0; i--) if (frames[i].lead === 0) return i;
      return frames.length - 1;
    }

    function apply(m) {
      var prev = manifest && manifest.frames[idx];
      var atNow = !prev || prev.lead === 0;
      manifest = m;
      var keepImgs = {};  // preload alle frames; glem dem, manifestet ikke længere nævner
      m.frames.forEach(function (f) {
        var u = frameUrl(f);
        if (!preloaded[u]) { preloaded[u] = new Image(); preloaded[u].src = u; }
        keepImgs[u] = preloaded[u];
      });
      preloaded = keepImgs;
      var n = m.frames.length;
      slider.max = n - 1;
      slider.disabled = play.disabled = false;
      var split = n > 1 ? (nowIndex(m.frames) + 0.5) / (n - 1) * 100 : 100;
      root.style.setProperty("--tv2nord-radar-split", Math.min(split, 100) + "%");
      // Står man på "nu", følger man med til det nye "nu"; ellers bevares tidspunktet hvis det findes.
      var keep = atNow ? -1 : m.frames.findIndex(function (f) { return f.time === prev.time; });
      show(keep >= 0 ? keep : nowIndex(m.frames));
      fetchFailed = false;
      updateStatus();
    }

    function refresh() {
      return fetch(new URL("manifest.json", base).href, { cache: "no-cache" })
        .then(function (r) {
          if (!r.ok) throw new Error("HTTP " + r.status);
          return r.json();
        })
        .then(function (m) {
          if (!m.frames || !m.frames.length) throw new Error("no frames");
          apply(m);
        })
        .catch(function () { fetchFailed = true; updateStatus(); });
    }

    play.addEventListener("click", function () { if (manifest) setPlaying(!timer); });
    slider.addEventListener("input", function () {
      setPlaying(false);
      show(+slider.value);
    });
    setPlaying(false);

    function mapFail() {
      mapLoading = false;
      mapFailed = true;
      if (map) { map.remove(); map = null; }
      updateStatus();
    }

    function initMap() {
      if (map || mapLoading) return;
      mapLoading = true;
      loadMaplibre().then(function (maplibregl) {
        if (!root.isConnected) { mapLoading = false; return; }
        map = new maplibregl.Map({
          container: mapEl,
          style: STYLE,
          center: [9.9, 57.0],
          zoom: 7.2,
          minZoom: 5.5,
          maxBounds: [[BBOX[0] - 1, BBOX[1] - 1], [BBOX[2] + 1, BBOX[3] + 1]],
          cooperativeGestures: root.getAttribute("data-cooperative") !== "false",
          attributionControl: { compact: true },
          locale: {
            "CooperativeGesturesHandler.WindowsHelpText": "Hold Ctrl nede og scroll for at zoome",
            "CooperativeGesturesHandler.MacHelpText": "Hold ⌘ nede og scroll for at zoome",
            "CooperativeGesturesHandler.MobileHelpText": "Brug to fingre til at flytte kortet",
            "NavigationControl.ZoomIn": "Zoom ind",
            "NavigationControl.ZoomOut": "Zoom ud"
          }
        });
        map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
        map.on("load", function () {
          mapLoading = false;
          mapReady = true;
          mapFailed = false;
          drawImage();
          updateStatus();
        });
        // Style-/tileserver nede før første load: vis fejl og prøv igen ved næste tick.
        map.on("error", function (e) { if (!mapReady && !e.sourceId) mapFail(); });
      }).catch(mapFail);
    }

    // Tick hvert 2. min: ryd op, hvis widgetten er fjernet fra DOM'en; spring over i skjult fane.
    function tick() {
      if (!root.isConnected) {
        clearInterval(refreshTimer);
        setPlaying(false);
        if (map) map.remove();
        map = null;
        return;
      }
      if (document.hidden) return;
      initMap();
      refresh();
    }

    initMap();
    refresh();
    refreshTimer = setInterval(tick, REFRESH_MS);
  }

  function mountAll() {
    Array.prototype.forEach.call(document.querySelectorAll("[data-tv2nord-radar]"), mount);
  }

  window.TV2NordRadar = { formatTime: formatTime, mount: mount };
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mountAll);
  else mountAll();
})();
