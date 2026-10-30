/* TV2 Nord nedbørsradar: <div data-tv2nord-radar data-src="…/data/"></div> + radar.css + radar.js
   Styling: Tailwind-utilities med prefix rd (web/src/radar.css -> web/radar.css). Klasser står som
   hele strenge, så Tailwind-scanneren finder dem. Mørk tilstand: class="dark" på en forfader. */
(function () {
  "use strict";
  if (window.TV2NordRadar) return; // radar.js indlæst to gange

  var MAPLIBRE = "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl";
  var STYLE = "https://tiles.openfreemap.org/styles/positron";
  var STYLE_DARK = "https://tiles.openfreemap.org/styles/dark";
  var DEFAULT_SRC = "https://services.tv2nord.dk/radar/data/";
  var BBOX = [7.6, 54.4, 15.4, 57.9];
  var NORDJYLLAND = [[8.15, 56.55], [11.25, 57.78]]; // startudsnit: hele regionen ved enhver bredde
  var ATTRIBUTION = "Radardata: DMI (CC BY 4.0)";
  var REFRESH_MS = 2 * 60 * 1000;
  var PLAY_MS = 700;
  var STALE_MIN = 45; // 30 min data age + DMI delay; measured from the newest observation
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

  // Tailwind-klasser (prefix rd). Roden har ingen baggrund/ramme; font og farve arves.
  var CLS = {
    root: "rd:group rd:relative rd:block rd:w-full rd:max-w-full rd:@container rd:[font:inherit] rd:text-inherit rd:text-left",
    stage: "rd:relative rd:isolate rd:h-[clamp(220px,75cqw,var(--tv2nord-radar-max-height,620px))] rd:overflow-hidden rd:rounded-md",
    map: "rd:h-full rd:w-full",
    badge: "rd:absolute rd:top-2.5 rd:left-2.5 rd:z-2 rd:hidden rd:group-[.is-forecast]:block rd:rounded-full rd:bg-salmon rd:px-2.5 rd:py-1 rd:text-xs rd:leading-normal rd:font-bold rd:tracking-[0.06em] rd:text-ink rd:uppercase rd:shadow-md",
    status: "rd:absolute rd:top-12 rd:left-1/2 rd:z-2 rd:block rd:empty:hidden rd:max-w-[calc(100%-120px)] rd:-translate-x-1/2 rd:box-border rd:rounded-lg rd:border rd:border-solid rd:border-ink/15 rd:bg-light rd:px-3.5 rd:py-2 rd:text-center rd:text-sm rd:font-semibold rd:text-ink rd:shadow-md rd:dark:border-white/20 rd:dark:bg-night rd:dark:text-white rd:data-[kind=warning]:border-salmon rd:data-[kind=error]:border-current rd:data-[kind=error]:text-red-700 rd:dark:data-[kind=error]:text-red-300",
    controls: "rd:flex rd:flex-wrap rd:items-center rd:gap-x-3 rd:gap-y-2 rd:pt-3 rd:pb-1",
    play: "rd:flex rd:h-11 rd:w-11 rd:flex-[0_0_44px] rd:cursor-pointer rd:items-center rd:justify-center rd:rounded-full rd:border-0 rd:bg-midnight rd:p-0 rd:text-white rd:transition-opacity rd:hover:enabled:opacity-85 rd:disabled:cursor-not-allowed rd:disabled:opacity-55 rd:focus-visible:outline-3 rd:focus-visible:outline-offset-2 rd:focus-visible:outline-salmon rd:dark:bg-light rd:dark:text-midnight " +
      "rd:before:ml-[3px] rd:before:border-y-8 rd:before:border-r-0 rd:before:border-l-[13px] rd:before:border-solid rd:before:border-y-transparent rd:before:border-l-current rd:before:content-[''] rd:aria-pressed:before:ml-0 rd:aria-pressed:before:h-[15px] rd:aria-pressed:before:w-3 rd:aria-pressed:before:border-x-4 rd:aria-pressed:before:border-y-0 rd:aria-pressed:before:border-current",
    sr: "rd:sr-only",
    slider: "tv2nord-radar__slider rd:m-0 rd:h-11 rd:min-w-0 rd:flex-[1_1_160px] rd:cursor-pointer rd:appearance-none rd:bg-transparent rd:focus:outline-none rd:disabled:cursor-not-allowed rd:disabled:opacity-55",
    time: "rd:flex-[1_0_100%] rd:font-semibold rd:tabular-nums rd:group-[.is-forecast]:before:text-salmon rd:group-[.is-forecast]:before:content-['Prognose_·_']",
    footer: "rd:flex rd:flex-wrap rd:items-end rd:justify-between rd:gap-x-4 rd:gap-y-2 rd:pt-1 rd:pb-3 rd:text-xs rd:text-current/70",
    legend: "rd:min-w-40 rd:flex-[0_1_220px]",
    legendBar: "rd:h-2 rd:rounded-full",
    legendWords: "rd:mt-0.5 rd:flex rd:justify-between"
  };

  var count = 0;

  function mount(root) {
    if (root.__tv2nordRadar) return;
    root.__tv2nordRadar = true;
    var id = "tv2nord-radar-" + (++count);
    var src = root.getAttribute("data-src") || DEFAULT_SRC;
    if (src.slice(-1) !== "/") src += "/";
    var base = new URL(src, location.href);

    root.classList.add("tv2nord-radar");
    CLS.root.split(" ").forEach(function (c) { root.classList.add(c); });
    root.setAttribute("role", "region");
    root.setAttribute("aria-label", "Nedbørsradar");

    var stage = el("div", CLS.stage, root);
    var mapEl = el("div", CLS.map, stage);
    mapEl.id = id + "-map";
    var badge = el("div", CLS.badge, stage);
    badge.textContent = "Prognose";
    var status = el("div", CLS.status, stage);
    status.id = id + "-status";
    status.setAttribute("role", "status");
    status.setAttribute("aria-live", "polite");

    var controls = el("div", CLS.controls, root);
    var play = el("button", CLS.play, controls);
    play.type = "button";
    play.id = id + "-play";
    play.disabled = true;
    var label = el("label", CLS.sr, controls);
    label.htmlFor = id + "-slider";
    label.textContent = "Tidspunkt";
    var slider = el("input", CLS.slider, controls);
    slider.type = "range";
    slider.id = id + "-slider";
    slider.min = 0;
    slider.max = 0;
    slider.step = 1;
    slider.disabled = true;
    var time = el("div", CLS.time, controls);
    time.id = id + "-time";
    time.setAttribute("aria-hidden", "true");

    var footer = el("div", CLS.footer, root);
    var legend = el("div", CLS.legend, footer);
    legend.setAttribute("role", "img");
    legend.setAttribute("aria-label", "Nedbør: let, moderat, kraftig");
    var bar = el("div", CLS.legendBar, legend);
    bar.style.background = "linear-gradient(to right, " + LEGEND.join(", ") + ")";
    var words = el("div", CLS.legendWords, legend);
    ["let", "moderat", "kraftig"].forEach(function (w) { el("span", "", words).textContent = w; });
    el("div", "", footer).textContent = ATTRIBUTION;

    var manifest = null, idx = 0, timer = null, map = null, mapReady = false, preloaded = {};
    var fetchFailed = false, mapFailed = false, mapLoading = false, refreshTimer = null;
    var styleReady = false, dark = isDark();

    // Mørk tilstand følger class="dark" på en forfader (tv2nord.dk sætter den på <html>).
    function isDark() { return !!root.closest(".dark"); }
    function styleUrl() { return dark ? STYLE_DARK : STYLE; }
    var themeObserver = new MutationObserver(function () {
      var d = isDark();
      if (d === dark) return;
      dark = d;
      if (map) {
        styleReady = false; // radar-source/-layer genskabes på style.load
        map.setStyle(styleUrl(), { diff: false });
      }
    });
    themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });

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
    }

    function setStatus(text, kind) {
      status.textContent = text || "";
      status.setAttribute("data-kind", kind); // tom tekst skjules af rd:empty:hidden
    }

    function updateStatus() {
      if (fetchFailed || mapFailed) return setStatus("Radaren er ikke tilgængelig lige nu", "error");
      if (!manifest) return setStatus("", "info");
      var newest = manifest.frames.filter(function (f) { return f.lead === 0; })[0] || manifest.frames[0];
      var age = Math.round((Date.now() - Date.parse(newest.time)) / 60000);
      if (age > STALE_MIN) return setStatus("Data er " + age + " min gamle", "warning");
      var dry = manifest.frames.every(function (f) { return !(f.max_dbz >= RAIN_DBZ); });
      setStatus(dry ? "Ingen nedbør i området" : "", "info");
    }

    function drawImage() {
      if (!mapReady || !styleReady || !manifest) return;
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
      styleReady = false;
      updateStatus();
    }

    function initMap() {
      if (map || mapLoading) return;
      mapLoading = true;
      loadMaplibre().then(function (maplibregl) {
        if (!root.isConnected) { mapLoading = false; return; }
        map = new maplibregl.Map({
          container: mapEl,
          style: styleUrl(),
          bounds: NORDJYLLAND,
          fitBoundsOptions: { padding: 8 },
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
        map.on("style.load", function () {
          styleReady = true;
          drawImage(); // også efter skift af basiskort: samme frame igen
        });
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
        themeObserver.disconnect();
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
