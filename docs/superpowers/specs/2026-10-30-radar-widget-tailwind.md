# Radar-widget: Tailwind, vanilla og dark mode

Besluttet 2026-10-30. Målet er, at komponenten kan placeres på tv2nord.dk (Statamic, Tailwind v4) og falde ind i siden.

## Krav
1. **Tailwind v4.** Widgetten styles med Tailwind-utilities. `web/radar.css` er et *genereret* build-output fra `web/src/radar.css` via Tailwind CLI (versionen pinnes i `web/package.json`). `node_modules/` gitignores, og `web/radar.css` committes, fordi serveren ikke har Node.
   - **Ingen preflight**: kun theme og utilities. Komponenten må ikke resette værtssidens styles.
   - **Prefix `rd`**: `@import "tailwindcss" prefix(rd)` eller tilsvarende uden preflight. Det giver klasser som `rd:flex` og CSS-variabler som `--rd-color-*`, så intet kolliderer med tv2nord.dk's egne Tailwind-klasser og variabler.
   - Klasser i `radar.js` skal stå som hele, statiske strenge, så Tailwind-scanneren finder dem. Ingen strengsammensætning af klassenavne.
   - Det, der ikke kan udtrykkes som utilities (range-sliderens `::-webkit-slider-*`/`::-moz-range-*` og overstyring af MapLibre's `.maplibregl-*`), skrives i en lille `@layer components`-blok i kilde-CSS'en og bruger theme-variablerne.
2. **Vanilla.** Rodelementet har ingen baggrund, ramme, afrunding eller skygge. Skrift og tekstfarve arves (`font: inherit; color: inherit`), så tv2nord.dk's `TV2`-font bruges automatisk. Komponenten har ingen titel og ingen beskrivelse. Kortet kan have en diskret afrunding som det eneste.
3. **Farver (forslag C, tv2nord.dk's egne)** som `@theme`-tokens:
   - `midnight #000523`: observeret del af slider, thumb og play-knap
   - `salmon #eb8c82`: prognosedel af slider, thumb og "Prognose"-mærke
   - `light #fff5f0`, `peach #ffe6d7`, `ink #0c0c0c`, `night #252525` efter behov
4. **Dark mode** følger tv2nord.dk: `@custom-variant dark (&:where(.dark, .dark *));`. Klassen `dark` sidder på `<html>`.
   - Observeret del, thumb og play-knap: `light #fff5f0` (midnight er usynlig på mørk baggrund). Play-ikonet: midnight.
   - Prognose: `salmon #eb8c82` i begge tilstande.
   - Sekundær tekst: `color-mix(in oklab, currentColor 70%, transparent)`.
   - **Basiskortet skifter style.** Lyst: OpenFreeMap `positron`. Mørkt: OpenFreeMap's mørke style (verificér URL'en, fx `https://tiles.openfreemap.org/styles/dark`, svarer 200 og er gyldig). Skiftet sker live, når `dark`-klassen på `<html>` ændres (MutationObserver), og radar-source/-layer skal genskabes efter `style.load`, med samme frame og position. Observeren frakobles ved teardown.
   - `data-theme="dark"` på roden understøttes ikke længere. Kun `.dark` på en forfader tæller.
5. **Selvstændig side** (`web/index.html`) beholder titel og beskrivelse. Den får samme dark-logik som tv2nord.dk: `dark` på `<html>` efter `prefers-color-scheme`, også ved live-skift. Baggrunden er lys `#fff5f0` eller mørk `#252525`, og fonten er en system-ui-fallback. Siden må gerne bruge `rd:`-utilities fra samme build.
6. **Uændret funktionalitet:** layout (højde `clamp(220px, 75cqw, var(--tv2nord-radar-max-height, 620px))` + container-type), fitBounds på Nordjylland, tidslinje, play, tilstande, refresh, teardown, `TV2NordRadar.formatTime`, `?src=` på index.html, tilgængelighed (label, aria) og ingen `innerHTML`.
7. **Statamic-snippet** (`web/statamic/radar.antlers.html`) forbliver tre linjer: CSS, div og script.

## Verifikation
- Browser (lokal server på `C:\git\radar_app-v2`, `?src=/out/live2/public/` og demo-data `out/demo/public/`): lyst og mørkt via `document.documentElement.classList.toggle('dark')`, desktop 1280 og mobil 375 (ingen vandret scroll). Radaren skal blive liggende efter live-skift af tema.
- **Indlejringstest**: en testside, der efterligner tv2nord.dk, med deres stylesheet `https://www.tv2nord.dk/build/assets/site-DWicNQhC.css` + `<html class="dark">` + komponenten. Den viser, at intet på værtssiden ændres (sammenlign computed styles på et par værts-elementer med og uden radar.css), og at komponenten arver font og farve.
- `pytest` (Python-delen er uændret) skal stadig være grøn.
