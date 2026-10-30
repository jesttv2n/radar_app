// Genererer den selvstændige Statamic-partial: radar.css og radar.js inline, så tv2nord.dk kun
// henter data (manifest + PNG'er) udefra. Kører som sidste trin i `npm run build`.
import { readFileSync, writeFileSync } from "node:fs";

const DATA_SRC = process.env.RADAR_DATA_SRC || "https://services.tv2nord.dk/radar/data/"; // midlertidig placering
const OUT = "statamic/resources/views/partials/components/radar.antlers.html";

const css = readFileSync("radar.css", "utf8").trim();
const js = readFileSync("radar.js", "utf8").trim();

// Et "</style" eller "</script" inde i indholdet ville lukke blokken for tidligt
for (const [name, text, tag] of [["radar.css", css, "style"], ["radar.js", js, "script"]]) {
  if (new RegExp(`</${tag}`, "i").test(text)) throw new Error(`${name} indeholder </${tag}`);
}
if (/\{\{ *\/noparse/.test(css + js)) throw new Error("indholdet lukker Antlers' noparse");

const partial = `{{# GENERERET af web/scripts/build-statamic.mjs – ret web/radar.js eller web/src/radar.css og kør "npm run build" i web/ #}}
{{ noparse }}
<style>${css}</style>
<div data-tv2nord-radar data-src="${DATA_SRC}"></div>
<script>${js}</script>
{{ /noparse }}
`;

writeFileSync(OUT, partial);
console.log(`${OUT}: ${(partial.length / 1024).toFixed(1)} KB, data fra ${DATA_SRC}`);
