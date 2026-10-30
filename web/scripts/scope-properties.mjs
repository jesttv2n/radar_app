// Efterbehandling af Tailwind-output (web/radar.css).
// Tailwind v4 udsender en fallback for browsere uden @property (gammel Safari/Firefox):
//   @layer properties{@supports (...){*,:before,:after,::backdrop{--tw-…}}}
// Universal-selektoren ville ramme hele værtssiden. Den scopes her til komponenten.
// Bygget fejler, hvis mønsteret ikke findes præcis én gang (fx efter en Tailwind-opgradering).
import { readFileSync, writeFileSync } from "node:fs";

const file = new URL("../radar.css", import.meta.url);
const css = readFileSync(file, "utf8");
const universal = "*,:before,:after,::backdrop{";
const scoped = ".tv2nord-radar,.tv2nord-radar *,.tv2nord-radar :before,.tv2nord-radar :after{";

const hits = css.split(universal).length - 1;
if (hits !== 1) {
  console.error(`scope-properties: forventede 1 forekomst af "${universal}", fandt ${hits}`);
  process.exit(1);
}
writeFileSync(file, css.replace(universal, scoped));
console.log("scope-properties: @layer properties-fallback scoped til .tv2nord-radar");
