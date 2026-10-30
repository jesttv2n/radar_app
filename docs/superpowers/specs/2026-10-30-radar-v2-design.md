# Nedbørsradar v2 – design

Besluttet 2026-10-30 på baggrund af spiken i `spike/` (se `spike/run_spike.py`, `spike/check_georef.py`).

## Formål
Et offentligt, interaktivt nedbørskort til seere på tv2nord.dk. Det viser de seneste 2 timers observeret radar plus en prognose 60 min frem. Kortet er indlejret som Statamic-widget og findes også som selvstændig side på `https://services.tv2nord.dk/radar/`.

## Beslutninger
- **Område:** hele Danmark (lon 7.6–15.4, lat 54.4–57.9). Kortet åbner zoomet ind på Nordjylland (center 9.9, 57.0, zoom 7.2), og man kan zoome ud til hele landet.
- **Data:** DMI composite fra `https://opendataapi.dmi.dk/v1/radardata` (ingen nøgle, CC BY 4.0). Kun `scanType=fullRange` (10-min-takt), fordi blanding med doppler giver zigzag i bevægelse og score.
- **Geometri:** composite-hjørnerne i `where` er pixelcentre. Det giver Affine(500, 0, x_UL − 250, 0, −500, y_UL + 250) i `projdef`. Data reprojiceres til EPSG:3857 og vises som MapLibre-image-source. API'ets `bbox` bruges aldrig.
- **Prognose:** pysteps S-PROG, bevægelse fra Lucas-Kanade på de 3 seneste fullRange-billeder, 6 leads à 10 min. Spiken målte CSI(≥1 mm/t) 0,34–0,57 ved +60 min, mod 0,11–0,37 for et frosset billede.
- **Kvalitet løbende:** hver kørsel scorer prognosen fra for 60 min siden mod nuværende observation og logger resultatet.
- **Drift:** Python-worker som systemd-timer på services.tv2nord.dk (ingen Docker på serveren, ingen backend-port). Nginx serverer web og data statisk og offentligt med CORS. pysteps bygges som wheel lokalt i Docker (`python:3.12-slim`, glibc 2.36 ≤ serverens 2.39).
- **Basiskort:** OpenFreeMap positron (gratis, ingen nøgle, tilladt i produktion). OSM-tiles er ikke tilladt til offentlig produktionstrafik, og CARTO kræver nøgle.
- **UI:** TV2 Nord-designsystemet (`tv2nord-design`-skill). Tider vises i Europe/Copenhagen. DMI krediteres.

## Udenfor scope (v1)
Push-varsler, mm-akkumulering, ensemble/usikkerhedskort, video-/TV-grafik-eksport, prognoser ud over 60 min.
