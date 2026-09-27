/* Aerial / street imagery panel — all free sources, built client-side.
   Sources, tried in order until one loads:
   - Esri World Imagery tiles (usually the newest; free basemap, attribution required)
   - Maryland iMAP 6-inch orthoimagery (2020 Western Shore / 2022 Eastern Shore)
   - USGS National Map NAIP Plus (~1 m)
   Links out to Google Street View / Maps and Mapillary (no API key, no billing).
*/
const Aerial = {
    IMAP: "https://geodata.md.gov/imap/rest/services/Imagery/MD_SixInchImagery/MapServer/export",
    NAIP: "https://imagery.nationalmap.gov/arcgis/rest/services/USGSNAIPPlus/ImageServer/exportImage",
    ESRI: "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile",

    SOURCES: [
        { id: "esri", label: "Satellite", sub: "newest", kind: "tiles" },
        { id: "imap", label: "MD 6-inch aerial", sub: "2020/2022", kind: "img" },
        { id: "naip", label: "NAIP 1-m", sub: "USGS", kind: "img" },
        { id: "wide", label: "Wide", sub: "", kind: "tiles" },
    ],

    bbox(lat, lon, meters) {
        const dLat = meters / 111320;
        const dLon = meters / (111320 * Math.cos(lat * Math.PI / 180));
        return `${lon - dLon},${lat - dLat},${lon + dLon},${lat + dLat}`;
    },
    imapUrl(lat, lon, meters = 45, size = 640) {
        return `${this.IMAP}?bbox=${this.bbox(lat, lon, meters)}&bboxSR=4326&imageSR=3857&size=${size},${size}&format=jpg&transparent=false&f=image`;
    },
    naipUrl(lat, lon, meters = 60, size = 640) {
        return `${this.NAIP}?bbox=${this.bbox(lat, lon, meters)}&bboxSR=4326&imageSR=3857&size=${size},${size}&format=jpg&f=image`;
    },
    streetViewUrl(lat, lon) { return `https://www.google.com/maps?q=&layer=c&cbll=${lat},${lon}&cbp=11,0,0,0,0`; },
    mapsUrl(lat, lon) { return `https://www.google.com/maps/search/?api=1&query=${lat},${lon}`; },
    satelliteUrl(lat, lon) { return `https://www.google.com/maps/@${lat},${lon},19z/data=!3m1!1e3`; },
    mapillaryUrl(lat, lon) { return `https://www.mapillary.com/app/?lat=${lat}&lng=${lon}&z=17.5`; },
    sdatUrl() { return "https://sdat.dat.maryland.gov/RealProperty/Pages/default.aspx"; },

    /* ── Slippy-tile grid (Esri) ───────────────────────────────────────── */
    tileXY(lat, lon, z) {
        const n = 2 ** z;
        const x = (lon + 180) / 360 * n;
        const latR = lat * Math.PI / 180;
        const y = (1 - Math.log(Math.tan(latR) + 1 / Math.cos(latR)) / Math.PI) / 2 * n;
        return { x, y };
    },
    /** Tile mosaic centred on the point, exactly covering a px×px square. */
    tileGridHtml(lat, lon, z, px) {
        const { x, y } = this.tileXY(lat, lon, z);
        const T = 256, half = px / 2 / T;
        let tiles = "";
        for (let ty = Math.floor(y - half); ty <= Math.floor(y + half); ty++) {
            for (let tx = Math.floor(x - half); tx <= Math.floor(x + half); tx++) {
                const left = (tx - x) * T + px / 2, top = (ty - y) * T + px / 2;
                tiles += `<img src="${this.ESRI}/${z}/${ty}/${tx}" style="position:absolute;left:${left.toFixed(1)}px;top:${top.toFixed(1)}px;width:${T}px;height:${T}px" alt="" loading="lazy">`;
            }
        }
        return `<div class="tile-grid" style="position:relative;width:${px}px;height:${px}px;overflow:hidden">${tiles}
                <div class="tile-attrib">Esri, Maxar, Earthstar Geographics</div></div>`;
    },

    /** HTML for a property's imagery block. */
    panel(p, id) {
        const lat = parseFloat(p.lat), lon = parseFloat(p.lon);
        if (!isFinite(lat) || !isFinite(lon)) {
            return `<div class="aerial-missing">No coordinates yet — the weekly title scan backfills them from SDAT.</div>`;
        }
        const uid = `aer-${id || Math.random().toString(36).slice(2)}`;
        const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
        return `
        <div class="aerial" id="${uid}" data-lat="${lat}" data-lon="${lon}" data-addr="${esc(p.property_address)}">
            <div class="aerial-tabs">
                ${this.SOURCES.map((s, i) => `<button class="aerial-tab${i === 0 ? " active" : ""}" data-src="${s.id}">${s.label}${s.sub ? ` <span class="aerial-date">${s.sub}</span>` : ""}</button>`).join("")}
            </div>
            <a class="aerial-img-wrap" href="${this.satelliteUrl(lat, lon)}" target="_blank" rel="noopener" title="Open in Google Maps satellite">
                <div class="aerial-stage"></div>
                <div class="aerial-fail">Imagery unavailable from all free sources right now — use the links below.</div>
            </a>
            <div class="aerial-links">
                <a href="${this.streetViewUrl(lat, lon)}" target="_blank" rel="noopener">Street View ↗</a>
                <a href="${this.satelliteUrl(lat, lon)}" target="_blank" rel="noopener">Google Satellite ↗</a>
                <a href="${this.mapsUrl(lat, lon)}" target="_blank" rel="noopener">Google Maps ↗</a>
                <a href="${this.mapillaryUrl(lat, lon)}" target="_blank" rel="noopener">Mapillary ↗</a>
                <a href="${this.sdatUrl()}" target="_blank" rel="noopener">SDAT ↗</a>
                <span class="aerial-coords">${lat.toFixed(5)}, ${lon.toFixed(5)}</span>
            </div>
            <div class="aerial-note">Imagery dates vary by source (iMAP: 2020 west / 2022 Eastern Shore). Condition may have changed since capture.</div>
        </div>`;
    },

    /** Render one source into the stage; on failure fall through to the next. */
    show(box, srcId, autoFallback) {
        const lat = parseFloat(box.dataset.lat), lon = parseFloat(box.dataset.lon);
        const stage = box.querySelector(".aerial-stage");
        const wrap = box.querySelector(".aerial-img-wrap");
        const px = Math.min(640, Math.max(280, Math.floor(wrap.clientWidth || 640)));
        wrap.classList.remove("failed");
        box.querySelectorAll(".aerial-tab").forEach((t) => t.classList.toggle("active", t.dataset.src === srcId));
        const order = this.SOURCES.map((s) => s.id);
        const next = () => {
            if (!autoFallback) { wrap.classList.add("failed"); return; }
            const i = order.indexOf(srcId);
            const nxt = order.slice(i + 1).find((s) => s !== "wide");
            if (nxt) this.show(box, nxt, true); else wrap.classList.add("failed");
        };
        if (srcId === "esri" || srcId === "wide") {
            stage.innerHTML = this.tileGridHtml(lat, lon, srcId === "wide" ? 17 : 19, px);
            const imgs = stage.querySelectorAll("img");
            let failed = 0, loaded = 0;
            imgs.forEach((im) => {
                im.onerror = () => { failed++; if (failed === imgs.length) next(); };
                im.onload = () => { loaded++; };
            });
            return;
        }
        const url = srcId === "naip" ? this.naipUrl(lat, lon, 60, px) : this.imapUrl(lat, lon, 45, px);
        stage.innerHTML = `<img class="aerial-img" src="${url}" alt="Aerial of ${box.dataset.addr}" loading="lazy">`;
        const img = stage.querySelector("img");
        img.onerror = next;
        img.onload = () => {
            // ArcGIS sometimes returns a tiny blank image instead of an error
            if (img.naturalWidth < 50) next();
        };
    },

    /** Wire tabs inside a container after insertion; starts the fallback chain. */
    bind(container) {
        container.querySelectorAll(".aerial:not([data-bound])").forEach((box) => {
            box.dataset.bound = "1";
            box.querySelectorAll(".aerial-tab").forEach((tab) => {
                tab.addEventListener("click", (e) => { e.preventDefault(); this.show(box, tab.dataset.src, false); });
            });
            this.show(box, "esri", true);
        });
    },
};
