/* Aerial / street imagery panel — all free sources, built client-side.
   - Maryland iMAP 6-inch orthoimagery (2020 Western Shore, 2022 Eastern Shore)
   - USGS National Map NAIP Plus (~1 m, refreshed every 2-3 yrs) as a second view
   - Links out to Google Street View / Maps and Mapillary (no API key, no billing)
*/
const Aerial = {
    IMAP: "https://geodata.md.gov/imap/rest/services/Imagery/MD_SixInchImagery/MapServer/export",
    NAIP: "https://imagery.nationalmap.gov/arcgis/rest/services/USGSNAIPPlus/ImageServer/exportImage",

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
    streetViewUrl(lat, lon) {
        return `https://www.google.com/maps?q=&layer=c&cbll=${lat},${lon}&cbp=11,0,0,0,0`;
    },
    mapsUrl(lat, lon) { return `https://www.google.com/maps/search/?api=1&query=${lat},${lon}`; },
    mapillaryUrl(lat, lon) { return `https://www.mapillary.com/app/?lat=${lat}&lng=${lon}&z=17.5`; },
    sdatUrl() { return "https://sdat.dat.maryland.gov/RealProperty/Pages/default.aspx"; },

    /** HTML for a property's imagery block. Returns "" when no coordinates. */
    panel(p, id) {
        const lat = parseFloat(p.lat), lon = parseFloat(p.lon);
        if (!isFinite(lat) || !isFinite(lon)) {
            return `<div class="aerial-missing">No coordinates yet — the weekly title scan backfills them from SDAT.</div>`;
        }
        const uid = `aer-${id || Math.random().toString(36).slice(2)}`;
        const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
        return `
        <div class="aerial" id="${uid}" data-lat="${lat}" data-lon="${lon}">
            <div class="aerial-tabs">
                <button class="aerial-tab active" data-src="imap">MD 6-inch aerial <span class="aerial-date">2020/2022</span></button>
                <button class="aerial-tab" data-src="naip">NAIP 1-m <span class="aerial-date">newer</span></button>
                <button class="aerial-tab" data-src="wide">Wide</button>
            </div>
            <a class="aerial-img-wrap" href="${this.imapUrl(lat, lon, 45, 1200)}" target="_blank" rel="noopener" title="Open full size">
                <img class="aerial-img" src="${this.imapUrl(lat, lon)}" alt="Aerial of ${esc(p.property_address)}" loading="lazy"
                     onerror="this.parentElement.classList.add('failed')">
                <div class="aerial-fail">Imagery unavailable for this location</div>
            </a>
            <div class="aerial-links">
                <a href="${this.streetViewUrl(lat, lon)}" target="_blank" rel="noopener">Street View ↗</a>
                <a href="${this.mapsUrl(lat, lon)}" target="_blank" rel="noopener">Google Maps ↗</a>
                <a href="${this.mapillaryUrl(lat, lon)}" target="_blank" rel="noopener">Mapillary ↗</a>
                <a href="${this.sdatUrl()}" target="_blank" rel="noopener">SDAT ↗</a>
                <span class="aerial-coords">${lat.toFixed(5)}, ${lon.toFixed(5)}</span>
            </div>
            <div class="aerial-note">Imagery date matters: iMAP is 2020 (west) / 2022 (Eastern Shore). Condition may have changed.</div>
        </div>`;
    },

    /** Wire tab clicks inside a container after it has been inserted into the DOM. */
    bind(container) {
        container.querySelectorAll(".aerial:not([data-bound])").forEach((box) => {
            box.dataset.bound = "1";
            const lat = parseFloat(box.dataset.lat), lon = parseFloat(box.dataset.lon);
            const img = box.querySelector(".aerial-img");
            const wrap = box.querySelector(".aerial-img-wrap");
            box.querySelectorAll(".aerial-tab").forEach((tab) => {
                tab.addEventListener("click", (e) => {
                    e.preventDefault();
                    box.querySelectorAll(".aerial-tab").forEach((t) => t.classList.remove("active"));
                    tab.classList.add("active");
                    wrap.classList.remove("failed");
                    const src = tab.dataset.src;
                    const url = src === "naip" ? this.naipUrl(lat, lon)
                              : src === "wide" ? this.imapUrl(lat, lon, 160)
                              : this.imapUrl(lat, lon);
                    img.src = url;
                    wrap.href = src === "naip" ? this.naipUrl(lat, lon, 60, 1200) : this.imapUrl(lat, lon, src === "wide" ? 160 : 45, 1200);
                });
            });
        });
    },
};
