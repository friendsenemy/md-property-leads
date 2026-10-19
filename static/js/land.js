/* Dirt Deals tab — raw land with development potential (Cody Bjugan's LDP play).
   Data: data/land/candidates.json, written weekly by engine2/land_watch.py.
   Every 5+ acre residential/agricultural parcel in Maryland, laid over the state's
   Septic Growth Tier map and MDP Generalized Zoning, scored for how much a homebuilder
   would want it once approved, plus new homes / builder purchases nearby and the
   seller signals the Title Scan found. Row id = SDAT account, the same id Title Leads
   uses, so notes on a parcel follow it across tabs. */
const LandApp = {
    rows: [], meta: {},
    state: { search: "", county: "", tier: "", minAcres: 0, seller: false, water: false, hideRezone: false,
             sort: "score", dir: -1, page: 1, perPage: 25 },

    TIER_LABEL: { "1": "Tier I · sewer", "2": "Tier II · sewer planned", "3": "Tier III · septic", "": "No tier map" },
    TAG_LABEL: {
        RAW_LAND: "Raw land", NEW_HOMES_NEARBY: "New homes nearby", BUILDER_BUYING_NEARBY: "Builders buying nearby",
        LONG_HELD: "Long-held", OUT_OF_STATE: "Out of state", MESSY_TITLE: "Messy title", WATERFRONT: "Waterfront",
        RECENT_REZONING: "Recently rezoned", NEEDS_REZONING: "Needs rezoning", RECENT_SALE: "Recently sold", ZONING_MISMATCH: "Zoning maps disagree",
    },
    SELLER_TAGS: ["LONG_HELD", "OUT_OF_STATE", "MESSY_TITLE"],

    async init() {
        this.bindEvents();
        try {
            const r = await fetch(`data/land/candidates.json?t=${Date.now()}`, { cache: "no-store" });
            if (!r.ok) throw new Error(r.status);
            const j = await r.json();
            this.meta = j;
            this.rows = (j.rows || []).map((x) => this.decorate(x));
        } catch { this.rows = []; }
        this.fillCounties();
        this.stats();
        this.render();
    },

    decorate(r) {
        r.id = r.account;
        r._seller = r.tags.some((t) => this.SELLER_TAGS.includes(t));
        r._lookup = this.lookupId(r);
        r._blob = [r.address, r.city, r.county, r.owner, r.owner2, r.account, r.zoning, r.gen_zone, r.tier, r._lookup, r.legal]
            .filter(Boolean).join(" ").toLowerCase();
        return r;
    },

    /* Tax Map / Grid / Parcel — how the county and Land Records find it. */
    lookupId(r) {
        const t = (s) => String(s || "").replace(/^0+(?=\w)/, "");
        const parts = [];
        if (r.tax_map) parts.push(`Map ${t(r.tax_map)}`);
        if (r.grid) parts.push(`Grid ${t(r.grid)}`);
        if (r.parcel) parts.push(`Parcel ${t(r.parcel)}`);
        if (r.lot) parts.push(`Lot ${r.lot}`);
        return parts.join(" · ");
    },

    fillCounties() {
        const cs = [...new Set(this.rows.map((r) => r.county).filter(Boolean))].sort();
        document.getElementById("landCounty").innerHTML = '<option value="">All counties</option>'
            + cs.map((c) => `<option value="${this.esc(c)}">${this.esc(c)}</option>`).join("");
    },

    stats() {
        const rs = this.rows;
        document.getElementById("lstatTotal").textContent = rs.length.toLocaleString();
        document.getElementById("lstatTier2").textContent = rs.filter((r) => r.tier_group === "2").length.toLocaleString();
        document.getElementById("lstatSeller").textContent = rs.filter((r) => r._seller).length.toLocaleString();
        document.getElementById("lstatAcres").textContent = Math.round(rs.reduce((n, r) => n + (r.acres || 0), 0)).toLocaleString();
        const m = this.meta, info = document.getElementById("landRunInfo");
        info.innerHTML = rs.length
            ? `<b>${rs.length.toLocaleString()}</b> parcels kept from <b>${(m.scanned || 0).toLocaleString()}</b> parcels of ${m.min_acres || 5}+ acres`
              + ` (${(m.qualified || 0).toLocaleString()} passed the screen; best ${rs.length.toLocaleString()} shown, up to 250 per county)`
              + `${m.generated_at ? ` · updated ${this.esc(this.fmtDT(m.generated_at))}` : ""}`
              + (m.dataset === "public" ? ` · <span style="color:var(--yellow)">owner names missing this run (public SDAT)</span>` : "")
            : "";
    },

    bindEvents() {
        let t;
        document.getElementById("landSearch").addEventListener("input", (e) => {
            clearTimeout(t); t = setTimeout(() => { this.state.search = e.target.value.trim().toLowerCase(); this.state.page = 1; this.render(); }, 200);
        });
        const on = (id, fn) => document.getElementById(id).addEventListener("change", (e) => { fn(e.target); this.state.page = 1; this.render(); });
        on("landCounty", (el) => { this.state.county = el.value; });
        on("landTier", (el) => { this.state.tier = el.value; });
        on("landAcres", (el) => { this.state.minAcres = Number(el.value) || 0; });
        on("landSeller", (el) => { this.state.seller = el.checked; });
        on("landWater", (el) => { this.state.water = el.checked; });
        on("landHideRezone", (el) => { this.state.hideRezone = el.checked; });
        document.querySelectorAll("#landTable th[data-sort]").forEach((th) => th.addEventListener("click", () => {
            const k = th.dataset.sort;
            this.state.dir = this.state.sort === k ? -this.state.dir : -1;
            this.state.sort = k; this.render();
        }));
        document.getElementById("landPrev").addEventListener("click", () => { if (this.state.page > 1) { this.state.page--; this.render(); } });
        document.getElementById("landNext").addEventListener("click", () => { this.state.page++; this.render(); });
        document.getElementById("landExport").addEventListener("click", () => this.exportCSV());
    },

    filtered() {
        const s = this.state;
        return this.rows.filter((r) =>
            (!s.county || r.county === s.county)
            && (!s.tier || (s.tier === "none" ? !r.tier_group : r.tier_group === s.tier))
            && (r.acres || 0) >= s.minAcres
            && (!s.seller || r._seller)
            && (!s.water || r.tags.includes("WATERFRONT"))
            && (!s.hideRezone || !r.tags.includes("NEEDS_REZONING"))
            && (!s.search || r._blob.includes(s.search)));
    },

    sorted(rows) {
        const k = this.state.sort, d = this.state.dir;
        const val = (r) => ({ score: r.score, acres: r.acres, lots: r.rough_lots, homes: r.new_homes_nearby, county: r.county || "" }[k]);
        return rows.slice().sort((a, b) => { const x = val(a), y = val(b); return (x > y ? 1 : x < y ? -1 : 0) * d || b.score - a.score; });
    },

    statusOf(r) { const s = SharedNotes.get(r.id); return (s && s.status) || "new"; },
    notesOf(r)  { const s = SharedNotes.get(r.id); return (s && s.notes) || ""; },
    _sharedHook: document.addEventListener("mdpl:notes-loaded", () => { try { if (LandApp.rows.length) LandApp.render(); } catch {} }),

    scoreColor(n) { return n >= 85 ? "var(--green)" : n >= 70 ? "var(--cyan)" : n >= 55 ? "var(--yellow)" : "var(--text-secondary)"; },

    chips(r) {
        return r.tags.map((t) => {
            const warn = ["NEEDS_REZONING", "RECENT_SALE", "ZONING_MISMATCH"].includes(t), sell = this.SELLER_TAGS.includes(t);
            const c = warn ? "var(--red)" : sell ? "var(--yellow)" : t === "WATERFRONT" ? "var(--cyan)" : "var(--text-dim)";
            return `<span style="display:inline-block; font-size:0.66rem; padding:1px 6px; margin:2px 3px 0 0; border:1px solid ${c}; color:${c}; border-radius:10px">${this.esc(this.TAG_LABEL[t] || t)}</span>`;
        }).join("");
    },

    render() {
        const rows = this.sorted(this.filtered());
        const pages = Math.max(1, Math.ceil(rows.length / this.state.perPage));
        if (this.state.page > pages) this.state.page = pages;
        const start = (this.state.page - 1) * this.state.perPage;
        const page = rows.slice(start, start + this.state.perPage);
        const tbody = document.getElementById("landBody"), empty = document.getElementById("landEmpty");
        if (!page.length) {
            tbody.innerHTML = ""; empty.style.display = "block";
            if (this.rows.length) {
                document.getElementById("landEmptyTitle").textContent = "No matches";
                document.getElementById("landEmptyText").textContent = "Loosen the filters or the search.";
            }
        } else {
            empty.style.display = "none";
            tbody.innerHTML = page.map((r) => {
                const st = this.statusOf(r);
                return `<tr data-id="${this.esc(r.id)}">
                    <td style="font-family:var(--font-mono); font-weight:700; font-size:1.05rem; color:${this.scoreColor(r.score)}">${r.score}</td>
                    <td class="property-cell">
                        <div class="address">${this.esc(r.address || "(no street address)")}${r.city ? `, ${this.esc(r.city)}` : ""}</div>
                        <div class="meta" style="font-family:var(--font-mono)">${this.esc(r._lookup)}${r._lookup ? " · " : ""}Acct ${this.esc(r.account)}</div>
                        <div>${this.chips(r)}</div>
                    </td>
                    <td class="name-cell" style="font-family:var(--font-mono); font-size:0.76rem">${this.esc(r.owner || "—")}${r.mail_state && r.mail_state !== "MD" ? ` <span style="color:var(--yellow)">(${this.esc(r.mail_state)})</span>` : ""}${this.notesOf(r) ? '<span class="note-badge" title="Has notes">✎</span>' : ""}</td>
                    <td class="county-cell">${this.esc(r.county)}</td>
                    <td class="value-cell" style="font-family:var(--font-mono)">${(r.acres || 0).toLocaleString(undefined, { maximumFractionDigits: 1 })}</td>
                    <td style="font-size:0.76rem">
                        <div style="color:${r.tier_group === "2" ? "var(--green)" : r.tier_group === "1" ? "var(--cyan)" : "var(--text-secondary)"}">${this.esc(this.TIER_LABEL[r.tier_group || ""])}</div>
                        <div style="color:var(--text-dim)">${this.esc(r.gen_zone || "zoning n/a")}${r.zoning ? ` · ${this.esc(r.zoning)}` : ""}</div>
                    </td>
                    <td style="font-family:var(--font-mono); font-size:0.76rem">${r.rough_lots ? "~" + r.rough_lots.toLocaleString() : "—"}</td>
                    <td style="font-size:0.74rem">${r.new_homes_nearby ? `${r.new_homes_nearby} new homes` : "—"}${r.builders_nearby && r.builders_nearby.length ? `<div style="color:var(--green)">${this.esc(r.builders_nearby.slice(0, 2).join(", "))}</div>` : ""}</td>
                    <td><span class="status-badge ${st}">${st.toUpperCase()}</span></td>
                </tr>`;
            }).join("");
            tbody.querySelectorAll("tr").forEach((tr) => tr.addEventListener("click", () => this.open(tr.dataset.id)));
        }
        const total = rows.length;
        document.getElementById("landPageInfo").textContent = total
            ? `Showing ${start + 1}–${Math.min(start + this.state.perPage, total)} of ${total.toLocaleString()} parcels` : "No parcels";
        document.getElementById("landPrev").disabled = this.state.page <= 1;
        document.getElementById("landNext").disabled = this.state.page >= pages;
    },

    row(label, value, style) {
        return `<div class="detail-row"><span class="label">${label}</span><span class="value"${style ? ` style="${style}"` : ""}>${value}</span></div>`;
    },

    open(id) {
        const r = this.rows.find((x) => x.id === id);
        if (!r) return;
        const q = encodeURIComponent(`${r.county} County Maryland zoning map`);
        const sq = encodeURIComponent(`${r.county} County Maryland water and sewer master plan`);
        const bq = encodeURIComponent(`homebuilder land acquisition ${r.county} County Maryland`);
        document.getElementById("modalBody").innerHTML = `
            <div class="detail-section">
                <h3>Why It Scored <span style="color:${this.scoreColor(r.score)}">${r.score}</span>
                    <span style="font-size:0.75rem; color:var(--text-dim); font-weight:400">· development side alone: ${r.dev_score}</span></h3>
                <ul style="margin:6px 0 0 18px; font-size:0.84rem; line-height:1.6">${r.reasons.map((x) => `<li>${this.esc(x)}</li>`).join("")}</ul>
                ${r.cautions.length ? `<div style="margin-top:10px; padding:8px 10px; border:1px solid var(--yellow); border-radius:var(--radius); font-size:0.8rem; line-height:1.5">
                    <b style="color:var(--yellow)">Check before you call</b>
                    <ul style="margin:4px 0 0 18px">${r.cautions.map((x) => `<li>${this.esc(x)}</li>`).join("")}</ul></div>` : ""}
            </div>
            <div class="detail-section">
                <h3>Look It Up</h3>
                ${this.row("Address", `${this.esc(r.address || "(no street address)")}${r.city ? `, ${this.esc(r.city)}` : ""} ${this.esc(r.zip || "")}`, "font-weight:600")}
                ${this.row("County", this.esc(r.county), "color:var(--purple)")}
                ${this.row("Tax Map / Grid / Parcel", this.esc(r._lookup || "—"), "font-family:var(--font-mono)")}
                ${this.row("SDAT Account #", this.esc(r.account), "font-family:var(--font-mono)")}
                ${r.legal ? this.row("Legal", this.esc(r.legal), "font-family:var(--font-mono); font-size:0.78rem") : ""}
                ${r.subdivision && r.subdivision !== "0000" ? this.row("Subdivision code", this.esc(r.subdivision), "font-family:var(--font-mono)") : ""}
            </div>
            <div class="detail-section">
                <h3>Development Check</h3>
                ${this.row("Acres", (r.acres || 0).toLocaleString(), "font-family:var(--font-mono)")}
                ${this.row("Septic Growth Tier", `${this.esc(r.tier || "Not on the state tier map")} <span style="color:var(--text-dim)">— ${this.esc(this.TIER_LABEL[r.tier_group || ""])}</span>`)}
                ${this.row("Zoning (county code)", this.esc(r.zoning || "—"), "font-family:var(--font-mono)")}
                ${this.row("Zoning (state category)", this.esc(r.gen_zone || "—"))}
                ${this.row("Sewer / water (SDAT)", `${this.esc(r.sewer || "—")} · ${this.esc(r.water || "—")}`)}
                ${this.row("Rough yield", r.rough_lots ? `~${r.rough_lots.toLocaleString()} homes <span style="color:var(--text-dim)">(gross guess before roads, stormwater and setbacks are engineered)</span>` : "—")}
                ${this.row("New homes within 1.5 mi", r.new_homes_nearby ? `${r.new_homes_nearby} built in the last 4 years` : "None found")}
                ${this.row("Builders buying nearby", r.builders_nearby && r.builders_nearby.length ? this.esc(r.builders_nearby.join(", ")) : "None found (needs the licensed SDAT run)")}
                ${this.row("Assessed land / buildings", `${this.money(r.land_value)} / ${this.money(r.impr_value)}`, "font-family:var(--font-mono)")}
            </div>
            <div class="detail-section">
                <h3>Seller Side</h3>
                ${this.row("Owner", this.esc(r.owner || "—") + (r.owner2 ? `<br>${this.esc(r.owner2)}` : ""), "font-family:var(--font-mono); color:var(--yellow)")}
                ${this.row("Tax bill mailed to", this.esc(r.mail || "—") + (r.mail_state && r.mail_state !== "MD" ? ` <b style="color:var(--yellow)">(out of state)</b>` : ""), "font-family:var(--font-mono); font-size:0.8rem")}
                ${this.row("Last transfer", `${this.esc(r.transfer_date || "—")}${r.sale_price ? ` · ${this.money(r.sale_price)}` : ""}`, "font-family:var(--font-mono)")}
                ${r.title_flags && r.title_flags.length ? this.row("Title Scan flags", this.esc(r.title_flags.join(", ")), "color:var(--red); font-size:0.78rem") : ""}
            </div>
            <div class="detail-section">
                <h3>How to Work It</h3>
                <ol style="margin:4px 0 0 18px; font-size:0.83rem; line-height:1.6">
                    <li><b>Confirm the growth story</b> on the county's own maps — <a href="https://www.google.com/search?q=${q}" target="_blank" rel="noopener">zoning map ↗</a>
                        and <a href="https://www.google.com/search?q=${sq}" target="_blank" rel="noopener">water &amp; sewer plan ↗</a>. Tier II means sewer is planned, not built: find the year it's scheduled.
                        Watch for school-capacity (APFO) moratoriums.</li>
                    <li><b>Ask a builder what they'd pay.</b> Call the land acquisition manager at a builder active nearby
                        (<a href="https://www.google.com/search?q=${bq}" target="_blank" rel="noopener">find them ↗</a>) with the address and acreage — they will tell you fast whether they want it.</li>
                    <li><b>Tie it up long.</b> Contract with a long due-diligence and closing period, contingent on approvals, small earnest money, and the right to assign.</li>
                    <li><b>Assign it or partner on entitlement.</b> Assign the contract to the builder for a fee, or bring in a developer to fund engineering and approvals and split the lift.</li>
                </ol>
            </div>
            <div class="detail-section">
                <h3>Imagery</h3>
                ${Aerial.panel({ ...r, property_address: r.address }, r.id)}
            </div>`;
        Aerial.bind(document.getElementById("modalBody"));
        const sel = document.getElementById("leadStatusSelect"), notes = document.getElementById("leadNotes");
        sel.value = this.statusOf(r); notes.value = this.notesOf(r);
        SharedNotes.footer(r.id);
        document.getElementById("saveLeadBtn").onclick = () => {
            SharedNotes.save("land", r.id, sel.value, notes.value, `${r.address || r._lookup}, ${r.county} — ${r.acres} ac${r.owner ? " — " + r.owner : ""}`);
            App.closeModal(); this.render();
        };
        document.getElementById("modalOverlay").classList.add("active");
    },

    exportCSV() {
        const rows = this.sorted(this.filtered());
        if (!rows.length) { alert("Nothing to export with the current filters."); return; }
        const H = ["Score", "Dev Score", "Account #", "Tax Map", "Grid", "Parcel", "Lot", "Address", "City", "Zip", "County",
            "Acres", "Tier", "Zoning Code", "Zoning Category", "Sewer", "Water", "Rough Homes", "New Homes Nearby", "Builders Nearby",
            "Owner", "Owner 2", "Mailing", "Mail State", "Last Transfer", "Sale Price", "Land Value", "Building Value",
            "Tags", "Cautions", "Lat", "Lon", "Status", "Notes"];
        const q = (v) => { const s = String(v == null ? "" : v); return /^=".*"$/.test(s) ? s : `"${s.replace(/"/g, '""')}"`; };
        const lines = [H.join(",")];
        rows.forEach((r) => lines.push([r.score, r.dev_score, `="${r.account}"`, r.tax_map, r.grid, r.parcel, r.lot, r.address, r.city, r.zip, r.county,
            r.acres, r.tier, r.zoning, r.gen_zone, r.sewer, r.water, r.rough_lots, r.new_homes_nearby, (r.builders_nearby || []).join("; "),
            r.owner, r.owner2, r.mail, r.mail_state, r.transfer_date, r.sale_price, r.land_value, r.impr_value,
            r.tags.map((t) => this.TAG_LABEL[t] || t).join("; "), r.cautions.join(" | "), r.lat, r.lon, this.statusOf(r), this.notesOf(r)].map(q).join(",")));
        const blob = new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = `md-dirt-deals-${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a); a.click(); a.remove();
        setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    },

    money(v) { const n = Number(v); return (v == null || v === "" || isNaN(n) || !n) ? "—" : "$" + Math.round(n).toLocaleString(); },
    fmtDT(s) { const d = new Date(s); return isNaN(d) ? (s || "") : d.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }); },
    esc(s) { return s == null ? "" : String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); },
};
