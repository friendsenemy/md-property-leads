/* Title Leads tab — Engine 2 (property-first, statewide).
   Reads data/title/summary.json, then one county shard or top.json at a time.
   Status + notes live in localStorage under their own key, same as engine 1. */
const TITLE_LS_KEY = "md_title_leads_local_v1";

const TitleApp = {
    summary: null,
    rows: [],
    local: {},
    state: { scope: "top", preset: "", search: "", minPriority: 0, status: "all", sort: "priority", dir: -1, page: 1, perPage: 25 },

    async init() {
        this.loadLocal();
        this.bindEvents();
        await this.loadSummary();
        await this.loadScope();
    },

    async loadSummary() {
        try {
            const r = await fetch(`data/title/summary.json?t=${Date.now()}`, { cache: "no-store" });
            if (!r.ok) throw new Error(r.status);
            this.summary = await r.json();
        } catch {
            this.summary = null;
            document.getElementById("titleEmptyTitle").textContent = "No title scan yet";
            document.getElementById("titleEmptyText").textContent =
                "Run the “Title Scan (Engine 2)” workflow on GitHub once. It pulls all Maryland parcels and takes about an hour the first time.";
            document.getElementById("titleEmpty").style.display = "block";
            return;
        }
        const sel = document.getElementById("titleScope");
        const counties = Object.entries(this.summary.counties || {}).sort((a, b) => a[1].county.localeCompare(b[1].county));
        sel.innerHTML = `<option value="top">All Maryland — top ${this.summary.config.per_county_limit ? "" : ""}priority</option>` +
            counties.map(([slug, c]) => `<option value="${slug}">${this.esc(c.county)} (${c.total_candidates.toLocaleString()})</option>`).join("");
        const presets = document.getElementById("titlePresets");
        presets.innerHTML = `<button class="filter-btn active" data-preset="">All</button>` +
            (this.summary.presets || []).map((p) => `<button class="filter-btn" data-preset="${p.id}">${this.esc(p.label)}</button>`).join("");
        presets.querySelectorAll(".filter-btn").forEach((b) => b.addEventListener("click", () => {
            presets.querySelectorAll(".filter-btn").forEach((x) => x.classList.remove("active"));
            b.classList.add("active"); this.state.preset = b.dataset.preset; this.state.page = 1; this.render();
        }));
        const s = this.summary;
        document.getElementById("titleRunInfo").innerHTML =
            `Scan: ${this.fmtDT(s.generated_at)} · ${Number(s.index_rows || 0).toLocaleString()} parcels indexed · ${Number(s.candidates || 0).toLocaleString()} candidates`;
        document.getElementById("tstatCandidates").textContent = Number(s.candidates || 0).toLocaleString();
        document.getElementById("tstatEstate").textContent = Number((s.classes || {}).E1 || 0 + 0).toLocaleString();
        const est = ["E1", "E2", "E3", "E4", "E5"].reduce((n, k) => n + ((s.classes || {})[k] || 0), 0);
        document.getElementById("tstatEstate").textContent = est.toLocaleString();
        document.getElementById("tstatCounties").textContent = Object.keys(s.counties || {}).length;
    },

    async loadScope() {
        if (!this.summary) return;
        const file = this.state.scope === "top" ? "top.json" : `${this.state.scope}.json`;
        try {
            const r = await fetch(`data/title/${file}?t=${Date.now()}`, { cache: "no-store" });
            const j = await r.json();
            this.rows = (j.rows || []).map((x) => this.decorate(x));
        } catch { this.rows = []; }
        this.state.page = 1;
        this.render();
    },

    decorate(r) {
        const p = r.property || {};
        return Object.assign(r, {
            _priority: r.scores.priority,
            _equity: p.estimated_equity != null ? parseFloat(p.estimated_equity) : null,
            _assessed: parseFloat(p.assessed_value) || 0,
            _years: p.years_since_transfer || 0,
            _search: `${p.owner_name} ${p.owner_name_2} ${p.property_address} ${p.city} ${p.zip_code} ${p.county} ${r.title_class} ${(r.flags || []).join(" ")}`.toLowerCase(),
        });
    },

    // ─── Local status/notes ───
    loadLocal() { try { this.local = JSON.parse(localStorage.getItem(TITLE_LS_KEY) || "{}"); } catch { this.local = {}; } },
    saveLocal() { try { localStorage.setItem(TITLE_LS_KEY, JSON.stringify(this.local)); } catch {} },
    statusOf(r) { return (this.local[r.id] && this.local[r.id].status) || "new"; },
    notesOf(r) { return (this.local[r.id] && this.local[r.id].notes) || ""; },

    bindEvents() {
        document.getElementById("titleScope").addEventListener("change", (e) => { this.state.scope = e.target.value; this.loadScope(); });
        let t;
        document.getElementById("titleSearch").addEventListener("input", (e) => {
            clearTimeout(t); t = setTimeout(() => { this.state.search = e.target.value.trim().toLowerCase(); this.state.page = 1; this.render(); }, 200);
        });
        const slider = document.getElementById("titleMinPriority");
        slider.addEventListener("input", (e) => {
            this.state.minPriority = parseInt(e.target.value, 10) || 0;
            document.getElementById("titleMinPriorityVal").textContent = this.state.minPriority;
            this.state.page = 1; this.render();
        });
        document.querySelectorAll("#titleStatusFilters .filter-btn").forEach((b) => b.addEventListener("click", () => {
            document.querySelectorAll("#titleStatusFilters .filter-btn").forEach((x) => x.classList.remove("active"));
            b.classList.add("active"); this.state.status = b.dataset.status; this.state.page = 1; this.render();
        }));
        document.querySelectorAll("#titleTable th[data-sort]").forEach((th) => th.addEventListener("click", () => {
            const k = th.dataset.sort;
            if (this.state.sort === k) this.state.dir *= -1; else { this.state.sort = k; this.state.dir = -1; }
            this.render();
        }));
        document.getElementById("titlePrev").addEventListener("click", () => { if (this.state.page > 1) { this.state.page--; this.render(); } });
        document.getElementById("titleNext").addEventListener("click", () => { this.state.page++; this.render(); });
        document.getElementById("titleExport").addEventListener("click", () => this.exportCSV());
    },

    presetMatch(r, preset) {
        if (!preset) return true;
        const spec = (this.summary.presets || []).find((p) => p.id === preset);
        if (!spec) return true;
        const flags = new Set(r.flags || []);
        if (spec.flags && !spec.flags.every((f) => flags.has(f))) return false;
        if (spec.any_flags && !spec.any_flags.some((f) => flags.has(f))) return false;
        if (spec.min_years_since_transfer && r._years < spec.min_years_since_transfer) return false;
        if (spec.min_equity && !(r._equity >= spec.min_equity)) return false;
        return true;
    },

    filtered() {
        const s = this.state;
        return this.rows.filter((r) =>
            r._priority >= s.minPriority &&
            this.presetMatch(r, s.preset) &&
            (s.status === "all" || this.statusOf(r) === s.status) &&
            (!s.search || r._search.includes(s.search)));
    },

    sorted(rows) {
        const k = this.state.sort, d = this.state.dir;
        const val = (r) => ({
            priority: r._priority, title: r.scores.title_complexity, equity: r._equity ?? -1, assessed: r._assessed,
            years: r._years, county: r.property.county || "", owner: r.property.owner_name || "", cls: r.title_class,
        })[k];
        return rows.slice().sort((a, b) => { const x = val(a), y = val(b); return (x > y ? 1 : x < y ? -1 : 0) * d; });
    },

    render() {
        const rows = this.sorted(this.filtered());
        const pages = Math.max(1, Math.ceil(rows.length / this.state.perPage));
        if (this.state.page > pages) this.state.page = pages;
        const start = (this.state.page - 1) * this.state.perPage;
        const page = rows.slice(start, start + this.state.perPage);
        const tbody = document.getElementById("titleBody");
        const empty = document.getElementById("titleEmpty");
        if (!page.length) {
            tbody.innerHTML = ""; empty.style.display = "block";
            if (this.rows.length) { document.getElementById("titleEmptyTitle").textContent = "No matches"; document.getElementById("titleEmptyText").textContent = "Loosen the preset, priority, or search."; }
        } else {
            empty.style.display = "none";
            tbody.innerHTML = page.map((r) => {
                const p = r.property, st = this.statusOf(r);
                return `<tr data-id="${this.esc(r.id)}">
                    <td><span class="prio prio-${this.tier(r._priority)}">${r._priority}</span></td>
                    <td class="property-cell">
                        <div class="address">${this.esc(p.property_address || "N/A")}${p.city ? `, ${this.esc(p.city)}` : ""}</div>
                        <div class="meta">${this.esc(p.property_type || "")}${r.other_parcels_same_owner && r.other_parcels_same_owner.length ? ` • owner on +${r.other_parcels_same_owner.length} parcels` : ""}</div>
                    </td>
                    <td class="name-cell" style="font-family:var(--font-mono); font-size:0.8rem">${this.esc(p.owner_name)}${this.notesOf(r) ? '<span class="note-badge" title="Has notes">✎</span>' : ""}</td>
                    <td><span class="cls cls-${r.title_class[0]}" title="${this.esc(r.title_class_label)}">${r.title_class}</span> <span class="flags">${this.flagChips(r.flags)}</span></td>
                    <td class="county-cell">${this.esc(p.county)}</td>
                    <td class="date-cell">${p.transfer_date && p.transfer_date !== "0000.00.00" ? this.esc(p.transfer_date.slice(0, 4)) + ` <span class="yrs">(${r._years}y)</span>` : "N/A"}</td>
                    <td class="value-cell">$${r._assessed.toLocaleString()}</td>
                    <td class="equity-cell">${App.equityBadge(p)}</td>
                    <td><span class="status-badge ${st}">${st.toUpperCase()}</span></td>
                </tr>`;
            }).join("");
            tbody.querySelectorAll("tr").forEach((tr) => tr.addEventListener("click", () => this.open(tr.dataset.id)));
        }
        const total = rows.length;
        document.getElementById("titlePageInfo").textContent = total ? `Showing ${start + 1}–${Math.min(start + this.state.perPage, total)} of ${total} properties` : "No properties";
        document.getElementById("titlePrev").disabled = this.state.page <= 1;
        document.getElementById("titleNext").disabled = this.state.page >= pages;
    },

    open(id) {
        const r = this.rows.find((x) => x.id === id);
        if (!r) return;
        const p = r.property, sc = r.scores;
        const money = (v) => (v != null && v !== "" && !isNaN(parseFloat(v))) ? "$" + parseFloat(v).toLocaleString() : "N/A";
        const list = (arr) => arr && arr.length ? `<ul class="reasons">${arr.map((x) => `<li>${this.esc(x)}</li>`).join("")}</ul>` : '<span style="color:var(--text-dim)">—</span>';
        const owners = (p.owners || []).map((o) => this.esc(o.normalized_name)).join(", ");
        document.getElementById("modalBody").innerHTML = `
            <div class="detail-section">
                <h3>Property</h3>
                <div class="detail-row"><span class="label">Address</span><span class="value" style="font-weight:600">${this.esc(p.property_address)}, ${this.esc(p.city)} ${this.esc(p.zip_code)}</span></div>
                <div class="detail-row"><span class="label">County</span><span class="value" style="color:var(--purple)">${this.esc(p.county)}</span></div>
                <div class="detail-row"><span class="label">Account #</span><span class="value" style="font-family:var(--font-mono)">${this.esc(p.account_number)}</span></div>
                <div class="detail-row"><span class="label">Type</span><span class="value">${this.esc(p.property_type)}${p.year_built ? ` · built ${this.esc(p.year_built)}` : ""}${p.square_footage ? ` · ${this.esc(p.square_footage)} sq ft` : ""}</span></div>
                <div class="detail-row"><span class="label">Assessed</span><span class="value" style="color:var(--green); font-family:var(--font-mono)">${money(p.assessed_value)} <span style="color:var(--text-dim)">(${money(p.land_value)} land / ${money(p.improvement_value)} impr)</span></span></div>
                <div class="detail-row"><span class="label">Last Transfer</span><span class="value" style="font-family:var(--font-mono)">${this.esc(p.transfer_date)} ${p.sale_price && p.sale_price !== "0" ? "· " + money(p.sale_price) : "· $0 (non-sale)"}${p.grantor ? ` · from ${this.esc(p.grantor.replace(/\s+/g, " "))}` : ""}</span></div>
                <div class="detail-row"><span class="label">Deed</span><span class="value" style="font-family:var(--font-mono)">Liber ${this.esc(p.deed_liber || "?")} / Folio ${this.esc(p.deed_folio || "?")}</span></div>
                <div class="detail-row"><span class="label">Occupancy</span><span class="value">${p.occupancy_code === "H" ? "Owner-occupied" : "Not owner-occupied"} · code ${this.esc(p.occupancy_code || "—")} · homestead ${this.esc(p.homestead_code || "none")}${p.condition_code ? ` · condition ${this.esc(p.condition_code)}` : ""}</span></div>
                ${p.legal_description ? `<div class="detail-row"><span class="label">Legal</span><span class="value" style="font-size:0.8rem">${this.esc(p.legal_description)}</span></div>` : ""}
            </div>
            <div class="detail-section">
                <h3>Owner</h3>
                <div class="detail-row"><span class="label">On Record</span><span class="value" style="font-family:var(--font-mono)">${this.esc(p.owner_name)}${p.owner_name_2 ? `<br>${this.esc(p.owner_name_2)}` : ""}</span></div>
                <div class="detail-row"><span class="label">Type</span><span class="value">${this.esc(p.owner_type)}${owners ? ` — ${owners}` : ""}</span></div>
                ${r.other_parcels_same_owner && r.other_parcels_same_owner.length ? `<div class="detail-row"><span class="label">Same owner also on</span><span class="value" style="font-family:var(--font-mono); font-size:0.8rem">${r.other_parcels_same_owner.map(this.esc).join(", ")}</span></div>` : ""}
                ${p.mailing_address ? `<div class="detail-row"><span class="label">Mailing</span><span class="value">${this.esc(p.mailing_address)}</span></div>` : ""}
            </div>
            <div class="detail-section">
                <h3>Title Signals — ${r.title_class}: ${this.esc(r.title_class_label)}</h3>
                <div class="score-grid">
                    <div><div class="score-label">Research Priority</div><div class="score-val prio-${this.tier(sc.priority)}">${sc.priority}</div></div>
                    <div><div class="score-label">Title Complexity</div><div class="score-val">${sc.title_complexity}</div>${list(sc.reasons.title)}</div>
                    <div><div class="score-label">Financial</div><div class="score-val">${sc.financial}</div>${list(sc.reasons.financial)}</div>
                    <div><div class="score-label">Distress</div><div class="score-val">${sc.distress}</div>${list(sc.reasons.distress)}</div>
                </div>
                <div style="margin-top:8px">${this.flagChips(r.flags)}</div>
                <div class="aerial-note" style="margin-top:10px">Death and probate status: not yet automated (no free authorized source). See README → Engine 2 for what is and isn't checked.</div>
            </div>
            <div class="detail-section">
                <h3>Estimated Equity</h3>
                ${App.equityDetails(p)}
            </div>
            <div class="detail-section">
                <h3>Imagery</h3>
                ${Aerial.panel(p, r.id)}
            </div>`;
        Aerial.bind(document.getElementById("modalBody"));
        const sel = document.getElementById("leadStatusSelect"), notes = document.getElementById("leadNotes");
        sel.value = this.statusOf(r); notes.value = this.notesOf(r);
        document.getElementById("saveLeadBtn").onclick = () => {
            this.local[r.id] = { status: sel.value, notes: notes.value, updated: new Date().toISOString() };
            this.saveLocal(); App.closeModal(); this.render();
        };
        document.getElementById("modalOverlay").classList.add("active");
    },

    exportCSV() {
        const rows = this.sorted(this.filtered());
        if (!rows.length) { alert("Nothing to export with the current filters."); return; }
        const H = ["Priority", "Title Class", "Flags", "Owner on Record", "Owner 2", "Owner Type", "Address", "City", "Zip", "County", "Account #",
            "Property Type", "Assessed", "Land", "Improvement", "Year Built", "Sq Ft", "Occupancy", "Homestead", "Condition",
            "Last Transfer", "Sale Price", "Years Since Transfer", "Deed Liber", "Deed Folio", "Est. Equity", "Equity %", "Equity Confidence",
            "Title Score", "Financial Score", "Distress Score", "Lat", "Lon", "Status", "Notes"];
        const q = (v) => `"${String(v == null ? "" : v).replace(/"/g, '""')}"`;
        const lines = [H.join(",")];
        rows.forEach((r) => { const p = r.property; lines.push([
            r._priority, r.title_class, (r.flags || []).join("|"), p.owner_name, p.owner_name_2, p.owner_type, p.property_address, p.city, p.zip_code, p.county, p.account_number,
            p.property_type, p.assessed_value, p.land_value, p.improvement_value, p.year_built, p.square_footage, p.occupancy_code, p.homestead_code, p.condition_code,
            p.transfer_date, p.sale_price, p.years_since_transfer, p.deed_liber, p.deed_folio, p.estimated_equity, p.equity_percent, p.equity_confidence,
            r.scores.title_complexity, r.scores.financial, r.scores.distress, p.lat, p.lon, this.statusOf(r), this.notesOf(r),
        ].map(q).join(",")); });
        const blob = new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = `md-title-leads-${this.state.scope}-${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a); a.click(); a.remove();
        setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    },

    tier(n) { return n >= 70 ? "high" : n >= 50 ? "mid" : "low"; },
    flagChips(flags) {
        const nice = { ESTATE_IN_NAME: "Estate", HEIRS_IN_NAME: "Heirs", DECEASED_IN_NAME: "Deceased", PERSONAL_REP: "Pers. Rep", LIFE_ESTATE: "Life Estate",
            CARE_OF: "C/O", ET_AL: "Et Al", SURVIVING: "Surviving", CONSERVATOR: "Conservator/POA", TRUSTEE: "Trustee", MULTIPLE_INDIVIDUALS: "Co-owners", STALE_OWNERSHIP: "Stale",
            ABSENTEE: "Absentee", NO_HOMESTEAD: "No Homestead", VACANT_LAND: "Vacant Lot", OLD_STRUCTURE: "Pre-1950", POOR_CONDITION: "Poor Cond.", BELOW_AVG_CONDITION: "Below-Avg Cond.", MAIL_MISMATCH: "Mail ≠ Site" };
        const strong = new Set(["ESTATE_IN_NAME", "HEIRS_IN_NAME", "DECEASED_IN_NAME", "PERSONAL_REP", "LIFE_ESTATE", "CONSERVATOR"]);
        return (flags || []).map((f) => `<span class="chip ${strong.has(f) ? "chip-strong" : ""}">${nice[f] || f}</span>`).join(" ");
    },
    fmtDT(s) { const d = new Date(s); return isNaN(d) ? (s || "") : d.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }); },
    esc(s) { return s == null ? "" : String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); },
};
