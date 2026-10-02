/* Failed Foreclosure tab — Tax-Property § 14-847.
   Where a certificate holder does not comply with the final judgment within 105
   days, the court may, on motion of the governing body, enter a judgment vesting
   title in the county in fee simple. A collector's deed TO a county is that
   outcome recorded in the land records, so we can see it in SDAT.

   The 90-day window BEFORE vesting — judgment entered, purchaser has not paid,
   an interested party may move to strike — exists only in the court docket.
   Maryland Judiciary Case Search prohibits automated access, so it is not here. */
const VESTED_LS_KEY = "md_vested_leads_local_v1";

const VestedApp = {
    rows: [], local: {}, generated: "",
    state: { search: "", county: "", sort: "year", dir: -1, page: 1, perPage: 25 },

    async init() {
        this.loadLocal();
        this.bindEvents();
        try {
            const r = await fetch(`data/surplus/vested.json?t=${Date.now()}`, { cache: "no-store" });
            if (!r.ok) throw new Error(r.status);
            const j = await r.json();
            this.rows = (j.rows || []).map((x) => this.decorate(x));
            this.generated = j.generated_at || "";
        } catch { this.rows = []; }
        this.fillCounties();
        this.stats();
        this.render();
    },

    decorate(r) {
        r.id = "fv-" + r.account;
        r._year = Number(r.conveyed_year || 0);
        r._assessed = Number(r.assessed_value || 0);
        r._blob = [r.address, r.city, r.county, r.owner_now, r.grantor, r.account].filter(Boolean).join(" ").toLowerCase();
        return r;
    },

    fillCounties() {
        const cs = [...new Set(this.rows.map((r) => r.county).filter(Boolean))].sort();
        document.getElementById("vestedCounty").innerHTML = '<option value="">All counties</option>'
            + cs.map((c) => `<option value="${this.esc(c)}">${this.esc(c)}</option>`).join("");
    },

    stats() {
        const now = new Date().getFullYear();
        document.getElementById("vstatTotal").textContent = this.rows.length.toLocaleString();
        document.getElementById("vstatRecent").textContent = this.rows.filter((r) => r._year >= now - 3).length.toLocaleString();
        const v = this.rows.reduce((n, r) => n + r._assessed, 0);
        document.getElementById("vstatValue").textContent = v >= 1e6 ? "$" + (v / 1e6).toFixed(1) + "M" : "$" + Math.round(v).toLocaleString();
        const info = document.getElementById("vestedRunInfo");
        info.innerHTML = this.rows.length
            ? `<b>${this.rows.length.toLocaleString()}</b> parcels deeded by a tax collector to a county${this.generated ? ` · updated ${this.esc(this.fmtDT(this.generated))}` : ""}`
            : "";
    },

    loadLocal() { try { this.local = JSON.parse(localStorage.getItem(VESTED_LS_KEY) || "{}"); } catch { this.local = {}; } },
    saveLocal() { try { localStorage.setItem(VESTED_LS_KEY, JSON.stringify(this.local)); } catch {} },
    statusOf(r) { const s = SharedNotes.get(r.id); if (s) return s.status || "new"; return (this.local[r.id] && this.local[r.id].status) || "new"; },
    notesOf(r)  { const s = SharedNotes.get(r.id); if (s) return s.notes || "";   return (this.local[r.id] && this.local[r.id].notes) || ""; },
    _sharedHook: document.addEventListener("mdpl:notes-loaded", () => { try { if (VestedApp.rows.length) VestedApp.render(); } catch {} }),

    bindEvents() {
        let t;
        document.getElementById("vestedSearch").addEventListener("input", (e) => {
            clearTimeout(t); t = setTimeout(() => { this.state.search = e.target.value.trim().toLowerCase(); this.state.page = 1; this.render(); }, 200);
        });
        document.getElementById("vestedCounty").addEventListener("change", (e) => { this.state.county = e.target.value; this.state.page = 1; this.render(); });
        document.querySelectorAll("#vestedTable th[data-sort]").forEach((th) => th.addEventListener("click", () => {
            const k = th.dataset.sort;
            this.state.dir = this.state.sort === k ? -this.state.dir : -1;
            this.state.sort = k; this.render();
        }));
        document.getElementById("vestedPrev").addEventListener("click", () => { if (this.state.page > 1) { this.state.page--; this.render(); } });
        document.getElementById("vestedNext").addEventListener("click", () => { this.state.page++; this.render(); });
        document.getElementById("vestedExport").addEventListener("click", () => this.exportCSV());
    },

    filtered() {
        const s = this.state;
        return this.rows.filter((r) => (!s.county || r.county === s.county) && (!s.search || r._blob.includes(s.search)));
    },

    sorted(rows) {
        const k = this.state.sort, d = this.state.dir;
        const val = (r) => ({ year: r._year, assessed: r._assessed, county: r.county || "", owner: r.owner_now || "" }[k]);
        return rows.slice().sort((a, b) => { const x = val(a), y = val(b); return (x > y ? 1 : x < y ? -1 : 0) * d; });
    },

    render() {
        const rows = this.sorted(this.filtered());
        const pages = Math.max(1, Math.ceil(rows.length / this.state.perPage));
        if (this.state.page > pages) this.state.page = pages;
        const start = (this.state.page - 1) * this.state.perPage;
        const page = rows.slice(start, start + this.state.perPage);
        const tbody = document.getElementById("vestedBody"), empty = document.getElementById("vestedEmpty");
        if (!page.length) {
            tbody.innerHTML = ""; empty.style.display = "block";
            if (this.rows.length) {
                document.getElementById("vestedEmptyTitle").textContent = "No matches";
                document.getElementById("vestedEmptyText").textContent = "Loosen the county or search.";
            }
        } else {
            empty.style.display = "none";
            tbody.innerHTML = page.map((r) => {
                const st = this.statusOf(r);
                return `<tr data-id="${this.esc(r.id)}">
                    <td class="date-cell">${r.conveyed_year || "—"}</td>
                    <td class="property-cell">
                        <div class="address">${this.esc(r.address || "N/A")}${r.city ? `, ${this.esc(r.city)}` : ""}</div>
                        <div class="meta" style="font-family:var(--font-mono)">${this.esc(r.account)}${r.year_built ? ` · built ${this.esc(r.year_built)}` : ""}</div>
                    </td>
                    <td class="name-cell" style="font-family:var(--font-mono); font-size:0.78rem">${this.esc(r.owner_now || "")}${this.notesOf(r) ? '<span class="note-badge" title="Has notes">✎</span>' : ""}</td>
                    <td class="county-cell">${this.esc(r.county || "")}</td>
                    <td style="font-family:var(--font-mono); font-size:0.74rem; color:var(--text-secondary)">${this.esc(r.grantor || "")}</td>
                    <td class="value-cell">${this.money(r._assessed)}</td>
                    <td><span class="status-badge ${st}">${st.toUpperCase()}</span></td>
                </tr>`;
            }).join("");
            tbody.querySelectorAll("tr").forEach((tr) => tr.addEventListener("click", () => this.open(tr.dataset.id)));
        }
        const total = rows.length;
        document.getElementById("vestedPageInfo").textContent = total
            ? `Showing ${start + 1}–${Math.min(start + this.state.perPage, total)} of ${total} parcels` : "No parcels";
        document.getElementById("vestedPrev").disabled = this.state.page <= 1;
        document.getElementById("vestedNext").disabled = this.state.page >= pages;
    },

    open(id) {
        const r = this.rows.find((x) => x.id === id);
        if (!r) return;
        document.getElementById("modalBody").innerHTML = `
            <div class="detail-section">
                <h3>What Happened Here</h3>
                <p style="font-size:0.85rem; line-height:1.55">A tax-sale purchaser won the lien and took it all the way to a final judgment
                foreclosing the right of redemption — then did not pay. Under <b>§ 14-847</b>, when the certificate holder fails to comply with
                the judgment within <b>105 days</b>, the court may, on motion of the governing body, enter judgment vesting title in the county
                <b>in fee simple</b>. The collector then deeded it to the county, which is the record you are looking at.</p>
            </div>
            <div class="detail-section">
                <h3>Property</h3>
                <div class="detail-row"><span class="label">Address</span><span class="value" style="font-weight:600">${this.esc(r.address || "")}${r.city ? `, ${this.esc(r.city)}` : ""} ${this.esc(r.zip || "")}</span></div>
                <div class="detail-row"><span class="label">County</span><span class="value" style="color:var(--purple)">${this.esc(r.county || "")}</span></div>
                <div class="detail-row"><span class="label">Account #</span><span class="value" style="font-family:var(--font-mono)">${this.esc(r.account)}</span></div>
                <div class="detail-row"><span class="label">Assessed</span><span class="value" style="color:var(--green); font-family:var(--font-mono)">${this.money(r._assessed)}</span></div>
                <div class="detail-row"><span class="label">Now owned by</span><span class="value" style="font-family:var(--font-mono); color:var(--yellow)">${this.esc(r.owner_now || "")}</span></div>
                <div class="detail-row"><span class="label">Deeded by</span><span class="value" style="font-family:var(--font-mono)">${this.esc(r.grantor || "")}</span></div>
                <div class="detail-row"><span class="label">Deed / date</span><span class="value" style="font-family:var(--font-mono)">${this.esc(r.deed || "—")} · ${this.esc(r.conveyed_on || "—")}</span></div>
                ${r.occupancy === "H" ? '<div class="detail-row"><span class="label">Note</span><span class="value">Flagged owner-occupied before the transfer</span></div>' : ""}
            </div>
            <div class="detail-section">
                <h3>Next Step</h3>
                <p style="font-size:0.85rem; line-height:1.55">The county owns this outright. It is not a homeowner lead — nobody is losing a
                house here, and the former owner's redemption rights ended at the judgment. It is an <b>acquisition</b> lead: ask the county's
                real property or surplus property office how this parcel is disposed of. Counties sell these by public sale, sealed bid or
                direct negotiation depending on the jurisdiction, and a parcel sitting on a county's books for years is one they generally
                want off.</p>
                <p style="font-size:0.78rem; color:var(--text-dim); margin-top:8px">The window <i>before</i> vesting — judgment entered, purchaser
                has not paid, and under §14-847 an interested party may move to strike after 90 days — lives only in the court docket. Case Search
                prohibits automated access, so it is not on this tab.</p>
            </div>
            <div class="detail-section">
                <h3>Imagery</h3>
                ${Aerial.panel(r, r.id)}
            </div>`;
        Aerial.bind(document.getElementById("modalBody"));
        const sel = document.getElementById("leadStatusSelect"), notes = document.getElementById("leadNotes");
        sel.value = this.statusOf(r); notes.value = this.notesOf(r);
        SharedNotes.footer(r.id);
        document.getElementById("saveLeadBtn").onclick = () => {
            this.local[r.id] = { status: sel.value, notes: notes.value, updated: new Date().toISOString() };
            this.saveLocal();
            SharedNotes.save("vested", r.id, sel.value, notes.value, `${r.address || ""}, ${r.city || ""} — ${r.owner_now || ""}`);
            App.closeModal(); this.render();
        };
        document.getElementById("modalOverlay").classList.add("active");
    },

    exportCSV() {
        const rows = this.sorted(this.filtered());
        if (!rows.length) { alert("Nothing to export with the current filters."); return; }
        const H = ["Year Vested", "Account #", "Address", "City", "Zip", "County", "Now Owned By", "Deeded By",
            "Deed", "Transfer Date", "Assessed", "Year Built", "Occupancy", "Lat", "Lon", "Status", "Notes"];
        const q = (v) => { const s = String(v == null ? "" : v); return /^=".*"$/.test(s) ? s : `"${s.replace(/"/g, '""')}"`; };
        const lines = [H.join(",")];
        rows.forEach((r) => lines.push([r.conveyed_year, `="${r.account}"`, r.address, r.city, r.zip, r.county,
            r.owner_now, r.grantor, r.deed, r.conveyed_on, r.assessed_value, r.year_built, r.occupancy,
            r.lat, r.lon, this.statusOf(r), this.notesOf(r)].map(q).join(",")));
        const blob = new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = `md-failed-foreclosures-${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a); a.click(); a.remove();
        setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    },

    money(v) { const n = Number(v); return (v == null || v === "" || isNaN(n) || !n) ? "—" : "$" + Math.round(n).toLocaleString(); },
    fmtDT(s) { const d = new Date(s); return isNaN(d) ? (s || "") : d.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }); },
    esc(s) { return s == null ? "" : String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); },
};
