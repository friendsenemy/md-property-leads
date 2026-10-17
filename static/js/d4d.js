/* Driving for Dollars tab: properties Ray spotted on the road, each with the deep-dive
   research, SDAT facts, imagery and the shared notes. Reads data/d4d/leads.json
   (written by engine2/d4d_add.py). The row id is the SDAT account number, the same id
   the shared notes sheet uses, so a note saved on a card is the note for that account. */
const D4D_LS_KEY = "md_d4d_leads_local_v1";

const D4DApp = {
    rows: [], local: {},
    state: { search: "", page: 1, perPage: 25 },
    VERDICT: { hot: ["HOT", "var(--red)"], warm: ["WARM", "var(--yellow)"], cold: ["COLD", "var(--cyan-dim)"], dead: ["DEAD", "var(--text-dim)"] },

    async init() {
        try { this.local = JSON.parse(localStorage.getItem(D4D_LS_KEY) || "{}"); } catch { this.local = {}; }
        let t;
        document.getElementById("d4dSearch").addEventListener("input", (e) => {
            clearTimeout(t); t = setTimeout(() => { this.state.search = e.target.value.trim().toLowerCase(); this.state.page = 1; this.render(); }, 200);
        });
        document.getElementById("d4dPrev").addEventListener("click", () => { if (this.state.page > 1) { this.state.page--; this.render(); } });
        document.getElementById("d4dNext").addEventListener("click", () => { this.state.page++; this.render(); });
        try {
            const r = await fetch(`data/d4d/leads.json?t=${Date.now()}`, { cache: "no-store" });
            if (!r.ok) throw new Error(r.status);
            this.rows = ((await r.json()).rows || []).map((x) => Object.assign(x, {
                _blob: [x.property_address, x.city, x.county, x.account, (x.owners || []).join(" "), x.headline].join(" ").toLowerCase(),
            }));
        } catch { this.rows = []; }
        document.getElementById("dstatTotal").textContent = this.rows.length;
        document.getElementById("dstatHot").textContent = this.rows.filter((r) => r.verdict === "hot" || r.verdict === "warm").length;
        this.render();
    },

    statusOf(r) { const s = SharedNotes.get(r.id); if (s) return s.status || "new"; return (this.local[r.id] && this.local[r.id].status) || "new"; },
    notesOf(r)  { const s = SharedNotes.get(r.id); if (s) return s.notes || "";   return (this.local[r.id] && this.local[r.id].notes) || ""; },
    _sharedHook: document.addEventListener("mdpl:notes-loaded", () => { try { if (D4DApp.rows.length) D4DApp.render(); } catch {} }),

    render() {
        const rows = this.rows.filter((r) => !this.state.search || this.state.search.split(/\s+/).every((w) => r._blob.includes(w)));
        const pages = Math.max(1, Math.ceil(rows.length / this.state.perPage));
        if (this.state.page > pages) this.state.page = pages;
        const start = (this.state.page - 1) * this.state.perPage;
        const page = rows.slice(start, start + this.state.perPage);
        document.getElementById("d4dEmpty").style.display = page.length ? "none" : "block";
        const tbody = document.getElementById("d4dBody");
        tbody.innerHTML = page.map((r) => {
            const st = this.statusOf(r), v = this.VERDICT[r.verdict] || [String(r.verdict || "—").toUpperCase(), "var(--text-secondary)"];
            return `<tr data-id="${this.esc(r.id)}">
                <td class="date-cell">${this.esc(r.spotted || "")}</td>
                <td class="property-cell"><div class="address">${this.esc(r.property_address)}, ${this.esc(r.city)}</div>
                    <div class="meta" style="font-family:var(--font-mono)">${this.esc(r.account)} · ${this.esc(r.county)}</div></td>
                <td class="name-cell" style="font-size:0.78rem">${(r.owners || []).map((o) => this.esc(o.split(" (")[0])).join("<br>")}${this.notesOf(r) ? '<span class="note-badge" title="Has notes">✎</span>' : ""}</td>
                <td style="font-size:0.8rem; color:var(--text-secondary)">${this.esc(r.headline || "")}</td>
                <td class="value-cell">${this.money(r.assessed_value)}</td>
                <td><span style="font-family:var(--font-mono); font-weight:700; color:${v[1]}">${v[0]}</span></td>
                <td><span class="status-badge ${st}">${st.toUpperCase()}</span></td>
            </tr>`;
        }).join("");
        tbody.querySelectorAll("tr").forEach((tr) => tr.addEventListener("click", () => this.open(tr.dataset.id)));
        document.getElementById("d4dPageInfo").textContent = rows.length ? `Showing ${start + 1}–${Math.min(start + this.state.perPage, rows.length)} of ${rows.length}` : "No properties";
        document.getElementById("d4dPrev").disabled = this.state.page <= 1;
        document.getElementById("d4dNext").disabled = this.state.page >= pages;
    },

    open(id) {
        const r = this.rows.find((x) => x.id === id);
        if (!r) return;
        const row = (l, v) => v ? `<div class="detail-row"><span class="label">${l}</span><span class="value">${v}</span></div>` : "";
        const list = (xs) => (xs || []).length ? `<ul class="reasons">${xs.map((x) => `<li>${this.esc(x)}</li>`).join("")}</ul>` : "";
        const v = this.VERDICT[r.verdict] || [String(r.verdict || "").toUpperCase(), "var(--text-secondary)"];
        document.getElementById("modalBody").innerHTML = `
            <div class="detail-section">
                <h3>Driving for Dollars · spotted ${this.esc(r.spotted || "")} <span style="margin-left:8px; color:${v[1]}">${v[0]}</span></h3>
                <p style="font-size:0.9rem; line-height:1.5">${this.esc(r.headline || "")}</p>
            </div>
            <div class="detail-section">
                <h3>Property</h3>
                ${row("Address", `<b>${this.esc(r.property_address)}, ${this.esc(r.city)} ${this.esc(r.zip_code)}</b>`)}
                ${row("County", this.esc(r.county))}
                ${row("Account #", `<span style="font-family:var(--font-mono)">${this.esc(r.account)}</span>${r.sdat_url ? ` · <a href="${this.esc(r.sdat_url)}" target="_blank" rel="noopener" style="color:var(--cyan)">SDAT ↗</a>` : ""}`)}
                ${row("Map / Grid / Parcel", this.esc([r.map, r.grid, r.parcel].filter(Boolean).join(" / ")))}
                ${row("Legal", this.esc(r.legal))}
                ${row("Type", this.esc([r.land_use, r.style, r.year_built ? "built " + r.year_built : "", r.sqft ? r.sqft.toLocaleString() + " sf" : ""].filter(Boolean).join(" · ")))}
                ${row("Lot", this.esc([r.acres ? r.acres + " ac" : "", r.zoning ? "zoned " + r.zoning : ""].filter(Boolean).join(" · ")))}
                ${row("Assessed", `${this.money(r.assessed_value)} <span class="sub">(${this.money(r.land_value)} land / ${this.money(r.impr_value)} impr)</span>`)}
                ${row("Last transfer", this.esc([r.last_transfer, r.last_price ? this.money(r.last_price) : "$0 / non-sale"].filter(Boolean).join(" · ")))}
                ${row("SDAT occupancy", this.esc((r.owner_occupied_code === "H" ? "Owner-occupied code" : "Not owner-occupied") + (r.homestead_qualified ? " · homestead active" : " · no active homestead credit")))}
                ${row("Tax bill mailed to", this.esc(r.owner_mailing))}
            </div>
            <div class="detail-section">
                <h3>Title</h3>
                ${row("Owners", (r.owners || []).map((o) => this.esc(o)).join("<br>"))}
                ${row("Vesting", this.esc(r.vesting))}
                ${row("Deed", `<span style="font-family:var(--font-mono); font-size:0.82rem">${this.esc(r.deed)}</span>`)}
            </div>
            <div class="detail-section"><h3>What the research found</h3>${list(r.findings)}</div>
            <div class="detail-section"><h3>Next steps</h3>${list(r.next_steps)}</div>
            <div class="detail-section"><h3>Imagery</h3>${Aerial.panel(r, r.id)}</div>
            ${(r.sources || []).length ? `<div class="detail-section"><h3>Sources</h3>${list(r.sources)}</div>` : ""}`;
        Aerial.bind(document.getElementById("modalBody"));
        const sel = document.getElementById("leadStatusSelect"), notes = document.getElementById("leadNotes");
        sel.value = this.statusOf(r); notes.value = this.notesOf(r);
        SharedNotes.footer(r.id);
        document.getElementById("saveLeadBtn").onclick = () => {
            this.local[r.id] = { status: sel.value, notes: notes.value, updated: new Date().toISOString() };
            try { localStorage.setItem(D4D_LS_KEY, JSON.stringify(this.local)); } catch {}
            SharedNotes.save("d4d", r.id, sel.value, notes.value, `${r.property_address}, ${r.city} (D4D)`);
            App.closeModal(); this.render();
        };
        document.getElementById("modalOverlay").classList.add("active");
    },

    money(v) { const n = Number(v); return (v == null || v === "" || isNaN(n) || !n) ? "—" : "$" + Math.round(n).toLocaleString(); },
    esc(s) { return s == null ? "" : String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); },
};
