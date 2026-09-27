/* ═══════════════════════════════════════════════
   MD Property Leads — Static dashboard
   Reads data/leads.json (written by GitHub Actions).
   Lead status + notes are kept in this browser's localStorage.
   ═══════════════════════════════════════════════ */

const DATA_URL = "data/leads.json";
const LS_KEY = "mdpl_lead_state_v1";

const App = {
    leads: [],
    meta: {},
    local: {},          // { [leadId]: { status, notes, updated } }
    state: {
        filter: "all",
        county: "",
        search: "",
        sortBy: "found_at",
        sortDir: "desc",
        page: 1,
        perPage: 25,
    },

    // ─── Init ───
    async init() {
        this.loadLocal();
        this.bindEvents();
        await this.loadData();
        this.render();
    },

    async loadData() {
        try {
            const resp = await fetch(`${DATA_URL}?t=${Date.now()}`, { cache: "no-store" });
            if (!resp.ok) throw new Error(resp.status);
            const data = await resp.json();
            this.leads = (data.leads || []).map((l) => this.decorate(l));
            this.meta = { generated_at: data.generated_at, last_run: data.last_run, runs: data.runs || [] };
        } catch (e) {
            console.error("Could not load leads.json", e);
            this.leads = [];
            this.meta = {};
            document.getElementById("emptyTitle").textContent = "No data file yet";
            document.getElementById("emptyText").textContent =
                "data/leads.json hasn't been generated. Run the scrape workflow on GitHub once and refresh.";
        }
    },

    decorate(lead) {
        const props = lead.properties || [];
        const primary = props[0] || {};
        const total = props.reduce((s, p) => s + (parseFloat(p.assessed_value) || 0), 0);
        return Object.assign({}, lead, {
            _primary: primary,
            _county: primary.county || "",
            _assessed: parseFloat(primary.assessed_value) || 0,   // primary property (matches the equity column)
            _portfolio: total,
            _equity: primary.estimated_equity != null ? parseFloat(primary.estimated_equity) : null,
            _search: [
                lead.full_name, lead.city, lead.survived_by,
                ...props.map((p) => `${p.property_address} ${p.city} ${p.county} ${p.owner_name}`),
            ].join(" ").toLowerCase(),
        });
    },

    // ─── Local state (status/notes) ───
    loadLocal() {
        try { this.local = JSON.parse(localStorage.getItem(LS_KEY) || "{}"); }
        catch { this.local = {}; }
    },
    saveLocal() {
        try { localStorage.setItem(LS_KEY, JSON.stringify(this.local)); } catch {}
    },
    statusOf(lead) { return (this.local[lead.id] && this.local[lead.id].status) || "new"; },
    notesOf(lead) { return (this.local[lead.id] && this.local[lead.id].notes) || ""; },
    setLead(id, status, notes) {
        this.local[id] = { status, notes, updated: new Date().toISOString() };
        this.saveLocal();
    },

    // ─── Events ───
    bindEvents() {
        let t;
        document.getElementById("searchInput").addEventListener("input", (e) => {
            clearTimeout(t);
            t = setTimeout(() => { this.state.search = e.target.value.trim().toLowerCase(); this.state.page = 1; this.render(); }, 200);
        });

        document.querySelectorAll(".filter-btn").forEach((btn) => {
            btn.addEventListener("click", () => {
                document.querySelectorAll(".filter-btn").forEach((b) => b.classList.remove("active"));
                btn.classList.add("active");
                this.state.filter = btn.dataset.filter;
                this.state.page = 1;
                this.render();
            });
        });

        document.getElementById("countySelect").addEventListener("change", (e) => {
            this.state.county = e.target.value; this.state.page = 1; this.render();
        });

        document.querySelectorAll("th[data-sort]").forEach((th) => {
            th.addEventListener("click", () => this.sortBy(th.dataset.sort));
        });

        document.getElementById("prevBtn").addEventListener("click", () => { if (this.state.page > 1) { this.state.page--; this.render(); } });
        document.getElementById("nextBtn").addEventListener("click", () => { this.state.page++; this.render(); });

        document.getElementById("exportBtn").addEventListener("click", () => this.exportCSV());

        document.getElementById("modalOverlay").addEventListener("click", (e) => { if (e.target.id === "modalOverlay") this.closeModal(); });
        document.getElementById("modalClose").addEventListener("click", () => this.closeModal());
        document.addEventListener("keydown", (e) => { if (e.key === "Escape") this.closeModal(); });
    },

    // ─── Filtering / sorting ───
    filtered() {
        const { filter, county, search } = this.state;
        return this.leads.filter((l) => {
            if (filter !== "all" && this.statusOf(l) !== filter) return false;
            if (county && l._county !== county) return false;
            if (search && !l._search.includes(search)) return false;
            return true;
        });
    },

    sorted(list) {
        const { sortBy, sortDir } = this.state;
        const dir = sortDir === "asc" ? 1 : -1;
        const val = (l) => {
            switch (sortBy) {
                case "full_name": return (l.last_name || l.full_name || "").toLowerCase();
                case "county": return l._county.toLowerCase();
                case "assessed_value": return l._assessed;
                case "estimated_equity": return l._equity == null ? -Infinity : l._equity;
                case "status": return this.statusOf(l);
                case "date_of_death": return l.date_of_death || "";
                default: return l.found_at || "";
            }
        };
        return list.slice().sort((a, b) => {
            const va = val(a), vb = val(b);
            if (va < vb) return -1 * dir;
            if (va > vb) return 1 * dir;
            return 0;
        });
    },

    sortBy(col) {
        if (this.state.sortBy === col) this.state.sortDir = this.state.sortDir === "desc" ? "asc" : "desc";
        else { this.state.sortBy = col; this.state.sortDir = "desc"; }
        document.querySelectorAll("th[data-sort]").forEach((th) => th.classList.toggle("sorted", th.dataset.sort === col));
        this.render();
    },

    // ─── Render ───
    render() {
        this.renderStats();
        this.renderCountyOptions();
        const list = this.sorted(this.filtered());
        const pages = Math.max(1, Math.ceil(list.length / this.state.perPage));
        if (this.state.page > pages) this.state.page = pages;
        const start = (this.state.page - 1) * this.state.perPage;
        this.renderRows(list.slice(start, start + this.state.perPage));
        this.renderPagination(list.length, pages);
    },

    renderStats() {
        const weekAgo = Date.now() - 7 * 864e5;
        const fresh = this.leads.filter((l) => new Date(l.found_at).getTime() > weekAgo).length;
        const props = this.leads.reduce((s, l) => s + (l.properties || []).length, 0);
        document.getElementById("statTotal").textContent = this.leads.length.toLocaleString();
        document.getElementById("statNew").textContent = fresh.toLocaleString();
        document.getElementById("statProperties").textContent = props.toLocaleString();

        const el = document.getElementById("lastScrapeInfo");
        const run = this.meta.last_run;
        if (run) {
            el.innerHTML = `Last scan: ${this.formatDateTime(run.completed_at)}<br>` +
                `${run.obituaries_scraped} obits · ${run.obituaries_checked} checked · ${run.leads_created} new leads`;
        } else {
            el.textContent = "No scans yet";
        }
    },

    renderCountyOptions() {
        const sel = document.getElementById("countySelect");
        if (sel.options.length > 1) return;
        const counts = {};
        this.leads.forEach((l) => { if (l._county) counts[l._county] = (counts[l._county] || 0) + 1; });
        Object.keys(counts).sort().forEach((c) => {
            const o = document.createElement("option");
            o.value = c; o.textContent = `${c} (${counts[c]})`;
            sel.appendChild(o);
        });
    },

    renderRows(rows) {
        const tbody = document.getElementById("leadsBody");
        const empty = document.getElementById("emptyState");
        if (!rows.length) {
            tbody.innerHTML = "";
            empty.style.display = "block";
            if (this.leads.length) {
                document.getElementById("emptyTitle").textContent = "No matches";
                document.getElementById("emptyText").textContent = "Try a different search, county, or status filter.";
            }
            return;
        }
        empty.style.display = "none";
        tbody.innerHTML = rows.map((lead) => {
            const p = lead._primary;
            const n = (lead.properties || []).length;
            const status = this.statusOf(lead);
            const hasNotes = !!this.notesOf(lead);
            return `
                <tr data-id="${lead.id}">
                    <td class="name-cell">
                        ${this.esc(lead.full_name)}${hasNotes ? '<span class="note-badge" title="Has notes">✎</span>' : ""}
                        ${lead.obituary_url ? `<a href="${this.esc(lead.obituary_url)}" target="_blank" rel="noopener" class="obit-link" onclick="event.stopPropagation()">View Obituary ↗</a>` : ""}
                    </td>
                    <td class="date-cell">${this.esc(this.formatDate(lead.date_of_death) || "N/A")}</td>
                    <td class="property-cell">
                        <div class="address">${this.esc(p.property_address || "N/A")}${p.city ? `, ${this.esc(p.city)}` : ""}</div>
                        <div class="meta">
                            ${n > 1 ? `+${n - 1} more propert${n - 1 === 1 ? "y" : "ies"} ` : ""}
                            ${p.property_type ? `• ${this.esc(p.property_type)}` : ""}
                        </div>
                    </td>
                    <td class="county-cell">${this.esc(lead._county || "N/A")}</td>
                    <td class="value-cell">${lead._assessed ? "$" + lead._assessed.toLocaleString() : "N/A"}${n > 1 && lead._portfolio > lead._assessed ? `<div class="meta" title="All ${n} matched properties">$${lead._portfolio.toLocaleString()} across ${n}</div>` : ""}</td>
                    <td class="equity-cell">${this.equityBadge(p)}</td>
                    <td><span class="status-badge ${status === "new" ? this.ageClass(lead) : status}">${this.statusLabel(lead, status)}</span></td>
                    <td class="date-cell">${this.formatDate(lead.found_at)}</td>
                </tr>`;
        }).join("");
        tbody.querySelectorAll("tr").forEach((tr) => tr.addEventListener("click", () => this.openLead(tr.dataset.id)));
    },

    renderPagination(total, pages) {
        const start = total ? (this.state.page - 1) * this.state.perPage + 1 : 0;
        const end = Math.min(this.state.page * this.state.perPage, total);
        document.getElementById("pageInfo").textContent = total ? `Showing ${start}–${end} of ${total} leads` : "No leads found";
        document.getElementById("prevBtn").disabled = this.state.page <= 1;
        document.getElementById("nextBtn").disabled = this.state.page >= pages;
    },

    // ─── Modal ───
    openLead(id) {
        const lead = this.leads.find((l) => l.id === id);
        if (!lead) return;
        const props = lead.properties || [];
        const money = (v) => (v != null && v !== "" && !isNaN(parseFloat(v))) ? "$" + parseFloat(v).toLocaleString() : "N/A";

        const propsHtml = props.map((p) => `
            <div style="margin-bottom:12px; padding:12px; background:var(--bg-card); border-radius:var(--radius); border:1px solid var(--border);">
                <div class="detail-row"><span class="label">Owner on Record</span><span class="value" style="font-family:var(--font-mono)">${this.esc(p.owner_name || "N/A")}</span></div>
                <div class="detail-row"><span class="label">Address</span><span class="value" style="font-weight:600">${this.esc(p.property_address || "N/A")}${p.city ? `, ${this.esc(p.city)}` : ""} ${this.esc(p.zip_code || "")}</span></div>
                <div class="detail-row"><span class="label">County</span><span class="value" style="color:var(--purple)">${this.esc(p.county || "N/A")}</span></div>
                <div class="detail-row"><span class="label">Type</span><span class="value">${this.esc(p.property_type || "N/A")}</span></div>
                <div class="detail-row"><span class="label">Assessed Value</span><span class="value" style="color:var(--green); font-family:var(--font-mono)">${money(p.assessed_value)}</span></div>
                <div class="detail-row"><span class="label">Land / Improvement</span><span class="value" style="font-family:var(--font-mono)">${money(p.land_value)} / ${money(p.improvement_value)}</span></div>
                <div class="detail-row"><span class="label">Year Built</span><span class="value">${this.esc(p.year_built || "N/A")}</span></div>
                <div class="detail-row"><span class="label">Sq Ft</span><span class="value">${this.esc(p.square_footage || "N/A")}</span></div>
                <div class="detail-row"><span class="label">Last Transfer</span><span class="value" style="font-family:var(--font-mono)">${this.esc(this.formatDate(p.transfer_date) || "N/A")} ${p.sale_price ? "· " + money(p.sale_price) : ""}</span></div>
                <div class="detail-row"><span class="label">Account #</span><span class="value" style="font-family:var(--font-mono)">${this.esc(p.account_number || "N/A")}</span></div>
                ${p.legal_description ? `<div class="detail-row"><span class="label">Legal</span><span class="value" style="font-size:0.8rem">${this.esc(p.legal_description)}</span></div>` : ""}
                <div style="margin-top:8px; padding-top:8px; border-top:1px solid var(--border);">
                    <div style="font-size:0.7rem; text-transform:uppercase; letter-spacing:1px; color:var(--cyan-dim); margin-bottom:6px;">Estimated Equity</div>
                    ${this.equityDetails(p)}
                </div>
                ${p.deed_liber ? `<div class="detail-row"><span class="label">Deed</span><span class="value" style="font-family:var(--font-mono)">Liber ${this.esc(p.deed_liber)} / Folio ${this.esc(p.deed_folio || "?")}</span></div>` : ""}
                <div style="margin-top:10px;">${Aerial.panel(p, (p.account_number || "").replace(/\W/g, ""))}</div>
            </div>`).join("");

        document.getElementById("modalBody").innerHTML = `
            <div class="detail-section">
                <h3>Deceased Information</h3>
                <div class="detail-row"><span class="label">Full Name</span><span class="value" style="font-weight:600">${this.esc(lead.full_name)}</span></div>
                <div class="detail-row"><span class="label">Date of Death</span><span class="value" style="font-family:var(--font-mono)">${this.esc(this.formatDate(lead.date_of_death) || "N/A")}</span></div>
                <div class="detail-row"><span class="label">Date of Birth</span><span class="value" style="font-family:var(--font-mono)">${this.esc(this.formatDate(lead.date_of_birth) || "N/A")}</span></div>
                <div class="detail-row"><span class="label">Age</span><span class="value">${lead.age || "N/A"}</span></div>
                <div class="detail-row"><span class="label">City</span><span class="value">${this.esc(lead.city || "N/A")}</span></div>
                <div class="detail-row"><span class="label">Survived By</span><span class="value">${this.esc(lead.survived_by || "N/A")}</span></div>
                <div class="detail-row"><span class="label">Source</span><span class="value" style="font-size:0.8rem">${this.esc(lead.source || "")}</span></div>
                ${lead.obituary_url ? `<div class="detail-row"><span class="label">Obituary</span><span class="value"><a href="${this.esc(lead.obituary_url)}" target="_blank" rel="noopener">Open on Legacy.com ↗</a></span></div>` : ""}
                ${lead.obituary_text ? `<div class="obit-snippet">${this.esc(lead.obituary_text)}</div>` : ""}
            </div>
            <div class="detail-section">
                <h3>Properties (${props.length})</h3>
                ${propsHtml || '<p style="color:var(--text-dim)">No property details available</p>'}
            </div>`;

        Aerial.bind(document.getElementById("modalBody"));

        const sel = document.getElementById("leadStatusSelect");
        const notes = document.getElementById("leadNotes");
        sel.value = this.statusOf(lead);
        notes.value = this.notesOf(lead);
        document.getElementById("saveLeadBtn").onclick = () => {
            this.setLead(lead.id, sel.value, notes.value);
            this.closeModal();
            this.render();
        };
        document.getElementById("modalOverlay").classList.add("active");
    },

    closeModal() { document.getElementById("modalOverlay").classList.remove("active"); },

    // ─── Export ───
    exportCSV() {
        const rows = this.sorted(this.filtered());
        if (!rows.length) { alert("Nothing to export with the current filters."); return; }
        const headers = ["First Name", "Middle Name", "Last Name", "Date of Death", "Date of Birth", "Age",
            "Obit City", "Owner on Record", "Property Address", "Property City", "County", "State", "Zip",
            "Property Type", "Assessed Value", "Land Value", "Improvement Value", "Year Built", "Sq Ft",
            "Last Transfer Date", "Last Sale Price", "Est. Equity", "Equity %", "Equity Confidence",
            "Account #", "Survived By", "Status", "Notes", "Obituary URL", "Found"];
        const q = (v) => `"${String(v == null ? "" : v).replace(/"/g, '""')}"`;
        const lines = [headers.join(",")];
        rows.forEach((l) => {
            const props = l.properties && l.properties.length ? l.properties : [{}];
            props.forEach((p) => {
                lines.push([
                    l.first_name, l.middle_name, l.last_name, l.date_of_death, l.date_of_birth, l.age,
                    l.city, p.owner_name, p.property_address, p.city, p.county, "MD", p.zip_code,
                    p.property_type, p.assessed_value, p.land_value, p.improvement_value, p.year_built, p.square_footage,
                    p.transfer_date, p.sale_price, p.estimated_equity, p.equity_percent, p.equity_confidence,
                    p.account_number, l.survived_by, this.statusOf(l), this.notesOf(l), l.obituary_url, l.found_at,
                ].map(q).join(","));
            });
        });
        const blob = new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = `md-property-leads-${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a); a.click(); a.remove();
        setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    },

    // ─── Helpers ───
    formatDate(s) {
        if (!s) return "";
        if (/^\d{4}$/.test(s)) return s;
        const d = new Date(/^\d{4}-\d{2}-\d{2}$/.test(s) ? s + "T12:00:00" : s);
        if (isNaN(d)) return s;
        return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
    },
    formatDateTime(s) {
        const d = new Date(s);
        return isNaN(d) ? (s || "") : d.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
    },
    daysOld(lead) {
        const d = new Date(lead.found_at);
        return isNaN(d) ? 0 : Math.floor((Date.now() - d.getTime()) / 864e5);
    },
    ageClass(lead) { return this.daysOld(lead) >= 7 ? "old" : "new"; },
    statusLabel(lead, status) {
        if (status !== "new") return status.toUpperCase();
        const d = this.daysOld(lead);
        if (d <= 0) return "NEW";
        if (d === 1) return "1-DAY OLD";
        return `${d}-DAYS OLD`;
    },
    equityTier(p) {
        if (!p || p.equity_confidence === "unknown" || p.equity_percent == null) return "unknown";
        if (p.equity_percent >= 70) return "high";
        if (p.equity_percent >= 40) return "medium";
        return "low";
    },
    equityBadge(p) {
        if (!p || p.estimated_equity == null) return '<span class="equity-badge unknown">N/A</span>';
        const pct = p.equity_percent != null ? `${p.equity_percent}%` : "";
        return `<span class="equity-badge ${this.equityTier(p)}" title="Confidence: ${p.equity_confidence || "unknown"}">$${parseFloat(p.estimated_equity).toLocaleString()} <span class="equity-pct">${pct}</span></span>`;
    },
    equityDetails(p) {
        const tier = this.equityTier(p);
        const conf = { high: "High", medium: "Medium", low: "Low", unknown: "Insufficient Data" };
        const m = (v) => v != null ? "$" + parseFloat(v).toLocaleString() : "N/A";
        return `
            <div class="detail-row"><span class="label">Est. Market Value</span><span class="value" style="font-family:var(--font-mono)">${m(p.estimated_market_value)}</span></div>
            <div class="detail-row"><span class="label">Est. Mortgage Bal.</span><span class="value" style="font-family:var(--font-mono); color:var(--red)">${p.estimated_mortgage_balance != null ? m(p.estimated_mortgage_balance) : "Unknown"}</span></div>
            <div class="detail-row"><span class="label">Est. Equity</span><span class="value equity-badge-inline ${tier}" style="font-family:var(--font-mono); font-weight:700">${m(p.estimated_equity)}${p.equity_percent != null ? ` (${p.equity_percent}%)` : ""}</span></div>
            <div class="detail-row"><span class="label">Confidence</span><span class="value"><span class="equity-conf-badge ${p.equity_confidence || "unknown"}">${conf[p.equity_confidence] || "Unknown"}</span></span></div>`;
    },
    esc(s) {
        if (s == null) return "";
        return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
    },
};

document.addEventListener("DOMContentLoaded", () => App.init());
