/* Surplus Funds Leads tab — tax-sale outcome watcher.
   Reads data/surplus/leads.json (and events.json for the conveyance log).
   Status + notes live in localStorage under their own key, same as the other tabs. */
const SURPLUS_LS_KEY = "md_surplus_leads_local_v1";

const STAGE_META = {
    CONVEYED:           { label: "Deed Conveyed",      cls: "stage-conveyed", blurb: "Purchaser paid the residue of the bid — a balance is owed to the former owner under TP §14-818(a)(4)." },
    FORECLOSURE_WINDOW: { label: "Foreclosure Window", cls: "stage-window",   blurb: "Past the statutory wait, title has NOT moved. The owner still holds it and can still sell." },
    CERT_STALE:         { label: "Needs Verifying",    cls: "stage-stale",    blurb: "Over 2 years since the sale and the owner never changed. Could be a void certificate (§14-833 — your opening), a redemption, a case still pending, or a deed SDAT has not indexed. Check the county tax account before calling." },
    LIEN_STRANDED:      { label: "Lien Stranded",     cls: "stage-stranded", blurb: "The bid was far above what the property is worth. To take the deed the purchaser must pay that residue, which costs more than the property — so they almost certainly never will. The owner keeps it, still delinquent, and no other investor is looking." },
    TOO_EARLY:          { label: "Too Early",          cls: "stage-early",    blurb: "Inside the 6-month statutory wait — no foreclosure may be filed yet." },
};

const SurplusApp = {
    rows: [], events: [], local: {}, generated: "",
    state: { search: "", county: "", stage: "all", minSurplus: 0, status: "all", repeat: false, sort: "est_surplus", dir: -1, page: 1, perPage: 25 },

    async init() {
        this.loadLocal();
        this.bindEvents();
        await this.load();
    },

    async load() {
        try {
            const r = await fetch(`data/surplus/leads.json?t=${Date.now()}`, { cache: "no-store" });
            if (!r.ok) throw new Error(r.status);
            const j = await r.json();
            this.rows = (j.rows || []).map((x) => this.decorate(x));
            this.generated = j.generated_at || "";
        } catch { this.rows = []; }
        try {
            const r = await fetch(`data/surplus/events.json?t=${Date.now()}`, { cache: "no-store" });
            if (r.ok) this.events = ((await r.json()) || {}).events || [];
        } catch { this.events = []; }
        this.fillCounties();
        this.stats();
        this.render();
    },

    decorate(r) {
        r.id = "sf-" + r.account;
        r._surplus = Number(r.est_surplus || 0);
        r._bid = Number(r.winning_bid || 0);
        r._consid = Number(r.consideration || 0);
        r._assessed = Number(r.assessed_value || 0);
        r._blob = [r.address, r.city, r.county, r.owner_of_record, r.owner_at_sale, r.sold_to, r.account]
            .filter(Boolean).join(" ").toLowerCase();
        return r;
    },

    fillCounties() {
        const counties = [...new Set(this.rows.map((r) => r.county).filter(Boolean))].sort();
        document.getElementById("surplusCounty").innerHTML =
            '<option value="">All counties</option>' +
            counties.map((c) => `<option value="${this.esc(c)}">${this.esc(c)}</option>`).join("");
    },

    stats() {
        const conveyed = this.rows.filter((r) => r.stage === "CONVEYED");
        const window_ = this.rows.filter((r) => r.stage === "FORECLOSURE_WINDOW" || r.stage === "CERT_STALE" || r.stage === "LIEN_STRANDED");
        const tracked = this.rows.filter((r) => r.deed_rational).reduce((n, r) => n + r._surplus, 0);
        document.getElementById("sstatConveyed").textContent = conveyed.length.toLocaleString();
        document.getElementById("sstatWindow").textContent = window_.length.toLocaleString();
        const rc = document.getElementById("sstatRepeat"); if (rc) rc.textContent = this.rows.filter((r) => r.repeat_sale).length.toLocaleString();
        document.getElementById("sstatTracked").textContent = tracked >= 1e6
            ? "$" + (tracked / 1e6).toFixed(1) + "M" : "$" + Math.round(tracked).toLocaleString();
        const info = document.getElementById("surplusRunInfo");
        if (!this.rows.length) { info.textContent = ""; return; }
        const hist = this.rows.filter((r) => r.historical).length;
        info.innerHTML = `<b>${this.rows.length.toLocaleString()}</b> opportunities`
            + (hist ? ` · <b>${hist.toLocaleString()}</b> found from collector deeds in the land records` : "")
            + (this.events.length ? ` · <b>${this.events.length.toLocaleString()}</b> new conveyance${this.events.length === 1 ? "" : "s"} caught week-over-week` : "")
            + (this.generated ? ` · updated ${this.esc(this.fmtDT(this.generated))}` : "");
    },

    loadLocal() { try { this.local = JSON.parse(localStorage.getItem(SURPLUS_LS_KEY) || "{}"); } catch { this.local = {}; } },
    saveLocal() { try { localStorage.setItem(SURPLUS_LS_KEY, JSON.stringify(this.local)); } catch {} },
    statusOf(r) { return (this.local[r.id] && this.local[r.id].status) || "new"; },
    notesOf(r) { return (this.local[r.id] && this.local[r.id].notes) || ""; },

    bindEvents() {
        let t;
        document.getElementById("surplusSearch").addEventListener("input", (e) => {
            clearTimeout(t); t = setTimeout(() => { this.state.search = e.target.value.trim().toLowerCase(); this.state.page = 1; this.render(); }, 200);
        });
        document.getElementById("surplusCounty").addEventListener("change", (e) => { this.state.county = e.target.value; this.state.page = 1; this.render(); });
        const slider = document.getElementById("surplusMin");
        slider.addEventListener("input", (e) => {
            const v = Number(e.target.value);
            document.getElementById("surplusMinVal").textContent = v ? "$" + (v / 1000) + "k" : "$0";
            this.state.minSurplus = v; this.state.page = 1; this.render();
        });
        document.querySelectorAll("#surplusStages .filter-btn").forEach((b) => b.addEventListener("click", () => {
            document.querySelectorAll("#surplusStages .filter-btn").forEach((x) => x.classList.remove("active"));
            b.classList.add("active"); this.state.stage = b.dataset.stage; this.state.page = 1; this.render();
        }));
        const rep = document.getElementById("surplusRepeat");
        if (rep) rep.addEventListener("change", (e) => { this.state.repeat = e.target.checked; this.state.page = 1; this.render(); });
        document.querySelectorAll("#surplusStatusFilters .filter-btn").forEach((b) => b.addEventListener("click", () => {
            document.querySelectorAll("#surplusStatusFilters .filter-btn").forEach((x) => x.classList.remove("active"));
            b.classList.add("active"); this.state.status = b.dataset.status; this.state.page = 1; this.render();
        }));
        document.querySelectorAll("#surplusTable th[data-sort]").forEach((th) => th.addEventListener("click", () => {
            const k = th.dataset.sort;
            this.state.dir = this.state.sort === k ? -this.state.dir : -1;
            this.state.sort = k; this.render();
        }));
        document.getElementById("surplusPrev").addEventListener("click", () => { if (this.state.page > 1) { this.state.page--; this.render(); } });
        document.getElementById("surplusNext").addEventListener("click", () => { this.state.page++; this.render(); });
        document.getElementById("surplusExport").addEventListener("click", () => this.exportCSV());
    },

    filtered() {
        const s = this.state;
        return this.rows.filter((r) =>
            (s.stage === "all" || r.stage === s.stage) &&
            (!s.county || r.county === s.county) &&
            (!s.minSurplus || r._surplus >= s.minSurplus) &&
            (!s.repeat || r.repeat_sale) &&
            (s.status === "all" || this.statusOf(r) === s.status) &&
            (!s.search || r._blob.includes(s.search))
        );
    },

    sorted(rows) {
        const k = this.state.sort, d = this.state.dir;
        const order = { CONVEYED: 4, FORECLOSURE_WINDOW: 3, CERT_STALE: 2, TOO_EARLY: 1 };
        const val = (r) => ({
            stage: order[r.stage] || 0, est_surplus: r._surplus, bid: r._bid, assessed: r._assessed,
            county: r.county || "", owner: r.owner_of_record || "", days: r.days_since_sale || 0,
        }[k]);
        return rows.slice().sort((a, b) => { const x = val(a), y = val(b); return (x > y ? 1 : x < y ? -1 : 0) * d; });
    },

    render() {
        const rows = this.sorted(this.filtered());
        const pages = Math.max(1, Math.ceil(rows.length / this.state.perPage));
        if (this.state.page > pages) this.state.page = pages;
        const start = (this.state.page - 1) * this.state.perPage;
        const page = rows.slice(start, start + this.state.perPage);
        const tbody = document.getElementById("surplusBody");
        const empty = document.getElementById("surplusEmpty");
        if (!page.length) {
            tbody.innerHTML = ""; empty.style.display = "block";
            if (this.rows.length) {
                document.getElementById("surplusEmptyTitle").textContent = "No matches";
                document.getElementById("surplusEmptyText").textContent = "Loosen the stage, county, minimum surplus, or search.";
            }
        } else {
            empty.style.display = "none";
            tbody.innerHTML = page.map((r) => {
                const st = this.statusOf(r), m = STAGE_META[r.stage] || { label: r.stage, cls: "" };
                return `<tr data-id="${this.esc(r.id)}">
                    <td><span class="stage ${m.cls}" title="${this.esc(m.blurb || "")}">${this.esc(m.label)}</span>
                        ${r.deed_rational && r.stage !== "CONVEYED" ? '<span class="chip chip-good" title="Bid is at or below assessed value, so taking the deed is profitable — this case is likely to complete">deed likely</span>' : ""}
                        ${r.vacant_lot ? '<span class="chip" title="No improvements on the parcel">vacant lot</span>' : ""}
                        ${r.repeat_sale ? `<span class="chip chip-hard" title="Sold at tax sale in ${(r.years_listed || []).join(", ")} — did not pay, nobody foreclosed, more than once">repeat ×${(r.years_listed || [2]).length}</span>` : ""}
                        ${r.historical ? '<span class="chip" title="Found from the collector deed in SDAT, not from a tax-sale list — this one reaches back before our list coverage">historical</span>' : ""}
                        ${r.conveyance_confidence === "HIGH" ? '<span class="chip chip-good" title="Owner on title today is the lien bidder named on the sale list">bidder holds title</span>' : ""}
                        ${r.conveyance_confidence === "MEDIUM" ? '<span class="chip" title="Title moved to an entity after the sale — tax-sale buyers are almost always LLCs">title moved</span>' : ""}
                        ${r.partial ? '<span class="chip" title="This year\'s list came from an Internet Archive capture of page 1 only — the county had more rows than we could recover">archive · partial</span>' : ""}
                        ${r.resold ? '<span class="chip" title="The tax-sale purchaser took the deed and has since sold the property to someone else. The surplus was owed to the owner BEFORE the collector deed — the current owner is a later buyer with no claim">resold since</span>' : ""}</td>
                    <td class="property-cell">
                        <div class="address"${r.no_situs ? ' style="color:var(--text-secondary); font-weight:400; font-size:0.8rem"' : ""}>${r.no_situs ? "no street address — " : ""}${this.esc(r.address || "N/A")}${r.city && !r.no_situs ? `, ${this.esc(r.city)}` : ""}</div>
                        <div class="meta" style="font-family:var(--font-mono)">${this.esc(r.account)}${r.year_built && String(r.year_built).replace(/0/g, "") ? ` · built ${this.esc(r.year_built)}` : ""}${r.no_situs && r.city ? ` · ${this.esc(r.city)}` : ""}</div>
                    </td>
                    <td class="name-cell" style="font-family:var(--font-mono); font-size:0.8rem">${this.esc(r.owner_of_record || "—")}
                        ${r.owner_at_sale && r.owner_at_sale !== r.owner_of_record ? `<div style="color:var(--text-dim); font-size:0.7rem">at sale: ${this.esc(r.owner_at_sale)}</div>` : ""}
                        ${this.notesOf(r) ? '<span class="note-badge" title="Has notes">✎</span>' : ""}</td>
                    <td class="county-cell">${this.esc(r.county || "")}</td>
                    <td class="date-cell">${this.esc(String(r.tax_sale_year || "—"))} <span class="yrs">${r.historical
                        ? (r.years_since_conveyance != null ? `(deeded ${r.years_since_conveyance}y ago)` : "")
                        : `(${Math.round((r.days_since_sale || 0) / 30)}mo)`}</span></td>
                    <td class="value-cell">${r._bid ? this.money(r._bid) : (r._consid
                        ? `<span title="No sale list covers this year, so this is the consideration recited on the collector's deed — on a tax deed normally the full purchase price">${this.money(r._consid)} <span class="yrs">deed</span></span>` : "—")}</td>
                    <td class="equity-cell" style="font-family:var(--font-mono); ${r.stage === "LIEN_STRANDED" ? "color:var(--yellow)" : (r._surplus > 0 && r.deed_rational ? "color:var(--green); font-weight:600" : "color:var(--text-secondary)")}">${
                        r.stage === "LIEN_STRANDED"
                            ? (r.bid_to_assessed != null ? (r.bid_to_assessed).toFixed(1) + "\u00d7 value" : (r.bid_to_face != null ? Math.round(r.bid_to_face) + "\u00d7 taxes" : "overbid"))
                            : (r.est_surplus == null ? "\u2014" : this.money(r._surplus))}</td>
                    <td><span class="status-badge ${st}">${st.toUpperCase()}</span></td>
                </tr>`;
            }).join("");
            tbody.querySelectorAll("tr").forEach((tr) => tr.addEventListener("click", () => this.open(tr.dataset.id)));
        }
        const total = rows.length;
        document.getElementById("surplusPageInfo").textContent = total
            ? `Showing ${start + 1}–${Math.min(start + this.state.perPage, total)} of ${total} parcels` : "No parcels";
        document.getElementById("surplusPrev").disabled = this.state.page <= 1;
        document.getElementById("surplusNext").disabled = this.state.page >= pages;
    },

    open(id) {
        const r = this.rows.find((x) => x.id === id);
        if (!r) return;
        const m = STAGE_META[r.stage] || { label: r.stage, blurb: "" };
        const ev = this.events.find((e) => e.account === r.account);
        document.getElementById("modalBody").innerHTML = `
            <div class="detail-section">
                <h3>Stage</h3>
                <div class="detail-row"><span class="label">Where it stands</span><span class="value" style="font-weight:600">${this.esc(m.label)}</span></div>
                <div class="detail-row"><span class="label">Meaning</span><span class="value" style="font-size:0.82rem">${this.esc(m.blurb)}</span></div>
                ${r.conveyance_note ? `<div class="detail-row"><span class="label">How we know</span><span class="value" style="font-size:0.82rem">${this.esc(r.conveyance_note)}</span></div>` : ""}
                <div class="detail-row"><span class="label">Since sale</span><span class="value">${r.days_since_sale} days${r.tax_sale_year ? ` · ${this.esc(String(r.tax_sale_year))} sale` : ""}</span></div>
                ${r.repeat_sale ? `<div class="detail-row"><span class="label">Repeat</span><span class="value" style="color:var(--yellow); font-weight:600">Sold at tax sale in ${(r.years_listed || []).join(", ")} — the owner did not pay and nobody foreclosed, ${(r.years_listed || []).length} times</span></div>` : ""}
            </div>
            <div class="detail-section">
                <h3>Property</h3>
                <div class="detail-row"><span class="label">Address</span><span class="value" style="font-weight:600">${this.esc(r.address || "")}${r.city ? `, ${this.esc(r.city)}` : ""} ${this.esc(r.zip || "")}</span></div>
                <div class="detail-row"><span class="label">County</span><span class="value" style="color:var(--purple)">${this.esc(r.county || "")}</span></div>
                <div class="detail-row"><span class="label">Account #</span><span class="value" style="font-family:var(--font-mono)">${this.esc(r.account)}</span></div>
                <div class="detail-row"><span class="label">Assessed</span><span class="value" style="color:var(--green); font-family:var(--font-mono)">${this.money(r._assessed)}</span></div>
                <div class="detail-row"><span class="label">Deed on record</span><span class="value" style="font-family:var(--font-mono)">${this.esc(r.deed || "—")} · ${this.esc(r.transfer_date || "—")}${r.grantor ? ` · from ${this.esc(r.grantor)}` : ""}</span></div>
                <div class="detail-row"><span class="label">Occupancy</span><span class="value">${r.occupancy === "H" ? "Owner-occupied" : "Not owner-occupied"}</span></div>
            </div>
            <div class="detail-section">
                <h3>Owner</h3>
                <div class="detail-row"><span class="label">On record now</span><span class="value" style="font-family:var(--font-mono)">${this.esc(r.owner_of_record || "—")}${r.owner2 ? `<br>${this.esc(r.owner2)}` : ""}</span></div>
                <div class="detail-row"><span class="label">Name at tax sale</span><span class="value" style="font-family:var(--font-mono)">${r.owner_at_sale ? this.esc(r.owner_at_sale) : (r.historical ? '<span style="color:var(--text-dim); font-family:var(--font-sans); font-size:0.8rem">not in SDAT — on the deed, see above</span>' : "—")}</span></div>
                ${r.legal ? `<div class="detail-row"><span class="label">Legal</span><span class="value" style="font-size:0.8rem">${this.esc(r.legal)}</span></div>` : ""}
                ${r.mail ? `<div class="detail-row"><span class="label">Mailing address</span><span class="value">${this.esc(r.mail)}</span></div>` : ""}
            </div>
            ${r.historical ? `<div class="detail-section">
                <h3>Who Is Owed The Surplus</h3>
                <p style="font-size:0.85rem; line-height:1.55">The balance was owed to whoever owned this parcel <b>before</b> the collector's deed.
                SDAT keeps only the current owner, so that name is not in our data. It is on the deed itself: pull
                <b style="font-family:var(--font-mono)">Liber / Folio ${this.esc(r.deed || "?")}</b> for ${this.esc(r.county || "")} County at
                mdlandrec.net (free registration) — the former owner is named as the party whose interest was foreclosed. The county's
                § 14-818(a)(6) notice list names them too, and says whether the balance was ever claimed.</p>
            </div>` : ""}
            <div class="detail-section">
                <h3>The Money</h3>
                ${r._bid ? `<div class="detail-row"><span class="label">Winning bid</span><span class="value" style="font-family:var(--font-mono)">${this.money(r._bid)}</span></div>`
                    : r._consid ? `<div class="detail-row"><span class="label">Deed consideration</span><span class="value" style="font-family:var(--font-mono)">${this.money(r._consid)} <span style="color:var(--text-dim); font-family:var(--font-sans); font-size:0.78rem">— recited on the collector's deed; on a tax deed this is normally the full purchase price</span></span></div>`
                    : ""}
                ${r.consideration_to_assessed != null ? `<div class="detail-row"><span class="label">Consideration ÷ assessed</span><span class="value">${(r.consideration_to_assessed * 100).toFixed(0)}%</span></div>` : ""}
                <div class="detail-row"><span class="label">Taxes owed at sale</span><span class="value" style="font-family:var(--font-mono)">${r.taxes_owed ? this.money(r.taxes_owed) : '<span style="color:var(--text-dim); font-family:var(--font-sans); font-size:0.8rem">not on any list we hold — only the collector\'s record has it</span>'}</span></div>
                <div class="detail-row"><span class="label">Est. surplus</span><span class="value" style="font-family:var(--font-mono); color:${r.est_surplus != null ? "var(--green)" : "var(--text-secondary)"}; font-weight:600">${r.est_surplus != null ? this.money(r._surplus)
                    : (r._consid ? `<span style="font-family:var(--font-sans); font-weight:400; font-size:0.82rem">≈ ${this.money(r._consid)} minus taxes, interest, penalties and costs of sale</span>` : "unknown")}</span></div>
                <div class="detail-row"><span class="label">Bid ÷ assessed</span><span class="value">${r.bid_to_assessed == null ? "—" : (r.bid_to_assessed * 100).toFixed(0) + "%"} ${r.bid_to_assessed == null ? "" : r.deed_rational
                    ? '<span class="chip chip-good">deed is profitable — case likely completes</span>'
                    : '<span class="chip chip-hard">bid exceeds value — purchaser is stranded, owner keeps it</span>'}</span></div>
                ${r.sold_to ? `<div class="detail-row"><span class="label">Lien buyer</span><span class="value">${this.esc(r.sold_to)}</span></div>` : ""}
                <div class="detail-row"><span class="label">Source</span><span class="value" style="font-size:0.78rem">${this.esc(r.source || "")}</span></div>
                <p style="font-size:0.76rem; color:var(--text-dim); margin:8px 0 0">Estimate only: bid minus the tax figure on the sale list. The exact balance is taxes + interest + penalties + costs of sale, which only the collector's record shows.</p>
            </div>
            ${ev ? `<div class="detail-section">
                <h3>Conveyance Detected</h3>
                <div class="detail-row"><span class="label">Owner before</span><span class="value" style="font-family:var(--font-mono)">${this.esc(ev.owner_before || "")}</span></div>
                <div class="detail-row"><span class="label">Owner after</span><span class="value" style="font-family:var(--font-mono); color:var(--yellow)">${this.esc(ev.owner_after || "")}</span></div>
                <div class="detail-row"><span class="label">Detected</span><span class="value">${this.esc(this.fmtDT(ev.detected))}</span></div>
                ${ev.notice_due_by ? `<div class="detail-row"><span class="label">Collector's notice due</span><span class="value" style="color:var(--yellow)">${this.esc(ev.notice_due_by)} — §14-818(a)(6), 90 days from the deed</span></div>` : ""}
            </div>` : ""}
            <div class="detail-section">
                <h3>Next Step</h3>
                <p style="font-size:0.85rem; line-height:1.55">${this.nextStep(r)}</p>
            </div>
            <div class="detail-section">
                <h3>Imagery</h3>
                ${Aerial.panel(r, r.id)}
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

    nextStep(r) {
        if (r.stage === "CONVEYED") {
            return (r.historical ? "Found from the collector's deed in the land records, so we know the foreclosure completed but not who owned it "
                + "before — SDAT only keeps the current owner. The county's surplus list (or the prior deed) gives you the name. " : "")
                + "The property is gone — this is a <b>surplus</b> lead, not a buy lead. The former owner is owed the balance. "
                + "Confirm the exact amount and whether it has been claimed by asking the county collector (see "
                + "<code>docs/surplus-mpia-request.md</code>), then tell the former owner the money exists and point them at the county's "
                + "free claim form. Do not offer to buy or take assignment of the claim without a Maryland lawyer signing off first.";
        }
        if (r.stage === "FORECLOSURE_WINDOW") {
            return "<b>Best buy lead of the four stages.</b> The lien sold but title has not moved, so the owner still holds it and can still sell. "
                + "They lose everything if the purchaser forecloses, so a sale that pays off the lien leaves them with something instead of nothing. "
                + "Move now — the window closes when the purchaser files.";
        }
        if (r.stage === "LIEN_STRANDED") {
            return "<b>Buy lead, and an uncontested one.</b> The purchaser bid far more than the property is worth. Under §14-818(a)(2) the "
                + "residue of that bid stays on credit until they take the deed — so taking it would cost them more than the property. They "
                + "will not. They are waiting on a redemption that pays interest, and if it never comes the certificate goes void at two years "
                + "and §14-833(d)(1) forfeits their money to the taxes on this very parcel.<br><br>"
                + "Meanwhile the owner still owns it and is still delinquent. Every other investor's list says “sold at tax sale” and skips it. "
                + "You can buy from the owner and redeem — and before a complaint is filed, §14-843 caps the purchaser's attorney fee at $500, "
                + "so redeeming is cheap. Confirm the lien is still unredeemed on the county tax account first.";
        }
        if (r.stage === "CERT_STALE") {
            return "<b>Verify before you call.</b> Over two years since the sale with the owner unchanged fits four different realities: the "
                + "certificate went void under §14-833 and the owner still owns it free of that purchaser (your opening); the owner redeemed long "
                + "ago; a complaint was filed in time and the case is still pending; or a deed was recorded and SDAT has not indexed it. "
                + "The county tax account tells you which. Do not tell someone they still own their home until you have checked.";
        }
        return "Too early to act. No foreclosure may be filed until the statutory wait runs. Worth a look now if the owner already has other "
            + "distress signals on the Title Leads tab.";
    },

    exportCSV() {
        const rows = this.sorted(this.filtered());
        if (!rows.length) { alert("Nothing to export with the current filters."); return; }
        const H = ["Stage", "Stage Meaning", "Deed Likely", "Bid/Taxes", "Deed Consideration", "Years Since Deed", "Legal Description", "Years Sold at Tax Sale", "Account #", "Address", "City", "Zip", "County",
            "Owner on Record", "Owner 2", "Owner at Tax Sale", "Mailing Address", "Tax Sale Year", "Tax Sale Status",
            "Lien Buyer", "Winning Bid", "Taxes Owed", "Est. Surplus", "Assessed", "Bid/Assessed", "Days Since Sale",
            "Repeat Sale", "Deed on Record", "Last Transfer", "Grantor", "Year Built", "Occupancy", "Lat", "Lon", "Source", "Status", "Notes"];
        const q = (v) => { const s = String(v == null ? "" : v); return /^=".*"$/.test(s) ? s : `"${s.replace(/"/g, '""')}"`; };
        const lines = [H.join(",")];
        rows.forEach((r) => {
            const m = STAGE_META[r.stage] || {};
            lines.push([m.label || r.stage, m.blurb || "", r.deed_rational ? "YES" : "no", r.bid_to_face, r.consideration, r.years_since_conveyance, r.legal, (r.years_listed || []).join(" "), `="${r.account}"`,
                r.address, r.city, r.zip, r.county, r.owner_of_record, r.owner2, r.owner_at_sale, r.mail,
                r.tax_sale_year, r.tax_sale_status, r.sold_to, r.winning_bid, r.taxes_owed, r.est_surplus,
                r.assessed_value, r.bid_to_assessed, r.days_since_sale, r.repeat_sale ? "YES" : "no",
                r.deed, r.transfer_date, r.grantor, r.year_built, r.occupancy, r.lat, r.lon, r.source,
                this.statusOf(r), this.notesOf(r)].map(q).join(","));
        });
        const blob = new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = `md-surplus-leads-${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a); a.click(); a.remove();
        setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    },

    money(v) { const n = Number(v); return (v == null || v === "" || isNaN(n)) ? "—" : "$" + Math.round(n).toLocaleString(); },
    fmtDT(s) { const d = new Date(s); return isNaN(d) ? (s || "") : d.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }); },
    esc(s) { return s == null ? "" : String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); },
};
