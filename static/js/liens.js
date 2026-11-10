/* Lien Leverage tab — Tim Bender's play: owners who think their house can't be sold because of
   judgments / state tax liens, especially old (about-to-expire) liens, liens on only one co-owner,
   and liens stacked on dead or absent owners. Data: data/judgments/leads.json, written weekly by
   engine2/judgment_watch.py from the Maryland Judiciary Judgment and Liens Search, matched to SDAT
   owners. Row id = SDAT account, the same id the other tabs use, so notes follow the parcel. */
const LiensApp = {
    rows: [], meta: {},
    state: { search: "", county: "", tag: "", minOwed: 0, hideOcc: true, highOnly: false, sort: "score", dir: -1, page: 1, perPage: 25 },

    TAG_LABEL: {
        STACKED: "Stacked liens", GOV_LIEN: "State / gov lien", EXPIRING: "Expiring ≤2 yrs", OLD_LIEN: "Old lien (8+ yrs)",
        ONE_SPOUSE_ONLY: "Only one spouse owes", ONE_OWNER_ONLY: "Only one co-owner owes", DEAD_OWNER: "Owner died / estate",
        CO_OWNER_DEAD: "Co-owner died", NOT_OWNER_OCCUPIED: "Not owner-occupied", OUT_OF_STATE: "Out of state", DISTRESS: "Distress signal",
        LONG_HELD: "Long-held", OWNER_OCCUPIED: "Owner-occupied", LIEN_BUY: "💰 Lien-buy candidate", FED_LIEN: "Federal lien", FED_OLD: "🏛 Old federal lien (8+ yrs)",
    },
    CTYPE: { GOV: "government", CONSUMER: "consumer debt — license needed to buy", STORE: "store / restitution", PERSON: "private person", BUSINESS: "business" },
    HOT: ["LIEN_BUY", "FED_OLD", "DEAD_OWNER", "CO_OWNER_DEAD", "EXPIRING", "ONE_SPOUSE_ONLY", "ONE_OWNER_ONLY", "GOV_LIEN", "STACKED"],

    async init() {
        this.bindEvents();
        try {
            const r = await fetch(`data/judgments/leads.json?t=${Date.now()}`, { cache: "no-store" });
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
        r.tags = r.tags || []; r.cautions = r.cautions || []; r.judgments = r.judgments || [];
        r._high = r.judgments.some((j) => j.confidence === "HIGH");
        r._oldest = Math.max(0, ...r.judgments.map((j) => j.age_years || 0));
        r._blob = [r.address, r.city, r.county, r.owner, r.owner2, r.account, r.legal,
            ...r.judgments.map((j) => `${j.case} ${j.creditor} ${j.debtor}`)].filter(Boolean).join(" ").toLowerCase();
        return r;
    },

    fillCounties() {
        const cs = [...new Set(this.rows.map((r) => r.county).filter(Boolean))].sort();
        document.getElementById("lienCounty").innerHTML = '<option value="">All counties</option>'
            + cs.map((c) => `<option value="${this.esc(c)}">${this.esc(c)}</option>`).join("");
    },

    stats() {
        const rs = this.rows.filter((r) => !r.occupied);
        const set = (id, v) => { document.getElementById(id).textContent = v; };
        set("jstatTotal", rs.length.toLocaleString());
        set("jstatDead", rs.filter((r) => r.tags.includes("DEAD_OWNER") || r.tags.includes("CO_OWNER_DEAD")).length.toLocaleString());
        set("jstatExpiring", rs.filter((r) => r.tags.includes("EXPIRING")).length.toLocaleString());
        set("jstatShare", rs.filter((r) => r.tags.includes("ONE_SPOUSE_ONLY") || r.tags.includes("ONE_OWNER_ONLY")).length.toLocaleString());
        set("jstatGov", rs.filter((r) => r.tags.includes("GOV_LIEN")).length.toLocaleString());
        const m = this.meta, info = document.getElementById("lienRunInfo");
        const st = m.stats || {};
        const vs = Object.values(st).filter((v) => v && typeof v === "object"), open = vs.reduce((n, v) => n + (v.open || 0), 0), loading = vs.filter((v) => !v.backfill_done).length;
        const prog = `<b>${open.toLocaleString()}</b> open judgments &amp; liens across ${vs.length} counties → <b>${rs.length.toLocaleString()}</b> non-homestead properties hit`
            + (loading ? ` · <span style="color:var(--yellow)">12-year history still loading in ${loading} count${loading > 1 ? "ies" : "y"}</span>` : "");
        info.innerHTML = m.generated_at ? `${prog} · updated ${this.esc(this.fmtDT(m.generated_at))}` : "";
    },

    bindEvents() {
        let t;
        document.getElementById("lienSearch").addEventListener("input", (e) => {
            clearTimeout(t); t = setTimeout(() => { this.state.search = e.target.value.trim().toLowerCase(); this.state.page = 1; this.render(); }, 200);
        });
        const on = (id, fn) => document.getElementById(id).addEventListener("change", (e) => { fn(e.target); this.state.page = 1; this.render(); });
        on("lienCounty", (el) => { this.state.county = el.value; });
        on("lienTag", (el) => { this.state.tag = el.value; });
        on("lienMin", (el) => { this.state.minOwed = Number(el.value) || 0; });
        on("lienHideOcc", (el) => { this.state.hideOcc = el.checked; });
        on("lienHigh", (el) => { this.state.highOnly = el.checked; });
        document.querySelectorAll("#lienTable th[data-sort]").forEach((th) => th.addEventListener("click", () => {
            const k = th.dataset.sort;
            this.state.dir = this.state.sort === k ? -this.state.dir : -1;
            this.state.sort = k; this.render();
        }));
        document.getElementById("lienPrev").addEventListener("click", () => { if (this.state.page > 1) { this.state.page--; this.render(); } });
        document.getElementById("lienNext").addEventListener("click", () => { this.state.page++; this.render(); });
        document.getElementById("lienExport").addEventListener("click", () => this.exportCSV());
    },

    filtered() {
        const s = this.state;
        return this.rows.filter((r) =>
            (!s.county || r.county === s.county)
            && (!s.tag || (s.tag === "SHARE" ? (r.tags.includes("ONE_SPOUSE_ONLY") || r.tags.includes("ONE_OWNER_ONLY"))
                : s.tag === "DEAD" ? (r.tags.includes("DEAD_OWNER") || r.tags.includes("CO_OWNER_DEAD")) : r.tags.includes(s.tag)))
            && (r.total_owed || 0) >= s.minOwed
            && (!s.hideOcc || !r.occupied)
            && (!s.highOnly || r._high)
            && (!s.search || r._blob.includes(s.search)));
    },

    sorted(rows) {
        const k = this.state.sort, d = this.state.dir;
        const val = (r) => ({ score: r.score, owed: r.total_owed, assessed: r.assessed, count: r.judgments.length, oldest: r._oldest, county: r.county || "" }[k]);
        return rows.slice().sort((a, b) => { const x = val(a), y = val(b); return (x > y ? 1 : x < y ? -1 : 0) * d || b.score - a.score; });
    },

    statusOf(r) { const s = SharedNotes.get(r.id); return (s && s.status) || "new"; },
    notesOf(r)  { const s = SharedNotes.get(r.id); return (s && s.notes) || ""; },
    _sharedHook: document.addEventListener("mdpl:notes-loaded", () => { try { if (LiensApp.rows.length) LiensApp.render(); } catch {} }),

    scoreColor(n) { return n >= 75 ? "var(--green)" : n >= 60 ? "var(--cyan)" : n >= 45 ? "var(--yellow)" : "var(--text-secondary)"; },

    chips(r) {
        return r.tags.filter((t) => t !== "NOT_OWNER_OCCUPIED").map((t) => {
            const hot = this.HOT.includes(t), c = t === "OWNER_OCCUPIED" ? "var(--red)" : hot ? "var(--yellow)" : "var(--text-dim)";
            return `<span style="display:inline-block; font-size:0.66rem; padding:1px 6px; margin:2px 3px 0 0; border:1px solid ${c}; color:${c}; border-radius:10px">${this.esc(this.TAG_LABEL[t] || t)}</span>`;
        }).join("");
    },

    render() {
        const rows = this.sorted(this.filtered());
        const pages = Math.max(1, Math.ceil(rows.length / this.state.perPage));
        if (this.state.page > pages) this.state.page = pages;
        const start = (this.state.page - 1) * this.state.perPage;
        const page = rows.slice(start, start + this.state.perPage);
        const tbody = document.getElementById("lienBody"), empty = document.getElementById("lienEmpty");
        if (!page.length) {
            tbody.innerHTML = ""; empty.style.display = "block";
            if (this.rows.length) {
                document.getElementById("lienEmptyTitle").textContent = "No matches";
                document.getElementById("lienEmptyText").textContent = "Loosen the filters or the search.";
            }
        } else {
            empty.style.display = "none";
            tbody.innerHTML = page.map((r) => {
                const st = this.statusOf(r);
                const top = r.judgments.slice().sort((a, b) => b.amount - a.amount)[0] || {};
                return `<tr data-id="${this.esc(r.id)}">
                    <td style="font-family:var(--font-mono); font-weight:700; font-size:1.05rem; color:${this.scoreColor(r.score)}">${r.score}</td>
                    <td class="property-cell">
                        <div class="address">${this.esc(r.address || "(no street address)")}${r.city ? `, ${this.esc(r.city)}` : ""}</div>
                        <div class="meta" style="font-family:var(--font-mono)">Acct ${this.esc(r.account)}${r.deed ? ` · Deed ${this.esc(r.deed)}` : ""}</div>
                        <div>${this.chips(r)}</div>
                    </td>
                    <td class="name-cell" style="font-family:var(--font-mono); font-size:0.76rem">${this.esc(r.owner || "—")}${r.owner2 ? `<br>${this.esc(r.owner2)}` : ""}${r.mail_state && r.mail_state !== "MD" ? ` <span style="color:var(--yellow)">(${this.esc(r.mail_state)})</span>` : ""}${this.notesOf(r) ? '<span class="note-badge" title="Has notes">✎</span>' : ""}</td>
                    <td class="county-cell">${this.esc(r.county)}</td>
                    <td class="value-cell" style="font-family:var(--font-mono)">${this.money(r.total_owed)}<div style="font-size:0.7rem; color:var(--text-dim)">${r.judgments.length} lien${r.judgments.length > 1 ? "s" : ""}</div></td>
                    <td style="font-size:0.74rem">${this.esc(top.creditor || "—")}<div style="color:var(--text-dim)">${this.esc(top.entry || "")}${top.kind && top.kind !== "JUDGMENT" ? ` · ${this.esc(top.kind)}` : ""}</div></td>
                    <td style="font-family:var(--font-mono); font-size:0.76rem">${this.money(r.assessed)}</td>
                    <td><span class="status-badge ${st}">${st.toUpperCase()}</span></td>
                </tr>`;
            }).join("");
            tbody.querySelectorAll("tr").forEach((tr) => tr.addEventListener("click", () => this.open(tr.dataset.id)));
        }
        const total = rows.length;
        document.getElementById("lienPageInfo").textContent = total
            ? `Showing ${start + 1}–${Math.min(start + this.state.perPage, total)} of ${total.toLocaleString()} parcels` : "No parcels";
        document.getElementById("lienPrev").disabled = this.state.page <= 1;
        document.getElementById("lienNext").disabled = this.state.page >= pages;
    },

    row(label, value, style) {
        return `<div class="detail-row"><span class="label">${label}</span><span class="value"${style ? ` style="${style}"` : ""}>${value}</span></div>`;
    },

    open(id) {
        const r = this.rows.find((x) => x.id === id);
        if (!r) return;
        const jrows = r.judgments.map((j) => `<tr>
            <td style="font-family:var(--font-mono)"><a href="https://jportal.mdcourts.gov/judgment/details.jsf?selectedCaseId=${encodeURIComponent(j.case)}" target="_blank" rel="noopener">${this.esc(j.case)}</a></td>
            <td>${this.esc(j.creditor)}<div style="color:${j.buyable ? "var(--green)" : "var(--text-dim)"}">${this.esc(j.kind)}${j.creditor_type ? ` · ${this.esc(this.CTYPE[j.creditor_type] || j.creditor_type)}` : ""}${j.buyable ? " · 💰 could buy" : ""}</div>${j.case_type ? `<div style="color:var(--text-dim)">${this.esc(j.case_type)}</div>` : ""}</td>
            <td>${this.esc(j.debtor)}${j.confidence !== "HIGH" ? ` <span style="color:var(--yellow)" title="Matched on first + last name only">(confirm)</span>` : ""}</td>
            <td style="font-family:var(--font-mono)">${this.money(j.amount)}</td>
            <td style="font-family:var(--font-mono)">${this.esc(j.entry)}<div style="color:var(--text-dim)">${j.age_years} yrs</div></td>
            <td style="font-family:var(--font-mono)">${this.esc(j.expires)}</td></tr>`).join("");
        document.getElementById("modalBody").innerHTML = `
            <div class="detail-section">
                <h3>Why It Scored <span style="color:${this.scoreColor(r.score)}">${r.score}</span></h3>
                <ul style="margin:6px 0 0 18px; font-size:0.84rem; line-height:1.6">${r.reasons.map((x) => `<li>${this.esc(x)}</li>`).join("")}</ul>
                ${r.cautions.length ? `<div style="margin-top:10px; padding:8px 10px; border:1px solid var(--yellow); border-radius:var(--radius); font-size:0.8rem; line-height:1.5">
                    <b style="color:var(--yellow)">Check before you call</b>
                    <ul style="margin:4px 0 0 18px">${r.cautions.map((x) => `<li>${this.esc(x)}</li>`).join("")}</ul></div>` : ""}
            </div>
            <div class="detail-section">
                <h3>Open Judgments &amp; Liens <span style="font-size:0.75rem; color:var(--text-dim); font-weight:400">· ${this.money(r.total_owed)} total</span></h3>
                <div style="overflow-x:auto"><table style="width:100%; font-size:0.76rem; border-collapse:collapse" class="mini-table">
                    <thead><tr style="text-align:left; color:var(--text-dim)"><th>Case</th><th>Owed to</th><th>Debtor (as filed)</th><th>Amount</th><th>Entered</th><th>Lapses (if not renewed)</th></tr></thead>
                    <tbody>${jrows}</tbody></table></div>
                <div style="margin-top:6px; font-size:0.76rem; color:var(--text-dim)">Amounts are what was entered, not today's payoff (interest adds up). A Maryland judgment lasts 12 years and can be renewed — check the case for a renewal before counting on it lapsing. IRS liens aren't in this list; search Land Records.</div>
            </div>
            <div class="detail-section">
                <h3>Look It Up</h3>
                ${this.row("Address", `${this.esc(r.address || "(no street address)")}${r.city ? `, ${this.esc(r.city)}` : ""} ${this.esc(r.zip || "")}`, "font-weight:600")}
                ${this.row("County", this.esc(r.county), "color:var(--purple)")}
                ${this.row("SDAT Account #", this.esc(r.account), "font-family:var(--font-mono)")}
                ${r.deed ? this.row("Deed (liber/folio)", this.esc(r.deed), "font-family:var(--font-mono)") : ""}
                ${r.legal ? this.row("Legal", this.esc(r.legal), "font-family:var(--font-mono); font-size:0.78rem") : ""}
            </div>
            <div class="detail-section">
                <h3>Owner &amp; Money</h3>
                ${this.row("Owner", this.esc(r.owner || "—") + (r.owner2 ? `<br>${this.esc(r.owner2)}` : ""), "font-family:var(--font-mono); color:var(--yellow)")}
                ${this.row("Tax bill mailed to", this.esc(r.mail || "—") + (r.mail_state && r.mail_state !== "MD" ? ` <b style="color:var(--yellow)">(out of state)</b>` : ""), "font-family:var(--font-mono); font-size:0.8rem")}
                ${this.row("Owner-occupied", r.occupied ? `<b style="color:var(--red)">Yes (homestead)</b>` : "No")}
                ${this.row("Last transfer", this.esc(r.transfer_date || "—"), "font-family:var(--font-mono)")}
                ${this.row("Assessed", this.money(r.assessed), "font-family:var(--font-mono)")}
                ${this.row("Assessed minus liens", `${this.money(r.equity_est)} <span style="color:var(--text-dim)">(mortgages not counted — check Land Records)</span>`, "font-family:var(--font-mono)")}
                ${r.title_class ? this.row("Title Scan", this.esc(r.title_class), "font-size:0.8rem") : ""}
                ${r.title_flags && r.title_flags.length ? this.row("Title Scan flags", this.esc(r.title_flags.join(", ")), "color:var(--red); font-size:0.78rem") : ""}
            </div>
            <div class="detail-section">
                <h3>How to Work It</h3>
                <ol style="margin:4px 0 0 18px; font-size:0.83rem; line-height:1.6">
                    <li><b>Confirm the debtor is the owner</b> — open the case in Case Search and compare the party address to the property / tax-bill address.</li>
                    <li><b>Land Records</b> — open mortgages and their releases, IRS (NFTL) or DOJ liens, and how the owners hold title (tenants by the entirety shields a house from one spouse's debts).</li>
                    <li><b>Obituaries, Find a Grave, Ancestry</b> for every owner and heir — who's alive, who signs.</li>
                    <li><b>Sort each lien:</b> about to lapse · negotiable (creditor's real recovery is small) · only on one share · real and senior.</li>
                    <li><b>Buy the interest cheap</b> and get paid by buyout, a note, partition, or curing and reselling.</li>
                </ol>
                ${r.tags.includes("LIEN_BUY") ? `<div style="margin-top:10px; padding:8px 10px; border:1px solid var(--green); border-radius:var(--radius); font-size:0.82rem; line-height:1.55">
                    <b style="color:var(--green)">💰 Lien-buy play (Logan Fullmer)</b>
                    <ol style="margin:4px 0 0 18px">
                        <li>Land Records: confirm the property is free &amp; clear (or there's equity behind this lien) and the judgment wasn't released.</li>
                        <li>Call the creditor (the business or person owed — the green row above). Offer cash for the judgment; old ones often sell for pennies.</li>
                        <li>Get a signed assignment of judgment and file it in the case with the clerk so you're the creditor of record.</li>
                        <li>Send the owner a payoff demand: judgment + post-judgment interest + costs.</li>
                        <li>Unpaid? Ask the court for a writ of execution on the property — it leads to a sheriff's sale (see the Writ Watch tab).</li>
                    </ol>
                    <div style="color:var(--text-dim); margin-top:4px">Don't buy consumer-debt judgments (debt buyers, banks, medical, landlords) — Maryland requires a collection agency license to collect those. Have an attorney paper your first one.</div>
                </div>` : ""}
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
            SharedNotes.save("liens", r.id, sel.value, notes.value, `${r.address || r.account}, ${r.county} — ${this.money(r.total_owed)} in liens${r.owner ? " — " + r.owner : ""}`);
            App.closeModal(); this.render();
        };
        document.getElementById("modalOverlay").classList.add("active");
    },

    exportCSV() {
        const rows = this.sorted(this.filtered());
        if (!rows.length) { alert("Nothing to export with the current filters."); return; }
        const H = ["Score", "Account #", "Address", "City", "Zip", "County", "Owner", "Owner 2", "Mailing", "Mail State", "Owner-Occupied",
            "Assessed", "Total Owed", "Assessed Minus Liens", "Lien Count", "Liens (case · creditor · amount · entered · lapses)",
            "Tags", "Cautions", "Deed", "Last Transfer", "Status", "Notes"];
        const q = (v) => { const s = String(v == null ? "" : v); return /^=".*"$/.test(s) ? s : `"${s.replace(/"/g, '""')}"`; };
        const lines = [H.join(",")];
        rows.forEach((r) => lines.push([r.score, `="${r.account}"`, r.address, r.city, r.zip, r.county, r.owner, r.owner2, r.mail, r.mail_state,
            r.occupied ? "Y" : "N", r.assessed, r.total_owed, r.equity_est, r.judgments.length,
            r.judgments.map((j) => `${j.case} · ${j.creditor} · $${j.amount} · ${j.entry} · ${j.expires}`).join(" | "),
            r.tags.map((t) => this.TAG_LABEL[t] || t).join("; "), r.cautions.join(" | "), r.deed, r.transfer_date, this.statusOf(r), this.notesOf(r)].map(q).join(",")));
        const blob = new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = `md-lien-leverage-${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a); a.click(); a.remove();
        setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    },

    money(v) { const n = Number(v); return (v == null || v === "" || isNaN(n)) ? "—" : "$" + Math.round(n).toLocaleString(); },
    fmtDT(s) { const d = new Date(s); return isNaN(d) ? (s || "") : d.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }); },
    esc(s) { return s == null ? "" : String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); },
};
