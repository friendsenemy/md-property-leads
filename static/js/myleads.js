/* My Leads: a simple CRM over the shared notes sheet.
   Every property anyone has saved a note on (any tab) shows here, newest activity first,
   with its status, latest note and the full note history. Leads get in three ways:
     1. a note saved on any card (automatic),
     2. the "+ Add to My Leads" button on any card (status "pursuing"),
     3. the "+ Add a lead" form here, for anything not in the tool (VA house, a tip, a referral).
   Nothing new is stored: it all reads/writes the same sheet SharedNotes already uses. */
const MyLeadsApp = {
    state: { search: "", status: "mine", when: "all", page: 1, perPage: 30 },
    STATUS: {
        pursuing: "Working it", working: "Working it", hot: "Hot", new: "New", contacted: "Contacted", followup: "Follow up",
        verified: "Verified", paid: "Paid out / dead", dead: "Dead / pass", closed: "Closed",
    },
    DONE: ["dead", "paid", "closed"],
    ADDED: "Added to My Leads", REMOVED: "Removed from My Leads",
    TAB_NAME: { title: "Title Leads", d4d: "Driving for Dollars", surplus: "Surplus", vested: "Failed Foreclosure", land: "Dirt Deals", myleads: "Added by hand", obits: "Death Leads" },
    COUNTY: { "01": "allegany", "02": "anne-arundel", "03": "baltimore-city", "04": "baltimore-county", "05": "calvert", "06": "caroline",
        "07": "carroll", "08": "cecil", "09": "charles", "10": "dorchester", "11": "frederick", "12": "garrett", "13": "harford",
        "14": "howard", "15": "kent", "16": "montgomery", "17": "prince-george-s", "18": "queen-anne-s", "19": "st-mary-s",
        "20": "somerset", "21": "talbot", "22": "washington", "23": "wicomico", "24": "worcester" },
    started: false,

    init() {
        if (this.started) { this.render(); return; }
        this.started = true;
        let t;
        document.getElementById("mlSearch").addEventListener("input", (e) => {
            clearTimeout(t); t = setTimeout(() => { this.state.search = e.target.value.trim().toLowerCase(); this.state.page = 1; this.render(); }, 200);
        });
        document.getElementById("mlStatus").addEventListener("change", (e) => { this.state.status = e.target.value; this.state.page = 1; this.render(); });
        document.getElementById("mlWhen").addEventListener("change", (e) => { this.state.when = e.target.value; this.state.page = 1; this.render(); });
        document.getElementById("mlPrev").addEventListener("click", () => { if (this.state.page > 1) { this.state.page--; this.render(); } });
        document.getElementById("mlNext").addEventListener("click", () => { this.state.page++; this.render(); });
        document.getElementById("mlRefresh").addEventListener("click", async () => { await SharedNotes.load(); this.render(); });
        document.getElementById("mlAddBtn").addEventListener("click", () => this.openAdd());
        document.addEventListener("mdpl:notes-loaded", () => this.render());
        // re-draw whenever you come back to this tab (adds made on other tabs show right away)
        const tab = document.querySelector('.view-tab[data-view="myleads"]');
        if (tab) tab.addEventListener("click", () => this.render());
        this.render();
    },

    leads() {
        return Object.entries(SharedNotes.history).map(([k, rows]) => {
            const latest = rows[0];
            // prefer a label that carries an address (has a digit) over a generic one like "Guy estate"
            const labelRow = rows.find((r) => /\d/.test(r.label || "") && /[a-z]{3}/i.test(r.label || "")) || rows.find((r) => (r.label || "").trim()) || latest;
            const noteRow = rows.find((r) => (r.notes || "").trim() && !/^(Added to My Leads|Status changed to )/.test(r.notes.trim())) || latest;
            return {
                key: k, rows, latest, status: latest.status || "new", tab: latest.tab || "",
                tabs: [...new Set(rows.map((r) => r.tab).filter(Boolean))],
                label: (labelRow.label || "").trim() || k, lastNote: (noteRow.notes || "").trim(),
                // "last touch" = last real note, not the add/remove bookkeeping rows
                ts: (rows.find((r) => !/^(Added to My Leads|Removed from My Leads)/.test((r.notes || "").trim())) || latest).ts,
                who: (rows.find((r) => !/^(Added to My Leads|Removed from My Leads)/.test((r.notes || "").trim())) || latest).author || "",
                inList: this.membership(rows), followUp: latest.status === "followup" ? this.fuDate(rows) : "",
                blob: rows.map((r) => `${r.label} ${r.notes} ${r.id}`).join(" ").toLowerCase(),
            };
        }).sort((a, b) => String(b.ts).localeCompare(String(a.ts)));
    },

    /* In My Leads only if someone put it there: the newest add/remove event decides.
       Add = "+ Add to My Leads", a follow-up date, status "Working it", or the add-a-lead form.
       Remove = "Remove from My Leads", or marking it Dead / Closed. Plain notes don't count. */
    membership(rows) {
        for (const r of rows) {
            const n = (r.notes || "").trim(), st = r.status || "";
            if (n.startsWith(this.REMOVED) || this.DONE.includes(st)) return false;
            if (n.startsWith(this.ADDED) || /^FOLLOW UP: /.test(n) || r.tab === "myleads" || ["pursuing", "working", "followup"].includes(st)) return true;
        }
        return false;
    },
    fuDate(rows) {
        for (const r of rows) { const m = /^FOLLOW UP: (\d{4}-\d{2}-\d{2})/.exec((r.notes || "").trim()); if (m) return m[1]; }
        return "";
    },
    today() { const d = new Date(); return new Date(d.getTime() - d.getTimezoneOffset() * 6e4).toISOString().slice(0, 10); },
    fuLabel(d) {
        if (!d) return "";
        const t = this.today(), dt = new Date(d + "T12:00:00");
        const nice = dt.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });
        return d < t ? `OVERDUE · ${nice}` : d === t ? `TODAY` : nice;
    },

    render() {
        if (!document.getElementById("mlBody")) return;
        const all = this.leads(), now = Date.now();
        const age = (l) => (now - new Date(l.ts).getTime()) / 864e5;
        const done = (l) => this.DONE.includes(l.status);
        const mine = all.filter((l) => l.inList), t = this.today();
        document.getElementById("mlstatTotal").textContent = mine.length;
        document.getElementById("mlstatHot").textContent = mine.filter((l) => l.followUp && l.followUp <= t).length;
        document.getElementById("mlstatWeek").textContent = mine.filter((l) => l.followUp && l.followUp > t).length;
        document.getElementById("mlstatStale").textContent = mine.filter((l) => !l.followUp && age(l) > 7).length;

        const s = this.state, maxAge = { today: 1, week: 7, month: 30 }[s.when];
        let rows = all.filter((l) => {
            if (s.status === "mine" && !l.inList) return false;
            if (s.status === "due" && !(l.inList && l.followUp && l.followUp <= t)) return false;
            if (s.status === "upcoming" && !(l.inList && l.followUp)) return false;
            if (s.status === "stale" && !(l.inList && !l.followUp && age(l) > 7)) return false;
            if (s.status === "notmine" && l.inList) return false;
            if (!["all", "mine", "due", "upcoming", "stale", "notmine"].includes(s.status) && l.status !== s.status) return false;
            if (maxAge && age(l) > maxAge) return false;
            return !s.search || s.search.split(/\s+/).every((w) => l.blob.includes(w));
        });
        // follow-ups first (overdue, today, soonest), then everything else by last touch
        if (s.status !== "notmine" && s.status !== "all") rows = rows.slice().sort((a, b) => (a.followUp || "9999").localeCompare(b.followUp || "9999") || String(b.ts).localeCompare(String(a.ts)));
        const pages = Math.max(1, Math.ceil(rows.length / s.perPage));
        if (s.page > pages) s.page = pages;
        const start = (s.page - 1) * s.perPage, page = rows.slice(start, start + s.perPage);
        document.getElementById("mlEmpty").style.display = page.length ? "none" : "block";
        const tTop = this.today();
        const body = document.getElementById("mlBody");
        body.innerHTML = page.map((l) => `<tr data-k="${this.esc(l.key)}">
            <td class="date-cell" title="${this.esc(SharedNotes.fmt(l.ts))}">${this.esc(this.ago(l.ts))}<div class="meta">${this.esc(l.who)}</div></td>
            <td class="property-cell"><div class="address">${this.esc(l.label)}</div>
                <div class="meta">${this.esc(l.tabs.map((t) => this.TAB_NAME[t] || t).join(" · "))}</div></td>
            <td><span class="status-badge ${this.esc(l.status)}">${this.esc((this.STATUS[l.status] || l.status).toUpperCase())}</span>${l.inList ? "" : '<div class="meta" style="margin-top:4px">not in My Leads</div>'}</td>
            <td style="font-family:var(--font-mono); font-size:0.78rem; white-space:nowrap; color:${l.followUp && l.followUp <= tTop ? "var(--red, #ff453a)" : "var(--yellow)"}">${this.esc(this.fuLabel(l.followUp))}</td>
            <td style="font-size:0.8rem; color:var(--text-secondary); max-width:520px">${this.esc(l.lastNote.slice(0, 220))}${l.lastNote.length > 220 ? "…" : ""}</td>
            <td style="text-align:center; font-family:var(--font-mono)">${l.rows.length}</td>
        </tr>`).join("");
        body.querySelectorAll("tr").forEach((tr) => tr.addEventListener("click", () => this.open(tr.dataset.k)));
        document.getElementById("mlPageInfo").textContent = rows.length ? `Showing ${start + 1}–${Math.min(start + s.perPage, rows.length)} of ${rows.length}` : "No leads";
        document.getElementById("mlPrev").disabled = s.page <= 1;
        document.getElementById("mlNext").disabled = s.page >= pages;
        const sync = document.getElementById("mlSync");
        if (sync) sync.textContent = SharedNotes.error ? "Could not reach the shared notes sheet" : (SharedNotes.loaded ? "" : "Loading notes…");
    },

    open(k) {
        const l = this.leads().find((x) => x.key === k);
        if (!l) return;
        const canOpen = l.tabs.some((t) => ["title", "d4d", "surplus", "vested", "land"].includes(t));
        document.getElementById("modalBody").innerHTML = `
            <div class="detail-section">
                <h3>My Leads · <span class="status-badge ${this.esc(l.status)}">${this.esc((this.STATUS[l.status] || l.status).toUpperCase())}</span></h3>
                <div class="detail-row"><span class="label">Property</span><span class="value" style="font-weight:600">${this.esc(l.label)}</span></div>
                <div class="detail-row"><span class="label">ID</span><span class="value" style="font-family:var(--font-mono)">${this.esc(l.rows[0].id)}</span></div>
                <div class="detail-row"><span class="label">In My Leads</span><span class="value">${l.inList ? "Yes" : "No"}${l.followUp ? ` · follow up <b>${this.esc(this.fuLabel(l.followUp))}</b>` : ""}</span></div>
                <div class="detail-row"><span class="label">Found in</span><span class="value">${this.esc(l.tabs.map((t) => this.TAB_NAME[t] || t).join(" · "))}</span></div>
                <div class="detail-row"><span class="label">Last touched</span><span class="value">${this.esc(SharedNotes.fmt(l.ts))} (${this.esc(this.ago(l.ts))}) by ${this.esc(l.who)}</span></div>
                ${canOpen ? `<div style="margin-top:10px"><button class="btn btn-primary" id="mlOpenCard">Open the full research card →</button></div>` : ""}
            </div>
            <div class="detail-section"><h3>Everything we've noted (${l.rows.length}, newest first)</h3>
                <div class="note-history">${l.rows.map((r) => SharedNotes.noteCard(r, this.TAB_NAME[r.tab] || r.tab || "")).join("")}</div>
            </div>`;
        const btn = document.getElementById("mlOpenCard");
        if (btn) btn.onclick = () => this.openCard(l);
        document.getElementById("noteHistory").innerHTML = "";
        const sel = document.getElementById("leadStatusSelect"), notes = document.getElementById("leadNotes");
        sel.value = l.status; if (sel.value !== l.status) sel.value = "new";
        notes.value = ""; notes.placeholder = "Add a new note (what happened, what's next). Change the status above if it moved.";
        SharedNotes.paintSync();
        this.setPin(l.rows[0].id, l);
        document.getElementById("saveLeadBtn").onclick = async () => {
            const text = notes.value.trim();
            if (!text && sel.value === l.status) { App.closeModal(); return; }
            await SharedNotes.save(l.tab || "myleads", l.rows[0].id, sel.value, text || `Status changed to ${this.STATUS[sel.value] || sel.value}`, l.label);
            App.closeModal(); this.render();
        };
        document.getElementById("modalOverlay").classList.add("active");
    },

    /* Jump to the tab the lead came from and open its full card there. */
    async openCard(l) {
        App.closeModal();
        const t = ["d4d", "title", "land", "surplus", "vested"].find((x) => l.tabs.includes(x));
        document.querySelector(`.view-tab[data-view="${t}"]`).click();
        const wait = async (fn) => { for (let i = 0; i < 80 && !fn(); i++) await new Promise((r) => setTimeout(r, 150)); return fn(); };
        const match = (rows) => (rows || []).find((x) => SharedNotes.key(x.id) === l.key);
        if (t === "d4d") {
            await wait(() => D4DApp.rows.length);
            const r = match(D4DApp.rows); if (r) D4DApp.open(r.id);
        } else if (t === "title") {
            await wait(() => TitleApp.rows && TitleApp.rows.length);
            let r = match(TitleApp.rows);
            if (!r) {
                const k = l.key, code = (k.length === 9 || k.length === 14) ? "0" + k[0] : k.slice(0, 2);
                const slug = this.COUNTY[code];
                if (slug) {
                    try {
                        const j = await (await fetch(`data/title/${slug}.json?t=${Date.now()}`, { cache: "no-store" })).json();
                        r = match(j.rows);
                        if (r) TitleApp.rows.push(TitleApp.decorate(r));
                    } catch {}
                }
            }
            if (r) TitleApp.open(r.id); else alert("That lead isn't in the Title Leads data anymore (it may have dropped off). Its notes are all still in My Leads.");
        } else if (t === "land") {
            await wait(() => LandApp.rows.length);
            const r = match(LandApp.rows);
            if (r) LandApp.open(r.id); else alert("That parcel isn't in the Dirt Deals data anymore (it may have dropped off). Its notes are all still in My Leads.");
        } else {
            const box = document.getElementById(t === "surplus" ? "surplusSearch" : "vestedSearch");
            await wait(() => box);
            if (box) { box.value = l.rows[0].id; box.dispatchEvent(new Event("input")); }
        }
    },

    /* "+ Add a lead" form: for something not in any tab (VA house, a tip, a referral). */
    openAdd() {
        document.getElementById("modalBody").innerHTML = `
            <div class="detail-section"><h3>Add a lead to My Leads</h3>
                <p style="font-size:0.85rem; color:var(--text-secondary)">For anything worth pursuing that isn't already a card in the tool. (For a card that's already here, open it and press <b>+ Add to My Leads</b>.)</p>
                <div class="ml-form">
                    <label>Property address *<input id="mlfAddr" placeholder="6339 Hillside Rd, Springfield VA 22152"></label>
                    <label>Account / parcel # (if you have it)<input id="mlfId" placeholder="0501051601 or Fairfax 0793 03 0001"></label>
                    <label>Owner(s)<input id="mlfOwner" placeholder="Brintzenhofe trust / Selario heirs"></label>
                    <label>Where it came from<input id="mlfSrc" placeholder="Neighbor tip, driving, referral, Zillow…"></label>
                    <label>Follow up on (optional)<input type="date" id="mlfFu"></label>
                </div>
            </div>`;
        document.getElementById("noteHistory").innerHTML = ""; SharedNotes.openId = null;
        const sel = document.getElementById("leadStatusSelect"), notes = document.getElementById("leadNotes");
        sel.value = "pursuing"; notes.value = ""; notes.placeholder = "What makes it worth pursuing, who to contact, next step.";
        SharedNotes.paintSync(); this.setPin(null);
        document.getElementById("saveLeadBtn").onclick = async () => {
            const v = (id) => document.getElementById(id).value.trim();
            const addr = v("mlfAddr");
            if (!addr) { document.getElementById("mlfAddr").focus(); return; }
            const id = v("mlfId") || ("ML-" + addr.toUpperCase().replace(/[^A-Z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 40));
            const label = addr + (v("mlfOwner") ? ` (${v("mlfOwner")})` : "");
            const fu = v("mlfFu");
            const text = [fu ? `FOLLOW UP: ${fu}` : this.ADDED, v("mlfSrc") ? `Source: ${v("mlfSrc")}` : "", notes.value.trim()].filter(Boolean).join("\n");
            await SharedNotes.save("myleads", id, fu ? "followup" : sel.value, text, label);
            App.closeModal(); this.render();
        };
        document.getElementById("modalOverlay").classList.add("active");
    },

    /* The "+ Add to My Leads" button in every card's footer. Shown only for cards that use
       the shared notes (SharedNotes.footer sets the current id); it marks the lead
       "pursuing" through that tab's own Save, so tab, id and label stay exactly right. */
    setPin(id, lead) {
        const box = document.getElementById("mlActions");
        if (!box) return;
        box.style.display = id ? "inline-flex" : "none";
        if (!id) return;
        const k = SharedNotes.key(id), rows = SharedNotes.history[k] || [];
        const inList = this.membership(rows), fu = (rows[0] || {}).status === "followup" ? this.fuDate(rows) : "";
        const add = document.getElementById("pinLeadBtn"), rem = document.getElementById("unpinLeadBtn");
        const date = document.getElementById("fuDate"), fuBtn = document.getElementById("fuBtn");
        add.style.display = inList ? "none" : ""; rem.style.display = inList ? "" : "none";
        date.value = fu || ""; date.min = this.today();
        const sel = () => document.getElementById("leadStatusSelect"), notes = () => document.getElementById("leadNotes");
        // the note box is pre-filled with the last note on most cards; only keep it if it was edited
        const typed = () => { const v = notes().value.trim(), prev = ((SharedNotes.get(id) || {}).notes || "").trim(); return v && v !== prev ? v : ""; };
        const go = (status, first) => {
            const t = typed();
            if (status) sel().value = status;
            notes().value = t ? `${first}\n${t}` : first;
            document.getElementById("saveLeadBtn").click();
        };
        add.onclick = () => go(null, this.ADDED);
        rem.onclick = () => go(null, this.REMOVED);
        fuBtn.onclick = () => { if (!date.value) { date.focus(); date.showPicker && date.showPicker(); return; } go("followup", `FOLLOW UP: ${date.value}`); };
    },

    ago(ts) {
        const m = (Date.now() - new Date(ts).getTime()) / 6e4;
        if (isNaN(m)) return "";
        if (m < 60) return `${Math.max(1, Math.round(m))} min ago`;
        if (m < 1440) return `${Math.round(m / 60)} hr ago`;
        const d = Math.round(m / 1440);
        return d === 1 ? "yesterday" : `${d} days ago`;
    },
    esc(s) { return s == null ? "" : String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); },
};

// Every card that uses shared notes calls SharedNotes.footer(id) when it opens: show the pin button there.
(() => {
    const orig = SharedNotes.footer.bind(SharedNotes);
    SharedNotes.footer = (id) => { orig(id); MyLeadsApp.setPin(id); };
    if (typeof App !== "undefined" && App.closeModal) {
        const close = App.closeModal.bind(App);
        App.closeModal = () => { close(); MyLeadsApp.setPin(null); };
    }
    document.addEventListener("click", (e) => {
        if (e.target.id === "modalClose" || e.target.id === "modalOverlay") MyLeadsApp.setPin(null);
    });
})();
