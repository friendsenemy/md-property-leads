/* Writ Watch tab — sheriff's sales of a homeowner's interest on a money judgment
   (the Baltimore XV play: HOA/condo/utility judgment -> Notice of Lien in circuit court ->
   Request for Writ of Execution -> writ issued -> sheriff levies (posts the writ on the house)
   -> sale no sooner than 30 days after the levy and 20+ days after the newspaper notice
   -> report of sale -> 30-day exceptions notice -> ratification -> sheriff's deed).
   Md. Rules 3-641..3-644 / 2-641..2-644, Rule 14-305, CJ 11-501 et seq.

   Sources:
   - Paste-in: Ray runs Maryland Judiciary Case Search himself (it prohibits automated access)
     and pastes a results list or a case-detail page here. This tab reads the docket, finds the
     writ / levy / sale entries and counts down to the earliest legal sale date.
   - Automatic: engine2/writ_watch.py (GitHub Actions, weekday mornings) reads Montgomery
     County's public sheriff-sale page and refreshes property facts -> data/writ/watch.json.
   Pasted cases are shared through the notes sheet as JSON rows under "writ:<case>#writ"
   (kept out of note history and My Leads). Notes/status use the normal id "writ:<case>". */
const WRIT_LS_KEY = "md_writ_local_v1";
const WRIT_SEARCH_KEY = "md_writ_searchlist_v1";

const WritApp = {
    rows: [], server: [], generated: "", local: {},
    state: { search: "", stage: "", sort: "sale", dir: 1 },

    // HOAs / creditors already seen using writs of execution on homes (from Baltimore XV's dockets, PG County).
    SEED_SEARCHES: [
        "Baltimore XV Properties", "Newsteps' Choice North", "Highbridge Community Association", "Brandywine Station Townhouse",
        "Brinkley Overlook Condominium", "Iverson Courts Condominium", "Southlake at Lake Arbor", "Frenchman's",
        "Saddlebrook Homeowners", "Serene Townhouse Village", "Tiers of Laurel Lake", "Snowden Pond at Montpelier",
        "Founders Woods HOA", "Balk Hill Village", "Huntcrest Condominium", "Vistas at Gallahan Farm",
        "Arbor Park Community Association", "Chester Grove Association", "Pinewood Hill Condominium",
        "Perrywood Community Association", "Marlborough Condominium", "Statewide Utilities",
    ],

    STAGES: [
        ["satisfied", "Paid / satisfied", 0], ["judgment", "Judgment", 1], ["lien", "Lien recorded", 2],
        ["writ_req", "Writ requested", 3], ["writ", "Writ issued", 4], ["levied", "Levied (posted)", 5],
        ["sold", "Sold — awaiting ratification", 6], ["ratified", "Ratified / deeded", 7], ["listed", "Sale advertised", 5],
    ],

    JURS: { "prince george": "PRIN", "montgomery": "MONT", "anne arundel": "ANNE", "charles": "CHAR", "calvert": "CALV",
        "st. mary": "STMA", "saint mary": "STMA", "st mary": "STMA", "baltimore county": "BACO", "baltimore city": "BACI",
        "howard": "HOWA", "frederick": "FRED", "carroll": "CARR", "harford": "HARF", "washington": "WASH", "cecil": "CECI",
        "queen anne": "QUEE", "talbot": "TALB", "wicomico": "WICO", "worcester": "WORC", "allegany": "ALLE", "garrett": "GARR",
        "caroline": "CARO", "dorchester": "DORC", "kent": "KENT", "somerset": "SOME" },
    DISTRICT_JURS: { "01": ["BACI"], "02": ["DORC", "SOME", "WICO", "WORC"], "03": ["CARO", "CECI", "KENT", "QUEE", "TALB"],
        "04": ["CALV", "CHAR", "STMA"], "05": ["PRIN"], "06": ["MONT"], "07": ["ANNE"], "08": ["BACO"], "09": ["HARF"],
        "10": ["CARR", "HOWA"], "11": ["FRED", "WASH"], "12": ["ALLE", "GARR"] },
    COUNTY_NAME: { PRIN: "Prince George's", MONT: "Montgomery", ANNE: "Anne Arundel", CHAR: "Charles", CALV: "Calvert", STMA: "St. Mary's",
        BACO: "Baltimore County", BACI: "Baltimore City", HOWA: "Howard", FRED: "Frederick", CARR: "Carroll", HARF: "Harford" },
    IMAP: "https://mdgeodata.md.gov/imap/rest/services/PlanningCadastre/MD_PropertyData/MapServer/0/query",

    async init() {
        this.loadLocal();
        this.bind();
        try {
            const r = await fetch(`data/writ/watch.json?t=${Date.now()}`, { cache: "no-store" });
            if (r.ok) { const j = await r.json(); this.server = j.rows || []; this.generated = j.generated_at || ""; this.sources = j.sources || {}; }
        } catch {}
        this.merge();
        this.paintSearchList();
        this.render();
        this.paintTriage();
        this.paintFirms();
    },
    _hook: document.addEventListener("mdpl:notes-loaded", () => { try { if (WritApp.started) { WritApp.merge(); WritApp.render(); } } catch {} }),

    loadLocal() { try { this.local = JSON.parse(localStorage.getItem(WRIT_LS_KEY) || "{}"); } catch { this.local = {}; } },
    saveLocal() { try { localStorage.setItem(WRIT_LS_KEY, JSON.stringify(this.local)); } catch {} },

    /* Case rows: shared-sheet pasted cases (newest), local copies, and the server file (Montgomery + refreshed facts). */
    merge() {
        this.started = true;
        const by = {};
        const put = (c, src) => { if (!c || !c.case) return; const k = c.case.toUpperCase(); by[k] = Object.assign({}, by[k] || {}, c, { _src: by[k] ? by[k]._src : src }); };
        this.server.forEach((c) => put(c, c.source || "server"));
        Object.values(this.local).forEach((c) => put(c, "pasted"));
        const ws = (SharedNotes.writs || {});
        Object.values(ws).forEach((row) => { try { put(JSON.parse(row.notes), "pasted"); } catch {} });
        this.rows = Object.values(by).filter((c) => !c.removed).map((c) => this.decorate(c));
    },

    decorate(c) {
        c.id = "writ:" + c.case.toUpperCase();
        const st = this.stageOf(c);
        c._stage = st; c._stageRank = (this.STAGES.find((s) => s[0] === st) || [0, 0, -1])[2];
        c._sale = this.earliestSale(c);
        c._days = c._sale ? Math.round((c._sale - Date.now()) / 864e5) : null;
        const p = c.prop || {};
        c._value = Number(p.value || 0);
        c._mort = this.estMortgage(p.price, p.tradate);
        c._equity = c._value ? Math.max(0, c._value - c._mort) : null;
        c._blob = [c.case, c.caption, c.plaintiff, c.defendant, c.address, c.city, p.address, p.acct, c.county].filter(Boolean).join(" ").toLowerCase();
        return c;
    },

    stageOf(c) {
        // paying after the sale does not undo it (Baltimore XV v. Newsteps' Choice, 2026)
        if (c.satisfied && !(c.sold && this.d(c.satisfied) > this.d(c.sold))) return "satisfied";
        if (c.ratified) return "ratified";
        if (c.sold) return "sold";
        if (c.sale_date && !c.levy) return "listed";
        if (c.levy) return "levied";
        if (c.writ_issued) return "writ";
        if (c.writ_req) return "writ_req";
        if (c.lien) return "lien";
        return "judgment";
    },
    d(s) { if (!s) return null; const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s); const dt = m ? new Date(+m[3], +m[1] - 1, +m[2], 12) : new Date(s.length === 10 ? s + "T12:00:00" : s); return isNaN(dt) ? null : dt; },
    plus(dt, days) { return dt ? new Date(dt.getTime() + days * 864e5) : null; },
    /* Earliest lawful sale: 30 days after the levy (Rule 3-644(a)); if only the writ has issued, the levy is
       still pending, so the floor is writ + 30 days (in practice months later). An advertised date wins. */
    earliestSale(c) {
        if (c.sold || c.satisfied) return null;
        if (c.sale_date) return this.d(c.sale_date);
        if (c.levy) return this.plus(this.d(c.levy), 30);
        if (c.writ_issued) return this.plus(this.d(c.writ_issued), 30);
        return null;
    },
    /* Rough first-mortgage balance: 80% of the last price, 30-yr fixed at that year's typical rate. Refis/HELOCs not counted. */
    estMortgage(price, tradate) {
        price = Number(price || 0); if (!price || !tradate) return 0;
        const y = +String(tradate).slice(0, 4), mo = +String(tradate).slice(4, 6) || 6;
        const rate = y < 1995 ? 8 : y < 2002 ? 7.2 : y < 2009 ? 6.2 : y < 2013 ? 4.6 : y < 2020 ? 4.1 : y < 2022 ? 3.0 : 6.6;
        const r = rate / 1200, n = 360, P = price * 0.8;
        const m = Math.max(0, (new Date().getFullYear() - y) * 12 + (new Date().getMonth() + 1 - mo));
        if (m >= n) return 0;
        return Math.max(0, P * (Math.pow(1 + r, n) - Math.pow(1 + r, m)) / (Math.pow(1 + r, n) - 1));
    },

    bind() {
        const $ = (id) => document.getElementById(id);
        $("writSearch").addEventListener("input", (e) => { this.state.search = e.target.value.trim().toLowerCase(); this.render(); });
        $("writStage").addEventListener("change", (e) => { this.state.stage = e.target.value; this.render(); });
        document.querySelectorAll("#writTable th[data-sort]").forEach((th) => th.addEventListener("click", () => {
            const k = th.dataset.sort; this.state.dir = this.state.sort === k ? -this.state.dir : (k === "sale" ? 1 : -1); this.state.sort = k; this.render();
        }));
        $("writPasteGo").addEventListener("click", () => this.importPaste());
        $("writFile").addEventListener("change", (e) => { this.importFile(e.target.files[0]); e.target.value = ""; });
        $("writExport").addEventListener("click", () => this.exportCSV());
        $("writAddSearch").addEventListener("click", () => {
            const v = $("writNewSearch").value.trim(); if (!v) return;
            const list = this.searchList(); if (!list.includes(v)) list.push(v);
            try { localStorage.setItem(WRIT_SEARCH_KEY, JSON.stringify(list)); } catch {}
            $("writNewSearch").value = ""; this.paintSearchList();
        });
    },

    searchList() { try { return JSON.parse(localStorage.getItem(WRIT_SEARCH_KEY) || "null") || this.SEED_SEARCHES.slice(); } catch { return this.SEED_SEARCHES.slice(); } },
    paintSearchList() {
        const el = document.getElementById("writSearchList");
        el.innerHTML = this.searchList().map((n) => `<button type="button" class="filter-btn" data-n="${this.esc(n)}" title="Copy, then paste into Case Search → Party Name Search → Business">${this.esc(n)}</button>`).join("");
        el.querySelectorAll("button").forEach((b) => b.addEventListener("click", async () => {
            try { await navigator.clipboard.writeText(b.dataset.n); b.textContent = "✓ copied"; setTimeout(() => { b.textContent = b.dataset.n; }, 1200); } catch {}
        }));
    },

    /* ---------- paste parsing ---------- */
    CASE_RE: /^(?:[A-Z]-\d{2,3}-[A-Z]{2}-\d{2}-\d{6}|\d{2}-\d{2}-\d{7}-\d{4}|\d{2}-\d{2}-[A-Z]{2}\d{5}-\d{4}|[A-Z]{2,5}\d{2}-\d{4,6}|\d{2}-[A-Z]-\d{2}-\d{6})$/,
    HOA_RE: /ASSOC|HOMEOWN|HMOWNR|CONDOMINIUM|COUNCIL OF (?:UNIT|CO-)|UNIT OWNERS|COMMUNITY|\bHOA\b|TOWNHOUSE|VILLAGE|RECREATION|\bPOA\b|OWNERS/i,

    async importPaste() {
        const ta = document.getElementById("writPaste"), msg = document.getElementById("writPasteMsg");
        const text = ta.value || "";
        const found = /Docket Entry Name|Case Information|Case Title:/i.test(text) ? this.parseDetails(text) : this.parseList(text);
        if (!found.length) { msg.textContent = "Nothing recognized. Paste a Case Search results page, or open a case and paste the whole case-detail page."; return; }
        msg.textContent = `Read ${found.length} case${found.length === 1 ? "" : "s"} — looking up the properties…`;
        let n = 0;
        for (const c of found) {
            const prev = this.rows.find((r) => r.case.toUpperCase() === c.case.toUpperCase()) || {};
            const merged = Object.assign({}, prev, c);
            delete merged._src; Object.keys(merged).filter((k) => k.startsWith("_") || k === "id").forEach((k) => delete merged[k]);
            if (!merged.prop && (merged.address || merged.acct)) { try { merged.prop = await this.lookup(merged); } catch {} }
            delete merged.removed; delete merged.removed_at;
            merged.updated = new Date().toISOString();
            this.local[merged.case.toUpperCase()] = merged; n++;
            SharedNotes.saveWrit && SharedNotes.saveWrit("writ:" + merged.case.toUpperCase() + "#writ", JSON.stringify(merged), merged.caption || merged.case);
        }
        this.saveLocal(); this.merge(); this.render(); this.paintTriage(); this.paintFirms();
        msg.textContent = `Added/updated ${n} case${n === 1 ? "" : "s"}. Results lists only show the case — open the ones with a judgment and paste their detail page to see writ / levy / sale dates.`;
        ta.value = "";
    },

    /* Case Search results table (copied with mouse or Ctrl+A): one case per row, cells on tabs or new lines. */
    parseList(text) {
        const toks = text.split(/\t|\r?\n/).map((t) => t.trim()).filter(Boolean);
        const out = [];
        for (let i = 0; i < toks.length; i++) {
            if (!this.CASE_RE.test(toks[i])) continue;
            const blk = [];
            for (let j = i + 1; j < toks.length && j < i + 12 && !this.CASE_RE.test(toks[j]); j++) blk.push(toks[j]);
            const court = blk.find((t) => /Court/.test(t)) || "";
            const ci = blk.indexOf(court);
            const type = ci >= 0 ? blk[ci + 1] || "" : "";
            const status = ci >= 0 ? blk[ci + 2] || "" : "";
            const filed = blk.find((t) => /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(t)) || "";
            const caption = blk.find((t) => /\bvs?\.?\s/i.test(t)) || blk[blk.length - 1] || "";
            if (/Criminal|Traffic|Citation|Custody|Divorce|Protective|Peace Order|Tenant Holding|Failure to Pay Rent|Wrongful Detainer/i.test(type)) continue;
            const [pl, df] = caption.split(/\s+vs?\.?\s+/i);
            out.push({ case: toks[i], court, county: this.countyFrom(court, toks[i]), case_type: type, case_status: status, filed, caption,
                plaintiff: (pl || "").trim(), defendant: (df || "").trim(), hoa: this.HOA_RE.test(pl || ""), from: "results list" });
        }
        return out;
    },

    /* One or more case-detail pages. Labels and values may be on the same line or the next one. */
    parseDetails(text) {
        const chunks = text.split(/(?=Case Number:)/).filter((x) => /Case Number:/.test(x));
        return chunks.map((ch) => this.parseDetail(ch)).filter(Boolean);
    },
    parseDetail(ch) {
        const raw = ch.split(/\r?\n/).map((t) => t.trim()).filter(Boolean);
        const L = [];
        for (let i = 0; i < raw.length; i++) {
            if (/:$/.test(raw[i]) && i + 1 < raw.length && !/:$/.test(raw[i + 1])) { L.push(raw[i] + " " + raw[i + 1]); i++; } else L.push(raw[i]);
        }
        const get = (lab, from = 0) => { for (let i = from; i < L.length; i++) { const m = new RegExp("^" + lab + ":?\\s*(.*)$", "i").exec(L[i]); if (m) return [m[1].trim(), i]; } return ["", -1]; };
        const [caseNo] = get("Case Number"); if (!caseNo) return null;
        const c = { case: caseNo.split(/\s/)[0], from: "case detail" };
        c.caption = get("Case Title")[0]; c.court = get("Court System")[0] || get("Location")[0];
        c.case_type = get("Case Type")[0]; c.case_status = get("Case Status")[0]; c.filed = get("Filing Date")[0];
        const [pl, df] = (c.caption || "").split(/\s+vs?\.?\s+/i);
        c.plaintiff = (pl || "").trim(); c.defendant = (df || "").trim(); c.hoa = this.HOA_RE.test(c.plaintiff);
        c.county = this.countyFrom(c.court, c.case);
        // first Defendant block -> name + address (usually the house)
        const di = L.findIndex((x) => /^Defendant$/i.test(x));
        if (di >= 0) {
            const nm = get("Name", di), ad = get("Address", di), ct = get("City", di), zp = get("ZIP Code", di);
            if (nm[1] >= 0 && nm[1] - di < 4) c.defendant = nm[0] || c.defendant;
            if (ad[1] >= 0 && ad[1] - di < 6) c.address = ad[0];
            if (ct[1] >= 0 && ct[1] - di < 8) c.city = ct[0];
            if (zp[1] >= 0 && zp[1] - di < 12) c.zip = zp[0];
        }
        const pa = L.findIndex((x) => /^Attorney\(s\) for the Plaintiff$/i.test(x));
        if (pa >= 0) { const nm = get("Name", pa); if (nm[1] >= 0 && nm[1] - pa < 3) c.plaintiff_atty = nm[0]; const a1 = get("Address Line 1", pa); if (a1[1] >= 0 && a1[1] - pa < 8) c.plaintiff_firm = a1[0]; }
        c.judgment_amt = (get("Amount of Judgment")[0] || get("Total Judgment Index")[0] || "").replace(/[^\d.]/g, "");
        c.judgment_date = get("Judgment Entry Date")[0] || get("Judgment Ordered Date")[0];
        const ip = L.findIndex((x) => /^Interested Person/i.test(x)); if (ip >= 0) { const nm = get("Name", ip); if (nm[1] - ip < 3) c.purchaser = nm[0]; }
        // docket: "File Date: X" then "Docket Entry Name: Y" then optional "Comment: Z"
        const dk = [];
        for (let i = 0; i < L.length; i++) {
            const fd = /^File Date:\s*(\d{1,2}\/\d{1,2}\/\d{4})/i.exec(L[i]); if (!fd) continue;
            const nm = /^Docket Entry Name:\s*(.*)$/i.exec(L[i + 1] || ""); if (!nm) continue;
            const cm = /^Comment:\s*(.*)$/i.exec(L[i + 2] || "");
            dk.push({ date: fd[1], name: nm[1], comment: cm ? cm[1] : "" });
        }
        const first = (re, after) => { const e = dk.find((x) => re.test(x.name + " " + x.comment) && (!after || this.d(x.date) >= this.d(after))); return e ? e.date : ""; };
        c.lien = first(/Lien of Judgment Recorded|LIEN OF JUDGMENT|Notice of Lien/i);
        c.writ_req = first(/Request for Writ of Execution/i);
        c.writ_issued = first(/Writ of Execution Issued|Writ in Execution/i);
        if (c.writ_issued) {
            // The levy is the sheriff POSTING the writ of execution ("WEXC Posted mmddyyyy"). A later
            // "Return of Service - Served" usually belongs to a garnishment writ, so only use it when
            // no garnishment was requested in between.
            const wi = this.d(c.writ_issued);
            const posted = dk.find((x) => this.d(x.date) >= wi && /WEXC|Posted|Levy|Schedule of Levy/i.test(x.name + " " + x.comment));
            if (posted) {
                const m = /(?:Posted|WEXC)\D*(\d{2})(\d{2})(\d{4})/i.exec(posted.comment || "");
                c.levy = m ? `${m[1]}/${m[2]}/${m[3]}` : posted.date;
            } else {
                const nextGarn = dk.find((x) => this.d(x.date) >= wi && /Garnishment/i.test(x.name));
                const served = dk.find((x) => this.d(x.date) >= wi && /Return of Service - Served|SV-SERVED/i.test(x.name + " " + x.comment)
                    && (!nextGarn || this.d(x.date) < this.d(nextGarn.date)));
                if (served) c.levy = served.date;
            }
            const garnAfter = dk.filter((x) => this.d(x.date) > wi && /Garnishment/i.test(x.name));
            if (garnAfter.length) c.garnish_after_writ = garnAfter[garnAfter.length - 1].date;
            const dismissGarn = dk.find((x) => this.d(x.date) > wi && /Dismiss Garnishment|Release.*Levy|Levy Released|Release of Property/i.test(x.name + " " + x.comment));
            if (dismissGarn) c.release_or_dismiss = dismissGarn.date + " — " + dismissGarn.name;
        }
        c.sold = first(/Report of Sale|Affidavit - Purchaser|Purchaser'?s Affidavit|Sheriff'?s Sale (Report|Held)/i);
        c.ratified = first(/Order.*Ratif|Ratification.*(Order|Granted)|Sale Ratified/i);
        c.satisfied = first(/Satisfaction of Judgment Entered|Notice of Satisfaction|Judgment Satisfied/i);
        c.bankruptcy = first(/Bankruptcy|Automatic Stay|Suggestion of Bankruptcy/i);
        c.exceptions = first(/Exception/i);
        c.garnish_tries = dk.filter((x) => /Garnishment/i.test(x.name) && (!c.writ_req || this.d(x.date) < this.d(c.writ_req))).length;
        c.service_issues = dk.filter((x) => /NON-?EST|NOT SERVED|EVAD|Alternative Service|Alternate Service/i.test(x.name + " " + x.comment)).length;
        c.docket = dk.filter((x) => /Writ|Lien|Levy|Satisf|Sale|Ratif|Exception|Bankrupt|Affidavit Judgment|Judgment Entered|Garnish|Motion to Strike|Appeal/i.test(x.name + " " + x.comment) || (c.writ_issued && /Return of Service/i.test(x.name) && this.d(x.date) >= this.d(c.writ_issued))).slice(-18);
        return c;
    },

    countyFrom(court, caseNo) {
        const t = String(court || "").toLowerCase();
        for (const k of Object.keys(this.JURS)) if (t.includes(k)) return this.JURS[k];
        if (/upper marlboro|hyattsville/.test(t)) return "PRIN";
        if (/rockville|silver spring/.test(t)) return "MONT";
        if (/annapolis|glen burnie/.test(t)) return "ANNE";
        if (/la plata/.test(t)) return "CHAR";
        if (/leonardtown/.test(t)) return "STMA";
        if (/prince frederick/.test(t)) return "CALV";
        const m = /^(?:[A-Z]-)?(\d{2})-/.exec(String(caseNo || ""));
        if (m && this.DISTRICT_JURS[m[1]] && this.DISTRICT_JURS[m[1]].length === 1) return this.DISTRICT_JURS[m[1]][0];
        return "";
    },

    /* State parcel layer (MD iMap, public, CORS-enabled): value, owner-occupied flag, last sale. */
    async lookup(c) {
        const a = String(c.address || "").toUpperCase().replace(/[.,#]/g, " ").replace(/\s+/g, " ").trim();
        const m = /^(\d+[A-Z]?)\s+(.+)$/.exec(a); if (!m) return null;
        const words = m[2].split(" ").filter((w) => !/^(UNIT|APT|STE|SUITE)$/.test(w));
        const SUF = /^(DRIVE|DR|ROAD|RD|LANE|LN|COURT|CT|STREET|ST|AVENUE|AVE|WAY|PLACE|PL|TERRACE|TER|TERR|CIRCLE|CIR|BOULEVARD|BLVD|PARKWAY|PKWY|HIGHWAY|HWY|TRAIL|TRL|LOOP|ROW|RUN|PIKE|SQUARE|SQ)$/;
        const name = []; for (const w of words) { if (SUF.test(w) && name.length) break; name.push(w); if (name.length >= 4) break; }
        const jurs = c.county ? [c.county] : [];
        const where = `ADDRESS LIKE '${(m[1] + " " + name.join(" ")).replace(/'/g, "''")}%'` + (jurs.length ? ` AND JURSCODE IN (${jurs.map((j) => `'${j}'`).join(",")})` : "");
        const u = `${this.IMAP}?where=${encodeURIComponent(where)}&outFields=ACCTID,ADDRESS,CITY,ZIPCODE,OOI,NFMTTLVL,TRADATE,CONSIDR1,DR1LIBER,DR1FOLIO,JURSCODE,OWNADD1,OWNCITY,DESCLU,YEARBLT,SDATWEBADR&returnGeometry=false&f=json`;
        const j = await (await fetch(u)).json();
        const f = (j.features || [])[0]; if (!f) return null;
        const p = f.attributes;
        return { acct: p.ACCTID, address: p.ADDRESS, city: p.CITY, zip: p.ZIPCODE, ooi: p.OOI, value: p.NFMTTLVL, tradate: p.TRADATE, price: p.CONSIDR1,
            deed: p.DR1LIBER ? `${p.DR1LIBER}/${p.DR1FOLIO}` : "", jurs: p.JURSCODE, mail: [p.OWNADD1, p.OWNCITY].filter(Boolean).join(", "),
            use: p.DESCLU, built: p.YEARBLT, sdat: p.SDATWEBADR, matches: (j.features || []).length };
    },

    /* Remove a case from the watch (for everyone): saves a "removed" marker so the shared sheet,
       this browser and the daily scan all skip it. Pasting the case again brings it back. */
    remove(id) {
        const r = this.rows.find((x) => x.id === id); if (!r) return;
        if (!confirm(`Remove ${r.case} (${r.defendant || r.caption || ""}) from Writ Watch?`)) return;
        const k = r.case.toUpperCase();
        const stub = { case: k, removed: true, removed_at: new Date().toISOString() };
        this.local[k] = stub; this.saveLocal();
        SharedNotes.saveWrit && SharedNotes.saveWrit("writ:" + k + "#writ", JSON.stringify(stub), "removed " + k);
        App.closeModal(); this.merge(); this.render();
    },

    /* ---------- Case Search export triage (CSV / Excel / JSON) ----------
       An export is the results list only — no docket entries — so it can't show writs. What it CAN do is
       cut a 400-case HOA list down to the handful worth opening: judgment cases still alive (filed within
       ~12 years, Open / Open-Judgment), ranked higher when the same debtor also has an open
       "Judgment - District Court Lien" (lien on the house = a writ on the house is possible). */
    TRIAGE_KEY: "md_writ_triage_v1", TRIAGE_DONE_KEY: "md_writ_triage_done_v1",
    async importFile(file) {
        const msg = document.getElementById("writPasteMsg");
        if (!file) return;
        msg.textContent = `Reading ${file.name}…`;
        let recs = [];
        try {
            const ext = (file.name.split(".").pop() || "").toLowerCase();
            if (ext === "json") {
                const j = JSON.parse(await file.text());
                const findArr = (o) => Array.isArray(o) ? o : (o && typeof o === "object" ? Object.values(o).map(findArr).find((a) => a && a.length) : null);
                recs = findArr(j) || [];
            } else if (ext === "csv" || ext === "txt") {
                recs = this.csvRecords(await file.text());
            } else {
                if (!window.XLSX) await new Promise((ok, bad) => { const sc = document.createElement("script"); sc.src = "https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"; sc.onload = ok; sc.onerror = bad; document.head.appendChild(sc); });
                const wb = XLSX.read(await file.arrayBuffer(), { type: "array", cellDates: true });
                recs = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], { defval: "" });
            }
        } catch (e) { msg.textContent = "Couldn't read that file: " + e.message; return; }
        const t = this.triage(recs);
        if (!t.list.length && !recs.length) { msg.textContent = "No rows found in that file."; return; }
        try { localStorage.setItem(this.TRIAGE_KEY, JSON.stringify({ file: file.name, at: new Date().toISOString(), total: t.total, list: t.list, liens: t.liens })); } catch {}
        msg.textContent = `${file.name}: ${t.total} cases → ${t.list.length} worth opening (${t.list.filter((x) => x.pri === "A").length} top priority).`;
        this.paintTriage();
    },
    csvRecords(text) {
        const rows = []; let row = [], f = "", q = false;
        for (let i = 0; i < text.length; i++) {
            const ch = text[i];
            if (q) { if (ch === '"' && text[i + 1] === '"') { f += '"'; i++; } else if (ch === '"') q = false; else f += ch; }
            else if (ch === '"') q = true; else if (ch === ",") { row.push(f); f = ""; }
            else if (ch === "\n" || ch === "\r") { if (ch === "\r" && text[i + 1] === "\n") i++; row.push(f); rows.push(row); row = []; f = ""; }
            else f += ch;
        }
        if (f || row.length) { row.push(f); rows.push(row); }
        const H = rows.shift() || [];
        return rows.filter((r) => r.some((x) => x)).map((r) => Object.fromEntries(H.map((h, i) => [h, r[i] || ""])));
    },
    surname(caption) {
        let d = String(caption || "").split(/\s+vs?\.?\s+/i).slice(1).join(" ").replace(/\bet\.?\s*al\.?/i, "").trim();
        if (!d) return "";
        if (d.includes(",")) d = d.split(",")[0]; else { const w = d.split(/\s+/); d = w[w.length - 1]; }
        return d.replace(/[^A-Za-z'\-]/g, "").toUpperCase();
    },
    triage(recs) {
        const norm = (o) => { const n = {}; Object.keys(o || {}).forEach((k) => { n[k.toLowerCase().replace(/[^a-z]/g, "")] = o[k]; }); return n; };
        const toDate = (v) => { if (v instanceof Date) return v; if (typeof v === "number") return new Date(Math.round((v - 25569) * 864e5)); return this.d(String(v || "").trim().split(/[ T]/)[0]) || (v ? new Date(v) : null); };
        const by = {};
        recs.map(norm).forEach((r) => {
            const cn = String(r.casenumber || r.case || "").trim(); if (!cn) return;
            const x = by[cn] || (by[cn] = { case: cn, caption: r.casecaption || r.casetitle || "", type: r.casetype || "", status: r.casestatus || "", court: r.court || "", filed: toDate(r.filingdate || r.filedate), plaintiff: false });
            if (/plaintiff/i.test(r.partytype || "")) x.plaintiff = true;
        });
        const all = Object.values(by), now = Date.now(), yrs = (d) => d && !isNaN(d) ? (now - d.getTime()) / 3.156e10 : 99;
        const liens = all.filter((c) => /District Court Lien/i.test(c.type) && !/closed/i.test(c.status) && yrs(c.filed) < 12.5).map((c) => ({ case: c.case, surname: this.surname(c.caption), filed: c.filed && c.filed.toISOString().slice(0, 10) }));
        const lienBy = {}; liens.forEach((l) => { if (l.surname) (lienBy[l.surname] = lienBy[l.surname] || []).push(l.case); });
        const list = all.filter((c) => c.plaintiff && /contract|confessed/i.test(c.type) && /open|reopen/i.test(c.status) && yrs(c.filed) < 12.5).map((c) => {
            const sn = this.surname(c.caption), lien = lienBy[sn] || [], age = yrs(c.filed);
            let pri = "B", why = "Old-system case still open — may hold a live judgment";
            if (/judgment/i.test(c.status)) { pri = "A"; why = "Judgment entered"; }
            else if (age < 0.5) { pri = "C"; why = "Suit filed recently — no judgment yet; re-check in a few months"; }
            else if (/^D-/i.test(c.case)) why = "New-system case still open — check for judgment / writ";
            if (lien.length) { pri = pri === "C" ? "B" : "A"; why += ` · same last name has an open circuit-court lien (${lien.join(", ")}) — confirm it is the same person`; }
            return { case: c.case, caption: c.caption, type: c.type, status: c.status, filed: c.filed && !isNaN(c.filed) ? c.filed.toISOString().slice(0, 10) : "", surname: sn, lien, pri, why };
        }).sort((a, b) => a.pri.localeCompare(b.pri) || (b.filed || "").localeCompare(a.filed || ""));
        return { total: all.length, list, liens };
    },
    /* Collection firms: which law firm files for which HOA, and how many of their cases reached a writ. */
    paintFirms() {
        const el = document.getElementById("writFirms"); if (!el) return;
        const by = {};
        this.rows.forEach((r) => {
            const f = (r.plaintiff_firm || r.plaintiff_atty || "").trim(); if (!f) return;
            const k = f.toUpperCase().replace(/[.,]/g, "").replace(/\s+/g, " ");
            const x = by[k] || (by[k] = { firm: f, attys: new Set(), hoas: new Set(), cases: 0, writs: 0 });
            if (r.plaintiff_atty) x.attys.add(r.plaintiff_atty); if (r.plaintiff) x.hoas.add(r.plaintiff);
            x.cases++; if (r.writ_req || r.writ_issued) x.writs++;
        });
        const list = Object.values(by).sort((a, b) => b.writs - a.writs || b.cases - a.cases);
        el.innerHTML = list.length ? `<details style="margin:10px 0"><summary><b>Collection firms</b> — ${list.length} firm${list.length === 1 ? "" : "s"} seen in pasted cases (a firm that requests writs for one HOA usually does it for its others)</summary>
            <table class="leads-table" style="font-size:0.8rem"><thead><tr><th>Firm</th><th>Attorney(s)</th><th>HOAs / creditors</th><th>Cases</th><th>With a writ</th></tr></thead><tbody>
            ${list.map((x) => `<tr><td>${this.esc(x.firm)}</td><td>${this.esc([...x.attys].join(", "))}</td><td>${this.esc([...x.hoas].join("; "))}</td><td>${x.cases}</td><td>${x.writs}</td></tr>`).join("")}
            </tbody></table></details>` : "";
    },
    paintTriage() {
        const el = document.getElementById("writTriage"); if (!el) return;
        let t = null, done = {};
        try { t = JSON.parse(localStorage.getItem(this.TRIAGE_KEY) || "null"); done = JSON.parse(localStorage.getItem(this.TRIAGE_DONE_KEY) || "{}"); } catch {}
        if (!t || !t.list) { el.innerHTML = ""; return; }
        const inTool = new Set(this.rows.map((r) => r.case.toUpperCase()));
        const left = t.list.filter((x) => !done[x.case] && !inTool.has(x.case.toUpperCase())).length;
        const PRI = { A: "🔥 Open first", B: "Check", C: "Later" };
        el.innerHTML = `<details open style="margin:10px 0"><summary><b>Cases to open</b> — from ${this.esc(t.file)} (${t.total} cases → ${t.list.length} worth a look, <b>${left}</b> left)
            <button class="btn" id="writTriageClear" style="margin-left:8px; padding:2px 8px">Clear list</button></summary>
            <div style="font-size:0.78rem; color:var(--text-secondary); margin:4px 0">Click a case number to copy it → Case Search → search by case number → select-all, copy, paste the page above. Tick it once you've checked it.</div>
            <table class="leads-table" style="font-size:0.8rem"><thead><tr><th></th><th>Priority</th><th>Filed</th><th>Case</th><th>Status</th><th>Debtor (caption)</th><th>Why</th></tr></thead><tbody>
            ${t.list.map((x) => { const isDone = done[x.case] || inTool.has(x.case.toUpperCase());
                return `<tr style="${isDone ? "opacity:.45" : ""}"><td><input type="checkbox" data-c="${this.esc(x.case)}" ${isDone ? "checked" : ""} ${inTool.has(x.case.toUpperCase()) ? "disabled title='Already pasted into the tool'" : ""}></td>
                <td>${PRI[x.pri]}</td><td>${this.esc(x.filed)}</td><td><button type="button" class="filter-btn" data-copy="${this.esc(x.case)}">${this.esc(x.case)}</button></td>
                <td>${this.esc(x.status)}</td><td>${this.esc((x.caption.split(/\s+vs?\.?\s+/i)[1] || x.caption))}</td><td>${this.esc(x.why)}</td></tr>`; }).join("")}
            </tbody></table></details>`;
        el.querySelectorAll("button[data-copy]").forEach((b) => b.addEventListener("click", async () => { try { await navigator.clipboard.writeText(b.dataset.copy); const o = b.textContent; b.textContent = "✓ copied"; setTimeout(() => { b.textContent = o; }, 1200); } catch {} }));
        el.querySelectorAll("input[data-c]").forEach((cb) => cb.addEventListener("change", () => { done[cb.dataset.c] = cb.checked || undefined; try { localStorage.setItem(this.TRIAGE_DONE_KEY, JSON.stringify(done)); } catch {} this.paintTriage(); }));
        const clr = document.getElementById("writTriageClear");
        if (clr) clr.addEventListener("click", (e) => { e.preventDefault(); try { localStorage.removeItem(this.TRIAGE_KEY); } catch {} this.paintTriage(); });
    },

    /* ---------- table ---------- */
    filtered() {
        const s = this.state;
        return this.rows.filter((r) => (!s.stage || r._stage === s.stage) && (!s.search || r._blob.includes(s.search)));
    },
    render() {
        const rows = this.filtered().slice();
        const k = this.state.sort, d = this.state.dir;
        const val = (r) => ({ sale: r._sale ? r._sale.getTime() : (r._stageRank >= 6 ? 9e15 : 8e15 - r._stageRank * 1e12), stage: r._stageRank, equity: r._equity || 0, value: r._value, county: r.county || "" }[k]);
        rows.sort((a, b) => { const x = val(a), y = val(b); return (x > y ? 1 : x < y ? -1 : 0) * d; });
        const live = this.rows.filter((r) => r._stageRank >= 3 && r._stageRank <= 5);
        document.getElementById("wstatLive").textContent = live.length;
        document.getElementById("wstatSoon").textContent = this.rows.filter((r) => r._days != null && r._days >= 0 && r._days <= 45).length;
        const eq = live.reduce((n, r) => n + (r._equity || 0), 0);
        document.getElementById("wstatEquity").textContent = eq >= 1e6 ? "$" + (eq / 1e6).toFixed(1) + "M" : "$" + Math.round(eq).toLocaleString();
        const src = this.sources && this.sources.montgomery ? ` · Montgomery sheriff page checked ${this.esc(this.fmtDT(this.sources.montgomery.checked))} (${this.sources.montgomery.count} listed)` : "";
        document.getElementById("writRunInfo").innerHTML = `<b>${this.rows.length}</b> cases watched${this.generated ? ` · auto-scan ${this.esc(this.fmtDT(this.generated))}` : ""}${src}`;
        const tbody = document.getElementById("writBody");
        document.getElementById("writEmpty").style.display = rows.length ? "none" : "block";
        tbody.innerHTML = rows.map((r) => {
            const st = this.STAGES.find((s) => s[0] === r._stage) || ["", r._stage];
            const days = r._days == null ? "" : r._days < 0 ? `<span style="color:var(--text-dim)">${-r._days}d ago</span>` : `<b style="color:${r._days <= 21 ? "var(--red)" : "var(--yellow)"}">in ${r._days}d</b>`;
            const p = r.prop || {};
            const status = this.statusOf(r);
            return `<tr data-id="${this.esc(r.id)}">
                <td><span class="status-badge ${r._stageRank >= 3 && r._stageRank <= 5 ? "pursuing" : r._stage === "satisfied" ? "dead" : "new"}">${this.esc(st[1])}</span></td>
                <td style="font-family:var(--font-mono); font-size:0.78rem">${r._sale ? this.esc(r._sale.toLocaleDateString("en-US")) + "<br>" + days : "—"}</td>
                <td class="property-cell"><div class="address">${this.esc(p.address || r.address || "address unknown")}${p.city || r.city ? ", " + this.esc(p.city || r.city) : ""}</div>
                    <div class="meta" style="font-family:var(--font-mono)">${this.esc(p.acct || "")} ${p.ooi === "H" ? '<span style="color:var(--red)">owner-occupied</span>' : p.ooi ? '<span style="color:var(--green)">not owner-occ.</span>' : ""}</div></td>
                <td style="font-size:0.78rem">${r.hoa ? "🏘 " : ""}${this.esc(r.plaintiff || "")}<div class="meta" style="font-family:var(--font-mono)">${this.esc(r.case)}${r.judgment_amt ? " · $" + Number(r.judgment_amt).toLocaleString() : ""}</div></td>
                <td class="name-cell" style="font-size:0.78rem">${this.esc(r.defendant || "")}${this.notesOf(r) ? '<span class="note-badge" title="Has notes">✎</span>' : ""}</td>
                <td class="value-cell">${this.money(r._value)}</td>
                <td class="value-cell" title="Assessed value minus a rough first-mortgage estimate">${r._equity == null ? "—" : this.money(r._equity)}</td>
                <td><span class="status-badge ${status}">${status.toUpperCase()}</span></td>
            </tr>`;
        }).join("");
        tbody.querySelectorAll("tr").forEach((tr) => tr.addEventListener("click", () => this.open(tr.dataset.id)));
    },

    statusOf(r) { const s = SharedNotes.get(r.id); return (s && s.status) || "new"; },
    notesOf(r) { const s = SharedNotes.get(r.id); return (s && s.notes) || ""; },

    open(id) {
        const r = this.rows.find((x) => x.id === id); if (!r) return;
        const p = r.prop || {};
        const row = (l, v, st) => v ? `<div class="detail-row"><span class="label">${l}</span><span class="value"${st ? ` style="${st}"` : ""}>${v}</span></div>` : "";
        const step = (l, v) => `<div class="detail-row"><span class="label">${l}</span><span class="value" style="font-family:var(--font-mono)">${v ? this.esc(v) : '<span style="color:var(--text-dim)">—</span>'}</span></div>`;
        const csUrl = "https://casesearch.courts.state.md.us/casesearch/case-detail-page?caseId=" + encodeURIComponent(String(r.case).replace(/-/g, ""));
        const maps = "https://www.google.com/maps/search/" + encodeURIComponent([p.address || r.address, p.city || r.city, "MD"].filter(Boolean).join(", "));
        const flags = [];
        if (p.ooi === "H") flags.push("Flagged owner-occupied (homestead). Expect a fight: exceptions, a bankruptcy filing (new $125K bankruptcy homestead, 6/1/2026), or a quiet-title suit. Keep your bid defensible against the equity.");
        if (r.satisfied && r.sold && this.d(r.satisfied) > this.d(r.sold)) flags.push(`Debtor paid the judgment (${r.satisfied}) AFTER the sale (${r.sold}). Under Baltimore XV v. Newsteps' Choice (Md. 2026) that does not undo the sale, but expect exceptions on price.`);
        else if (r.satisfied) flags.push("Judgment shows paid/satisfied before any sale — the levy is released, nothing to buy.");
        if (r.bankruptcy) flags.push(`Bankruptcy entry on the docket (${r.bankruptcy}) — the automatic stay stops the sale. Check PACER.`);
        if (r.from === "case detail" && !r.lien && r.county !== "BACI") flags.push("No 'Lien of Judgment Recorded in Circuit Court' entry found — without it the clerk can't issue a writ against land (Rule 3-641(a)). Check the circuit-court JG case.");
        if (r.garnish_tries) flags.push(`The creditor tried ${r.garnish_tries} garnishment(s) first — real collection effort, so the house is likely next.`);
        if (r.levy && !r.sold && !r.satisfied && !r.sale_date) {
            const age = Math.round((Date.now() - this.d(r.levy)) / 864e5);
            if (age > 120) flags.push(`Levy is ${age} days old with no sale — past the 120 days after which the court may release it on the owner's motion (Rule 3-643(c)). The creditor may have stopped pushing; check for a settlement or a new levy before spending time.`);
        }
        if (r.garnish_after_writ) flags.push(`The creditor went back to garnishment after the writ (${r.garnish_after_writ}) — a sign they're collecting another way instead of selling.`);
        if (r.release_or_dismiss) flags.push(`Later entry: ${r.release_or_dismiss}. Often means a payment deal or payoff — watch for a Satisfaction.`);
        if (r.service_issues) flags.push("Service problems in the docket (non-est / evading). Pull the file: a judgment entered without proper service can unwind the sale (Douglas v. Highbridge).");
        document.getElementById("modalBody").innerHTML = `
            <div class="detail-section"><h3>Where This Sits</h3>
                ${step("Judgment", (r.judgment_date || "") + (r.judgment_amt ? "  ·  $" + Number(r.judgment_amt).toLocaleString() : ""))}
                ${step("Lien recorded in circuit ct", r.lien)}
                ${step("Writ requested", r.writ_req)}
                ${step("Writ issued", r.writ_issued)}
                ${step("Levy (posted on property)", r.levy)}
                ${step("Sale advertised for", r.sale_date ? r.sale_date + (r.sale_time ? " " + r.sale_time : "") : "")}
                ${step("Earliest lawful sale", r._sale ? r._sale.toLocaleDateString("en-US") + (r._days != null ? (r._days >= 0 ? `  (in ${r._days} days)` : `  (${-r._days} days ago)`) : "") : "")}
                ${step("Sold / report of sale", r.sold)}
                ${step("Ratified", r.ratified)}
                ${step("Paid / satisfied", r.satisfied)}
                <p style="font-size:0.78rem; color:var(--text-dim); margin-top:6px">Sale can't happen until 30 days after the levy and 20 days after the newspaper notice (Rule 3-644). Watch the county legal-notice paper and the courthouse door from the levy date on. Re-paste the case page every week or two to refresh.</p>
            </div>
            <div class="detail-section"><h3>Case</h3>
                ${row("Case", `<a href="${csUrl}" target="_blank" rel="noopener" style="font-family:var(--font-mono)">${this.esc(r.case)} ↗</a>`)}
                ${row("Caption", this.esc(r.caption || ""))}
                ${row("Creditor", (r.hoa ? "🏘 " : "") + this.esc(r.plaintiff || ""))}
                ${row("Creditor's lawyer", this.esc([r.plaintiff_atty, r.plaintiff_firm].filter(Boolean).join(" — ")))}
                ${row("Owner / debtor", this.esc(r.defendant || ""), "font-weight:600")}
                ${row("Debtor address (court)", this.esc([r.address, r.city, r.zip].filter(Boolean).join(", ")))}
                ${row("Buyer already on case", this.esc(r.purchaser || ""), "color:var(--yellow)")}
                ${row("Source", this.esc(r.from || r._src || ""))}
            </div>
            <div class="detail-section"><h3>Property & Rough Equity</h3>
                ${p.acct ? "" : '<p style="font-size:0.82rem; color:var(--yellow)">No parcel match yet — check the address in SDAT.</p>'}
                ${row("Parcel", this.esc([p.address, p.city, p.zip].filter(Boolean).join(", ")) + (p.acct ? ` · <span style="font-family:var(--font-mono)">${this.esc(p.acct)}</span>` : ""))}
                ${row("Use / built", this.esc([p.use, p.built && +p.built > 0 ? "built " + p.built : ""].filter(Boolean).join(" · ")))}
                ${row("Owner-occupied flag", p.ooi ? (p.ooi === "H" ? "Yes (H)" : "No (" + this.esc(p.ooi) + ") — flag is often wrong; drive by") : "")}
                ${row("Tax bill goes to", this.esc(p.mail || ""))}
                ${row("Assessed value", this.money(p.value), "color:var(--green); font-family:var(--font-mono)")}
                ${row("Last sale", p.tradate ? `${String(p.tradate).slice(4, 6)}/${String(p.tradate).slice(0, 4)} for ${this.money(p.price)} · deed ${this.esc(p.deed)}` : "")}
                ${row("Est. 1st mortgage left", r._mort ? this.money(r._mort) : (p.tradate ? "$0 (paid off by now if never refinanced)" : ""), "font-family:var(--font-mono)")}
                ${row("Est. owner equity", r._equity == null ? "" : this.money(r._equity), "color:var(--green); font-weight:700")}
                <p style="font-size:0.75rem; color:var(--text-dim)">Estimate = 80% loan at that year's typical rate, 30-yr. Refinances, HELOCs, other judgments and taxes are NOT counted — pull every deed of trust and judgment in Land Records before bidding. Courts measure an unconscionable price against this equity (McCartney v. Frost; Baltimore XV, 2026).</p>
                ${row("Links", `<a href="${maps}" target="_blank" rel="noopener">Map ↗</a>${p.sdat ? ` · <a href="${this.esc(p.sdat)}" target="_blank" rel="noopener">SDAT ↗</a>` : ""}`)}
            </div>
            ${flags.length ? `<div class="detail-section"><h3>Watch Out</h3>${flags.map((f) => `<div class="detail-row" style="display:block; font-size:0.82rem">⚠ ${this.esc(f)}</div>`).join("")}</div>` : ""}
            <div class="detail-section"><button type="button" class="btn" id="writRemoveBtn" style="color:var(--red); border-color:var(--red)">🗑 Remove from Writ Watch</button>
                <span style="font-size:0.75rem; color:var(--text-dim); margin-left:8px">Removes it for everyone. Paste the case again to bring it back.</span></div>
            ${(r.docket || []).length ? `<div class="detail-section"><h3>Key Docket Entries</h3>${r.docket.map((x) => `<div class="detail-row"><span class="label" style="font-family:var(--font-mono)">${this.esc(x.date)}</span><span class="value" style="font-size:0.8rem">${this.esc(x.name)}${x.comment ? ` — <span style="color:var(--text-dim)">${this.esc(x.comment.slice(0, 120))}</span>` : ""}</span></div>`).join("")}</div>` : ""}`;
        document.getElementById("writRemoveBtn").onclick = () => this.remove(r.id);
        const sel = document.getElementById("leadStatusSelect"), notes = document.getElementById("leadNotes");
        sel.value = this.statusOf(r); notes.value = this.notesOf(r);
        SharedNotes.footer(r.id);
        document.getElementById("saveLeadBtn").onclick = () => {
            SharedNotes.save("writ", r.id, sel.value, notes.value, `${p.address || r.address || r.case} — ${r.defendant || ""} (${r.plaintiff || ""})`);
            App.closeModal(); this.render();
        };
        document.getElementById("modalOverlay").classList.add("active");
    },

    exportCSV() {
        const rows = this.filtered(); if (!rows.length) { alert("Nothing to export."); return; }
        const H = ["Stage", "Earliest Sale", "Case", "Creditor", "HOA", "Debtor", "Address", "City", "County", "Account", "Owner-Occupied", "Assessed", "Est Mortgage", "Est Equity", "Judgment $", "Judgment Date", "Lien", "Writ Requested", "Writ Issued", "Levy", "Sold", "Satisfied", "Status", "Notes"];
        const q = (v) => `"${String(v == null ? "" : v).replace(/"/g, '""')}"`;
        const lines = [H.join(",")];
        rows.forEach((r) => { const p = r.prop || {}; lines.push([r._stage, r._sale ? r._sale.toISOString().slice(0, 10) : "", r.case, r.plaintiff, r.hoa ? "Y" : "", r.defendant, p.address || r.address, p.city || r.city, r.county, p.acct ? `="${p.acct}"` : "", p.ooi, p.value, Math.round(r._mort || 0), r._equity == null ? "" : Math.round(r._equity), r.judgment_amt, r.judgment_date, r.lien, r.writ_req, r.writ_issued, r.levy, r.sold, r.satisfied, this.statusOf(r), this.notesOf(r)].map(q).join(",")); });
        const a = document.createElement("a"); a.href = URL.createObjectURL(new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8;" }));
        a.download = `md-writ-watch-${new Date().toISOString().slice(0, 10)}.csv`; document.body.appendChild(a); a.click(); a.remove();
    },

    money(v) { const n = Number(v); return (v == null || v === "" || isNaN(n) || !n) ? "—" : "$" + Math.round(n).toLocaleString(); },
    fmtDT(s) { const d = new Date(s); return isNaN(d) ? (s || "") : d.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }); },
    esc(s) { return s == null ? "" : String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); },
};
