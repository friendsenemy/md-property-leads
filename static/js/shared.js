/* Shared status + notes for every tab.
   Store: a Google Sheet behind an Apps Script web app (see docs/shared-notes-setup.md).
   Each save appends a row {ts, author, tab, id, label, status, notes}; the sheet is the
   full history, and the latest row per lead id is what the tabs show. Each browser's
   localStorage stays as an offline cache so nothing is lost if the sheet is unreachable. */
const SharedNotes = {
    url: (window.MDPL && window.MDPL.SHARED_NOTES_URL) || "",
    latest: {},      // id -> {status, notes, author, ts, tab, label}
    history: {},     // id -> [rows newest first]
    loaded: false,
    enabled() { return !!this.url; },

    author() { try { return localStorage.getItem("mdpl_author") || ""; } catch { return ""; } },
    setAuthor(v) { try { localStorage.setItem("mdpl_author", (v || "").trim()); } catch {} },

    async load() {
        if (!this.enabled()) { this.loaded = true; return; }
        try {
            const r = await fetch(`${this.url}?t=${Date.now()}`, { cache: "no-store" });
            const j = await r.json();
            const rows = (j.rows || []).slice().sort((a, b) => String(b.ts).localeCompare(String(a.ts)));
            this.latest = {}; this.history = {};
            for (const row of rows) {
                if (!row.id) continue;
                (this.history[row.id] = this.history[row.id] || []).push(row);
                if (!this.latest[row.id]) this.latest[row.id] = row;
            }
            this.loaded = true; this.error = null;
        } catch (e) {
            this.loaded = true; this.error = String(e);
        }
        document.dispatchEvent(new CustomEvent("mdpl:notes-loaded"));
        this.paintSync();
    },

    get(id) { return this.latest[id] || null; },

    /* Called by every tab's Save. Appends to the sheet and updates the in-memory view. */
    async save(tab, id, status, notes, label) {
        const row = { ts: new Date().toISOString(), author: this.author() || "unknown", tab, id, label: label || "", status, notes };
        this.latest[id] = row;
        (this.history[id] = this.history[id] || []).unshift(row);
        if (!this.enabled()) return true;
        try {
            // text/plain keeps the request "simple" so Apps Script needs no CORS preflight
            const r = await fetch(this.url, { method: "POST", body: JSON.stringify(row) });
            const j = await r.json().catch(() => ({}));
            if (j && j.ok === false) throw new Error(j.error || "save rejected");
            this.error = null; this.paintSync("Saved for everyone");
            return true;
        } catch (e) {
            this.error = String(e); this.paintSync();
            return false;
        }
    },

    /* The modal footer: who-you-are box, note history, sync state. Called by each tab's open(). */
    footer(id) {
        const who = document.getElementById("noteAuthor");
        if (who && !who.value) who.value = this.author();
        const hist = document.getElementById("noteHistory");
        if (hist) {
            const rows = (this.history[id] || []).filter((r) => (r.notes || "").trim() || r.status);
            hist.innerHTML = rows.length
                ? rows.slice(0, 12).map((r) => `<div class="note"><div class="who">${this.esc(r.author || "?")} · ${this.esc(this.fmt(r.ts))} · ${this.esc((r.status || "").toUpperCase())}</div>${this.esc(r.notes || "")}</div>`).join("")
                : "";
        }
        this.paintSync();
    },

    paintSync(msg) {
        const el = document.getElementById("noteSync");
        if (!el) return;
        if (!this.enabled()) { el.className = "note-sync"; el.textContent = "Notes are saved in this browser only — shared notes not set up yet (docs/shared-notes-setup.md)"; return; }
        if (this.error) { el.className = "note-sync err"; el.textContent = "Could not reach the shared sheet — saved here, will not show for others until it reconnects"; return; }
        el.className = "note-sync on"; el.textContent = msg || "Shared — everyone sees these";
    },

    fmt(ts) { const d = new Date(ts); return isNaN(d) ? (ts || "") : d.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }); },
    esc(s) { return s == null ? "" : String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); },
};
document.addEventListener("DOMContentLoaded", () => {
    const who = document.getElementById("noteAuthor");
    if (who) who.addEventListener("change", (e) => SharedNotes.setAuthor(e.target.value));
    SharedNotes.load();
});
