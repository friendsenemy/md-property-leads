/* ═══════════════════════════════════════════════
   MD Property Leads — Frontend JavaScript
   ═══════════════════════════════════════════════ */

const App = {
    state: {
        leads: [],
        total: 0,
        page: 1,
        pages: 1,
        perPage: 25,
        filter: "all",
        search: "",
        sortBy: "created_at",
        sortDir: "desc",
        stats: {},
        scrapeRunning: false,
    },

    // ─── Init ───
    init() {
        this.bindEvents();
        this.loadStats();
        this.loadLeads();
        this.pollScrapeStatus();
        // Poll scrape status every 5 seconds
        setInterval(() => this.pollScrapeStatus(), 5000);
    },

    // ─── Event Binding ───
    bindEvents() {
        // Search
        const searchInput = document.getElementById("searchInput");
        let searchTimeout;
        searchInput.addEventListener("input", (e) => {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                this.state.search = e.target.value;
                this.state.page = 1;
                this.loadLeads();
            }, 300);
        });

        // Filter buttons
        document.querySelectorAll(".filter-btn").forEach((btn) => {
            btn.addEventListener("click", () => {
                document.querySelectorAll(".filter-btn").forEach(b => b.classList.remove("active"));
                btn.classList.add("active");
                this.state.filter = btn.dataset.filter;
                this.state.page = 1;
                this.loadLeads();
            });
        });

        // Scrape button
        document.getElementById("scrapeBtn").addEventListener("click", () => {
            this.startScrape();
        });

        // Export button
        document.getElementById("exportBtn").addEventListener("click", () => {
            this.exportSkipTrace();
        });

        // Modal close
        document.getElementById("modalOverlay").addEventListener("click", (e) => {
            if (e.target.id === "modalOverlay") this.closeModal();
        });
        document.getElementById("modalClose").addEventListener("click", () => {
            this.closeModal();
        });

        // Keyboard shortcuts
        document.addEventListener("keydown", (e) => {
            if (e.key === "Escape") this.closeModal();
        });
    },

    // ─── API Calls ───
    async loadStats() {
        try {
            const resp = await fetch("/api/stats");
            const stats = await resp.json();
            this.state.stats = stats;
            this.renderStats(stats);
        } catch (e) {
            console.error("Failed to load stats:", e);
        }
    },

    async loadLeads() {
        try {
            const params = new URLSearchParams({
                status: this.state.filter,
                search: this.state.search,
                sort: this.state.sortBy,
                dir: this.state.sortDir,
                page: this.state.page,
                per_page: this.state.perPage,
            });

            const resp = await fetch(`/api/leads?${params}`);
            const data = await resp.json();

            this.state.leads = data.leads;
            this.state.total = data.total;
            this.state.pages = data.pages;

            this.renderLeads();
            this.renderPagination();
        } catch (e) {
            console.error("Failed to load leads:", e);
        }
    },

    async startScrape() {
        const btn = document.getElementById("scrapeBtn");
        btn.disabled = true;
        btn.classList.add("loading");

        try {
            const resp = await fetch("/api/scrape/start", { method: "POST" });
            const data = await resp.json();

            if (resp.ok) {
                this.state.scrapeRunning = true;
                this.updateScrapeBar(true, "Starting scrape pipeline...");
            } else {
                alert(data.error || "Failed to start scrape");
            }
        } catch (e) {
            console.error("Failed to start scrape:", e);
        } finally {
            btn.disabled = false;
            btn.classList.remove("loading");
        }
    },

    async pollScrapeStatus() {
        try {
            const resp = await fetch("/api/scrape/status");
            const status = await resp.json();

            if (status.running) {
                this.state.scrapeRunning = true;
                this.updateScrapeBar(true, status.message);
            } else if (this.state.scrapeRunning) {
                // Scrape just finished
                this.state.scrapeRunning = false;
                this.updateScrapeBar(false, status.message);
                this.loadStats();
                this.loadLeads();

                // Show completion briefly then hide
                setTimeout(() => {
                    document.getElementById("scrapeBar").classList.remove("active");
                }, 5000);
            }
        } catch (e) {
            // Silently ignore polling errors
        }
    },

    exportSkipTrace() {
        const status = this.state.filter !== "all" ? `?status=${this.state.filter}` : "";
        window.location.href = `/api/export/skip-trace${status}`;
    },

    async updateLeadStatus(leadId, status, notes) {
        try {
            await fetch(`/api/leads/${leadId}/status`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ status, notes }),
            });
            this.loadLeads();
            this.loadStats();
        } catch (e) {
            console.error("Failed to update lead:", e);
        }
    },

    // ─── Rendering ───
    renderStats(stats) {
        document.getElementById("statTotal").textContent = stats.total_leads || 0;
        document.getElementById("statNew").textContent = stats.new_leads || 0;
        document.getElementById("statHot").textContent = stats.hot || 0;
        document.getElementById("statContacted").textContent = stats.contacted || 0;
        document.getElementById("statProperties").textContent = stats.total_properties || 0;

        // Last scrape info
        const lastScrape = stats.last_scrape;
        const el = document.getElementById("lastScrapeInfo");
        if (lastScrape) {
            const time = lastScrape.completed_at || lastScrape.started_at;
            el.textContent = `Last scan: ${this.formatDate(time)} — ${lastScrape.leads_created} leads`;
        } else {
            el.textContent = "No scans run yet";
        }
    },

    renderLeads() {
        const tbody = document.getElementById("leadsBody");
        const emptyState = document.getElementById("emptyState");

        if (!this.state.leads.length) {
            tbody.innerHTML = "";
            emptyState.style.display = "block";
            return;
        }

        emptyState.style.display = "none";
        tbody.innerHTML = this.state.leads.map((lead) => {
            const props = lead.properties || [];
            const primaryProp = props[0] || {};
            const totalValue = props.reduce((sum, p) => {
                return sum + (parseFloat(p.assessed_value) || 0);
            }, 0);

            return `
                <tr onclick="App.openLead(${lead.lead_id})" data-id="${lead.lead_id}">
                    <td class="name-cell">
                        ${this.escapeHtml(lead.full_name)}
                        ${lead.obituary_url ?
                            `<a href="${lead.obituary_url}" target="_blank" class="obit-link"
                                onclick="event.stopPropagation()">View Obituary ↗</a>` : ""}
                    </td>
                    <td class="date-cell">${this.escapeHtml(lead.date_of_death || "N/A")}</td>
                    <td class="property-cell">
                        <div class="address">${this.escapeHtml(primaryProp.property_address || "N/A")}</div>
                        <div class="meta">
                            ${props.length > 1 ? `+${props.length - 1} more properties` : ""}
                            ${primaryProp.property_type ? `• ${primaryProp.property_type}` : ""}
                        </div>
                    </td>
                    <td class="county-cell">${this.escapeHtml(primaryProp.county || "N/A")}</td>
                    <td class="value-cell">${totalValue ? "$" + totalValue.toLocaleString() : "N/A"}</td>
                    <td>
                        <span class="status-badge ${lead.status}">${this.getStatusLabel(lead)}</span>
                    </td>
                    <td class="date-cell">${this.formatDate(lead.lead_created_at)}</td>
                </tr>
            `;
        }).join("");
    },

    renderPagination() {
        const info = document.getElementById("pageInfo");
        const start = (this.state.page - 1) * this.state.perPage + 1;
        const end = Math.min(this.state.page * this.state.perPage, this.state.total);
        info.textContent = this.state.total
            ? `Showing ${start}-${end} of ${this.state.total} leads`
            : "No leads found";

        document.getElementById("prevBtn").disabled = this.state.page <= 1;
        document.getElementById("nextBtn").disabled = this.state.page >= this.state.pages;
    },

    // ─── Modal ───
    openLead(leadId) {
        const lead = this.state.leads.find((l) => l.lead_id === leadId);
        if (!lead) return;

        const modal = document.getElementById("modalOverlay");
        const body = document.getElementById("modalBody");
        const props = lead.properties || [];

        let propsHtml = props.map((p) => `
            <div style="margin-bottom:12px; padding:12px; background:var(--bg-card);
                        border-radius:var(--radius); border:1px solid var(--border);">
                <div class="detail-row">
                    <span class="label">Address</span>
                    <span class="value" style="font-weight:600">${this.escapeHtml(p.property_address || "N/A")}</span>
                </div>
                <div class="detail-row">
                    <span class="label">County</span>
                    <span class="value" style="color:var(--purple)">${this.escapeHtml(p.county || "N/A")}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Type</span>
                    <span class="value">${this.escapeHtml(p.property_type || "N/A")}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Assessed Value</span>
                    <span class="value" style="color:var(--green); font-family:var(--font-mono)">
                        ${p.assessed_value ? "$" + parseFloat(p.assessed_value).toLocaleString() : "N/A"}
                    </span>
                </div>
                <div class="detail-row">
                    <span class="label">Land / Improvement</span>
                    <span class="value" style="font-family:var(--font-mono)">
                        ${p.land_value ? "$" + parseFloat(p.land_value).toLocaleString() : "N/A"} /
                        ${p.improvement_value ? "$" + parseFloat(p.improvement_value).toLocaleString() : "N/A"}
                    </span>
                </div>
                <div class="detail-row">
                    <span class="label">Year Built</span>
                    <span class="value">${this.escapeHtml(p.year_built || "N/A")}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Lot Size</span>
                    <span class="value">${this.escapeHtml(p.lot_size || "N/A")}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Account #</span>
                    <span class="value" style="font-family:var(--font-mono)">${this.escapeHtml(p.account_number || "N/A")}</span>
                </div>
            </div>
        `).join("");

        body.innerHTML = `
            <div class="detail-section">
                <h3>Deceased Information</h3>
                <div class="detail-row">
                    <span class="label">Full Name</span>
                    <span class="value" style="font-weight:600">${this.escapeHtml(lead.full_name)}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Date of Death</span>
                    <span class="value" style="font-family:var(--font-mono)">${this.escapeHtml(lead.date_of_death || "N/A")}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Date of Birth</span>
                    <span class="value" style="font-family:var(--font-mono)">${this.escapeHtml(lead.date_of_birth || "N/A")}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Age</span>
                    <span class="value">${lead.age || "N/A"}</span>
                </div>
                <div class="detail-row">
                    <span class="label">City</span>
                    <span class="value">${this.escapeHtml(lead.obit_city || "N/A")}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Survived By</span>
                    <span class="value">${this.escapeHtml(lead.survived_by || "N/A")}</span>
                </div>
                ${lead.obituary_url ? `
                <div class="detail-row">
                    <span class="label">Obituary</span>
                    <span class="value"><a href="${lead.obituary_url}" target="_blank">View on Legacy.com ↗</a></span>
                </div>` : ""}
            </div>

            <div class="detail-section">
                <h3>Properties (${props.length})</h3>
                ${propsHtml || '<p style="color:var(--text-dim)">No property details available</p>'}
            </div>
        `;

        // Set status dropdown
        const statusSelect = document.getElementById("leadStatusSelect");
        statusSelect.value = lead.status;

        // Set notes
        const notesField = document.getElementById("leadNotes");
        notesField.value = lead.notes || "";

        // Save button handler
        const saveBtn = document.getElementById("saveLeadBtn");
        saveBtn.onclick = () => {
            this.updateLeadStatus(lead.lead_id, statusSelect.value, notesField.value);
            this.closeModal();
        };

        modal.classList.add("active");
    },

    closeModal() {
        document.getElementById("modalOverlay").classList.remove("active");
    },

    // ─── Pagination ───
    prevPage() {
        if (this.state.page > 1) {
            this.state.page--;
            this.loadLeads();
        }
    },

    nextPage() {
        if (this.state.page < this.state.pages) {
            this.state.page++;
            this.loadLeads();
        }
    },

    // ─── Sorting ───
    sortBy(column) {
        if (this.state.sortBy === column) {
            this.state.sortDir = this.state.sortDir === "desc" ? "asc" : "desc";
        } else {
            this.state.sortBy = column;
            this.state.sortDir = "desc";
        }
        this.loadLeads();
    },

    // ─── Helpers ───
    updateScrapeBar(active, message) {
        const bar = document.getElementById("scrapeBar");
        const msg = document.getElementById("scrapeMessage");
        if (active) {
            bar.classList.add("active");
            msg.textContent = message;
        } else {
            msg.textContent = message;
            // Keep visible briefly after completion
        }
    },

    formatDate(dateStr) {
        if (!dateStr) return "N/A";
        try {
            const d = new Date(dateStr);
            if (isNaN(d)) return dateStr;
            return d.toLocaleDateString("en-US", {
                month: "short", day: "numeric", year: "numeric"
            });
        } catch {
            return dateStr;
        }
    },

    getStatusLabel(lead) {
        if (lead.status !== "new") return lead.status;
        // For "new" leads, show age in days
        try {
            const created = new Date(lead.lead_created_at);
            if (isNaN(created)) return "new";
            const now = new Date();
            const diffMs = now - created;
            const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
            if (diffDays === 0) return "new";
            if (diffDays === 1) return "1-Day Old";
            return `${diffDays}-Day Old`;
        } catch {
            return "new";
        }
    },

    escapeHtml(str) {
        if (!str) return "";
        const div = document.createElement("div");
        div.textContent = str;
        return div.innerHTML;
    },
};

// Boot
document.addEventListener("DOMContentLoaded", () => App.init());
