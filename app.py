"""
MD Property Leads - Flask Application

Real estate lead generator that cross-references Maryland obituaries
with SDAT property records.
"""

import os
import io
import csv
import logging
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template, jsonify, request, Response
from apscheduler.schedulers.background import BackgroundScheduler

import database as db
from scraper import scrape_legacy_obituaries, fetch_obituary_details
from property_lookup import search_property_by_name

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.urandom(24).hex()

# Global scrape lock to prevent concurrent runs
scrape_lock = threading.Lock()
scrape_status = {"running": False, "message": "Idle"}


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Scrape Pipeline - OPTIMIZED with concurrent lookups
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def _process_single_obituary(obit):
    """
    Process one obituary: fetch details, insert to DB, search SDAT, create lead.
    Thread-safe - each call handles its own DB + HTTP operations.
    Returns (obit_name, num_properties, is_new) tuple.
    """
    name = obit.get("full_name", "Unknown")
    try:
        # Fetch full obituary page for dates, survived-by, and full text
        obit_url = obit.get("obituary_url", "")
        if obit_url:
            try:
                details = fetch_obituary_details(obit_url)
                if details:
                    # Merge details into obit â only fill empty fields
                    for key in ("date_of_death", "date_of_birth", "survived_by",
                                "obituary_text", "age"):
                        if details.get(key) and not obit.get(key):
                            obit[key] = details[key]
            except Exception as e:
                logger.debug(f"Could not fetch details for {name}: {e}")

        # Insert obituary into database
        obit_id = db.insert_obituary(obit)
        if not obit_id:
            return (name, 0, False)  # Duplicate

        # Search for property by owner name, passing city for targeting
        last_name = obit.get("last_name", "")
        first_name = obit.get("first_name", "")
        city = obit.get("city", "")

        if not last_name:
            return (name, 0, True)

        properties = search_property_by_name(last_name, first_name, city=city)

        if properties:
            # Save properties and create lead
            for prop in properties:
                db.insert_property(obit_id, prop)
            db.create_lead(obit_id)
            logger.info(f"LEAD: {name} - {len(properties)} properties found")
            return (name, len(properties), True)

        return (name, 0, True)

    except Exception as e:
        logger.error(f"Error processing {name}: {e}")
        return (name, 0, True)


def run_scrape_pipeline():
    """
    Full pipeline: scrape obituaries -> lookup properties -> create leads.
    Uses concurrent processing for SDAT lookups (4 workers).
    """
    global scrape_status

    if not scrape_lock.acquire(blocking=False):
        logger.warning("Scrape already in progress, skipping.")
        return

    try:
        scrape_status = {"running": True, "message": "Starting scrape..."}
        log_id = db.log_scrape_start()
        obits_found = 0
        props_matched = 0
        leads_created = 0

        # Step 1: Scrape obituaries
        scrape_status["message"] = "Scraping obituaries from Legacy.com..."
        logger.info("Starting obituary scrape...")
        obituaries = scrape_legacy_obituaries(max_pages=2)
        obits_found = len(obituaries)
        logger.info(f"Found {obits_found} obituaries")

        # Step 2: Process obituaries with concurrent SDAT lookups
        # Using 4 workers to balance speed vs. not hammering SDAT
        processed = 0
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {}
            for obit in obituaries:
                future = executor.submit(_process_single_obituary, obit)
                futures[future] = obit.get("full_name", "Unknown")

            for future in as_completed(futures):
                processed += 1
                name = futures[future]
                try:
                    result_name, num_props, is_new = future.result()
                    if num_props > 0:
                        props_matched += num_props
                        leads_created += 1
                    # Update status message periodically
                    if processed % 5 == 0 or processed == obits_found:
                        scrape_status["message"] = (
                            f"Checking property records ({processed}/{obits_found}): "
                            f"{name}"
                        )
                except Exception as e:
                    logger.error(f"Error processing {name}: {e}")

        # Step 3: Log completion
        db.log_scrape_end(log_id, obits_found, props_matched, leads_created)
        scrape_status = {
            "running": False,
            "message": (
                f"Completed: {obits_found} obituaries scanned, "
                f"{leads_created} leads created"
            ),
        }
        logger.info(
            f"Scrape complete: {obits_found} obits, "
            f"{props_matched} properties, {leads_created} leads"
        )

    except Exception as e:
        logger.error(f"Scrape pipeline error: {e}")
        scrape_status = {"running": False, "message": f"Error: {str(e)}"}
        try:
            db.log_scrape_end(log_id, obits_found, props_matched, leads_created,
                            status="error", error=str(e))
        except Exception:
            pass

    finally:
        scrape_lock.release()


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Routes - Pages
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.route("/")
def index():
    """Main dashboard page."""
    return render_template("index.html")


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Routes - API
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ

@app.route("/api/leads")
def api_leads():
    """Get paginated leads list."""
    status = request.args.get("status", "all")
    search = request.args.get("search", "")
    sort_by = request.args.get("sort", "created_at")
    sort_dir = request.args.get("dir", "desc")
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 25, type=int)

    leads, total = db.get_leads(
        status=status, search=search,
        sort_by=sort_by, sort_dir=sort_dir,
        page=page, per_page=per_page,
    )

    return jsonify({
        "leads": leads,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page if per_page > 0 else 0,
    })


@app.route("/api/leads/<int:lead_id>", methods=["PUT"])
def api_update_lead(lead_id):
    """Update a lead's status or notes."""
    data = request.get_json()
    status = data.get("status")
    notes = data.get("notes")
    db.update_lead_status(lead_id, status, notes)
    return jsonify({"success": True})


@app.route("/api/stats")
def api_stats():
    """Get dashboard statistics."""
    stats = db.get_stats()
    return jsonify(stats)


@app.route("/api/scrape/start", methods=["POST"])
def api_scrape_start():
    """Start a scrape run."""
    if scrape_status["running"]:
        return jsonify({"message": "Scrape already running", "success": False}), 409

    thread = threading.Thread(target=run_scrape_pipeline)
    thread.daemon = True
    thread.start()

    return jsonify({"message": "Scrape started", "success": True})


@app.route("/api/scrape/status")
def api_scrape_status():
    """Get current scrape status."""
    return jsonify(scrape_status)


@app.route("/api/export")
def api_export():
    """Export leads as CSV for skip tracing."""
    status = request.args.get("status", "all")
    leads = db.get_leads_for_export(status)

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "First Name", "Last Name", "Middle Name", "Full Name",
        "Date of Death", "Date of Birth", "Age",
        "Obituary City", "Obituary State",
        "Property Address", "City", "County", "State", "Zip Code",
        "Property Type", "Assessed Value", "Land Value", "Improvement Value",
        "Lot Size", "Year Built", "Account Number",
        "Survived By / Possible Heirs",
        "Obituary URL",
        "Lead Status", "Notes", "Lead Date",
    ])

    for lead in leads:
        writer.writerow([
            lead.get("first_name", ""),
            lead.get("last_name", ""),
            lead.get("middle_name", ""),
            lead.get("full_name", ""),
            lead.get("date_of_death", ""),
            lead.get("date_of_birth", ""),
            lead.get("age", ""),
            lead.get("obit_city", ""),
            lead.get("obit_state", ""),
            lead.get("property_address", ""),
            lead.get("city", ""),
            lead.get("county", ""),
            lead.get("state", ""),
            lead.get("zip_code", ""),
            lead.get("property_type", ""),
            lead.get("assessed_value", ""),
            lead.get("land_value", ""),
            lead.get("improvement_value", ""),
            lead.get("lot_size", ""),
            lead.get("year_built", ""),
            lead.get("account_number", ""),
            lead.get("survived_by", ""),
            lead.get("obituary_url", ""),
            lead.get("status", ""),
            lead.get("notes", ""),
            lead.get("lead_date", ""),
        ])

    output.seek(0)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=md_skip_trace_{timestamp}.csv"
        },
    )


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Startup
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def create_app():
    """Application factory."""
    db.init_db()

    # Set up daily scheduled scraping
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scrape_pipeline,
        trigger="cron",
        hour=12,
        minute=0,
        id="daily_scrape",
        name="Daily Obituary Scrape",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started - daily scrape at 12:00 PM")

    return app


# Gunicorn entry point
application = create_app()

if __name__ == "__main__":
    application.run(debug=True, host="0.0.0.0", port=5000)
