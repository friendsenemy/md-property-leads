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


# ââââââââââââââââââââââââââââââââââââââââââââââ
#  Scrape Pipeline
# ââââââââââââââââââââââââââââââââââââââââââââââ

def run_scrape_pipeline():
    """
    Full pipeline: scrape obituaries -> lookup properties -> create leads.
    Only leads with property matches are kept.
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

        # Step 2: For each obituary, check SDAT for property ownership
        for i, obit in enumerate(obituaries):
            scrape_status["message"] = (
                f"Checking property records ({i+1}/{obits_found}): "
                f"{obit.get('full_name', 'Unknown')}"
            )

            # Insert obituary into database
            obit_id = db.insert_obituary(obit)
            if not obit_id:
                continue  # Duplicate

            # Search for property by owner name
            last_name = obit.get("last_name", "")
            first_name = obit.get("first_name", "")

            if not last_name:
                continue

            properties = search_property_by_name(last_name, first_name)

            if properties:
                props_matched += len(properties)

                # Fetch additional obituary details for matched leads
                if obit.get("obituary_url"):
                    details = fetch_obituary_details(obit["obituary_url"])
                    if details:
                        # Update obituary with additional info
                        pass  # Details are enrichment, stored in initial scrape

                # Save properties and create lead
                for prop in properties:
                    db.insert_property(obit_id, prop)

                db.create_lead(obit_id)
                leads_created += 1
                logger.info(
                    f"LEAD: {obit['full_name']} - "
                    f"{len(properties)} properties found"
                )

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


# ââââââââââââââââââââââââââââââââââââââââââââââ
#  Routes - Pages
# ââââââââââââââââââââââââââââââââââââââââââââââ

@app.route("/")
def index():
    """Main dashboard page."""
    return render_template("index.html")


# ââââââââââââââââââââââââââââââââââââââââââââââ
#  Routes - API
# ââââââââââââââââââââââââââââââââââââââââââââââ

@app.route("/api/leads")
def api_leads():
    """Get paginated leads list."""
    status = request.args.get("status", "all")
    search = request.args.get("search", "")
    sort_by = request.args.get("sort", "created_at")
    sort_dir = request.args.get("dir", "desc")
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 25))

    leads, total = db.get_leads(
        status=status, search=search, sort_by=sort_by,
        sort_dir=sort_dir, page=page, per_page=per_page
    )

    return jsonify({
        "leads": leads,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page if per_page else 1,
    })


@app.route("/api/leads/<int:lead_id>/status", methods=["POST"])
def api_update_lead(lead_id):
    """Update a lead's status."""
    data = request.get_json()
    status = data.get("status", "new")
    notes = data.get("notes")
    db.update_lead_status(lead_id, status, notes)
    return jsonify({"success": True})


@app.route("/api/stats")
def api_stats():
    """Get dashboard statistics."""
    stats = db.get_stats()
    return jsonify(stats)


@app.route("/api/scrape/start", methods=["POST"])
def api_start_scrape():
    """Manually trigger a scrape run."""
    if scrape_status["running"]:
        return jsonify({"error": "Scrape already in progress"}), 409

    thread = threading.Thread(target=run_scrape_pipeline, daemon=True)
    thread.start()
    return jsonify({"success": True, "message": "Scrape started"})


@app.route("/api/scrape/status")
def api_scrape_status():
    """Get current scrape status."""
    return jsonify(scrape_status)


@app.route("/api/export/skip-trace")
def api_export_skip_trace():
    """Export leads as a skip tracing CSV."""
    status = request.args.get("status", "all")
    leads = db.get_leads_for_export(status)

    output = io.StringIO()
    writer = csv.writer(output)

    # Skip tracing standard headers
    writer.writerow([
        "First Name", "Last Name", "Middle Name", "Full Name",
        "Date of Death", "Date of Birth", "Age",
        "Obituary City", "Obituary State",
        "Property Address", "Property City", "Property County",
        "Property State", "Property Zip",
        "Property Type", "Assessed Value", "Land Value",
        "Improvement Value", "Lot Size", "Year Built",
        "Account Number",
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


# ââââââââââââââââââââââââââââââââââââââââââââââ
#  Startup
# ââââââââââââââââââââââââââââââââââââââââââââââ

def create_app():
    """Application factory."""
    db.init_db()

    # Set up daily scheduled scraping
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scrape_pipeline,
        trigger="cron",
        hour=6,
        minute=0,
        id="daily_scrape",
        name="Daily Obituary Scrape",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started - daily scrape at 6:00 AM")

    return app


if __name__ == "__main__":
    application = create_app()
    port = int(os.environ.get("PORT", 5000))
    application.run(debug=True, host="0.0.0.0", port=port)
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


# ââââââââââââââââââââââââââââââââââââââââââââââ
#  Scrape Pipeline
# ââââââââââââââââââââââââââââââââââââââââââââââ

def run_scrape_pipeline():
    """
    Full pipeline: scrape obituaries -> lookup properties -> create leads.
    Only leads with property matches are kept.
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

        # Step 2: For each obituary, check SDAT for property ownership
        for i, obit in enumerate(obituaries):
            scrape_status["message"] = (
                f"Checking property records ({i+1}/{obits_found}): "
                f"{obit.get('full_name', 'Unknown')}"
            )

            # Insert obituary into database
            obit_id = db.insert_obituary(obit)
            if not obit_id:
                continue  # Duplicate

            # Search for property by owner name
            last_name = obit.get("last_name", "")
            first_name = obit.get("first_name", "")

            if not last_name:
                continue

            properties = search_property_by_name(last_name, first_name)

            if properties:
                props_matched += len(properties)

                # Fetch additional obituary details for matched leads
                if obit.get("obituary_url"):
                    details = fetch_obituary_details(obit["obituary_url"])
                    if details:
                        # Update obituary with additional info
                        pass  # Details are enrichment, stored in initial scrape

                # Save properties and create lead
                for prop in properties:
                    db.insert_property(obit_id, prop)

                db.create_lead(obit_id)
                leads_created += 1
                logger.info(
                    f"LEAD: {obit['full_name']} - "
                    f"{len(properties)} properties found"
                )

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


# ââââââââââââââââââââââââââââââââââââââââââââââ
#  Routes - Pages
# ââââââââââââââââââââââââââââââââââââââââââââââ

@app.route("/")
def index():
    """Main dashboard page."""
    return render_template("index.html")


# ââââââââââââââââââââââââââââââââââââââââââââââ
#  Routes - API
# ââââââââââââââââââââââââââââââââââââââââââââââ

@app.route("/api/leads")
def api_leads():
    """Get paginated leads list."""
    status = request.args.get("status", "all")
    search = request.args.get("search", "")
    sort_by = request.args.get("sort", "created_at")
    sort_dir = request.args.get("dir", "desc")
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 25))

    leads, total = db.get_leads(
        status=status, search=search, sort_by=sort_by,
        sort_dir=sort_dir, page=page, per_page=per_page
    )

    return jsonify({
        "leads": leads,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page if per_page else 1,
    })


@app.route("/api/leads/<int:lead_id>/status", methods=["POST"])
def api_update_lead(lead_id):
    """Update a lead's status."""
    data = request.get_json()
    status = data.get("status", "new")
    notes = data.get("notes")
    db.update_lead_status(lead_id, status, notes)
    return jsonify({"success": True})


@app.route("/api/stats")
def api_stats():
    """Get dashboard statistics."""
    stats = db.get_stats()
    return jsonify(stats)


@app.route("/api/scrape/start", methods=["POST"])
def api_start_scrape():
    """Manually trigger a scrape run."""
    if scrape_status["running"]:
        return jsonify({"error": "Scrape already in progress"}), 409

    thread = threading.Thread(target=run_scrape_pipeline, daemon=True)
    thread.start()
    return jsonify({"success": True, "message": "Scrape started"})


@app.route("/api/scrape/status")
def api_scrape_status():
    """Get current scrape status."""
    return jsonify(scrape_status)


@app.route("/api/export/skip-trace")
def api_export_skip_trace():
    """Export leads as a skip tracing CSV."""
    status = request.args.get("status", "all")
    leads = db.get_leads_for_export(status)

    output = io.StringIO()
    writer = csv.writer(output)

    # Skip tracing standard headers
    writer.writerow([
        "First Name", "Last Name", "Middle Name", "Full Name",
        "Date of Death", "Date of Birth", "Age",
        "Obituary City", "Obituary State",
        "Property Address", "Property City", "Property County",
        "Property State", "Property Zip",
        "Property Type", "Assessed Value", "Land Value",
        "Improvement Value", "Lot Size", "Year Built",
        "Account Number",
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


# ââââââââââââââââââââââââââââââââââââââââââââââ
#  Startup
# ââââââââââââââââââââââââââââââââââââââââââââââ

def create_app():
    """Application factory."""
    db.init_db()

    # Set up daily scheduled scraping
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scrape_pipeline,
        trigger="cron",
        hour=6,
        minute=0,
        id="daily_scrape",
        name="Daily Obituary Scrape",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started - daily scrape at 6:00 AM")

    return app


if __name__ == "__main__":
    application = create_app()
    application.run(debug=True, host="0.0.0.0", port=5000)
