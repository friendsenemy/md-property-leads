"""Email the new strong auction leads through the shared-notes Apps Script.

The Apps Script (see docs/shared-notes-setup.md) accepts
    POST {action:"alert", token, subject, html}
and sends it with MailApp to the addresses in its Settings sheet. The token is
a GitHub Actions secret matched against the script's ALERT_TOKEN, so a random
visitor who finds the web-app URL cannot make it email anyone.

Nothing here is sent unless at least one lead is NEW today and STRONG (or a
POSSIBLE one with a surplus estimate of $50k+).
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys

import requests

log = logging.getLogger("alert")
AUCTIONS = "data/surplus/auctions.json"
CONFIG_JS = "static/js/config.js"
DASH = "https://pagesofpurposellc.com/md-property-leads/#surplus"


def notes_url():
    m = re.search(r'SHARED_NOTES_URL:\s*"([^"]+)"', open(CONFIG_JS, encoding="utf-8").read())
    return m.group(1) if m else ""


def money(v):
    return "$" + f"{round(v):,}" if v is not None else "—"


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    url, token = notes_url(), os.environ.get("ALERT_TOKEN", "")
    if not url:
        log.info("no notes URL; skipping email"); return 0
    d = json.load(open(AUCTIONS, encoding="utf-8"))
    rows = [r for r in d["rows"] if r.get("is_new") and (
        r["tier"] == "STRONG" or (r.get("surplus_est") and r["surplus_est"][1] >= 50000))]
    if not rows:
        log.info("nothing new and strong today"); return 0
    rows.sort(key=lambda r: -(r["surplus_est"][1] if r.get("surplus_est") else 0))
    sold = [r for r in rows if r["stage"] == "AUCTION_SOLD"]
    sched = [r for r in rows if r["stage"] == "AUCTION_SCHEDULED"]

    def block(r):
        se = r.get("surplus_est") or [0, 0, 0]
        return (f"<p style='margin:0 0 14px'><b style='font-size:16px'>{r['address']}, {r.get('city') or ''} {r.get('zip') or ''}</b>"
                f" — {r['county']} County<br>"
                f"<b>{'Former owner' if r['stage']=='AUCTION_SOLD' else 'Owner'}:</b> {r.get('owner_of_record') or '—'}"
                + (f" · mail: {r['mail']}" if r.get("mail") else "") + "<br>"
                + (f"<b>Sold {r.get('sale_date')}</b> for <b>{money(r.get('hammer'))}</b> · " if r['stage']=='AUCTION_SOLD'
                   else f"<b>Sale {r.get('sale_date')}</b> · assessed {money(r.get('assessed_value'))} · ")
                + f"est. payoff {money((r.get('payoff_est') or [None,None,None])[1])} · "
                f"<b style='color:#0a7'>est. {'surplus' if r['stage']=='AUCTION_SOLD' else 'equity'} {money(se[1])}</b> (range {money(se[0])}–{money(se[2])})<br>"
                f"<span style='color:#666'>{r.get('why','')}</span> · tier <b>{r['tier']}</b> · acct {r['account']}</p>")

    html = (f"<div style='font-family:Arial,sans-serif;font-size:14px;max-width:680px'>"
            f"<p>{len(rows)} new lead{'s' if len(rows)!=1 else ''} this morning. Open the dashboard → Surplus Funds Leads → 🔔 Auction Sold for the letter and the find-them links.</p>"
            + (f"<h3 style='margin:18px 0 8px'>Sold at auction — surplus likely ({len(sold)})</h3>" + "".join(block(r) for r in sold) if sold else "")
            + (f"<h3 style='margin:18px 0 8px'>Scheduled — owner still holds title ({len(sched)})</h3>" + "".join(block(r) for r in sched) if sched else "")
            + f"<p style='margin-top:18px'><a href='{DASH}'>Open the dashboard</a></p>"
            f"<p style='color:#888;font-size:12px'>Estimates from the owner's purchase price and year. Confirm in the Circuit Court case. "
            f"Any agreement goes through a Maryland lawyer first (RP §7-301 et seq., §7-314/315).</p></div>")
    subj = f"🔔 {len(rows)} foreclosure surplus lead{'s' if len(rows)!=1 else ''}: " + ", ".join(
        f"{r['address'].split(',')[0]} ({money((r.get('surplus_est') or [0,0,0])[1])})" for r in rows[:3])
    r = requests.post(url, data=json.dumps({"action": "alert", "token": token, "subject": subj, "html": html}),
                      headers={"Content-Type": "text/plain"}, timeout=60, allow_redirects=True)
    log.info("alert POST %s: %s", r.status_code, r.text[:200])
    return 0


if __name__ == "__main__":
    sys.exit(main())
