"""One-off: does Ray's Smarty plan include property + financial data? Prints field names and a few values."""
import json, os, requests
ID, TOK = os.environ["SMARTY_AUTH_ID"], os.environ["SMARTY_AUTH_TOKEN"]
auth = {"auth-id": ID, "auth-token": TOK}
for street, city, z in [("9217 MIMOSA DR", "LA PLATA", "20646"), ("1127 HAMLIN RD", "WALDORF", "20601")]:
    print("=" * 70, "\n", street, city)
    addr = {"street": street, "city": city, "state": "MD", "zipcode": z}
    for path, extra in [("property/principal", {}), ("property/financial", {})]:
        e = requests.get(f"https://us-enrichment.api.smarty.com/lookup/search/{path}", params={**auth, **addr, **extra}, timeout=30)
        print("--", path, e.status_code)
        if not e.ok:
            print(e.text[:300]); continue
        j = e.json()
        a = (j[0] if isinstance(j, list) and j else j).get("attributes", {}) if j else {}
        keep = ["owner_full_name", "owner_occupancy_status", "tax_delinquent_year", "mortgage_amount", "mortgage_type",
                "lender_name", "mortgage_recording_date", "mortgage_due_date", "loan_amount", "deed_sale_date",
                "deed_sale_price", "ownership_transfer_date", "contact_full_address", "total_market_value", "assessed_value"]
        print("fields returned:", len(a))
        print(json.dumps({k: a.get(k) for k in keep if a.get(k) not in (None, "")}, indent=1))
        fh = a.get("financial_history") or []
        print("financial_history entries:", len(fh))
        for h in fh[:8]:
            print("  ", {k: h.get(k) for k in ("document_type_description", "recording_date", "mortgage_amount", "mortgage_type", "lender_name", "deed_sale_price") if h.get(k)})
