# Shared notes: one Google Sheet everyone's notes go to

Right now each person's statuses and notes live in their own browser, so what
Todd types at home never reaches Ray. This puts them in one Google Sheet that
the dashboard reads on load and writes to on every Save. Free, no server, you
own the data, and the sheet itself is a readable log of every note anyone
ever made.

Takes about five minutes, once.

## 1. Make the sheet

1. Go to sheets.google.com, make a new blank spreadsheet.
2. Name it **MD Property Leads — Notes**.
3. In row 1 type these seven headers, one per column, A through G:

       ts   author   tab   id   label   status   notes

## 2. Add the script

1. In the sheet: **Extensions → Apps Script**.
2. Delete whatever is in the editor and paste the whole block below.
3. Click the save icon.

```javascript
// MD Property Leads — shared notes endpoint.
// GET  returns every row as JSON; the dashboard keeps the newest per lead.
// POST appends one row. Body is JSON: {ts, author, tab, id, label, status, notes}

const SHEET = "Sheet1";
const COLS = ["ts", "author", "tab", "id", "label", "status", "notes"];

function doGet() {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET);
  const values = sh.getDataRange().getValues();
  const rows = [];
  for (let i = 1; i < values.length; i++) {
    const r = {};
    COLS.forEach((c, j) => { r[c] = values[i][j] instanceof Date ? values[i][j].toISOString() : String(values[i][j] ?? ""); });
    if (r.id) rows.push(r);
  }
  return ContentService.createTextOutput(JSON.stringify({ rows }))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  try {
    const b = JSON.parse(e.postData.contents || "{}");
    if (!b.id) throw new Error("missing id");
    const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET);
    sh.appendRow(COLS.map((c) => c === "ts" ? (b.ts || new Date().toISOString()) : String(b[c] ?? "")));
    return ContentService.createTextOutput(JSON.stringify({ ok: true }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ ok: false, error: String(err) }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}
```

## 3. Publish it as a web app

1. Top right: **Deploy → New deployment**.
2. Gear icon next to "Select type" → **Web app**.
3. Description: `notes`. **Execute as: Me.** **Who has access: Anyone.**
   ("Anyone" is what lets the dashboard call it without a login. The URL is
   long and unguessable; nobody can read the sheet itself without your share.)
4. **Deploy**. Approve the permissions when Google asks (it is your own script
   touching your own sheet).
5. Copy the **Web app URL**. It looks like
   `https://script.google.com/macros/s/AKfycb.../exec`.

## 4. Tell the dashboard

Send that URL to Claude, or paste it yourself into `static/js/config.js`:

```javascript
window.MDPL = {
    SHARED_NOTES_URL: "https://script.google.com/macros/s/AKfycb.../exec",
};
```

Commit, and the next page load shows everyone's notes. The status line under
the status box turns green and says "Shared — everyone sees these".

## How it behaves

- Each person types their name once in the **Your name** box; it is remembered
  in that browser and stamped on their notes.
- The modal shows the full history for a lead: who wrote what, when, and the
  status they set. The newest status and note are what the table shows.
- Saves still write to the browser too, so if the sheet is unreachable nothing
  is lost; the status line goes red and says so.
- To give Todd the sheet itself (not needed for the dashboard), share it from
  Google Sheets like any file.

## If you ever change the script

Apps Script needs a **new deployment** (Deploy → Manage deployments → edit →
new version) for changes to take effect. The URL stays the same.
