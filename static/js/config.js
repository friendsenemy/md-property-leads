/* Dashboard settings. Edit here, commit, done.

   SHARED_NOTES_URL: the Google Apps Script web-app URL from docs/shared-notes-setup.md.
   When set, every status change and note from every person using the dashboard is
   written to one shared Google Sheet and shown to everyone. When empty, notes stay
   in each person's own browser (the old behaviour). */
window.MDPL = {
    // Passcode gate. SHA-256 of the passcode; the passcode itself is never in the repo.
    // To change it: ask Claude, or run in any browser console:
    //   crypto.subtle.digest("SHA-256", new TextEncoder().encode("NEW-PASSCODE")).then(b=>console.log([...new Uint8Array(b)].map(x=>x.toString(16).padStart(2,"0")).join("")))
    GATE_SHA256: "d1535e1637bf202923fa3be45b53f49bb9423195425dd4e893dbef689e75d73e",
    SHARED_NOTES_URL: "https://script.google.com/macros/s/AKfycbzpEwPb5I4hEjA5YhtzfpcJSVRlhDJvQv5kAeV24KDJ9X9K5mA7QHkq1HJ-tYa_d2igUw/exec",
};
