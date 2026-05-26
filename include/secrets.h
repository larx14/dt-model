// Copy this file to secrets.h and fill in your credentials.
// secrets.h is in .gitignore — never commit it.

// ── MQTT (assigned by lab staff) ─────────────────────────────────────────────
#define MQTT_USER     "HTIT_51"              // your device ID, e.g. "HTIT_3"
#define MQTT_PASSWORD "cubG38j0Lraj"

// ── WiFi ─────────────────────────────────────────────────────────────────────
#define EDUROAM true                        // set false to use regular WiFi below

// Eduroam (UTwente)
#define EAP_USERNAME  "l.a.pinto@student.utwente.nl" // your UTwente email
#define EAP_PASSWORD  "Gatswick77!934"

// Regular WPA2 — only needed if EDUROAM is false
#define WIFI_SSID     "your_network_name"
#define WIFI_PASSWORD "xxxxxxxxxx"

// ── OTA ───────────────────────────────────────────────────────────────────────
// Also set --auth in platformio.ini [env:ArduinoOTA] upload_flags to match.
#define OTA_PASSWORD  "tqAB183"

// ── Optional ─────────────────────────────────────────────────────────────────
// Google Geolocation API for WiFi-based location when GPS has no fix.
// Enable "Geolocation API" in Google Cloud Console — free tier: 40k req/month.
#define GOOGLE_GEO_API_KEY "YOUR_API_KEY_HERE"
