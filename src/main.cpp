#include <Arduino.h>
#include <HardwareSerial.h>
#include <TinyGPS++.h>
#include <Adafruit_GFX.h>
#include <Adafruit_ST7735.h>
#include <SPI.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <ArduinoOTA.h>
#include "secrets.h"
#include <WiFiClientSecure.h>
#include <HTTPClient.h>

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

#include <Preferences.h>

// Declare a mutex for screen access
SemaphoreHandle_t screenMutex;
Preferences preferences;

// GPS module pins
#define GNSS_TX 33
#define GNSS_RX 34
#define V_EXT 3
#define GPSBaud 115200

// TFT display pins
#define TFT_MOSI 42
#define TFT_SCLK 41
#define TFT_DC 40
#define TFT_RST 39
#define TFT_CS 38
#define TFT_BL 21

#define BAT_READ_PIN 1
#define ADC_CTR_PIN 2
#define lower_threshold_battery 3.3
#define GPS_HDOP_THRESHOLD 300    // Accept fixes with HDOP <= 3.0 (TinyGPS++ centesimal: 300 = 3.0)
#define WIFI_GEO_INTERVAL  30000  // Min ms between WiFi geolocation attempts
// Wi-Fi credentials
const char *ssid = WIFI_SSID;
const char *password = WIFI_PASSWORD;

// MQTT broker details
const char *mqtt_server = "mqtt.iot-lab.utwente.nl";
const int mqtt_port = 1883;
const char *mqtt_user = MQTT_USER;
const char *mqtt_password = MQTT_PASSWORD;

const unsigned long rebootTimeoutWiFi = 120000; // 2 minute in milliseconds

bool ValidFix = false;
bool ValidTime = false;
unsigned long lastWifiGeoAttempt = 0;
String g_locationSource = "";    // last published location source: "gps", "wifi", or "cached"
String g_locationAccuracy = "";  // last published accuracy string
String lastLat;
String lastLng;

const unsigned long rebootTimeout = 1200000; // 20 minutes in milliseconds
unsigned long startupTime = 0;

// ─── Fixed network constants ───────────────────────────────────────────────────
#ifndef EAP_SSID
#define EAP_SSID     "eduroam"
#endif
#ifndef EAP_IDENTITY
#define EAP_IDENTITY EAP_USERNAME
#endif
#ifndef MQTT_TOPIC
#define MQTT_TOPIC   "trackers/" MQTT_USER "/gps"
#endif
#ifndef MQTT_UPDATE_FREQUENCY
#define MQTT_UPDATE_FREQUENCY 5000
#endif

const char *mqtt_topic = MQTT_TOPIC;

WiFiClient espClient;
PubSubClient client(espClient);
TinyGPSPlus gps;
Adafruit_ST7735 tft = Adafruit_ST7735(TFT_CS, TFT_DC, TFT_MOSI, TFT_SCLK, TFT_RST);

String IP = "999.999.999.999";
bool first_valid_fix = false;
unsigned long lastDisplayUpdate = 0;
const unsigned long displayInterval = 1000; // update every 1 second

unsigned long lastmqttUpdate = 0;
const unsigned long mqttInterval = MQTT_UPDATE_FREQUENCY;

void WriteStringScreen(const char *text, int line, int textSize = 1);
void startOTA();
void setup_wifi();
void setup_wifi_eduroam();

void reconnect();

float getBatteryCapacity(float voltage)
{
  // Voltage and capacity breakpoints from the approximate Li-ion curve
  const int n = 21; // Number of data points

  // Voltage levels in descending order
  const float voltages[n] = {
    4.28, 4.14, 4.06, 4.02, 3.96, 3.89, 3.84, 3.80, 3.73, 3.71,
    3.67, 3.64, 3.61, 3.61, 3.59, 3.59, 3.54, 3.52, 3.49, 3.41, 3.30
};

const float capacities[n] = {
    100.0, 95.0, 90.0, 85.0, 80.0, 75.0, 70.0, 65.0, 60.0, 55.0,
    50.0, 45.0, 40.0, 35.0, 30.0, 25.0, 20.0, 15.0, 10.0, 5.0, 0.0
};

  // If the voltage is at or above the highest value, return 100%
  if (voltage >= voltages[0])
  {
    return 100.0;
  }
  // If below the lowest value, return 0%
  if (voltage <= voltages[n - 1])
  {
    return -10.0;
  }

  // Find the interval in which the voltage falls and linearly interpolate
  for (int i = 0; i < n - 1; i++)
  {
    if (voltage <= voltages[i] && voltage > voltages[i + 1])
    {
      float fraction = (voltage - voltages[i]) / (voltages[i + 1] - voltages[i]);
      return capacities[i] + fraction * (capacities[i + 1] - capacities[i]);
    }
  }
  return 0.0; // Fallback (should not occur)
}

// Query Google Geolocation API using nearby WiFi APs.
// Returns true and fills lat/lng/accuracy on success.
bool getWifiGeolocation(String &lat, String &lng, String &accuracy)
{
  int numNetworks = WiFi.scanNetworks(false, true); // blocking, show hidden
  if (numNetworks <= 0)
  {
    WiFi.scanDelete();
    return false;
  }

  // Build request body – limit to 10 strongest APs to keep payload small
  JsonDocument reqDoc;
  JsonArray wifiArr = reqDoc["wifiAccessPoints"].to<JsonArray>();
  int limit = min(numNetworks, 10);
  for (int i = 0; i < limit; i++)
  {
    JsonObject ap = wifiArr.add<JsonObject>();
    ap["macAddress"] = WiFi.BSSIDstr(i);
    ap["signalStrength"] = WiFi.RSSI(i);
  }
  WiFi.scanDelete();

  char reqBuffer[512];
  serializeJson(reqDoc, reqBuffer);

  WiFiClientSecure secureClient;
  secureClient.setInsecure(); // skip cert verification (acceptable for research context)
  HTTPClient http;

  String url = "https://www.googleapis.com/geolocation/v1/geolocate?key=";
  url += GOOGLE_GEO_API_KEY;

  if (!http.begin(secureClient, url))
  {
    return false;
  }
  http.addHeader("Content-Type", "application/json");
  int httpCode = http.POST(reqBuffer);

  if (httpCode != 200)
  {
    Serial.printf("WiFi geo API returned %d\n", httpCode);
    http.end();
    return false;
  }

  String response = http.getString();
  http.end();

  JsonDocument respDoc;
  DeserializationError err = deserializeJson(respDoc, response);
  if (err || !respDoc["location"]["lat"].is<float>())
  {
    Serial.println("WiFi geo: JSON parse error");
    return false;
  }

  lat = String(respDoc["location"]["lat"].as<float>(), 4);
  lng = String(respDoc["location"]["lng"].as<float>(), 4);
  accuracy = String(respDoc["accuracy"].as<float>(), 0) + "m";
  return true;
}

void mqttTask(void *parameter)
{
  while (true)
  {
    if (!client.connected())
    {
      reconnect();
    }
    client.loop();
    vTaskDelay(1000 / portTICK_PERIOD_MS); // Run every second
  }
}

void GPSTask(void *parameter)
{
  while (true)
  {
    while (Serial1.available() > 0)
    {
      char c = Serial1.read();
      gps.encode(c);
    }
    unsigned long currentMillis = millis();
    if (currentMillis - lastDisplayUpdate >= displayInterval)
    {
      lastDisplayUpdate = currentMillis;

      // Update display and publish data if new GPS data is received
      if (gps.time.isUpdated() || gps.satellites.isUpdated())
      {

        // Retrieve time information — GPS time is always UTC, no timezone offset applied
        String gps_time = String(gps.time.hour()) + ":" +
                          String(gps.time.minute()) + ":" +
                          String(gps.time.second());

        // Retrieve satellite count
        String gps_sat = String(gps.satellites.value());

        // Variables to hold latitude and longitude
        String lat_str;
        String lng_str;
        bool valid_fix = gps.location.isValid() &&
                         gps.hdop.isValid() &&
                         gps.hdop.value() <= GPS_HDOP_THRESHOLD &&
                         gps.location.age() < 2000;
        String Fix_quality = String(gps.hdop.value() / 100.0, 1); // e.g. "2.5"

        if (gps.time.isValid())
        {
          ValidTime = true;
        }

        if (valid_fix && !first_valid_fix)
        {
          // Retrieve latitude and longitude
          double lat = gps.location.lat();
          double lng = gps.location.lng();
          lat_str = String(lat, 4);
          lng_str = String(lng, 4);
          preferences.putString("latitude", lat_str);
          preferences.putString("longitude", lng_str);
          first_valid_fix = true;
          ValidFix = true;
        }
        else if (valid_fix && first_valid_fix)
        {
          double lat = gps.location.lat();
          double lng = gps.location.lng();
          lat_str = String(lat, 4);
          lng_str = String(lng, 4);
          ValidFix = true;
        }
        else
        {
          // Use placeholders for invalid fix
          lat_str = "N/A";
          lng_str = "N/A";
        }

        if ((currentMillis - startupTime) > rebootTimeout)
        {
          if (((ValidFix == false) || (ValidTime == false)) &&
              (g_locationSource != "wifi"))
          {
            Serial.println("Rebooting due to timeout... validFix");
            ESP.restart();
          }
        }

        // read bat voltage
        float adc_value = analogRead(BAT_READ_PIN);
        float v_adc = (adc_value / 4095.0) * 3.65;
        float v_bat = v_adc * 4.9; // Adjust for the voltage divider

        float capacity = getBatteryCapacity(v_bat);
        String voltage_string = String(v_bat, 2);
        String capacity_string = String(capacity, 2);

        float MCU_Temp = temperatureRead();
        String MCU_Temp_string = String(MCU_Temp, 2);

        String mqtt_topic_string = mqtt_topic;
        // Display time, satellite count, latitude, longitude, and fix quality
        WriteStringScreen(("Time: " + gps_time + " UTC").c_str(), 1);
        WriteStringScreen(("Sat: " + gps_sat + " Temp: " + MCU_Temp_string + "C").c_str(), 2);
        WriteStringScreen(("Lat: " + lat_str + " - Long: " + lng_str).c_str(), 3);
        // WriteStringScreen(("Long: " + lng_str).c_str(), 3);
        String fixLine;
        if      (g_locationSource == "gps")    fixLine = "GPS hdop:" + Fix_quality;
        else if (g_locationSource == "wifi")   fixLine = "WiFi " + g_locationAccuracy;
        else if (g_locationSource == "cached") fixLine = "Cached";
        else                                   fixLine = "No fix";
        WriteStringScreen(fixLine.c_str(), 4);
        WriteStringScreen(("IP: " + IP).c_str(), 5);
        if (capacity < 0)
        {
          WriteStringScreen("Charge battery", 6);
        }
        else
        {
          WriteStringScreen(("Battery: " + voltage_string + "V or " + capacity_string + "%").c_str(), 6);
        }
      
        WriteStringScreen(("MQTT: " + mqtt_topic_string).c_str(), 7);

        // Print data to Serial Monitor
        Serial.print("Time: ");
        Serial.println(gps_time);
        Serial.print("Sat: ");
        Serial.println(gps_sat);
        Serial.print("Lat: ");
        Serial.println(lat_str);
        Serial.print("Long: ");
        Serial.println(lng_str);
        Serial.print("Fix_Quality: ");
        Serial.println(Fix_quality);
        Serial.print("Location: ");
        Serial.println(fixLine);
        Serial.print("Ip: ");
        Serial.println(IP);
        Serial.print("Battery Voltage: ");
        Serial.println(voltage_string);
        Serial.print("Battery Capacity: ");
        Serial.println(capacity_string);
        Serial.print("MQTT topic: ");
        Serial.println(mqtt_topic_string);
        Serial.print("MCU Temp: ");
        Serial.println(MCU_Temp_string);
        Serial.println("=======================");

        // Prepare JSON document and publish to MQTT
        unsigned long currentMillis = millis();
        if (currentMillis - lastmqttUpdate >= mqttInterval)
        {
          lastmqttUpdate = currentMillis;

          // Determine coordinates and location source for this publish
          String pub_lat = lat_str;
          String pub_lng = lng_str;
          String locationSource;
          String locationAccuracy;

          if (valid_fix)
          {
            locationSource = "gps";
            locationAccuracy = Fix_quality; // HDOP value
          }
          else
          {
            // Try WiFi geolocation if key is configured, MQTT is up, and enough time has passed
            bool wifiGeoKeySet = strcmp(GOOGLE_GEO_API_KEY, "YOUR_API_KEY_HERE") != 0;
            if (wifiGeoKeySet && client.connected() && (currentMillis - lastWifiGeoAttempt >= WIFI_GEO_INTERVAL))
            {
              lastWifiGeoAttempt = currentMillis;
              String wLat, wLng, wAcc;
              if (getWifiGeolocation(wLat, wLng, wAcc))
              {
                pub_lat = wLat;
                pub_lng = wLng;
                locationSource = "wifi";
                locationAccuracy = wAcc;
                Serial.println("WiFi geo: " + wLat + ", " + wLng + " acc " + wAcc);
              }
              else
              {
                pub_lat = lastLat;
                pub_lng = lastLng;
                locationSource = "cached";
              }
            }
            else
            {
              pub_lat = lastLat;
              pub_lng = lastLng;
              locationSource = "cached";
            }
          }

          g_locationSource = locationSource;
          g_locationAccuracy = locationAccuracy;

          JsonDocument doc = JsonDocument();
          doc["time"] = gps_time; // UTC — GPS always reports UTC, no timezone offset applied
          doc["satellites"] = gps_sat;
          doc["valid_fix"] = valid_fix;
          doc["latitude"] = pub_lat;
          doc["longitude"] = pub_lng;
          doc["fix_quality"] = Fix_quality;
          doc["IP"] = IP;
          doc["battery_voltage"] = voltage_string;
          doc["battery_capacity"] = capacity_string;
          doc["mqtt_topic"] = mqtt_topic_string;
          doc["MCU_Temp"] = MCU_Temp_string;
          doc["first_valid_fix"] = first_valid_fix;
          doc["location_source"] = locationSource;
          if (locationAccuracy.length() > 0)
          {
            doc["location_accuracy"] = locationAccuracy;
          }

          // Serialize JSON to buffer
          char buffer[1024];
          size_t n = serializeJson(doc, buffer);

          Serial.print("Serialized JSON size: ");
          Serial.println(n);

          if (n > 1024)
          {
            Serial.println("Error: JSON payload exceeds buffer size!");
          }

          // Publish JSON to MQTT
          if (client.publish(mqtt_topic, buffer, n))
          {
            Serial.println("MQTT message published successfully.");
          }
          else
          {
            Serial.println("Error: Failed to publish MQTT message.");
          }
        }
      }
    }
    vTaskDelay(100 / portTICK_PERIOD_MS);
  }
}
void mqttCallback(char *topic, byte *payload, unsigned int length)
{
  String message;
  for (unsigned int i = 0; i < length; i++)
  {
    message += (char)payload[i];
  }

  Serial.print("Message received on topic: ");
  Serial.println(topic);
  Serial.print("Message: ");
  Serial.println(message);
  String resetTopic = String(mqtt_topic) + "/reset";
  // Check if the message is a reset command
  if (String(topic) == resetTopic && message == "RESET")
  {
    Serial.println("Reset command received. Restarting...");
    delay(1000);   // Optional delay before reset
    ESP.restart(); // Reset the controller
  }
}

void setup()
{
  Serial.begin(115200);
  screenMutex = xSemaphoreCreateMutex();
  analogSetAttenuation(ADC_11db);

  preferences.begin("gps-data", false); // Namespace: "gps-data", read/write mode
  lastLat = preferences.getString("latitude", "52.2391");
  lastLng = preferences.getString("longitude", "6.8570");

  // Power external rail before anything that depends on it (TFT, GPS)
  pinMode(V_EXT, OUTPUT);
  digitalWrite(V_EXT, HIGH);
  delay(100); // let rail stabilise

  // Initialize TFT display
  pinMode(TFT_BL, OUTPUT);
  digitalWrite(TFT_BL, HIGH);
  tft.initR(INITR_MINI160x80_PLUGIN);
  tft.setRotation(1);
  tft.fillScreen(ST7735_BLACK);
  WriteStringScreen("GPS Test", 0);

  // Initialize GPS module

  pinMode(ADC_CTR_PIN, OUTPUT);
  digitalWrite(ADC_CTR_PIN, HIGH);

  Serial1.begin(GPSBaud, SERIAL_8N1, GNSS_TX, GNSS_RX);

  // UC6580: enable GPS + GLONASS + BeiDou + Galileo + QZSS (mask=31)
  // Checksums verified via NMEA XOR. Module ignores unknown commands safely.
  delay(500); // allow module to boot before sending config
  Serial1.println("$PQTMCFGCNST,W,31*05");
  delay(100);
  Serial1.println("$PQTMSAVEPAR*5A");
  delay(100);

  setup_wifi();
  IP = WiFi.localIP().toString();
  startOTA();
  client.setCallback(mqttCallback);
  client.setServer(mqtt_server, mqtt_port);

  xTaskCreate(mqttTask, "MQTT", 4096, NULL, 1, NULL);
  xTaskCreate(GPSTask, "GPS", 16384, NULL, 1, NULL); // 16 KB: extra headroom for WiFi scan + HTTPS
  startupTime = millis();
}

void loop()
{
  ArduinoOTA.handle();
}

void WriteStringScreen(const char *text, int line, int textSize)
{
  // Take the mutex before accessing the screen
  if (xSemaphoreTake(screenMutex, portMAX_DELAY) == pdTRUE)
  {
    // Clear the line by drawing a black rectangle
    int lineHeight = textSize * 10; // Adjust line height based on text size
    tft.fillRect(0, line * lineHeight, tft.width(), lineHeight, ST7735_BLACK);

    // Write the new text
    tft.setCursor(0, line * lineHeight);
    tft.setTextColor(ST7735_BLUE);
    tft.setTextSize(textSize);
    tft.println(text);

    // Release the mutex after updating the screen
    xSemaphoreGive(screenMutex);
  }
}

void startOTA()
{
  ArduinoOTA
      .onStart([]()
               {
      String type;
      if (ArduinoOTA.getCommand() == U_FLASH) {
        type = "sketch";
      } else {  // U_SPIFFS
        type = "filesystem";
      }

      // NOTE: if updating SPIFFS this would be the place to unmount SPIFFS using SPIFFS.end()
      Serial.println("Start updating " + type); })
      .onEnd([]()
             { Serial.println("\nEnd"); })
      .onProgress([](unsigned int progress, unsigned int total)
                  { Serial.printf("Progress: %u%%\r", (progress / (total / 100))); })
      .onError([](ota_error_t error)
               {
      Serial.printf("Error[%u]: ", error);
      if (error == OTA_AUTH_ERROR) {
        Serial.println("Auth Failed");
      } else if (error == OTA_BEGIN_ERROR) {
        Serial.println("Begin Failed");
      } else if (error == OTA_CONNECT_ERROR) {
        Serial.println("Connect Failed");
      } else if (error == OTA_RECEIVE_ERROR) {
        Serial.println("Receive Failed");
      } else if (error == OTA_END_ERROR) {
        Serial.println("End Failed");
      } });

  ArduinoOTA.setPassword(OTA_PASSWORD);
  ArduinoOTA.begin();
}

void setup_wifi()
{
  delay(10);
  // We start by connecting to a WiFi network
  Serial.println();
  Serial.print("Connecting to ");
  WiFi.mode(WIFI_STA);
  Serial.println(WIFI_SSID);
  WiFi.setHostname("Lab_Sign");

  if (EDUROAM)
  {
    WiFi.begin(EAP_SSID, WPA2_AUTH_PEAP, EAP_IDENTITY, EAP_USERNAME, EAP_PASSWORD);
  }
  else
  {
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  }

  WriteStringScreen("WiFi: Connecting...", 0);

  unsigned long startAttemptTime = millis();
  while (WiFi.status() != WL_CONNECTED)
  {
    delay(500);
    Serial.print(".");
    if ((millis() - startAttemptTime) > rebootTimeoutWiFi)
    {
      Serial.println("WiFi connection timeout. Rebooting...");
      ESP.restart();
    }
  }

  Serial.println("");
  Serial.println("WiFi connected");
  Serial.println("IP address: ");
  Serial.println(WiFi.localIP());

  WriteStringScreen("WiFi: Connected", 0);
}

void reconnect()
{
  // Loop until we're reconnected
  unsigned long startAttemptTime = millis();
  while (!client.connected())
  {
    Serial.print("Attempting MQTT connection...");
    WriteStringScreen("MQTT: Connecting...", 0);
    if ((millis() - startAttemptTime) > rebootTimeoutWiFi)
    {
      Serial.println("WiFi connection timeout. Rebooting...");
      ESP.restart();
    }
    // Attempt to connect
    if (client.connect((String(MQTT_USER) + "_gps").c_str(), MQTT_USER, MQTT_PASSWORD))
    {
      Serial.println("connected");
      WriteStringScreen("MQTT: Connected", 0);
      // Subscribe
      client.subscribe((String(mqtt_topic) + "/reset").c_str());

      String IP_Topic = String(mqtt_topic) + "/ip";
      // String IP = String( WiFi.localIP());
      JsonDocument doc = JsonDocument();
      doc["IP"] = IP;

      char buffer[512];
      size_t n = serializeJson(doc, buffer);

      // Publish JSON to MQTT
      client.publish(mqtt_topic, buffer, n);
    }
    else
    {
      Serial.print("failed, rc=");
      Serial.print(client.state());
      Serial.println(" try again in 5 seconds");
      WriteStringScreen("MQTT: Failed", 1);
      // Wait 5 seconds before retrying
      delay(5000);
    }
  }
}