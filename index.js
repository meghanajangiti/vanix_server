const express = require("express");
const mqtt = require("mqtt");
const Datastore = require("nedb-promises");
const path = require("path");
const multer = require("multer");
const fs = require("fs");
const crypto = require("crypto");
const { GoogleGenAI, Type } = require("@google/genai");

// ─────────────────────────────────────────────────────────
// 1. CONFIGURATION & SERVER INITIALIZATION
// ─────────────────────────────────────────────────────────
const app = express();
const PORT = process.env.PORT || 3000;

const UPLOAD_DIR = path.join(__dirname, "uploads");
if (!fs.existsSync(UPLOAD_DIR)) {
  fs.mkdirSync(UPLOAD_DIR, { recursive: true });
}

const upload = multer({
  dest: UPLOAD_DIR,
  limits: { fileSize: 10 * 1024 * 1024 },
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ─────────────────────────────────────────────────────────
// 2. GEMINI AI CLIENT
// ─────────────────────────────────────────────────────────
const apiKey = process.env.GEMINI_API_KEY;
if (!apiKey) {
  console.warn("⚠️ [WARNING] GEMINI_API_KEY environment variable is not set!");
}
const ai = new GoogleGenAI({ apiKey: apiKey || "placeholder_key" });

function getGeminiStatus(err) {
  const status = Number(err?.status || err?.code || err?.response?.status || 0);
  if (status) return status;

  const message = String(err?.message || "").toLowerCase();
  if (message.includes("429") || message.includes("resource exhausted") || message.includes("rate limit")) return 429;
  if (message.includes("503") || message.includes("unavailable") || message.includes("high demand")) return 503;
  return 0;
}

function isRetryableGeminiError(err) {
  const status = getGeminiStatus(err);
  return status === 429 || status === 503;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function geminiErrorMessage(err) {
  const status = getGeminiStatus(err);
  const raw = String(err?.message || err || "Unknown Gemini error");

  if (status === 429) {
    return "Gemini rate limit reached. Please try again in a moment.";
  }
  if (status === 503) {
    return "Gemini is temporarily unavailable due to high demand. Please try again in a moment.";
  }
  if (status === 400 || status === 401 || status === 403) {
    return "Gemini API authentication or request configuration is invalid. Check GEMINI_API_KEY and the Gemini API configuration.";
  }
  return raw.slice(0, 500);
}

async function generateGeminiContent({ contents, config, operation = "AI request" }) {
  if (!apiKey) {
    const err = new Error("GEMINI_API_KEY is not configured.");
    err.status = 500;
    throw err;
  }

  const models = [GEMINI_MODEL];
  if (GEMINI_FALLBACK_MODEL && GEMINI_FALLBACK_MODEL !== GEMINI_MODEL) {
    models.push(GEMINI_FALLBACK_MODEL);
  }

  let lastError = null;

  for (let modelIndex = 0; modelIndex < models.length; modelIndex++) {
    const model = models[modelIndex];
    const attempts = GEMINI_MAX_RETRIES + 1;

    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        console.log(`[GEMINI] ${operation} | model=${model} | attempt=${attempt}/${attempts}`);

        const response = await ai.models.generateContent({
          model,
          contents,
          config,
        });

        console.log(`[GEMINI] ${operation} | success | model=${model}`);
        return response;
      } catch (err) {
        lastError = err;
        const status = getGeminiStatus(err);
        const retryable = isRetryableGeminiError(err);
        const finalAttempt = attempt >= attempts;

        console.warn(
          `[GEMINI] ${operation} failed | model=${model} | status=${status || "unknown"} | attempt=${attempt}/${attempts} | retryable=${retryable}`
        );

        if (!retryable) {
          throw err;
        }

        if (!finalAttempt) {
          const exponential = GEMINI_BASE_RETRY_MS * Math.pow(2, attempt - 1);
          const jitter = Math.floor(Math.random() * 300);
          const delay = exponential + jitter;
          console.log(`[GEMINI] Retrying ${operation} in ${delay}ms...`);
          await sleep(delay);
        }
      }
    }

    if (modelIndex < models.length - 1 && isRetryableGeminiError(lastError)) {
      console.warn(
        `[GEMINI] Primary model ${model} unavailable after retries. Falling back to ${models[modelIndex + 1]}.`
      );
    }
  }

  const wrapped = new Error(geminiErrorMessage(lastError));
  wrapped.status = getGeminiStatus(lastError) || 503;
  wrapped.cause = lastError;
  wrapped.isGeminiTransient = true;
  throw wrapped;
}

// ─────────────────────────────────────────────────────────
// 3. DATABASE SETUP
// ─────────────────────────────────────────────────────────
const db = Datastore.create({
  filename: path.join(__dirname, "appointments.db"),
  autoload: true,
});

const sessionDb = Datastore.create({
  filename: path.join(__dirname, "visitor_sessions.db"),
  autoload: true,
});

const eventDb = Datastore.create({
  filename: path.join(__dirname, "kiosk_events.db"),
  autoload: true,
});

const KIOSK_ID = process.env.KIOSK_ID || "device1";
const FRONTDESK_NUMBER = process.env.FRONTDESK_NUMBER || "6305141921";
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-2.5-flash";
const GEMINI_FALLBACK_MODEL = process.env.GEMINI_FALLBACK_MODEL || "gemini-2.5-flash-lite";
const GEMINI_MAX_RETRIES = Math.max(0, Number(process.env.GEMINI_MAX_RETRIES || 3));
const GEMINI_BASE_RETRY_MS = Math.max(250, Number(process.env.GEMINI_BASE_RETRY_MS || 1000));
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || `http://localhost:${PORT}`;

// Multi-device tracking storage: { [deviceId]: { deviceId, online, lastSeen, ... } }
const devices = {};

// ─────────────────────────────────────────────────────────
// 4. MQTT BROKER SETUP (Supports multi-device connection)
// ─────────────────────────────────────────────────────────
const MQTT_BROKER = process.env.MQTT_BROKER || "mqtt://broker.emqx.io:1883";
const MQTT_TOPIC_CMD = "vanix/hardware/device1";
const mqttClient = mqtt.connect(MQTT_BROKER, {
  reconnectPeriod: 3000,
  connectTimeout: 10000,
  keepalive: 30,
});

mqttClient.on("connect", () => {
  console.log(`[MQTT BROKER] Connected to ${MQTT_BROKER}`);
  // Wildcard subscription to accept multiple hardware kiosks/modems simultaneously
  mqttClient.subscribe("vanix/hardware/+/status", (err) => {
    if (err) console.error("[MQTT SUBSCRIBE ERROR]", err.message);
    else console.log("[MQTT] Subscribed to wildcard status channel: vanix/hardware/+/status");
  });
  mqttClient.subscribe("vanix/hardware/+/event", (err) => {
    if (err) console.error("[MQTT SUBSCRIBE ERROR]", err.message);
    else console.log("[MQTT] Subscribed to wildcard event channel: vanix/hardware/+/event");
  });
  mqttClient.subscribe("vanix/hardware/device1", (err) => {
    if (err) console.error("[MQTT SUBSCRIBE ERROR]", err.message);
  });
});

mqttClient.on("close", () => {
  console.warn("[MQTT BROKER] Connection closed");
});

mqttClient.on("error", (err) => {
  console.error("[MQTT ERROR]", err.message);
});

mqttClient.on("message", async (topic, payload) => {
  const text = payload.toString().trim();
  const now = new Date().toISOString();
  const parts = topic.split("/");
  // Topic structure: vanix / hardware / <DEVICE_ID> / [status|event]
  const deviceId = parts[2] || KIOSK_ID;
  const channel = parts[3] || "status";

  if (!devices[deviceId]) {
    devices[deviceId] = {
      deviceId,
      online: true,
      lastSeen: now,
      lastPayload: text,
    };
  }

  devices[deviceId].lastSeen = now;
  devices[deviceId].lastPayload = text;
  devices[deviceId].online = true;

  if (channel === "status") {
    try {
      const status = JSON.parse(text);
      devices[deviceId] = {
        ...devices[deviceId],
        ...status,
        deviceId: status.deviceId || deviceId,
        online: true,
        lastSeen: now,
      };
    } catch (err) {
      devices[deviceId].rawStatus = text;
    }
    return;
  }

  if (channel === "event") {
    devices[deviceId].lastEvent = text;

    let event;
    try {
      event = JSON.parse(text);
    } catch (_) {
      event = {
        type: "RAW",
        deviceId: deviceId,
        data: { message: text },
        timestamp: now,
      };
    }

    try {
      await eventDb.insert({
        kioskId: deviceId,
        ...event,
        rawPayload: text,
        createdAt: new Date(),
      });
    } catch (err) {
      console.error("[EVENT DB ERROR]", err.message);
    }

    if (event.type === "SOS") {
      const data = event.data || {};
      const reason = cleanText(data.reason || "Emergency assistance requested");
      const source = cleanText(data.source || "EC200");

      publishSms(
        FRONTDESK_NUMBER,
        `VANIX EMERGENCY: Device ${deviceId}. Source: ${source}. ${reason}`
      );

      devices[deviceId].lastEvent = `SOS: ${reason}`;
    }
    return;
  }
});

const safePublish = (topic, message) => {
  if (mqttClient.connected) {
    mqttClient.publish(topic, message, (err) => {
      if (err) console.error(`[MQTT ERROR] ${err.message}`);
    });
  } else {
    console.warn("[MQTT DISCONNECTED] Skipped message:", message);
  }
};

// ─────────────────────────────────────────────────────────
// SMART KIOSK HELPERS
// ─────────────────────────────────────────────────────────
const makeToken = () => crypto.randomBytes(18).toString("hex");

const publishKiosk = (payload) => {
  safePublish(MQTT_TOPIC_CMD, payload);
};

const publishSms = (phone, message) => {
  const digits = String(phone || "").replace(/\D/g, "");

  if (!/^\d{10,15}$/.test(digits)) {
    console.warn("[SMS] Invalid destination:", phone);
    return false;
  }

  const body = cleanText(message).slice(0, 450);
  safePublish(MQTT_TOPIC_CMD, `SMS:${digits}:${body}`);
  return true;
};

const createVisitorSession = async (data) => {
  const token = makeToken();
  await sessionDb.insert({
    token,
    data,
    createdAt: new Date(),
    expiresAt: new Date(Date.now() + 15 * 60 * 1000),
  });
  return token;
};

const getVisitorSession = async (token) => {
  const session = await sessionDb.findOne({ token });
  if (!session) return null;
  if (new Date(session.expiresAt).getTime() < Date.now()) return null;
  return session;
};

const normalizeUrgency = (value) => {
  const allowed = ["Low", "Medium", "High", "Emergency"];
  return allowed.includes(value) ? value : "Low";
};

// ─────────────────────────────────────────────────────────
// 5. MULTI-CATEGORY CATALOGS
// ─────────────────────────────────────────────────────────
const CATALOGS = {
  hospital: {
    key: "hospital",
    icon: "🏥",
    title: "Hospital Kiosk",
    bookingEnabled: true,
    hasCompanies: false,
    itemLabel: "Doctor / Specialty",
    inputLabel: "Describe your symptoms",
    inputPlaceholder:
      "e.g. I've had chest tightness and shortness of breath since this morning...",
    bookButtonLabel: "Book Appointment",
    aiButtonLabel: "🤖 Get AI Recommendation",
    aiRole:
      "a hospital front-desk triage assistant, not a diagnosing physician",
    aiInstruction:
      "Pick the single most appropriate specialist for the patient's symptoms and classify urgency. " +
      'Use "Emergency" only for symptoms suggesting an immediate life-threatening condition ' +
      "(severe chest pain, difficulty breathing, stroke signs, uncontrolled bleeding, loss of consciousness).",
    items: [
      { id: 1, name: "🩺 General Physician", location: "Room 101" },
      { id: 2, name: "❤️ Cardiologist", location: "Room 102" },
      { id: 3, name: "🧴 Dermatologist", location: "Room 103" },
      { id: 4, name: "👶 Pediatrician", location: "Room 104" },
      { id: 5, name: "👩‍⚕️ Gynecologist", location: "Room 105" },
      { id: 6, name: "🦴 Orthopedic", location: "Room 106" },
      { id: 7, name: "🧠 Neurologist", location: "Room 107" },
      { id: 8, name: "👁️ Ophthalmologist", location: "Room 108" },
      { id: 9, name: "👂 ENT Specialist", location: "Room 109" },
      { id: 10, name: "🫁 Pulmonologist", location: "Room 110" },
      { id: 11, name: "🩻 Gastroenterologist", location: "Room 111" },
      { id: 12, name: "🧠 Psychiatrist", location: "Room 112" },
    ],
  },
  mall: {
    key: "mall",
    icon: "🛍️",
    title: "Mall Directory Kiosk",
    bookingEnabled: false,
    hasCompanies: false,
    itemLabel: "Store / Facility",
    inputLabel: "What are you looking for?",
    inputPlaceholder:
      "e.g. I need running shoes and then a coffee before the movie...",
    aiButtonLabel: "🤖 Find It For Me",
    aiRole: "a friendly mall information and wayfinding assistant",
    aiInstruction:
      "Recommend the single best matching store or facility for what the visitor wants, " +
      'and briefly explain why. Use "Emergency" urgency only for things like a lost child, ' +
      "medical emergency, or security issue — otherwise use Low or Medium.",
    items: [
      { id: 1, name: "👟 Nike", location: "2nd Floor, Unit 210" },
      { id: 2, name: "🍔 Food Court", location: "3rd Floor" },
      { id: 3, name: "📱 Croma Electronics", location: "1st Floor, Unit 105" },
      { id: 4, name: "👗 Zara", location: "Ground Floor, Unit 12" },
      { id: 5, name: "🎬 PVR Cinemas", location: "4th Floor" },
      { id: 6, name: "💊 Apollo Pharmacy", location: "Ground Floor, Unit 3" },
      { id: 7, name: "☕ Starbucks", location: "1st Floor, Unit 118" },
      { id: 8, name: "🧸 Hamleys Toy Store", location: "2nd Floor, Unit 225" },
      {
        id: 9,
        name: "🏦 ATM / Bank Kiosk",
        location: "Ground Floor, near Entrance A",
      },
      {
        id: 10,
        name: "🅿️ Parking / Security Help Desk",
        location: "Basement 1",
      },
    ],
  },
  company: {
    key: "company",
    icon: "🏢",
    title: "Company Reception Kiosk",
    bookingEnabled: true,
    hasCompanies: true,
    itemLabel: "Department / Contact",
    inputLabel: "Reason for your visit",
    inputPlaceholder:
      "e.g. I'm here to follow up on a vendor invoice that hasn't been paid...",
    bookButtonLabel: "Check In / Book Meeting",
    aiButtonLabel: "🤖 Route My Visit",
    aiRole: "a corporate reception and visitor-routing assistant",
    aiInstruction:
      "Recommend the single best department or contact at this company for the visitor's stated " +
      "reason for visiting, and briefly explain why. Use Medium/High urgency for time-sensitive " +
      "business matters, Emergency only for genuine safety/security incidents.",
    companies: [
      {
        id: 1,
        name: "TechNova Solutions",
        departments: [
          { id: 1, name: "👔 HR Department", location: "3rd Floor, Room 301" },
          {
            id: 2,
            name: "💰 Finance & Accounts",
            location: "2nd Floor, Room 205",
          },
          { id: 3, name: "💻 IT Support", location: "1st Floor, Room 110" },
          {
            id: 4,
            name: "📈 Sales & Business Development",
            location: "4th Floor, Room 402",
          },
          { id: 5, name: "🎯 Marketing", location: "4th Floor, Room 410" },
          {
            id: 6,
            name: "👥 Reception / General Enquiry",
            location: "Ground Floor Lobby",
          },
        ],
      },
      {
        id: 2,
        name: "Skyline Industries",
        departments: [
          { id: 1, name: "👔 HR Department", location: "2nd Floor, Room 220" },
          {
            id: 2,
            name: "💰 Finance & Accounts",
            location: "2nd Floor, Room 230",
          },
          {
            id: 3,
            name: "🏭 Operations & Procurement",
            location: "1st Floor, Room 115",
          },
          {
            id: 4,
            name: "📈 Sales & Client Relations",
            location: "3rd Floor, Room 305",
          },
          {
            id: 5,
            name: "⚖️ Legal & Compliance",
            location: "3rd Floor, Room 315",
          },
          {
            id: 6,
            name: "👥 Reception / General Enquiry",
            location: "Ground Floor Lobby",
          },
        ],
      },
      {
        id: 3,
        name: "Bluewave Enterprises",
        departments: [
          { id: 1, name: "👔 HR Department", location: "5th Floor, Room 501" },
          {
            id: 2,
            name: "💰 Finance & Accounts",
            location: "5th Floor, Room 510",
          },
          { id: 3, name: "💻 IT Support", location: "2nd Floor, Room 208" },
          {
            id: 4,
            name: "📈 Sales & Business Development",
            location: "6th Floor, Room 602",
          },
          {
            id: 5,
            name: "🛠️ Facilities / Admin",
            location: "Ground Floor, Room 5",
          },
          {
            id: 6,
            name: "👥 Reception / General Enquiry",
            location: "Ground Floor Lobby",
          },
        ],
      },
    ],
  },
};

const getCatalog = (key) => CATALOGS[key] || CATALOGS.hospital;

const resolveItems = (catKey, companyId) => {
  const cat = getCatalog(catKey);
  if (cat.hasCompanies) {
    const company =
      cat.companies.find((c) => c.id === parseInt(companyId)) ||
      cat.companies[0];
    return {
      items: company.departments,
      companyId: company.id,
      companyName: company.name,
    };
  }
  return { items: cat.items, companyId: null, companyName: null };
};

const findItem = (catKey, itemId, companyId) => {
  const { items } = resolveItems(catKey, companyId);
  return items.find((i) => i.id === parseInt(itemId));
};

const cleanText = (str) =>
  str
    ? String(str)
        .replace(/[^\x00-\x7F]/g, "")
        .trim()
    : "";
const isValidPhone = (phone) => /^[0-9]{10}$/.test(phone);

// ─────────────────────────────────────────────────────────
// 6. FRONTEND WEB INTERFACE
// ─────────────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Multi-Category AI Kiosk</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-main: #090b10;
            --card-bg: rgba(22, 27, 38, 0.75);
            --primary: #4facfe;
            --accent-voice: linear-gradient(135deg, #8A2387, #E94057, #F27121);
            --accent-btn: linear-gradient(135deg, #11998e, #38ef7d);
            --accent-triage: linear-gradient(135deg, #7F00FF, #E100FF);
            --accent-company: linear-gradient(135deg, #FF8008, #FFC837);
            --text-main: #ffffff;
            --text-muted: #a0aec0;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Plus Jakarta Sans', sans-serif; }
        body { background: var(--bg-main); color: var(--text-main); padding: 24px; min-height: 100vh; }
        .container { max-width: 720px; margin: 0 auto; }

        h2 {
            text-align: center;
            margin-bottom: 18px;
            font-size: 26px;
            font-weight: 800;
            background: linear-gradient(135deg, #00f2fe, #4facfe);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .cat-tabs { display: flex; gap: 8px; margin-bottom: 20px; }
        .cat-tab {
            flex: 1; text-align: center; padding: 12px 8px; border-radius: 10px;
            background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.08);
            cursor: pointer; font-weight: 700; font-size: 13px; color: var(--text-muted);
            transition: all 0.15s ease;
        }
        .cat-tab.active { background: var(--card-bg); border-color: var(--primary); color: #fff; }

        .card {
            background: var(--card-bg);
            border: 1px solid rgba(255, 255, 255, 0.08);
            backdrop-filter: blur(12px);
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 20px;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.36);
        }

        .title {
            font-size: 13px;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 1.2px;
            color: var(--primary);
            margin-bottom: 16px;
        }

        .row { display: flex; gap: 12px; }
        input, select, textarea {
            width: 100%;
            padding: 12px 14px;
            margin-bottom: 12px;
            border-radius: 8px;
            border: 1px solid rgba(255,255,255,0.15);
            background: #121722;
            color: #fff;
            font-size: 14px;
            outline: none;
            font-family: inherit;
        }
        textarea { resize: vertical; min-height: 70px; }
        input:focus, select:focus, textarea:focus { border-color: var(--primary); }

        button {
            width: 100%;
            padding: 12px;
            background: var(--accent-btn);
            border: none;
            border-radius: 8px;
            color: #fff;
            font-weight: 700;
            cursor: pointer;
            margin-bottom: 8px;
        }

        button.btn-voice { background: var(--accent-voice); }
        button.btn-recording { background: linear-gradient(135deg, #FF0000, #990000); animation: pulse 1.2s infinite; }
        button.btn-warn { background: linear-gradient(135deg, #ff9966, #ff5e62); }
        button.btn-done { background: linear-gradient(135deg, #00c6ff, #0072ff); }
        button.btn-cancel { background: linear-gradient(135deg, #ed213a, #93291e); }
        button.btn-triage { background: var(--accent-triage); }

        .company-picker { display: none; margin-bottom: 4px; }
        .company-picker .cp-label {
            font-size: 11px; font-weight: 800; text-transform: uppercase;
            letter-spacing: 1px; color: #FFC837; margin-bottom: 10px;
        }
        #companySelect {
            border: 1px solid rgba(255, 200, 55, 0.45);
            background: #1a1608;
            color: #ffdf94;
            font-weight: 700;
        }
        #companySelect:focus { border-color: #FFC837; }

        .prompt-box {
            background: rgba(242, 113, 33, 0.15);
            border: 1px solid #F27121;
            padding: 14px;
            border-radius: 8px;
            font-size: 13px;
            color: #ffab76;
            margin-bottom: 12px;
            display: none;
            line-height: 1.4;
        }

        .triage-result {
            display: none;
            background: rgba(127, 0, 255, 0.12);
            border: 1px solid #B429F9;
            padding: 16px;
            border-radius: 10px;
            margin-bottom: 12px;
        }
        .triage-result .doc-name { font-size: 16px; font-weight: 800; margin-bottom: 4px; }
        .triage-result .doc-location { font-size: 12px; color: var(--text-muted); margin-bottom: 8px; }
        .triage-result .reasoning { font-size: 13px; color: var(--text-muted); line-height: 1.5; margin-bottom: 10px; }
        .urgency-badge { display: inline-block; font-size: 10px; font-weight: 800; text-transform: uppercase; padding: 4px 10px; border-radius: 20px; margin-bottom: 10px; }
        .urgency-Low { background: #11998e; color: #fff; }
        .urgency-Medium { background: #f7b733; color: #1a1a1a; }
        .urgency-High { background: #fc4a1a; color: #fff; }
        .urgency-Emergency { background: #ed213a; color: #fff; animation: pulse 1s infinite; }

        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.5; } 100% { opacity: 1; } }

        .device-badge-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 10px;
            padding: 10px 14px;
            margin-bottom: 8px;
        }
        .status-pill {
            font-size: 11px;
            font-weight: 800;
            padding: 3px 8px;
            border-radius: 20px;
            text-transform: uppercase;
        }
        .status-pill-online { background: #11998e; color: #fff; }
        .status-pill-offline { background: #ed213a; color: #fff; }

        .appt-item {
            background: rgba(255,255,255,0.03);
            padding: 16px;
            border-radius: 10px;
            margin-bottom: 12px;
            border-left: 4px solid var(--primary);
        }
        .appt-item.Completed { border-left-color: #00c6ff; opacity: 0.65; }
        .appt-item.Cancelled { border-left-color: #ed213a; opacity: 0.45; }

        .tag { font-size: 10px; font-weight: 800; padding: 3px 8px; border-radius: 4px; text-transform: uppercase; }
        .tag-Scheduled { background: #11998e; color: #fff; }
        .tag-Completed { background: #0072ff; color: #fff; }
        .tag-Cancelled { background: #ed213a; color: #fff; }

        .actions-row { margin-top: 12px; display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
        .status-bar {
            position: fixed;
            bottom: 20px;
            left: 50%;
            transform: translateX(-50%);
            background: #121722;
            border: 1px solid var(--primary);
            padding: 10px 24px;
            border-radius: 20px;
            font-size: 12px;
            color: #00ff88;
            font-weight: 700;
        }
    </style>
</head>
<body>
    <div class="container">
        <h2 id="pageTitle">🏥 Hospital Kiosk</h2>

        <div class="cat-tabs" id="catTabs"></div>

        <!-- MULTI-DEVICE KIOSK HEALTH -->
        <div class="card" id="healthCard">
            <div class="title">📡 Kiosk Connectivity & Hardware Status</div>
            <div id="healthContent" style="font-size:13px; color:var(--text-muted);">Detecting hardware modules...</div>
        </div>

        <!-- COMPANY PICKER (only shown for Company category) -->
        <div class="company-picker" id="companyPicker">
            <div class="cp-label">🏢 Select Company</div>
            <select id="companySelect" onchange="selectCompany(parseInt(this.value))"></select>
        </div>

        <!-- AI ASSISTANT -->
        <div class="card">
            <div class="title" id="aiCardTitle">🤖 AI Assistant</div>
            <p id="aiCardDesc" style="font-size:12px; color:var(--text-muted); margin-bottom:14px;"></p>
            <textarea id="aiInput" placeholder=""></textarea>
            <button class="btn-triage" id="aiBtn" onclick="runAiAssist()"></button>
            <div id="triageResult" class="triage-result">
                <span id="triageUrgency" class="urgency-badge"></span>
                <div id="triageDocName" class="doc-name"></div>
                <div id="triageDocLocation" class="doc-location"></div>
                <div id="triageReasoning" class="reasoning"></div>
                <button id="useThisBtn" class="btn-done" style="width:auto; padding:8px 16px; font-size:13px;" onclick="applyTriageItem()">Use This</button>
            </div>
        </div>

        <!-- VOICE BOOKING KIOSK -->
        <div class="card" id="voiceCard">
            <div class="title">🎤 Voice Booking Kiosk</div>
            <p style="font-size:12px; color:var(--text-muted); margin-bottom:14px;">
                Press start and state your full name, 10-digit mobile number, who you need, and time.
            </p>
            <div id="aiPrompt" class="prompt-box"></div>
            <button id="recordBtn" class="btn-voice" onclick="toggleRecording()">🎤 Start Speaking</button>
            <button id="resetVoiceBtn" style="display:none; background:#333; color:#ccc;" onclick="resetVoiceContext()">Reset Voice Session</button>
        </div>

        <!-- MANUAL / HYBRID BOOKING FORM -->
        <div class="card" id="manualCard">
            <div class="title" id="manualCardTitle">🏥 Self-Booking / Manual Input</div>
            <input type="text" id="visitorName" placeholder="Full Name">
            <input type="text" id="visitorPhone" placeholder="10-Digit Mobile Number">
            <select id="itemSelect"></select>
            <div class="row">
                <input type="date" id="apptDate">
                <input type="text" id="apptTime" placeholder="Time (e.g. 10:30 AM)">
            </div>
            <button id="bookBtn" onclick="bookAppointment()"></button>
        </div>

        <!-- SMART WAYFINDING -->
        <div class="card" id="navigationCard" style="display:none;">
            <div class="title">🧭 Smart Wayfinding</div>
            <p style="font-size:12px; color:var(--text-muted); margin-bottom:14px;">Describe one or more things you want to find. The AI can build a multi-stop visit plan.</p>
            <textarea id="navInput" placeholder="e.g. I need shoes, coffee and then the cinema"></textarea>
            <button class="btn-done" onclick="runNavigation()">🧭 Build My Route</button>
            <div id="navResult" class="triage-result"></div>
            <button id="phoneBtn" class="btn-voice" style="display:none;" onclick="createPhonePass()">📱 Continue on My Phone</button>
        </div>

        <!-- DASHBOARD -->
        <div class="card" id="dashboardCard">
            <div class="title">📋 Dashboard & Live Schedule</div>
            <select id="filterItem" onchange="loadAppointments()"></select>
            <div id="appointmentsList">Loading...</div>
        </div>
    </div>

    <div class="status-bar" id="statusBox">SYSTEM ONLINE</div>

    <script>
        const CATALOGS = ${JSON.stringify(CATALOGS)};
        let currentCategory = 'hospital';
        let currentCompanyId = null;

        document.getElementById('apptDate').valueAsDate = new Date();

        let mediaRecorder = null;
        let audioChunks = [];
        let isRecording = false;
        let currentDraft = null;
        let mediaStream = null;
        let lastTriageItemId = null;
        let lastNavigation = null;

        function updateStatus(txt) {
            document.getElementById('statusBox').innerText = txt;
        }

        function renderTabs() {
            const tabs = document.getElementById('catTabs');
            tabs.innerHTML = Object.values(CATALOGS).map(cat => \`
                <div class="cat-tab \${cat.key === currentCategory ? 'active' : ''}" onclick="setCategory('\${cat.key}')">
                    \${cat.icon} \${cat.title.split(' ')[0]}
                </div>
            \`).join('');
        }

        function renderCompanyPicker(cat) {
            const picker = document.getElementById('companyPicker');
            if (!cat.hasCompanies) {
                picker.style.display = 'none';
                return;
            }
            picker.style.display = 'block';

            if (!currentCompanyId || !cat.companies.some(c => c.id === currentCompanyId)) {
                currentCompanyId = cat.companies[0].id;
            }

            document.getElementById('companySelect').innerHTML = cat.companies.map(c => \`
                <option value="\${c.id}" \${c.id === currentCompanyId ? 'selected' : ''}>\${c.name}</option>
            \`).join('');
        }

        function selectCompany(companyId) {
            currentCompanyId = companyId;
            refreshItemLists();
            document.getElementById('triageResult').style.display = 'none';
            renderCompanyPicker(CATALOGS[currentCategory]);
            loadAppointments();
        }

        function currentItems() {
            const cat = CATALOGS[currentCategory];
            if (cat.hasCompanies) {
                const company = cat.companies.find(c => c.id === currentCompanyId) || cat.companies[0];
                return company.departments;
            }
            return cat.items;
        }

        function refreshItemLists() {
            const cat = CATALOGS[currentCategory];
            const items = currentItems();

            const itemSelect = document.getElementById('itemSelect');
            itemSelect.innerHTML = items.map(i => \`<option value="\${i.id}">\${i.name} (\${i.location})</option>\`).join('');

            const filterItem = document.getElementById('filterItem');
            filterItem.innerHTML = '<option value="ALL">All ' + cat.itemLabel + 's</option>' +
                items.map(i => \`<option value="\${i.id}">\${i.name}</option>\`).join('');
        }

        async function runNavigation() {
            const query = document.getElementById('navInput').value.trim();
            if (!query) return alert('Tell me where you want to go.');
            updateStatus('Building smart route...');
            try {
                const r = await fetch('/api/navigation', {
                    method: 'POST', headers: {'Content-Type':'application/json'},
                    body: JSON.stringify({ category: currentCategory, companyId: currentCompanyId, query })
                });
                const data = await r.json();
                if (!r.ok || data.error) throw new Error(data.error || 'Navigation failed');
                lastNavigation = data;
                const box = document.getElementById('navResult');
                box.innerHTML = '<div style="font-weight:800;margin-bottom:8px;">' + (data.title || 'Your route') + '</div>' +
                    data.stops.map((s, i) => '<div style="padding:9px 0;border-bottom:1px solid rgba(255,255,255,.08);"><b>' + (i + 1) + '. ' + s.itemName + '</b><br><small>' + s.location + '</small><br><span style="font-size:12px;">' + (s.reason || '') + '</span></div>').join('');
                box.style.display = 'block';
                document.getElementById('phoneBtn').style.display = 'block';
                updateStatus('Route ready.');
            } catch (err) {
                updateStatus('Navigation failed.');
                alert(err.message);
            }
        }

        async function createPhonePass() {
            if (!lastNavigation) return;
            try {
                const r = await fetch('/api/qr-session', {
                    method:'POST', headers:{'Content-Type':'application/json'},
                    body: JSON.stringify({ category: currentCategory, companyId: currentCompanyId, navigation: lastNavigation })
                });
                const data = await r.json();
                if (!r.ok || data.error) throw new Error(data.error || 'Could not create phone pass');
                window.open(data.url, '_blank', 'noopener,noreferrer');
                updateStatus('Phone pass created.');
            } catch (err) { alert(err.message); }
        }

        async function refreshHealth() {
            try {
                const r = await fetch('/api/kiosk/status');
                const s = await r.json();
                const container = document.getElementById('healthContent');

                if (!s.devices || s.devices.length === 0) {
                    container.innerHTML = '<span>Broker Connected: <b>' + (s.mqttConnected ? 'YES' : 'NO') + '</b> | No active hardware devices detected yet.</span>';
                    return;
                }

                container.innerHTML = s.devices.map(d => \`
                    <div class="device-badge-row">
                        <div>
                            <b>ID / IMEI: \${d.deviceId}</b>
                            <div style="font-size:11px; color:var(--text-muted); margin-top:2px;">
                                Last seen: \${d.lastSeen ? new Date(d.lastSeen).toLocaleTimeString() : '—'}
                                \${d.csq !== undefined ? ' | CSQ: ' + d.csq : ''}
                            </div>
                        </div>
                        <span class="status-pill \${d.online ? 'status-pill-online' : 'status-pill-offline'}">
                            \${d.online ? 'ONLINE' : 'OFFLINE'}
                        </span>
                    </div>
                \`).join('');
            } catch (_) {
                document.getElementById('healthContent').innerText = 'Server reachable; kiosk status unavailable.';
            }
        }

        function setCategory(key) {
            currentCategory = key;
            currentCompanyId = null;
            const cat = CATALOGS[key];

            document.getElementById('pageTitle').innerText = cat.icon + ' ' + cat.title;
            document.getElementById('aiCardTitle').innerText = '🤖 ' + cat.itemLabel + ' Assistant';
            document.getElementById('aiCardDesc').innerText = 'Tell the AI what you need — it will recommend the right ' + cat.itemLabel.toLowerCase() + '.';
            document.getElementById('aiInput').placeholder = cat.inputPlaceholder;
            document.getElementById('aiBtn').innerText = cat.aiButtonLabel;

            renderCompanyPicker(cat);

            const bookingOn = cat.bookingEnabled !== false;
            document.getElementById('voiceCard').style.display = bookingOn ? 'block' : 'none';
            document.getElementById('manualCard').style.display = bookingOn ? 'block' : 'none';
            document.getElementById('dashboardCard').style.display = bookingOn ? 'block' : 'none';
            document.getElementById('navigationCard').style.display = (key === 'mall' || key === 'company') ? 'block' : 'none';

            if (bookingOn) {
                document.getElementById('manualCardTitle').innerText = cat.icon + ' Self-Booking / Manual Input';
                document.getElementById('bookBtn').innerText = cat.bookButtonLabel || 'Book';
                refreshItemLists();
            }

            document.getElementById('triageResult').style.display = 'none';
            document.getElementById('useThisBtn').style.display = bookingOn ? 'inline-block' : 'none';
            document.getElementById('aiInput').value = '';
            lastTriageItemId = null;
            resetVoiceContext();

            renderTabs();
            if (bookingOn) loadAppointments();
        }

        function syncDraftToForm(draft) {
            if (!draft) return;
            if (draft.visitorName) document.getElementById('visitorName').value = draft.visitorName;
            if (draft.visitorPhone) document.getElementById('visitorPhone').value = draft.visitorPhone;
            if (draft.matchedItemId) document.getElementById('itemSelect').value = draft.matchedItemId;
            if (draft.appointmentDate) document.getElementById('apptDate').value = draft.appointmentDate;
            if (draft.appointmentTime) document.getElementById('apptTime').value = draft.appointmentTime;
        }

        function resetVoiceContext() {
            currentDraft = null;
            document.getElementById('aiPrompt').style.display = 'none';
            document.getElementById('resetVoiceBtn').style.display = 'none';
            document.getElementById('recordBtn').innerText = '🎤 Start Speaking';
            document.getElementById('recordBtn').className = 'btn-voice';
        }

        async function toggleRecording() {
            const btn = document.getElementById("recordBtn");

            if (!isRecording) {
                try {
                    mediaStream = await navigator.mediaDevices.getUserMedia({ audio: true });
                    mediaRecorder = new MediaRecorder(mediaStream);
                    audioChunks = [];

                    mediaRecorder.ondataavailable = (e) => { if (e.data.size > 0) audioChunks.push(e.data); };
                    mediaRecorder.onstop = uploadAudio;

                    mediaRecorder.start();
                    isRecording = true;
                    btn.innerText = "🛑 Stop & Submit Audio";
                    btn.className = "btn-recording";
                    updateStatus("Listening...");
                } catch (err) {
                    alert("Microphone access denied.");
                }
            } else {
                if (mediaRecorder && mediaRecorder.state !== "inactive") mediaRecorder.stop();
                if (mediaStream) mediaStream.getTracks().forEach(t => t.stop());
                isRecording = false;
                btn.className = "btn-voice";
                updateStatus("Processing audio...");
            }
        }

        function uploadAudio() {
            const audioBlob = new Blob(audioChunks, { type: 'audio/wav' });
            const formData = new FormData();
            formData.append("audio", audioBlob, "voice_booking.wav");
            formData.append("category", currentCategory);
            if (currentCompanyId) formData.append("companyId", currentCompanyId);

            if (currentDraft) {
                formData.append("draft", JSON.stringify(currentDraft));
            }

            fetch('/api/voice-book', { method: 'POST', body: formData })
            .then(res => res.json())
            .then(data => {
                const promptBox = document.getElementById('aiPrompt');
                const resetBtn = document.getElementById('resetVoiceBtn');

                if (data.isComplete) {
                    resetVoiceContext();
                    updateStatus("Voice booking #" + data.appointment.apptId + " confirmed!");
                    loadAppointments();
                } else if (data.missingDetails) {
                    currentDraft = data.draft;
                    syncDraftToForm(data.draft);

                    promptBox.innerText = "⚠️ Missing: " + data.missingDetails.join(", ") + ". Speak again OR complete the fields manually below.";
                    promptBox.style.display = 'block';
                    resetBtn.style.display = 'block';
                    document.getElementById('recordBtn').innerText = "🎤 Speak Missing Details";
                    updateStatus("Incomplete details.");
                } else {
                    alert("Error: " + (data.error || "Voice processing failed."));
                }
            });
        }

        function runAiAssist() {
            const query = document.getElementById('aiInput').value.trim();
            if (!query) return alert("Please describe what you need first.");

            updateStatus("AI thinking...");
            fetch('/api/ai-assist', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ category: currentCategory, companyId: currentCompanyId, query })
            })
            .then(r => r.json())
            .then(data => {
                if (data.error) {
                    updateStatus("AI request failed.");
                    return alert("Error: " + data.error);
                }

                lastTriageItemId = data.recommendedItemId;

                document.getElementById('triageDocName').innerText = data.itemName;
                document.getElementById('triageDocLocation').innerText = data.location;
                document.getElementById('triageReasoning').innerText = data.reasoning;

                const badge = document.getElementById('triageUrgency');
                badge.innerText = data.urgency;
                badge.className = 'urgency-badge urgency-' + data.urgency;

                document.getElementById('triageResult').style.display = 'block';
                updateStatus(data.urgency === 'Emergency' ? "⚠️ EMERGENCY flagged by AI!" : "AI recommendation ready.");
            })
            .catch(() => updateStatus("AI request failed."));
        }

        function applyTriageItem() {
            if (!lastTriageItemId) return;
            document.getElementById('itemSelect').value = lastTriageItemId;
            document.getElementById('visitorName').scrollIntoView({ behavior: 'smooth' });
            updateStatus("Pre-filled from AI recommendation.");
        }

        function loadAppointments() {
            const itemId = document.getElementById('filterItem').value;
            let url = '/api/appointments?category=' + encodeURIComponent(currentCategory) + '&itemId=' + encodeURIComponent(itemId);
            if (currentCompanyId) url += '&companyId=' + encodeURIComponent(currentCompanyId);

            fetch(url)
                .then(r => r.json())
                .then(data => {
                    const list = document.getElementById('appointmentsList');
                    if (!Array.isArray(data) || data.length === 0) {
                        list.innerHTML = "<p style='font-size:13px; color:#aaa; text-align:center; padding: 12px;'>No matching bookings found.</p>";
                        return;
                    }
                    list.innerHTML = data.map(a => \`
                        <div class="appt-item \${a.status}">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <strong>#\${a.apptId} - \${a.visitorName}</strong>
                                <span class="tag tag-\${a.status}">\${a.status}</span>
                            </div>
                            <small style="color:var(--text-muted); display:block; margin-top:4px;">
                                📞 \${a.visitorPhone} | <b>\${a.itemName}</b>\${a.companyName ? ' @ ' + a.companyName : ''}
                            </small>
                            <small style="color:var(--text-muted);">
                                📅 \${a.date} at <b>\${a.time}</b>
                            </small>

                            \${a.status === 'Scheduled' ? \`
                            <div class="actions-row">
                                <input type="text" id="newtime-\${a._id}" placeholder="New Time" style="width:110px; margin:0; padding:6px 10px; font-size:12px;">
                                <input type="text" id="newphone-\${a._id}" placeholder="New Mobile" style="width:125px; margin:0; padding:6px 10px; font-size:12px;">
                                <button class="btn-warn" onclick="reschedule('\${a._id}')" style="width:auto; padding:6px 12px; font-size:12px; margin:0;">Modify</button>
                                <button class="btn-done" onclick="updateStatusAppt('\${a._id}', 'Completed')" style="width:auto; padding:6px 12px; font-size:12px; margin:0;">Complete</button>
                                <button class="btn-cancel" onclick="updateStatusAppt('\${a._id}', 'Cancelled')" style="width:auto; padding:6px 12px; font-size:12px; margin:0;">Cancel</button>
                            </div>
                            \` : ''}
                        </div>
                    \`).join('');
                });
        }

        function bookAppointment() {
            const name = document.getElementById('visitorName').value.trim();
            const phone = document.getElementById('visitorPhone').value.trim();
            const itemId = document.getElementById('itemSelect').value;
            const date = document.getElementById('apptDate').value;
            const time = document.getElementById('apptTime').value.trim();

            if (!name) return alert("Please enter your name.");
            if (!/^[0-9]{10}$/.test(phone)) return alert("Please enter a valid 10-digit mobile number.");
            if (!date) return alert("Please select a date.");
            if (!time) return alert("Please enter a time.");

            fetch('/api/book', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ category: currentCategory, companyId: currentCompanyId, name, phone, itemId, date, time })
            })
            .then(r => r.json())
            .then(data => {
                if (data.error) return alert(data.error);
                updateStatus("Confirmed #" + data.apptId + " (SMS Sent)");
                document.getElementById('visitorName').value = '';
                document.getElementById('visitorPhone').value = '';
                document.getElementById('apptTime').value = '';
                resetVoiceContext();
                loadAppointments();
            });
        }

        function reschedule(id) {
            const newTime = document.getElementById('newtime-' + id).value.trim();
            const newPhone = document.getElementById('newphone-' + id).value.trim();

            if (!newTime && !newPhone) {
                return alert("Please enter a new time or a new 10-digit mobile number.");
            }

            if (newPhone && !/^[0-9]{10}$/.test(newPhone)) {
                return alert("Please enter a valid 10-digit mobile number.");
            }

            updateStatus("Updating & sending SMS...");
            fetch('/api/reschedule', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id, newTime, newPhone })
            })
            .then(r => r.json())
            .then(data => {
                if (data.error) return alert(data.error);
                updateStatus("Updated! SMS Sent.");
                loadAppointments();
            });
        }

        function updateStatusAppt(id, status) {
            updateStatus("Updating status to " + status + "...");
            fetch('/api/status', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id, status })
            })
            .then(r => r.json())
            .then(data => {
                if (data.error) return alert(data.error);
                updateStatus("Status updated. SMS Sent.");
                loadAppointments();
            });
        }

        setCategory('hospital');
        refreshHealth();
        setInterval(() => {
    // Check if the user is currently typing in any input or textarea
    const isTyping = document.activeElement && 
        (document.activeElement.tagName === 'INPUT' || document.activeElement.tagName === 'TEXTAREA');

    // Only refresh if the user is NOT actively typing
    if (!isTyping && CATALOGS[currentCategory].bookingEnabled !== false) {
        loadAppointments();
    }
}, 10000); // 10 seconds interval
    </script>
</body>
</html>`);
});

// ─────────────────────────────────────────────────────────
// 7. REST API: VOICE RECOGNITION (category + company aware)
// ─────────────────────────────────────────────────────────
app.post("/api/voice-book", upload.single("audio"), async (req, res) => {
  const audioFilePath = req.file ? req.file.path : null;

  try {
    if (!audioFilePath)
      return res.status(400).json({ error: "No audio file received" });

    const categoryKey = req.body.category || "hospital";
    const catalog = getCatalog(categoryKey);

    if (catalog.bookingEnabled === false) {
      return res
        .status(400)
        .json({ error: `Booking is not available for ${catalog.title}.` });
    }

    const { items, companyId, companyName } = resolveItems(
      categoryKey,
      req.body.companyId
    );

    let draft = {};
    if (req.body.draft) {
      try {
        draft = JSON.parse(req.body.draft);
      } catch (e) {}
    }

    const audioBuffer = fs.readFileSync(audioFilePath);
    const base64Audio = audioBuffer.toString("base64");

    const bookingSchema = {
      type: Type.OBJECT,
      properties: {
        visitorName: { type: Type.STRING },
        visitorPhone: { type: Type.STRING },
        matchedItemId: { type: Type.INTEGER },
        appointmentDate: { type: Type.STRING },
        appointmentTime: { type: Type.STRING },
      },
      required: [],
    };

    const itemList = items
      .map((i) => `${i.id}: ${cleanText(i.name)}`)
      .join(", ");
    const companyContext = companyName ? ` at ${companyName}` : "";

    const promptText = `Transcribe this spoken request and extract booking parameters for a ${
      catalog.title
    }${companyContext}.
      Existing gathered info: ${JSON.stringify(draft)}.
      Current date is: ${new Date().toISOString().split("T")[0]}.
      Available ${catalog.itemLabel} IDs: ${itemList}.
      Extract missing items (visitorName, visitorPhone, matchedItemId, appointmentDate, appointmentTime) and return updated JSON.`;

    const response = await generateGeminiContent({
      operation: "voice booking",
      contents: [
        {
          inlineData: {
            mimeType: req.file.mimetype || "audio/wav",
            data: base64Audio,
          },
        },
        { text: promptText },
      ],
      config: {
        responseMimeType: "application/json",
        responseSchema: bookingSchema,
      },
    });

    const extracted = JSON.parse(response.text || "{}");

    const merged = {
      visitorName: extracted.visitorName || draft.visitorName || "",
      visitorPhone: extracted.visitorPhone || draft.visitorPhone || "",
      matchedItemId: extracted.matchedItemId || draft.matchedItemId || null,
      appointmentDate: extracted.appointmentDate || draft.appointmentDate || "",
      appointmentTime: extracted.appointmentTime || draft.appointmentTime || "",
    };

    if (merged.visitorPhone) {
      merged.visitorPhone = String(merged.visitorPhone).replace(/\D/g, "");
    }

    const missing = [];
    if (!merged.visitorName.trim()) missing.push("Full Name");
    if (!isValidPhone(merged.visitorPhone))
      missing.push("10-Digit Phone Number");
    if (!merged.matchedItemId) missing.push(catalog.itemLabel);
    if (!merged.appointmentDate.trim()) missing.push("Date");
    if (!merged.appointmentTime.trim()) missing.push("Time");

    if (missing.length > 0) {
      return res.json({
        isComplete: false,
        missingDetails: missing,
        draft: merged,
      });
    }

    const item = items.find((i) => i.id === parseInt(merged.matchedItemId));
    const apptId = Math.floor(1000 + Math.random() * 9000);

    const record = {
      apptId,
      category: categoryKey,
      visitorName: cleanText(merged.visitorName),
      visitorPhone: merged.visitorPhone,
      itemId: merged.matchedItemId,
      itemName: item ? item.name : "Unassigned",
      location: item ? item.location : "",
      date: merged.appointmentDate,
      time: merged.appointmentTime,
      status: "Scheduled",
      createdAt: new Date(),
    };
    if (catalog.hasCompanies) {
      record.companyId = companyId;
      record.companyName = companyName;
    }

    const newAppt = await db.insert(record);

    safePublish(MQTT_TOPIC_CMD, "200");
    const smsPayload = `SMS:${merged.visitorPhone}:Voice booking #${apptId} confirmed for ${merged.appointmentDate} at ${merged.appointmentTime}.`;
    safePublish(MQTT_TOPIC_CMD, smsPayload);

    return res.json({ isComplete: true, appointment: newAppt });
  } catch (err) {
    console.error("[VOICE AI ERROR]:", err);
    const status = getGeminiStatus(err);
    const httpStatus = status === 429 ? 429 : status === 503 ? 503 : 500;
    return res.status(httpStatus).json({
      error: geminiErrorMessage(err),
      code: status === 429 ? "GEMINI_RATE_LIMIT" : status === 503 ? "GEMINI_UNAVAILABLE" : "GEMINI_ERROR",
      retryable: status === 429 || status === 503,
    });
  } finally {
    if (audioFilePath && fs.existsSync(audioFilePath)) {
      try {
        fs.unlinkSync(audioFilePath);
      } catch (e) {}
    }
  }
});

// ─────────────────────────────────────────────────────────
// 7B. REST API: AI DIRECTORY / TRIAGE ASSISTANT (category + company aware)
// ─────────────────────────────────────────────────────────
app.post("/api/ai-assist", async (req, res) => {
  try {
    const { category, companyId, query } = req.body;
    if (!query || !String(query).trim()) {
      return res.status(400).json({ error: "Please describe what you need." });
    }

    const categoryKey = category || "hospital";
    const catalog = getCatalog(categoryKey);
    const { items, companyName } = resolveItems(categoryKey, companyId);

    const assistSchema = {
      type: Type.OBJECT,
      properties: {
        recommendedItemId: { type: Type.INTEGER },
        reasoning: { type: Type.STRING },
        urgency: {
          type: Type.STRING,
          enum: ["Low", "Medium", "High", "Emergency"],
        },
      },
      required: ["recommendedItemId", "reasoning", "urgency"],
    };

    const itemList = items
      .map((i) => `${i.id}: ${cleanText(i.name)}`)
      .join(", ");
    const companyContext = companyName
      ? ` The visitor is at ${companyName}.`
      : "";

    const promptText = `You are ${catalog.aiRole}.${companyContext}
      ${catalog.aiInstruction}
      Available options (id: name): ${itemList}.
      If nothing fits clearly, default to id ${items[0].id}.
      Visitor's own words: """${String(query).slice(0, 1000)}"""
      Respond only with the JSON object described by the schema.`;

    const response = await generateGeminiContent({
      operation: "AI directory / triage",
      contents: [{ text: promptText }],
      config: {
        responseMimeType: "application/json",
        responseSchema: assistSchema,
      },
    });

    const result = JSON.parse(response.text || "{}");
    const item = items.find((i) => i.id === parseInt(result.recommendedItemId));

    if (!item) {
      return res
        .status(502)
        .json({
          error: "AI could not match an option. Please try rephrasing.",
        });
    }

    if (result.urgency === "Emergency") {
      safePublish(
        MQTT_TOPIC_CMD,
        `SMS:FRONTDESK:AI flagged an EMERGENCY case at the ${catalog.title}${companyContext}.`
      );
    }

    return res.json({
      recommendedItemId: item.id,
      itemName: item.name,
      location: item.location,
      reasoning: cleanText(result.reasoning) || result.reasoning,
      urgency: result.urgency || "Low",
    });
  } catch (err) {
    console.error("[AI ASSIST ERROR]:", err);
    const status = getGeminiStatus(err);
    const httpStatus = status === 429 ? 429 : status === 503 ? 503 : 500;
    return res.status(httpStatus).json({
      error: geminiErrorMessage(err),
      code: status === 429 ? "GEMINI_RATE_LIMIT" : status === 503 ? "GEMINI_UNAVAILABLE" : "GEMINI_ERROR",
      retryable: status === 429 || status === 503,
    });
  }
});

// ─────────────────────────────────────────────────────────
// 8. REST API: MANUAL BOOKING, RESCHEDULE, STATUS
// ─────────────────────────────────────────────────────────
app.get("/api/appointments", async (req, res) => {
  try {
    const { category, itemId, companyId } = req.query;
    let query = {};
    if (category) query.category = category;
    if (itemId && itemId !== "ALL") query.itemId = parseInt(itemId);
    if (companyId) query.companyId = parseInt(companyId);
    const docs = await db.find(query).sort({ createdAt: -1 });
    res.json(docs);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/book", async (req, res) => {
  try {
    const { category, companyId, name, phone, itemId, date, time } = req.body;
    if (!name || !phone || !date || !time || !itemId) {
      return res.status(400).json({ error: "All fields are required" });
    }
    if (!isValidPhone(phone)) {
      return res
        .status(400)
        .json({ error: "Invalid 10-digit mobile phone number" });
    }

    const categoryKey = category || "hospital";
    const catalog = getCatalog(categoryKey);

    if (catalog.bookingEnabled === false) {
      return res
        .status(400)
        .json({ error: `Booking is not available for ${catalog.title}.` });
    }

    const resolved = resolveItems(categoryKey, companyId);
    const item = resolved.items.find((i) => i.id === parseInt(itemId));
    const apptId = Math.floor(1000 + Math.random() * 9000);

    const record = {
      apptId,
      category: categoryKey,
      visitorName: cleanText(name),
      visitorPhone: phone,
      itemId: parseInt(itemId),
      itemName: item ? item.name : "Unassigned",
      location: item ? item.location : "",
      date,
      time,
      status: "Scheduled",
      createdAt: new Date(),
    };
    if (catalog.hasCompanies) {
      record.companyId = resolved.companyId;
      record.companyName = resolved.companyName;
    }

    const newAppt = await db.insert(record);

    safePublish(MQTT_TOPIC_CMD, "200");
    const smsPayload = `SMS:${phone}:Booking #${apptId} for ${cleanText(
      item ? item.name : "your visit"
    )} confirmed for ${date} at ${time}.`;
    safePublish(MQTT_TOPIC_CMD, smsPayload);

    res.json(newAppt);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/reschedule", async (req, res) => {
  try {
    const { id, newTime, newPhone } = req.body;
    if (!id || (!newTime && !newPhone))
      return res.status(400).json({ error: "Provide a new time or a new mobile number" });

    if (newPhone && !isValidPhone(newPhone)) {
      return res.status(400).json({ error: "Invalid 10-digit mobile number" });
    }

    const appt = await db.findOne({ _id: id });
    if (!appt) return res.status(404).json({ error: "Booking not found" });

    const updateFields = {};
    if (newTime) updateFields.time = newTime;
    if (newPhone) updateFields.visitorPhone = newPhone;

    await db.update({ _id: id }, { $set: updateFields });

    const cleanItem = cleanText(appt.itemName);
    const targetPhone = newPhone || appt.visitorPhone;
    const timeUsed = newTime || appt.time;

    const smsPayload = `SMS:${targetPhone}:Your booking for ${cleanItem} has been updated. Time: ${timeUsed}.`;
    safePublish(MQTT_TOPIC_CMD, smsPayload);

    res.json({ success: true, updated: updateFields });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/status", async (req, res) => {
  try {
    const { id, status } = req.body;
    if (!id || !status)
      return res.status(400).json({ error: "Missing required fields" });

    const appt = await db.findOne({ _id: id });
    if (!appt) return res.status(404).json({ error: "Booking not found" });

    await db.update({ _id: id }, { $set: { status } });

    const cleanItem = cleanText(appt.itemName);
    if (status === "Completed") {
      const smsPayload = `SMS:${appt.visitorPhone}:Thank you! Your booking with ${cleanItem} (#${appt.apptId}) is now COMPLETED.`;
      safePublish(MQTT_TOPIC_CMD, smsPayload);
    } else if (status === "Cancelled") {
      const smsPayload = `SMS:${appt.visitorPhone}:Your booking for ${cleanItem} on ${appt.date} has been CANCELLED.`;
      safePublish(MQTT_TOPIC_CMD, smsPayload);
    }

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─────────────────────────────────────────────────────────
// 9. SMART KIOSK APIs (Supports Multi-Device Health)
// ─────────────────────────────────────────────────────────
app.get("/api/catalog", (req, res) => {
  res.json(CATALOGS);
});

app.get("/api/kiosk/status", (req, res) => {
  const now = Date.now();
  const list = Object.values(devices).map((d) => {
    const elapsedSec = (now - new Date(d.lastSeen).getTime()) / 1000;
    // Mark as offline if no update has arrived in > 45 seconds
    const isOnline = mqttClient.connected && elapsedSec < 45;
    return {
      ...d,
      online: isOnline,
    };
  });

  res.json({
    kioskId: KIOSK_ID,
    mqttConnected: mqttClient.connected,
    devices: list,
  });
});

app.post("/api/kiosk/command", (req, res) => {
  const { command } = req.body || {};
  if (!command || typeof command !== "string" || command.length > 200) {
    return res.status(400).json({ error: "Invalid command" });
  }
  publishKiosk(command);
  res.json({ success: true, command });
});

app.post("/api/sos", async (req, res) => {
  const source = cleanText(req.body?.source || "KIOSK");
  const message = cleanText(req.body?.message || "Emergency assistance requested");
  const event = {
    type: "SOS",
    kioskId: KIOSK_ID,
    source,
    message,
    timestamp: new Date().toISOString(),
  };
  try {
    await eventDb.insert({ ...event, createdAt: new Date() });
  } catch (err) {
    console.error("[SOS DB ERROR]", err.message);
  }
  publishKiosk("SOS");
  publishKiosk(`SMS:FRONTDESK:EMERGENCY at VANIX kiosk ${KIOSK_ID}. ${message}`);
  res.json({ success: true, event });
});

app.post("/api/navigation", async (req, res) => {
  try {
    const categoryKey = req.body?.category || "mall";
    const catalog = getCatalog(categoryKey);
    const { items, companyName } = resolveItems(categoryKey, req.body?.companyId);
    const query = cleanText(req.body?.query || "");
    if (!query) return res.status(400).json({ error: "Navigation request is empty." });
    if (!items.length) return res.status(400).json({ error: "No destinations are configured." });

    const navSchema = {
      type: Type.OBJECT,
      properties: {
        title: { type: Type.STRING },
        stops: {
          type: Type.ARRAY,
          items: {
            type: Type.OBJECT,
            properties: {
              itemId: { type: Type.INTEGER },
              reason: { type: Type.STRING },
            },
            required: ["itemId", "reason"],
          },
        },
      },
      required: ["title", "stops"],
    };

    const itemList = items.map(i => `${i.id}: ${cleanText(i.name)} — ${cleanText(i.location)}`).join("\n");
    const prompt = `You are a wayfinding assistant. ${companyName ? `The visitor is at ${companyName}.` : ""}
User request: "${query.slice(0, 1000)}"
Available destinations:
${itemList}
Return only a JSON route. Include only destinations that clearly match the request, in a sensible visit order. Maximum 6 stops. Never invent an ID.`;

    let result;
    try {
      const response = await generateGeminiContent({
        operation: "navigation",
        contents: [{ text: prompt }],
        config: { responseMimeType: "application/json", responseSchema: navSchema },
      });
      result = JSON.parse(response.text || "{}");
    } catch (err) {
      console.warn("[NAV AI FALLBACK]", err.message);
      const q = query.toLowerCase();
      const fallback = items.filter(i => q.split(/\\W+/).some(w => w.length > 2 && cleanText(i.name).toLowerCase().includes(w))).slice(0, 6);
      result = { title: "Suggested destinations", stops: fallback.map(i => ({ itemId: i.id, reason: "Matches a term in your request." })) };
    }

    const stops = Array.isArray(result.stops) ? result.stops.map(s => {
      const item = items.find(i => i.id === parseInt(s.itemId));
      return item ? { itemId: item.id, itemName: item.name, location: item.location, reason: cleanText(s.reason) } : null;
    }).filter(Boolean).slice(0, 6) : [];

    if (!stops.length) return res.status(422).json({ error: "I could not match your request to the configured destinations." });
    res.json({ category: categoryKey, companyName, title: cleanText(result.title) || "Your route", stops });
  } catch (err) {
    console.error("[NAV ERROR]", err);
    res.status(500).json({ error: "Failed to build navigation route." });
  }
});

app.post("/api/navigation/send-to-kiosk", (req, res) => {
  try {
    const navigation = req.body?.navigation;

    if (!navigation || !Array.isArray(navigation.stops) || !navigation.stops.length) {
      return res.status(400).json({ error: "Navigation route is required." });
    }

    const compact = JSON.stringify({
      title: cleanText(navigation.title || "Your route"),
      stops: navigation.stops.slice(0, 6).map((s) => ({
        itemId: Number(s.itemId),
        itemName: cleanText(s.itemName),
        location: cleanText(s.location),
        reason: cleanText(s.reason),
      })),
    });

    publishKiosk(`NAV:${compact}`);

    res.json({
      success: true,
      kioskId: KIOSK_ID,
      stops: Math.min(navigation.stops.length, 6),
    });
  } catch (err) {
    console.error("[NAV SEND ERROR]", err);
    res.status(500).json({ error: "Failed to send navigation to kiosk." });
  }
});

app.post("/api/qr-session/send-to-kiosk", async (req, res) => {
  try {
    const navigation = req.body?.navigation || null;

    const token = await createVisitorSession({
      category: req.body?.category || "mall",
      companyId: req.body?.companyId || null,
      navigation,
    });

    const url = `${PUBLIC_BASE_URL.replace(/\/$/, "")}/continue/${token}`;

    publishKiosk(`QR:${url}`);

    res.json({
      success: true,
      token,
      url,
      expiresInSeconds: 900,
    });
  } catch (err) {
    console.error("[QR SEND ERROR]", err);
    res.status(500).json({ error: "Failed to create/send QR session." });
  }
});

app.post("/api/qr-session", async (req, res) => {
  try {
    const data = {
      category: req.body?.category || "mall",
      companyId: req.body?.companyId || null,
      navigation: req.body?.navigation || null,
    };
    const token = await createVisitorSession(data);
    const url = `${PUBLIC_BASE_URL.replace(/\/$/, "")}/continue/${token}`;
    res.json({ token, url, expiresInSeconds: 900 });
  } catch (err) {
    res.status(500).json({ error: "Failed to create phone session." });
  }
});

app.get("/continue/:token", async (req, res) => {
  const session = await getVisitorSession(req.params.token);
  if (!session) return res.status(404).send("<h2>Session expired</h2><p>Please return to the kiosk and create a new phone pass.</p>");
  const navigation = session.data.navigation;
  const stopHtml = (navigation?.stops || []).map(s =>
    '<li><b>' + cleanText(s.itemName) + '</b><br><small>' + cleanText(s.location) + '</small><br>' + cleanText(s.reason || '') + '</li>'
  ).join('');
  res.send('<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>VANIX Visit Pass</title><style>body{margin:0;padding:24px;background:#080d18;color:#fff;font-family:Arial,sans-serif}main{max-width:520px;margin:auto;background:#111a2b;padding:24px;border-radius:20px}h1{color:#53c7ff}li{margin:14px 0;padding:12px;background:#18243a;border-radius:12px}small{color:#9aa8bd}</style></head><body><main><h1>VANIX Visit Pass</h1><p>' + cleanText(navigation?.title || 'Your saved route') + '</p><ol>' + stopHtml + '</ol><p><small>This pass is temporary and expires in 15 minutes.</small></p></main></body></html>');
});

app.get("/api/kiosk/events", async (req, res) => {
  try {
    const limit = Math.min(
      Math.max(parseInt(req.query.limit || "50", 10), 1),
      200
    );

    const docs = await eventDb
      .find({})
      .sort({ createdAt: -1 })
      .limit(limit);

    res.json(docs);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/kiosk/ping", (req, res) => {
  publishKiosk("PING");
  res.json({
    success: true,
    command: "PING",
    kioskId: KIOSK_ID,
  });
});

// ─────────────────────────────────────────────────────────
// 8B. AI CONFIGURATION / HEALTH
// ─────────────────────────────────────────────────────────
app.get("/api/ai-status", (req, res) => {
  res.json({
    configured: Boolean(apiKey),
    primaryModel: GEMINI_MODEL,
    fallbackModel: GEMINI_FALLBACK_MODEL,
    maxRetriesPerModel: GEMINI_MAX_RETRIES,
    baseRetryMs: GEMINI_BASE_RETRY_MS,
    retryOn: [429, 503],
  });
});

// ─────────────────────────────────────────────────────────
// 9. START SERVER
// ─────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", () => {
  console.log(`[KIOSK SERVER] Live on http://localhost:${PORT}`);
});