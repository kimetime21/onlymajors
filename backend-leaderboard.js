/**
 * OnlyMajors · leaderboard backend
 * ------------------------------------------------------------
 * Minimal Express service that fronts the DataGolf API for the
 * OnlyMajors frontend (onlymajors.com).
 *
 * Deploy at:  api.onlymajors.com  (or onlymajors.com/api via a path rewrite)
 *
 *   GET  /api/leaderboard/:majorId   → live + projected money per golfer
 *   GET  /api/field/:majorId         → current field for a major
 *   GET  /api/health                 → cheap health check
 *
 * Why a backend at all?  DataGolf's paid endpoints don't support browser CORS
 * and the API key shouldn't ever ship in client-side JS. This service runs
 * in your own infrastructure (Vercel / Railway / Fly / a VPS), holds the key
 * in an environment variable, queries DataGolf, normalizes the shape to what
 * the frontend expects, and caches for 60 seconds to stay inside rate limits.
 *
 * SETUP
 *   1. npm init -y
 *   2. npm i express
 *   3. Save this file as backend-leaderboard.js
 *   4. Set DATAGOLF_API_KEY in your environment
 *   5. node backend-leaderboard.js
 *   6. Frontend's fetchMajorLeaderboard() uncomments the /api call line
 *
 * DEPLOY
 *   - Vercel:   add as an /api/* serverless function (each handler in its own file)
 *   - Railway:  push a repo with this file + package.json, set env var, deploy
 *   - Fly.io:   fly launch, set DATAGOLF_API_KEY secret, fly deploy
 *   - Any VPS:  systemd/pm2 + a reverse proxy
 * ------------------------------------------------------------
 */

import express from "express";
import cors from "cors";

const PORT      = process.env.PORT || 3001;
const DG_KEY    = process.env.DATAGOLF_API_KEY;
const DG_BASE   = "https://feeds.datagolf.com";
const CACHE_TTL = 60_000;          // 60 seconds — be kind to the DataGolf API
const ALLOWED_ORIGINS = (process.env.CORS_ORIGINS || "https://onlymajors.com,https://www.onlymajors.com").split(",");

if (!DG_KEY) {
  console.error("✖  DATAGOLF_API_KEY env var not set — exiting");
  process.exit(1);
}

const app = express();
// CORS: handle "*" wildcard, allow null-origin (file:// previews), and a list of explicit origins.
app.use(cors({
  origin: (origin, callback) => {
    if (ALLOWED_ORIGINS.includes("*"))   return callback(null, true);
    if (!origin)                          return callback(null, true);  // server-to-server, curl, file://
    if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
    callback(new Error(`CORS: ${origin} not in allow-list`));
  },
}));
app.use(express.json());

// ─────────────────────────────────────────────────────────────
// Major ID → DataGolf event_id mapping
// These IDs are stable across years. Find them in DataGolf's
// /get-schedule endpoint if any ever shift.
// ─────────────────────────────────────────────────────────────
const DG_EVENT_IDS = {
  masters: 14,
  pga:     33,
  usopen:  26,
  open:    100,
};

// ─────────────────────────────────────────────────────────────
// Tiny in-memory cache
// ─────────────────────────────────────────────────────────────
const cache = new Map();
async function cached(key, ttl, fn) {
  const hit = cache.get(key);
  if (hit && Date.now() - hit.ts < ttl) return hit.data;
  const data = await fn();
  cache.set(key, { ts: Date.now(), data });
  return data;
}

// ─────────────────────────────────────────────────────────────
// DataGolf calls
// ─────────────────────────────────────────────────────────────
async function fetchDG(path, params = {}) {
  const qs = new URLSearchParams({ ...params, key: DG_KEY, file_format: "json" });
  const url = `${DG_BASE}${path}?${qs}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`DataGolf ${path} HTTP ${res.status}`);
  return await res.json();
}

// ─────────────────────────────────────────────────────────────
// Name → frontend golferId resolver.
// The frontend has a curated list of golfers keyed by short ids
// ("scottie", "rory", etc). DataGolf uses "Last, First" + numeric dg_id.
// For stability, the cleanest production move is to add dg_id to each
// golfer in the frontend's GOLFERS table and match on that. Until then,
// match by name.
// ─────────────────────────────────────────────────────────────
const FRONTEND_GOLFER_IDS = [
  ["scottie",   "scottie scheffler"],
  ["rory",      "rory mcilroy"],
  ["xander",    "xander schauffele"],
  ["bryson",    "bryson dechambeau"],
  ["morikawa",  "collin morikawa"],
  ["aberg",     "ludvig aberg"],     // Åberg → aberg after normalization
  ["hovland",   "viktor hovland"],
  ["hideki",    "hideki matsuyama"],
  ["rahm",      "jon rahm"],
  ["cantlay",   "patrick cantlay"],
  ["jt",        "justin thomas"],
  ["spieth",    "jordan spieth"],
  ["theegala",  "sahith theegala"],
  ["koepka",    "brooks koepka"],
  ["niemann",   "joaquin niemann"],
  ["fleetwood", "tommy fleetwood"],
  ["homa",      "max homa"],
  ["henley",    "russell henley"],
  ["burns",     "sam burns"],
  ["hatton",    "tyrrell hatton"],
  ["minwoo",    "min woo lee"],
  ["lowry",     "shane lowry"],
  ["camyoung",  "cameron young"],
  ["keegan",    "keegan bradley"],
  ["rose",      "justin rose"],
  ["fitz",      "matt fitzpatrick"],
  ["reed",      "patrick reed"],
  ["macintyre", "robert macintyre"],
  ["potgieter", "aldrich potgieter"],
  ["smalley",   "alex smalley"],
  ["jaeger",    "stephan jaeger"],
  ["hisatsune", "ryo hisatsune"],
  ["greyserman","max greyserman"],
  ["danbrown",  "daniel brown"],
  ["blanchet",  "chandler blanchet"],
  ["kitayama",  "kurt kitayama"],
  ["gerard",    "ryan gerard"],
  ["haotong",   "haotong li"],
  ["camsmith",  "cameron smith"],
  ["sungjae",   "sungjae im"],
  ["tomkim",    "tom kim"],
  ["siwoo",     "si woo kim"],
  ["straka",    "sepp straka"],
  ["conners",   "corey conners"],
  ["adamscott", "adam scott"],
  ["day",       "jason day"],
  ["english",   "harris english"],
  ["griffin",   "ben griffin"],
  ["zalatoris", "will zalatoris"],
  ["dj",        "dustin johnson"],
  ["finau",     "tony finau"],
  ["bhatia",    "akshay bhatia"],
  ["pavon",     "matthieu pavon"],
  ["mcnealy",   "maverick mcnealy"],
  ["gotterup",  "chris gotterup"],
  ["fowler",    "rickie fowler"],
  ["rai",       "aaron rai"],
  ["woodland",  "gary woodland"],
];
const NORMALIZE = (s) =>
  (s || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "").replace(/[^a-z]/g, "");
const NAME_TO_ID = new Map(FRONTEND_GOLFER_IDS.map(([id, name]) => [NORMALIZE(name), id]));

function matchGolferId(dgPlayerName) {
  if (!dgPlayerName) return null;
  // DataGolf returns "Last, First" — flip first
  const flipped = dgPlayerName.includes(",")
    ? dgPlayerName.split(",").map(s => s.trim()).reverse().join(" ")
    : dgPlayerName;
  return NAME_TO_ID.get(NORMALIZE(flipped)) || null;
}

// ─────────────────────────────────────────────────────────────
// Routes
// ─────────────────────────────────────────────────────────────
app.get("/api/health", (req, res) => {
  res.json({ ok: true, cached_keys: [...cache.keys()] });
});

app.get("/api/leaderboard/:majorId", async (req, res) => {
  const majorId = req.params.majorId;
  const eventId = DG_EVENT_IDS[majorId];
  if (!eventId) return res.status(404).json({ error: `unknown major: ${majorId}` });

  try {
    const data = await cached(`lb:${majorId}`, CACHE_TTL, () =>
      fetchDG("/preds/live-tournament-stats", { stats: "sg_total", display: "value" })
    );

    const golfers = {};
    const players = Array.isArray(data?.live_stats) ? data.live_stats
                  : Array.isArray(data) ? data : [];
    for (const p of players) {
      const matchedGid = matchGolferId(p.player_name);
      // Unmatched players still come through with a synthetic key so the
      // frontend can show the full field on its leaderboard. They aren't
      // pickable for fantasy until added to the curated GOLFERS list.
      const gid = matchedGid || `dg-${p.dg_id || NORMALIZE(p.player_name)}`;
      const made = p.made_cut !== false;
      const pos  = (typeof p.position === "number")
                 ? (p.tied ? `T${p.position}` : `${p.position}`)
                 : (p.position || "");
      golfers[gid] = {
        // For unmatched players, ship name+country so the frontend can render
        // them without a GOLFERS lookup. Matched players ignore these fields.
        matched:     !!matchedGid,
        name:        matchedGid ? null : (p.player_name || ""),
        country:     matchedGid ? null : (p.country || ""),
        position:    pos,
        score:       typeof p.total === "number" ? p.total : null,
        thru:        p.thru ?? "",
        status:      made ? "made_cut" : "mc",
        // DataGolf's projected/final money — use directly OR ignore and let
        // the frontend's projectedPrize() apply your league's tie rule.
        projMoney:   p.proj_money  ?? null,
        finalMoney:  p.final_money ?? null,
      };
    }
    res.json({
      tournamentName: data?.event_name || null,
      currentRound:   data?.current_round || null,
      lastUpdated:    data?.last_updated || null,
      golfers,
    });
  } catch (err) {
    console.error(`leaderboard ${majorId}:`, err.message);
    res.status(502).json({ error: err.message });
  }
});

app.get("/api/field/:majorId", async (req, res) => {
  const majorId = req.params.majorId;
  const eventId = DG_EVENT_IDS[majorId];
  if (!eventId) return res.status(404).json({ error: `unknown major: ${majorId}` });

  try {
    const data = await cached(`field:${majorId}`, CACHE_TTL * 5, () =>
      fetchDG("/field-updates", { tour: "pga" })
    );
    const ids = (data?.field || [])
      .map(p => matchGolferId(p.player_name))
      .filter(Boolean);
    res.json({ count: ids.length, golfers: ids, lastUpdated: data?.last_updated || null });
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`✓  OnlyMajors backend listening on :${PORT}`);
  console.log(`   GET /api/leaderboard/pga`);
  console.log(`   GET /api/field/pga`);
  console.log(`   GET /api/health`);
  console.log(`   CORS allowed: ${ALLOWED_ORIGINS.join(", ")}`);
});
