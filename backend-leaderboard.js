/**
 * OnlyMajors · leaderboard + stats backend
 * ------------------------------------------------------------
 * Express service that fronts the DataGolf API for the OnlyMajors frontend.
 *
 * Routes:
 *   GET  /api/leaderboard/:majorId            → live + projected money per golfer
 *                                                + per-round snapshots (r1..r4)
 *   GET  /api/stats/:majorId/:golferId        → SG breakdown, round-by-round,
 *                                                traditional stats, season totals
 *   GET  /api/field/:majorId                  → current field for a major
 *   GET  /api/health                          → cheap health check
 *
 * Why a backend at all?  DataGolf's paid endpoints don't support browser CORS
 * and the API key shouldn't ever ship in client-side JS. This service runs
 * in your own infrastructure (Railway / Fly / Vercel / a VPS), holds the key
 * in an environment variable, queries DataGolf, normalizes the shape, and
 * caches for 60 seconds to stay inside rate limits.
 *
 * The round-snapshot feature works without persistence — every refresh
 * overwrites the current round's slot, so when round increments the last
 * write is the end-of-round value. (If Railway restarts mid-tournament you
 * lose history; for that risk add a JSON-on-disk persistence layer.)
 *
 * SETUP
 *   1. npm i express cors
 *   2. Set DATAGOLF_API_KEY in your environment
 *   3. node backend-leaderboard.js
 * ------------------------------------------------------------
 */

import express from "express";
import cors from "cors";
import pg from "pg";
import crypto from "crypto";

const PORT      = process.env.PORT || 3001;
const DG_KEY    = process.env.DATAGOLF_API_KEY;
const DG_BASE   = "https://feeds.datagolf.com";
const CACHE_TTL = 60_000;          // 60 seconds — be kind to the DataGolf API
const STATS_TTL = 90_000;          // stats are slower-moving
const SEASON_TTL = 6 * 60 * 60_000; // 6 hours
const SESSION_TTL_DAYS = 30;
const ALLOWED_ORIGINS = (process.env.CORS_ORIGINS || "https://onlymajors.com,https://www.onlymajors.com").split(",");
const DATABASE_URL = process.env.DATABASE_URL;
const DEFAULT_LEAGUE_ID = 1;       // "Experts PGA Fantasy" — auto-seeded on first boot

if (!DG_KEY) {
  console.error("✖  DATAGOLF_API_KEY env var not set — exiting");
  process.exit(1);
}

// ─────────────────────────────────────────────────────────────
// Postgres pool + schema init
// If DATABASE_URL is unset we fall back to in-memory state.
// ─────────────────────────────────────────────────────────────
// Railway's INTERNAL Postgres (postgres.railway.internal) doesn't use SSL.
// Railway's PUBLIC Postgres (*.proxy.rlwy.net) requires SSL with relaxed cert
// checking. Default to no-SSL only for the explicit internal hostname.
const pool = DATABASE_URL
  ? new pg.Pool({
      connectionString: DATABASE_URL,
      ssl: DATABASE_URL.includes(".railway.internal") ? false : { rejectUnauthorized: false },
      max: 4,
    })
  : null;

async function initSchema() {
  if (!pool) {
    console.warn("⚠  DATABASE_URL not set — running with in-memory state only");
    return;
  }
  // Base tables (idempotent)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS leagues (
      id           BIGSERIAL PRIMARY KEY,
      name         TEXT NOT NULL,
      invite_code  TEXT UNIQUE,
      format       TEXT NOT NULL DEFAULT 'season_money',
      scope        TEXT NOT NULL DEFAULT 'season',
      major_id     TEXT,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS users (
      id            BIGSERIAL PRIMARY KEY,
      email         TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      display_name  TEXT,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS league_members (
      id          BIGSERIAL PRIMARY KEY,
      league_id   BIGINT NOT NULL REFERENCES leagues(id) ON DELETE CASCADE,
      user_id     BIGINT REFERENCES users(id) ON DELETE CASCADE,
      team_id     TEXT NOT NULL,
      team_name   TEXT,
      team_color  TEXT,
      role        TEXT NOT NULL DEFAULT 'member',
      joined_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (league_id, team_id)
    );
    CREATE INDEX IF NOT EXISTS idx_league_members_user ON league_members (user_id);

    CREATE TABLE IF NOT EXISTS sessions (
      token       TEXT PRIMARY KEY,
      user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      expires_at  TIMESTAMPTZ NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS picks (
      team_id    TEXT NOT NULL,
      major_id   TEXT NOT NULL,
      starters   JSONB NOT NULL DEFAULT '[]'::jsonb,
      bench      JSONB NOT NULL DEFAULT '[]'::jsonb,
      subs       JSONB NOT NULL DEFAULT '[]'::jsonb,
      submitted  BOOLEAN NOT NULL DEFAULT false,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (team_id, major_id)
    );

    CREATE TABLE IF NOT EXISTS chat_messages (
      id         BIGSERIAL PRIMARY KEY,
      team_id    TEXT NOT NULL,
      text       TEXT NOT NULL,
      ts         BIGINT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_chat_ts ON chat_messages (ts);

    CREATE TABLE IF NOT EXISTS round_snapshots (
      major_id    TEXT NOT NULL,
      dg_id       BIGINT NOT NULL,
      round       INT NOT NULL CHECK (round BETWEEN 1 AND 4),
      proj_money  NUMERIC,
      final_money NUMERIC,
      score       NUMERIC,
      position    TEXT,
      status      TEXT,
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (major_id, dg_id, round)
    );

    CREATE TABLE IF NOT EXISTS profiles (
      team_id      TEXT PRIMARY KEY,
      display_name TEXT,
      team_name    TEXT,
      email        TEXT,
      updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // Migrations: add league_id to picks + chat_messages (idempotent).
  await pool.query(`
    ALTER TABLE picks
      ADD COLUMN IF NOT EXISTS league_id BIGINT NOT NULL DEFAULT 1
        REFERENCES leagues(id) ON DELETE CASCADE;
    ALTER TABLE chat_messages
      ADD COLUMN IF NOT EXISTS league_id BIGINT NOT NULL DEFAULT 1
        REFERENCES leagues(id) ON DELETE CASCADE;
  `);
  // Replace picks PRIMARY KEY to include league_id (only if not already done).
  await pool.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints
         WHERE table_name='picks'
           AND constraint_type='PRIMARY KEY'
           AND constraint_name='picks_pkey'
      ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.key_column_usage
         WHERE table_name='picks'
           AND constraint_name='picks_pkey'
           AND column_name='league_id'
      ) THEN
        ALTER TABLE picks DROP CONSTRAINT picks_pkey;
        ALTER TABLE picks ADD PRIMARY KEY (league_id, team_id, major_id);
      END IF;
    END $$;
  `);

  // Seed the default league + 5 team slots if not already present.
  await pool.query(`
    INSERT INTO leagues (id, name, invite_code, format, scope)
    VALUES (1, 'Experts PGA Fantasy', 'EXPERTS', 'season_money', 'season')
    ON CONFLICT (id) DO NOTHING;
    SELECT setval('leagues_id_seq', GREATEST((SELECT MAX(id) FROM leagues), 1));
  `);
  for (const [teamId, teamName, color] of [
    ["thorne", "Team Thorne", "#3A5F8A"],
    ["larry",  "Team Larry",  "#8A3A3A"],
    ["boo",    "Team Boo",    "#3A8A5A"],
    ["caleb",  "Team Caleb",  "#8A6A3A"],
    ["austin", "Team Austin", "#5A3A8A"],
  ]) {
    await pool.query(
      `INSERT INTO league_members (league_id, team_id, team_name, team_color)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (league_id, team_id) DO NOTHING`,
      [1, teamId, teamName, color]
    );
  }

  console.log("✓  Postgres schema ready · default league seeded");
}

initSchema().catch(e => console.error("✖  schema init failed:", e.message));

// Helper: load all persisted snapshots into the in-memory SNAPSHOTS map on boot.
// Keeps the rest of the codebase unchanged — SNAPSHOTS is still the cache,
// the DB is just the durable source of truth.
async function loadSnapshotsFromDB() {
  if (!pool) return;
  try {
    const { rows } = await pool.query(`SELECT * FROM round_snapshots`);
    for (const r of rows) {
      SNAPSHOTS[r.major_id] = SNAPSHOTS[r.major_id] || {};
      SNAPSHOTS[r.major_id][r.dg_id] = SNAPSHOTS[r.major_id][r.dg_id] || {};
      SNAPSHOTS[r.major_id][r.dg_id][r.round] = {
        projMoney:  r.proj_money == null ? null : Number(r.proj_money),
        finalMoney: r.final_money == null ? null : Number(r.final_money),
        score:      r.score == null ? null : Number(r.score),
        position:   r.position,
        status:     r.status,
        ts:         r.updated_at ? new Date(r.updated_at).getTime() : Date.now(),
      };
    }
    console.log(`✓  loaded ${rows.length} snapshot rows from DB`);
  } catch (e) {
    console.error("✖  snapshot load failed:", e.message);
  }
}

async function persistSnapshot(majorId, dgId, round, snap) {
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO round_snapshots (major_id, dg_id, round, proj_money, final_money, score, position, status, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
       ON CONFLICT (major_id, dg_id, round)
       DO UPDATE SET proj_money = EXCLUDED.proj_money,
                     final_money = EXCLUDED.final_money,
                     score = EXCLUDED.score,
                     position = EXCLUDED.position,
                     status = EXCLUDED.status,
                     updated_at = NOW()`,
      [majorId, dgId, round, snap.projMoney, snap.finalMoney, snap.score, snap.position, snap.status]
    );
  } catch (e) {
    console.warn(`snapshot persist failed (${majorId}/${dgId}/r${round}):`, e.message);
  }
}

const app = express();
app.use(cors({
  origin: (origin, callback) => {
    if (ALLOWED_ORIGINS.includes("*"))    return callback(null, true);
    // file:// previews and server-to-server requests have either no Origin
    // header or the literal string "null". Allow both during prototype life.
    if (!origin || origin === "null")     return callback(null, true);
    if (ALLOWED_ORIGINS.includes(origin))  return callback(null, true);
    callback(new Error(`CORS: ${origin} not in allow-list`));
  },
  allowedHeaders: ["Content-Type", "Authorization"],
}));
app.use(express.json());

// ─────────────────────────────────────────────────────────────
// Auth helpers — scrypt password hashing + opaque session tokens
// ─────────────────────────────────────────────────────────────
function hashPassword(plain) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(plain, salt, 64).toString("hex");
  return `${salt}:${hash}`;
}
function verifyPassword(plain, stored) {
  if (!stored || !stored.includes(":")) return false;
  const [salt, hash] = stored.split(":");
  const candidate = crypto.scryptSync(plain, salt, 64);
  const expected = Buffer.from(hash, "hex");
  if (candidate.length !== expected.length) return false;
  return crypto.timingSafeEqual(candidate, expected);
}
function newSessionToken() {
  return crypto.randomBytes(32).toString("hex");
}
function isValidEmail(s) {
  return typeof s === "string" && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s);
}

// Attach user info to req if a valid Bearer token is present. Does NOT
// reject — that's done by requireAuth on protected routes.
async function authContext(req, _res, next) {
  if (!pool) return next();
  const header = req.headers.authorization || "";
  const token = header.replace(/^Bearer\s+/i, "").trim();
  if (!token) return next();
  try {
    const { rows } = await pool.query(
      `SELECT u.id AS user_id, u.email, u.display_name, s.expires_at
         FROM sessions s JOIN users u ON u.id = s.user_id
        WHERE s.token = $1 AND s.expires_at > NOW()`,
      [token]
    );
    if (rows.length === 1) {
      req.user = {
        id:          Number(rows[0].user_id),
        email:       rows[0].email,
        displayName: rows[0].display_name,
      };
      req.token = token;
    }
  } catch (e) {
    console.warn("authContext lookup failed:", e.message);
  }
  next();
}
app.use(authContext);

function requireAuth(req, res, next) {
  if (!pool) return next();  // dev mode — no DB, no auth
  if (!req.user) return res.status(401).json({ error: "auth required" });
  next();
}

// ─────────────────────────────────────────────────────────────
// Major ID → DataGolf event_id mapping
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
// ─────────────────────────────────────────────────────────────
const FRONTEND_GOLFER_IDS = [
  ["scottie",   "scottie scheffler"],
  ["rory",      "rory mcilroy"],
  ["xander",    "xander schauffele"],
  ["bryson",    "bryson dechambeau"],
  ["morikawa",  "collin morikawa"],
  ["aberg",     "ludvig aberg"],
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
  return NAME_TO_ID.get(NORMALIZE(prettyName(dgPlayerName))) || null;
}

// DataGolf returns "Last, First" — flip to "First Last".
function prettyName(dgPlayerName) {
  if (!dgPlayerName) return "";
  return dgPlayerName.includes(",")
    ? dgPlayerName.split(",").map(s => s.trim()).reverse().join(" ")
    : dgPlayerName;
}

// ─────────────────────────────────────────────────────────────
// Round-by-round snapshot store + reverse lookup tables
// ─────────────────────────────────────────────────────────────
// SNAPSHOTS[majorId][dg_id][round] = { projMoney, score, position, status }
// Each refresh OVERWRITES the current_round slot — so by the time round
// increments, the last value written is the end-of-previous-round value.
const SNAPSHOTS = {};

// dg_id → short id (reverse lookup for the stats endpoint)
const DGID_TO_GID = new Map();
// short id → dg_id
const GID_TO_DGID = new Map();
// short id → display name (last seen pretty name)
const GID_TO_NAME = new Map();

// Hydrates the dg_id ↔ short id reverse maps from a DataGolf player list.
// Runs unconditionally (no current_round gate) so /api/stats can resolve
// a short id even when DataGolf's response is missing current_round.
function hydrateNameMaps(players) {
  for (const p of players || []) {
    if (!p?.dg_id) continue;
    const gid = matchGolferId(p.player_name);
    if (gid) {
      DGID_TO_GID.set(p.dg_id, gid);
      GID_TO_DGID.set(gid, p.dg_id);
      GID_TO_NAME.set(gid, prettyName(p.player_name));
    }
  }
}

function recordRoundSnapshot(majorId, currentRound, players) {
  // Always hydrate the reverse maps first — needed by /api/stats regardless
  // of whether we have a valid round number to snapshot under.
  hydrateNameMaps(players);

  if (!currentRound || currentRound < 1 || currentRound > 4) return;
  SNAPSHOTS[majorId] = SNAPSHOTS[majorId] || {};
  const store = SNAPSHOTS[majorId];

  for (const p of players || []) {
    if (!p?.dg_id) continue;
    store[p.dg_id] = store[p.dg_id] || {};
    store[p.dg_id][currentRound] = {
      projMoney: p.proj_money ?? null,
      finalMoney: p.final_money ?? null,
      score:     typeof p.total === "number" ? p.total : null,
      position:  typeof p.position === "number"
                 ? (p.tied ? `T${p.position}` : `${p.position}`)
                 : (p.position || ""),
      status:    p.made_cut !== false ? "made_cut" : "mc",
      ts:        Date.now(),
    };
  }
}

// In-play has a different field shape than live-tournament-stats:
//   p.current_score, p.current_pos, p.prize_money_projected, p.R1..p.R4
function recordRoundSnapshotInPlay(majorId, currentRound, players) {
  hydrateNameMaps(players);

  if (!currentRound || currentRound < 1 || currentRound > 4) return;
  SNAPSHOTS[majorId] = SNAPSHOTS[majorId] || {};
  const store = SNAPSHOTS[majorId];

  for (const p of players || []) {
    if (!p?.dg_id) continue;
    store[p.dg_id] = store[p.dg_id] || {};
    const pos = typeof p.current_pos === "number" ? `${p.current_pos}` : (p.current_pos || "");
    const snap = {
      projMoney:  p.prize_money_projected ?? null,
      finalMoney: p.final_money ?? null,
      score:      typeof p.current_score === "number" ? p.current_score : null,
      position:   pos,
      status:     pos === "MC" || pos === "CUT" ? "mc" : "made_cut",
      ts:         Date.now(),
    };
    store[p.dg_id][currentRound] = snap;
    // Write through to Postgres (fire-and-forget). On Railway restart, the
    // snapshot store is rehydrated from DB so round progression is preserved.
    persistSnapshot(majorId, p.dg_id, currentRound, snap);
  }
}

function getPlayerRounds(majorId, dgId) {
  const store = SNAPSHOTS[majorId]?.[dgId];
  if (!store) return { r1: null, r2: null, r3: null, r4: null };
  const m = (r) => store[r]?.finalMoney ?? store[r]?.projMoney ?? null;
  return { r1: m(1), r2: m(2), r3: m(3), r4: m(4) };
}

// ─────────────────────────────────────────────────────────────
// Historical-round backfill
// ─────────────────────────────────────────────────────────────
// DataGolf's live-tournament-stats accepts round=1..4 for per-round views.
// We use that to retroactively populate score/position into SNAPSHOTS for
// rounds the server missed (e.g. if the server came up mid-tournament).
// Projected money is NOT available historically — it's a live snapshot —
// so r1/r2/r3 money stays null. Only score + position can be recovered.
// ─────────────────────────────────────────────────────────────
const BACKFILLED = new Set();   // "<majorId>:<round>" markers

async function backfillRoundsFromDG(majorId, rounds = [1, 2, 3]) {
  const summary = {};
  for (const round of rounds) {
    const marker = `${majorId}:${round}`;
    if (BACKFILLED.has(marker)) {
      summary[`r${round}`] = "already_backfilled";
      continue;
    }
    try {
      const data = await fetchDG("/preds/live-tournament-stats", {
        stats: "sg_total",
        display: "value",
        round: String(round),
      });
      const players = Array.isArray(data?.live_stats) ? data.live_stats : [];
      let count = 0;
      for (const p of players) {
        if (!p.dg_id) continue;
        SNAPSHOTS[majorId] = SNAPSHOTS[majorId] || {};
        SNAPSHOTS[majorId][p.dg_id] = SNAPSHOTS[majorId][p.dg_id] || {};
        // Don't overwrite a live snapshot — only fill empty slots.
        if (SNAPSHOTS[majorId][p.dg_id][round]) continue;
        const pos = typeof p.position === "number"
                  ? (p.tied ? `T${p.position}` : `${p.position}`)
                  : (p.position || "");
        SNAPSHOTS[majorId][p.dg_id][round] = {
          projMoney: null,      // not historically recoverable
          finalMoney: null,
          score:     typeof p.total === "number" ? p.total : null,
          position:  pos,
          status:    p.made_cut !== false ? "made_cut" : "mc",
          backfilled: true,
          ts: Date.now(),
        };
        count++;
        // Hydrate reverse maps from backfill players too.
        const gid = matchGolferId(p.player_name);
        if (gid) {
          DGID_TO_GID.set(p.dg_id, gid);
          GID_TO_DGID.set(gid, p.dg_id);
          GID_TO_NAME.set(gid, prettyName(p.player_name));
        }
      }
      BACKFILLED.add(marker);
      summary[`r${round}`] = `filled ${count} players`;
    } catch (e) {
      summary[`r${round}_error`] = e.message;
    }
  }
  return summary;
}

// ─────────────────────────────────────────────────────────────
// Routes
// ─────────────────────────────────────────────────────────────
app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    cached_keys: [...cache.keys()],
    snapshot_majors: Object.keys(SNAPSHOTS),
    snapshot_player_count: Object.fromEntries(
      Object.entries(SNAPSHOTS).map(([m, players]) => [m, Object.keys(players).length])
    ),
  });
});

app.get("/api/leaderboard/:majorId", async (req, res) => {
  const majorId = req.params.majorId;
  const eventId = DG_EVENT_IDS[majorId];
  if (!eventId) return res.status(404).json({ error: `unknown major: ${majorId}` });

  try {
    // /preds/in-play returns: current_pos, current_score, thru,
    // R1, R2, R3, R4 round scores, prize_money_projected, make_cut probabilities.
    // This is the right endpoint for "leaderboard with projected money".
    const data = await cached(`lb:${majorId}`, CACHE_TTL, () =>
      fetchDG("/preds/in-play", { tour: "pga", dead_heat: "yes", odds_format: "percent" })
    );

    const players = Array.isArray(data?.data) ? data.data
                  : Array.isArray(data) ? data : [];
    const currentRound = data?.current_round || data?.round || null;

    // Hydrate the dg_id ↔ short id reverse maps + record round-money snapshot.
    recordRoundSnapshotInPlay(majorId, currentRound, players);

    const golfers = {};
    for (const p of players) {
      const matchedGid = matchGolferId(p.player_name);
      const gid = matchedGid || `dg-${p.dg_id || NORMALIZE(p.player_name)}`;
      const made = p.make_cut !== 0 && p.current_pos !== "MC" && p.current_pos !== "CUT";
      const pos  = typeof p.current_pos === "number"
                 ? `${p.current_pos}`
                 : (p.current_pos || "");
      const moneyRounds = p.dg_id ? getPlayerRounds(majorId, p.dg_id) : { r1: null, r2: null, r3: null, r4: null };

      golfers[gid] = {
        matched:     !!matchedGid,
        name:        prettyName(p.player_name || ""),
        country:     p.country || "",
        dg_id:       p.dg_id || null,
        position:    pos,
        score:       typeof p.current_score === "number" ? p.current_score : null,
        thru:        p.thru ?? "",
        status:      made ? "made_cut" : "mc",
        projMoney:   p.prize_money_projected ?? p.proj_money ?? null,
        finalMoney:  p.final_money ?? null,
        // Per-round projected money — money we've snapshotted as the tournament
        // progressed. r1/r2/r3 only populate if we were online when each round ended.
        r1: moneyRounds.r1,
        r2: moneyRounds.r2,
        r3: moneyRounds.r3,
        r4: moneyRounds.r4,
        // Round-by-round STROKE scores from DataGolf — always available
        // historically. Frontend can use these in the stats panel even when
        // money snapshots are missing.
        r1Score: p.R1 ?? p.r1 ?? null,
        r2Score: p.R2 ?? p.r2 ?? null,
        r3Score: p.R3 ?? p.r3 ?? null,
        r4Score: p.R4 ?? p.r4 ?? null,
      };
    }
    res.json({
      tournamentName: data?.event_name || null,
      currentRound,
      lastUpdated:    data?.last_updated || null,
      golfers,
    });
  } catch (err) {
    console.error(`leaderboard ${majorId}:`, err.message);
    res.status(502).json({ error: err.message });
  }
});

// ─────────────────────────────────────────────────────────────
// /api/stats/:majorId/:golferId
// Returns: tournament SG breakdown + round-by-round + traditional + season
// ─────────────────────────────────────────────────────────────
app.get("/api/stats/:majorId/:golferId", async (req, res) => {
  const { majorId, golferId } = req.params;
  if (!DG_EVENT_IDS[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });

  try {
    // Resolve golferId → dg_id. Accept either a short id ("scottie") or a
    // synthetic "dg-12345" id forwarded straight from the frontend.
    let dgId = null;
    if (golferId.startsWith("dg-")) {
      dgId = parseInt(golferId.slice(3), 10) || null;
    } else {
      dgId = GID_TO_DGID.get(golferId) || null;
    }

    // Hydrate the reverse maps from in-play. Same cache key as /api/leaderboard
    // since they share the underlying DataGolf call.
    const lbData = await cached(`lb:${majorId}`, CACHE_TTL, () =>
      fetchDG("/preds/in-play", { tour: "pga", dead_heat: "yes", odds_format: "percent" })
    );
    const lbPlayers = Array.isArray(lbData?.data) ? lbData.data
                    : Array.isArray(lbData) ? lbData : [];
    recordRoundSnapshotInPlay(majorId, lbData?.current_round || lbData?.round, lbPlayers);
    if (!dgId) dgId = GID_TO_DGID.get(golferId) || null;
    if (!dgId) return res.status(404).json({ error: `no dg_id resolved for ${golferId}` });

    // Pull this player's row from in-play for native round-by-round scores.
    const inPlayPlayer = lbPlayers.find(p => p.dg_id === dgId) || {};

    // 1. Tournament SG breakdown + traditional stats (event_avg).
    const sgData = await cached(`sg:${majorId}`, STATS_TTL, () =>
      fetchDG("/preds/live-tournament-stats", {
        stats: "sg_putt,sg_arg,sg_app,sg_ott,sg_t2g,sg_total,distance,accuracy,gir,prox_fw,prox_rgh,scrambling",
        display: "value",
        round: "event_avg",
      })
    );
    const sgList = Array.isArray(sgData?.live_stats) ? sgData.live_stats : [];
    const sgPlayer = sgList.find(p => p.dg_id === dgId) || {};

    // 2. Round-by-round — prefer native R1..R4 from in-play (always available
    // historically), augment with money snapshots when we have them.
    const roundSnaps = SNAPSHOTS[majorId]?.[dgId] || {};
    const roundScoreFromInPlay = (r) =>
      inPlayPlayer[`R${r}`] ?? inPlayPlayer[`r${r}`] ?? null;
    const rounds = [1, 2, 3, 4].map(r => {
      const snap = roundSnaps[r];
      return {
        round: r,
        roundScore: roundScoreFromInPlay(r),                          // strokes for this round
        scoreToPar: snap?.score ?? null,                              // cumulative ToPar after this round
        position:   snap?.position ?? null,                           // pos at end of round
        money:      snap?.finalMoney ?? snap?.projMoney ?? null,      // proj money at end of round
        status:     snap?.status ?? null,
      };
    });

    // 3. Season-long skill decomposition (cached aggressively).
    let season = null;
    try {
      const seasonData = await cached(`season:pga`, SEASON_TTL, () =>
        fetchDG("/preds/skill-decompositions", { tour: "pga", display: "value" })
      );
      const arr = Array.isArray(seasonData?.players) ? seasonData.players
                : Array.isArray(seasonData) ? seasonData : [];
      const sp = arr.find(p => p.dg_id === dgId);
      if (sp) {
        season = {
          sgTotal: sp.true_sg_total ?? sp.sg_total ?? null,
          sgOtt:   sp.true_sg_ott   ?? sp.sg_ott   ?? null,
          sgApp:   sp.true_sg_app   ?? sp.sg_app   ?? null,
          sgArg:   sp.true_sg_arg   ?? sp.sg_arg   ?? null,
          sgPutt:  sp.true_sg_putt  ?? sp.sg_putt  ?? null,
          driving: sp.driving_dist  ?? null,
          accuracy: sp.driving_acc  ?? null,
        };
      }
    } catch (e) {
      // Season endpoint may not be in user's subscription tier — fall through.
      console.warn("season endpoint:", e.message);
    }

    res.json({
      golferId,
      dgId,
      name: GID_TO_NAME.get(golferId) || prettyName(sgPlayer.player_name || ""),
      country: sgPlayer.country || "",
      tournament: {
        sg: {
          total: sgPlayer.sg_total ?? null,
          ott:   sgPlayer.sg_ott   ?? null,
          app:   sgPlayer.sg_app   ?? null,
          arg:   sgPlayer.sg_arg   ?? null,
          putt:  sgPlayer.sg_putt  ?? null,
          t2g:   sgPlayer.sg_t2g   ?? null,
        },
        traditional: {
          drivingDistance: sgPlayer.distance ?? null,
          drivingAccuracy: sgPlayer.accuracy ?? null,
          gir:             sgPlayer.gir      ?? null,
          scrambling:      sgPlayer.scrambling ?? null,
          proxFw:          sgPlayer.prox_fw  ?? null,
          proxRgh:         sgPlayer.prox_rgh ?? null,
        },
        rounds,
        currentRound: sgData?.current_round || null,
        lastUpdated:  sgData?.last_updated  || null,
      },
      season,
    });
  } catch (err) {
    console.error(`stats ${majorId}/${golferId}:`, err.message);
    res.status(502).json({ error: err.message });
  }
});

// Manual backfill trigger — useful for one-shot use:
//   curl https://onlymajors-production.up.railway.app/api/backfill/pga
// Returns a per-round summary of how many players we filled.
app.get("/api/backfill/:majorId", async (req, res) => {
  const majorId = req.params.majorId;
  if (!DG_EVENT_IDS[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  // Optional ?rounds=1,2,3 — defaults to 1,2,3.
  const rounds = (req.query.rounds || "1,2,3")
    .split(",")
    .map(s => parseInt(s.trim(), 10))
    .filter(r => r >= 1 && r <= 4);
  try {
    const summary = await backfillRoundsFromDG(majorId, rounds);
    res.json({ ok: true, majorId, rounds, summary });
  } catch (err) {
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

// ─────────────────────────────────────────────────────────────
// Persistence routes — picks, chat, profiles
// All require Postgres. If pool is null they return 503.
// ─────────────────────────────────────────────────────────────
function requireDb(res) {
  if (!pool) {
    res.status(503).json({ error: "database not configured" });
    return false;
  }
  return true;
}

// GET /api/picks                       → all picks for all teams + all majors
// GET /api/picks/:teamId               → all majors for one team
// PUT /api/picks/:teamId/:majorId      → set/replace one slot
//   body: { starters: [], bench: [], subs: [], submitted: bool }
app.get("/api/picks", async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const { rows } = await pool.query(`SELECT * FROM picks`);
    // Reshape into nested form the frontend expects:
    //   { [teamId]: { [majorId]: { starters, bench, subs, submitted } } }
    const out = {};
    for (const r of rows) {
      out[r.team_id] = out[r.team_id] || {};
      out[r.team_id][r.major_id] = {
        starters:  r.starters  || [],
        bench:     r.bench     || [],
        subs:      r.subs      || [],
        submitted: r.submitted,
      };
    }
    res.json(out);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put("/api/picks/:teamId/:majorId", async (req, res) => {
  if (!requireDb(res)) return;
  const { teamId, majorId } = req.params;
  const { starters = [], bench = [], subs = [], submitted = false } = req.body || {};
  try {
    await pool.query(
      `INSERT INTO picks (team_id, major_id, starters, bench, subs, submitted, updated_at)
       VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6, NOW())
       ON CONFLICT (team_id, major_id)
       DO UPDATE SET starters = EXCLUDED.starters,
                     bench    = EXCLUDED.bench,
                     subs     = EXCLUDED.subs,
                     submitted = EXCLUDED.submitted,
                     updated_at = NOW()`,
      [teamId, majorId, JSON.stringify(starters), JSON.stringify(bench), JSON.stringify(subs), submitted]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET  /api/chat                       → last 200 messages, oldest first
// POST /api/chat                       → { teamId, text } → inserts row
app.get("/api/chat", async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const { rows } = await pool.query(
      `SELECT id, team_id AS "teamId", text, ts
         FROM chat_messages
        ORDER BY ts DESC
        LIMIT 200`
    );
    res.json(rows.reverse());   // oldest first for UI
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/chat", async (req, res) => {
  if (!requireDb(res)) return;
  const { teamId, text } = req.body || {};
  if (!teamId || !text || !text.trim()) return res.status(400).json({ error: "teamId and text required" });
  try {
    const ts = Date.now();
    const { rows } = await pool.query(
      `INSERT INTO chat_messages (team_id, text, ts) VALUES ($1, $2, $3)
       RETURNING id, team_id AS "teamId", text, ts`,
      [teamId, text.trim().slice(0, 1000), ts]
    );
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/profiles            → all profiles { [teamId]: { displayName, teamName, email } }
// PUT /api/profiles/:teamId    → upsert profile
app.get("/api/profiles", async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const { rows } = await pool.query(`SELECT team_id, display_name, team_name, email FROM profiles`);
    const out = {};
    for (const r of rows) {
      out[r.team_id] = {
        displayName: r.display_name,
        teamName:    r.team_name,
        email:       r.email,
      };
    }
    res.json(out);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put("/api/profiles/:teamId", async (req, res) => {
  if (!requireDb(res)) return;
  const { teamId } = req.params;
  const { displayName = null, teamName = null, email = null } = req.body || {};
  try {
    await pool.query(
      `INSERT INTO profiles (team_id, display_name, team_name, email, updated_at)
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT (team_id)
       DO UPDATE SET display_name = EXCLUDED.display_name,
                     team_name    = EXCLUDED.team_name,
                     email        = EXCLUDED.email,
                     updated_at   = NOW()`,
      [teamId, displayName, teamName, email]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─────────────────────────────────────────────────────────────
// Auth routes — signup, login, logout, /me, /leagues/mine
// ─────────────────────────────────────────────────────────────
// Helper: return current user + their league memberships
async function meAndLeagues(userId) {
  const userRow = (await pool.query(
    `SELECT id, email, display_name FROM users WHERE id = $1`, [userId]
  )).rows[0];
  const leagueRows = (await pool.query(
    `SELECT l.id, l.name, l.invite_code, l.format, l.scope, l.major_id,
            lm.team_id, lm.team_name, lm.team_color, lm.role
       FROM league_members lm
       JOIN leagues l ON l.id = lm.league_id
      WHERE lm.user_id = $1
      ORDER BY lm.joined_at ASC`,
    [userId]
  )).rows;
  return {
    user: {
      id:          Number(userRow.id),
      email:       userRow.email,
      displayName: userRow.display_name,
    },
    leagues: leagueRows.map(r => ({
      id:        Number(r.id),
      name:      r.name,
      inviteCode: r.invite_code,
      format:    r.format,
      scope:     r.scope,
      majorId:   r.major_id,
      teamId:    r.team_id,
      teamName:  r.team_name,
      teamColor: r.team_color,
      role:      r.role,
    })),
  };
}

// POST /api/auth/signup
//   body: { email, password, displayName, teamId, leagueCode? }
//   - creates user
//   - joins league by code (defaults to "EXPERTS") and claims the given team
//   - returns { token, user, leagues }
app.post("/api/auth/signup", async (req, res) => {
  if (!requireDb(res)) return;
  const { email, password, displayName, teamId, leagueCode = "EXPERTS" } = req.body || {};
  if (!isValidEmail(email))            return res.status(400).json({ error: "invalid email" });
  if (!password || password.length < 6) return res.status(400).json({ error: "password must be at least 6 characters" });
  if (!displayName || !displayName.trim()) return res.status(400).json({ error: "displayName required" });
  if (!teamId)                          return res.status(400).json({ error: "teamId required" });

  const normalizedEmail = email.trim().toLowerCase();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    // Ensure email isn't already taken.
    const exists = await client.query(`SELECT 1 FROM users WHERE email = $1`, [normalizedEmail]);
    if (exists.rows.length) {
      await client.query("ROLLBACK");
      return res.status(409).json({ error: "email already registered" });
    }
    // Resolve league.
    const lr = await client.query(`SELECT id FROM leagues WHERE invite_code = $1`, [leagueCode]);
    if (!lr.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: `unknown league code: ${leagueCode}` });
    }
    const leagueId = Number(lr.rows[0].id);
    // Check team slot is unclaimed.
    const slot = await client.query(
      `SELECT id, user_id FROM league_members WHERE league_id = $1 AND team_id = $2`,
      [leagueId, teamId]
    );
    if (!slot.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: `team ${teamId} not in league ${leagueCode}` });
    }
    if (slot.rows[0].user_id) {
      await client.query("ROLLBACK");
      return res.status(409).json({ error: `team ${teamId} already claimed` });
    }
    // Create user.
    const u = await client.query(
      `INSERT INTO users (email, password_hash, display_name)
       VALUES ($1, $2, $3) RETURNING id`,
      [normalizedEmail, hashPassword(password), displayName.trim()]
    );
    const userId = Number(u.rows[0].id);
    // Claim the team slot.
    await client.query(
      `UPDATE league_members SET user_id = $1 WHERE id = $2`,
      [userId, slot.rows[0].id]
    );
    // Create session.
    const token = newSessionToken();
    const expires = new Date(Date.now() + SESSION_TTL_DAYS * 24 * 60 * 60 * 1000);
    await client.query(
      `INSERT INTO sessions (token, user_id, expires_at) VALUES ($1, $2, $3)`,
      [token, userId, expires]
    );
    await client.query("COMMIT");
    const payload = await meAndLeagues(userId);
    res.json({ token, ...payload });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("signup failed:", err.message);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// POST /api/auth/login  { email, password } → { token, user, leagues }
app.post("/api/auth/login", async (req, res) => {
  if (!requireDb(res)) return;
  const { email, password } = req.body || {};
  if (!isValidEmail(email) || !password) return res.status(400).json({ error: "email + password required" });
  try {
    const { rows } = await pool.query(
      `SELECT id, password_hash FROM users WHERE email = $1`,
      [email.trim().toLowerCase()]
    );
    if (!rows.length || !verifyPassword(password, rows[0].password_hash)) {
      return res.status(401).json({ error: "incorrect email or password" });
    }
    const userId = Number(rows[0].id);
    const token = newSessionToken();
    const expires = new Date(Date.now() + SESSION_TTL_DAYS * 24 * 60 * 60 * 1000);
    await pool.query(
      `INSERT INTO sessions (token, user_id, expires_at) VALUES ($1, $2, $3)`,
      [token, userId, expires]
    );
    const payload = await meAndLeagues(userId);
    res.json({ token, ...payload });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/auth/logout — invalidates the current session
app.post("/api/auth/logout", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  try {
    await pool.query(`DELETE FROM sessions WHERE token = $1`, [req.token]);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/me  → { user, leagues }
app.get("/api/me", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const payload = await meAndLeagues(req.user.id);
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/leagues/:leagueId/members — public list of who's in which team slot
app.get("/api/leagues/:leagueId/members", async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const { rows } = await pool.query(
      `SELECT lm.team_id, lm.team_name, lm.team_color, lm.user_id,
              u.display_name, u.email
         FROM league_members lm
         LEFT JOIN users u ON u.id = lm.user_id
        WHERE lm.league_id = $1
        ORDER BY lm.joined_at ASC`,
      [req.params.leagueId]
    );
    res.json(rows.map(r => ({
      teamId:       r.team_id,
      teamName:     r.team_name,
      teamColor:    r.team_color,
      claimed:      r.user_id != null,
      displayName:  r.display_name,
      email:        r.email,
    })));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, async () => {
  console.log(`✓  OnlyMajors backend listening on :${PORT}`);
  console.log(`   GET    /api/leaderboard/:majorId`);
  console.log(`   GET    /api/stats/:majorId/:golferId`);
  console.log(`   GET    /api/backfill/:majorId`);
  console.log(`   GET    /api/field/:majorId`);
  console.log(`   GET    /api/picks    PUT /api/picks/:teamId/:majorId`);
  console.log(`   GET    /api/chat     POST /api/chat`);
  console.log(`   GET    /api/profiles PUT  /api/profiles/:teamId`);
  console.log(`   POST   /api/auth/signup  /api/auth/login  /api/auth/logout`);
  console.log(`   GET    /api/me  /api/leagues/:id/members`);
  console.log(`   GET    /api/health`);
  console.log(`   CORS allowed: ${ALLOWED_ORIGINS.join(", ")}`);
  // Wait for the schema to be ready, then rehydrate snapshots from DB.
  // initSchema started at boot; this just gives it a moment if it's racing.
  setTimeout(() => loadSnapshotsFromDB(), 1000);
});
