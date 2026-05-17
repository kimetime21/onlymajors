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

const PORT      = process.env.PORT || 3001;
const DG_KEY    = process.env.DATAGOLF_API_KEY;
const DG_BASE   = "https://feeds.datagolf.com";
const CACHE_TTL = 60_000;          // 60 seconds — be kind to the DataGolf API
const STATS_TTL = 90_000;          // stats are slower-moving
const SEASON_TTL = 6 * 60 * 60_000; // 6 hours
const ALLOWED_ORIGINS = (process.env.CORS_ORIGINS || "https://onlymajors.com,https://www.onlymajors.com").split(",");
const DATABASE_URL = process.env.DATABASE_URL;

if (!DG_KEY) {
  console.error("✖  DATAGOLF_API_KEY env var not set — exiting");
  process.exit(1);
}

// ─────────────────────────────────────────────────────────────
// Postgres pool + schema init
// If DATABASE_URL is unset we fall back to in-memory state.
// ─────────────────────────────────────────────────────────────
const pool = DATABASE_URL
  ? new pg.Pool({
      connectionString: DATABASE_URL,
      ssl: DATABASE_URL.includes("railway") ? { rejectUnauthorized: false } : false,
      max: 4,
    })
  : null;

async function initSchema() {
  if (!pool) {
    console.warn("⚠  DATABASE_URL not set — running with in-memory state only");
    return;
  }
  await pool.query(`
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
  console.log("✓  Postgres schema ready");
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
    if (!origin)                           return callback(null, true);
    if (ALLOWED_ORIGINS.includes(origin))  return callback(null, true);
    callback(new Error(`CORS: ${origin} not in allow-list`));
  },
}));
app.use(express.json());

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

app.listen(PORT, async () => {
  console.log(`✓  OnlyMajors backend listening on :${PORT}`);
  console.log(`   GET    /api/leaderboard/:majorId`);
  console.log(`   GET    /api/stats/:majorId/:golferId`);
  console.log(`   GET    /api/backfill/:majorId`);
  console.log(`   GET    /api/field/:majorId`);
  console.log(`   GET    /api/picks    PUT /api/picks/:teamId/:majorId`);
  console.log(`   GET    /api/chat     POST /api/chat`);
  console.log(`   GET    /api/profiles PUT  /api/profiles/:teamId`);
  console.log(`   GET    /api/health`);
  console.log(`   CORS allowed: ${ALLOWED_ORIGINS.join(", ")}`);
  // Wait for the schema to be ready, then rehydrate snapshots from DB.
  // initSchema started at boot; this just gives it a moment if it's racing.
  setTimeout(() => loadSnapshotsFromDB(), 1000);
});
