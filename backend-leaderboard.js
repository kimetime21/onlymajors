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

const PORT      = process.env.PORT || 3001;
const DG_KEY    = process.env.DATAGOLF_API_KEY;
const DG_BASE   = "https://feeds.datagolf.com";
const CACHE_TTL = 60_000;          // 60 seconds — be kind to the DataGolf API
const STATS_TTL = 90_000;          // stats are slower-moving
const SEASON_TTL = 6 * 60 * 60_000; // 6 hours
const ALLOWED_ORIGINS = (process.env.CORS_ORIGINS || "https://onlymajors.com,https://www.onlymajors.com").split(",");

if (!DG_KEY) {
  console.error("✖  DATAGOLF_API_KEY env var not set — exiting");
  process.exit(1);
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

function recordRoundSnapshot(majorId, currentRound, players) {
  if (!currentRound || currentRound < 1 || currentRound > 4) return;
  SNAPSHOTS[majorId] = SNAPSHOTS[majorId] || {};
  const store = SNAPSHOTS[majorId];

  for (const p of players) {
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

    // Maintain dg_id ↔ short id reverse map for the stats endpoint.
    const gid = matchGolferId(p.player_name);
    if (gid) {
      DGID_TO_GID.set(p.dg_id, gid);
      GID_TO_DGID.set(gid, p.dg_id);
      GID_TO_NAME.set(gid, prettyName(p.player_name));
    }
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
    const data = await cached(`lb:${majorId}`, CACHE_TTL, () =>
      fetchDG("/preds/live-tournament-stats", { stats: "sg_total", display: "value" })
    );

    const players = Array.isArray(data?.live_stats) ? data.live_stats
                  : Array.isArray(data) ? data : [];
    const currentRound = data?.current_round || null;

    // Record snapshot for the current round on every fetch. Each subsequent
    // write to the same (round, player) overwrites — so the last value
    // before the round increments is the end-of-round value.
    recordRoundSnapshot(majorId, currentRound, players);

    // If the tournament is mid- or late-round and we never captured the
    // earlier rounds, retroactively pull score+position from DataGolf for
    // R1..(currentRound-1). Fire-and-forget — don't block this response.
    if (currentRound && currentRound > 1) {
      const need = [];
      for (let r = 1; r < currentRound; r++) {
        if (!BACKFILLED.has(`${majorId}:${r}`)) need.push(r);
      }
      if (need.length) {
        backfillRoundsFromDG(majorId, need)
          .then(s => console.log(`auto-backfill ${majorId}:`, s))
          .catch(e => console.warn(`auto-backfill ${majorId} failed:`, e.message));
      }
    }

    const golfers = {};
    for (const p of players) {
      const matchedGid = matchGolferId(p.player_name);
      const gid = matchedGid || `dg-${p.dg_id || NORMALIZE(p.player_name)}`;
      const made = p.made_cut !== false;
      const pos  = (typeof p.position === "number")
                 ? (p.tied ? `T${p.position}` : `${p.position}`)
                 : (p.position || "");
      const rounds = p.dg_id ? getPlayerRounds(majorId, p.dg_id) : { r1: null, r2: null, r3: null, r4: null };

      golfers[gid] = {
        matched:     !!matchedGid,
        name:        prettyName(p.player_name || ""),
        country:     p.country || "",
        dg_id:       p.dg_id || null,
        position:    pos,
        score:       typeof p.total === "number" ? p.total : null,
        thru:        p.thru ?? "",
        status:      made ? "made_cut" : "mc",
        projMoney:   p.proj_money  ?? null,
        finalMoney:  p.final_money ?? null,
        // Per-round projected money snapshots — populated as the tournament
        // progresses. Frontend merges these into EARN[majorId][gid].r1..r4.
        r1: rounds.r1,
        r2: rounds.r2,
        r3: rounds.r3,
        r4: rounds.r4,
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

    // Trigger a leaderboard fetch so SNAPSHOTS + reverse maps are warm.
    // Hydrate UNCONDITIONALLY (cache hit or miss) — the prior implementation
    // only hydrated inside the .then() which never ran on cache hit, so a
    // server restart followed by an unrelated /api/stats call would 404.
    const lbData = await cached(`lb:${majorId}`, CACHE_TTL, () =>
      fetchDG("/preds/live-tournament-stats", { stats: "sg_total", display: "value" })
    );
    recordRoundSnapshot(
      majorId,
      lbData?.current_round,
      Array.isArray(lbData?.live_stats) ? lbData.live_stats : []
    );
    if (!dgId) dgId = GID_TO_DGID.get(golferId) || null;
    if (!dgId) return res.status(404).json({ error: `no dg_id resolved for ${golferId}` });

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

    // 2. Round-by-round from our snapshot store (positions, scores, money).
    const roundSnaps = SNAPSHOTS[majorId]?.[dgId] || {};
    const rounds = [1, 2, 3, 4].map(r => {
      const snap = roundSnaps[r];
      if (!snap) return { round: r, score: null, scoreToPar: null, position: null, money: null };
      return {
        round: r,
        scoreToPar: snap.score,           // cumulative score-to-par at end of round
        position:   snap.position,
        money:      snap.finalMoney ?? snap.projMoney ?? null,
        status:     snap.status,
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

app.listen(PORT, () => {
  console.log(`✓  OnlyMajors backend listening on :${PORT}`);
  console.log(`   GET /api/leaderboard/:majorId`);
  console.log(`   GET /api/stats/:majorId/:golferId`);
  console.log(`   GET /api/backfill/:majorId`);
  console.log(`   GET /api/field/:majorId`);
  console.log(`   GET /api/health`);
  console.log(`   CORS allowed: ${ALLOWED_ORIGINS.join(", ")}`);
});
