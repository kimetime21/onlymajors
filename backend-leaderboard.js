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

    -- Season archives — historical per-year per-league snapshots, ingested
    -- from PDFs / spreadsheets from past seasons. JSONB blob covers champion,
    -- per-major winners, per-team totals & trophies. Schema is intentionally
    -- loose so we can iterate the shape without DB migrations.
    CREATE TABLE IF NOT EXISTS season_archives (
      league_id   BIGINT NOT NULL REFERENCES leagues(id) ON DELETE CASCADE,
      year        INT NOT NULL,
      data        JSONB NOT NULL,
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (league_id, year)
    );
  `);

  // SEED the default league FIRST so existing-row backfill below has a valid
  // FK target (league 1) when we add the NOT NULL DEFAULT 1 column to picks.
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

  // Now add the league_id columns. Existing rows backfill to 1, which is a
  // valid FK target now that league 1 exists.
  await pool.query(`
    ALTER TABLE picks
      ADD COLUMN IF NOT EXISTS league_id BIGINT NOT NULL DEFAULT 1
        REFERENCES leagues(id) ON DELETE CASCADE;
    ALTER TABLE chat_messages
      ADD COLUMN IF NOT EXISTS league_id BIGINT NOT NULL DEFAULT 1
        REFERENCES leagues(id) ON DELETE CASCADE;
    ALTER TABLE round_snapshots
      ADD COLUMN IF NOT EXISTS round_score NUMERIC;
    ALTER TABLE picks
      ADD COLUMN IF NOT EXISTS score_prediction INTEGER;
    ALTER TABLE leagues
      ADD COLUMN IF NOT EXISTS member_model TEXT NOT NULL DEFAULT 'open';
    ALTER TABLE leagues
      ADD COLUMN IF NOT EXISTS visibility TEXT NOT NULL DEFAULT 'public';
    ALTER TABLE leagues
      ADD COLUMN IF NOT EXISTS commissioner_id BIGINT REFERENCES users(id) ON DELETE SET NULL;
    ALTER TABLE leagues
      ADD COLUMN IF NOT EXISTS league_type TEXT NOT NULL DEFAULT 'all_four';
    ALTER TABLE leagues
      ADD COLUMN IF NOT EXISTS included_majors JSONB NOT NULL DEFAULT '["masters","pga","usopen","open"]'::jsonb;
  `);
  // EXPERTS league keeps the legacy pre-allocated 5-slot model. Everything
  // else defaults to 'open' (no preset slots, members auto-added on join).
  await pool.query(`UPDATE leagues SET member_model = 'slots' WHERE id = $1`, [1]);

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

  console.log("✓  Postgres schema ready · default league seeded");
  await seedHistoricalArchives();
}

// One-time seed of the EXPERTS league's historical season archives, sourced
// from the league's PDFs. Inserts only the (league_id, year) rows that don't
// already exist, so this is safe to run on every boot.
async function seedHistoricalArchives() {
  if (!pool) return;
  const ARCHIVES = {
    2022: {
      champion: { teamId: "boo", teamName: "Team Boo", total: 11668618 },
      majorWinners: {
        masters: { teamId: "boo",   teamName: "Team Boo",   earnings: 3810000 },
        pga:     { teamId: "larry", teamName: "Team Larry", earnings: 3434189 },
        usopen:  { teamId: "boo",   teamName: "Team Boo",   earnings: 3497058 },
        open:    { teamId: "caleb", teamName: "Team Caleb", earnings: 2831489 },
      },
      topEarner: { teamName: "Team Boo", majorShort: "Masters", amount: 3810000 },
      teams: [
        { teamId: "boo",    teamName: "Team Boo",    rank: 1, total: 11668618, byMajor: { masters: 3810000, pga: 2029643, usopen: 3497058, open: 2331917 } },
        { teamId: "caleb",  teamName: "Team Caleb",  rank: 2, total:  7726213, byMajor: { masters:  675562, pga: 2919300, usopen: 1299862, open: 2831489 } },
        { teamId: "larry",  teamName: "Team Larry",  rank: 3, total:  7535255, byMajor: { masters:  675750, pga: 3434189, usopen: 2715244, open:  710072 } },
        { teamId: "thorne", teamName: "Team Thorne", rank: 4, total:  4376285, byMajor: { masters:  550350, pga: 1945514, usopen: 1148599, open:  731822 } },
        { teamId: "austin", teamName: "Team Austin", rank: 5, total:  2848161, byMajor: { masters:  336333, pga:  817939, usopen: 1065910, open:  627979 } },
      ],
    },
    2023: {
      champion: { teamId: "thorne", teamName: "Team Thorne", total: 10516466 },
      majorWinners: {
        masters: { teamId: "thorne", teamName: "Team Thorne", earnings: 4749000 },
        pga:     { teamId: "caleb",  teamName: "Team Caleb",  earnings: 3869750 },
        usopen:  { teamId: "austin", teamName: "Team Austin", earnings: 2336778 },
        open:    { teamId: "boo",    teamName: "Team Boo",    earnings: 3163067 },
      },
      topEarner: { teamName: "Team Thorne", majorShort: "Masters", amount: 4749000 },
      teams: [
        { teamId: "thorne", teamName: "Team Thorne", rank: 1, total: 10516466, byMajor: { masters: 4749000, pga: 3425900, usopen: 1689873, open:  651693 } },
        { teamId: "caleb",  teamName: "Team Caleb",  rank: 2, total:  7146333, byMajor: { masters: 1402200, pga: 3869750, usopen: 1461041, open:  413342 } },
        { teamId: "larry",  teamName: "Team Larry",  rank: 3, total:  6881473, byMajor: { masters: 4081200, pga:  379150, usopen: 1716781, open:  704342 } },
        { teamId: "boo",    teamName: "Team Boo",    rank: 4, total:  5661610, byMajor: { masters: 1054800, pga:  592761, usopen:  850982, open: 3163067 } },
        { teamId: "austin", teamName: "Team Austin", rank: 5, total:  5608167, byMajor: { masters: 1732500, pga:  394672, usopen: 2336778, open: 1144217 } },
      ],
    },
    2024: {
      // Austin took the season — totals reconciled against the final Open
      // numbers (Austin ~$12.8M, Larry ~$12.5M, ~$250k margin).
      champion: { teamId: "austin", teamName: "Team Austin", total: 12812710 },
      majorWinners: {
        masters: { teamId: "thorne", teamName: "Team Thorne", earnings: 4846000 },
        pga:     { teamId: "larry",  teamName: "Team Larry",  earnings: 6159202 },
        usopen:  { teamId: "caleb",  teamName: "Team Caleb",  earnings: 5570337 },
        open:    { teamId: "thorne", teamName: "Team Thorne", earnings: 1286300 },
      },
      topEarner: { teamName: "Team Larry", majorShort: "PGA", amount: 6159202 },
      teams: [
        { teamId: "austin", teamName: "Team Austin", rank: 1, total: 12812710, byMajor: { masters: 4453000, pga: 4684387, usopen: 2858180, open:  817143 } },
        { teamId: "larry",  teamName: "Team Larry",  rank: 2, total: 12560367, byMajor: { masters: 4670900, pga: 6159202, usopen: 1065408, open:  664857 } },
        { teamId: "caleb",  teamName: "Team Caleb",  rank: 3, total: 12351598, byMajor: { masters: 4401400, pga: 1695361, usopen: 5570337, open:  684500 } },
        { teamId: "thorne", teamName: "Team Thorne", rank: 4, total: 10457459, byMajor: { masters: 4846000, pga: 2543088, usopen: 1782071, open: 1286300 } },
        { teamId: "boo",    teamName: "Team Boo",    rank: 5, total:  5832833, byMajor: { masters:  887500, pga:  828525, usopen: 3387408, open:  729400 } },
      ],
    },
    2025: {
      champion: { teamId: "boo", teamName: "Team Boo", total: 9923712 },
      majorWinners: {
        // Larry + Caleb tied at $6,342,000 for the Masters; the PDF highlights
        // Larry as the winner.
        masters: { teamId: "larry",  teamName: "Team Larry",  earnings: 6342000 },
        pga:     { teamId: "boo",    teamName: "Team Boo",    earnings: 5003677 },
        usopen:  { teamId: "caleb",  teamName: "Team Caleb",  earnings: 1429327 },
        open:    { teamId: "thorne", teamName: "Team Thorne", earnings: 1553017 },
      },
      topEarner: { teamName: "Team Larry", majorShort: "Masters", amount: 6342000 },
      teams: [
        { teamId: "boo",    teamName: "Team Boo",    rank: 1, total: 9923712, byMajor: { masters: 2929500, pga: 5003677, usopen:  716351, open: 1274184 } },
        { teamId: "larry",  teamName: "Team Larry",  rank: 2, total: 8932433, byMajor: { masters: 6342000, pga:  531082, usopen:  772910, open: 1286441 } },
        { teamId: "thorne", teamName: "Team Thorne", rank: 3, total: 8245571, byMajor: { masters:  835800, pga: 4838667, usopen: 1018087, open: 1553017 } },
        { teamId: "caleb",  teamName: "Team Caleb",  rank: 4, total: 8153359, byMajor: { masters: 6342000, pga:  156494, usopen: 1429327, open:  225538 } },
        { teamId: "austin", teamName: "Team Austin", rank: 5, total: 7273112, byMajor: { masters: 1218000, pga: 3562835, usopen: 1038001, open: 1454276 } },
      ],
    },
  };

  try {
    const existing = await pool.query(
      `SELECT year FROM season_archives WHERE league_id = $1`,
      [DEFAULT_LEAGUE_ID]
    );
    const have = new Set(existing.rows.map(r => Number(r.year)));
    for (const [yearStr, data] of Object.entries(ARCHIVES)) {
      const year = Number(yearStr);
      if (have.has(year)) continue;
      await pool.query(
        `INSERT INTO season_archives (league_id, year, data) VALUES ($1, $2, $3::jsonb)`,
        [DEFAULT_LEAGUE_ID, year, JSON.stringify(data)]
      );
      console.log(`✓  seeded ${year} archive for EXPERTS league`);
    }
  } catch (err) {
    console.warn("⚠  historical archive seed failed:", err.message);
  }
}

// One-time cleanup: delete any orphan leagues that were created under the
// old code path that didn't set a commissioner AND didn't auto-claim a
// member. EXPERTS (id=1) is always preserved.
async function cleanupOrphanLeagues() {
  if (!pool) return;
  try {
    const r = await pool.query(`
      DELETE FROM leagues
       WHERE commissioner_id IS NULL
         AND id != 1
         AND NOT EXISTS (
           SELECT 1 FROM league_members
            WHERE league_members.league_id = leagues.id
              AND user_id IS NOT NULL
         )
       RETURNING id, name`);
    if (r.rowCount > 0) {
      console.log(`✓  cleaned up ${r.rowCount} orphan league(s): ` +
        r.rows.map(x => `${x.id}=${x.name}`).join(", "));
    }
  } catch (err) {
    console.warn("⚠  orphan league cleanup failed:", err.message);
  }
}

initSchema()
  .then(() => cleanupOrphanLeagues())
  .catch(e => console.error("✖  schema init failed:", e.message));

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
        roundScore: r.round_score == null ? null : Number(r.round_score),
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
      `INSERT INTO round_snapshots (major_id, dg_id, round, proj_money, final_money, score, round_score, position, status, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
       ON CONFLICT (major_id, dg_id, round)
       DO UPDATE SET proj_money = EXCLUDED.proj_money,
                     final_money = EXCLUDED.final_money,
                     score = EXCLUDED.score,
                     round_score = EXCLUDED.round_score,
                     position = EXCLUDED.position,
                     status = EXCLUDED.status,
                     updated_at = NOW()`,
      [majorId, dgId, round, snap.projMoney, snap.finalMoney, snap.score, snap.roundScore ?? null, snap.position, snap.status]
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

// Resolve the team_id the authenticated user owns in the given league.
async function getUserTeamId(userId, leagueId = DEFAULT_LEAGUE_ID) {
  if (!pool) return null;
  const { rows } = await pool.query(
    `SELECT team_id FROM league_members WHERE user_id = $1 AND league_id = $2`,
    [userId, leagueId]
  );
  return rows[0]?.team_id || null;
}

// Extract the user from a request's auth header if a valid token is present,
// otherwise null. Doesn't error like requireAuth — useful for endpoints that
// behave slightly differently for signed-in vs anonymous callers (e.g. league
// creation auto-claims slot 1 for the creator when authed).
async function getOptionalUser(req) {
  if (!pool) return null;
  const auth = req.headers.authorization || "";
  if (!auth.startsWith("Bearer ")) return null;
  const token = auth.slice(7).trim();
  if (!token) return null;
  try {
    const { rows } = await pool.query(
      `SELECT u.id, u.email, u.display_name AS "displayName"
         FROM sessions s
         JOIN users u ON u.id = s.user_id
        WHERE s.token = $1 AND s.expires_at > NOW()`,
      [token]
    );
    return rows[0] || null;
  } catch { return null; }
}

// Pick the right leagueId for a request. Order of resolution:
//   1. explicit ?leagueId=X (or body.leagueId)  — must be one the user belongs to
//   2. user's earliest-joined league             — covers single-league users
//   3. DEFAULT_LEAGUE_ID                          — last-resort fallback
// Returns null if the user has no league memberships at all.
async function resolveLeagueId(userId, requested) {
  if (!pool) return DEFAULT_LEAGUE_ID;
  // List the user's leagues once so we can both validate explicit picks and
  // fall back to "first joined" cleanly.
  const { rows } = await pool.query(
    `SELECT league_id FROM league_members WHERE user_id = $1 ORDER BY joined_at ASC`,
    [userId]
  );
  const memberLeagues = rows.map(r => Number(r.league_id));
  if (requested != null && requested !== "") {
    const id = Number(requested);
    if (Number.isFinite(id) && memberLeagues.includes(id)) return id;
    return null;   // caller will 403/404
  }
  return memberLeagues[0] ?? DEFAULT_LEAGUE_ID;
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
// Par per major (used to recover round-stroke totals from cumulative score
// in the snapshot store when DataGolf only gave us score-to-par).
const MAJOR_PAR = {
  masters: 72,  // Augusta National
  pga:     70,  // Aronimink (2026)
  usopen:  70,  // Shinnecock Hills (2026)
  open:    70,  // Royal Birkdale (2026)
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

  // Capture ALL completed rounds, not just the current one. DataGolf returns
  // R1..R4 strokes on each player, so we can backfill any rounds we missed.
  for (const p of players || []) {
    if (!p?.dg_id) continue;
    store[p.dg_id] = store[p.dg_id] || {};
    const pos = typeof p.current_pos === "number" ? `${p.current_pos}` : (p.current_pos || "");
    const isMC = pos === "MC" || pos === "CUT" || p.make_cut === 0;

    // Snapshot the CURRENT round with everything we know now.
    store[p.dg_id][currentRound] = {
      projMoney:  p.prize_money_projected ?? null,
      finalMoney: p.final_money ?? null,
      score:      typeof p.current_score === "number" ? p.current_score : null,
      roundScore: p[`R${currentRound}`] ?? p[`r${currentRound}`] ?? null,
      position:   pos,
      status:     isMC ? "mc" : "made_cut",
      ts:         Date.now(),
    };
    persistSnapshot(majorId, p.dg_id, currentRound, store[p.dg_id][currentRound]);

    // Backfill stroke totals for any earlier rounds we don't already have.
    for (let r = 1; r < currentRound; r++) {
      const earlierStrokes = p[`R${r}`] ?? p[`r${r}`] ?? null;
      if (earlierStrokes == null) continue;
      const prior = store[p.dg_id][r];
      if (prior && prior.roundScore != null) continue;   // already captured
      store[p.dg_id][r] = {
        ...(prior || {}),
        projMoney:  prior?.projMoney ?? null,
        finalMoney: prior?.finalMoney ?? null,
        score:      prior?.score ?? null,
        roundScore: earlierStrokes,
        position:   prior?.position ?? "",
        status:     prior?.status ?? (isMC && r > currentRound ? "mc" : "made_cut"),
        ts:         Date.now(),
      };
      persistSnapshot(majorId, p.dg_id, r, store[p.dg_id][r]);
    }
  }
}

// Build a leaderboard response purely from SNAPSHOTS — used when DataGolf's
// in-play has moved on to a different tournament but we want to serve the
// final results for a completed major.
function buildSnapshotLeaderboard(majorId) {
  const par = MAJOR_PAR[majorId] ?? 72;
  const snaps = SNAPSHOTS[majorId] || {};
  const golfers = {};
  for (const dgIdStr of Object.keys(snaps)) {
    const dgId = parseInt(dgIdStr, 10);
    const player = snaps[dgId];
    if (!player) continue;
    const latest = player[4] || player[3] || player[2] || player[1];
    if (!latest) continue;

    const gid = DGID_TO_GID.get(dgId) || `dg-${dgId}`;
    const name = GID_TO_NAME.get(gid) || null;
    const moneyAt = (r) => player[r]?.finalMoney ?? player[r]?.projMoney ?? null;
    // Stroke totals: prefer roundScore (DataGolf's R1..R4), else derive from
    // cumulative score-to-par + par. Derivation requires prior round's score.
    const cumulative = (r) => player[r]?.score;
    const strokesAt = (r) => {
      if (player[r]?.roundScore != null) return player[r].roundScore;
      const cur = cumulative(r);
      const prev = r === 1 ? 0 : cumulative(r-1);
      if (cur == null || prev == null) return null;
      return par + (cur - prev);
    };

    golfers[gid] = {
      matched:    !gid.startsWith("dg-"),
      name:       name,
      country:    "",
      dg_id:      dgId,
      position:   latest.position || "",
      score:      latest.score ?? null,
      thru:       "",
      status:     latest.status || "made_cut",
      projMoney:  player[4]?.projMoney ?? null,
      finalMoney: player[4]?.finalMoney ?? player[4]?.projMoney ?? null,
      r1: moneyAt(1), r2: moneyAt(2), r3: moneyAt(3), r4: moneyAt(4),
      r1Score: strokesAt(1), r2Score: strokesAt(2), r3Score: strokesAt(3), r4Score: strokesAt(4),
    };
  }
  return golfers;
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
    // Pull DataGolf's current in-play. Whatever tournament is active right now
    // shows up here — could be this major (during major week) or a totally
    // different PGA Tour event (the rest of the year).
    const data = await cached(`lb:${majorId}`, CACHE_TTL, () =>
      fetchDG("/preds/in-play", { tour: "pga", dead_heat: "yes", odds_format: "percent" })
    );

    const players = Array.isArray(data?.data) ? data.data
                  : Array.isArray(data) ? data : [];
    const currentRound = data?.current_round || data?.round || null;

    // Keep name maps warm regardless of which tournament in-play is showing.
    hydrateNameMaps(players);

    // Detect if DataGolf's current in-play IS this major. Compare its event_id
    // to our expected one. Be permissive about the field name.
    const inPlayEventId = data?.event_id ?? data?.eventId ?? data?.event ?? null;
    const isThisMajorLive = inPlayEventId != null && Number(inPlayEventId) === Number(eventId);

    // Only snapshot when in-play IS this major. Otherwise we'd corrupt this
    // major's final R4 snapshot with a totally different tournament's data.
    if (isThisMajorLive) {
      recordRoundSnapshotInPlay(majorId, currentRound, players);
    }

    let golfers;
    if (isThisMajorLive) {
      // LIVE MODE — build from in-play + snapshot money overlay
      golfers = {};
      for (const p of players) {
        const matchedGid = matchGolferId(p.player_name);
        const gid = matchedGid || `dg-${p.dg_id || NORMALIZE(p.player_name)}`;
        const pos = typeof p.current_pos === "number" ? `${p.current_pos}` : (p.current_pos || "");
        const made = p.make_cut !== 0 && pos !== "MC" && pos !== "CUT";
        const moneyRounds = p.dg_id ? getPlayerRounds(majorId, p.dg_id) : { r1: null, r2: null, r3: null, r4: null };

        golfers[gid] = {
          matched:    !!matchedGid,
          name:       prettyName(p.player_name || ""),
          country:    p.country || "",
          dg_id:      p.dg_id || null,
          position:   pos,
          score:      typeof p.current_score === "number" ? p.current_score : null,
          thru:       p.thru ?? "",
          status:     made ? "made_cut" : "mc",
          projMoney:  p.prize_money_projected ?? p.proj_money ?? null,
          finalMoney: p.final_money ?? null,
          r1: moneyRounds.r1, r2: moneyRounds.r2, r3: moneyRounds.r3, r4: moneyRounds.r4,
          r1Score: p.R1 ?? p.r1 ?? null,
          r2Score: p.R2 ?? p.r2 ?? null,
          r3Score: p.R3 ?? p.r3 ?? null,
          r4Score: p.R4 ?? p.r4 ?? null,
        };
      }
    } else {
      // HISTORICAL MODE — in-play is a different tournament; serve only from
      // persisted snapshots so this major's final results stay accurate.
      golfers = buildSnapshotLeaderboard(majorId);
    }

    res.json({
      tournamentName: isThisMajorLive ? (data?.event_name || null) : null,
      currentRound:   isThisMajorLive ? currentRound : null,
      isActive:       isThisMajorLive,
      lastUpdated:    isThisMajorLive ? (data?.last_updated || null) : null,
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

// /api/field/:majorId — returns the FULL field for a major from DataGolf,
// not just players curated in our matcher table. Tries multiple DataGolf
// sources (in-play if the major is live, /field-updates if DataGolf has the
// major queued as the next event, snapshots otherwise) and returns rich
// player rows the frontend can render even if the player isn't in GOLFERS.
app.get("/api/field/:majorId", async (req, res) => {
  const majorId = req.params.majorId;
  const eventId = DG_EVENT_IDS[majorId];
  if (!eventId) return res.status(404).json({ error: `unknown major: ${majorId}` });

  try {
    let rawPlayers = [];
    let lastUpdated = null;
    let eventName = null;
    let source = "none";

    // Source 1: live in-play (cached) when this major is currently being played
    const lbCached = cache.get(`lb:${majorId}`)?.data;
    const lbEventId = lbCached?.event_id ?? lbCached?.eventId ?? null;
    if (lbCached && lbEventId && Number(lbEventId) === Number(eventId)) {
      rawPlayers = Array.isArray(lbCached.data) ? lbCached.data : [];
      lastUpdated = lbCached.last_updated;
      eventName = lbCached.event_name;
      source = "in-play";
    }

    // Source 2: /field-updates — DataGolf's "next tournament" field. Only use
    // it if its event_id matches this major (otherwise it's a different week's
    // field). Cache 5x longer than leaderboards since fields move slowly.
    if (rawPlayers.length === 0) {
      const fieldData = await cached(`field:${majorId}`, CACHE_TTL * 5, () =>
        fetchDG("/field-updates", { tour: "pga" }).catch(() => null)
      );
      const fieldEventId = fieldData?.event_id ?? fieldData?.eventId ?? null;
      if (fieldData && fieldEventId && Number(fieldEventId) === Number(eventId)) {
        rawPlayers = Array.isArray(fieldData.field) ? fieldData.field : [];
        lastUpdated = fieldData.last_updated;
        eventName = fieldData.event_name;
        source = "field-updates";
      }
    }

    // Source 3: snapshots from a prior tournament week (for completed majors).
    if (rawPlayers.length === 0 && SNAPSHOTS[majorId]) {
      rawPlayers = Object.keys(SNAPSHOTS[majorId]).map(dgIdStr => {
        const dgId = Number(dgIdStr);
        const gid  = DGID_TO_GID.get(dgId);
        return {
          dg_id: dgId,
          player_name: GID_TO_NAME.get(gid) || `Player ${dgId}`,
          country: "",
        };
      });
      source = "snapshots";
    }

    // Hydrate name maps + build rich response.
    hydrateNameMaps(rawPlayers);

    const players = rawPlayers.map(p => {
      const matchedGid = matchGolferId(p.player_name);
      const gid = matchedGid || `dg-${p.dg_id || NORMALIZE(p.player_name)}`;
      return {
        gid,
        name:    prettyName(p.player_name || ""),
        country: p.country || "",
        dg_id:   p.dg_id || null,
        matched: !!matchedGid,
      };
    });

    res.json({
      count: players.length,
      source,
      eventName,
      lastUpdated,
      players,
    });
  } catch (err) {
    console.error(`field ${majorId}:`, err.message);
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
app.get("/api/picks", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const leagueId = await resolveLeagueId(req.user.id, req.query.leagueId);
    if (leagueId == null) return res.status(403).json({ error: "not a member of that league" });
    const { rows } = await pool.query(
      `SELECT team_id, major_id, starters, bench, subs, submitted, score_prediction
         FROM picks WHERE league_id = $1`,
      [leagueId]
    );
    const out = {};
    for (const r of rows) {
      out[r.team_id] = out[r.team_id] || {};
      out[r.team_id][r.major_id] = {
        starters:        r.starters  || [],
        bench:           r.bench     || [],
        subs:            r.subs      || [],
        submitted:       r.submitted,
        scorePrediction: r.score_prediction == null ? null : Number(r.score_prediction),
      };
    }
    res.json(out);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put("/api/picks/:teamId/:majorId", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { teamId, majorId } = req.params;
  const { starters = [], bench = [], subs = [], submitted = false, scorePrediction = null, leagueId: bodyLeagueId } = req.body || {};
  try {
    const leagueId = await resolveLeagueId(req.user.id, req.query.leagueId ?? bodyLeagueId);
    if (leagueId == null) return res.status(403).json({ error: "not a member of that league" });
    // Ownership check — the authenticated user must own teamId in this league.
    const ownerTeam = await getUserTeamId(req.user.id, leagueId);
    if (ownerTeam !== teamId) {
      return res.status(403).json({ error: "you can only edit your own team's picks" });
    }
    const sp = scorePrediction == null ? null : Math.max(-20, Math.min(10, Math.round(Number(scorePrediction))));
    await pool.query(
      `INSERT INTO picks (league_id, team_id, major_id, starters, bench, subs, submitted, score_prediction, updated_at)
       VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7, $8, NOW())
       ON CONFLICT (league_id, team_id, major_id)
       DO UPDATE SET starters = EXCLUDED.starters,
                     bench    = EXCLUDED.bench,
                     subs     = EXCLUDED.subs,
                     submitted = EXCLUDED.submitted,
                     score_prediction = EXCLUDED.score_prediction,
                     updated_at = NOW()`,
      [leagueId, teamId, majorId, JSON.stringify(starters), JSON.stringify(bench), JSON.stringify(subs), submitted, sp]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET  /api/chat                       → last 200 messages, oldest first
// POST /api/chat                       → { teamId, text } → inserts row
app.get("/api/chat", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const leagueId = await resolveLeagueId(req.user.id, req.query.leagueId);
    if (leagueId == null) return res.status(403).json({ error: "not a member of that league" });
    const { rows } = await pool.query(
      `SELECT id, team_id AS "teamId", text, ts
         FROM chat_messages
        WHERE league_id = $1
        ORDER BY ts DESC
        LIMIT 200`,
      [leagueId]
    );
    res.json(rows.reverse());   // oldest first for UI
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/chat", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { text, leagueId: bodyLeagueId } = req.body || {};
  if (!text || !text.trim()) return res.status(400).json({ error: "text required" });
  try {
    const leagueId = await resolveLeagueId(req.user.id, req.query.leagueId ?? bodyLeagueId);
    if (leagueId == null) return res.status(403).json({ error: "not a member of that league" });
    // Force teamId from the authenticated user's claim in this league.
    const teamId = await getUserTeamId(req.user.id, leagueId);
    if (!teamId) return res.status(403).json({ error: "you must claim a team to chat" });
    const ts = Date.now();
    const { rows } = await pool.query(
      `INSERT INTO chat_messages (league_id, team_id, text, ts) VALUES ($1, $2, $3, $4)
       RETURNING id, team_id AS "teamId", text, ts`,
      [leagueId, teamId, text.trim().slice(0, 1000), ts]
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

app.put("/api/profiles/:teamId", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { teamId } = req.params;
  const { displayName = null, teamName = null, email = null } = req.body || {};
  try {
    // Ownership: the user must own this team_id in ANY of their leagues.
    const ownership = await pool.query(
      `SELECT 1 FROM league_members WHERE user_id = $1 AND team_id = $2 LIMIT 1`,
      [req.user.id, teamId]
    );
    if (!ownership.rows.length) {
      return res.status(403).json({ error: "you can only edit your own profile" });
    }
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
    // Resolve league + member_model.
    const lr = await client.query(
      `SELECT id, member_model FROM leagues WHERE invite_code = $1`, [leagueCode]
    );
    if (!lr.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: `unknown league code: ${leagueCode}` });
    }
    const leagueId    = Number(lr.rows[0].id);
    const memberModel = lr.rows[0].member_model || "open";
    const visibility  = (await client.query(
      `SELECT visibility FROM leagues WHERE id = $1`, [leagueId]
    )).rows[0]?.visibility || "public";
    if (visibility === "private" && memberModel !== "slots") {
      await client.query("ROLLBACK");
      return res.status(403).json({ error: "this league is private — ask the commissioner to add you" });
    }
    let slotRow = null;
    if (memberModel === "slots") {
      if (!teamId) {
        await client.query("ROLLBACK");
        return res.status(400).json({ error: "this league requires picking a team" });
      }
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
      slotRow = slot.rows[0];
    }
    // Create user.
    const u = await client.query(
      `INSERT INTO users (email, password_hash, display_name)
       VALUES ($1, $2, $3) RETURNING id`,
      [normalizedEmail, hashPassword(password), displayName.trim()]
    );
    const userId = Number(u.rows[0].id);
    if (memberModel === "slots" && slotRow) {
      // Legacy slot claim.
      await client.query(
        `UPDATE league_members SET user_id = $1 WHERE id = $2`,
        [userId, slotRow.id]
      );
    } else {
      // Open model — add a new member row with displayName as team name.
      const count = Number((await client.query(
        `SELECT COUNT(*)::int AS n FROM league_members WHERE league_id = $1`,
        [leagueId]
      )).rows[0].n);
      const newTeamId   = `lg${leagueId}-u${userId}`;
      const newColor    = LEAGUE_COLORS[count % LEAGUE_COLORS.length];
      await client.query(
        `INSERT INTO league_members (league_id, team_id, team_name, team_color, user_id)
         VALUES ($1, $2, $3, $4, $5)`,
        [leagueId, newTeamId, displayName.trim(), newColor, userId]
      );
    }
    if (leagueId) {
      await client.query(
        `UPDATE leagues SET commissioner_id = $1
          WHERE id = $2 AND commissioner_id IS NULL`,
        [userId, leagueId]
      );
    }
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

// GET /api/leagues/by-code/:code — look up a league by invite code, return
// league info + team slots. Used by the signup form so users can type a code
// and immediately see what teams are still up for grabs.
app.get("/api/leagues/by-code/:code", async (req, res) => {
  if (!requireDb(res)) return;
  const code = String(req.params.code || "").trim().toUpperCase();
  if (!code) return res.status(400).json({ error: "code required" });
  try {
    const lr = await pool.query(
      `SELECT id, name, invite_code, format, scope, major_id
         FROM leagues WHERE invite_code = $1`,
      [code]
    );
    if (!lr.rows.length) return res.status(404).json({ error: "league not found" });
    const league = lr.rows[0];
    const mr = await pool.query(
      `SELECT lm.team_id, lm.team_name, lm.team_color, lm.user_id,
              u.display_name, u.email
         FROM league_members lm
         LEFT JOIN users u ON u.id = lm.user_id
        WHERE lm.league_id = $1
        ORDER BY lm.joined_at ASC`,
      [league.id]
    );
    res.json({
      league: {
        id:         Number(league.id),
        name:       league.name,
        code:       league.invite_code,
        format:     league.format,
        scope:      league.scope,
        majorId:    league.major_id,
        memberModel: league.member_model || "open",
      },
      members: mr.rows.map(r => ({
        teamId:       r.team_id,
        teamName:     r.team_name,
        teamColor:    r.team_color,
        claimed:      r.user_id != null,
        displayName:  r.display_name,
        email:        r.email,
      })),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/leagues — create a new league with auto-provisioned team slots.
//   body: { name, code?, teamCount? }
//   - name: human-readable league name (required, max 80 chars)
//   - code: invite code testers will type to join. If omitted, a random
//     6-character code is generated. Must be unique, uppercased.
//   - teamCount: how many team slots to create (default 5, min 2, max 12)
//   Returns { league, members } in the same shape as /api/leagues/by-code.
const LEAGUE_COLORS = [
  "#3A5F8A", "#8A3A3A", "#3A8A5A", "#8A6A3A", "#5A3A8A",
  "#3A8A8A", "#8A3A6A", "#6A8A3A", "#8A5A3A", "#3A6A8A",
  "#6A3A8A", "#8A8A3A",
];
function randomLeagueCode() {
  // Avoid ambiguous chars (0/O, 1/I) so codes are easy to read aloud / type.
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let out = "";
  for (let i = 0; i < 6; i++) {
    out += alphabet[Math.floor(Math.random() * alphabet.length)];
  }
  return out;
}
app.post("/api/leagues", async (req, res) => {
  if (!requireDb(res)) return;
  const rawName = String(req.body?.name || "").trim();
  if (!rawName) return res.status(400).json({ error: "league name required" });
  if (rawName.length > 80) return res.status(400).json({ error: "league name too long (max 80)" });
  let code = String(req.body?.code || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "");
  if (code && code.length > 16) return res.status(400).json({ error: "code too long (max 16)" });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // If user didn't supply a code, generate one and retry on collision.
    if (!code) {
      for (let attempt = 0; attempt < 8; attempt++) {
        const candidate = randomLeagueCode();
        const exists = await client.query(
          `SELECT 1 FROM leagues WHERE invite_code = $1`,
          [candidate]
        );
        if (!exists.rows.length) { code = candidate; break; }
      }
      if (!code) {
        await client.query("ROLLBACK");
        return res.status(500).json({ error: "could not generate a unique code, please retry" });
      }
    } else {
      const exists = await client.query(
        `SELECT 1 FROM leagues WHERE invite_code = $1`,
        [code]
      );
      if (exists.rows.length) {
        await client.query("ROLLBACK");
        return res.status(409).json({ error: `league code "${code}" is taken, try another` });
      }
    }

    // Visibility, league type, included majors from body. Commissioner = authed creator.
    const visibility = (String(req.body?.visibility || "public").toLowerCase() === "private") ? "private" : "public";
    const VALID_TYPES = new Set(["single_major","remaining_majors","all_four","four_plus_one"]);
    let leagueType = String(req.body?.leagueType || "all_four");
    if (!VALID_TYPES.has(leagueType)) leagueType = "all_four";
    let included = Array.isArray(req.body?.includedMajors) ? req.body.includedMajors.filter(x => typeof x === "string") : null;
    if (!included || !included.length) {
      // Default included_majors by type
      included = leagueType === "all_four"        ? ["masters","pga","usopen","open"]
              : leagueType === "four_plus_one"    ? ["masters","pga","usopen","open","players"]
              : leagueType === "remaining_majors" ? ["masters","pga","usopen","open"]
              :                                     ["masters","pga","usopen","open"];
    }
    const creatorEarly = await getOptionalUser(req);
    const lr = await client.query(
      `INSERT INTO leagues (name, invite_code, format, scope, visibility, commissioner_id, league_type, included_majors)
       VALUES ($1, $2, 'season_money', 'season', $3, $4, $5, $6::jsonb)
       RETURNING id, name, invite_code, format, scope, major_id, visibility, commissioner_id, member_model, league_type, included_majors`,
      [rawName, code, visibility, creatorEarly?.id || null, leagueType, JSON.stringify(included)]
    );
    const league = lr.rows[0];
    const leagueId = Number(league.id);

    // Open-membership model: no pre-created team slots. If authed, the
    // creator is added as the first member with their displayName as team.
    const creator = creatorEarly;
    const members = [];
    if (creator) {
      const teamId    = `lg${leagueId}-u${creator.id}`;
      const teamName  = creator.displayName || "Commissioner";
      const teamColor = LEAGUE_COLORS[0];
      await client.query(
        `INSERT INTO league_members (league_id, team_id, team_name, team_color, user_id)
         VALUES ($1, $2, $3, $4, $5)`,
        [leagueId, teamId, teamName, teamColor, creator.id]
      );
      members.push({
        teamId, teamName, teamColor,
        claimed:     true,
        displayName: creator.displayName,
        email:       creator.email,
      });
    }

    await client.query("COMMIT");
    res.json({
      league: {
        id:             leagueId,
        name:           league.name,
        code:           league.invite_code,
        format:         league.format,
        scope:          league.scope,
        majorId:        league.major_id,
        visibility:     league.visibility,
        commissionerId: league.commissioner_id == null ? null : Number(league.commissioner_id),
        memberModel:    league.member_model || "open",
        leagueType:     league.league_type,
        includedMajors: league.included_majors,
      },
      members,
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("create league failed:", err.message);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// POST /api/leagues/:leagueId/join — authed user joins a league. For 'slots'
// leagues (EXPERTS) they pass a teamId to claim a specific unclaimed slot.
// For 'open' leagues they just call it with no body — a member row is auto-
// created with their displayName as the team name.
app.post("/api/leagues/:leagueId/join", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const leagueId = Number(req.params.leagueId);
  const { teamId } = req.body || {};
  if (!Number.isFinite(leagueId)) return res.status(400).json({ error: "invalid leagueId" });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const lr = await client.query(
      `SELECT id, member_model FROM leagues WHERE id = $1`, [leagueId]
    );
    if (!lr.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "league not found" });
    }
    const model      = lr.rows[0].member_model || "open";
    const visibility = (await client.query(
      `SELECT visibility FROM leagues WHERE id = $1`, [leagueId]
    )).rows[0]?.visibility || "public";
    if (visibility === "private") {
      await client.query("ROLLBACK");
      return res.status(403).json({ error: "this league is private — ask the commissioner to add you" });
    }

    // Already a member? Friendly 409.
    const existing = await client.query(
      `SELECT 1 FROM league_members WHERE league_id = $1 AND user_id = $2`,
      [leagueId, req.user.id]
    );
    if (existing.rows.length) {
      await client.query("ROLLBACK");
      return res.status(409).json({ error: "you're already in this league" });
    }

    if (model === "slots") {
      // Legacy EXPERTS path — must pass teamId, claim an unclaimed slot.
      if (!teamId) {
        await client.query("ROLLBACK");
        return res.status(400).json({ error: "this league requires picking a team — teamId is required" });
      }
      const slot = await client.query(
        `SELECT id, user_id FROM league_members WHERE league_id = $1 AND team_id = $2`,
        [leagueId, teamId]
      );
      if (!slot.rows.length) {
        await client.query("ROLLBACK");
        return res.status(404).json({ error: "team slot not in this league" });
      }
      if (slot.rows[0].user_id) {
        await client.query("ROLLBACK");
        return res.status(409).json({ error: "that team is already claimed" });
      }
      await client.query(
        `UPDATE league_members SET user_id = $1 WHERE id = $2`,
        [req.user.id, slot.rows[0].id]
      );
      await client.query(
        `UPDATE leagues SET commissioner_id = $1
          WHERE id = $2 AND commissioner_id IS NULL`,
        [req.user.id, leagueId]
      );
      await client.query("COMMIT");
      const payload = await meAndLeagues(req.user.id);
      return res.json({ ok: true, leagueId, teamId, ...payload });
    }

    // Open model — auto-add a new member row with the user's displayName as
    // their team name. Color rotates through the palette by member count.
    const count = Number((await client.query(
      `SELECT COUNT(*)::int AS n FROM league_members WHERE league_id = $1`,
      [leagueId]
    )).rows[0].n);
    const u = (await client.query(
      `SELECT display_name AS "displayName" FROM users WHERE id = $1`,
      [req.user.id]
    )).rows[0];
    const newTeamId   = `lg${leagueId}-u${req.user.id}`;
    const newTeamName = (u?.displayName || "Member").trim();
    const newColor    = LEAGUE_COLORS[count % LEAGUE_COLORS.length];
    await client.query(
      `INSERT INTO league_members (league_id, team_id, team_name, team_color, user_id)
       VALUES ($1, $2, $3, $4, $5)`,
      [leagueId, newTeamId, newTeamName, newColor, req.user.id]
    );
    // Adopt orphan leagues — if no commissioner is set, the joiner becomes it.
    await client.query(
      `UPDATE leagues SET commissioner_id = $1
        WHERE id = $2 AND commissioner_id IS NULL`,
      [req.user.id, leagueId]
    );
    await client.query("COMMIT");
    const payload = await meAndLeagues(req.user.id);
    res.json({ ok: true, leagueId, teamId: newTeamId, ...payload });
  } catch (err) {
    await client.query("ROLLBACK");
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Commissioner-only endpoints. Helper checks that the requesting user is
// the commissioner of the given league.
async function requireCommissioner(req, res, leagueId) {
  const { rows } = await pool.query(
    `SELECT commissioner_id FROM leagues WHERE id = $1`, [leagueId]
  );
  if (!rows.length) { res.status(404).json({ error: "league not found" }); return null; }
  const commId = rows[0].commissioner_id == null ? null : Number(rows[0].commissioner_id);
  if (commId !== Number(req.user.id)) {
    res.status(403).json({ error: "only the commissioner can do that" });
    return null;
  }
  return commId;
}

// PATCH /api/leagues/:id  — commissioner updates league name / visibility
app.patch("/api/leagues/:id", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const leagueId = Number(req.params.id);
  if (!Number.isFinite(leagueId)) return res.status(400).json({ error: "invalid id" });
  const ok = await requireCommissioner(req, res, leagueId);
  if (ok == null) return;
  const sets = []; const vals = [];
  if (typeof req.body?.name === "string") {
    const n = req.body.name.trim();
    if (!n) return res.status(400).json({ error: "name cannot be empty" });
    if (n.length > 80) return res.status(400).json({ error: "name too long" });
    vals.push(n); sets.push(`name = $${vals.length}`);
  }
  if (typeof req.body?.visibility === "string") {
    const v = req.body.visibility === "private" ? "private" : "public";
    vals.push(v); sets.push(`visibility = $${vals.length}`);
  }
  if (!sets.length) return res.json({ ok: true });
  vals.push(leagueId);
  await pool.query(`UPDATE leagues SET ${sets.join(", ")} WHERE id = $${vals.length}`, vals);
  res.json({ ok: true });
});

// POST /api/leagues/:id/regenerate-code — commissioner gets a fresh code.
app.post("/api/leagues/:id/regenerate-code", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const leagueId = Number(req.params.id);
  if (!Number.isFinite(leagueId)) return res.status(400).json({ error: "invalid id" });
  const ok = await requireCommissioner(req, res, leagueId);
  if (ok == null) return;
  try {
    let next = null;
    for (let attempt = 0; attempt < 8; attempt++) {
      const candidate = randomLeagueCode();
      const exists = await pool.query(
        `SELECT 1 FROM leagues WHERE invite_code = $1`, [candidate]
      );
      if (!exists.rows.length) { next = candidate; break; }
    }
    if (!next) return res.status(500).json({ error: "couldn't generate a unique code" });
    await pool.query(`UPDATE leagues SET invite_code = $1 WHERE id = $2`, [next, leagueId]);
    res.json({ ok: true, code: next });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/leagues/:id/members/:teamId — commissioner renames a team
app.patch("/api/leagues/:id/members/:teamId", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const leagueId = Number(req.params.id);
  if (!Number.isFinite(leagueId)) return res.status(400).json({ error: "invalid id" });
  const ok = await requireCommissioner(req, res, leagueId);
  if (ok == null) return;
  const name  = typeof req.body?.teamName  === "string" ? req.body.teamName.trim()  : null;
  const color = typeof req.body?.teamColor === "string" ? req.body.teamColor.trim() : null;
  const sets = []; const vals = [];
  if (name)  { if (name.length > 80) return res.status(400).json({ error: "name too long" }); vals.push(name);  sets.push(`team_name = $${vals.length}`); }
  if (color) { vals.push(color); sets.push(`team_color = $${vals.length}`); }
  if (!sets.length) return res.json({ ok: true });
  vals.push(leagueId); vals.push(req.params.teamId);
  await pool.query(
    `UPDATE league_members SET ${sets.join(", ")} WHERE league_id = $${vals.length-1} AND team_id = $${vals.length}`,
    vals
  );
  res.json({ ok: true });
});

// DELETE /api/leagues/:id/members/:teamId — commissioner kicks a member. The
// row stays (so historical picks/snapshots remain valid) but user_id and
// scoped picks are cleared so they no longer participate. For 'open' leagues
// we delete the row outright since there's no preset slot to keep.
app.delete("/api/leagues/:id/members/:teamId", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const leagueId = Number(req.params.id);
  if (!Number.isFinite(leagueId)) return res.status(400).json({ error: "invalid id" });
  const ok = await requireCommissioner(req, res, leagueId);
  if (ok == null) return;
  // Don't let commissioner kick themselves
  const target = await pool.query(
    `SELECT lm.user_id, l.member_model FROM league_members lm
       JOIN leagues l ON l.id = lm.league_id
      WHERE lm.league_id = $1 AND lm.team_id = $2`,
    [leagueId, req.params.teamId]
  );
  if (!target.rows.length) return res.status(404).json({ error: "member not found" });
  if (Number(target.rows[0].user_id) === Number(req.user.id)) {
    return res.status(400).json({ error: "you can't kick yourself" });
  }
  if (target.rows[0].member_model === "slots") {
    // Unclaim the slot but leave the row so a new user can take it
    await pool.query(
      `UPDATE league_members SET user_id = NULL WHERE league_id = $1 AND team_id = $2`,
      [leagueId, req.params.teamId]
    );
  } else {
    await pool.query(
      `DELETE FROM league_members WHERE league_id = $1 AND team_id = $2`,
      [leagueId, req.params.teamId]
    );
    await pool.query(
      `DELETE FROM picks WHERE league_id = $1 AND team_id = $2`,
      [leagueId, req.params.teamId]
    );
  }
  res.json({ ok: true });
});

// Backward-compat alias — older clients still hit /claim
// Backward-compat alias — older clients still hit /claim
app.post("/api/leagues/:leagueId/claim", requireAuth, (req, res, next) => {
  req.url = `/api/leagues/${req.params.leagueId}/join`;
  app.handle(req, res, next);
});

// GET /api/leagues/:leagueId/archive — full season archive for a league
//   returns { years: [{ year, data }] }, newest year first
app.get("/api/leagues/:leagueId/archive", async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const { rows } = await pool.query(
      `SELECT year, data FROM season_archives
        WHERE league_id = $1
        ORDER BY year DESC`,
      [req.params.leagueId]
    );
    res.json({
      years: rows.map(r => ({ year: Number(r.year), data: r.data })),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/leagues/:leagueId/archive/:year — ingest a year's data (auth
// required; user must be a member of the league). Used to seed historical
// PDFs / spreadsheets. Body is the full JSONB blob — we don't lock down the
// shape so the format can evolve.
app.post("/api/leagues/:leagueId/archive/:year", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const leagueId = await resolveLeagueId(req.user.id, req.params.leagueId);
    if (leagueId == null) return res.status(403).json({ error: "not a member of that league" });
    const year = Number(req.params.year);
    if (!Number.isFinite(year) || year < 1990 || year > 2100) {
      return res.status(400).json({ error: "invalid year" });
    }
    const data = req.body || {};
    await pool.query(
      `INSERT INTO season_archives (league_id, year, data, updated_at)
       VALUES ($1, $2, $3::jsonb, NOW())
       ON CONFLICT (league_id, year)
       DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()`,
      [leagueId, year, JSON.stringify(data)]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/me/leagues-summary — one round trip with everything the Hub needs
// to render rich cards for each league the user is in: league info, my team,
// full members list, and per-team-per-major picks. The frontend uses its
// existing EARN / MAJORS knowledge to compute season standings, picks status,
// and live position from this raw data.
app.get("/api/me/leagues-summary", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const userId = req.user.id;
    const leagueRows = (await pool.query(
      `SELECT l.id, l.name, l.invite_code, l.format, l.scope, l.major_id,
              l.member_model, l.visibility, l.commissioner_id,
              l.league_type, l.included_majors,
              lm.team_id, lm.team_name, lm.team_color
         FROM leagues l
         JOIN league_members lm ON lm.league_id = l.id
        WHERE lm.user_id = $1
        ORDER BY lm.joined_at ASC`,
      [userId]
    )).rows;
    if (leagueRows.length === 0) return res.json({ leagues: [] });
    const leagueIds = leagueRows.map(r => Number(r.id));

    const memberRows = (await pool.query(
      `SELECT lm.league_id, lm.team_id, lm.team_name, lm.team_color, lm.user_id,
              u.display_name
         FROM league_members lm
         LEFT JOIN users u ON u.id = lm.user_id
        WHERE lm.league_id = ANY($1::bigint[])
        ORDER BY lm.joined_at ASC`,
      [leagueIds]
    )).rows;

    const pickRows = (await pool.query(
      `SELECT league_id, team_id, major_id, starters, bench, subs, submitted, score_prediction
         FROM picks
        WHERE league_id = ANY($1::bigint[])`,
      [leagueIds]
    )).rows;

    const membersByLeague = {};
    for (const m of memberRows) {
      const lid = Number(m.league_id);
      (membersByLeague[lid] = membersByLeague[lid] || []).push({
        teamId:      m.team_id,
        teamName:    m.team_name,
        teamColor:   m.team_color,
        claimed:     m.user_id != null,
        displayName: m.display_name,
      });
    }
    const picksByLeague = {};
    for (const p of pickRows) {
      const lid = Number(p.league_id);
      picksByLeague[lid] = picksByLeague[lid] || {};
      picksByLeague[lid][p.team_id] = picksByLeague[lid][p.team_id] || {};
      picksByLeague[lid][p.team_id][p.major_id] = {
        starters:        p.starters || [],
        bench:           p.bench    || [],
        subs:            p.subs     || [],
        submitted:       p.submitted,
        scorePrediction: p.score_prediction == null ? null : Number(p.score_prediction),
      };
    }
    res.json({
      leagues: leagueRows.map(r => ({
        league: {
          id:             Number(r.id),
          name:           r.name,
          code:           r.invite_code,
          format:         r.format,
          scope:          r.scope,
          majorId:        r.major_id,
          memberModel:    r.member_model || "open",
          visibility:     r.visibility   || "public",
          commissionerId: r.commissioner_id == null ? null : Number(r.commissioner_id),
          leagueType:     r.league_type   || "all_four",
          includedMajors: r.included_majors,
        },
        myTeam: {
          teamId:    r.team_id,
          teamName:  r.team_name,
          teamColor: r.team_color,
        },
        members: membersByLeague[Number(r.id)] || [],
        picks:   picksByLeague[Number(r.id)] || {},
      })),
    });
  } catch (err) {
    console.error("leagues-summary failed:", err.message);
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
  console.log(`   GET    /api/leagues/by-code/:code   POST /api/leagues`);
  console.log(`   GET    /api/me/leagues-summary`);
  console.log(`   GET    /api/leagues/:id/archive   POST /api/leagues/:id/archive/:year`);
  console.log(`   GET    /api/health`);
  console.log(`   CORS allowed: ${ALLOWED_ORIGINS.join(", ")}`);
  // Wait for the schema to be ready, then rehydrate snapshots from DB.
  // initSchema started at boot; this just gives it a moment if it's racing.
  setTimeout(() => loadSnapshotsFromDB(), 1000);
});
