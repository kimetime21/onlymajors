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
const FRONTEND_URL = process.env.FRONTEND_URL || "https://onlymajors.com";

// ───────── Email (Resend) ─────────────────────────────────────────────────
const RESEND_API_KEY = process.env.RESEND_API_KEY;
const EMAIL_FROM     = process.env.EMAIL_FROM     || "OnlyMajors <notify@onlymajors.com>";
const EMAIL_REPLY_TO = process.env.EMAIL_REPLY_TO || "hello@onlymajors.com";
const EMAIL_ENABLED  = Boolean(RESEND_API_KEY);
if (!EMAIL_ENABLED) {
  console.warn("⚠  RESEND_API_KEY not set — email notifications disabled");
}

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
  // Pre-flight: ensure the users table + member_number column exist FIRST,
  // before anything else that might fail. Each statement is isolated so a
  // failure here can't block the others. Idempotent.
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id            BIGSERIAL PRIMARY KEY,
        email         TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        display_name  TEXT,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS member_number INT`);
    // First / last name fields. Display name remains the public handle and
    // defaults to "First L." when not customized. display_name_customized
    // marks whether the user has manually edited it (so name changes can
    // auto-update the default without overwriting a chosen handle).
    await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT`);
    await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name  TEXT`);
    await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name_customized BOOLEAN NOT NULL DEFAULT false`);
    // Backfill first / last from existing display_name on a best-effort basis
    // (split on whitespace; one-token names become first_name only).
    await pool.query(`
      UPDATE users
         SET first_name = COALESCE(first_name, NULLIF(split_part(display_name, ' ', 1), '')),
             last_name  = COALESCE(last_name,
                NULLIF(
                  CASE WHEN position(' ' in display_name) > 0
                       THEN substring(display_name from position(' ' in display_name) + 1)
                       ELSE NULL END
                , ''))
       WHERE display_name IS NOT NULL
         AND (first_name IS NULL OR last_name IS NULL)
    `);
    await pool.query(`
      DO $$ BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_constraint WHERE conname = 'users_member_number_key'
        ) THEN
          BEGIN
            ALTER TABLE users ADD CONSTRAINT users_member_number_key UNIQUE (member_number);
          EXCEPTION WHEN duplicate_object THEN NULL;
          END;
        END IF;
      END $$;
    `);
    // Explicit founder assignment: hardcoded so the original five members of
    // the Experts league get the numbers everyone remembers, regardless of the
    // order their accounts happened to be created. Matches case-insensitively
    // on first_name, falling back to display_name. Wipes #1-5 first so the
    // UNIQUE constraint can't collide if someone else currently holds those
    // slots. Idempotent — re-running is a no-op once numbers are in place.
    try {
      await pool.query(`UPDATE users SET member_number = NULL WHERE member_number BETWEEN 1 AND 5`);
      const founders = [
        ["austin", 1],
        ["larry",  2],
        ["boo",    3],
        ["caleb",  4],
        ["thorne", 5],
      ];
      for (const [name, n] of founders) {
        await pool.query(`
          WITH target AS (
            SELECT id FROM users
             WHERE LOWER(COALESCE(NULLIF(first_name, ''), NULLIF(split_part(display_name, ' ', 1), ''))) = $1
               AND member_number IS NULL
             ORDER BY created_at ASC, id ASC
             LIMIT 1
          )
          UPDATE users SET member_number = $2 FROM target WHERE users.id = target.id
        `, [name, n]);
      }
      console.log("✓  founder member numbers assigned (Austin=1, Larry=2, Boo=3, Caleb=4, Thorne=5)");
    } catch (err) {
      console.error("⚠  founder member-number assignment failed:", err.message);
    }
    // Backfill any remaining NULL member_numbers in signup order, starting
    // from MAX+1 (so #1-5 the founders kept above stay untouched).
    await pool.query(`
      WITH ranked AS (
        SELECT id, ROW_NUMBER() OVER (ORDER BY created_at ASC, id ASC) AS rn
          FROM users WHERE member_number IS NULL
      ),
      offset_val AS (SELECT COALESCE(MAX(member_number), 0) AS off FROM users)
      UPDATE users SET member_number = offset_val.off + ranked.rn
        FROM ranked, offset_val
       WHERE users.id = ranked.id
    `);
    console.log("✓  member_number column ensured + backfilled");
    // Atomic future allocation: back member_number with a Postgres SEQUENCE
    // seeded just past the current MAX. Signups call nextval() inside their
    // transaction — collision-proof even under simultaneous signups.
    try {
      await pool.query(`CREATE SEQUENCE IF NOT EXISTS member_number_seq START WITH 1`);
      await pool.query(`
        SELECT setval('member_number_seq', GREATEST(
          (SELECT COALESCE(MAX(member_number), 0) FROM users),
          (SELECT last_value FROM member_number_seq)
        ))
      `);
      console.log("✓  member_number_seq aligned to current MAX");
    } catch (err) {
      console.error("⚠  member_number_seq setup failed:", err.message);
    }
  } catch (err) {
    console.error("⚠  pre-flight member_number migration failed:", err.message);
  }

  // Pre-flight: ensure Club Championship tables exist. CRITICAL — moved up
  // here (above the main schema block) so it runs even if anything later in
  // initSchema throws. Without this the CC tables silently never get created
  // on a fresh Railway DB, and CC PUT/GET endpoints all 500.
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS club_championship_entries (
        user_id           BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        major_id          TEXT   NOT NULL,
        year              INT    NOT NULL,
        starters          JSONB  NOT NULL DEFAULT '[]'::jsonb,
        bench             JSONB  NOT NULL DEFAULT '[]'::jsonb,
        subs              JSONB  NOT NULL DEFAULT '[]'::jsonb,
        score_prediction  INT,
        submitted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (user_id, major_id, year)
      )
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_cc_entries_major
        ON club_championship_entries (major_id, year)
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS club_championship_results (
        user_id      BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        major_id     TEXT   NOT NULL,
        year         INT    NOT NULL,
        total        NUMERIC NOT NULL DEFAULT 0,
        rank         INT,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (user_id, major_id, year)
      )
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_cc_results_major
        ON club_championship_results (major_id, year, rank)
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS club_championship_archive (
        major_id          TEXT NOT NULL,
        year              INT  NOT NULL,
        champion_user_id  BIGINT REFERENCES users(id) ON DELETE SET NULL,
        champion_name     TEXT NOT NULL,
        champion_total    NUMERIC NOT NULL,
        runners_up        JSONB DEFAULT '[]'::jsonb,
        finalized_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (major_id, year)
      )
    `);
    console.log("✓  club_championship_* tables ensured (pre-flight)");
  } catch (err) {
    console.error("⚠  CC pre-flight migration failed:", err.message);
  }

  // Pre-flight: notification tables. Same defensive treatment as CC —
  // some prior migration is silently failing and these never get created,
  // which breaks the entire email pipeline (test endpoint + scheduled
  // notifications both query notification_log). Idempotent.
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS notification_prefs (
        user_id        BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
        picks_open     BOOLEAN NOT NULL DEFAULT true,
        wed_reminder   BOOLEAN NOT NULL DEFAULT true,
        round_wrap     BOOLEAN NOT NULL DEFAULT true,
        mc_alert       BOOLEAN NOT NULL DEFAULT true,
        sat_reminder   BOOLEAN NOT NULL DEFAULT true,
        unsub_token    TEXT NOT NULL DEFAULT encode(gen_random_bytes(16), 'hex'),
        updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS notification_log (
        user_id    BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        kind       TEXT   NOT NULL,
        major_id   TEXT   NOT NULL,
        round      INT,
        sent_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        message_id TEXT
      )
    `);
    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_log_uniq
        ON notification_log (user_id, kind, major_id, COALESCE(round, 0))
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_notification_log_user ON notification_log (user_id)
    `);
    // Seed default prefs (all-on) for any existing user without a row.
    await pool.query(`
      INSERT INTO notification_prefs (user_id)
      SELECT u.id FROM users u
       WHERE NOT EXISTS (SELECT 1 FROM notification_prefs np WHERE np.user_id = u.id)
    `);
    console.log("✓  notification_* tables ensured (pre-flight)");
  } catch (err) {
    console.error("⚠  notification pre-flight migration failed:", err.message);
  }

  // Pre-flight: Caddie's Call tables. 19th-Hole side game where members
  // answer 5 auto-generated prop questions per major. Phase 1: schema only,
  // endpoints exist but dormant (no frontend yet). Phase 2+ ship for The
  // Open in July.
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS caddies_call_questions (
        id              BIGSERIAL PRIMARY KEY,
        major_id        TEXT   NOT NULL,
        year            INT    NOT NULL,
        slot            INT    NOT NULL,
        kind            TEXT   NOT NULL,
        question_text   TEXT   NOT NULL,
        type            TEXT   NOT NULL,
        options         JSONB  NOT NULL DEFAULT '[]'::jsonb,
        resolver_data   JSONB  NOT NULL DEFAULT '{}'::jsonb,
        locks_at        TIMESTAMPTZ,
        correct_answer  TEXT,
        scored_at       TIMESTAMPTZ,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_caddies_call_q_slot
        ON caddies_call_questions (major_id, year, slot)
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS caddies_call_answers (
        user_id      BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        question_id  BIGINT NOT NULL REFERENCES caddies_call_questions(id) ON DELETE CASCADE,
        answer       TEXT NOT NULL,
        submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (user_id, question_id)
      )
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_caddies_call_a_question
        ON caddies_call_answers (question_id)
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS caddies_call_results (
        user_id    BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        major_id   TEXT   NOT NULL,
        year       INT    NOT NULL,
        correct    INT    NOT NULL DEFAULT 0,
        answered   INT    NOT NULL DEFAULT 0,
        scored_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (user_id, major_id, year)
      )
    `);
    console.log("✓  caddies_call_* tables ensured (pre-flight)");
  } catch (err) {
    console.error("⚠  Caddie's Call pre-flight migration failed:", err.message);
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
      member_number INT UNIQUE,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    ALTER TABLE users ADD COLUMN IF NOT EXISTS member_number INT;
    DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'users_member_number_key') THEN
        BEGIN
          ALTER TABLE users ADD CONSTRAINT users_member_number_key UNIQUE (member_number);
        EXCEPTION WHEN duplicate_object THEN NULL;
        END;
      END IF;
    END $$;
    -- Backfill: assign #1, #2, #3 ... ordered by signup time so the founder
    -- of the platform gets #1. Idempotent — only fills NULL rows.
    WITH ranked AS (
      SELECT id, ROW_NUMBER() OVER (ORDER BY created_at ASC, id ASC) AS rn
        FROM users WHERE member_number IS NULL
    )
    UPDATE users SET member_number = (
      SELECT COALESCE(MAX(member_number), 0) FROM users
    ) + ranked.rn
    FROM ranked WHERE users.id = ranked.id;

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

    CREATE TABLE IF NOT EXISTS password_resets (
      token       TEXT PRIMARY KEY,
      user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      expires_at  TIMESTAMPTZ NOT NULL,
      used_at     TIMESTAMPTZ,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_password_resets_user ON password_resets (user_id);

    -- Per-user notification toggles. All default to TRUE so a user opts OUT,
    -- not in. Row is created lazily on first read; absence means "all on".
    CREATE TABLE IF NOT EXISTS notification_prefs (
      user_id        BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      picks_open     BOOLEAN NOT NULL DEFAULT true,
      wed_reminder   BOOLEAN NOT NULL DEFAULT true,
      round_wrap     BOOLEAN NOT NULL DEFAULT true,
      mc_alert       BOOLEAN NOT NULL DEFAULT true,
      sat_reminder   BOOLEAN NOT NULL DEFAULT true,
      unsub_token    TEXT NOT NULL DEFAULT encode(gen_random_bytes(16), 'hex'),
      updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    -- Idempotency log. We never want to spam the same person about the
    -- same event twice. The composite unique INDEX (not constraint — we
    -- need COALESCE so NULL rounds collapse to 0) is the safety net.
    CREATE TABLE IF NOT EXISTS notification_log (
      user_id    BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      kind       TEXT   NOT NULL,  -- 'picks_open' | 'wed_reminder' | 'round_wrap' | 'mc_alert' | 'sat_reminder'
      major_id   TEXT   NOT NULL,
      round      INT,              -- for round_wrap; null otherwise
      sent_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      message_id TEXT              -- Resend message id, for traceability
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_log_uniq
      ON notification_log (user_id, kind, major_id, COALESCE(round, 0));
    CREATE INDEX IF NOT EXISTS idx_notification_log_user ON notification_log (user_id);

    -- ─── Club Championship: platform-wide single-major contest ───────────
    -- Entries (one row per user per major). Roster is 4 starters + 2 bench
    -- to match every other league. Submitted_at powers the "earliest entry
    -- wins ties" tiebreaker (after total + score prediction).
    CREATE TABLE IF NOT EXISTS club_championship_entries (
      user_id           BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      major_id          TEXT   NOT NULL,
      year              INT    NOT NULL,
      starters          JSONB  NOT NULL DEFAULT '[]'::jsonb,
      bench             JSONB  NOT NULL DEFAULT '[]'::jsonb,
      subs              JSONB  NOT NULL DEFAULT '[]'::jsonb,
      score_prediction  INT,
      submitted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (user_id, major_id, year)
    );
    CREATE INDEX IF NOT EXISTS idx_cc_entries_major
      ON club_championship_entries (major_id, year);

    -- Per-major running results — refreshed on a cadence as the tournament
    -- plays out. Rank is computed at refresh time.
    CREATE TABLE IF NOT EXISTS club_championship_results (
      user_id      BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      major_id     TEXT   NOT NULL,
      year         INT    NOT NULL,
      total        NUMERIC NOT NULL DEFAULT 0,
      rank         INT,
      updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (user_id, major_id, year)
    );
    CREATE INDEX IF NOT EXISTS idx_cc_results_major
      ON club_championship_results (major_id, year, rank);

    -- Hall of Fame — finalized winner per (major, year). One row added
    -- when each contest closes. Champion name snapshotted so an account
    -- rename doesn't break the archive.
    CREATE TABLE IF NOT EXISTS club_championship_archive (
      major_id          TEXT NOT NULL,
      year              INT  NOT NULL,
      champion_user_id  BIGINT REFERENCES users(id) ON DELETE SET NULL,
      champion_name     TEXT NOT NULL,
      champion_total    NUMERIC NOT NULL,
      runners_up        JSONB DEFAULT '[]'::jsonb,
      finalized_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (major_id, year)
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
  // Defensive standalone migration for member_number — runs as its own query
  // so it's isolated from anything else that might fail. Idempotent.
  try {
    await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS member_number INT`);
    await pool.query(`
      DO $$ BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_constraint WHERE conname = 'users_member_number_key'
        ) THEN
          BEGIN
            ALTER TABLE users ADD CONSTRAINT users_member_number_key UNIQUE (member_number);
          EXCEPTION WHEN duplicate_object THEN NULL;
          END;
        END IF;
      END $$;
    `);
    await pool.query(`
      WITH ranked AS (
        SELECT id, ROW_NUMBER() OVER (ORDER BY created_at ASC, id ASC) AS rn
          FROM users WHERE member_number IS NULL
      ),
      offset_val AS (SELECT COALESCE(MAX(member_number), 0) AS off FROM users)
      UPDATE users SET member_number = offset_val.off + ranked.rn
        FROM ranked, offset_val
       WHERE users.id = ranked.id
    `);
    console.log("✓  member_number column ensured + backfilled");
  } catch (err) {
    console.error("⚠  member_number migration failed:", err.message);
  }

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
    ALTER TABLE leagues
      ADD COLUMN IF NOT EXISTS founded_year INT;
  `);
  // Pin EXPERTS to its real founding year so the Hub card reads "Since 2022".
  await pool.query(`UPDATE leagues SET founded_year = 2022 WHERE id = 1 AND founded_year IS NULL`);
  // Backfill any league without a founded_year using its created_at row.
  await pool.query(`UPDATE leagues SET founded_year = EXTRACT(YEAR FROM created_at)::int WHERE founded_year IS NULL`);
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

// ───────── Email layout (shared brand wrapper for ALL notifications) ──────
// Every notification — test, password reset, picks open, Wednesday reminder,
// round wrap, MC alert, Saturday reminder — passes its content into these
// helpers so every email has the same header, footer, button styling, and
// brand feel. Inline CSS only (Gmail strips <style>), table-based layout
// (Outlook), max-width 560px, mobile-friendly without media queries.
const BRAND = {
  green:     "#2b4535",
  greenInk:  "#1f3327",
  cream:     "#f5f1e8",
  creamSoft: "#faf7ef",
  ink:       "#1a1a1a",
  body:      "#4a4a4a",
  dim:       "#7a7466",
  faint:     "#b8b2a3",
  rule:      "#ece8db",
  white:     "#ffffff",
};

function emailHtml({ preheader = "", heading, intro, bodyHtml = "", ctaLabel, ctaUrl, footerNote }) {
  const stack = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif";
  return `<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="x-apple-disable-message-reformatting" />
  <meta name="color-scheme" content="light" />
  <meta name="supported-color-schemes" content="light" />
  <title>OnlyMajors</title>
</head>
<body style="margin:0;padding:0;background:${BRAND.cream};font-family:${stack};color:${BRAND.ink};">
  <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;mso-hide:all;">${preheader || heading || ""}</div>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.cream};">
    <tr>
      <td align="center" style="padding:40px 16px;">
        <table role="presentation" width="560" cellpadding="0" cellspacing="0" border="0" style="background:${BRAND.white};border-radius:14px;max-width:560px;width:100%;box-shadow:0 1px 3px rgba(43,69,53,0.06),0 8px 24px rgba(43,69,53,0.06);overflow:hidden;">
          <tr>
            <td style="background:${BRAND.creamSoft};padding:28px 32px 22px;text-align:center;border-bottom:1px solid ${BRAND.rule};">
              <div style="color:${BRAND.green};font-size:22px;font-weight:800;letter-spacing:-0.02em;line-height:1;">OnlyMajors</div>
              <div style="color:${BRAND.dim};font-size:10px;letter-spacing:0.22em;text-transform:uppercase;margin-top:6px;">Fantasy golf when it counts</div>
            </td>
          </tr>
          <tr>
            <td style="padding:30px 32px 8px;">
              <h1 style="color:${BRAND.ink};font-size:22px;font-weight:700;margin:0 0 12px;line-height:1.3;">${heading}</h1>
              ${intro ? `<p style="color:${BRAND.body};font-size:15px;line-height:1.55;margin:0 0 18px;">${intro}</p>` : ""}
              ${bodyHtml}
              ${ctaUrl ? `
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:22px 0 6px;">
                <tr>
                  <td style="background:${BRAND.green};border-radius:8px;">
                    <a href="${ctaUrl}" style="display:inline-block;color:${BRAND.white};font-size:15px;font-weight:600;text-decoration:none;padding:12px 26px;font-family:${stack};">${ctaLabel || "Open OnlyMajors"}</a>
                  </td>
                </tr>
              </table>` : ""}
            </td>
          </tr>
          <tr>
            <td style="padding:24px 32px 28px;border-top:1px solid ${BRAND.rule};background:${BRAND.creamSoft};">
              ${footerNote ? `<p style="color:${BRAND.dim};font-size:12px;line-height:1.55;margin:0 0 12px;text-align:center;">${footerNote}</p>` : ""}
              <p style="color:${BRAND.dim};font-size:11px;line-height:1.5;margin:0;text-align:center;">
                <a href="https://onlymajors.com/settings#notifications" style="color:${BRAND.green};text-decoration:none;font-weight:600;">Email preferences</a>
                &nbsp;·&nbsp;
                <a href="mailto:hello@onlymajors.com" style="color:${BRAND.green};text-decoration:none;font-weight:600;">Get in touch</a>
              </p>
              <p style="color:${BRAND.faint};font-size:10px;letter-spacing:0.06em;text-align:center;margin:14px 0 0;">OnlyMajors · onlymajors.com</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`;
}

// Plain-text companion. Required for deliverability — major clients (Gmail
// included) penalize HTML-only emails. Mirrors structure, omits styling.
function emailText({ heading, intro, bodyText = "", ctaLabel, ctaUrl, footerNote }) {
  const lines = [
    "OnlyMajors — fantasy golf when it counts",
    "",
    heading,
    intro ? "\n" + intro : "",
    bodyText ? "\n" + bodyText : "",
    ctaUrl ? `\n${ctaLabel || "Open OnlyMajors"}: ${ctaUrl}` : "",
    "",
    "—",
    footerNote || "",
    "Email preferences: https://onlymajors.com/settings#notifications",
    "Get in touch: hello@onlymajors.com",
  ];
  return lines.filter((l, i, a) => !(l === "" && a[i - 1] === "")).join("\n");
}

// ───────── Email (Resend) helper ──────────────────────────────────────────
// Provider-agnostic on purpose. If we ever switch to AWS SES or Postmark,
// we swap the body of this function and every caller keeps working.
async function sendEmail({ to, subject, html, text, replyTo, tag }) {
  if (!EMAIL_ENABLED) {
    console.warn(`[email] skipped — no RESEND_API_KEY (would have sent: "${subject}" → ${to})`);
    return { skipped: true };
  }
  if (!to || !subject || (!html && !text)) {
    throw new Error("sendEmail requires { to, subject, html|text }");
  }
  const payload = {
    from:     EMAIL_FROM,
    to:       Array.isArray(to) ? to : [to],
    subject,
    reply_to: replyTo || EMAIL_REPLY_TO,
    ...(html ? { html } : {}),
    ...(text ? { text } : {}),
    ...(tag  ? { tags: [{ name: "type", value: tag }] } : {}),
  };
  const resp = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${RESEND_API_KEY}`,
      "Content-Type":  "application/json",
    },
    body: JSON.stringify(payload),
  });
  const body = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    console.error(`[email] Resend ${resp.status}:`, body);
    throw new Error(body?.message || `Resend returned ${resp.status}`);
  }
  console.log(`[email] sent "${subject}" → ${to} (id=${body.id})`);
  return { id: body.id };
}

// ───────── Major metadata (for notification scheduling) ───────────────────
// Mirrors the front-end MAJORS constant — only the fields a notification
// needs (dates, course, label). Kept in code rather than a DB table because
// the calendar barely changes; bump these once a year.
const MAJORS_META = {
  masters: { name: "The Masters",          short: "Masters",   course: "Augusta National GC", city: "Augusta, GA",        picksOpenDate: "2026-04-06", firstTeeTime: "2026-04-09T07:50-04:00", lastFinish: "2026-04-12T20:00-04:00", teeTimeLabel: "Apr 9, 7:50 AM ET",
             rounds: { r1: "2026-04-09T07:50-04:00", r2: "2026-04-10T07:50-04:00", r3: "2026-04-11T09:00-04:00", r4: "2026-04-12T09:30-04:00" },
             roundEnds: { r1: "2026-04-09T20:00-04:00", r2: "2026-04-10T20:00-04:00", r3: "2026-04-11T20:00-04:00", r4: "2026-04-12T20:00-04:00" } },
  pga:     { name: "PGA Championship",     short: "PGA",       course: "Aronimink GC",        city: "Newtown Square, PA", picksOpenDate: "2026-05-11", firstTeeTime: "2026-05-14T07:00-04:00", lastFinish: "2026-05-17T20:00-04:00", teeTimeLabel: "May 14, 7:00 AM ET",
             rounds: { r1: "2026-05-14T07:00-04:00", r2: "2026-05-15T07:00-04:00", r3: "2026-05-16T09:00-04:00", r4: "2026-05-17T09:30-04:00" },
             roundEnds: { r1: "2026-05-14T20:00-04:00", r2: "2026-05-15T20:00-04:00", r3: "2026-05-16T20:00-04:00", r4: "2026-05-17T20:00-04:00" } },
  usopen:  { name: "U.S. Open",            short: "U.S. Open", course: "Shinnecock Hills",    city: "Southampton, NY",    picksOpenDate: "2026-06-15", firstTeeTime: "2026-06-18T06:45-04:00", lastFinish: "2026-06-21T20:00-04:00", teeTimeLabel: "Jun 18, 6:45 AM ET",
             rounds: { r1: "2026-06-18T06:45-04:00", r2: "2026-06-19T06:45-04:00", r3: "2026-06-20T09:00-04:00", r4: "2026-06-21T09:30-04:00" },
             roundEnds: { r1: "2026-06-18T20:00-04:00", r2: "2026-06-19T20:00-04:00", r3: "2026-06-20T20:00-04:00", r4: "2026-06-21T20:00-04:00" } },
  open:    { name: "The Open Championship", short: "The Open", course: "Royal Birkdale",      city: "Southport, England", picksOpenDate: "2026-07-13", firstTeeTime: "2026-07-16T06:35+01:00", lastFinish: "2026-07-19T20:00+01:00", teeTimeLabel: "Jul 16, 6:35 AM BST",
             rounds: { r1: "2026-07-16T06:35+01:00", r2: "2026-07-17T06:35+01:00", r3: "2026-07-18T08:30+01:00", r4: "2026-07-19T09:30+01:00" },
             roundEnds: { r1: "2026-07-16T20:00+01:00", r2: "2026-07-17T20:00+01:00", r3: "2026-07-18T20:00+01:00", r4: "2026-07-19T20:00+01:00" } },
};
function majorStatus(majorId, now = new Date()) {
  const m = MAJORS_META[majorId];
  if (!m) return null;
  const start = new Date(m.firstTeeTime).getTime();
  const end   = new Date(m.lastFinish).getTime() + 60 * 60 * 1000;
  const t = now.getTime();
  if (t < start) return "upcoming";
  if (t < end)   return "live";
  return "complete";
}

// ───────── Notification helpers ───────────────────────────────────────────
// One source of truth for: "should this user be notified about this event,
// and have we already done it?" Every notification trigger calls these.
const NOTIFICATION_KINDS = ["picks_open", "wed_reminder", "round_wrap", "mc_alert", "sat_reminder"];

async function getOrCreatePrefs(userId) {
  const { rows } = await pool.query(
    `INSERT INTO notification_prefs (user_id) VALUES ($1)
     ON CONFLICT (user_id) DO UPDATE SET user_id = EXCLUDED.user_id
     RETURNING *`,
    [userId]
  );
  return rows[0];
}

// Atomic: insert a row in notification_log if we haven't already sent this
// exact notification. Returns true if INSERTED (caller should send the email);
// false if a row already exists (caller skips, no spam).
async function reserveNotification({ userId, kind, majorId, round = null, messageId = null }) {
  try {
    await pool.query(
      `INSERT INTO notification_log (user_id, kind, major_id, round, message_id)
       VALUES ($1, $2, $3, $4, $5)`,
      [userId, kind, majorId, round, messageId]
    );
    return true;
  } catch (err) {
    if (err.code === "23505") return false; // unique_violation = already sent
    throw err;
  }
}

// ───────── Picks Open notification ────────────────────────────────────────
// Fires once per (user, major) when:
//   - calendar has passed the major's picksOpenDate
//   - major is still upcoming (not started yet)
//   - user belongs to at least one league that includes this major
//   - user's picks_open preference is true
// Idempotency is guaranteed by the UNIQUE constraint on notification_log.
async function sendPicksOpenEmail(user, major) {
  const ctaUrl = `${FRONTEND_URL}/?utm_source=email&utm_campaign=picks_open&utm_content=${major.id}`;
  const layoutArgs = {
    preheader:  `Picks for ${major.name} are open — make yours.`,
    heading:    `Picks open: ${major.short}`,
    intro:      `The field is set for <strong>${major.name}</strong> at ${major.course}. Lock in your four starters and two bench before first tee on ${major.teeTimeLabel}.`,
    ctaLabel:   "Make your picks",
    ctaUrl,
    bodyHtml: `
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:8px 0 0;background:${BRAND.creamSoft};border:1px solid ${BRAND.rule};border-radius:8px;">
        <tr>
          <td style="padding:14px 16px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
            <div style="color:${BRAND.dim};font-size:10px;text-transform:uppercase;letter-spacing:0.18em;font-weight:600;">Course</div>
            <div style="color:${BRAND.ink};font-size:14px;font-weight:600;margin-top:2px;">${major.course} · ${major.city}</div>
            <div style="color:${BRAND.dim};font-size:10px;text-transform:uppercase;letter-spacing:0.18em;font-weight:600;margin-top:10px;">First tee</div>
            <div style="color:${BRAND.ink};font-size:14px;font-weight:600;margin-top:2px;">${major.teeTimeLabel}</div>
          </td>
        </tr>
      </table>`,
    footerNote: "You're receiving this because Picks Open notifications are on. Manage in Settings.",
  };
  return sendEmail({
    to:      user.email,
    subject: `Picks open: ${major.name}`,
    tag:     "picks-open",
    html:    emailHtml(layoutArgs),
    text:    emailText({ ...layoutArgs, bodyText: `Course: ${major.course} · ${major.city}\nFirst tee: ${major.teeTimeLabel}` }),
  });
}

async function checkPicksOpenForMajor(majorId) {
  if (!pool || !EMAIL_ENABLED) return { skipped: true };
  const meta = MAJORS_META[majorId];
  if (!meta) return { skipped: true, reason: "unknown major" };
  // Calendar gate
  const now = new Date();
  if (new Date(meta.picksOpenDate) > now) return { skipped: true, reason: "picks not yet open" };
  if (majorStatus(majorId, now) !== "upcoming") return { skipped: true, reason: "major no longer upcoming" };

  // Recipients: every distinct user in any league that includes this major,
  // who has picks_open prefs on (or no row yet — defaults to on), and who
  // hasn't already been notified for this major.
  const { rows: recipients } = await pool.query(
    `SELECT DISTINCT u.id, u.email, u.display_name
       FROM users u
       JOIN league_members lm ON lm.user_id = u.id
       JOIN leagues l         ON l.id = lm.league_id
      LEFT JOIN notification_prefs np ON np.user_id = u.id
      LEFT JOIN notification_log nl
             ON nl.user_id = u.id AND nl.kind = 'picks_open' AND nl.major_id = $1
      WHERE COALESCE(np.picks_open, true) = true
        AND nl.user_id IS NULL
        AND u.email IS NOT NULL
        AND (l.included_majors IS NULL OR $1 = ANY(l.included_majors))`,
    [majorId]
  );

  const results = { sent: 0, failed: 0, total: recipients.length };
  for (const u of recipients) {
    const reserved = await reserveNotification({ userId: u.id, kind: "picks_open", majorId });
    if (!reserved) continue;
    try {
      const { id } = await sendPicksOpenEmail({ email: u.email, displayName: u.display_name }, { id: majorId, ...meta });
      if (id) {
        await pool.query(
          `UPDATE notification_log SET message_id = $1
            WHERE user_id = $2 AND kind = 'picks_open' AND major_id = $3 AND round IS NULL`,
          [id, u.id, majorId]
        );
      }
      results.sent++;
    } catch (err) {
      console.error(`[picks_open] failed for user ${u.id}:`, err.message);
      // Roll back the reservation so we retry next pass.
      await pool.query(
        `DELETE FROM notification_log
          WHERE user_id = $1 AND kind = 'picks_open' AND major_id = $2 AND round IS NULL AND message_id IS NULL`,
        [u.id, majorId]
      );
      results.failed++;
    }
  }
  if (results.total > 0) {
    console.log(`[picks_open] ${majorId}: sent=${results.sent} failed=${results.failed} total=${results.total}`);
  }
  return results;
}

// ───────── Wednesday picks-close reminder ─────────────────────────────────
// Fires Wednesday evening (tournament-local) to users who haven't completed
// their roster for an upcoming major. "Complete" = 4 starters + 2 bench.
async function sendWedReminderEmail(user, major, missingPieces) {
  const ctaUrl = `${FRONTEND_URL}/?utm_source=email&utm_campaign=wed_reminder&utm_content=${major.id}`;
  const layoutArgs = {
    preheader:  `R1 tees off tomorrow — last call for ${major.short} picks.`,
    heading:    `${major.short} picks lock tomorrow`,
    intro:      `First tee at <strong>${major.course}</strong> is ${major.teeTimeLabel} — about <strong>~14 hours</strong> away. Your roster is still missing ${missingPieces}.`,
    ctaLabel:   "Finish your picks",
    ctaUrl,
    bodyHtml:   "",
    footerNote: "Wednesday reminders only fire when your roster is incomplete. Manage in Settings.",
  };
  return sendEmail({
    to:      user.email,
    subject: `Last call: ${major.name} picks lock tomorrow`,
    tag:     "wed-reminder",
    html:    emailHtml(layoutArgs),
    text:    emailText({ ...layoutArgs, bodyText: `Tee off: ${major.teeTimeLabel}\nRoster gap: ${missingPieces}` }),
  });
}

async function checkWedReminderForMajor(majorId) {
  if (!pool || !EMAIL_ENABLED) return { skipped: true };
  const meta = MAJORS_META[majorId];
  if (!meta) return { skipped: true };
  const now = new Date();
  // Only Wed evening within the picks-open window for an upcoming major.
  // Day-of-week is computed in ET to match tournament cadence.
  const etDay = new Date(now.toLocaleString("en-US", { timeZone: "America/New_York" }));
  const dayOfWeek = etDay.getDay(); // 0 Sun … 3 Wed
  const hour = etDay.getHours();
  if (dayOfWeek !== 3 || hour < 17) return { skipped: true, reason: "not Wed evening ET" };
  if (new Date(meta.picksOpenDate) > now) return { skipped: true, reason: "picks not yet open" };
  if (majorStatus(majorId, now) !== "upcoming") return { skipped: true, reason: "major no longer upcoming" };
  // Recipients: members of any league that includes this major, prefs on,
  // not already sent, AND missing starters/bench rows for THIS major.
  const { rows: recipients } = await pool.query(
    `SELECT DISTINCT u.id, u.email, u.display_name, lm.team_id, lm.league_id,
            p.starters, p.bench
       FROM users u
       JOIN league_members lm ON lm.user_id = u.id
       JOIN leagues l         ON l.id = lm.league_id
       LEFT JOIN notification_prefs np ON np.user_id = u.id
       LEFT JOIN notification_log nl
              ON nl.user_id = u.id AND nl.kind = 'wed_reminder' AND nl.major_id = $1
       LEFT JOIN picks p
              ON p.team_id = lm.team_id AND p.major_id = $1 AND p.league_id = lm.league_id
      WHERE COALESCE(np.wed_reminder, true) = true
        AND nl.user_id IS NULL
        AND u.email IS NOT NULL
        AND (l.included_majors IS NULL OR $1 = ANY(l.included_majors))`,
    [majorId]
  );
  const results = { sent: 0, failed: 0, total: 0 };
  for (const u of recipients) {
    const starters = Array.isArray(u.starters) ? u.starters.filter(Boolean) : [];
    const bench    = Array.isArray(u.bench)    ? u.bench.filter(Boolean)    : [];
    const needStart = Math.max(0, 4 - starters.length);
    const needBench = Math.max(0, 2 - bench.length);
    if (needStart === 0 && needBench === 0) continue;
    results.total++;
    const reserved = await reserveNotification({ userId: u.id, kind: "wed_reminder", majorId });
    if (!reserved) continue;
    const missing = [
      needStart && `${needStart} starter${needStart === 1 ? "" : "s"}`,
      needBench && `${needBench} bench`,
    ].filter(Boolean).join(" + ");
    try {
      const { id } = await sendWedReminderEmail({ email: u.email, displayName: u.display_name }, { id: majorId, ...meta }, missing);
      if (id) {
        await pool.query(
          `UPDATE notification_log SET message_id = $1
            WHERE user_id = $2 AND kind = 'wed_reminder' AND major_id = $3 AND round IS NULL`,
          [id, u.id, majorId]
        );
      }
      results.sent++;
    } catch (err) {
      console.error(`[wed_reminder] failed for user ${u.id}:`, err.message);
      await pool.query(
        `DELETE FROM notification_log
          WHERE user_id = $1 AND kind = 'wed_reminder' AND major_id = $2 AND round IS NULL AND message_id IS NULL`,
        [u.id, majorId]
      );
      results.failed++;
    }
  }
  if (results.total > 0) console.log(`[wed_reminder] ${majorId}: sent=${results.sent} failed=${results.failed} total=${results.total}`);
  return results;
}

// ───────── Round wrap digest (R1, R2, R3, R4) ─────────────────────────────
// Per-user digest stacking each league the user is in, summarizing the
// round that just finished. Idempotent per (user, major, round).
async function sendRoundWrapEmail(user, major, round, leagueLines, mcAlertHtml, clubChampBlock) {
  const ctaUrl = `${FRONTEND_URL}/?utm_source=email&utm_campaign=round_wrap&utm_content=${major.id}_r${round}`;
  const isFinal = round === 4;
  const layoutArgs = {
    preheader:  isFinal ? `${major.short} is in the books — final Money List inside.`
                        : `${major.short} R${round} wrap — your team and Money List.`,
    heading:    isFinal ? `${major.short} — Final` : `${major.short} — Round ${round} wrap`,
    intro:      isFinal ? `The 2026 ${major.name} is complete. Here's how your teams finished.`
                        : `Round ${round} is in the books. Here's where you stand.`,
    ctaLabel:   isFinal ? "See the Money List" : "Open the Leaderboard",
    ctaUrl,
    // Order: MC alert (R2 only) → per-league lines → Club Championship block.
    // Each piece returns "" when it has nothing to render.
    bodyHtml:   `${mcAlertHtml || ""}${leagueLines || ""}${clubChampBlock || ""}`,
    footerNote: isFinal ? "Season averages update automatically." : "Manage round-end digests in Settings.",
  };
  return sendEmail({
    to:      user.email,
    subject: isFinal ? `${major.name}: Final Money List` : `${major.short} R${round}: Round wrap`,
    tag:     "round-wrap",
    html:    emailHtml(layoutArgs),
    text:    emailText({ ...layoutArgs, bodyText: `${major.short} Round ${round} wrap. Open onlymajors.com for the full Money List.` }),
  });
}

async function checkRoundWrapForMajor(majorId) {
  if (!pool || !EMAIL_ENABLED) return { skipped: true };
  const meta = MAJORS_META[majorId];
  if (!meta?.roundEnds) return { skipped: true };
  const now = new Date();
  // Detect which round JUST ended — the most recent r{N} whose end time is
  // in the past and within the last 12 hours.
  let triggerRound = null;
  for (const round of [1, 2, 3, 4]) {
    const endTime = new Date(meta.roundEnds[`r${round}`]).getTime();
    if (endTime <= now.getTime() && now.getTime() - endTime < 12 * 60 * 60 * 1000) {
      triggerRound = round;
    }
  }
  if (!triggerRound) return { skipped: true, reason: "no round just ended" };
  // Recipients: every member of any league including this major, prefs on,
  // not already sent for this round.
  const { rows: recipients } = await pool.query(
    `SELECT DISTINCT u.id, u.email, u.display_name
       FROM users u
       JOIN league_members lm ON lm.user_id = u.id
       JOIN leagues l         ON l.id = lm.league_id
       LEFT JOIN notification_prefs np ON np.user_id = u.id
       LEFT JOIN notification_log nl
              ON nl.user_id = u.id AND nl.kind = 'round_wrap' AND nl.major_id = $1 AND nl.round = $2
      WHERE COALESCE(np.round_wrap, true) = true
        AND nl.user_id IS NULL
        AND u.email IS NOT NULL
        AND (l.included_majors IS NULL OR $1 = ANY(l.included_majors))`,
    [majorId, triggerRound]
  );
  const results = { sent: 0, failed: 0, total: recipients.length, round: triggerRound };
  for (const u of recipients) {
    // Build a per-league summary line (stub for now — we have enough hooks
    // in the codebase to flesh this out later when standings logic moves to
    // backend; for v1 the digest is a clean "see the Leaderboard" funnel.)
    const leagueLines = `
      <p style="color:${BRAND.body};font-size:14px;line-height:1.55;margin:0 0 12px;">
        Round ${triggerRound} wrapped. Open the Leaderboard to see how your roster moved, and check the Money List for where you stand across the season.
      </p>`;
    // MC alert: if this is R2 and the user has MC starters, prepend a callout.
    let mcAlertHtml = "";
    if (triggerRound === 2) {
      mcAlertHtml = await buildMcAlertBlock(u.id, majorId);
    }
    const reserved = await reserveNotification({ userId: u.id, kind: "round_wrap", majorId, round: triggerRound });
    if (!reserved) continue;
    try {
      // Club Championship roster summary — only renders if the user is
      // enrolled. Returns "" today (feature-flagged off until enrollment
      // backend ships, task #153).
      const clubChampBlock = await buildClubChampBlock(u.id, majorId, triggerRound);
      const { id } = await sendRoundWrapEmail({ email: u.email, displayName: u.display_name }, { id: majorId, ...meta }, triggerRound, leagueLines, mcAlertHtml, clubChampBlock);
      if (id) {
        await pool.query(
          `UPDATE notification_log SET message_id = $1
            WHERE user_id = $2 AND kind = 'round_wrap' AND major_id = $3 AND round = $4`,
          [id, u.id, majorId, triggerRound]
        );
      }
      // Also mark mc_alert sent if we included one, so we don't double-send.
      if (mcAlertHtml) {
        await reserveNotification({ userId: u.id, kind: "mc_alert", majorId, round: triggerRound, messageId: id });
      }
      results.sent++;
    } catch (err) {
      console.error(`[round_wrap] failed for user ${u.id}:`, err.message);
      await pool.query(
        `DELETE FROM notification_log
          WHERE user_id = $1 AND kind = 'round_wrap' AND major_id = $2 AND round = $3 AND message_id IS NULL`,
        [u.id, majorId, triggerRound]
      );
      results.failed++;
    }
  }
  if (results.total > 0) console.log(`[round_wrap] ${majorId} R${triggerRound}: sent=${results.sent} failed=${results.failed} total=${results.total}`);
  return results;
}

// Club Championship roster block — included in the round-wrap digest when
// the user has entered the platform-wide contest for the focus major.
// Pulls the user's 6-starter roster, their current total, and their rank
// out of the contest leaderboard. Returns "" if they're not entered, the
// contest backend hasn't been built yet, or anything errors.
async function buildClubChampBlock(userId, majorId, round) {
  try {
    const year = ccYear(majorId);
    const { rows } = await pool.query(`
      SELECT cce.starters, ccr.total, ccr.rank,
             (SELECT COUNT(*) FROM club_championship_entries
               WHERE major_id = $2 AND year = $3) AS field_size
        FROM club_championship_entries cce
        LEFT JOIN club_championship_results ccr
               ON ccr.user_id = cce.user_id AND ccr.major_id = cce.major_id AND ccr.year = cce.year
       WHERE cce.user_id = $1 AND cce.major_id = $2 AND cce.year = $3`,
      [userId, majorId, year]);
    if (!rows.length) return "";
    const e = rows[0];
    // Hydrate the roster line items with current earnings from SNAPSHOTS.
    // Each "starter" is a curated golfer id; map to name + total for display.
    const starters = Array.isArray(e.starters) ? e.starters : [];
    const roster = starters.map((gid) => {
      const name = (typeof GID_TO_NAME !== "undefined" && GID_TO_NAME?.get?.(gid))
        || (Array.isArray(FRONTEND_GOLFER_IDS)
              ? (FRONTEND_GOLFER_IDS.find(([id]) => id === gid)?.[1] || gid)
              : gid);
      // Per-golfer earnings: sum across all rounds in SNAPSHOTS.
      let total = 0;
      const dgIdEntry = (typeof GID_TO_DGID !== "undefined" && GID_TO_DGID?.get?.(gid));
      const snap = SNAPSHOTS[majorId] || {};
      const golferSnaps = dgIdEntry != null ? (snap[dgIdEntry] || {}) : {};
      for (const r of [1, 2, 3, 4]) {
        const s = golferSnaps[r];
        const m = s?.finalMoney ?? s?.projMoney;
        if (typeof m === "number") total = m; // each round's snap is cumulative
      }
      return { name, total };
    });
    return renderClubChampBlock({
      rank:      e.rank == null ? null : Number(e.rank),
      fieldSize: Number(e.field_size || 0),
      total:     Number(e.total || 0),
      roster,
      round,
    });
  } catch (err) {
    console.error("[club_champ_block] failed for user", userId, err.message);
    return "";
  }
}

// Helper that renders the Club Championship block given a populated entry
// object. Kept separate from the data fetch so the round-wrap test endpoint
// can pass mock data through it.
function renderClubChampBlock({ rank, fieldSize, total, roster, round }) {
  const fmtM = (n) => n == null ? "—" : `$${(n/1e6).toFixed(2)}M`;
  const fmtK = (n) => n == null ? "—" : (n >= 1e6 ? `$${(n/1e6).toFixed(2)}M` : `$${Math.round(n/1000)}K`);
  const rosterRows = (roster || []).map(r => `
    <tr>
      <td style="padding:4px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
        <span style="color:${BRAND.body};font-size:13px;">${r.name}</span>
      </td>
      <td style="padding:4px 0;text-align:right;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
        <span style="color:${BRAND.ink};font-size:13px;font-weight:600;">${fmtK(r.total)}</span>
      </td>
    </tr>`).join("");
  return `
    <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:18px 0 0;background:${BRAND.creamSoft};border:1px solid ${BRAND.rule};border-radius:8px;">
      <tr><td style="padding:14px 16px;">
        <div style="color:${BRAND.dim};font-size:10px;text-transform:uppercase;letter-spacing:0.18em;font-weight:600;">Club Championship</div>
        <div style="margin-top:4px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
          <span style="color:${BRAND.ink};font-size:18px;font-weight:700;">${fmtM(total)}</span>
          <span style="color:${BRAND.dim};font-size:13px;margin-left:6px;">#${rank} of ${fieldSize}</span>
        </div>
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-top:10px;border-top:1px solid ${BRAND.rule};">
          ${rosterRows}
        </table>
      </td></tr>
    </table>`;
}

// MC alert block — piggybacks on the R2 round-wrap. Returns "" if the user
// has no MC starters or has opted out, otherwise a small callout HTML block.
async function buildMcAlertBlock(userId, majorId) {
  try {
    // Honor mc_alert pref.
    const { rows: prefRows } = await pool.query(
      `SELECT mc_alert FROM notification_prefs WHERE user_id = $1`, [userId]
    );
    if (prefRows.length && prefRows[0].mc_alert === false) return "";
    // Quick read of the user's picks + an EARN sentinel for cut status. The
    // round_snapshots table tracks status ('MC', 'WD', 'CUT'); if any of
    // the user's starters has status indicating they missed the cut, fire.
    const { rows: cutRows } = await pool.query(
      `SELECT 1
         FROM league_members lm
         JOIN picks p ON p.team_id = lm.team_id AND p.major_id = $2 AND p.league_id = lm.league_id
        WHERE lm.user_id = $1
          AND EXISTS (
            SELECT 1 FROM round_snapshots rs
             WHERE rs.major_id = $2 AND rs.round = 2 AND rs.status IN ('MC','WD','CUT')
               AND rs.dg_id::text = ANY (
                 SELECT jsonb_array_elements_text(p.starters)
               )
          )
        LIMIT 1`,
      [userId, majorId]
    );
    if (!cutRows.length) return "";
    return `
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:0 0 16px;background:#fdf3ed;border:1px solid #f0d9c8;border-radius:8px;">
        <tr><td style="padding:12px 14px;">
          <div style="color:#8a3a1a;font-size:11px;text-transform:uppercase;letter-spacing:0.18em;font-weight:600;">Subs available</div>
          <div style="color:${BRAND.ink};font-size:14px;font-weight:600;margin-top:3px;">A starter missed the cut.</div>
          <div style="color:${BRAND.body};font-size:13px;line-height:1.5;margin-top:4px;">Sub them out before Saturday tees off so their non-earnings stop counting.</div>
        </td></tr>
      </table>`;
  } catch (err) {
    console.error("[mc_alert] block build failed:", err.message);
    return "";
  }
}

// ───────── Saturday final-sub reminder ────────────────────────────────────
// Fires Saturday evening (~9pm course-local) to users who still have unused
// bench AND at least one MC/struggling starter — i.e., users for whom subbing
// out would change their team total.
async function sendSatReminderEmail(user, major) {
  const ctaUrl = `${FRONTEND_URL}/?utm_source=email&utm_campaign=sat_reminder&utm_content=${major.id}`;
  const layoutArgs = {
    preheader:  `Sunday tees off in hours — last sub window for ${major.short}.`,
    heading:    `Last sub window: ${major.short}`,
    intro:      `Sunday's final round at ${major.course} tees off in the morning. You still have bench moves available and at least one starter who's out of contention.`,
    ctaLabel:   "Review your subs",
    ctaUrl,
    bodyHtml:   "",
    footerNote: "Saturday reminders only fire when subs would change your total.",
  };
  return sendEmail({
    to:      user.email,
    subject: `Last sub window: ${major.name}`,
    tag:     "sat-reminder",
    html:    emailHtml(layoutArgs),
    text:    emailText({ ...layoutArgs, bodyText: `Sunday final round tomorrow. Open My Picks to make subs.` }),
  });
}

async function checkSatReminderForMajor(majorId) {
  if (!pool || !EMAIL_ENABLED) return { skipped: true };
  const meta = MAJORS_META[majorId];
  if (!meta) return { skipped: true };
  const now = new Date();
  // Only Saturday evening ET (~6pm-11pm) during a live major.
  const etDay = new Date(now.toLocaleString("en-US", { timeZone: "America/New_York" }));
  const dayOfWeek = etDay.getDay(); // 6 = Saturday
  const hour = etDay.getHours();
  if (dayOfWeek !== 6 || hour < 18) return { skipped: true, reason: "not Sat evening ET" };
  if (majorStatus(majorId, now) !== "live") return { skipped: true, reason: "major not live" };
  // Candidates: in a league w/ this major, prefs on, not already sent, has
  // picks with at least one MC/WD/CUT starter and at least one bench player
  // currently NOT subbed in (i.e. still on the bench).
  const { rows: recipients } = await pool.query(
    `SELECT DISTINCT u.id, u.email, u.display_name, lm.team_id, lm.league_id, p.starters, p.bench, p.subs
       FROM users u
       JOIN league_members lm ON lm.user_id = u.id
       JOIN leagues l         ON l.id = lm.league_id
       JOIN picks p           ON p.team_id = lm.team_id AND p.major_id = $1 AND p.league_id = lm.league_id
       LEFT JOIN notification_prefs np ON np.user_id = u.id
       LEFT JOIN notification_log nl
              ON nl.user_id = u.id AND nl.kind = 'sat_reminder' AND nl.major_id = $1
      WHERE COALESCE(np.sat_reminder, true) = true
        AND nl.user_id IS NULL
        AND u.email IS NOT NULL
        AND (l.included_majors IS NULL OR $1 = ANY(l.included_majors))`,
    [majorId]
  );
  const results = { sent: 0, failed: 0, total: 0 };
  for (const u of recipients) {
    // Eligibility: at least one MC/WD/CUT starter + at least one unused bench
    const starters = Array.isArray(u.starters) ? u.starters.filter(Boolean) : [];
    const bench    = Array.isArray(u.bench)    ? u.bench.filter(Boolean)    : [];
    const subs     = Array.isArray(u.subs)     ? u.subs                     : [];
    const subbedIn = new Set(subs.map(s => s?.in).filter(Boolean));
    const unusedBench = bench.filter(g => !subbedIn.has(g));
    if (unusedBench.length === 0) continue;
    // Check at least one starter has cut/MC status.
    const { rows: cutRows } = await pool.query(
      `SELECT 1 FROM round_snapshots
        WHERE major_id = $1 AND round = 2 AND status IN ('MC','WD','CUT')
          AND dg_id::text = ANY ($2::text[]) LIMIT 1`,
      [majorId, starters.map(String)]
    );
    if (!cutRows.length) continue;
    results.total++;
    const reserved = await reserveNotification({ userId: u.id, kind: "sat_reminder", majorId });
    if (!reserved) continue;
    try {
      const { id } = await sendSatReminderEmail({ email: u.email, displayName: u.display_name }, { id: majorId, ...meta });
      if (id) {
        await pool.query(
          `UPDATE notification_log SET message_id = $1
            WHERE user_id = $2 AND kind = 'sat_reminder' AND major_id = $3 AND round IS NULL`,
          [id, u.id, majorId]
        );
      }
      results.sent++;
    } catch (err) {
      console.error(`[sat_reminder] failed for user ${u.id}:`, err.message);
      await pool.query(
        `DELETE FROM notification_log
          WHERE user_id = $1 AND kind = 'sat_reminder' AND major_id = $2 AND round IS NULL AND message_id IS NULL`,
        [u.id, majorId]
      );
      results.failed++;
    }
  }
  if (results.total > 0) console.log(`[sat_reminder] ${majorId}: sent=${results.sent} failed=${results.failed} total=${results.total}`);
  return results;
}

async function runNotificationSweep() {
  if (!pool || !EMAIL_ENABLED) return;
  try {
    for (const majorId of Object.keys(MAJORS_META)) {
      await checkPicksOpenForMajor(majorId);
      await checkWedReminderForMajor(majorId);
      await checkRoundWrapForMajor(majorId);
      await checkSatReminderForMajor(majorId);
    }
  } catch (err) {
    console.error("[notifications] sweep failed:", err);
  }
}

// Admin endpoint to fire the sweep on demand (useful for testing).
app.post("/api/admin/notifications/sweep", requireAuth, async (req, res) => {
  const results = {};
  for (const majorId of Object.keys(MAJORS_META)) {
    results[majorId] = {
      picks_open:   await checkPicksOpenForMajor(majorId),
      wed_reminder: await checkWedReminderForMajor(majorId),
      round_wrap:   await checkRoundWrapForMajor(majorId),
      sat_reminder: await checkSatReminderForMajor(majorId),
    };
  }
  res.json({ ok: true, results });
});

// ─── Club Championship endpoints ──────────────────────────────────────────
// GET  /api/club-championship/picks/:majorId    — caller's roster for a major
// PUT  /api/club-championship/picks/:majorId    — upsert caller's roster
// GET  /api/club-championship/standings/:majorId — top 10 + caller pinned
// GET  /api/club-championship/archive           — Hall of Fame
// All scoped to the current calendar year of the focus major.
function ccYear(majorId) {
  const meta = MAJORS_META[majorId];
  if (meta?.firstTeeTime) return new Date(meta.firstTeeTime).getFullYear();
  return new Date().getFullYear();
}

app.get("/api/club-championship/picks/:majorId", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  if (!MAJORS_META[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  try {
    const year = ccYear(majorId);
    const { rows } = await pool.query(
      `SELECT starters, bench, subs, score_prediction, submitted_at
         FROM club_championship_entries
        WHERE user_id = $1 AND major_id = $2 AND year = $3`,
      [req.user.id, majorId, year]
    );
    if (!rows.length) return res.json({ majorId, year, enrolled: false });
    const r = rows[0];
    res.json({
      majorId, year, enrolled: true,
      starters:        r.starters || [],
      bench:           r.bench || [],
      subs:            r.subs || [],
      scorePrediction: r.score_prediction,
      submittedAt:     r.submitted_at,
    });
  } catch (err) {
    console.error("[GET cc picks] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

app.put("/api/club-championship/picks/:majorId", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  if (!MAJORS_META[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  const { starters = [], bench = [], subs = [], scorePrediction = null } = req.body || {};
  // Validation: Club Championship roster is 6 starters, no bench (different
  // from league play). Bench accepted but optional — kept in schema for
  // forward-compat if the format ever expands.
  if (!Array.isArray(starters) || starters.length > 6) {
    return res.status(400).json({ error: "starters must be an array of up to 6 golfers" });
  }
  if (!Array.isArray(bench) || bench.length > 2) {
    return res.status(400).json({ error: "bench must be an array of up to 2 golfers" });
  }
  const sp = scorePrediction == null ? null : Math.max(-20, Math.min(10, Math.round(Number(scorePrediction))));
  try {
    // Lock once submission is closed (after first tee of the major).
    const meta = MAJORS_META[majorId];
    if (meta?.firstTeeTime && new Date(meta.firstTeeTime) <= new Date()) {
      return res.status(409).json({ error: "entries closed at first tee" });
    }
    const year = ccYear(majorId);
    // Upsert. submitted_at intentionally NOT touched on update — it anchors
    // the "earliest entry" tiebreaker forever.
    await pool.query(
      `INSERT INTO club_championship_entries
         (user_id, major_id, year, starters, bench, subs, score_prediction)
       VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7)
       ON CONFLICT (user_id, major_id, year)
       DO UPDATE SET
         starters         = EXCLUDED.starters,
         bench            = EXCLUDED.bench,
         subs             = EXCLUDED.subs,
         score_prediction = EXCLUDED.score_prediction,
         updated_at       = NOW()`,
      [req.user.id, majorId, year, JSON.stringify(starters), JSON.stringify(bench), JSON.stringify(subs), sp]
    );
    // Read back the canonical row so the response reflects what's actually
    // stored (including the original submitted_at on updates).
    const { rows } = await pool.query(
      `SELECT starters, bench, subs, score_prediction, submitted_at
         FROM club_championship_entries
        WHERE user_id = $1 AND major_id = $2 AND year = $3`,
      [req.user.id, majorId, year]
    );
    const r = rows[0] || {};
    res.json({
      ok: true,
      majorId, year, enrolled: true,
      starters:        r.starters || [],
      bench:           r.bench || [],
      subs:            r.subs || [],
      scorePrediction: r.score_prediction,
      submittedAt:     r.submitted_at,
    });
  } catch (err) {
    // Verbose error reporting — includes the Postgres error code and detail
    // so we can pinpoint schema mismatches without digging through Railway
    // logs. Safe to leave in: req.body never echoed back.
    console.error("[PUT cc picks] failed:", err);
    res.status(500).json({
      error:  err.message,
      code:   err.code || null,
      detail: err.detail || null,
      where:  err.where || null,
      hint:   err.hint || null,
    });
  }
});

// Live-total helper for Club Championship standings. Builds a dg_id-keyed
// money map from current SNAPSHOTS, then sums each entry's starters. Used
// when the periodic scoring job hasn't populated the results table yet
// (during active tournament play).
function computeLiveCcStandings(entries) {
  const sum = (starters, majorId) => {
    let total = 0;
    for (const gid of starters || []) {
      if (!gid) continue;
      const dgId = GID_TO_DGID.get(gid);
      if (!dgId) continue;
      const snaps = SNAPSHOTS[majorId]?.[dgId] || {};
      // Prefer the latest round we have. finalMoney > projMoney.
      const latest = snaps[4] || snaps[3] || snaps[2] || snaps[1];
      if (!latest) continue;
      total += latest.finalMoney ?? latest.projMoney ?? 0;
    }
    return total;
  };
  // Group by (majorId, user) — entries already filtered by major in callers.
  const ranked = entries.map(e => ({
    userId:   Number(e.user_id),
    name:     e.display_name || `Member ${e.user_id}`,
    starters: Array.isArray(e.starters) ? e.starters : [],
    total:    sum(e.starters || [], e.major_id),
    submittedAt: e.submitted_at,
  })).sort((a, b) => {
    if (b.total !== a.total) return b.total - a.total;
    // Tie-break by earliest submission
    return new Date(a.submittedAt).getTime() - new Date(b.submittedAt).getTime();
  });
  let rank = 0, prevTotal = null;
  for (let i = 0; i < ranked.length; i++) {
    if (ranked[i].total !== prevTotal) rank = i + 1;
    ranked[i].rank = rank;
    prevTotal = ranked[i].total;
  }
  return ranked;
}

app.get("/api/club-championship/standings/:majorId", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  if (!MAJORS_META[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  try {
    const year = ccYear(majorId);
    // Top 10 with display info from users.
    let { rows: top10 } = await pool.query(
      `SELECT r.user_id, u.display_name, r.total, r.rank
         FROM club_championship_results r
         JOIN users u ON u.id = r.user_id
        WHERE r.major_id = $1 AND r.year = $2
        ORDER BY r.rank ASC NULLS LAST
        LIMIT 10`,
      [majorId, year]
    );
    // If the periodic scoring job hasn't populated results yet (active
    // tournament), compute live totals from snapshots so per-person numbers
    // update each refresh.
    let liveStandings = null;
    if (top10.length === 0) {
      const { rows: liveEntries } = await pool.query(
        `SELECT e.user_id, e.starters, e.submitted_at,
                u.display_name, e.major_id
           FROM club_championship_entries e
           JOIN users u ON u.id = e.user_id
          WHERE e.major_id = $1 AND e.year = $2`,
        [majorId, year]
      );
      liveStandings = computeLiveCcStandings(liveEntries);
      top10 = liveStandings.slice(0, 10).map(r => ({
        user_id:      r.userId,
        display_name: r.name,
        total:        r.total,
        rank:         r.rank,
      }));
    }
    // Caller's own row + field size.
    const { rows: mine } = await pool.query(
      `SELECT r.total, r.rank
         FROM club_championship_results r
        WHERE r.user_id = $1 AND r.major_id = $2 AND r.year = $3`,
      [req.user.id, majorId, year]
    );
    const { rows: countRows } = await pool.query(
      `SELECT COUNT(*)::int AS n FROM club_championship_entries WHERE major_id = $1 AND year = $2`,
      [majorId, year]
    );
    // Pull every entry's starters once — used to attach picks to the top10
    // rows so the frontend can expand a row and see what someone picked.
    // Cheap because club_championship_entries is small (≤ Clubhouse size).
    const { rows: starterRows } = await pool.query(
      `SELECT user_id, starters FROM club_championship_entries
        WHERE major_id = $1 AND year = $2`,
      [majorId, year]
    );
    const startersByUser = {};
    for (const r of starterRows) {
      const arr = Array.isArray(r.starters) ? r.starters : [];
      startersByUser[Number(r.user_id)] = arr.filter(Boolean);
    }
    // Submitters list (everyone who has entered). Ordered by submitted_at so
    // earliest entries appear first — also the tie-break order for the score
    // prediction. Pre-tournament this is what the averages widget displays;
    // during-tournament the top10 results take over.
    const { rows: submittersRows } = await pool.query(
      `SELECT e.user_id, u.display_name, u.first_name, u.last_name,
              e.submitted_at, e.score_prediction
         FROM club_championship_entries e
         JOIN users u ON u.id = e.user_id
        WHERE e.major_id = $1 AND e.year = $2
        ORDER BY e.submitted_at ASC, e.user_id ASC`,
      [majorId, year]
    );
    res.json({
      majorId, year,
      fieldSize: countRows[0]?.n || 0,
      top10: top10.map(r => ({
        userId:   Number(r.user_id),
        name:     r.display_name || `Member ${r.user_id}`,
        total:    Number(r.total),
        rank:     r.rank == null ? null : Number(r.rank),
        starters: startersByUser[Number(r.user_id)] || [],
      })),
      submitters: submittersRows.map(r => {
        const first = r.first_name?.trim() || null;
        const last  = r.last_name?.trim()  || null;
        // Prefer "First L." for the social context.
        const name  = (first && last)
          ? `${first} ${last[0].toUpperCase()}.`
          : (r.display_name || first || `Member ${r.user_id}`);
        return {
          userId:          Number(r.user_id),
          name,
          submittedAt:     r.submitted_at,
          scorePrediction: r.score_prediction,
        };
      }),
      you: (() => {
        // Use scored row if available, otherwise pull live total from the
        // computed standings for this caller.
        if (mine.length) {
          return {
            total: Number(mine[0].total),
            rank:  mine[0].rank == null ? null : Number(mine[0].rank),
          };
        }
        if (liveStandings) {
          const youRow = liveStandings.find(r => r.userId === Number(req.user.id));
          if (youRow) return { total: youRow.total, rank: youRow.rank };
        }
        return null;
      })(),
    });
  } catch (err) {
    console.error("[GET cc standings] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// Current user's Club Championship career summary — avg, PR, wins,
// majors played. Read from club_championship_results (finalize populates).
app.get("/api/me/club-championship/summary", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const { rows } = await pool.query(
      `SELECT
         COALESCE(AVG(total), 0)::bigint  AS avg_total,
         COALESCE(MAX(total), 0)::bigint  AS best_finish,
         COUNT(DISTINCT major_id)::int    AS majors_played,
         COUNT(*) FILTER (WHERE rank = 1)::int AS wins,
         MIN(rank) AS best_rank
         FROM club_championship_results
        WHERE user_id = $1`,
      [req.user.id]
    );
    const r = rows[0] || {};
    res.json({
      avgTotal:     Number(r.avg_total || 0),
      bestFinish:   Number(r.best_finish || 0),
      majorsPlayed: r.majors_played || 0,
      wins:         r.wins || 0,
      bestRank:     r.best_rank == null ? null : Number(r.best_rank),
    });
  } catch (err) {
    console.error("[GET me/cc summary] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// Top-10 members by career average across all Club Championships they've
// entered. Uses club_championship_results (populated by finalize). Sorted
// by avg desc; tie-break by total earnings then majors played.
app.get("/api/club-championship/averages", async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const { rows } = await pool.query(
      `SELECT r.user_id,
              u.first_name, u.last_name, u.display_name,
              AVG(r.total)::bigint     AS avg_total,
              SUM(r.total)::bigint     AS total_earnings,
              COUNT(DISTINCT r.major_id)::int AS majors_played,
              MAX(r.total)::bigint     AS best_finish,
              MIN(r.rank)::int         AS best_rank
         FROM club_championship_results r
         JOIN users u ON u.id = r.user_id
        GROUP BY r.user_id, u.first_name, u.last_name, u.display_name
        HAVING COUNT(DISTINCT r.major_id) >= 1
        ORDER BY AVG(r.total) DESC, SUM(r.total) DESC
        LIMIT 10`
    );
    const fmtName = (r) => {
      const f = r.first_name?.trim(), l = r.last_name?.trim();
      if (f && l) return `${f} ${l[0].toUpperCase()}.`;
      return r.display_name || `Member ${r.user_id}`;
    };
    res.json({
      top10: rows.map((r, i) => ({
        rank:          i + 1,
        userId:        Number(r.user_id),
        name:          fmtName(r),
        avgTotal:      Number(r.avg_total),
        totalEarnings: Number(r.total_earnings),
        majorsPlayed:  r.majors_played,
        bestFinish:    Number(r.best_finish),
        bestRank:      r.best_rank,
      })),
    });
  } catch (err) {
    console.error("[GET cc averages] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/club-championship/archive", async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const { rows } = await pool.query(
      `SELECT major_id, year, champion_user_id, champion_name, champion_total, runners_up, finalized_at
         FROM club_championship_archive
        ORDER BY year DESC, finalized_at DESC`
    );
    res.json({
      majors: rows.map(r => ({
        majorId:        r.major_id,
        year:           r.year,
        championUserId: r.champion_user_id == null ? null : Number(r.champion_user_id),
        championName:   r.champion_name,
        championTotal:  Number(r.champion_total),
        runnersUp:      r.runners_up || [],
        finalizedAt:    r.finalized_at,
      })),
    });
  } catch (err) {
    console.error("[GET cc archive] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// Admin-only: finalize a completed Club Championship. Reads entries,
// computes live standings from snapshots, writes champion + runners-up
// to the archive table. Idempotent — ON CONFLICT UPDATE.
app.post("/api/admin/club-championship/finalize/:majorId", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  if (!MAJORS_META[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  try {
    const year = ccYear(majorId);
    const { rows: entries } = await pool.query(
      `SELECT e.user_id, e.starters, e.submitted_at, u.display_name, u.first_name, u.last_name, e.major_id
         FROM club_championship_entries e
         JOIN users u ON u.id = e.user_id
        WHERE e.major_id = $1 AND e.year = $2`,
      [majorId, year]
    );
    if (entries.length === 0) {
      return res.status(400).json({ error: "no entries to archive" });
    }
    const ranked = computeLiveCcStandings(entries);
    const winner = ranked[0];
    if (!winner) return res.status(400).json({ error: "unable to compute champion" });
    // "First L." display for the archive to match the standings widget.
    const nameFor = (userId) => {
      const e = entries.find(x => Number(x.user_id) === userId);
      const f = e?.first_name?.trim(), l = e?.last_name?.trim();
      if (f && l) return `${f} ${l[0].toUpperCase()}.`;
      return e?.display_name || `Member ${userId}`;
    };
    const runnersUp = ranked.slice(1, 10).map(r => ({
      userId: r.userId, name: nameFor(r.userId), total: r.total, rank: r.rank,
    }));
    await pool.query(
      `INSERT INTO club_championship_archive
         (major_id, year, champion_user_id, champion_name, champion_total, runners_up, finalized_at)
         VALUES ($1, $2, $3, $4, $5, $6::jsonb, NOW())
       ON CONFLICT (major_id, year) DO UPDATE
         SET champion_user_id = EXCLUDED.champion_user_id,
             champion_name    = EXCLUDED.champion_name,
             champion_total   = EXCLUDED.champion_total,
             runners_up       = EXCLUDED.runners_up,
             finalized_at     = NOW()`,
      [majorId, year, winner.userId, nameFor(winner.userId), winner.total, JSON.stringify(runnersUp)]
    );
    // Also snapshot into results table so scored-mode standings queries
    // return the same numbers post-finalization.
    for (const r of ranked) {
      await pool.query(
        `INSERT INTO club_championship_results (user_id, major_id, year, total, rank)
           VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (user_id, major_id, year) DO UPDATE
           SET total = EXCLUDED.total, rank = EXCLUDED.rank`,
        [r.userId, majorId, year, r.total, r.rank]
      ).catch(e => console.warn("cc results upsert:", e.message));
    }
    res.json({ ok: true, majorId, year, champion: {
      userId: winner.userId, name: nameFor(winner.userId), total: winner.total,
    }, runnersUp });
  } catch (err) {
    console.error("[POST cc finalize] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// ── /api/me/notifications ─────────────────────────────────────────────────
// GET → current preference toggles for the authed user.
// PUT → upsert; body keys default to the existing value if omitted.
app.get("/api/me/notifications", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const prefs = await getOrCreatePrefs(req.user.id);
    res.json({
      picksOpen:   prefs.picks_open,
      wedReminder: prefs.wed_reminder,
      roundWrap:   prefs.round_wrap,
      mcAlert:     prefs.mc_alert,
      satReminder: prefs.sat_reminder,
    });
  } catch (err) {
    console.error("[GET /api/me/notifications] failed:", err);
    res.status(500).json({ error: err.message });
  }
});
app.put("/api/me/notifications", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const b = req.body || {};
  const bool = (v, fallback) => (typeof v === "boolean" ? v : fallback);
  try {
    const current = await getOrCreatePrefs(req.user.id);
    const next = {
      picks_open:   bool(b.picksOpen,   current.picks_open),
      wed_reminder: bool(b.wedReminder, current.wed_reminder),
      round_wrap:   bool(b.roundWrap,   current.round_wrap),
      mc_alert:     bool(b.mcAlert,     current.mc_alert),
      sat_reminder: bool(b.satReminder, current.sat_reminder),
    };
    await pool.query(
      `UPDATE notification_prefs
          SET picks_open = $2, wed_reminder = $3, round_wrap = $4,
              mc_alert = $5, sat_reminder = $6, updated_at = NOW()
        WHERE user_id = $1`,
      [req.user.id, next.picks_open, next.wed_reminder, next.round_wrap, next.mc_alert, next.sat_reminder]
    );
    res.json({
      picksOpen:   next.picks_open,
      wedReminder: next.wed_reminder,
      roundWrap:   next.round_wrap,
      mcAlert:     next.mc_alert,
      satReminder: next.sat_reminder,
    });
  } catch (err) {
    console.error("[PUT /api/me/notifications] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// Public status endpoint — diagnoses why emails might not be flowing.
// Reveals NO secrets: just config flags + recent notification_log activity.
app.get("/api/admin/email-status", async (req, res) => {
  try {
    const out = {
      emailEnabled:        !!RESEND_API_KEY,
      apiKeyConfigured:    !!RESEND_API_KEY,
      apiKeyLength:        RESEND_API_KEY ? RESEND_API_KEY.length : 0,
      from:                EMAIL_FROM || null,
      replyTo:             EMAIL_REPLY_TO || null,
      notificationSweepRunning: !!global.__notificationSweepStarted,
    };
    if (pool) {
      const { rows: recent } = await pool.query(
        `SELECT kind, COUNT(*)::int AS count, MAX(sent_at) AS most_recent
           FROM notification_log
          GROUP BY kind
          ORDER BY most_recent DESC NULLS LAST`
      );
      out.recentActivity = recent;
      const { rows: prefRows } = await pool.query(
        `SELECT
           COUNT(*) FILTER (WHERE picks_open)   AS picks_open_on,
           COUNT(*) FILTER (WHERE wed_reminder) AS wed_reminder_on,
           COUNT(*) FILTER (WHERE round_wrap)   AS round_wrap_on,
           COUNT(*) FILTER (WHERE mc_alert)     AS mc_alert_on,
           COUNT(*) FILTER (WHERE sat_reminder) AS sat_reminder_on,
           COUNT(*)::int AS total
         FROM notification_prefs`
      );
      out.notificationPrefSummary = prefRows[0];
    }
    res.json(out);
  } catch (err) {
    console.error("[admin/email-status] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// Admin-only test endpoint to verify the email pipeline end-to-end.
// Call from any logged-in browser via the dev console or curl:
//   curl -X POST https://api.onlymajors.com/api/admin/test-email \
//     -H "Authorization: Bearer $TOKEN"
app.post("/api/admin/test-email", requireAuth, async (req, res) => {
  try {
    if (!req.user?.email) return res.status(400).json({ error: "no user email on session" });
    const layoutArgs = {
      preheader: "OnlyMajors pipeline test — looking good.",
      heading:   "Pipeline is live.",
      intro:     `If you're reading this, the Resend pipeline is wired correctly end to end — DKIM, SPF, the works — and ready to power every notification we have planned.`,
      bodyHtml: `
        <p style="color:${BRAND.body};font-size:14px;line-height:1.55;margin:0 0 8px;">Five notifications coming online:</p>
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:6px 0 8px;">
          ${[
            ["Picks Open",                  "Field set; pick your team"],
            ["Wednesday close reminder",    "Last call before R1"],
            ["End-of-round digest",         "Standings + your team's round"],
            ["MC alert",                    "When a starter missed the cut"],
            ["Saturday final-sub reminder", "Last sub window before Sunday"],
          ].map(([k, v]) => `
            <tr>
              <td style="padding:6px 0;border-bottom:1px solid ${BRAND.rule};font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
                <span style="color:${BRAND.green};font-size:13px;font-weight:600;">${k}</span>
                <span style="color:${BRAND.dim};font-size:13px;"> · ${v}</span>
              </td>
            </tr>`).join("")}
        </table>`,
      footerNote: "This is a one-off test — no action needed.",
    };
    const result = await sendEmail({
      to:      req.user.email,
      subject: "OnlyMajors test email",
      tag:     "test",
      html:    emailHtml(layoutArgs),
      text:    emailText({ ...layoutArgs, bodyText: "Five notifications coming online: Picks Open, Wednesday close reminder, End-of-round digest, MC alert, Saturday final-sub reminder." }),
    });
    res.json({ ok: true, ...result });
  } catch (err) {
    console.error("[admin/test-email] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

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

// Per-major payout tables (1-indexed by finish position). Used to derive
// projected money from DataGolf's win/top-N probabilities when /preds/in-play
// doesn't expose `prize_money_projected` directly (Scratch Plus tier as of
// June 2026). Top 70 + ties earn at every major. Numbers based on 2025
// purses scaled up modestly for 2026.
const MAJOR_PAYOUTS = {
  usopen: {
    purse: 21500000,
    ranks: [
      4300000, 2320000, 1460000, 1020000,  848000,
       753000,  700000,  619000,  557000,  505000,
       463000,  433000,  405000,  379000,  358000,
       341000,  327000,  314000,  302000,  290000,
       279000,  268000,  257000,  246000,  235000,
       225000,  215000,  207000,  200000,  193000,
       186000,  180000,  174000,  168000,  162000,
       156000,  151000,  146000,  141000,  137000,
       133000,  129000,  125000,  121000,  117000,
       113000,  110000,  107000,  104000,  101000,
        99000,   97000,   95000,   93000,   91000,
        89500,   88000,   86500,   85000,   83500,
        82000,   80500,   79000,   77500,   76000,
        74500,   73000,   71500,   70000,   68500,
    ],
  },
  open: {
    purse: 17000000,
    ranks: [
      3100000, 1759000, 1128000,  878000,  706000,
       614000,  521000,  430000,  393000,  365000,
       337000,  319000,  305000,  287000,  273000,
       259000,  246000,  235000,  223000,  212000,
       201000,  192000,  183000,  174000,  165000,
       156000,  148000,  142000,  136000,  130000,
       124000,  119000,  114000,  109000,  105000,
       101000,   97000,   93000,   89000,   85000,
        82000,   79000,   76000,   73000,   71000,
        69000,   67000,   65000,   63000,   62000,
        61000,   60000,   59000,   58000,   57000,
        56000,   55000,   54500,   54000,   53500,
        53000,   52500,   52000,   51500,   51000,
        50500,   50000,   49500,   49000,   48500,
    ],
  },
  masters: {
    purse: 21000000,
    ranks: [
      3780000, 2268000, 1428000,  1008000,  840000,
       756000,  703500,  651000,  609000,  567000,
       525000,  483000,  441000,  399000,  378000,
       357000,  336000,  315000,  294000,  273000,
       252000,  235000,  219000,  205000,  191000,
       177000,  166000,  159000,  153000,  148000,
       143000,  138000,  133000,  128000,  124000,
       120000,  117000,  114000,  111000,  108000,
       105000,  102000,   99000,   96000,   93000,
        90000,   87000,   84000,   81000,   78000,
        75000,   73500,   72000,   70500,   69000,
        68000,   67000,   66000,   65000,   64000,
    ],
  },
  pga: {
    purse: 19000000,
    ranks: [
      3420000, 2052000, 1292000,  912000,  760000,
       684000,  636500,  589000,  551000,  513000,
       475000,  437000,  399000,  361000,  342000,
       323000,  304000,  285000,  266000,  247000,
       228000,  213000,  198000,  185000,  172000,
       159000,  150000,  144000,  138000,  133000,
       128000,  124000,  120000,  116000,  112000,
       108000,  105000,  102000,   99000,   96000,
        93000,   90000,   87000,   84000,   81000,
        78000,   75000,   72000,   69000,   66000,
    ],
  },
};

// Average payout across a finish-position band — used by the probability →
// money projector. Bands match DG's exposed thresholds (top_5, top_10, top_20).
function avgPayoutBand(ranks, fromRank, toRank) {
  let sum = 0, n = 0;
  for (let i = fromRank; i <= toRank && i <= ranks.length; i++) {
    sum += ranks[i-1];
    n++;
  }
  return n > 0 ? sum / n : 0;
}
// Expected prize money from DG's probabilities. DG's odds-format=percent
// returns these as percentages (0-100), but defensively handle 0-1 too.
function expectedMoneyFromProbs(p, majorId) {
  const tbl = MAJOR_PAYOUTS[majorId];
  if (!tbl) return null;
  const norm = (v) => {
    const n = Number(v);
    if (!Number.isFinite(n)) return 0;
    return n > 1 ? n / 100 : n;
  };
  const win  = norm(p.win);
  const t5   = norm(p.top_5);
  const t10  = norm(p.top_10);
  const t20  = norm(p.top_20);
  const cut  = norm(p.make_cut);
  const ranks = tbl.ranks;
  // Probability bands (cumulative top_N includes everything inside it).
  const pBand2_5    = Math.max(0, t5  - win);
  const pBand6_10   = Math.max(0, t10 - t5);
  const pBand11_20  = Math.max(0, t20 - t10);
  const pBand21_cut = Math.max(0, cut - t20);
  const expected =
    win        * (ranks[0] || 0) +
    pBand2_5   * avgPayoutBand(ranks,  2,  5) +
    pBand6_10  * avgPayoutBand(ranks,  6, 10) +
    pBand11_20 * avgPayoutBand(ranks, 11, 20) +
    pBand21_cut* avgPayoutBand(ranks, 21, ranks.length);
  return Math.round(expected);
}

// Position-based projector: "what would you cash if play stopped right now."
// Sorts the active field by current_score, assigns sequential ranks (ties
// share the sum of their slots' payouts), maps each player to their payout.
// This is what members EXPECT — current standings drive current dollars —
// versus DG's probabilistic model that weights skill/talent heavily.
function buildPositionMoneyMap(playersInPlay, majorId) {
  const tbl = MAJOR_PAYOUTS[majorId];
  if (!tbl) return {};
  const ranks = tbl.ranks;
  const active = (playersInPlay || [])
    .filter(p => {
      if (p?.dg_id == null) return false;
      const pos = String(p.current_pos || "").toUpperCase();
      if (pos === "MC" || pos === "CUT" || pos === "WD" || pos === "DQ") return false;
      if (p.make_cut === 0) return false;
      return typeof p.current_score === "number";
    })
    .sort((a, b) => a.current_score - b.current_score);
  const byDgId = {};
  let rank = 1;
  let i = 0;
  while (i < active.length) {
    const score = active[i].current_score;
    let j = i;
    while (j < active.length && active[j].current_score === score) j++;
    const tied = j - i;
    let totalPayout = 0;
    for (let r = rank; r < rank + tied; r++) {
      if (r <= ranks.length) totalPayout += ranks[r - 1];
    }
    const perPlayer = Math.round(totalPayout / tied);
    for (let k = i; k < j; k++) {
      byDgId[active[k].dg_id] = perPlayer;
    }
    rank += tied;
    i = j;
  }
  return byDgId;
}

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

// Pulls Official World Golf Ranking (OWGR) top players from DataGolf and
// returns a Map<dg_id, { rank }>. OWGR is published weekly and available
// year-round — unlike pre-tournament odds, which DG only publishes a few
// days before each event. Year-round availability means the Top 50 section
// of the picker is always populated, even on Monday of tournament week
// before DG's tournament-specific odds drop. Cached 24 hours since OWGR
// only refreshes weekly.
//
// Source: DataGolf /preds/get-dg-rankings — returns top ~500 players with
// both DG's internal ranking and OWGR rank. We use owgr_rank (the official
// world ranking) as the standard golf-savvy reference point.
async function fetchOwgrMap() {
  return cached("dg-owgr-map", 24 * 60 * 60_000, async () => {
    try {
      const data = await fetchDG("/preds/get-dg-rankings", {});
      const rows = Array.isArray(data?.rankings) ? data.rankings
                 : Array.isArray(data)            ? data
                 : [];
      const map = new Map();
      for (const r of rows) {
        const dgId = Number(r.dg_id);
        const owgr = Number(r.owgr_rank);
        if (Number.isFinite(dgId) && Number.isFinite(owgr) && owgr > 0) {
          map.set(dgId, { rank: owgr });
        }
      }
      return map;
    } catch (err) {
      console.warn("[owgr] fetch failed:", err.message);
      return new Map();
    }
  });
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

// ─────────────────────────────────────────────────────────────
// Live-event tracking. PREV_LIVE[majorId][dg_id] holds the previous
// fetch's snapshot of position/score/status for diffing. RECENT_EVENTS
// is a ring buffer of the last EVENT_BUFFER_SIZE events per major.
const PREV_LIVE = {};
const RECENT_EVENTS = {};
const EVENT_BUFFER_SIZE = 50;
const MIN_POSITION_DELTA = 2;     // ≥ 2 spots to emit a climb/fall
const MIN_PAYOUT_DELTA   = 25000; // ≥ $25K change to flag a money-significant move

function parsePos(p) {
  if (p == null || p === "") return null;
  const s = String(p).toUpperCase();
  if (s === "MC" || s === "CUT" || s === "WD" || s === "DQ") return null;
  const m = s.match(/^T?(\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

// Diff each player against the previous snapshot, emit structured events.
// Called once per leaderboard fetch with the freshly built golfers object.
function emitLiveEvents(majorId, golfers, prevAggregateLeader) {
  PREV_LIVE[majorId] = PREV_LIVE[majorId] || {};
  RECENT_EVENTS[majorId] = RECENT_EVENTS[majorId] || [];
  const prev = PREV_LIVE[majorId];
  const events = RECENT_EVENTS[majorId];
  const now = Date.now();
  const pushEvent = (e) => {
    events.unshift({ ...e, ts: now });
    if (events.length > EVENT_BUFFER_SIZE) events.length = EVENT_BUFFER_SIZE;
  };
  let newLeader = null;
  let newLeaderScore = null;
  for (const [gid, g] of Object.entries(golfers)) {
    if (!g.dg_id) continue;
    const prevP = prev[g.dg_id];
    const newPos = parsePos(g.position);
    if (newPos === 1) {
      if (newLeader === null || (g.score != null && newLeaderScore != null && g.score < newLeaderScore)) {
        newLeader = { gid, name: g.name, dgId: g.dg_id, score: g.score };
        newLeaderScore = g.score;
      }
    }
    if (prevP) {
      const oldPos = parsePos(prevP.position);
      const oldStatus = prevP.status;
      const oldMoney = prevP.projMoney || 0;
      const newMoney = g.projMoney || 0;
      const moneyDelta = newMoney - oldMoney;
      // WD detection
      if (oldStatus === "made_cut" && g.status === "wd") {
        pushEvent({ type: "withdrew", name: g.name, gid, dgId: g.dg_id });
      }
      // Missed cut detection (only meaningful R2+ when cut applies)
      if (oldStatus === "made_cut" && g.status === "mc") {
        pushEvent({ type: "missed_cut", name: g.name, gid, dgId: g.dg_id });
      }
      // Position movement (only for made_cut players)
      if (g.status === "made_cut" && oldPos != null && newPos != null) {
        const delta = oldPos - newPos; // positive = climbed
        if (Math.abs(delta) >= MIN_POSITION_DELTA && Math.abs(moneyDelta) >= MIN_PAYOUT_DELTA) {
          pushEvent({
            type: delta > 0 ? "climb" : "fall",
            name: g.name, gid, dgId: g.dg_id,
            oldPos: prevP.position, newPos: g.position,
            score: g.score, moneyDelta,
          });
        }
      }
    }
    prev[g.dg_id] = {
      position: g.position, status: g.status,
      score: g.score, projMoney: g.projMoney,
    };
  }
  // New sole leader event
  if (newLeader && (!prevAggregateLeader || prevAggregateLeader.dgId !== newLeader.dgId)) {
    const scoreFmt = newLeader.score == null ? ""
                   : newLeader.score === 0  ? "E"
                   : newLeader.score > 0    ? `+${newLeader.score}`
                   : `${newLeader.score}`;
    pushEvent({
      type: "new_leader",
      name: newLeader.name, gid: newLeader.gid, dgId: newLeader.dgId,
      score: newLeader.score, scoreFmt,
    });
    PREV_LIVE[majorId].__leader = { dgId: newLeader.dgId, score: newLeader.score };
  }
}

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
  // Build position-based money map for this snapshot pass.
  const snapshotPosMoney = buildPositionMoneyMap(players, majorId);

  // Capture ALL completed rounds, not just the current one. DataGolf returns
  // R1..R4 strokes on each player, so we can backfill any rounds we missed.
  for (const p of players || []) {
    if (!p?.dg_id) continue;
    store[p.dg_id] = store[p.dg_id] || {};
    const pos = typeof p.current_pos === "number" ? `${p.current_pos}` : (p.current_pos || "");
    const isMC = pos === "MC" || pos === "CUT" || p.make_cut === 0;

    // Snapshot the CURRENT round with everything we know now. When DG
    // doesn't supply prize_money_projected (Scratch+ tier on /preds/in-play),
    // use position-based "what would you cash if play stopped now" money.
    const dgProj = p.prize_money_projected ?? null;
    const derivedProj = isMC ? 0 : (snapshotPosMoney[p.dg_id] ?? 0);
    store[p.dg_id][currentRound] = {
      projMoney:  dgProj != null ? dgProj : derivedProj,
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

// ───────── Caddie's Call — Phase 1 skeleton ─────────────────────────────
// 19th-Hole side game: 5 prop questions per major, auto-generated from
// DataGolf data. Phase 1 ships 4 starter generators with hardcoded options
// (course-aware thresholds come in Phase 2). The factory selects 5
// generators from the library, runs them, persists the question rows.
//
// Question generator shape:
//   {
//     kind:           "winning_score" | "hole_in_one" | ...,
//     question_text:  user-facing string,
//     type:           "single_choice" | "binary" | "player_pick",
//     options:        [{ key, label }, ...],
//     resolver_data:  arbitrary JSON the scoring job needs at resolve time
//   }
//
// Each generator is `async (majorMeta, ctx) => ({ ... })`. Async so future
// generators can hit DataGolf for course-specific historical data.
const CADDIES_CALL_GENERATORS = {
  winning_score: async (m) => ({
    kind:          "winning_score",
    question_text: "What will the winning score be?",
    type:          "single_choice",
    options: [
      { key: "under_-10",   label: "-10 or better" },
      { key: "-5_to_-10",   label: "-5 to -9" },
      { key: "E_to_-4",     label: "E to -4" },
      { key: "+1_or_worse", label: "+1 or worse" },
    ],
    resolver_data: {
      brackets: [
        { key: "under_-10",   max: -10 },
        { key: "-5_to_-10",   min: -9, max: -5 },
        { key: "E_to_-4",     min: -4, max: 0 },
        { key: "+1_or_worse", min: 1 },
      ],
    },
  }),
  hole_in_one: async () => ({
    kind:          "hole_in_one",
    question_text: "Will there be at least one hole-in-one in the field?",
    type:          "binary",
    options: [{ key: "yes", label: "Yes" }, { key: "no", label: "No" }],
    resolver_data: {},
  }),
  round_of_63: async () => ({
    kind:          "round_of_63",
    question_text: "Will any player shoot 63 or lower in any round?",
    type:          "binary",
    options: [{ key: "yes", label: "Yes" }, { key: "no", label: "No" }],
    resolver_data: { threshold: 63 },
  }),
  thirty_six_hole_leader_wins: async () => ({
    kind:          "thirty_six_hole_leader_wins",
    question_text: "Will the 36-hole leader win the tournament?",
    type:          "binary",
    options: [{ key: "yes", label: "Yes" }, { key: "no", label: "No" }],
    resolver_data: {},
  }),
  team_over_3m: async (m) => ({
    kind:          "team_over_3m",
    question_text: "Will YOUR team score more than $3M?",
    type:          "binary",
    options: [
      { key: "over",  label: "Over $3M" },
      { key: "under", label: "Under $3M" },
    ],
    resolver_data: { threshold: 3000000 },
  }),
  top10_player_pick: async (m, ctx) => {
    // Pull the field and pre-tournament OWGR ranks. Players choose one
    // from the OWGR Top 50 to "lock" as a top-10 finish.
    try {
      const owgrMap = await fetchOwgrMap();
      const fieldData = await fetchDG("/field-updates", { tour: "pga" }).catch(() => null);
      const field = Array.isArray(fieldData?.field) ? fieldData.field : [];
      const ranked = field
        .map(p => ({
          gid:   matchGolferId(p.player_name) || `dg-${p.dg_id || NORMALIZE(p.player_name)}`,
          name:  prettyName(p.player_name || ""),
          rank:  owgrMap.get(Number(p.dg_id))?.rank ?? 9999,
        }))
        .filter(r => r.rank <= 75)
        .sort((a, b) => a.rank - b.rank)
        .slice(0, 50);
      return {
        kind:          "top10_player_pick",
        question_text: "Pick a player to finish top-10",
        type:          "player_pick",
        options: ranked.map(r => ({ key: r.gid, label: `${r.name} · #${r.rank}` })),
        resolver_data: {},
      };
    } catch (err) {
      // Field not available yet — fall back to a stub.
      return {
        kind:          "top10_player_pick",
        question_text: "Pick a player to finish top-10",
        type:          "player_pick",
        options: [],
        resolver_data: { error: err.message },
      };
    }
  },
};

// Pick which generators to use for a given major. Phase 1: hardcoded to
// the 4 starter generators. Phase 3 (post-Open) replaces this with a
// scoring algorithm that picks 5 from the full ~20-question library.
function pickCaddiesCallGeneratorsForMajor(majorId, year) {
  return ["winning_score", "hole_in_one", "round_of_63", "thirty_six_hole_leader_wins", "top10_player_pick"];
}

// Run the factory for a given major + year. Idempotent: if questions
// already exist for this (major, year), returns them unchanged. Otherwise
// runs each generator and persists 5 rows in caddies_call_questions.
async function runCaddiesCallFactory(majorId, year, opts = {}) {
  if (!pool) throw new Error("DB not configured");
  const meta = MAJORS_META[majorId];
  if (!meta) throw new Error(`unknown major: ${majorId}`);
  // Already generated? Skip unless force=true (used for label/option refresh).
  const { rows: existing } = await pool.query(
    `SELECT id, slot, kind, question_text, type, options, resolver_data, locks_at, correct_answer, scored_at
       FROM caddies_call_questions
      WHERE major_id = $1 AND year = $2 AND slot <= 5
      ORDER BY slot ASC`,
    [majorId, year]
  );
  if (existing.length >= 4 && !opts.force) return existing;
  // Generate and persist.
  const generatorNames = pickCaddiesCallGeneratorsForMajor(majorId, year);
  const locksAt = meta.firstTeeTime ? new Date(meta.firstTeeTime).toISOString() : null;
  const out = [];
  for (let i = 0; i < generatorNames.length; i++) {
    const name = generatorNames[i];
    const gen  = CADDIES_CALL_GENERATORS[name];
    if (!gen) continue;
    const q = await gen(meta, { majorId, year });
    const { rows } = await pool.query(
      `INSERT INTO caddies_call_questions
         (major_id, year, slot, kind, question_text, type, options, resolver_data, locks_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9)
       ON CONFLICT (major_id, year, slot)
       DO UPDATE SET
         kind          = EXCLUDED.kind,
         question_text = EXCLUDED.question_text,
         type          = EXCLUDED.type,
         options       = EXCLUDED.options,
         resolver_data = EXCLUDED.resolver_data,
         locks_at      = EXCLUDED.locks_at,
         updated_at    = NOW()
       RETURNING id, slot, kind, question_text, type, options, resolver_data, locks_at, correct_answer, scored_at`,
      [majorId, year, i + 1, q.kind, q.question_text, q.type,
       JSON.stringify(q.options), JSON.stringify(q.resolver_data), locksAt]
    );
    out.push(rows[0]);
  }
  return out;
}

// Seed slot 11: the "personal stakes" question — Will YOUR team score more
// than $3M? Per-user resolution: each member's correct answer depends on
// their team's actual earnings, computed at scoring time.
async function runCaddiesCallBonusFactory(majorId, year) {
  if (!pool) throw new Error("DB not configured");
  const meta = MAJORS_META[majorId];
  if (!meta) throw new Error(`unknown major: ${majorId}`);
  const { rows: existing } = await pool.query(
    `SELECT id FROM caddies_call_questions
      WHERE major_id = $1 AND year = $2 AND slot = 11`,
    [majorId, year]
  );
  if (existing.length > 0) return existing;
  const gen = CADDIES_CALL_GENERATORS.team_over_3m;
  const q = await gen(meta);
  const locksAt = meta.firstTeeTime ? new Date(meta.firstTeeTime).toISOString() : null;
  const { rows } = await pool.query(
    `INSERT INTO caddies_call_questions
       (major_id, year, slot, kind, question_text, type, options, resolver_data, locks_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9)
     ON CONFLICT (major_id, year, slot)
     DO UPDATE SET
       kind          = EXCLUDED.kind,
       question_text = EXCLUDED.question_text,
       type          = EXCLUDED.type,
       options       = EXCLUDED.options,
       resolver_data = EXCLUDED.resolver_data,
       locks_at      = EXCLUDED.locks_at,
       updated_at    = NOW()
     RETURNING id, slot, kind, question_text, type, options, resolver_data, locks_at, correct_answer, scored_at`,
    [majorId, year, 11, q.kind, q.question_text, q.type,
     JSON.stringify(q.options), JSON.stringify(q.resolver_data), locksAt]
  );
  return rows;
}

// Compute a user's team total earnings for a major. Uses their PRIMARY
// league team (earliest membership). Returns 0 if they're not in any
// league or have no picks for the major.
async function computeUserTeamTotalForMajor(userId, majorId) {
  if (!pool) return 0;
  // Primary league team (earliest membership).
  const { rows: mems } = await pool.query(
    `SELECT lm.team_id FROM league_members lm
      WHERE lm.user_id = $1
      ORDER BY lm.joined_at ASC NULLS LAST, lm.id ASC
      LIMIT 1`,
    [userId]
  );
  if (!mems.length) return 0;
  const teamId = mems[0].team_id;
  const { rows: picks } = await pool.query(
    `SELECT starters, subs FROM picks WHERE team_id = $1 AND major_id = $2`,
    [teamId, majorId]
  );
  if (!picks.length) return 0;
  const starters = picks[0].starters || [];
  const subs     = picks[0].subs || [];
  // Reflect subs: replace starter gid with subbed-in gid (matches league play scoring).
  const finalStarters = starters.map((gid, i) => {
    const sub = (Array.isArray(subs) ? subs : []).find(s => s?.outIndex === i);
    return sub?.in || gid;
  });
  // Sum earnings from SNAPSHOTS.
  const snap = SNAPSHOTS[majorId];
  if (!snap) return 0;
  let total = 0;
  for (const gid of finalStarters) {
    if (!gid) continue;
    const dgId = GID_TO_DGID.get(gid);
    if (dgId == null) continue;
    const row = snap[dgId];
    if (!row) continue;
    total += Number(row.earnings || row.money || 0);
  }
  return total;
}

// Generate H2H matchup questions from DataGolf. Returns up to `count`
// questions, ordered by how close to 50/50 the matchup is (i.e., the most
// interesting ones go first). If DG returns nothing, returns [].
async function generateMatchupQuestions(majorMeta, count = 5) {
  try {
    const data = await fetchDG("/betting-tools/matchups", { tour: "pga", market: "tournament_matchups", odds_format: "decimal" }).catch(() => null);
    let pool = Array.isArray(data?.match_list) ? data.match_list
             : Array.isArray(data?.matchups)   ? data.matchups
             : Array.isArray(data)             ? data
             : [];
    if (pool.length === 0) {
      // Fall back to round 1 matchups if tournament-level isn't available.
      const data2 = await fetchDG("/betting-tools/matchups", { tour: "pga", market: "round_matchups", round: 1, odds_format: "decimal" }).catch(() => null);
      pool = Array.isArray(data2?.match_list) ? data2.match_list
           : Array.isArray(data2?.matchups)   ? data2.matchups
           : Array.isArray(data2)             ? data2
           : [];
    }
    if (pool.length === 0) return [];
    // Closer to 50/50 = more interesting. Score by absolute distance from 0.5.
    const scored = pool.map(m => {
      const p1 = Number(m.odds?.datagolf?.p1 ?? m.dg_p1_prob ?? 0.5);
      const p1Prob = p1 > 1 ? 1 / p1 : p1; // handle decimal odds vs probability
      return { m, dist: Math.abs(p1Prob - 0.5) };
    });
    scored.sort((a, b) => a.dist - b.dist);
    const selected = scored.slice(0, count).map(s => s.m);
    return selected.map(m => {
      const p1Raw = m.p1_player_name || m.player_1 || "";
      const p2Raw = m.p2_player_name || m.player_2 || "";
      const p1Name = prettyName(p1Raw);
      const p2Name = prettyName(p2Raw);
      const p1Gid = matchGolferId(p1Raw) || `dg-${m.p1_dg_id || m.dg_id_1 || NORMALIZE(p1Raw)}`;
      const p2Gid = matchGolferId(p2Raw) || `dg-${m.p2_dg_id || m.dg_id_2 || NORMALIZE(p2Raw)}`;
      return {
        kind:          "head_to_head_matchup",
        question_text: `Lower 4-round total: ${p1Name} or ${p2Name}?`,
        type:          "binary",
        options: [
          { key: p1Gid, label: p1Name },
          { key: p2Gid, label: p2Name },
        ],
        resolver_data: { p1Gid, p2Gid, p1Name, p2Name },
      };
    });
  } catch (err) {
    console.warn("[matchup gen] failed:", err.message);
    return [];
  }
}

// Run the matchups factory: seeds slots 6-10 with H2H matchup questions.
// Idempotent — re-running won't duplicate. Each slot is preserved if already
// populated.
async function runCaddiesCallMatchupsFactory(majorId, year) {
  if (!pool) throw new Error("DB not configured");
  const meta = MAJORS_META[majorId];
  if (!meta) throw new Error(`unknown major: ${majorId}`);
  const { rows: existing } = await pool.query(
    `SELECT id, slot FROM caddies_call_questions
      WHERE major_id = $1 AND year = $2 AND slot BETWEEN 6 AND 10`,
    [majorId, year]
  );
  if (existing.length >= 5) return existing;
  const questions = await generateMatchupQuestions(meta, 5);
  if (questions.length === 0) {
    throw new Error("DataGolf returned no matchups (probably not published yet)");
  }
  const locksAt = meta.firstTeeTime ? new Date(meta.firstTeeTime).toISOString() : null;
  const out = [];
  for (let i = 0; i < questions.length; i++) {
    const q = questions[i];
    const slot = 6 + i;
    const { rows } = await pool.query(
      `INSERT INTO caddies_call_questions
         (major_id, year, slot, kind, question_text, type, options, resolver_data, locks_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9)
       ON CONFLICT (major_id, year, slot)
       DO UPDATE SET
         kind          = EXCLUDED.kind,
         question_text = EXCLUDED.question_text,
         type          = EXCLUDED.type,
         options       = EXCLUDED.options,
         resolver_data = EXCLUDED.resolver_data,
         locks_at      = EXCLUDED.locks_at,
         updated_at    = NOW()
       RETURNING id, slot, kind, question_text, type, options, resolver_data, locks_at, correct_answer, scored_at`,
      [majorId, year, slot, q.kind, q.question_text, q.type,
       JSON.stringify(q.options), JSON.stringify(q.resolver_data), locksAt]
    );
    out.push(rows[0]);
  }
  return out;
}

// ─────────── Caddie's Call routes (dormant — no frontend uses these yet) ─
// GET  /api/caddies-call/:majorId        → list questions + current user's answers
// PUT  /api/caddies-call/:majorId/:slot   → submit/update an answer (locks at first tee)
// POST /api/admin/caddies-call/:majorId/run-factory → seed questions (manual trigger)
function ccYearForMajor(majorId) {
  const meta = MAJORS_META[majorId];
  if (meta?.firstTeeTime) return new Date(meta.firstTeeTime).getFullYear();
  return new Date().getFullYear();
}

app.get("/api/caddies-call/:majorId", requireAuth, async (req, res, next) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  // Skip reserved sub-paths so sibling routes handle them (Express matches
  // in definition order; /hall-of-fame would otherwise match :majorId first).
  if (majorId === "hall-of-fame") return next();
  if (!MAJORS_META[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  try {
    const year = ccYearForMajor(majorId);
    const { rows: questions } = await pool.query(
      `SELECT id, slot, kind, question_text, type, options, locks_at, correct_answer, scored_at
         FROM caddies_call_questions
        WHERE major_id = $1 AND year = $2
        ORDER BY slot ASC`,
      [majorId, year]
    );
    const { rows: myAnswers } = await pool.query(
      `SELECT a.question_id, a.answer, a.submitted_at, a.updated_at
         FROM caddies_call_answers a
         JOIN caddies_call_questions q ON q.id = a.question_id
        WHERE a.user_id = $1 AND q.major_id = $2 AND q.year = $3`,
      [req.user.id, majorId, year]
    );
    const answerMap = Object.fromEntries(myAnswers.map(a => [a.question_id, a]));
    res.json({
      majorId, year,
      locksAt: questions[0]?.locks_at || null,
      questions: questions.map(q => ({
        id:             Number(q.id),
        slot:           q.slot,
        kind:           q.kind,
        questionText:   q.question_text,
        type:           q.type,
        options:        q.options || [],
        correctAnswer:  q.correct_answer,
        scoredAt:       q.scored_at,
        myAnswer:       answerMap[q.id]?.answer || null,
        mySubmittedAt:  answerMap[q.id]?.submitted_at || null,
      })),
    });
  } catch (err) {
    console.error("[GET caddies-call] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

app.put("/api/caddies-call/:majorId/:slot", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  const slot = parseInt(req.params.slot, 10);
  if (!MAJORS_META[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  if (!slot || slot < 1 || slot > 11) return res.status(400).json({ error: "slot must be 1-11" });
  const { answer } = req.body || {};
  if (!answer || typeof answer !== "string") return res.status(400).json({ error: "answer required" });
  try {
    const year = ccYearForMajor(majorId);
    const { rows } = await pool.query(
      `SELECT id, locks_at FROM caddies_call_questions
        WHERE major_id = $1 AND year = $2 AND slot = $3`,
      [majorId, year, slot]
    );
    if (!rows.length) return res.status(404).json({ error: "question not found" });
    const q = rows[0];
    if (q.locks_at && new Date(q.locks_at) <= new Date()) {
      return res.status(409).json({ error: "Caddie's Call locks at first tee" });
    }
    await pool.query(
      `INSERT INTO caddies_call_answers (user_id, question_id, answer)
       VALUES ($1, $2, $3)
       ON CONFLICT (user_id, question_id)
       DO UPDATE SET answer = EXCLUDED.answer, updated_at = NOW()`,
      [req.user.id, q.id, answer]
    );
    res.json({ ok: true, questionId: Number(q.id), answer });
  } catch (err) {
    console.error("[PUT caddies-call] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// Resolve a single Caddie's Call question against tournament outcomes.
// Returns the correct answer key, or null if it can't be resolved yet.
// All resolvers read from SNAPSHOTS (which is the authoritative source for
// completed majors). Add new question types here as more generators ship.
async function resolveCaddiesCallQuestion(question, majorId) {
  const snap = SNAPSHOTS[majorId];
  if (!snap) return null;
  const allEntries = Object.entries(snap);
  if (allEntries.length === 0) return null;
  // Helper: per-player total stroke counts and finish positions from snapshot.
  // SNAPSHOTS[majorId][dgId] is nested by round: { 1: {roundScore, position,
  // status, ...}, 2: {...}, 3: {...}, 4: {...} }. Read the roundScore from
  // each round object. Position/status come from the latest available round.
  const finalScores = allEntries
    .map(([dgIdStr, s]) => {
      const dgId = Number(dgIdStr);
      const gid  = DGID_TO_GID.get(dgId) || `dg-${dgId}`;
      const r1   = s[1]?.roundScore ?? null;
      const r2   = s[2]?.roundScore ?? null;
      const r3   = s[3]?.roundScore ?? null;
      const r4   = s[4]?.roundScore ?? null;
      const total = [r1, r2, r3, r4].filter(x => typeof x === "number").reduce((a, b) => a + b, 0);
      const latest = s[4] || s[3] || s[2] || s[1] || {};
      return {
        dgId, gid, r1, r2, r3, r4, total,
        finalPosition: latest.position ?? null,
        status: latest.status || "ok",
      };
    })
    .filter(p => p.r1 != null && p.r2 != null);
  switch (question.kind) {
    case "winning_score": {
      // Lowest 4-round total → score vs par. Par 70 for U.S. Open Shinnecock;
      // for Phase 1 we hardcode 280 (4 × 70). Phase 2 puts par in MAJORS_META.
      const par = 280;
      const finished = finalScores.filter(p => p.r3 != null && p.r4 != null);
      if (!finished.length) return null;
      const winner = finished.reduce((a, b) => a.total < b.total ? a : b);
      const scoreToPar = winner.total - par;
      const brackets = question.resolver_data?.brackets || [];
      for (const b of brackets) {
        if ((b.min == null || scoreToPar >= b.min) && (b.max == null || scoreToPar <= b.max)) {
          return b.key;
        }
      }
      return null;
    }
    case "hole_in_one": {
      // Phase 1: stub. The snapshot doesn't track ace events directly. For
      // now, count it as "no" once tournament wraps. Phase 2: parse DG's
      // per-hole event log when we ingest it.
      const finished = finalScores.filter(p => p.r4 != null);
      if (!finished.length) return null;
      return "no";
    }
    case "round_of_63": {
      // Yes if any round score is <= 63 (assuming par 70 → -7).
      const lowest = finalScores
        .flatMap(p => [p.r1, p.r2, p.r3, p.r4].filter(x => typeof x === "number"));
      if (!lowest.length) return null;
      return Math.min(...lowest) <= 63 ? "yes" : "no";
    }
    case "thirty_six_hole_leader_wins": {
      // Player with lowest r1+r2 vs player with lowest r1+r2+r3+r4.
      const after36 = finalScores
        .filter(p => p.r1 != null && p.r2 != null)
        .map(p => ({ ...p, t36: p.r1 + p.r2 }))
        .sort((a, b) => a.t36 - b.t36);
      const finished = finalScores
        .filter(p => p.r3 != null && p.r4 != null)
        .sort((a, b) => a.total - b.total);
      if (!after36.length || !finished.length) return null;
      const leader36 = after36[0];
      const winner   = finished[0];
      return leader36.gid === winner.gid ? "yes" : "no";
    }
    case "top10_player_pick": {
      // Each user's answer is a gid. We don't return a single "correct"
      // answer — the scoring loop handles this case by checking each
      // user's answer against the actual top-10 list. Return a marker.
      const finished = finalScores
        .filter(p => p.r3 != null && p.r4 != null)
        .sort((a, b) => a.total - b.total)
        .slice(0, 10);
      if (!finished.length) return null;
      // Pack the top-10 gids into the correct_answer field as a JSON array.
      return JSON.stringify(finished.map(p => p.gid));
    }
    case "head_to_head_matchup": {
      // Whoever has the lower total wins. Both must have completed the
      // tournament (or both DNF in same way) for the question to resolve.
      const { p1Gid, p2Gid } = question.resolver_data || {};
      if (!p1Gid || !p2Gid) return null;
      const p1 = finalScores.find(p => p.gid === p1Gid);
      const p2 = finalScores.find(p => p.gid === p2Gid);
      if (!p1 || !p2) return null;
      // If one missed cut and other made it, made-cut wins.
      const p1Made = p1.r3 != null && p1.r4 != null;
      const p2Made = p2.r3 != null && p2.r4 != null;
      if (p1Made && !p2Made) return p1Gid;
      if (p2Made && !p1Made) return p2Gid;
      if (!p1Made && !p2Made) return "push";
      if (p1.total < p2.total) return p1Gid;
      if (p2.total < p1.total) return p2Gid;
      return "push";
    }
    default:
      return null;
  }
}

// Score a single user's answer against a resolved question. Returns true /
// false. Handles the player_pick case (correct_answer is a JSON array).
function scoreCaddiesCallAnswer(question, userAnswer) {
  if (!question.correct_answer || !userAnswer) return false;
  if (question.kind === "top10_player_pick") {
    try { return JSON.parse(question.correct_answer).includes(userAnswer); }
    catch { return false; }
  }
  return question.correct_answer === userAnswer;
}

// Score all unscored questions for a major + rebuild per-user results rows.
// Idempotent — re-running won't double-count.
async function runCaddiesCallScoring(majorId) {
  if (!pool) throw new Error("DB not configured");
  const year = ccYearForMajor(majorId);
  // 1. Resolve every unscored question.
  const { rows: questions } = await pool.query(
    `SELECT id, kind, options, resolver_data, correct_answer, scored_at
       FROM caddies_call_questions
      WHERE major_id = $1 AND year = $2`,
    [majorId, year]
  );
  let resolvedCount = 0;
  for (const q of questions) {
    if (q.correct_answer) continue;
    // team_over_3m has no single correct answer — it's per-user. Mark it as
    // "PER_USER" so the scoring loop knows it's ready to evaluate, then the
    // per-user branch in scoring computes each member's actual outcome.
    if (q.kind === "team_over_3m") {
      // Only mark scoreable once snapshots actually exist for the major.
      if (SNAPSHOTS[majorId] && Object.keys(SNAPSHOTS[majorId]).length > 0) {
        await pool.query(
          `UPDATE caddies_call_questions SET correct_answer = 'PER_USER', scored_at = NOW(), updated_at = NOW()
            WHERE id = $1`,
          [q.id]
        );
        resolvedCount++;
      }
      continue;
    }
    const answer = await resolveCaddiesCallQuestion({
      id: q.id, kind: q.kind, options: q.options, resolver_data: q.resolver_data
    }, majorId);
    if (answer == null) continue;
    await pool.query(
      `UPDATE caddies_call_questions SET correct_answer = $1, scored_at = NOW(), updated_at = NOW()
        WHERE id = $2`,
      [answer, q.id]
    );
    resolvedCount++;
  }
  // 2. Rebuild per-user results.
  const { rows: scoredQs } = await pool.query(
    `SELECT id, kind, correct_answer FROM caddies_call_questions
      WHERE major_id = $1 AND year = $2 AND correct_answer IS NOT NULL`,
    [majorId, year]
  );
  if (!scoredQs.length) return { resolved: resolvedCount, usersScored: 0 };
  const questionIds = scoredQs.map(q => q.id);
  const { rows: allAnswers } = await pool.query(
    `SELECT user_id, question_id, answer
       FROM caddies_call_answers
      WHERE question_id = ANY($1::bigint[])`,
    [questionIds]
  );
  const qById = Object.fromEntries(scoredQs.map(q => [q.id, q]));
  // Pre-compute team totals for users who answered the team_over_3m question
  // — that one's per-user, can't be resolved with a single correct answer.
  const teamQuestionIds = scoredQs.filter(q => q.kind === "team_over_3m").map(q => q.id);
  const teamTotals = {};
  if (teamQuestionIds.length > 0) {
    const { rows: relevantUsers } = await pool.query(
      `SELECT DISTINCT user_id FROM caddies_call_answers WHERE question_id = ANY($1::bigint[])`,
      [teamQuestionIds]
    );
    for (const u of relevantUsers) {
      teamTotals[u.user_id] = await computeUserTeamTotalForMajor(u.user_id, majorId);
    }
  }
  const byUser = {};
  for (const a of allAnswers) {
    const q = qById[a.question_id];
    if (!q) continue;
    if (!byUser[a.user_id]) byUser[a.user_id] = { correct: 0, answered: 0 };
    byUser[a.user_id].answered += 1;
    // team_over_3m: per-user — compare their team total to threshold.
    if (q.kind === "team_over_3m") {
      const threshold = q.resolver_data?.threshold || 3000000;
      const total = teamTotals[a.user_id] || 0;
      const actuallyOver = total > threshold;
      const userPickedOver = a.answer === "over";
      if (userPickedOver === actuallyOver) byUser[a.user_id].correct += 1;
      continue;
    }
    if (scoreCaddiesCallAnswer(q, a.answer)) byUser[a.user_id].correct += 1;
  }
  for (const userId of Object.keys(byUser)) {
    const { correct, answered } = byUser[userId];
    await pool.query(
      `INSERT INTO caddies_call_results (user_id, major_id, year, correct, answered, scored_at)
       VALUES ($1, $2, $3, $4, $5, NOW())
       ON CONFLICT (user_id, major_id, year)
       DO UPDATE SET correct = EXCLUDED.correct, answered = EXCLUDED.answered, scored_at = NOW()`,
      [userId, majorId, year, correct, answered]
    );
  }
  return { resolved: resolvedCount, usersScored: Object.keys(byUser).length };
}

// Admin-only: trigger scoring manually. Will be wired to the Sunday-night
// scoring job in Phase 4 polish.
app.post("/api/admin/caddies-call/:majorId/score", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  if (!MAJORS_META[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  try {
    const result = await runCaddiesCallScoring(majorId);
    res.json({ ok: true, ...result });
  } catch (err) {
    console.error("[POST admin/caddies-call/score] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/me/caddies-call/summary — current user's lifetime Call Rate.
app.get("/api/me/caddies-call/summary", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const { rows } = await pool.query(
      `SELECT COALESCE(SUM(correct),0)::int AS correct,
              COALESCE(SUM(answered),0)::int AS answered,
              COUNT(*)::int AS majors_played
         FROM caddies_call_results
        WHERE user_id = $1`,
      [req.user.id]
    );
    const r = rows[0];
    res.json({
      correct:      r.correct,
      answered:     r.answered,
      majorsPlayed: r.majors_played,
      callRate:     r.answered > 0 ? Math.round((r.correct / r.answered) * 100) / 100 : null,
    });
  } catch (err) {
    console.error("[GET me/caddies-call/summary] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/caddies-call/:majorId/averages — clubhouse-wide consensus on
// every question for a major. Each row carries the question + the count
// of members who picked each option, plus the total submitter count.
// Used by the new Clubhouse CaddiesCallCard's averages dropdown.
app.get("/api/caddies-call/:majorId/averages", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  if (!MAJORS_META[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  try {
    const year = ccYearForMajor(majorId);
    const { rows: questions } = await pool.query(
      `SELECT id, slot, kind, question_text, type, options, correct_answer, scored_at
         FROM caddies_call_questions
        WHERE major_id = $1 AND year = $2
        ORDER BY slot ASC`,
      [majorId, year]
    );
    const qIds = questions.map(q => Number(q.id));
    let countsByQuestion = {};
    if (qIds.length > 0) {
      const { rows: tallies } = await pool.query(
        `SELECT question_id, answer, COUNT(*)::int AS n
           FROM caddies_call_answers
          WHERE question_id = ANY($1::int[])
          GROUP BY question_id, answer`,
        [qIds]
      );
      countsByQuestion = tallies.reduce((acc, t) => {
        const k = Number(t.question_id);
        if (!acc[k]) acc[k] = {};
        acc[k][t.answer] = t.n;
        return acc;
      }, {});
    }
    // Total distinct submitters for the major (anyone who answered ≥1 Q)
    const { rows: subRows } = await pool.query(
      `SELECT COUNT(DISTINCT a.user_id)::int AS n
         FROM caddies_call_answers a
         JOIN caddies_call_questions q ON q.id = a.question_id
        WHERE q.major_id = $1 AND q.year = $2`,
      [majorId, year]
    );
    const totalSubmitters = subRows[0]?.n || 0;
    res.json({
      majorId, year, totalSubmitters,
      questions: questions.map(q => {
        const id = Number(q.id);
        const tallies = countsByQuestion[id] || {};
        const totalThisQ = Object.values(tallies).reduce((a, b) => a + b, 0);
        const opts = (q.options || []).map(o => ({
          key:   o.key,
          label: o.label,
          count: tallies[o.key] || 0,
          pct:   totalThisQ > 0 ? Math.round(((tallies[o.key] || 0) / totalThisQ) * 1000) / 10 : 0,
        }));
        return {
          id, slot: q.slot, kind: q.kind,
          questionText:  q.question_text,
          type:          q.type,
          options:       opts,
          totalAnswers:  totalThisQ,
          correctAnswer: q.correct_answer,
          scoredAt:      q.scored_at,
        };
      }),
    });
  } catch (err) {
    console.error("[GET caddies-call/averages] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/caddies-call/hall-of-fame — clubhouse-wide career leaderboard.
// Surfaces top callers by Call Rate (min 5 answered) and by total wins.
// Used by the new Clubhouse CaddiesCallCard's HoF dropdown.
app.get("/api/caddies-call/hall-of-fame", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  try {
    const minAnswered = 5;
    const { rows } = await pool.query(
      `SELECT u.id AS user_id,
              u.first_name, u.last_name, u.display_name, u.member_number,
              COALESCE(SUM(r.correct),0)::int  AS correct,
              COALESCE(SUM(r.answered),0)::int AS answered,
              COUNT(r.major_id)::int            AS majors_played
         FROM users u
         LEFT JOIN caddies_call_results r ON r.user_id = u.id
        GROUP BY u.id, u.first_name, u.last_name, u.display_name, u.member_number
       HAVING COALESCE(SUM(r.answered),0) >= $1
        ORDER BY (COALESCE(SUM(r.correct),0)::float
                / NULLIF(COALESCE(SUM(r.answered),0),0)) DESC NULLS LAST,
                 COALESCE(SUM(r.correct),0) DESC`,
      [minAnswered]
    );
    const fmtName = (r) => {
      const f = r.first_name, l = r.last_name;
      if (f && l) return `${f} ${l[0].toUpperCase()}.`;
      return r.display_name || `Member #${r.member_number || "?"}`;
    };
    const callers = rows.map(r => ({
      userId:       Number(r.user_id),
      name:         fmtName(r),
      memberNumber: r.member_number,
      correct:      r.correct,
      answered:     r.answered,
      majorsPlayed: r.majors_played,
      callRate:     r.answered > 0 ? Math.round((r.correct / r.answered) * 1000) / 10 : null,
    }));
    res.json({
      minAnswered,
      topCallRate: callers.slice(0, 10),
      topWins:     [...callers].sort((a, b) => b.correct - a.correct).slice(0, 10),
    });
  } catch (err) {
    console.error("[GET caddies-call/hall-of-fame] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// Admin-only: trigger the bonus factory (slot 11 — "Will your team score
// more than $3M?"). Idempotent.
app.post("/api/admin/caddies-call/:majorId/run-bonus-factory", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  if (!MAJORS_META[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  try {
    const year = ccYearForMajor(majorId);
    const result = await runCaddiesCallBonusFactory(majorId, year);
    res.json({ ok: true, majorId, year, bonusCount: result.length });
  } catch (err) {
    console.error("[POST admin/caddies-call/run-bonus-factory] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// Admin-only: trigger the matchups factory (slots 6-10). Pulls live H2H
// matchups from DataGolf, picks the 5 closest-to-50/50 ones.
app.post("/api/admin/caddies-call/:majorId/run-matchups-factory", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  if (!MAJORS_META[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  try {
    const year = ccYearForMajor(majorId);
    const result = await runCaddiesCallMatchupsFactory(majorId, year);
    res.json({ ok: true, majorId, year, matchupCount: result.length });
  } catch (err) {
    console.error("[POST admin/caddies-call/run-matchups-factory] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// Admin-only: trigger the factory manually. Will become an automatic
// cron-triggered run at picks-open Monday in Phase 3.
app.post("/api/admin/caddies-call/:majorId/run-factory", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  if (!MAJORS_META[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  try {
    const year = ccYearForMajor(majorId);
    // ?force=true (or {force:true} body) refreshes existing questions — used
    // when generator labels/options change after seeding.
    const force = req.query.force === "true" || req.body?.force === true;
    const result = await runCaddiesCallFactory(majorId, year, { force });
    res.json({ ok: true, majorId, year, questionCount: result.length, refreshed: force });
  } catch (err) {
    console.error("[POST admin/caddies-call/run-factory] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

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

// Diagnostic: what does DataGolf currently think is in-play, and does
// its event_id match the one we have hardcoded? Used when the leaderboard
// is showing empty during a major we KNOW is underway — almost always a
// stale event_id mapping.
app.get("/api/admin/datagolf-diag", async (req, res) => {
  try {
    const [inPlay, field] = await Promise.all([
      fetchDG("/preds/in-play", { tour: "pga", dead_heat: "yes", odds_format: "percent" }).catch(e => ({ error: e.message })),
      fetchDG("/field-updates", { tour: "pga" }).catch(e => ({ error: e.message })),
    ]);
    const inPlayPlayers = Array.isArray(inPlay?.data) ? inPlay.data
                        : Array.isArray(inPlay) ? inPlay : [];
    const fieldPlayerCount = Array.isArray(field?.field) ? field.field.length
                           : Array.isArray(field?.data)  ? field.data.length : 0;
    // Sample one player to see what scoring fields DG is populating right now.
    const sample = inPlayPlayers[0] || null;
    const sampleSummary = sample ? {
      player_name:   sample.player_name,
      current_pos:   sample.current_pos ?? null,
      current_score: sample.current_score ?? null,
      thru:          sample.thru ?? null,
      R1:            sample.R1 ?? sample.r1 ?? null,
      keys:          Object.keys(sample),
    } : null;
    res.json({
      expected: DG_EVENT_IDS,
      inPlay: {
        event_id:     inPlay?.event_id ?? inPlay?.eventId ?? null,
        event_name:   inPlay?.event_name ?? null,
        current_round: inPlay?.current_round ?? inPlay?.round ?? null,
        last_updated: inPlay?.last_updated ?? null,
        player_count: inPlayPlayers.length,
        sample:       sampleSummary,
        error:        inPlay?.error,
      },
      fieldUpdates: {
        event_id:     field?.event_id ?? field?.eventId ?? null,
        event_name:   field?.event_name ?? null,
        last_updated: field?.last_updated ?? null,
        player_count: fieldPlayerCount,
        // Sample to verify am field shape on /field-updates
        sample: (() => {
          const list = Array.isArray(field?.field) ? field.field
                     : Array.isArray(field?.data)  ? field.data : [];
          const s = list[0];
          if (!s) return null;
          return {
            player_name: s.player_name,
            dg_id:       s.dg_id,
            am:          s.am ?? null,
            amateur:     s.amateur ?? null,
            keys:        Object.keys(s),
          };
        })(),
        // Count of amateurs found in field-updates
        amateur_count: (() => {
          const list = Array.isArray(field?.field) ? field.field
                     : Array.isArray(field?.data)  ? field.data : [];
          return list.filter(p =>
            p.am === 1 || p.am === true || p.amateur === true || p.amateur === 1
          ).length;
        })(),
        error:        field?.error,
      },
    });
  } catch (err) {
    console.error("[datagolf-diag] failed:", err);
    res.status(500).json({ error: err.message });
  }
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
    // DG's Scratch+ tier sometimes returns current_round=null even with a
    // populated field. Infer from the field instead: highest R{N} with any
    // recorded scores is the current round.
    let currentRound = data?.current_round || data?.round || null;
    if (currentRound == null && players.length > 0) {
      const hasR = (r) => players.some(p => p[`R${r}`] != null || p[`r${r}`] != null);
      if (hasR(4))      currentRound = 4;
      else if (hasR(3)) currentRound = 3;
      else if (hasR(2)) currentRound = 2;
      else if (hasR(1)) currentRound = 1;
    }

    // Keep name maps warm regardless of which tournament in-play is showing.
    hydrateNameMaps(players);

    // Detect if DataGolf's current in-play IS this major. Primary signal:
    // in-play's own event_id field. Fallback signal: /field-updates says
    // this major is "the current week" — useful when in-play hasn't flipped
    // into live mode yet (early Thursday morning, no leader through 9 etc.)
    // but the field roster is already loaded.
    const inPlayEventId = data?.event_id ?? data?.eventId ?? data?.event ?? null;
    let isThisMajorLive = inPlayEventId != null && Number(inPlayEventId) === Number(eventId);

    // Always pull field-updates (cached): gives us amateur status that
    // /preds/in-play doesn't expose on Scratch+, plus an event_id fallback
    // when in-play hasn't flipped to live mode yet.
    let fieldData = null;
    try {
      fieldData = await cached("field-current", CACHE_TTL, () =>
        fetchDG("/field-updates", { tour: "pga" }).catch(() => null)
      );
    } catch (_) { /* swallow */ }
    const fieldList = Array.isArray(fieldData?.field) ? fieldData.field
                    : Array.isArray(fieldData?.data)  ? fieldData.data : [];
    const fieldAmateurs = {};
    for (const fp of fieldList) {
      if (fp?.dg_id == null) continue;
      const isAm = fp.am === 1 || fp.am === true || fp.amateur === true || fp.amateur === 1;
      if (isAm) fieldAmateurs[fp.dg_id] = true;
    }

    if (!isThisMajorLive && players.length >= 50) {
      // Field-updates confirms which event DG considers current. If it's
      // this major AND in-play has the full field with scoring fields
      // present on at least some rows, accept it as live.
      const fieldEventId = fieldData?.event_id ?? fieldData?.eventId ?? null;
      if (fieldEventId != null && Number(fieldEventId) === Number(eventId)) {
        const hasLiveFields = players.some(p =>
          p.current_pos != null || p.current_score != null || p.thru != null ||
          p.R1 != null || p.r1 != null
        );
        if (hasLiveFields) isThisMajorLive = true;
      }
    }

    // Only snapshot when in-play IS this major. Otherwise we'd corrupt this
    // major's final R4 snapshot with a totally different tournament's data.
    if (isThisMajorLive) {
      recordRoundSnapshotInPlay(majorId, currentRound, players);
    }

    let golfers;
    if (isThisMajorLive) {
      // LIVE MODE — build from in-play + snapshot money overlay
      // Build position-based money map once for the whole field.
      const positionMoney = buildPositionMoneyMap(players, majorId);
      golfers = {};
      for (const p of players) {
        const matchedGid = matchGolferId(p.player_name);
        const gid = matchedGid || `dg-${p.dg_id || NORMALIZE(p.player_name)}`;
        const pos = typeof p.current_pos === "number" ? `${p.current_pos}` : (p.current_pos || "");
        const posUpper = String(pos).toUpperCase();
        // Distinct status detection. Order matters — WD/DQ trump MC.
        const isWD = posUpper === "WD" || p.wd === 1 || p.wd === true;
        const isDQ = posUpper === "DQ" || p.dq === 1 || p.dq === true;
        const isMC = !isWD && !isDQ && (posUpper === "MC" || posUpper === "CUT" || p.make_cut === 0);
        const status = isWD ? "wd" : isDQ ? "dfl" : isMC ? "mc" : "made_cut";
        const moneyRounds = p.dg_id ? getPlayerRounds(majorId, p.dg_id) : { r1: null, r2: null, r3: null, r4: null };

        // Money projections. Members want "what would you cash if play
        // stopped right now" — that's position-based, not probability-based.
        // Position map handles ties (group payouts split evenly across the
        // tied slots). MC/WD/DQ → $0. If DG ever surfaces real projections
        // on this tier, those take precedence.
        const dgProj = p.prize_money_projected ?? p.proj_money ?? null;
        const computedProj = (status === "made_cut")
          ? (positionMoney[p.dg_id] ?? 0)
          : 0;
        const projMoney = dgProj != null ? dgProj : computedProj;
        golfers[gid] = {
          matched:    !!matchedGid,
          name:       prettyName(p.player_name || ""),
          country:    p.country || "",
          dg_id:      p.dg_id || null,
          // Amateur flag — primary source is /field-updates (where DG always
          // includes am), fallback is /preds/in-play's own p.am (not present
          // on Scratch+ tier). Standard golf convention: render "(a)" after.
          amateur:    (p.dg_id != null && fieldAmateurs[p.dg_id] === true)
                       || p.am === 1 || p.am === true || p.amateur === true,
          position:   isWD ? "WD" : isDQ ? "DQ" : pos,
          score:      typeof p.current_score === "number" ? p.current_score : null,
          thru:       p.thru ?? "",
          status,
          projMoney,
          projMoneySource: dgProj != null ? "datagolf" : "derived",
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

    // Emit live events (position changes, MC, WD, leader changes) by diffing
    // against the previous fetch. Stored in RECENT_EVENTS ring buffer.
    if (isThisMajorLive) {
      try {
        const prevLeader = PREV_LIVE[majorId]?.__leader || null;
        emitLiveEvents(majorId, golfers, prevLeader);
      } catch (e) { console.warn("emitLiveEvents:", e.message); }
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

// Live event feed for the major. Optional ?leagueId scopes the events to
// only golfers rostered by members of that league (your fantasy lineup
// view). No leagueId → entire field (Club Championship view).
app.get("/api/leaderboard/:majorId/events", async (req, res) => {
  if (!requireDb(res)) return;
  const { majorId } = req.params;
  if (!DG_EVENT_IDS[majorId]) return res.status(404).json({ error: `unknown major: ${majorId}` });
  const events = RECENT_EVENTS[majorId] || [];
  const leagueId = req.query.leagueId ? Number(req.query.leagueId) : null;
  let filtered = events;
  if (leagueId) {
    try {
      const { rows } = await pool.query(
        `SELECT starters, bench FROM picks WHERE league_id = $1 AND major_id = $2`,
        [leagueId, majorId]
      );
      const rosterDgIds = new Set();
      const rosterGids  = new Set();
      for (const r of rows) {
        const starters = Array.isArray(r.starters) ? r.starters : [];
        const bench    = Array.isArray(r.bench)    ? r.bench    : [];
        for (const gid of [...starters, ...bench].filter(Boolean)) {
          rosterGids.add(gid);
          const dgId = GID_TO_DGID.get(gid);
          if (dgId) rosterDgIds.add(dgId);
        }
      }
      filtered = events.filter(e =>
        (e.dgId && rosterDgIds.has(e.dgId)) ||
        (e.gid  && rosterGids.has(e.gid))
      );
    } catch (e) {
      console.warn("event filter failed:", e.message);
      filtered = events;
    }
  }
  res.json({ majorId, leagueId, events: filtered.slice(0, 8) });
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
    // Same currentRound inference as the leaderboard endpoint.
    let statsRound = lbData?.current_round || lbData?.round || null;
    if (statsRound == null && lbPlayers.length > 0) {
      const hasR = (r) => lbPlayers.some(p => p[`R${r}`] != null || p[`r${r}`] != null);
      if (hasR(4))      statsRound = 4;
      else if (hasR(3)) statsRound = 3;
      else if (hasR(2)) statsRound = 2;
      else if (hasR(1)) statsRound = 1;
    }
    recordRoundSnapshotInPlay(majorId, statsRound, lbPlayers);
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
    // historically), augment with money snapshots.
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
    // 2b. Live current-round status from in-play. Position, score-to-par, thru.
    // For a today-score we approximate: cumulative ToPar minus prior round's
    // cumulative ToPar (or current_score directly if first round).
    const curRound = lbData?.current_round || lbData?.round || null;
    let todayScoreToPar = null;
    if (curRound) {
      const cumNow = typeof inPlayPlayer.current_score === "number" ? inPlayPlayer.current_score : null;
      if (curRound === 1) {
        todayScoreToPar = cumNow;
      } else {
        const priorSnap = roundSnaps[curRound - 1];
        if (cumNow != null && priorSnap?.score != null) {
          todayScoreToPar = cumNow - priorSnap.score;
        }
      }
    }
    const current = {
      round:           curRound,
      position:        typeof inPlayPlayer.current_pos === "number"
                         ? `${inPlayPlayer.current_pos}`
                         : (inPlayPlayer.current_pos || null),
      scoreToPar:      typeof inPlayPlayer.current_score === "number"
                         ? inPlayPlayer.current_score : null,
      todayScoreToPar,
      thru:            inPlayPlayer.thru ?? null,
      teeTime:         inPlayPlayer.teetime || null,
    };

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
      current,
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
// /api/tee-times/:majorId/:round — surfaces tee times for a specific round
// of a major from DataGolf's /field-updates endpoint. Returns a map of
// golfer-id → ISO datetime + a short friendly label ("8:42 AM ET"). Used
// by the Picks tab to show the next-round tee time next to each starter
// once the previous round wraps. Cached 5 minutes — tee times rarely shift.
app.get("/api/tee-times/:majorId/:round", async (req, res) => {
  const { majorId } = req.params;
  const round = parseInt(req.params.round, 10);
  const eventId = DG_EVENT_IDS[majorId];
  if (!eventId) return res.status(404).json({ error: `unknown major: ${majorId}` });
  if (!round || round < 1 || round > 4) {
    return res.status(400).json({ error: "round must be 1-4" });
  }
  try {
    const cacheKey = `tee:${majorId}:r${round}`;
    const data = await cached(cacheKey, 5 * 60_000, () =>
      fetchDG("/field-updates", { tour: "pga" }).catch(() => null)
    );
    if (!data || Number(data.event_id ?? data.eventId) !== Number(eventId)) {
      return res.json({ majorId, round, times: {}, source: "no-field-data" });
    }
    const field = Array.isArray(data.field) ? data.field : [];
    // DataGolf returns teetimes as an array of per-round objects, each with
    // shape { round_num, teetime, start_hole, wave, course_name, ... }. We
    // find the object matching the requested round and return its details.
    // Legacy fallbacks left in for forward-compat if the schema ever changes.
    const extractTeeTime = (p, r) => {
      const tt = p.teetimes;
      if (!tt) {
        // Legacy flat keys.
        if (p[`r${r}_teetime`])  return { iso: p[`r${r}_teetime`] };
        if (p[`r${r}_tee_time`]) return { iso: p[`r${r}_tee_time`] };
        return null;
      }
      if (Array.isArray(tt)) {
        // Array of per-round objects — find by round_num.
        const row = tt.find(x => x && Number(x.round_num) === r);
        if (row?.teetime) {
          return {
            iso:       row.teetime,
            startHole: row.start_hole ?? null,
            wave:      row.wave || null,
            courseName: row.course_name || null,
          };
        }
        return null;
      }
      // Defensive: handle string/object shapes too in case DG changes.
      if (typeof tt === "string") return r === 1 ? { iso: tt } : null;
      if (typeof tt === "object") {
        const v = tt[`r${r}`] || tt[r] || tt[String(r)] || null;
        return v ? { iso: v } : null;
      }
      return null;
    };
    const times = {};
    for (const p of field) {
      const tee = extractTeeTime(p, round);
      if (!tee?.iso) continue;
      const gid = matchGolferId(p.player_name) || `dg-${p.dg_id || NORMALIZE(p.player_name)}`;
      // DataGolf returns "2026-06-18 07:52" — a naive datetime that's already
      // in ET (course local time). Parse it as ET to avoid TZ drift.
      const isoUtc = new Date(tee.iso.replace(" ", "T") + "-04:00").toISOString();
      const d = new Date(isoUtc);
      if (Number.isNaN(d.getTime())) continue;
      const label = d.toLocaleTimeString("en-US", {
        timeZone: "America/New_York", hour: "numeric", minute: "2-digit"
      }) + " ET";
      times[gid] = {
        iso:       isoUtc,
        label,
        startHole: tee.startHole,
        wave:      tee.wave,
      };
    }
    res.json({
      majorId, round,
      eventName:  data.event_name || null,
      lastUpdated: data.last_updated || null,
      fieldSize:   field.length,
      times,
    });
  } catch (err) {
    console.error("[tee-times] failed:", err.message);
    res.status(500).json({ error: err.message });
  }
});

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

    // Pull OWGR ranks for the field. OWGR is year-round so the Top 50
    // section is always populated, even before DG publishes tournament-
    // specific odds. Soft failure → empty map → frontend falls back to A-Z.
    const owgrMap = await fetchOwgrMap();

    const players = rawPlayers.map(p => {
      const matchedGid = matchGolferId(p.player_name);
      const gid = matchedGid || `dg-${p.dg_id || NORMALIZE(p.player_name)}`;
      const dgId = p.dg_id ? Number(p.dg_id) : null;
      const owgr = dgId != null ? owgrMap.get(dgId) : null;
      return {
        gid,
        name:    prettyName(p.player_name || ""),
        country: p.country || "",
        dg_id:   p.dg_id || null,
        matched: !!matchedGid,
        // favoriteRank: now driven by OWGR — 1 = world #1. Null if the
        // player isn't ranked (outside the OWGR top ~500).
        favoriteRank: owgr?.rank ?? null,
        owgrRank:     owgr?.rank ?? null,
      };
    });

    res.json({
      count: players.length,
      source,
      eventName,
      lastUpdated,
      hasFavorites: owgrMap.size > 0,
      rankingSource: "owgr",
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
  const { displayName = null, teamName = null, email = null,
          firstName = null, lastName = null } = req.body || {};
  // If an email is provided, it must look like an email. (Null/empty means
  // "no change to email" — we don't blank out a user's login by accident.)
  const cleanEmail = (typeof email === "string" && email.trim()) ? email.trim().toLowerCase() : null;
  if (cleanEmail && !isValidEmail(cleanEmail)) {
    return res.status(400).json({ error: "That email doesn't look valid." });
  }
  const client = await pool.connect();
  try {
    // Ownership: the user must own this team_id in ANY of their leagues.
    const ownership = await client.query(
      `SELECT 1 FROM league_members WHERE user_id = $1 AND team_id = $2 LIMIT 1`,
      [req.user.id, teamId]
    );
    if (!ownership.rows.length) {
      return res.status(403).json({ error: "you can only edit your own profile" });
    }
    // Uniqueness check — Postgres will throw on the UPDATE below if the
    // email is already taken by another user, but we surface a friendlier
    // 409 before we even open the transaction.
    if (cleanEmail) {
      const dupe = await client.query(
        `SELECT 1 FROM users WHERE lower(email) = $1 AND id <> $2 LIMIT 1`,
        [cleanEmail, req.user.id]
      );
      if (dupe.rows.length) {
        return res.status(409).json({ error: "That email is already in use by another account." });
      }
    }
    await client.query("BEGIN");
    // 1) Per-team profile row (display name, team name, contact email).
    await client.query(
      `INSERT INTO profiles (team_id, display_name, team_name, email, updated_at)
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT (team_id)
       DO UPDATE SET display_name = EXCLUDED.display_name,
                     team_name    = EXCLUDED.team_name,
                     email        = EXCLUDED.email,
                     updated_at   = NOW()`,
      [teamId, displayName, teamName, cleanEmail]
    );
    // 2) Login email on the user row — only if the caller actually supplied
    //    a new email (so renaming a team doesn't accidentally touch login).
    if (cleanEmail) {
      await client.query(
        `UPDATE users SET email = $1 WHERE id = $2`,
        [cleanEmail, req.user.id]
      );
    }
    // 3) Display name + first/last sync on the user row. Rules:
    //    - If the caller sent firstName / lastName, persist those.
    //    - If the caller sent displayName, treat it as a customization and
    //      mark display_name_customized = true so future name edits don't
    //      overwrite it.
    //    - If the caller updated names but NOT the display name, regenerate
    //      the default ("First L.") — but only if the user hadn't already
    //      customized their handle.
    const fName = typeof firstName === "string" ? firstName.trim() : null;
    const lName = typeof lastName  === "string" ? lastName.trim()  : null;
    if (fName !== null) {
      await client.query(`UPDATE users SET first_name = NULLIF($1, '') WHERE id = $2`, [fName, req.user.id]);
    }
    if (lName !== null) {
      await client.query(`UPDATE users SET last_name = NULLIF($1, '') WHERE id = $2`, [lName, req.user.id]);
    }
    if (typeof displayName === "string" && displayName.trim()) {
      await client.query(
        `UPDATE users SET display_name = $1, display_name_customized = true WHERE id = $2`,
        [displayName.trim(), req.user.id]
      );
    } else if (fName !== null || lName !== null) {
      // Names changed but no explicit display name — only regenerate the
      // default if the user hasn't customized their handle.
      const row = (await client.query(
        `SELECT first_name, last_name, display_name_customized FROM users WHERE id = $1`,
        [req.user.id]
      )).rows[0];
      if (row && !row.display_name_customized) {
        const next = deriveDisplayName(row.first_name, row.last_name);
        if (next) {
          await client.query(`UPDATE users SET display_name = $1 WHERE id = $2`, [next, req.user.id]);
        }
      }
    }
    await client.query("COMMIT");
    res.json({ ok: true, email: cleanEmail || undefined });
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    // Postgres unique_violation surfaces as code 23505 even if we missed it above.
    if (err.code === "23505") {
      return res.status(409).json({ error: "That email is already in use by another account." });
    }
    console.error("[PUT /api/profiles] failed:", err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// ─────────────────────────────────────────────────────────────
// Auth routes — signup, login, logout, /me, /leagues/mine
// ─────────────────────────────────────────────────────────────
// Helper: return current user + their league memberships
// Default display name = "First L." (e.g. "Austin K."). One-token names
// just become "First". Empty input → null so callers can fall back to email.
function deriveDisplayName(first, last) {
  const f = (first || "").trim();
  const l = (last  || "").trim();
  if (!f && !l) return null;
  if (!l) return f;
  return `${f} ${l[0].toUpperCase()}.`;
}

async function meAndLeagues(userId) {
  // Fault-tolerant SELECT — if first_name / last_name haven't migrated yet
  // on a given environment, fall back to the older shape so auth still works.
  let userRow;
  try {
    userRow = (await pool.query(
      `SELECT id, email, display_name, member_number, first_name, last_name, display_name_customized
         FROM users WHERE id = $1`, [userId]
    )).rows[0];
  } catch (err) {
    if (err.code === "42703" /* undefined_column */) {
      console.warn("[meAndLeagues] new columns missing — using legacy shape");
      userRow = (await pool.query(
        `SELECT id, email, display_name FROM users WHERE id = $1`, [userId]
      )).rows[0];
    } else {
      throw err;
    }
  }
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
      id:                 Number(userRow.id),
      email:              userRow.email,
      displayName:        userRow.display_name,
      firstName:          userRow.first_name || null,
      lastName:           userRow.last_name  || null,
      displayNameCustom:  !!userRow.display_name_customized,
      memberNumber:       userRow.member_number != null ? Number(userRow.member_number) : null,
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
  // leagueCode is now OPTIONAL — by default we create the account only and
  // let the user pick a league from the Clubhouse. Pass leagueCode in only
  // if you want the legacy "join on signup" behavior.
  const body = req.body || {};
  const { email, password, teamId, leagueCode = null } = body;
  const firstName = (body.firstName || "").trim();
  const lastName  = (body.lastName  || "").trim();
  // Auto-derive displayName from first/last unless the caller sends one.
  const incomingDisplay = (body.displayName || "").trim();
  const derivedDisplay  = deriveDisplayName(firstName, lastName);
  const displayName = incomingDisplay || derivedDisplay;
  if (!isValidEmail(email))            return res.status(400).json({ error: "invalid email" });
  if (!password || password.length < 6) return res.status(400).json({ error: "password must be at least 6 characters" });
  if (!firstName)                       return res.status(400).json({ error: "first name required" });
  if (!displayName) return res.status(400).json({ error: "displayName required" });

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
    // Account-only signup path: create user, issue token, return. Skip all
    // the league join + slot claim machinery.
    if (!leagueCode) {
      const u = await client.query(
        `INSERT INTO users (email, password_hash, display_name, first_name, last_name,
                            display_name_customized, member_number)
         VALUES ($1, $2, $3, $4, $5, $6, nextval('member_number_seq'))
         RETURNING id, member_number`,
        [normalizedEmail, hashPassword(password), displayName.trim(),
         firstName || null, lastName || null,
         !!incomingDisplay /* customized only if caller sent one */]
      );
      const userId = Number(u.rows[0].id);
      const token = newSessionToken();
      const expires = new Date(Date.now() + SESSION_TTL_DAYS * 24 * 60 * 60 * 1000);
      await client.query(
        `INSERT INTO sessions (token, user_id, expires_at) VALUES ($1, $2, $3)`,
        [token, userId, expires]
      );
      await client.query("COMMIT");
      const payload = await meAndLeagues(userId);
      return res.json({ token, ...payload });
    }
    // Legacy path: signup + league join in one transaction.
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
    // Create user — auto-assigns the next sequential membership number from
    // member_number_seq. nextval() is atomic so racing signups can't collide.
    const u = await client.query(
      `INSERT INTO users (email, password_hash, display_name, member_number)
       VALUES ($1, $2, $3, nextval('member_number_seq'))
       RETURNING id, member_number`,
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

// POST /api/auth/login  { identifier, password } → { token, user, leagues }
//   identifier = email address OR membership number (with or without leading #)
//   Backwards-compatible: also accepts { email, password } for older clients.
app.post("/api/auth/login", async (req, res) => {
  if (!requireDb(res)) return;
  const body = req.body || {};
  const identifier = String(body.identifier ?? body.email ?? "").trim();
  const password = body.password;
  if (!identifier || !password) {
    return res.status(400).json({ error: "email or membership number + password required" });
  }
  try {
    // Decide which column to match. A bare digit string (e.g. "1") or one
    // prefixed with "#" (e.g. "#1") is treated as a membership number;
    // anything else is treated as an email.
    const numericPart = identifier.replace(/^#/, "").trim();
    const isMemberNumber = /^\d+$/.test(numericPart) && numericPart.length > 0 && numericPart.length < 12;
    let rows;
    if (isMemberNumber) {
      try {
        ({ rows } = await pool.query(
          `SELECT id, password_hash FROM users WHERE member_number = $1`,
          [Number(numericPart)]
        ));
      } catch (e) {
        if (e.code === "42703") return res.status(503).json({ error: "membership numbers not yet available — sign in with email instead" });
        throw e;
      }
    } else {
      ({ rows } = await pool.query(
        `SELECT id, password_hash FROM users WHERE lower(email) = $1`,
        [identifier.toLowerCase()]
      ));
    }
    if (!rows.length || !verifyPassword(password, rows[0].password_hash)) {
      return res.status(401).json({ error: "incorrect login or password" });
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

// POST /api/auth/change-password
//   body: { currentPassword, newPassword }
// Verifies current password, sets new hash, and revokes all OTHER sessions
// (current one survives so the user doesn't get logged out of the device
// they're using). Returns ok on success.
app.post("/api/auth/change-password", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const { currentPassword, newPassword } = req.body || {};
  if (!currentPassword || !newPassword) {
    return res.status(400).json({ error: "current and new passwords are both required" });
  }
  if (typeof newPassword !== "string" || newPassword.length < 8) {
    return res.status(400).json({ error: "new password must be at least 8 characters" });
  }
  if (newPassword === currentPassword) {
    return res.status(400).json({ error: "new password must be different from current password" });
  }
  try {
    const { rows } = await pool.query(
      `SELECT password_hash FROM users WHERE id = $1`, [req.user.id]
    );
    if (!rows.length || !verifyPassword(currentPassword, rows[0].password_hash)) {
      return res.status(401).json({ error: "current password is incorrect" });
    }
    await pool.query(
      `UPDATE users SET password_hash = $1 WHERE id = $2`,
      [hashPassword(newPassword), req.user.id]
    );
    // Security: revoke every OTHER session — anyone signed in elsewhere
    // (e.g. an old phone) gets kicked. Current device's session stays alive.
    await pool.query(
      `DELETE FROM sessions WHERE user_id = $1 AND token <> $2`,
      [req.user.id, req.token]
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("[change-password] failed:", err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/auth/request-reset
//   body: { email }
// Generates a single-use 1-hour reset token and emails the user a link.
// ALWAYS returns 200 — never leaks whether the email exists in the system
// (would let attackers enumerate registered emails). Light throttle to one
// active token per user at a time.
app.post("/api/auth/request-reset", async (req, res) => {
  if (!requireDb(res)) return;
  const email = (req.body?.email || "").trim().toLowerCase();
  if (!isValidEmail(email)) {
    // Same shape as success — don't tip attackers off.
    return res.json({ ok: true });
  }
  try {
    const { rows } = await pool.query(
      `SELECT id FROM users WHERE lower(email) = $1 LIMIT 1`, [email]
    );
    if (rows.length) {
      const userId = rows[0].id;
      // Throttle: if there's a token issued in the last 60s for this user,
      // don't issue another one. Prevents abuse via the request form.
      const recent = await pool.query(
        `SELECT 1 FROM password_resets
          WHERE user_id = $1 AND created_at > NOW() - INTERVAL '60 seconds'
          LIMIT 1`,
        [userId]
      );
      if (!recent.rows.length) {
        // Invalidate any previous unused tokens for this user — only the
        // freshest link should work.
        await pool.query(
          `UPDATE password_resets SET used_at = NOW()
            WHERE user_id = $1 AND used_at IS NULL`,
          [userId]
        );
        const token = crypto.randomBytes(32).toString("hex");
        await pool.query(
          `INSERT INTO password_resets (token, user_id, expires_at)
           VALUES ($1, $2, NOW() + INTERVAL '1 hour')`,
          [token, userId]
        );
        // Note: we route via the SPA's root with a ?reset= query param so
        // Vercel doesn't 404 on an unknown /reset path. The frontend reads
        // the query string regardless of which path it lands on.
        const link = `${FRONTEND_URL}/?reset=${encodeURIComponent(token)}`;
        // Fire the email (best-effort — don't fail the request if email errors).
        const layoutArgs = {
          preheader:  "Reset your OnlyMajors password — link expires in 1 hour.",
          heading:    "Reset your password",
          intro:      `Someone asked to reset the password for your OnlyMajors account. If that was you, use the button below to set a new one. The link expires in <strong>1 hour</strong>.`,
          ctaLabel:   "Set a new password",
          ctaUrl:     link,
          bodyHtml: `
            <p style="color:${BRAND.dim};font-size:12px;line-height:1.5;margin:18px 0 4px;">Or paste this URL into your browser:</p>
            <p style="margin:0 0 4px;word-break:break-all;font-size:12px;line-height:1.4;"><a href="${link}" style="color:${BRAND.green};text-decoration:none;">${link}</a></p>`,
          footerNote: "If you didn't request this, you can safely ignore it — your password stays the same.",
        };
        sendEmail({
          to:      email,
          subject: "Reset your OnlyMajors password",
          tag:     "password-reset",
          html:    emailHtml(layoutArgs),
          text:    emailText({ ...layoutArgs, bodyText: `Or paste this URL into your browser:\n${link}` }),
        }).catch(e => console.error("[request-reset] email send failed:", e.message));
      }
    }
    // Always 200, no matter what.
    res.json({ ok: true });
  } catch (err) {
    console.error("[request-reset] failed:", err);
    // Still return 200 to avoid leaking via timing/error.
    res.json({ ok: true });
  }
});

// POST /api/auth/reset-password
//   body: { token, newPassword }
// Validates the token (exists, not expired, not used), swaps the password
// hash, marks the token used, and revokes EVERY session for that user
// (because an unknown party may have been logged in elsewhere).
app.post("/api/auth/reset-password", async (req, res) => {
  if (!requireDb(res)) return;
  const { token, newPassword } = req.body || {};
  if (!token || !newPassword) {
    return res.status(400).json({ error: "token and new password are required" });
  }
  if (typeof newPassword !== "string" || newPassword.length < 8) {
    return res.status(400).json({ error: "new password must be at least 8 characters" });
  }
  try {
    const { rows } = await pool.query(
      `SELECT user_id, expires_at, used_at FROM password_resets WHERE token = $1`,
      [token]
    );
    if (!rows.length) {
      return res.status(400).json({ error: "That reset link is invalid." });
    }
    const row = rows[0];
    if (row.used_at) {
      return res.status(400).json({ error: "That reset link has already been used." });
    }
    if (new Date(row.expires_at) < new Date()) {
      return res.status(400).json({ error: "That reset link has expired. Request a new one." });
    }
    const userId = row.user_id;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `UPDATE users SET password_hash = $1 WHERE id = $2`,
        [hashPassword(newPassword), userId]
      );
      await client.query(
        `UPDATE password_resets SET used_at = NOW() WHERE token = $1`, [token]
      );
      // Boot every session — if an attacker was already inside, they're out.
      await client.query(`DELETE FROM sessions WHERE user_id = $1`, [userId]);
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK").catch(() => {});
      throw e;
    } finally {
      client.release();
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("[reset-password] failed:", err);
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
      `INSERT INTO leagues (name, invite_code, format, scope, visibility, commissioner_id, league_type, included_majors, founded_year)
       VALUES ($1, $2, 'season_money', 'season', $3, $4, $5, $6::jsonb, EXTRACT(YEAR FROM NOW())::int)
       RETURNING id, name, invite_code, format, scope, major_id, visibility, commissioner_id, member_model, league_type, included_majors, founded_year`,
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
        foundedYear:    league.founded_year || new Date().getFullYear(),
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

// DELETE /api/leagues/:id — commissioner deletes the league. Wipes picks,
// chat, archives, and members; the leagues row goes last. EXPERTS (id=1) is
// hard-protected.
app.delete("/api/leagues/:id", requireAuth, async (req, res) => {
  if (!requireDb(res)) return;
  const leagueId = Number(req.params.id);
  if (!Number.isFinite(leagueId)) return res.status(400).json({ error: "invalid id" });
  if (leagueId === 1) return res.status(403).json({ error: "EXPERTS can't be deleted" });
  const ok = await requireCommissioner(req, res, leagueId);
  if (ok == null) return;
  try {
    await pool.query(`DELETE FROM picks           WHERE league_id = $1`, [leagueId]);
    await pool.query(`DELETE FROM chat_messages   WHERE league_id = $1`, [leagueId]);
    await pool.query(`DELETE FROM season_archives WHERE league_id = $1`, [leagueId]);
    await pool.query(`DELETE FROM league_members  WHERE league_id = $1`, [leagueId]);
    await pool.query(`DELETE FROM leagues         WHERE id = $1`, [leagueId]);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
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
              l.league_type, l.included_majors, l.founded_year,
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
    // Fetch the caller's membership number + name fields for the
    // membership-card UI. Fault-tolerant: if any of the newer columns
    // haven't migrated yet on a given environment, fall back to null so
    // the whole endpoint doesn't 500.
    let memberNumber = null, firstName = null, lastName = null, displayNameCustom = false;
    try {
      const meRow = (await pool.query(
        `SELECT member_number, first_name, last_name, display_name_customized
           FROM users WHERE id = $1`, [userId]
      )).rows[0];
      memberNumber       = meRow?.member_number != null ? Number(meRow.member_number) : null;
      firstName          = meRow?.first_name || null;
      lastName           = meRow?.last_name  || null;
      displayNameCustom  = !!meRow?.display_name_customized;
    } catch (e) {
      if (e.code === "42703") {
        console.warn("[leagues-summary] new user columns missing — returning legacy shape");
        // Fall back to just member_number on its own.
        try {
          const meRow = (await pool.query(`SELECT member_number FROM users WHERE id = $1`, [userId])).rows[0];
          memberNumber = meRow?.member_number != null ? Number(meRow.member_number) : null;
        } catch (_) {}
      } else {
        throw e;
      }
    }
    res.json({
      me: {
        memberNumber,
        firstName,
        lastName,
        displayNameCustom,
      },
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
          foundedYear:    r.founded_year   || null,
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
  // Notification sweep — runs once 30s after boot (so initSchema has settled)
  // and then every hour. Each notification type has its own day-of-week /
  // hour-of-day gate; the hourly sweep is what makes those gates fire
  // promptly. All triggers idempotent via notification_log.
  global.__notificationSweepStarted = true;
  setTimeout(() => { runNotificationSweep(); }, 30_000);
  setInterval(()  => { runNotificationSweep(); }, 60 * 60_000);
});
