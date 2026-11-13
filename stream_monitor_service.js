// stream_monitor_service.js
//
// One background loop (every 10s) that turns what is already happening in
// NLM into platform events. It only reads: channels, SRS's stream list,
// stream_target_health_history and recordings. Nothing in the streaming
// path depends on it, so if it fails, streaming is unaffected.
//
//   channels.is_live false→true   stream.started (+ encoder pre-flight ~25s later)
//   channels.is_live true→false   stream.ended, and stream.dropped when a
//                                 scheduled service still had >2 min to run
//   SRS recv bitrate              stream.degraded / stream.recovered
//   stream_target_health_history  target.failed / .recovered / .delivering /
//                                 .reconnecting / .stopped
//   recordings (new rows)         recording.ready

const { emitEvent } = require("./platform_events");
const {
  analyzeLiveStream,
  clearStreamAnalysis,
} = require("./stream_health_service");
const alertService = require("./alert_service");

const TICK_MS = 10 * 1000;
const PREFLIGHT_DELAY_MS = 25 * 1000;
const BITRATE_WINDOW = 30; // samples (~5 min) for the "normal" baseline
const BITRATE_WARMUP_SAMPLES = 6; // first ~60s after going live are ignored
const BITRATE_CONFIRM_SAMPLES = 3; // ~30s below/above threshold before acting
const DROP_GRACE_REMAINING_MS = 2 * 60 * 1000;

const SRS_API_URL = String(
  process.env.SRS_API_URL || "http://localhost:1985",
).replace(/\/$/, "");
const SRS_INTERNAL_HLS_BASE_URL =
  process.env.SRS_INTERNAL_HLS_BASE_URL || "http://127.0.0.1:8080";

let pool = null;
let timer = null;
let running = false;
let seeded = false;

const channelState = new Map(); // channelId -> { live, session, startedAt }
const bitrateState = new Map(); // channelId -> { samples, degraded, below, above, baseline }
const failedTargets = new Set(); // targetId currently failed
const preflightTimers = new Map(); // channelId -> timeout
let lastHealthId = 0;
let lastRecordingId = 0;

const settingsCache = new Map(); // orgId -> { at, value }
async function orgSettings(organizationId) {
  const hit = settingsCache.get(organizationId);
  if (hit && Date.now() - hit.at < 60000) return hit.value;
  const value = await alertService
    .getSettings(organizationId)
    .catch(() => null);
  settingsCache.set(organizationId, { at: Date.now(), value });
  return value;
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

async function fetchSrsStreams() {
  try {
    const response = await fetch(`${SRS_API_URL}/api/v1/streams/?count=1000`, {
      signal: AbortSignal.timeout(5000),
    });
    const data = await response.json();
    const map = new Map();
    for (const stream of data.streams || []) {
      if (String(stream.app || "live") !== "live") continue;
      map.set(String(stream.name), stream);
    }
    return map;
  } catch {
    return null; // SRS unreachable: skip bitrate checks this tick
  }
}

// ── encoder pre-flight ──────────────────────────────────────
function check(id, label, status, measured, message, fix) {
  return { id, label, status, measured, message, fix: fix || null };
}

async function buildPreflight(channel, srsStream, targets) {
  const uptime = channel.live_started_at
    ? Math.max(
        0,
        Math.floor(
          (Date.now() - new Date(channel.live_started_at).getTime()) / 1000,
        ),
      )
    : 0;
  // Always measure fresh: the shared analyzer caches per stream key, so a
  // reconnect with corrected OBS settings would otherwise report the old ones.
  clearStreamAnalysis(String(srsStream?.name || channel.stream_key));
  const analysis = await analyzeLiveStream({
    stream: { ...srsStream, uptime_seconds: uptime },
    internalHlsBaseUrl: SRS_INTERNAL_HLS_BASE_URL,
  });
  const video = analysis.video || {};
  const audio = analysis.audio || {};
  const keyframeSeconds = Number(analysis.keyframe_interval?.seconds) || null;
  const totalKbps = Math.round(Number(srsStream?.kbps?.recv_30s || 0));
  const platforms = new Set(
    targets.map((t) => String(t.target_type || t.platform || "").toLowerCase()),
  );
  const socialTargets = platforms.has("facebook") || platforms.has("youtube");
  const checks = [];

  // Video codec
  const codec = String(video.codec || "").toLowerCase();
  if (!codec) {
    checks.push(
      check(
        "video_codec",
        "Video",
        "fail",
        null,
        "No video track was detected.",
        "Check that a video source is active in your encoder scene.",
      ),
    );
  } else if (codec === "h264") {
    checks.push(
      check(
        "video_codec",
        "Video codec",
        "pass",
        "H.264",
        "H.264 works everywhere.",
      ),
    );
  } else {
    checks.push(
      check(
        "video_codec",
        "Video codec",
        socialTargets ? "fail" : "warn",
        codec.toUpperCase(),
        `${codec.toUpperCase()} isn't accepted by every destination.`,
        "In OBS: Settings → Output → Encoder, choose an H.264 encoder (x264 or NVENC H.264).",
      ),
    );
  }

  // Resolution
  if (video.width && video.height) {
    const res = `${video.width}×${video.height}`;
    if (video.height > 1080)
      checks.push(
        check(
          "resolution",
          "Resolution",
          "warn",
          res,
          "Above 1080p. Facebook and most viewers' connections top out at 1080p.",
          "Set the output (scaled) resolution to 1920×1080 in OBS: Settings → Video.",
        ),
      );
    else if (video.height < 720)
      checks.push(
        check(
          "resolution",
          "Resolution",
          "warn",
          res,
          "Below 720p looks soft on TVs and laptops.",
          "Set the output resolution to 1280×720 or 1920×1080 in OBS: Settings → Video.",
        ),
      );
    else
      checks.push(
        check(
          "resolution",
          "Resolution",
          "pass",
          res,
          "Good broadcast resolution.",
        ),
      );
  }

  // Frame rate
  if (video.fps) {
    const fps = Math.round(video.fps * 100) / 100;
    if (fps < 23 || fps > 61)
      checks.push(
        check(
          "fps",
          "Frame rate",
          "warn",
          `${fps} fps`,
          "Unusual frame rate; playback may stutter.",
          "Use 30 fps (or 60 for fast motion) in OBS: Settings → Video → Common FPS Values.",
        ),
      );
    else
      checks.push(
        check(
          "fps",
          "Frame rate",
          "pass",
          `${fps} fps`,
          "Frame rate is in the normal range.",
        ),
      );
  }

  // Keyframe interval
  if (keyframeSeconds) {
    const kf = `${keyframeSeconds} s`;
    if (keyframeSeconds <= 2.5)
      checks.push(
        check(
          "keyframe",
          "Keyframe interval",
          "pass",
          kf,
          "Matches the 2-second interval Facebook and YouTube expect.",
        ),
      );
    else
      checks.push(
        check(
          "keyframe",
          "Keyframe interval",
          keyframeSeconds > 4 ? "fail" : "warn",
          kf,
          "Facebook and YouTube expect a keyframe every 2 seconds. Longer intervals cause buffering and delayed starts.",
          "In OBS: Settings → Output → Output Mode: Advanced → Streaming → Keyframe Interval = 2.",
        ),
      );
  } else {
    checks.push(
      check(
        "keyframe",
        "Keyframe interval",
        "warn",
        null,
        "Couldn't measure the keyframe interval yet.",
        "Make sure Keyframe Interval is set to 2 in your encoder.",
      ),
    );
  }

  // Bitrate
  if (totalKbps > 0) {
    const tall = Number(video.height || 0);
    const min = tall >= 1080 ? 3500 : tall >= 720 ? 2000 : 800;
    if (totalKbps < min)
      checks.push(
        check(
          "bitrate",
          "Bitrate",
          "warn",
          `${totalKbps} kbps`,
          `Low for ${tall || "this"}p; the picture may look blocky.`,
          `Aim for about ${tall >= 1080 ? "4500–6000" : "2500–4000"} kbps if the venue's upload speed allows (test it at speedtest.net; keep the bitrate under 75% of upload).`,
        ),
      );
    else if (totalKbps > 9000)
      checks.push(
        check(
          "bitrate",
          "Bitrate",
          "warn",
          `${totalKbps} kbps`,
          "Very high. Many venue connections can't sustain this, and some destinations cap the bitrate.",
          "Lower the video bitrate to 4500–6000 kbps for 1080p.",
        ),
      );
    else
      checks.push(
        check(
          "bitrate",
          "Bitrate",
          "pass",
          `${totalKbps} kbps`,
          "Bitrate fits the resolution.",
        ),
      );
  } else {
    checks.push(
      check(
        "bitrate",
        "Bitrate",
        "fail",
        "0 kbps",
        "NLM isn't receiving data.",
        "Check that the encoder is streaming and connected to the internet.",
      ),
    );
  }

  // Audio
  const audioCodec = String(audio.codec || "").toLowerCase();
  if (!audioCodec) {
    checks.push(
      check(
        "audio",
        "Audio",
        "fail",
        null,
        "No audio track. Viewers will hear silence, and some platforms reject streams without audio.",
        "In OBS, make sure a Desktop Audio or Mic/Aux source is active and not muted.",
      ),
    );
  } else {
    const rate = Number(audio.sample_rate) || null;
    const detail = `${audioCodec.toUpperCase()}${rate ? ` ${rate / 1000} kHz` : ""}${audio.channels ? `, ${audio.channels === 2 ? "stereo" : audio.channels === 1 ? "mono" : `${audio.channels} ch`}` : ""}`;
    if (audioCodec !== "aac")
      checks.push(
        check(
          "audio",
          "Audio",
          "warn",
          detail,
          "AAC is the safest audio codec for live platforms.",
          "In OBS, audio is AAC by default; check any custom encoder settings.",
        ),
      );
    else if (rate && rate !== 48000 && rate !== 44100)
      checks.push(
        check(
          "audio",
          "Audio",
          "warn",
          detail,
          "Unusual sample rate.",
          "Set OBS: Settings → Audio → Sample Rate to 48 kHz.",
        ),
      );
    else
      checks.push(
        check("audio", "Audio", "pass", detail, "Audio track detected."),
      );
  }

  if (!analysis.probe_ok) {
    checks.push(
      check(
        "probe",
        "Deep check",
        "warn",
        null,
        "NLM couldn't fully inspect the stream yet; some values come from the ingest server only.",
        "Run the check again in a minute.",
      ),
    );
  }

  const fails = checks.filter((c) => c.status === "fail").length;
  const warns = checks.filter((c) => c.status === "warn").length;
  const grade = fails ? "fail" : warns ? "warn" : "pass";
  const summary = fails
    ? `${fails} problem(s) need fixing before this stream looks right on ${socialTargets ? "Facebook/YouTube" : "your destinations"}.`
    : warns
      ? `Stream is working. ${warns} setting(s) could be improved.`
      : "Encoder settings look good.";

  return {
    grade,
    summary,
    checks,
    measured: {
      video,
      audio,
      keyframe_seconds: keyframeSeconds,
      total_kbps: totalKbps,
      analyzer_source: analysis.analyzer_source,
      analyzed_at: analysis.analyzed_at,
    },
  };
}

async function runPreflight(channelId, { session = null, emit = true } = {}) {
  const channelResult = await pool.query(
    `SELECT id, organization_id, name, stream_key, is_live, live_started_at FROM channels WHERE id = $1`,
    [channelId],
  );
  const channel = channelResult.rows[0];
  if (!channel) return { ok: false, message: "Channel not found" };
  if (!channel.is_live)
    return {
      ok: false,
      code: "offline",
      message: "Start your encoder first; the check needs a live stream.",
    };

  const srs = await fetchSrsStreams();
  const srsStream = srs?.get(String(channel.stream_key));
  if (!srsStream)
    return {
      ok: false,
      code: "no_ingest",
      message:
        "NLM's ingest server doesn't see this stream yet. Try again in a few seconds.",
    };

  const targets = (
    await pool.query(
      `SELECT platform, target_type FROM social_destinations WHERE channel_id = $1 AND enabled = TRUE`,
      [channel.id],
    )
  ).rows;

  const report = await buildPreflight(channel, srsStream, targets);
  const liveSession =
    session ||
    (channel.live_started_at
      ? new Date(channel.live_started_at).toISOString()
      : null);
  const saved = await pool.query(
    `INSERT INTO channel_preflight_reports
       (organization_id, channel_id, live_session, grade, summary, checks, measured)
     VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id, created_at`,
    [
      channel.organization_id,
      channel.id,
      liveSession,
      report.grade,
      report.summary,
      JSON.stringify(report.checks),
      JSON.stringify(report.measured),
    ],
  );

  if (emit) {
    await emitEvent({
      organizationId: channel.organization_id,
      channelId: channel.id,
      type: "stream.preflight.completed",
      severity: report.grade === "fail" ? "warning" : "info",
      title: `Encoder check: ${report.summary}`,
      data: {
        channel_name: channel.name,
        grade: report.grade,
        summary: report.summary,
        checks: report.checks,
        report_id: saved.rows[0].id,
      },
    });
  }
  return {
    ok: true,
    report: {
      id: saved.rows[0].id,
      created_at: saved.rows[0].created_at,
      ...report,
    },
  };
}

function schedulePreflight(channel, session) {
  clearTimeout(preflightTimers.get(channel.id));
  const t = setTimeout(async () => {
    preflightTimers.delete(channel.id);
    const state = channelState.get(channel.id);
    if (!state?.live || state.session !== session) return;
    try {
      const result = await runPreflight(channel.id, { session });
      if (!result.ok && result.code === "no_ingest") {
        // One retry: SRS can lag a few seconds behind is_live.
        setTimeout(
          () => runPreflight(channel.id, { session }).catch(() => {}),
          20000,
        ).unref?.();
      }
    } catch (error) {
      console.error(
        `[MONITOR] Pre-flight failed for channel ${channel.id}:`,
        error.message,
      );
    }
  }, PREFLIGHT_DELAY_MS);
  t.unref?.();
  preflightTimers.set(channel.id, t);
}

// ── live transitions ────────────────────────────────────────
async function scheduledWindowStillOpen(channelId) {
  const result = await pool
    .query(
      `SELECT title, scheduled_end FROM scheduled_streams
       WHERE channel_id = $1
         AND scheduled_start <= NOW()
         AND scheduled_end IS NOT NULL
         AND scheduled_end > NOW() + ($2 || ' milliseconds')::interval
         AND COALESCE(status, 'scheduled') NOT IN ('cancelled', 'canceled', 'completed', 'failed')
       ORDER BY scheduled_start DESC LIMIT 1`,
      [channelId, String(DROP_GRACE_REMAINING_MS)],
    )
    .catch(() => ({ rows: [] }));
  return result.rows[0] || null;
}

async function checkChannels(srs) {
  const result = await pool.query(
    `SELECT id, organization_id, name, stream_key, is_live, live_started_at FROM channels`,
  );
  const seen = new Set();
  for (const channel of result.rows) {
    seen.add(channel.id);
    const live = Boolean(channel.is_live);
    const session = channel.live_started_at
      ? new Date(channel.live_started_at).toISOString()
      : null;
    const prev = channelState.get(channel.id);

    if (!seeded || !prev) {
      channelState.set(channel.id, {
        live,
        session,
        startedAt: channel.live_started_at,
      });
      continue;
    }

    if (!prev.live && live) {
      channelState.set(channel.id, {
        live,
        session,
        startedAt: channel.live_started_at,
      });
      bitrateState.delete(channel.id);
      await emitEvent({
        organizationId: channel.organization_id,
        channelId: channel.id,
        type: "stream.started",
        title: `${channel.name} went live`,
        data: { channel_name: channel.name, live_started_at: session },
      });
      schedulePreflight(channel, session);
    } else if (prev.live && !live) {
      channelState.set(channel.id, { live, session: null, startedAt: null });
      bitrateState.delete(channel.id);
      clearTimeout(preflightTimers.get(channel.id));
      const duration = prev.startedAt
        ? Math.max(
            0,
            Math.round(
              (Date.now() - new Date(prev.startedAt).getTime()) / 1000,
            ),
          )
        : null;
      await emitEvent({
        organizationId: channel.organization_id,
        channelId: channel.id,
        type: "stream.ended",
        title: `${channel.name} ended`,
        data: { channel_name: channel.name, duration_seconds: duration },
      });
      const window = await scheduledWindowStillOpen(channel.id);
      if (window) {
        const active = await pool
          .query(
            `SELECT COUNT(*)::int AS n FROM social_destinations
             WHERE channel_id = $1 AND (is_running = TRUE OR platform_broadcast_id IS NOT NULL)`,
            [channel.id],
          )
          .catch(() => ({ rows: [{ n: 0 }] }));
        await emitEvent({
          organizationId: channel.organization_id,
          channelId: channel.id,
          type: "stream.dropped",
          severity: "critical",
          title: `${channel.name} dropped during “${window.title}”`,
          data: {
            channel_name: channel.name,
            scheduled_title: window.title,
            scheduled_end: window.scheduled_end,
            active_targets: active.rows[0].n,
            duration_seconds: duration,
          },
        });
      }
    } else if (live && prev.session !== session) {
      // Reconnected between ticks (new session): treat as a fresh start.
      channelState.set(channel.id, {
        live,
        session,
        startedAt: channel.live_started_at,
      });
      bitrateState.delete(channel.id);
      schedulePreflight(channel, session);
    }

    if (live && srs)
      await checkBitrate(channel, srs.get(String(channel.stream_key)));
  }
  for (const id of channelState.keys())
    if (!seen.has(id)) channelState.delete(id);
}

async function checkBitrate(channel, srsStream) {
  if (!srsStream?.publish?.active) return;
  const kbps = Math.round(Number(srsStream.kbps?.recv_30s || 0));
  const state = bitrateState.get(channel.id) || {
    samples: [],
    count: 0,
    degraded: false,
    below: 0,
    above: 0,
    baseline: null,
  };
  state.count += 1;
  bitrateState.set(channel.id, state);
  if (state.count <= BITRATE_WARMUP_SAMPLES) {
    if (kbps > 0) state.samples.push(kbps);
    return;
  }

  const settings = await orgSettings(channel.organization_id);
  const dropPct = Number(settings?.degradedThresholdPercent ?? 40);
  const baseline = state.samples.length >= 3 ? median(state.samples) : null;
  if (!baseline || baseline < 300) {
    if (kbps > 0) state.samples.push(kbps);
    if (state.samples.length > BITRATE_WINDOW) state.samples.shift();
    return;
  }
  state.baseline = Math.round(baseline);
  const low = kbps < baseline * (1 - dropPct / 100);
  const healthy = kbps >= baseline * 0.8;

  if (!state.degraded) {
    state.below = low ? state.below + 1 : 0;
    // Only learn "normal" from normal samples, so a slow slide doesn't
    // become the new baseline.
    if (!low) {
      state.samples.push(kbps);
      if (state.samples.length > BITRATE_WINDOW) state.samples.shift();
    }
    if (state.below >= BITRATE_CONFIRM_SAMPLES) {
      state.degraded = true;
      state.above = 0;
      await emitEvent({
        organizationId: channel.organization_id,
        channelId: channel.id,
        type: "stream.degraded",
        severity: "warning",
        title: `Bitrate dropped on ${channel.name}`,
        data: {
          channel_name: channel.name,
          current_kbps: kbps,
          baseline_kbps: state.baseline,
          drop_percent: Math.round((1 - kbps / baseline) * 100),
        },
      });
    }
  } else {
    state.above = healthy ? state.above + 1 : 0;
    if (state.above >= BITRATE_CONFIRM_SAMPLES) {
      state.degraded = false;
      state.below = 0;
      await emitEvent({
        organizationId: channel.organization_id,
        channelId: channel.id,
        type: "stream.recovered",
        title: `Bitrate recovered on ${channel.name}`,
        data: {
          channel_name: channel.name,
          current_kbps: kbps,
          baseline_kbps: state.baseline,
        },
      });
    }
  }
}

// ── stream target health ────────────────────────────────────
function targetLabel(row) {
  const kind = String(row.target_type || row.platform || "target");
  const pretty =
    {
      facebook: "Facebook",
      youtube: "YouTube",
      rtmp: "RTMP",
      rtmps: "RTMPS",
      srt: "SRT",
    }[kind.toLowerCase()] || kind;
  return row.target_name && row.target_name.toLowerCase() !== kind.toLowerCase()
    ? `${pretty} (${row.target_name})`
    : pretty;
}

async function checkTargetHealth() {
  const result = await pool.query(
    `SELECT h.*, sd.name AS target_name, sd.platform, sd.target_type, c.name AS channel_name
     FROM stream_target_health_history h
     JOIN social_destinations sd ON sd.id = h.target_id
     JOIN channels c ON c.id = h.channel_id
     WHERE h.id > $1
     ORDER BY h.id
     LIMIT 300`,
    [lastHealthId],
  );
  for (const row of result.rows) {
    lastHealthId = Math.max(lastHealthId, Number(row.id));
    const severity = String(row.severity || "").toLowerCase();
    const failing =
      row.requires_attention ||
      ["error", "critical"].includes(severity) ||
      ["target_start_failed", "reconnect_exhausted", "worker_stale"].includes(
        row.event_type,
      );
    const base = {
      organizationId: row.organization_id,
      channelId: row.channel_id,
      data: {
        channel_name: row.channel_name,
        target_id: row.target_id,
        target_label: targetLabel(row),
        target_type: row.target_type || row.platform,
        health_event: row.event_type,
        health_status: row.health_status,
        message: row.message || null,
        failure_code: row.failure_code || null,
        reconnect_count: row.reconnect_count,
        bitrate_kbps: row.current_bitrate_kbps,
      },
    };

    if (failing) {
      if (!failedTargets.has(row.target_id)) {
        failedTargets.add(row.target_id);
        await emitEvent({
          ...base,
          type: "target.failed",
          severity: "critical",
          title: `${targetLabel(row)} failed on ${row.channel_name}`,
        });
      }
    } else if (row.event_type === "delivery_verified") {
      await emitEvent({
        ...base,
        type: "target.delivering",
        title: `${targetLabel(row)} delivering on ${row.channel_name}`,
      });
      if (failedTargets.delete(row.target_id)) {
        await emitEvent({
          ...base,
          type: "target.recovered",
          title: `${targetLabel(row)} recovered on ${row.channel_name}`,
        });
      }
    } else if (row.event_type === "reconnect_scheduled") {
      await emitEvent({
        ...base,
        type: "target.reconnecting",
        severity: "warning",
        title: `${targetLabel(row)} reconnecting on ${row.channel_name}`,
      });
    } else if (row.event_type === "target_stopped") {
      failedTargets.delete(row.target_id);
      await emitEvent({
        ...base,
        type: "target.stopped",
        title: `${targetLabel(row)} stopped on ${row.channel_name}`,
      });
    }
  }
}

// ── recordings ──────────────────────────────────────────────
async function checkRecordings() {
  const result = await pool
    .query(
      `SELECT r.*, c.name AS channel_name
       FROM recordings r LEFT JOIN channels c ON c.id = r.channel_id
       WHERE r.id > $1 AND r.organization_id IS NOT NULL
       ORDER BY r.id LIMIT 100`,
      [lastRecordingId],
    )
    .catch(() => ({ rows: [] }));
  for (const row of result.rows) {
    lastRecordingId = Math.max(lastRecordingId, Number(row.id));
    const data = {
      channel_name: row.channel_name || null,
      recording_id: row.id,
    };
    for (const key of [
      "title",
      "filename",
      "duration",
      "duration_seconds",
      "size",
      "file_size",
      "created_at",
    ]) {
      if (row[key] !== undefined && row[key] !== null) data[key] = row[key];
    }
    await emitEvent({
      organizationId: row.organization_id,
      channelId: row.channel_id,
      type: "recording.ready",
      title: `Recording ready${row.channel_name ? ` for ${row.channel_name}` : ""}`,
      data,
    });
  }
}

// ── loop ────────────────────────────────────────────────────
async function seedCursors() {
  const health = await pool
    .query(
      `SELECT COALESCE(MAX(id), 0) AS id FROM stream_target_health_history`,
    )
    .catch(() => ({ rows: [{ id: 0 }] }));
  lastHealthId = Number(health.rows[0].id);
  const rec = await pool
    .query(`SELECT COALESCE(MAX(id), 0) AS id FROM recordings`)
    .catch(() => ({ rows: [{ id: 0 }] }));
  lastRecordingId = Number(rec.rows[0].id);
  // Targets that are failed right now count as already alerted, so a backend
  // restart doesn't re-send old failures.
  const failed = await pool
    .query(`SELECT id FROM social_destinations WHERE status = 'failed'`)
    .catch(() => ({ rows: [] }));
  for (const row of failed.rows) failedTargets.add(row.id);
}

async function tick() {
  if (running || !pool) return;
  running = true;
  try {
    if (!seeded) await seedCursors();
    const srs = await fetchSrsStreams();
    await checkChannels(srs);
    seeded = true;
    await checkTargetHealth();
    await checkRecordings();
  } catch (error) {
    console.error("[MONITOR] tick failed:", error.message);
  } finally {
    running = false;
  }
}

// Gives the module its database without starting the loop (the pre-flight
// API works even on instances where the monitor is switched off).
function initStreamMonitor(db) {
  pool = db;
}

function startStreamMonitor(db) {
  pool = db;
  if (timer) return;
  timer = setInterval(tick, TICK_MS);
  timer.unref?.();
  setTimeout(tick, 3000).unref?.();
  console.log(
    "[MONITOR] Stream monitor started (alerts, pre-flight, webhooks).",
  );
}

async function latestPreflight(channelId) {
  const result = await pool.query(
    `SELECT id, grade, summary, checks, measured, live_session, created_at
     FROM channel_preflight_reports WHERE channel_id = $1
     ORDER BY id DESC LIMIT 1`,
    [channelId],
  );
  return result.rows[0] || null;
}

module.exports = {
  initStreamMonitor,
  startStreamMonitor,
  runPreflight,
  latestPreflight,
  buildPreflight,
  _tickForTests: () => tick(),
};
