const { spawn } = require("child_process");

const analyzerCache = new Map();
const bitrateHistory = new Map();
const hlsState = new Map();
const analyzerInFlight = new Map();

const ANALYZER_CACHE_MS = Number(process.env.STREAM_ANALYZER_CACHE_MS || 30000);
const PROBE_TIMEOUT_MS = Number(
  process.env.STREAM_ANALYZER_PROBE_TIMEOUT_MS || 9000,
);
const HISTORY_MAX_SAMPLES = 20;

const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

const parseRate = (value) => {
  if (!value) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const text = String(value);
  if (text.includes("/")) {
    const [a, b] = text.split("/").map(Number);
    if (Number.isFinite(a) && Number.isFinite(b) && b !== 0) return a / b;
  }
  const n = Number(text);
  return Number.isFinite(n) ? n : null;
};

const round = (value, digits = 0) => {
  if (!Number.isFinite(Number(value))) return null;
  const factor = 10 ** digits;
  return Math.round(Number(value) * factor) / factor;
};

const pushBitrateSample = (streamName, bitrateKbps) => {
  if (
    !streamName ||
    !Number.isFinite(Number(bitrateKbps)) ||
    Number(bitrateKbps) <= 0
  ) {
    return [];
  }
  const now = Date.now();
  const samples = bitrateHistory.get(streamName) || [];
  samples.push({ at: now, value: Number(bitrateKbps) });
  while (samples.length > HISTORY_MAX_SAMPLES) samples.shift();
  bitrateHistory.set(streamName, samples);
  return samples;
};

const calculateBitrateStability = (samples = []) => {
  if (samples.length < 3)
    return {
      score: null,
      variation_percent: null,
      sample_count: samples.length,
    };
  const values = samples
    .map((s) => Number(s.value))
    .filter((v) => Number.isFinite(v) && v > 0);
  if (values.length < 3)
    return {
      score: null,
      variation_percent: null,
      sample_count: values.length,
    };
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance =
    values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  const stdDev = Math.sqrt(variance);
  const cv = mean > 0 ? stdDev / mean : 1;
  const variationPercent = cv * 100;
  const score = clamp(Math.round(100 - variationPercent * 2.5), 0, 100);
  return {
    score,
    variation_percent: round(variationPercent, 1),
    sample_count: values.length,
  };
};

const runFfprobe = (url) =>
  new Promise((resolve) => {
    const args = [
      "-v",
      "error",
      "-read_intervals",
      "%+6",
      "-show_streams",
      "-show_packets",
      "-show_entries",
      "stream=index,codec_type,codec_name,profile,width,height,r_frame_rate,avg_frame_rate,bit_rate,sample_rate,channels,channel_layout:packet=stream_index,pts_time,flags,size",
      "-of",
      "json",
      url,
    ];

    let stdout = "";
    let stderr = "";
    let settled = false;
    let proc;

    try {
      proc = spawn("ffprobe", args, { stdio: ["ignore", "pipe", "pipe"] });
    } catch (error) {
      return resolve({ ok: false, error: error.message });
    }

    const timer = setTimeout(() => {
      if (!settled) {
        try {
          proc.kill("SIGKILL");
        } catch {}
      }
    }, PROBE_TIMEOUT_MS);

    proc.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
      if (stdout.length > 8 * 1024 * 1024)
        stdout = stdout.slice(-8 * 1024 * 1024);
    });
    proc.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
      if (stderr.length > 16000) stderr = stderr.slice(-16000);
    });

    proc.on("error", (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ ok: false, error: error.message });
    });

    proc.on("close", (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        const parsed = JSON.parse(stdout || "{}");
        resolve({
          ok: Boolean(parsed.streams?.length),
          code,
          signal,
          data: parsed,
          error: stderr.trim() || null,
        });
      } catch (error) {
        resolve({
          ok: false,
          code,
          signal,
          error: stderr.trim() || error.message,
        });
      }
    });
  });

const deriveKeyframeInterval = (probeData, videoStreamIndex, fps) => {
  const packets = (probeData?.packets || [])
    .filter(
      (packet) => Number(packet.stream_index) === Number(videoStreamIndex),
    )
    .map((packet) => ({
      pts: Number(packet.pts_time),
      key: String(packet.flags || "").includes("K"),
    }))
    .filter((packet) => Number.isFinite(packet.pts));

  const keyframes = packets
    .filter((packet) => packet.key)
    .map((packet) => packet.pts);
  if (keyframes.length >= 2) {
    const gaps = [];
    for (let i = 1; i < keyframes.length; i += 1)
      gaps.push(keyframes[i] - keyframes[i - 1]);
    const average = gaps.reduce((sum, value) => sum + value, 0) / gaps.length;
    return {
      seconds: round(average, 2),
      frames: fps ? Math.round(average * fps) : null,
      measured: true,
    };
  }
  return { seconds: null, frames: null, measured: false };
};

const estimatePacketBitrate = (probeData, streamIndex) => {
  const packets = (probeData?.packets || [])
    .filter((packet) => Number(packet.stream_index) === Number(streamIndex))
    .map((packet) => ({
      pts: Number(packet.pts_time),
      size: Number(packet.size),
    }))
    .filter(
      (packet) => Number.isFinite(packet.pts) && Number.isFinite(packet.size),
    );
  if (packets.length < 2) return null;
  const firstPts = packets[0].pts;
  const lastPts = packets[packets.length - 1].pts;
  const duration = lastPts - firstPts;
  if (duration <= 0) return null;
  const bits = packets.reduce((sum, packet) => sum + packet.size * 8, 0);
  return round(bits / duration / 1000, 0);
};

const inspectHls = async (url, cacheKey) => {
  const previous = hlsState.get(cacheKey);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 4500);
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        Accept:
          "application/vnd.apple.mpegurl,application/x-mpegURL,text/plain,*/*",
      },
    });
    const text = response.ok ? await response.text() : "";
    const mediaSequence = Number(
      (text.match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/) || [])[1],
    );
    const segments = text
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line && !line.startsWith("#"));
    const lastSegment = segments[segments.length - 1] || null;
    const signature = `${Number.isFinite(mediaSequence) ? mediaSequence : "x"}:${lastSegment || "none"}`;
    const now = Date.now();
    const changedAt =
      previous && previous.signature === signature ? previous.changedAt : now;
    hlsState.set(cacheKey, { signature, changedAt, checkedAt: now });
    return {
      reachable: response.ok && text.includes("#EXTM3U"),
      http_status: response.status,
      segment_count: segments.length,
      media_sequence: Number.isFinite(mediaSequence) ? mediaSequence : null,
      last_segment: lastSegment,
      seconds_since_playlist_change: round((now - changedAt) / 1000, 1),
      fresh:
        response.ok &&
        text.includes("#EXTM3U") &&
        (segments.length > 0 || text.includes("#EXT-X-STREAM-INF")) &&
        now - changedAt < 20000,
    };
  } catch (error) {
    return {
      reachable: false,
      http_status: null,
      segment_count: 0,
      media_sequence: null,
      last_segment: null,
      seconds_since_playlist_change: previous
        ? round((Date.now() - previous.changedAt) / 1000, 1)
        : null,
      fresh: false,
      error:
        error.name === "AbortError" ? "HLS check timed out" : error.message,
    };
  } finally {
    clearTimeout(timer);
  }
};

const buildWarnings = ({
  video,
  audio,
  keyframe,
  bitrateStability,
  hls,
  stream,
}) => {
  const warnings = [];
  if (!video?.codec)
    warnings.push({
      code: "VIDEO_MISSING",
      severity: "critical",
      title: "Video not detected",
      message: "No active video track was detected on the incoming stream.",
    });
  if (!audio?.codec)
    warnings.push({
      code: "AUDIO_MISSING",
      severity: "warning",
      title: "Audio not detected",
      message: "No active audio track was detected on the incoming stream.",
    });
  if (video?.fps && (video.fps < 23 || video.fps > 61))
    warnings.push({
      code: "FPS_OUTSIDE_RECOMMENDED",
      severity: "warning",
      title: "Unusual frame rate",
      message: `Current: ${video.fps} fps. Recommended broadcast range: 23.976–60 fps.`,
    });
  if (audio?.sample_rate && Number(audio.sample_rate) !== 48000)
    warnings.push({
      code: "AUDIO_SAMPLE_RATE",
      severity: "warning",
      title: "Audio sample rate",
      message: `Current: ${round(Number(audio.sample_rate) / 1000, 1)} kHz. Recommended: 48 kHz.`,
    });
  if (keyframe?.seconds && keyframe.seconds > 3)
    warnings.push({
      code: "KEYFRAME_INTERVAL",
      severity: "warning",
      title: "Keyframe interval",
      message: `Current: ${keyframe.seconds} sec. Recommended: approximately 2 sec.`,
    });
  if (bitrateStability?.score !== null && bitrateStability.score < 70)
    warnings.push({
      code: "BITRATE_UNSTABLE",
      severity: "warning",
      title: "Bitrate instability",
      message: `Rolling bitrate variation is ${bitrateStability.variation_percent}%. Check encoder/network stability.`,
    });
  if (stream?.publish?.active && Number(stream?.kbps?.recv_30s || 0) <= 0)
    warnings.push({
      code: "NO_INGEST_BITRATE",
      severity: "critical",
      title: "No ingest bitrate",
      message: "SRS sees the publisher but no rolling input bitrate.",
    });
  if (hls && !hls.reachable)
    warnings.push({
      code: "HLS_UNREACHABLE",
      severity: "critical",
      title: "HLS unavailable",
      message: "The internal HLS playlist could not be read.",
    });
  else if (hls && !hls.fresh)
    warnings.push({
      code: "HLS_STALE",
      severity: "warning",
      title: "HLS playlist may be stale",
      message: "The playlist has not advanced recently.",
    });
  return warnings;
};

const calculateHealth = ({
  stream,
  video,
  audio,
  keyframe,
  bitrateStability,
  hls,
  probeOk,
}) => {
  const components = [];
  const add = (key, label, weight, score, status, detail) =>
    components.push({
      key,
      label,
      weight,
      score: round(score, 0),
      status,
      detail,
    });
  const active = Boolean(stream?.publish?.active);
  const bitrate = Number(stream?.kbps?.recv_30s || 0);

  add(
    "input",
    "Input availability",
    20,
    active && bitrate > 0 ? 100 : active ? 45 : 0,
    active && bitrate > 0 ? "healthy" : active ? "warning" : "critical",
    bitrate > 0 ? `${Math.round(bitrate)} kbps received` : "No rolling bitrate",
  );
  add(
    "video",
    "Video",
    15,
    video?.codec ? 100 : probeOk ? 0 : 60,
    video?.codec ? "healthy" : probeOk ? "critical" : "unknown",
    video?.codec
      ? `${String(video.codec).toUpperCase()} ${video.width || "?"}x${video.height || "?"}`
      : "Not confirmed",
  );
  add(
    "audio",
    "Audio",
    10,
    audio?.codec ? 100 : probeOk ? 0 : 60,
    audio?.codec ? "healthy" : probeOk ? "critical" : "unknown",
    audio?.codec
      ? `${String(audio.codec).toUpperCase()} ${audio.sample_rate || "?"} Hz`
      : "Not confirmed",
  );
  const stabilityScore =
    bitrateStability?.score === null ? 85 : bitrateStability.score;
  add(
    "bitrate",
    "Bitrate stability",
    15,
    stabilityScore,
    stabilityScore >= 80
      ? "healthy"
      : stabilityScore >= 60
        ? "warning"
        : "critical",
    bitrateStability?.variation_percent === null
      ? "Building history"
      : `${bitrateStability.variation_percent}% variation`,
  );
  const fpsScore = !video?.fps
    ? 85
    : video.fps >= 23 && video.fps <= 61
      ? 100
      : 60;
  add(
    "fps",
    "FPS stability",
    10,
    fpsScore,
    fpsScore >= 80 ? "healthy" : "warning",
    video?.fps ? `${video.fps} fps` : "FPS unavailable",
  );
  const keyframeScore = !keyframe?.seconds
    ? 85
    : keyframe.seconds <= 2.5
      ? 100
      : keyframe.seconds <= 4
        ? 70
        : 35;
  add(
    "keyframe",
    "Keyframe compliance",
    10,
    keyframeScore,
    keyframeScore >= 80
      ? "healthy"
      : keyframeScore >= 60
        ? "warning"
        : "critical",
    keyframe?.seconds ? `${keyframe.seconds}s GOP` : "Measurement pending",
  );
  const hlsScore = !hls
    ? 75
    : hls.reachable && hls.fresh
      ? 100
      : hls.reachable
        ? 60
        : 0;
  add(
    "hls",
    "HLS delivery",
    15,
    hlsScore,
    hlsScore >= 80 ? "healthy" : hlsScore >= 50 ? "warning" : "critical",
    hls?.reachable
      ? hls.fresh
        ? "Playlist fresh"
        : "Playlist stale"
      : "Playlist unavailable",
  );
  add(
    "analyzer",
    "Analyzer",
    5,
    probeOk ? 100 : 60,
    probeOk ? "healthy" : "warning",
    probeOk ? "FFprobe verified" : "Using SRS telemetry",
  );

  const totalWeight = components.reduce(
    (sum, component) => sum + component.weight,
    0,
  );
  const score = Math.round(
    components.reduce(
      (sum, component) => sum + component.score * component.weight,
      0,
    ) / totalWeight,
  );
  const status = score >= 85 ? "healthy" : score >= 65 ? "warning" : "critical";
  return { score, status, components };
};

const analyzeLiveStream = async ({ stream, internalHlsBaseUrl }) => {
  const streamName = String(stream?.name || "");
  const cacheKey = `${stream?.app || "live"}:${streamName}`;
  const bitrate = Number(stream?.kbps?.recv_30s || 0);
  const samples = pushBitrateSample(cacheKey, bitrate);
  const bitrateStability = calculateBitrateStability(samples);

  const cached = analyzerCache.get(cacheKey);
  if (cached && Date.now() - cached.at < ANALYZER_CACHE_MS) {
    return { ...cached.value, bitrate_stability: bitrateStability };
  }

  const hlsBase = String(internalHlsBaseUrl || "").replace(/\/$/, "");
  const hlsUrl = `${hlsBase}/live/${encodeURIComponent(streamName)}.m3u8`;
  const [probe, hls] = await Promise.all([
    runFfprobe(hlsUrl),
    inspectHls(hlsUrl, cacheKey),
  ]);

  const streams = probe.data?.streams || [];
  const probedVideo = streams.find((item) => item.codec_type === "video");
  const probedAudio = streams.find((item) => item.codec_type === "audio");
  const srsVideo = stream?.video || {};
  const srsAudio = stream?.audio || {};
  const fps = round(
    parseRate(
      probedVideo?.avg_frame_rate || probedVideo?.r_frame_rate || srsVideo?.fps,
    ),
    2,
  );

  const video = {
    codec: probedVideo?.codec_name || srsVideo?.codec || null,
    profile: probedVideo?.profile || srsVideo?.profile || null,
    width: Number(probedVideo?.width || srsVideo?.width || 0) || null,
    height: Number(probedVideo?.height || srsVideo?.height || 0) || null,
    fps,
    bitrate_kbps:
      estimatePacketBitrate(probe.data, probedVideo?.index) ||
      (Number(probedVideo?.bit_rate) > 0
        ? round(Number(probedVideo.bit_rate) / 1000, 0)
        : null),
  };

  const audio = {
    codec: probedAudio?.codec_name || srsAudio?.codec || null,
    bitrate_kbps:
      estimatePacketBitrate(probe.data, probedAudio?.index) ||
      (Number(probedAudio?.bit_rate) > 0
        ? round(Number(probedAudio.bit_rate) / 1000, 0)
        : null),
    sample_rate:
      Number(probedAudio?.sample_rate || srsAudio?.sample_rate || 0) || null,
    channels: Number(probedAudio?.channels || srsAudio?.channels || 0) || null,
    channel_layout: probedAudio?.channel_layout || null,
  };

  const keyframe = deriveKeyframeInterval(probe.data, probedVideo?.index, fps);
  const health = calculateHealth({
    stream,
    video,
    audio,
    keyframe,
    bitrateStability,
    hls,
    probeOk: probe.ok,
  });
  const warnings = buildWarnings({
    video,
    audio,
    keyframe,
    bitrateStability,
    hls,
    stream,
  });

  const value = {
    analyzed_at: new Date().toISOString(),
    analyzer_source: probe.ok ? "ffprobe+srs" : "srs",
    probe_ok: probe.ok,
    probe_error: probe.ok
      ? null
      : probe.error || "FFprobe could not verify the stream",
    protocol: stream?.publish?.active ? "RTMP/SRT" : null,
    video,
    audio,
    keyframe_interval: keyframe,
    bitrate_stability: bitrateStability,
    hls,
    warnings,
    health,
  };

  analyzerCache.set(cacheKey, { at: Date.now(), value });
  return value;
};

const getCachedStreamAnalysis = (stream) => {
  const streamName = String(stream?.name || "");
  const cacheKey = `${stream?.app || "live"}:${streamName}`;
  const bitrate = Number(stream?.kbps?.recv_30s || 0);
  const samples = pushBitrateSample(cacheKey, bitrate);
  const bitrateStability = calculateBitrateStability(samples);
  const cached = analyzerCache.get(cacheKey);
  if (!cached) {
    return {
      analyzed_at: null,
      analyzer_source: "pending",
      probe_ok: false,
      pending: true,
      bitrate_stability: bitrateStability,
      warnings: [],
      health: null,
    };
  }
  return {
    ...cached.value,
    pending: false,
    bitrate_stability: bitrateStability,
  };
};

const scheduleLiveStreamAnalysis = ({ stream, internalHlsBaseUrl }) => {
  const streamName = String(stream?.name || "");
  const cacheKey = `${stream?.app || "live"}:${streamName}`;
  const cached = analyzerCache.get(cacheKey);
  if (cached && Date.now() - cached.at < ANALYZER_CACHE_MS) return;
  if (analyzerInFlight.has(cacheKey)) return;

  const promise = analyzeLiveStream({ stream, internalHlsBaseUrl })
    .catch((error) => {
      console.warn(`[STREAM ANALYZER] ${streamName}: ${error.message}`);
    })
    .finally(() => analyzerInFlight.delete(cacheKey));
  analyzerInFlight.set(cacheKey, promise);
};

const clearStreamAnalysis = (streamName) => {
  for (const key of analyzerCache.keys())
    if (key.endsWith(`:${streamName}`)) analyzerCache.delete(key);
  for (const key of bitrateHistory.keys())
    if (key.endsWith(`:${streamName}`)) bitrateHistory.delete(key);
  for (const key of hlsState.keys())
    if (key.endsWith(`:${streamName}`)) hlsState.delete(key);
};

module.exports = {
  analyzeLiveStream,
  getCachedStreamAnalysis,
  scheduleLiveStreamAnalysis,
  clearStreamAnalysis,
};
