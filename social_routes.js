
/*
|--------------------------------------------------------------------------
| SOCIAL DESTINATIONS (Facebook / YouTube simulcasting)
|--------------------------------------------------------------------------
*/
const socialProcesses = new Map(); // destinationId -> ChildProcess

const SOCIAL_PLATFORMS = {
  facebook: {
    label: "Facebook",
    rtmpBase: "rtmps://live-api-s.facebook.com:443/rtmp",
  },
  youtube: {
    label: "YouTube",
    rtmpBase: "rtmp://a.rtmp.youtube.com/live2",
  },
};

async function isSrsStreamLive(streamKey) {
  try {
    const res = await fetch("http://127.0.0.1:1985/api/v1/streams/", {
      signal: AbortSignal.timeout(5000),
    });
    const data = await res.json();
    const streams = data.streams || [];
    return streams.some(
      (s) => s.name === streamKey && s.publish && s.publish.active,
    );
  } catch (err) {
    console.error("[SOCIAL] SRS stream check failed:", err.message);
    return false;
  }
}

async function getOwnedChannel(channelId, organizationId) {
  const result = await pool.query(
    `SELECT * FROM channels WHERE id = $1 AND organization_id = $2`,
    [channelId, organizationId],
  );
  return result.rows[0] || null;
}

app.get(
  "/api/channels/:channelId/social-destinations",
  authenticateAdmin,
  resolveOrganizationForRequest,
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const result = await pool.query(
        `SELECT * FROM social_destinations WHERE channel_id = $1 ORDER BY platform`,
        [channel.id],
      );

      res.json({ ok: true, destinations: result.rows });
    } catch (error) {
      console.error("Get Social Destinations Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to fetch social destinations" });
    }
  },
);

app.post(
  "/api/channels/:channelId/social-destinations",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const { platform, stream_key } = req.body;

      if (!platform || !SOCIAL_PLATFORMS[platform]) {
        return res.status(400).json({
          ok: false,
          message: "platform must be 'facebook' or 'youtube'",
        });
      }
      if (!stream_key) {
        return res
          .status(400)
          .json({ ok: false, message: "stream_key is required" });
      }

      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const result = await pool.query(
        `
        INSERT INTO social_destinations (channel_id, platform, stream_key)
        VALUES ($1, $2, $3)
        ON CONFLICT (channel_id, platform)
        DO UPDATE SET stream_key = EXCLUDED.stream_key, updated_at = now()
        RETURNING *
        `,
        [channel.id, platform, stream_key],
      );

      res.json({ ok: true, destination: result.rows[0] });
    } catch (error) {
      console.error("Save Social Destination Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to save social destination" });
    }
  },
);

app.delete(
  "/api/channels/:channelId/social-destinations/:id",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const existingProc = socialProcesses.get(Number(req.params.id));
      if (existingProc) {
        existingProc.kill("SIGTERM");
        socialProcesses.delete(Number(req.params.id));
      }

      await pool.query(
        `DELETE FROM social_destinations WHERE id = $1 AND channel_id = $2`,
        [req.params.id, channel.id],
      );

      res.json({ ok: true, message: "Social destination removed" });
    } catch (error) {
      console.error("Delete Social Destination Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to delete social destination" });
    }
  },
);

app.post(
  "/api/channels/:channelId/social-destinations/:id/start",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const destResult = await pool.query(
        `SELECT * FROM social_destinations WHERE id = $1 AND channel_id = $2`,
        [req.params.id, channel.id],
      );
      const destination = destResult.rows[0];
      if (!destination) {
        return res
          .status(404)
          .json({ ok: false, message: "Social destination not found" });
      }

      if (socialProcesses.has(destination.id)) {
        return res
          .status(400)
          .json({ ok: false, message: "Already simulcasting to this platform" });
      }

      const live = await isSrsStreamLive(channel.stream_key);
      if (!live) {
        return res.status(400).json({
          ok: false,
          message: "Main stream is not live yet. Start streaming first.",
        });
      }

      const platformConfig = SOCIAL_PLATFORMS[destination.platform];
      const destinationUrl = `${platformConfig.rtmpBase}/${destination.stream_key}`;
      const sourceUrl = `rtmp://127.0.0.1/live/${channel.stream_key}`;

      const proc = spawn("ffmpeg", [
        "-i", sourceUrl,
        "-c", "copy",
        "-f", "flv",
        destinationUrl,
      ]);

      proc.stderr.on("data", (data) => {
        console.log(`[SOCIAL ${destination.platform} #${destination.id}]`, data.toString().slice(0, 300));
      });

      proc.on("exit", (code) => {
        console.log(`[SOCIAL ${destination.platform} #${destination.id}] exited with code ${code}`);
        socialProcesses.delete(destination.id);
        pool
          .query(
            `UPDATE social_destinations SET is_running = false, ffmpeg_pid = NULL WHERE id = $1`,
            [destination.id],
          )
          .catch((err) =>
            console.error("[SOCIAL] Failed to update state on exit:", err.message),
          );
      });

      socialProcesses.set(destination.id, proc);

      await pool.query(
        `UPDATE social_destinations SET is_running = true, ffmpeg_pid = $1, started_at = now() WHERE id = $2`,
        [proc.pid, destination.id],
      );

      res.json({ ok: true, message: `Simulcasting to ${platformConfig.label} started` });
    } catch (error) {
      console.error("Start Social Destination Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to start simulcast" });
    }
  },
);

app.post(
  "/api/channels/:channelId/social-destinations/:id/stop",
  authenticateAdmin,
  resolveOrganizationForRequest,
  requireRole("super_admin", "admin", "operator"),
  requireOrganizationRole("owner", "admin"),
  async (req, res) => {
    try {
      const channel = await getOwnedChannel(
        req.params.channelId,
        req.organization.id,
      );
      if (!channel) {
        return res
          .status(404)
          .json({ ok: false, message: "Channel not found" });
      }

      const destId = Number(req.params.id);
      const proc = socialProcesses.get(destId);

      if (proc) {
        proc.kill("SIGTERM");
        socialProcesses.delete(destId);
      }

      await pool.query(
        `UPDATE social_destinations SET is_running = false, ffmpeg_pid = NULL WHERE id = $1 AND channel_id = $2`,
        [destId, channel.id],
      );

      res.json({ ok: true, message: "Simulcast stopped" });
    } catch (error) {
      console.error("Stop Social Destination Error:", error);
      res
        .status(500)
        .json({ ok: false, message: "Failed to stop simulcast" });
    }
  },
);
