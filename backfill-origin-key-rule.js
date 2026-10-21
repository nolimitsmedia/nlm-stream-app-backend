// backfill-origin-key-rule.js
//
// Checks (default) or fixes (--apply) the "NLM origin key" Edge Rule on
// EVERY Bunny pull zone whose origin is this server (BUNNY_ORIGIN_URL).
// nginx refuses raw-IP origin requests without the X-NLM-Origin-Key header,
// so a zone without the rule cannot play anything.
//
// Zones are discovered from Bunny itself (all pull zones whose OriginUrl is
// BUNNY_ORIGIN_URL), not only from the organizations table, so the shared
// platform zones (e.g. nlmstream, nlmstream-hls) are covered too. Zones the
// database references but Bunny does not list are reported as warnings.
//
// Run from the backend directory:
//   node backfill-origin-key-rule.js           # report only, changes nothing
//   node backfill-origin-key-rule.js --apply   # add/update rules where needed
//
// Requires BUNNY_ACCOUNT_API_KEY, BUNNY_ORIGIN_URL and BUNNY_ORIGIN_KEY in
// .env. Never prints the key. Exit code 0 = every origin zone is OK (after
// --apply, if given); 1 = something still needs attention.

const pool = require("./db");
const bunny = require("./bunny_client");

const APPLY = process.argv.includes("--apply");
const normalizeOrigin = (value) =>
  String(value || "")
    .trim()
    .replace(/\/+$/, "")
    .toLowerCase();

async function listAllPullZones() {
  const zones = [];
  for (let page = 1; page <= 50; page += 1) {
    const data = await bunny.callBunnyApi(
      "GET",
      `/pullzone?page=${page}&perPage=1000`,
    );
    const items = Array.isArray(data) ? data : data?.Items || [];
    zones.push(...items);
    if (Array.isArray(data) || !data?.HasMoreItems) break;
  }
  return zones;
}

function describeRule(zone) {
  const rules = (zone.EdgeRules || []).filter(bunny.isOriginKeyRule);
  if (!rules.length) return { state: "MISSING", detail: "no origin-key rule" };
  const key = (process.env.BUNNY_ORIGIN_KEY || "").trim();
  const good = rules.find((r) => r.Enabled && r.ActionParameter2 === key);
  const patterns = rules
    .flatMap((r) => (r.Triggers || []).flatMap((t) => t.PatternMatches || []))
    .join(", ");
  if (good)
    return { state: "OK", detail: `enabled, current key, url ${patterns}` };
  if (rules.some((r) => !r.Enabled))
    return { state: "DISABLED", detail: `url ${patterns}` };
  return {
    state: "STALE KEY",
    detail: `key differs from BUNNY_ORIGIN_KEY, url ${patterns}`,
  };
}

(async () => {
  let exitCode = 0;
  try {
    if (!bunny.isBunnyAccountConfigured()) {
      throw new Error("BUNNY_ACCOUNT_API_KEY is not configured");
    }
    const origin = normalizeOrigin(process.env.BUNNY_ORIGIN_URL);
    if (!origin) throw new Error("BUNNY_ORIGIN_URL is not configured");
    if (!(process.env.BUNNY_ORIGIN_KEY || "").trim()) {
      throw new Error(
        "BUNNY_ORIGIN_KEY is not configured (it must equal /root/nlm-origin-key.txt)",
      );
    }

    const allZones = await listAllPullZones();
    const originZones = allZones.filter(
      (z) => normalizeOrigin(z.OriginUrl) === origin,
    );
    console.log(
      `Bunny lists ${allZones.length} pull zone(s); ${originZones.length} use this server (${process.env.BUNNY_ORIGIN_URL}) as origin.`,
    );
    console.log(
      APPLY
        ? "Mode: APPLY (fixing where needed)\n"
        : "Mode: CHECK ONLY (no changes)\n",
    );

    let needsWork = 0;
    for (const listed of originZones) {
      // The list endpoint may omit rules; read the zone itself.
      const zone = await bunny.getPullZone(listed.Id);
      const before = describeRule(zone);
      if (before.state === "OK") {
        console.log(`✓ ${zone.Name} [${zone.Id}] — OK (${before.detail})`);
        continue;
      }
      if (!APPLY) {
        console.log(
          `✗ ${zone.Name} [${zone.Id}] — ${before.state} (${before.detail})`,
        );
        needsWork += 1;
        continue;
      }
      try {
        const result = await bunny.ensureOriginKeyEdgeRule(zone.Id);
        const after = describeRule(await bunny.getPullZone(zone.Id));
        const ok = after.state === "OK";
        console.log(
          `${ok ? "✓" : "✗"} ${zone.Name} [${zone.Id}] — was ${before.state}, ${result.status}; now ${after.state} (${after.detail})`,
        );
        if (!ok) needsWork += 1;
      } catch (error) {
        console.log(`✗ ${zone.Name} [${zone.Id}] — FAILED: ${error.message}`);
        needsWork += 1;
      }
    }

    const db = await pool.query(
      `SELECT id, name, bunny_pull_zone_id FROM organizations
        WHERE bunny_pull_zone_id IS NOT NULL ORDER BY id`,
    );
    const originIds = new Set(originZones.map((z) => String(z.Id)));
    for (const org of db.rows) {
      if (!originIds.has(String(org.bunny_pull_zone_id))) {
        console.log(
          `! organization ${org.id} (${org.name}) references pull zone ${org.bunny_pull_zone_id}, which Bunny does not list with this origin — check it in the dashboard.`,
        );
      }
    }

    console.log(
      needsWork
        ? `\n${needsWork} zone(s) need attention${APPLY ? "" : " — re-run with --apply to fix"}.`
        : "\nAll origin zones carry the current origin key.",
    );
    exitCode = needsWork ? 1 : 0;
  } catch (error) {
    console.error(`ERROR: ${error.message}`);
    exitCode = 1;
  }
  await pool.end().catch(() => {});
  process.exit(exitCode);
})();
