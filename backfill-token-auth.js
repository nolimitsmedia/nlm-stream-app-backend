// backfill-token-auth.js
//
// ONE-TIME script — run once, then this file can be deleted. Enables
// Token Authentication (with the shared BUNNY_HLS_TOKEN_KEY) on every
// existing organization's dedicated Bunny pull zone that was created
// before createPullZoneForOrganization() started doing this
// automatically. See bunny_client.js's enableTokenAuthForPullZone() for
// the real incident this fixes — an org (nlm-one-church) was found
// serving live HLS with zero authentication.
//
// Run from the backend directory: node backfill-token-auth.js
// Deliberately processes one org at a time (not Promise.all) so a single
// failure is easy to isolate in the output rather than an interleaved
// mess of concurrent logs.

const pool = require("./db");
const bunny = require("./bunny_client");

(async () => {
  if (!bunny.isBunnyAccountConfigured()) {
    console.error(
      "BUNNY_ACCOUNT_API_KEY is not configured — cannot reach Bunny's API. Aborting.",
    );
    process.exit(1);
  }

  const result = await pool.query(
    `SELECT id, name, slug, bunny_pull_zone_id, bunny_pull_zone_hostname
     FROM organizations
     WHERE bunny_pull_zone_id IS NOT NULL
     ORDER BY id`,
  );

  console.log(
    `Found ${result.rows.length} organization(s) with a dedicated pull zone.\n`,
  );

  let succeeded = 0;
  let failed = 0;

  for (const org of result.rows) {
    try {
      await bunny.enableTokenAuthForPullZone(org.bunny_pull_zone_id);
      console.log(
        `✓ [${org.id}] ${org.name} (${org.bunny_pull_zone_hostname}) — Token Authentication enabled`,
      );
      succeeded += 1;
    } catch (err) {
      console.error(
        `✗ [${org.id}] ${org.name} (${org.bunny_pull_zone_hostname}) — FAILED: ${err.message}`,
      );
      failed += 1;
    }
  }

  console.log(`\nDone. ${succeeded} succeeded, ${failed} failed.`);
  if (failed > 0) {
    console.log(
      "Check the failed org(s) manually in the Bunny dashboard — do not assume they're fixed.",
    );
  }

  await pool.end();
})();
