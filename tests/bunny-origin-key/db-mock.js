// Test preload: replaces ./db with an in-memory pool returning TEST_ORGS,
// so backfill-origin-key-rule.js runs without a real database or .env.
const Module = require("module");
const original = Module._load;
Module._load = function (request, parent, isMain) {
  if (request === "./db" || request === "./db.js") {
    return {
      async query() { return { rows: JSON.parse(process.env.TEST_ORGS || "[]") }; },
      async end() {},
    };
  }
  return original.apply(this, arguments);
};
