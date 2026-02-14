const test = require("node:test");
const assert = require("node:assert/strict");

const handler = require("../api/scan");
const { createRes } = require("./helpers");

function resetScanEnv() {
  process.env.GITHUB_TOKEN = "ghp_test";
  process.env.GITHUB_OWNER = "owner";
  process.env.GITHUB_REPO = "repo";
  process.env.GITHUB_EVENT_TYPE = "scan";
  process.env.SCAN_SECRET = "scan_secret";
}

test("scan endpoint rejects unauthorized requests", async () => {
  resetScanEnv();
  const req = {
    method: "POST",
    headers: { authorization: "Bearer wrong" },
    body: { products: [] },
  };
  const res = createRes();
  await handler(req, res);
  assert.equal(res.statusCode, 401);
  assert.equal(res.body.ok, false);
});

test("scan endpoint rejects payload without products", async () => {
  resetScanEnv();
  const req = {
    method: "POST",
    headers: { authorization: "Bearer scan_secret" },
    body: {},
  };
  const res = createRes();
  await handler(req, res);
  assert.equal(res.statusCode, 400);
});

test("scan endpoint fails closed when SCAN_SECRET is missing", async () => {
  resetScanEnv();
  delete process.env.SCAN_SECRET;
  const req = {
    method: "POST",
    headers: { authorization: "Bearer scan_secret" },
    body: { products: [{ title: "iPhone 14", price_eur: 500 }] },
  };
  const res = createRes();
  await handler(req, res);
  assert.equal(res.statusCode, 500);
  assert.equal(res.body.ok, false);
});

test("scan endpoint rejects invalid product shape", async () => {
  resetScanEnv();
  const req = {
    method: "POST",
    headers: { authorization: "Bearer scan_secret" },
    body: { products: [{ title: "Only title" }] },
  };
  const res = createRes();
  await handler(req, res);
  assert.equal(res.statusCode, 400);
});

test("scan endpoint dispatches valid payload to github", async () => {
  resetScanEnv();
  const calls = [];
  const previousFetch = global.fetch;
  global.fetch = async (url, options) => {
    calls.push({ url, options });
    return {
      ok: true,
      text: async () => "",
    };
  };

  const req = {
    method: "POST",
    headers: { authorization: "Bearer scan_secret" },
    body: {
      products: [{ title: "iPhone 14", price_eur: 500, category: "apple_phone" }],
    },
  };
  const res = createRes();
  await handler(req, res);
  global.fetch = previousFetch;

  assert.equal(res.statusCode, 202);
  assert.equal(calls.length, 1);
  assert.match(calls[0].url, /dispatches$/);
  const body = JSON.parse(calls[0].options.body);
  assert.equal(body.client_payload.command, "scan");
  assert.equal(body.client_payload.products.length, 1);
});
