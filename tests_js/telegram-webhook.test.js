const test = require("node:test");
const assert = require("node:assert/strict");

const handler = require("../api/telegram-webhook");
const { createRes } = require("./helpers");

function resetTelegramEnv() {
  process.env.TELEGRAM_BOT_TOKEN = "token";
  process.env.GITHUB_TOKEN = "ghp_test";
  process.env.GITHUB_OWNER = "owner";
  process.env.GITHUB_REPO = "repo";
  process.env.GITHUB_EVENT_TYPE = "scan";
  process.env.TELEGRAM_WEBHOOK_SECRET_TOKEN = "webhook_secret";
  process.env.TELEGRAM_ALLOWED_CHAT_IDS = "";
}

test("telegram webhook rejects invalid secret header", async () => {
  resetTelegramEnv();
  const req = {
    method: "POST",
    headers: { "x-telegram-bot-api-secret-token": "wrong" },
    body: {},
  };
  const res = createRes();
  await handler(req, res);
  assert.equal(res.statusCode, 401);
});

test("telegram webhook serves help command directly", async () => {
  resetTelegramEnv();
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
    headers: { "x-telegram-bot-api-secret-token": "webhook_secret" },
    body: {
      message: {
        chat: { id: 123 },
        from: { id: 999 },
        text: "/help",
      },
    },
  };
  const res = createRes();
  await handler(req, res);
  global.fetch = previousFetch;

  assert.equal(res.statusCode, 200);
  assert.equal(calls.length, 1);
  assert.match(calls[0].url, /sendMessage$/);
});

test("telegram webhook dispatches /scan to github actions", async () => {
  resetTelegramEnv();
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
    headers: { "x-telegram-bot-api-secret-token": "webhook_secret" },
    body: {
      message: {
        chat: { id: 123 },
        from: { id: 999 },
        text: '/scan {"title":"iPhone 14","price_eur":500,"category":"apple_phone"}',
      },
    },
  };
  const res = createRes();
  await handler(req, res);
  global.fetch = previousFetch;

  assert.equal(res.statusCode, 200);
  assert.equal(calls.length, 2);
  const dispatchCall = calls.find((item) => item.url.includes("/dispatches"));
  assert.ok(dispatchCall);
  const payload = JSON.parse(dispatchCall.options.body);
  assert.equal(payload.client_payload.command, "scan");
  assert.equal(payload.client_payload.products.length, 1);
});

test("telegram webhook denies unauthorized chat when allowlist is set", async () => {
  resetTelegramEnv();
  process.env.TELEGRAM_ALLOWED_CHAT_IDS = "42";
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
    headers: { "x-telegram-bot-api-secret-token": "webhook_secret" },
    body: {
      message: {
        chat: { id: 123 },
        from: { id: 999 },
        text: "/status",
      },
    },
  };
  const res = createRes();
  await handler(req, res);
  global.fetch = previousFetch;

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.denied, true);
  assert.equal(calls.length, 1);
  assert.match(calls[0].url, /sendMessage$/);
});
