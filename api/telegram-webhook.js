const TELEGRAM_API_BASE = "https://api.telegram.org";
const DEFAULT_DISPATCH_EVENT = "scan";
const MAX_LAST_LIMIT = 10;
const STRATEGY_PROFILE_VAR = "STRATEGY_PROFILE";
const STRATEGY_PROFILES = new Set(["conservative", "balanced", "aggressive"]);

function envOrDefault(name, fallback) {
  const value = process.env[name];
  if (!value || !value.trim()) {
    return fallback;
  }
  return value.trim();
}

function parseAllowedChatIds(value) {
  return new Set(
    (value || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean)
  );
}

function stripCodeFence(raw) {
  let text = (raw || "").trim();
  if (!text.startsWith("```")) {
    return text;
  }
  text = text.replace(/^```[a-zA-Z]*\s*/, "");
  text = text.replace(/```$/, "");
  return text.trim();
}

function parseProductsArg(arg) {
  const raw = stripCodeFence(arg);
  const parsed = JSON.parse(raw);
  if (Array.isArray(parsed)) {
    return parsed.filter((item) => item && typeof item === "object");
  }
  if (parsed && typeof parsed === "object") {
    return [parsed];
  }
  return [];
}

async function sendTelegramMessage(botToken, chatId, text) {
  const url = `${TELEGRAM_API_BASE}/bot${botToken}/sendMessage`;
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      disable_web_page_preview: true,
    }),
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Telegram sendMessage failed: ${response.status} ${body}`);
  }
}

async function dispatchToGitHub({ githubToken, owner, repo, eventType, payload }) {
  const url = `https://api.github.com/repos/${owner}/${repo}/dispatches`;
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${githubToken}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      event_type: eventType,
      client_payload: payload,
    }),
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`GitHub dispatch failed: ${response.status} ${body}`);
  }
}

async function githubApiRequest({ githubToken, method, path, body }) {
  const response = await fetch(`https://api.github.com${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${githubToken}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type": "application/json",
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await response.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  return {
    ok: response.ok,
    status: response.status,
    text,
    json,
  };
}

async function getGitHubVariable({ githubToken, owner, repo, name }) {
  const result = await githubApiRequest({
    githubToken,
    method: "GET",
    path: `/repos/${owner}/${repo}/actions/variables/${encodeURIComponent(name)}`,
  });
  if (result.status === 404) {
    return null;
  }
  if (!result.ok) {
    throw new Error(`GitHub variable read failed: ${result.status} ${result.text}`);
  }
  return result.json || null;
}

async function upsertGitHubVariable({ githubToken, owner, repo, name, value }) {
  const update = await githubApiRequest({
    githubToken,
    method: "PATCH",
    path: `/repos/${owner}/${repo}/actions/variables/${encodeURIComponent(name)}`,
    body: { name, value },
  });
  if (update.ok) {
    return;
  }
  if (update.status !== 404) {
    throw new Error(`GitHub variable update failed: ${update.status} ${update.text}`);
  }
  const create = await githubApiRequest({
    githubToken,
    method: "POST",
    path: `/repos/${owner}/${repo}/actions/variables`,
    body: { name, value },
  });
  if (!create.ok) {
    throw new Error(`GitHub variable create failed: ${create.status} ${create.text}`);
  }
}

function parseProfileArg(rawArg) {
  const raw = (rawArg || "").trim().toLowerCase();
  if (!raw || raw === "show" || raw === "get" || raw === "status") {
    return { mode: "show" };
  }
  const token = raw.split(/\s+/)[0];
  if (!STRATEGY_PROFILES.has(token)) {
    return { mode: "invalid", value: token };
  }
  return { mode: "set", value: token };
}

function commandHelpText() {
  return [
    "Tech_Sniper_IT comandi:",
    "/start - Inizializza il bot",
    "/help - Mostra questa guida",
    "/id - Mostra chat_id corrente",
    "/rules - Mostra regole arbitraggio",
    "/scan [json_prodotto|json_array] - Avvia scan su GitHub Actions",
    "/smoke - Test veloce (pochi prodotti) su GitHub Actions",
    "/status - Stato runtime (via GitHub Actions)",
    "/last [n] - Ultime opportunita da Supabase (via GitHub Actions)",
    "/profile [show|conservative|balanced|aggressive] - Mostra/imposta strategy profile",
  ].join("\n");
}

function rulesText() {
  return [
    "Regole attive:",
    "photography -> MPB + Rebuy (max)",
    "apple_phone -> TrendDevice + Rebuy (max)",
    "general_tech -> Rebuy",
    "Condizioni target: Grado A / Ottimo / Come nuovo",
    "Notifica quando spread > MIN_SPREAD_EUR",
  ].join("\n");
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  const webhookSecret = process.env.TELEGRAM_WEBHOOK_SECRET_TOKEN;
  const secretHeader = req.headers["x-telegram-bot-api-secret-token"];
  if (webhookSecret && secretHeader !== webhookSecret) {
    return res.status(401).json({ ok: false, error: "Invalid webhook secret" });
  }

  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) {
    return res.status(500).json({ ok: false, error: "Missing env configuration" });
  }
  const githubToken = process.env.GITHUB_TOKEN;
  const githubOwner = process.env.GITHUB_OWNER;
  const githubRepo = process.env.GITHUB_REPO;
  const eventType = envOrDefault("GITHUB_EVENT_TYPE", DEFAULT_DISPATCH_EVENT);

  const update = req.body || {};
  const message = update.message || update.edited_message;
  const chatId = message?.chat?.id;
  const userId = message?.from?.id;
  const text = (message?.text || "").trim();
  if (!chatId || !text.startsWith("/")) {
    return res.status(200).json({ ok: true, ignored: true });
  }

  const allowedChatIds = parseAllowedChatIds(process.env.TELEGRAM_ALLOWED_CHAT_IDS);
  if (allowedChatIds.size > 0 && !allowedChatIds.has(String(chatId))) {
    try {
      await sendTelegramMessage(botToken, chatId, "Chat non autorizzata.");
    } catch {
      // Best-effort reply for unauthorized requests.
    }
    return res.status(200).json({ ok: true, denied: true });
  }

  const commandToken = text.split(/\s+/)[0] || "";
  const command = commandToken.replace(/^\/([^@\s]+).*$/, "$1").toLowerCase();
  const args = text.slice(commandToken.length).trim();
  const githubReady = Boolean(githubToken && githubOwner && githubRepo);

  try {
    if (command === "start") {
      await sendTelegramMessage(botToken, chatId, "Bot operativo.\n" + commandHelpText());
      return res.status(200).json({ ok: true });
    }
    if (command === "help") {
      await sendTelegramMessage(botToken, chatId, commandHelpText());
      return res.status(200).json({ ok: true });
    }
    if (command === "id") {
      await sendTelegramMessage(botToken, chatId, `chat_id: ${chatId}`);
      return res.status(200).json({ ok: true });
    }
    if (command === "rules") {
      await sendTelegramMessage(botToken, chatId, rulesText());
      return res.status(200).json({ ok: true });
    }

    if (command === "profile") {
      if (!githubReady) {
        await sendTelegramMessage(
          botToken,
          chatId,
          "Config mancante su Vercel: GITHUB_TOKEN/GITHUB_OWNER/GITHUB_REPO."
        );
        return res.status(200).json({ ok: true });
      }
      const parsed = parseProfileArg(args);
      if (parsed.mode === "invalid") {
        await sendTelegramMessage(
          botToken,
          chatId,
          "Valore non valido. Usa: /profile conservative | balanced | aggressive"
        );
        return res.status(200).json({ ok: true });
      }
      if (parsed.mode === "show") {
        const variable = await getGitHubVariable({
          githubToken,
          owner: githubOwner,
          repo: githubRepo,
          name: STRATEGY_PROFILE_VAR,
        });
        const current = (variable?.value || envOrDefault(STRATEGY_PROFILE_VAR, "balanced")).toLowerCase();
        await sendTelegramMessage(
          botToken,
          chatId,
          `Profilo strategia attivo: ${current}\nUsa /profile conservative|balanced|aggressive`
        );
        return res.status(200).json({ ok: true });
      }
      await upsertGitHubVariable({
        githubToken,
        owner: githubOwner,
        repo: githubRepo,
        name: STRATEGY_PROFILE_VAR,
        value: parsed.value,
      });
      await sendTelegramMessage(
        botToken,
        chatId,
        `Profilo strategia aggiornato a '${parsed.value}'. Sara applicato alle prossime run.`
      );
      return res.status(200).json({ ok: true });
    }

    if (command === "scan") {
      if (!githubReady) {
        await sendTelegramMessage(
          botToken,
          chatId,
          "Config mancante su Vercel: GITHUB_TOKEN/GITHUB_OWNER/GITHUB_REPO."
        );
        return res.status(200).json({ ok: true });
      }
      let products = null;
      if (args) {
        try {
          products = parseProductsArg(args);
        } catch {
          await sendTelegramMessage(botToken, chatId, "JSON non valido. Invia un oggetto o array JSON.");
          return res.status(200).json({ ok: true });
        }
        if (!products.length) {
          await sendTelegramMessage(botToken, chatId, "Payload vuoto: nessun prodotto valido.");
          return res.status(200).json({ ok: true });
        }
      }
      await dispatchToGitHub({
        githubToken,
        owner: githubOwner,
        repo: githubRepo,
        eventType,
        payload: {
          source: "telegram",
          command: "scan",
          chat_id: String(chatId),
          user_id: String(userId || ""),
          ...(products ? { products } : {}),
        },
      });
      if (products) {
        await sendTelegramMessage(botToken, chatId, `Scan accodato su GitHub Actions (${products.length} prodotti).`);
      } else {
        await sendTelegramMessage(
          botToken,
          chatId,
          "Scan accodato su GitHub Actions (modalita default: sorgenti prodotto configurate nel worker)."
        );
      }
      return res.status(200).json({ ok: true });
    }

    if (command === "smoke") {
      if (!githubReady) {
        await sendTelegramMessage(
          botToken,
          chatId,
          "Config mancante su Vercel: GITHUB_TOKEN/GITHUB_OWNER/GITHUB_REPO."
        );
        return res.status(200).json({ ok: true });
      }

      const products = [
        { title: "Apple iPhone 14 128GB", price_eur: 650, category: "apple_phone" },
        { title: "Apple Watch Ultra 2 GPS + Cellular 49mm", price_eur: 750, category: "smartwatch" },
        { title: "Valve Steam Deck OLED 1TB", price_eur: 420, category: "handheld_console" },
      ];

      await dispatchToGitHub({
        githubToken,
        owner: githubOwner,
        repo: githubRepo,
        eventType,
        payload: {
          source: "telegram",
          command: "scan",
          mode: "smoke",
          chat_id: String(chatId),
          user_id: String(userId || ""),
          products,
        },
      });
      await sendTelegramMessage(botToken, chatId, `Smoke test accodato su GitHub Actions (${products.length} prodotti).`);
      return res.status(200).json({ ok: true });
    }

    if (command === "status") {
      if (!githubReady) {
        await sendTelegramMessage(
          botToken,
          chatId,
          "Config mancante su Vercel: GITHUB_TOKEN/GITHUB_OWNER/GITHUB_REPO."
        );
        return res.status(200).json({ ok: true });
      }
      await dispatchToGitHub({
        githubToken,
        owner: githubOwner,
        repo: githubRepo,
        eventType,
        payload: {
          source: "telegram",
          command: "status",
          chat_id: String(chatId),
          user_id: String(userId || ""),
        },
      });
      await sendTelegramMessage(botToken, chatId, "Richiesta status accodata su GitHub Actions.");
      return res.status(200).json({ ok: true });
    }

    if (command === "last") {
      if (!githubReady) {
        await sendTelegramMessage(
          botToken,
          chatId,
          "Config mancante su Vercel: GITHUB_TOKEN/GITHUB_OWNER/GITHUB_REPO."
        );
        return res.status(200).json({ ok: true });
      }
      const requested = Number.parseInt(args || "5", 10);
      const limit = Number.isFinite(requested)
        ? Math.max(1, Math.min(requested, MAX_LAST_LIMIT))
        : 5;
      await dispatchToGitHub({
        githubToken,
        owner: githubOwner,
        repo: githubRepo,
        eventType,
        payload: {
          source: "telegram",
          command: "last",
          chat_id: String(chatId),
          user_id: String(userId || ""),
          limit,
        },
      });
      await sendTelegramMessage(botToken, chatId, `Richiesta ultime opportunita (${limit}) accodata su GitHub Actions.`);
      return res.status(200).json({ ok: true });
    }

    await sendTelegramMessage(botToken, chatId, "Comando non riconosciuto.\n" + commandHelpText());
    return res.status(200).json({ ok: true });
  } catch (error) {
    const details = error instanceof Error ? error.message : "unknown_error";
    try {
      await sendTelegramMessage(botToken, chatId, `Errore comando: ${details}`);
    } catch {
      // If Telegram send fails too, still return API error.
    }
    // Always acknowledge Telegram updates to prevent endless retries.
    return res.status(200).json({ ok: false, error: details });
  }
}
