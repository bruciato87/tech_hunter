const DEFAULT_DISPATCH_EVENT = "scan";

function envOrDefault(name, fallback) {
  const value = process.env[name];
  if (!value || !value.trim()) {
    return fallback;
  }
  return value.trim();
}

function parseProducts(payload) {
  const value = payload?.products;
  const normalize = (candidate) => {
    if (!candidate || typeof candidate !== "object") {
      return null;
    }
    const title = typeof candidate.title === "string" ? candidate.title.trim() : "";
    const priceRaw = candidate.price_eur ?? candidate.price;
    const price = Number(priceRaw);
    if (!title || !Number.isFinite(price) || price <= 0) {
      return null;
    }
    return {
      ...candidate,
      title,
      price_eur: price,
    };
  };
  if (Array.isArray(value)) {
    return value
      .map((item) => normalize(item))
      .filter(Boolean);
  }
  if (value && typeof value === "object") {
    const normalized = normalize(value);
    return normalized ? [normalized] : [];
  }
  if (payload && typeof payload === "object" && payload.title) {
    const normalized = normalize(payload);
    return normalized ? [normalized] : [];
  }
  return [];
}

async function dispatchToGitHub({ githubToken, owner, repo, eventType, payload }) {
  const response = await fetch(`https://api.github.com/repos/${owner}/${repo}/dispatches`, {
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

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  const scanSecret = process.env.SCAN_SECRET;
  if (!scanSecret || !scanSecret.trim()) {
    return res.status(500).json({ ok: false, error: "Missing env configuration: SCAN_SECRET" });
  }
  const authHeader = req.headers.authorization || "";
  if (authHeader !== `Bearer ${scanSecret.trim()}`) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  const githubToken = process.env.GITHUB_TOKEN;
  const githubOwner = process.env.GITHUB_OWNER;
  const githubRepo = process.env.GITHUB_REPO;
  const eventType = envOrDefault("GITHUB_EVENT_TYPE", DEFAULT_DISPATCH_EVENT);
  if (!githubToken || !githubOwner || !githubRepo) {
    return res.status(500).json({ ok: false, error: "Missing env configuration" });
  }

  const products = parseProducts(req.body || {});
  if (!products.length) {
    return res.status(400).json({ ok: false, error: "No valid products supplied" });
  }

  try {
    await dispatchToGitHub({
      githubToken,
      owner: githubOwner,
      repo: githubRepo,
      eventType,
      payload: {
        source: "vercel_scan_api",
        command: "scan",
        products,
      },
    });
    return res.status(202).json({ ok: true, queued: true, product_count: products.length });
  } catch (error) {
    const details = error instanceof Error ? error.message : "unknown_error";
    return res.status(500).json({ ok: false, error: details });
  }
}
