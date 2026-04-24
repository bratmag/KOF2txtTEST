// netlify/functions/tc-proxy.js
//
// Proxy mellom KOF2TXT-extensionen og Trimble Connect API.

exports.handler = async function handler(event) {
  try {
    if (event.httpMethod !== "POST") {
      return jsonResponse(405, { ok: false, error: "Method not allowed" });
    }

    const body = safeJsonParse(event.body) || {};
    const { action } = body;

    if (action === "listProjectKofFiles") return await handleListProjectKofFiles(body);
    if (action === "downloadKofFile") return await handleDownloadKofFile(body);
    if (action === "probeCore") return await handleProbeCore(body);

    return jsonResponse(400, { ok: false, error: `Unknown action: ${String(action)}` });
  } catch (err) {
    console.error("tc-proxy fatal:", err);
    return jsonResponse(500, { ok: false, error: err?.message || String(err) });
  }
};

function jsonResponse(statusCode, data) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    },
    body: JSON.stringify(data, null, 2)
  };
}

function safeJsonParse(text) {
  try { return JSON.parse(text); } catch { return null; }
}

function shortText(text, max = 1200) {
  if (typeof text !== "string") return text;
  return text.length > max ? text.slice(0, max) + "..." : text;
}

function safeHost(url) {
  try { return new URL(url).host; } catch { return null; }
}

let _regionCache = null;

async function discoverRegions() {
  if (_regionCache) return _regionCache;

  try {
    const res = await fetch("https://app.connect.trimble.com/tc/api/2.0/regions");
    if (res.ok) {
      const data = await res.json();
      _regionCache = data;
      return data;
    }
  } catch (err) {
    console.error("discoverRegions failed:", err?.message || String(err));
  }

  return null;
}

function getCoreBaseUrl(projectLocation) {
  const loc = String(projectLocation || "").toLowerCase();

  if (loc === "europe") return "https://app21.connect.trimble.com/tc/api/2.0";
  if (loc === "asia") return "https://app.asia.connect.trimble.com/tc/api/2.0";

  return "https://app.connect.trimble.com/tc/api/2.0";
}

async function getCoreBaseUrlAsync(projectLocation) {
  const loc = String(projectLocation || "").toLowerCase();

  const regions = await discoverRegions();

  if (regions && Array.isArray(regions)) {
    const match = regions.find((r) => {
      const id = String(r.id || r.name || r.location || "").toLowerCase();
      return id === loc || id.includes(loc);
    });

    if (match) {
      // Viktig: Trimble sitt regions-API kan gi origin som //app21...
      // men tc-api er komplett URL. Derfor prioriterer vi tc-api.
      const tcApi = match["tc-api"] || match.tcApi || match.tc_api;

      if (tcApi) {
        return String(tcApi).replace(/\/+$/, "");
      }

      const rawUrl =
        match.origin ||
        match.api ||
        match.apiOrigin ||
        match.baseUrl ||
        match.url;

      if (rawUrl) {
        const withProtocol = String(rawUrl).startsWith("//")
          ? `https:${rawUrl}`
          : String(rawUrl);

        const base = withProtocol.replace(/\/+$/, "");
        return base.endsWith("/tc/api/2.0") ? base : `${base}/tc/api/2.0`;
      }
    }
  }

  return getCoreBaseUrl(projectLocation);
}

async function fetchRaw(url, options = {}, timeoutMs = 30000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    const contentType = res.headers.get("content-type") || "";
    const text = await res.text();
    const json = safeJsonParse(text);

    return {
      url,
      ok: res.ok,
      status: res.status,
      statusText: res.statusText,
      contentType,
      text,
      json
    };
  } finally {
    clearTimeout(timer);
  }
}

async function fetchJsonWithBearer(url, token) {
  return fetchRaw(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json"
    }
  });
}

async function fetchTextNoAuth(url) {
  return fetchRaw(url, { method: "GET" }, 60000);
}

function extractPossibleUrl(payload) {
  if (!payload || typeof payload !== "object") return null;

  return (
    payload.downloadUrl ||
    payload.downloadURL ||
    payload.uploadUrl ||
    payload.url ||
    payload.href ||
    payload.link ||
    payload.signedUrl ||
    payload.presignedUrl ||
    payload.preSignedUrl ||
    payload.data?.downloadUrl ||
    payload.data?.downloadURL ||
    payload.data?.uploadUrl ||
    payload.data?.url ||
    payload.details?.downloadUrl ||
    payload.details?.downloadURL ||
    payload.details?.uploadUrl ||
    payload.details?.url ||
    payload.result?.downloadUrl ||
    payload.result?.downloadURL ||
    payload.result?.uploadUrl ||
    payload.result?.url ||
    null
  );
}

// ─── Metadata ────────────────────────────────────────────────────────────────

async function getFileMetadata({ token, projectLocation, fileId }) {
  const base = await getCoreBaseUrlAsync(projectLocation);
  const url = `${base}/files/${encodeURIComponent(fileId)}`;
  const res = await fetchJsonWithBearer(url, token);

  return {
    ok: res.ok,
    url,
    status: res.status,
    preview: shortText(res.text, 1000),
    data: res.json
  };
}

async function getFileVersions({ token, projectLocation, fileId }) {
  const base = await getCoreBaseUrlAsync(projectLocation);
  const url = `${base}/files/${encodeURIComponent(fileId)}/versions?tokenThumburl=false`;
  const res = await fetchJsonWithBearer(url, token);

  return {
    ok: res.ok,
    url,
    status: res.status,
    preview: shortText(res.text, 1000),
    data: res.json
  };
}

// ─── Download ────────────────────────────────────────────────────────────────

async function tryCoreCandidates({ token, projectLocation, fileId, versionId }) {
  const base = await getCoreBaseUrlAsync(projectLocation);

  const candidates = [
    {
      name: "fs-downloadurl",
      url: `${base}/files/fs/${encodeURIComponent(fileId)}/downloadurl?versionId=${encodeURIComponent(versionId)}`
    },
    {
      name: "fs-downloadurl-versionId-path",
      url: `${base}/files/fs/${encodeURIComponent(versionId)}/downloadurl?versionId=${encodeURIComponent(versionId)}`
    },
    {
      name: "blobstore-versionId",
      url: `${base}/files/${encodeURIComponent(versionId)}/blobstore`
    },
    {
      name: "download-versionId",
      url: `${base}/files/${encodeURIComponent(versionId)}/download`
    }
  ];

  const diagnostics = [];

  for (const candidate of candidates) {
    try {
      const res = await fetchJsonWithBearer(candidate.url, token);
      const signedUrl = extractPossibleUrl(res.json);

      const looksLikeText =
        typeof res.text === "string" &&
        res.text.length > 0 &&
        !res.contentType.includes("application/json") &&
        !res.contentType.includes("text/html");

      diagnostics.push({
        name: candidate.name,
        url: candidate.url,
        status: res.status,
        ok: res.ok,
        foundSignedUrl: !!signedUrl,
        looksLikeText,
        preview: shortText(res.text, 300)
      });

      if (signedUrl) {
        const fileRes = await fetchTextNoAuth(signedUrl);

        if (fileRes.ok) {
          return {
            ok: true,
            source: candidate.name,
            mode: "signedUrl",
            signedUrlHost: safeHost(signedUrl),
            diagnostics,
            text: fileRes.text,
            contentType: fileRes.contentType
          };
        }

        diagnostics.push({
          name: `${candidate.name}-signed-url-fetch`,
          ok: false,
          status: fileRes.status,
          contentType: fileRes.contentType,
          signedUrlHost: safeHost(signedUrl),
          preview: shortText(fileRes.text, 300)
        });
      }

      if (res.ok && looksLikeText) {
        return {
          ok: true,
          source: candidate.name,
          mode: "directText",
          diagnostics,
          text: res.text,
          contentType: res.contentType
        };
      }
    } catch (err) {
      diagnostics.push({
        name: candidate.name,
        url: candidate.url,
        ok: false,
        error: err?.message || String(err)
      });
    }
  }

  return {
    ok: false,
    error: "Fant ingen fungerende download-kandidat.",
    diagnostics
  };
}

async function handleDownloadKofFile(body) {
  const { token, projectId, projectLocation, fileId, fileName } = body;

  if (!token || !fileId) {
    return jsonResponse(400, { ok: false, error: "Mangler token eller fileId" });
  }

  const metadata = await getFileMetadata({ token, projectLocation, fileId });

  if (!metadata.ok || !metadata.data) {
    return jsonResponse(200, {
      ok: false,
      step: "metadata",
      project: { id: projectId, location: projectLocation },
      file: { id: fileId, name: fileName },
      metadata
    });
  }

  const versions = await getFileVersions({ token, projectLocation, fileId });

  let versionId = metadata.data.versionId || metadata.data.id || fileId;

  if (versions.ok && Array.isArray(versions.data) && versions.data.length > 0) {
    versionId =
      versions.data[0]?.versionId ||
      versions.data[0]?.id ||
      versions.data[0]?.version?.id ||
      versionId;
  }

  const download = await tryCoreCandidates({
    token,
    projectLocation,
    fileId,
    versionId
  });

  if (!download.ok) {
    return jsonResponse(200, {
      ok: false,
      step: "download",
      project: { id: projectId, location: projectLocation },
      file: {
        id: metadata.data.id,
        versionId,
        name: metadata.data.name || fileName,
        parentId: metadata.data.parentId || null
      },
      download
    });
  }

  return jsonResponse(200, {
    ok: true,
    project: { id: projectId, location: projectLocation },
    file: {
      id: metadata.data.id,
      versionId,
      name: metadata.data.name || fileName,
      parentId: metadata.data.parentId || null
    },
    source: {
      candidate: download.source,
      mode: download.mode,
      signedUrlHost: download.signedUrlHost || null
    },
    contentType: download.contentType,
    text: download.text
  });
}

async function handleProbeCore(body) {
  const { token, projectId, projectLocation, fileId, fileName } = body;

  if (!token || !fileId) {
    return jsonResponse(400, { ok: false, error: "Mangler token eller fileId" });
  }

  const metadata = await getFileMetadata({ token, projectLocation, fileId });
  const versions = await getFileVersions({ token, projectLocation, fileId });

  let versionId = metadata.data?.versionId || metadata.data?.id || fileId;

  if (versions.ok && Array.isArray(versions.data) && versions.data.length > 0) {
    versionId =
      versions.data[0]?.versionId ||
      versions.data[0]?.id ||
      versions.data[0]?.version?.id ||
      versionId;
  }

  const probe = await tryCoreCandidates({
    token,
    projectLocation,
    fileId,
    versionId
  });

  return jsonResponse(200, {
    ok: true,
    probe: "core",
    project: { id: projectId, location: projectLocation },
    file: { id: fileId, name: fileName, versionId },
    metadata,
    versions,
    probeResult: probe
  });
}

// ─── List KOF-files ──────────────────────────────────────────────────────────

async function handleListProjectKofFiles(body) {
  const { token, projectId, projectLocation } = body;

  if (!token || !projectId) {
    return jsonResponse(400, { ok: false, error: "Mangler token eller projectId" });
  }

  const listResult = await tryListProjectFilesCandidates({
    token,
    projectId,
    projectLocation
  });

  return jsonResponse(200, listResult);
}

async function tryListProjectFilesCandidates({ token, projectId, projectLocation }) {
  const base = await getCoreBaseUrlAsync(projectLocation);
  const regions = await discoverRegions();

  const candidates = [
    {
      name: "search-kof",
      url: `${base}/search?projectId=${encodeURIComponent(projectId)}&query=.kof&type=file`
    },
    {
      name: "projects-files",
      url: `${base}/projects/${encodeURIComponent(projectId)}/files`
    },
    {
      name: "projects-files-recursive",
      url: `${base}/projects/${encodeURIComponent(projectId)}/files?recursive=true`
    }
  ];

  const diagnostics = [];

  for (const candidate of candidates) {
    try {
      const res = await fetchJsonWithBearer(candidate.url, token);

      diagnostics.push({
        name: candidate.name,
        url: candidate.url,
        ok: res.ok,
        status: res.status,
        preview: shortText(res.text, 400)
      });

      if (!res.ok || !res.json) continue;

      const files = normalizeFilesFromAnyResponse(res.json)
        .filter((f) => f && f.id && isKofName(f.name))
        .sort((a, b) =>
          String(a.name).localeCompare(String(b.name), undefined, {
            sensitivity: "base"
          })
        );

      if (files.length) {
        return {
          ok: true,
          action: "listProjectKofFiles",
          project: { id: projectId, location: projectLocation },
          resolvedBaseUrl: base,
          source: candidate.name,
          candidatesTried: diagnostics.length,
          files,
          diagnostics
        };
      }
    } catch (err) {
      diagnostics.push({
        name: candidate.name,
        url: candidate.url,
        ok: false,
        error: err?.message || String(err)
      });
    }
  }

  return {
    ok: false,
    action: "listProjectKofFiles",
    error: "Fant ingen fungerende kandidat for fillisting, eller ingen .kof-filer i prosjektet.",
    project: { id: projectId, location: projectLocation },
    resolvedBaseUrl: base,
    regionsDiscovered: regions,
    candidatesTried: diagnostics.length,
    diagnostics
  };
}

function isKofName(name) {
  return /\.kof$/i.test(String(name || ""));
}

function normalizePathValue(pathValue) {
  if (!pathValue) return "";

  if (typeof pathValue === "string") return pathValue;

  if (Array.isArray(pathValue)) {
    return pathValue
      .map((item) => {
        if (!item) return "";
        if (typeof item === "string") return item;
        if (typeof item === "object") return item.name || item.title || item.id || "";
        return "";
      })
      .filter(Boolean)
      .join("/");
  }

  if (typeof pathValue === "object") {
    return pathValue.name || pathValue.title || pathValue.id || "";
  }

  return String(pathValue);
}

function normalizeFilesFromAnyResponse(payload) {
  const out = [];
  const seen = new Set();
  walkAny(payload, [], out, seen);
  return out;
}

function walkAny(node, pathParts, out, seen) {
  if (node == null) return;

  if (Array.isArray(node)) {
    for (const item of node) walkAny(item, pathParts, out, seen);
    return;
  }

  if (typeof node !== "object") return;

  const details = node.details && typeof node.details === "object"
    ? node.details
    : null;

  const effectiveName =
    node.name ||
    node.fileName ||
    node.filename ||
    node.title ||
    details?.name ||
    details?.fileName ||
    null;

  const effectiveId =
    node.id ||
    node.fileId ||
    node.versionId ||
    details?.id ||
    details?.fileId ||
    null;

  const effectiveParentId =
    node.parentId ||
    node.parent?.id ||
    details?.parentId ||
    null;

  const effectiveVersionId =
    node.versionId ||
    details?.versionId ||
    null;

  const effectivePath =
    node.path ||
    node.folderPath ||
    node.fullPath ||
    node.location ||
    details?.path ||
    null;

  const childPath = effectiveName ? [...pathParts, effectiveName] : pathParts;

  if (effectiveId && effectiveName) {
    const normalized = {
      id: String(effectiveId),
      name: String(effectiveName),
      versionId: effectiveVersionId ? String(effectiveVersionId) : null,
      parentId: effectiveParentId ? String(effectiveParentId) : null,
      path: effectivePath ? normalizePathValue(effectivePath) : buildPath(pathParts)
    };

    const key = `${normalized.id}|${normalized.name}|${normalized.path}`;

    if (!seen.has(key)) {
      seen.add(key);
      out.push(normalized);
    }
  }

  for (const [key, value] of Object.entries(node)) {
    if (
      key === "parent" ||
      key === "parents" ||
      key === "_links" ||
      key === "links" ||
      key === "permissions"
    ) {
      continue;
    }

    if (Array.isArray(value) || (value && typeof value === "object")) {
      walkAny(value, childPath, out, seen);
    }
  }
}

function buildPath(parts) {
  const p = (parts || [])
    .filter(Boolean)
    .map((x) => String(x).trim())
    .filter(Boolean);

  return p.length ? p.join("/") : "";
}
