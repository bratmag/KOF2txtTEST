// netlify/functions/tc-proxy.js
//
// Proxy between the KOF2TXT extension and the Trimble Connect API.

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
    if (action === "uploadConvertedTxt") return await handleUploadConvertedTxt(body);

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

function md5Base64(buffer) {
  return require("crypto").createHash("md5").update(buffer).digest("base64");
}

function isUploadAlreadyCompleted(res) {
  const text = String(res?.text || "");
  const details = String(res?.json?.details || res?.json?.message || "");
  return /file upload already completed/i.test(text) ||
    /file upload already completed/i.test(details);
}

let regionCache = null;

async function discoverRegions() {
  if (regionCache) return regionCache;

  try {
    const res = await fetch("https://app.connect.trimble.com/tc/api/2.0/regions");
    if (res.ok) {
      const data = await res.json();
      regionCache = data;
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

async function fetchWithBearer(url, token, options = {}, timeoutMs = 30000) {
  return fetchRaw(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
      ...(options.headers || {})
    }
  }, timeoutMs);
}

async function fetchTextNoAuth(url) {
  return fetchRaw(url, { method: "GET" }, 60000);
}

function extractPossibleUrl(payload) {
  if (!payload || typeof payload !== "object") return null;

  const direct =
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
    null;

  if (direct) return direct;

  if (Array.isArray(payload.contents)) {
    for (const item of payload.contents) {
      const nested = extractPossibleUrl(item);
      if (nested) return nested;
    }
  }

  if (Array.isArray(payload.data?.contents)) {
    for (const item of payload.data.contents) {
      const nested = extractPossibleUrl(item);
      if (nested) return nested;
    }
  }

  if (Array.isArray(payload.result?.contents)) {
    for (const item of payload.result.contents) {
      const nested = extractPossibleUrl(item);
      if (nested) return nested;
    }
  }

  return null;
}

function extractUploadInfo(payload) {
  if (!payload || typeof payload !== "object") {
    return { uploadId: null, uploadUrl: null, completeUrl: null };
  }

  return {
    uploadId:
      payload.uploadId ||
      payload.id ||
      payload.data?.uploadId ||
      payload.data?.id ||
      payload.result?.uploadId ||
      payload.result?.id ||
      null,
    fileId:
      payload.fileId ||
      payload.data?.fileId ||
      payload.result?.fileId ||
      null,
    uploadUrl: extractPossibleUrl(payload),
    contents:
      payload.contents ||
      payload.data?.contents ||
      payload.result?.contents ||
      [],
    completeUrl:
      payload.completeUrl ||
      payload.completionUrl ||
      payload.data?.completeUrl ||
      payload.data?.completionUrl ||
      payload.result?.completeUrl ||
      payload.result?.completionUrl ||
      null
  };
}

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

async function uploadToSignedUrl(uploadUrl, fileBuffer, diagnostics) {
  const body = new Uint8Array(fileBuffer);
  const methods = [
    { method: "PUT", headers: { "Content-Type": "text/plain; charset=utf-8" } },
    { method: "PUT", headers: { "Content-Type": "application/octet-stream" } },
    { method: "PUT", headers: {} },
    { method: "POST", headers: { "Content-Type": "text/plain; charset=utf-8" } }
  ];

  for (const candidate of methods) {
    const res = await fetchRaw(uploadUrl, {
      method: candidate.method,
      headers: candidate.headers,
      body
    }, 120000);

    diagnostics.push({
      step: "signed-upload",
      method: candidate.method,
      status: res.status,
      ok: res.ok,
      host: safeHost(uploadUrl),
      preview: shortText(res.text, 300)
    });

    if (res.ok) {
      return { ok: true, method: candidate.method };
    }
  }

  return { ok: false, error: "Ingen signed URL upload-metode fungerte." };
}

function buildCompleteBodies({ uploadId, uploadInfo, fileBuffer, digest, digestHeader }) {
  const fileId = uploadInfo?.fileId || null;
  const contents = Array.isArray(uploadInfo?.contents) ? uploadInfo.contents : [];
  const content = contents[0] && typeof contents[0] === "object" ? contents[0] : {};
  const size = fileBuffer.length;
  const contentWithoutUrl = { ...content };
  delete contentWithoutUrl.url;

  const contentDigest = {
    ...contentWithoutUrl,
    digest: digestHeader,
    md5: digest,
    contentMD5: digest,
    contentMd5: digest,
    size
  };

  return [
    { label: "body-format-single-part-underscore", body: { format: "SINGLE_PART" } },
    { label: "body-format-single-part-lower", body: { format: "single_part" } },
    { label: "body-format-singlepart-lower", body: { format: "singlepart" } },
    { label: "body-format-single-part-camel", body: { format: "singlePart" } },
    { label: "body-format-single-part-with-upload-id", body: { uploadId, format: "SINGLE_PART" } },
    { label: "body-format-single-part-with-file-id", body: { fileId, format: "SINGLE_PART" } },
    { label: "body-type-singlepart-with-file-id", body: { fileId, type: "SINGLEPART" } },
    { label: "body-file-id-only", body: { fileId } },
    { label: "body-upload-id-only", body: { uploadId } },
    { label: "body-digest-fields", body: { digest: digestHeader, md5: digest, contentMD5: digest, size } },
    { label: "body-file-id-digest-fields", body: { fileId, digest: digestHeader, md5: digest, contentMD5: digest, size } },
    { label: "body-contents-digest", body: { contents: [contentDigest] } },
    { label: "body-format-contents-digest", body: { format: "SINGLE_PART", contents: [contentDigest] } },
    { label: "body-type-contents-digest", body: { type: "SINGLEPART", contents: [contentDigest] } },
    { label: "body-file-id-contents-digest", body: { fileId, contents: [contentDigest] } },
    { label: "body-upload-id-contents-digest", body: { uploadId, contents: [contentDigest] } }
  ].filter((candidate) => {
    if (candidate.body.fileId === null) return false;
    return true;
  });
}

async function completeUpload({ token, projectLocation, uploadId, completeUrl, uploadInfo, fileBuffer, diagnostics }) {
  const base = await getCoreBaseUrlAsync(projectLocation);
  const candidates = [];
  const digest = md5Base64(fileBuffer);
  const digestHeader = `MD5=${digest}`;
  const digestHeaderLower = `md5=${digest}`;

  if (completeUrl) {
    candidates.push({
      label: "provided-complete-url-digest",
      url: completeUrl,
      headers: { Digest: digestHeader },
      body: undefined
    });
  }
  if (uploadId) {
    const completePath = `${base}/files/fs/upload/${encodeURIComponent(uploadId)}/complete`;

    for (const bodyCandidate of buildCompleteBodies({ uploadId, uploadInfo, fileBuffer, digest, digestHeader })) {
      candidates.push({
        label: `complete-path-${bodyCandidate.label}`,
        url: completePath,
        headers: { "Content-Type": "application/json", Digest: digestHeader },
        body: bodyCandidate.body
      });
    }

    candidates.push({
      label: "complete-path-json-content-type-no-body",
      url: completePath,
      headers: { "Content-Type": "application/json", Digest: digestHeader },
      body: undefined
    });
    candidates.push({
      label: "complete-path-json-content-type-lower-digest-no-body",
      url: completePath,
      headers: { "Content-Type": "application/json", Digest: digestHeaderLower },
      body: undefined
    });
    candidates.push({
      label: "complete-path-octet-content-type-no-body",
      url: completePath,
      headers: { "Content-Type": "application/octet-stream", Digest: digestHeader },
      body: undefined
    });
    candidates.push({
      label: "complete-path-text-content-type-no-body",
      url: completePath,
      headers: { "Content-Type": "text/plain", Digest: digestHeader },
      body: undefined
    });
    candidates.push({
      label: "complete-path-singlepart-query-no-body",
      url: `${completePath}?format=SINGLEPART`,
      headers: { "Content-Type": "application/json", Digest: digestHeader },
      body: undefined
    });
    candidates.push({
      label: "complete-path-type-singlepart-query-no-body",
      url: `${completePath}?type=SINGLEPART`,
      headers: { "Content-Type": "application/json", Digest: digestHeader },
      body: undefined
    });
    candidates.push({
      label: "complete-path-multipart-false-query-no-body",
      url: `${completePath}?multipart=false`,
      headers: { "Content-Type": "application/json", Digest: digestHeader },
      body: undefined
    });
    candidates.push({
      label: "complete-path-digest-empty-json",
      url: completePath,
      headers: { "Content-Type": "application/json", Digest: digestHeader },
      body: {}
    });
    candidates.push({
      label: "complete-path-singlepart-body",
      url: completePath,
      headers: { "Content-Type": "application/json", Digest: digestHeader },
      body: { format: "SINGLEPART" }
    });
    candidates.push({
      label: "complete-path-type-singlepart-body",
      url: completePath,
      headers: { "Content-Type": "application/json", Digest: digestHeader },
      body: { type: "SINGLEPART" }
    });
    candidates.push({
      label: "complete-path-upload-id-body",
      url: completePath,
      headers: { "Content-Type": "application/json", Digest: digestHeader },
      body: { uploadId }
    });

    candidates.push({
      label: "upload-complete-query",
      url: `${base}/files/fs/upload/complete?uploadId=${encodeURIComponent(uploadId)}`,
      headers: { Digest: digestHeader },
      body: {}
    });
    candidates.push({
      label: "upload-id-root",
      url: `${base}/files/fs/upload/${encodeURIComponent(uploadId)}`,
      headers: { Digest: digestHeader },
      body: {}
    });
  }

  for (const candidate of candidates) {
    const hasBody = candidate.body !== undefined;
    const res = await fetchWithBearer(candidate.url, token, {
      method: "POST",
      headers: candidate.headers || {},
      body: hasBody ? JSON.stringify(candidate.body) : undefined
    });

    diagnostics.push({
      step: "complete-upload",
      variant: candidate.label,
      url: candidate.url,
      status: res.status,
      ok: res.ok,
      preview: shortText(res.text, 300)
    });

    if (res.ok) {
      return { ok: true, response: res.json || res.text };
    }

    if (isUploadAlreadyCompleted(res)) {
      return {
        ok: true,
        alreadyCompleted: true,
        response: {
          status: "completed",
          completionState: "already_completed"
        }
      };
    }
  }

  return { ok: false, error: "Kunne ikke fullføre opplastingen." };
}

async function tryDirectMultipartUpload({ token, projectLocation, parentId, fileName, fileBuffer }) {
  const base = await getCoreBaseUrlAsync(projectLocation);
  const uploadTargets = [
    { url: `${base}/files?parentId=${encodeURIComponent(parentId)}`, mode: "form-data-file" },
    { url: `${base}/files?parentId=${encodeURIComponent(parentId)}&name=${encodeURIComponent(fileName)}`, mode: "octet-stream-name-query", contentType: "application/octet-stream" },
    { url: `${base}/files?parentId=${encodeURIComponent(parentId)}&fileName=${encodeURIComponent(fileName)}`, mode: "octet-stream-filename-query", contentType: "application/octet-stream" }
  ];
  const diagnostics = [];

  for (const target of uploadTargets) {
    let body;
    let headers = {};

    if (target.mode === "form-data-file") {
      const form = new FormData();
      form.append("file", new Blob([fileBuffer], { type: "text/plain;charset=utf-8" }), fileName);
      body = form;
    } else {
      body = new Uint8Array(fileBuffer);
      headers = { "Content-Type": target.contentType };
    }

    const res = await fetchWithBearer(target.url, token, {
      method: "POST",
      headers,
      body
    }, 120000);

    diagnostics.push({
      mode: "direct-multipart",
      variant: target.mode,
      url: target.url,
      status: res.status,
      ok: res.ok,
      preview: shortText(res.text, 300)
    });

    if (res.ok) {
      return {
        ok: true,
        mode: "direct-multipart",
        diagnostics,
        response: res.json || res.text
      };
    }
  }

  return { ok: false, mode: "direct-multipart", diagnostics };
}

async function trySignedUploadFlow({ token, projectLocation, parentId, fileName, fileBuffer }) {
  const base = await getCoreBaseUrlAsync(projectLocation);
  const endpoints = [
    {
      label: "parentId-only",
      url: `${base}/files/fs/upload?parentId=${encodeURIComponent(parentId)}`,
      body: { name: fileName }
    },
    {
      label: "parentType-folder-lower",
      url: `${base}/files/fs/upload?parentId=${encodeURIComponent(parentId)}&parentType=folder`,
      body: { name: fileName }
    },
    {
      label: "parentType-folder-upper",
      url: `${base}/files/fs/upload?parentId=${encodeURIComponent(parentId)}&parentType=FOLDER`,
      body: { name: fileName }
    },
    {
      label: "parentType-projectfile",
      url: `${base}/files/fs/upload?parentId=${encodeURIComponent(parentId)}&parentType=PROJECT_FILE`,
      body: { name: fileName }
    },
    {
      label: "folderId-only",
      url: `${base}/files/fs/upload?folderId=${encodeURIComponent(parentId)}`,
      body: { name: fileName }
    },
    {
      label: "json-parent-body",
      url: `${base}/files/fs/upload`,
      body: { name: fileName, parentId, parentType: "FOLDER" }
    }
  ];
  const diagnostics = [];

  for (const endpoint of endpoints) {
    const initRes = await fetchWithBearer(endpoint.url, token, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(endpoint.body)
    });

    diagnostics.push({
      mode: "signed-init",
      variant: endpoint.label,
      url: endpoint.url,
      status: initRes.status,
      ok: initRes.ok,
      preview: shortText(initRes.text, 300)
    });

    if (!initRes.ok || !initRes.json) continue;

    const uploadInfo = extractUploadInfo(initRes.json);
    if (!uploadInfo.uploadUrl) continue;

    const uploadRes = await uploadToSignedUrl(uploadInfo.uploadUrl, fileBuffer, diagnostics);
    if (!uploadRes.ok) continue;

    if (!uploadInfo.uploadId && !uploadInfo.completeUrl) {
      return {
        ok: true,
        mode: "signed-upload",
        diagnostics,
        response: initRes.json
      };
    }

    const completeRes = await completeUpload({
      token,
      projectLocation,
      uploadId: uploadInfo.uploadId,
      completeUrl: uploadInfo.completeUrl,
      uploadInfo,
      fileBuffer,
      diagnostics
    });

    if (completeRes.ok) {
      return {
        ok: true,
        mode: "signed-upload",
        diagnostics,
        response: completeRes.response
      };
    }
  }

  return { ok: false, mode: "signed-upload", diagnostics };
}

async function handleUploadConvertedTxt(body) {
  const { token, projectId, projectLocation, parentId, fileName, text } = body;

  if (!token || !projectId || !parentId || !fileName || typeof text !== "string") {
    return jsonResponse(400, { ok: false, error: "Mangler token, projectId, parentId, fileName eller text" });
  }

  const fileBuffer = Buffer.from(text, "utf8");
  const attempts = [];

  const direct = await tryDirectMultipartUpload({
    token,
    projectLocation,
    parentId,
    fileName,
    fileBuffer
  });
  attempts.push(direct);

  if (direct.ok) {
    return jsonResponse(200, {
      ok: true,
      action: "uploadConvertedTxt",
      project: { id: projectId, location: projectLocation },
      upload: {
        mode: direct.mode,
        parentId,
        fileName,
        size: fileBuffer.length
      },
      response: direct.response,
      diagnostics: direct.diagnostics
    });
  }

  const signed = await trySignedUploadFlow({
    token,
    projectLocation,
    parentId,
    fileName,
    fileBuffer
  });
  attempts.push(signed);

  if (signed.ok) {
    return jsonResponse(200, {
      ok: true,
      action: "uploadConvertedTxt",
      project: { id: projectId, location: projectLocation },
      upload: {
        mode: signed.mode,
        parentId,
        fileName,
        size: fileBuffer.length
      },
      response: signed.response,
      diagnostics: signed.diagnostics
    });
  }

  return jsonResponse(200, {
    ok: false,
    action: "uploadConvertedTxt",
    error: "Kunne ikke laste opp TXT-filen automatisk.",
    project: { id: projectId, location: projectLocation },
    upload: {
      parentId,
      fileName,
      size: fileBuffer.length
    },
    attempts
  });
}

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
      name: "projects-files-recursive",
      url: `${base}/projects/${encodeURIComponent(projectId)}/files?recursive=true`
    },
    {
      name: "projects-files",
      url: `${base}/projects/${encodeURIComponent(projectId)}/files`
    },
    {
      name: "search-kof",
      url: `${base}/search?projectId=${encodeURIComponent(projectId)}&query=.kof&type=file`
    }
  ];

  const diagnostics = [];
  const filesByKey = new Map();
  const successSources = [];

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
        successSources.push({
          name: candidate.name,
          fileCount: files.length
        });

        for (const file of files) {
          const key = `${file.id}|${file.parentId || ""}|${file.name}`;
          if (!filesByKey.has(key)) {
            filesByKey.set(key, file);
          }
        }
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

  const files = Array.from(filesByKey.values()).sort((a, b) =>
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
      source: successSources.map((x) => x.name).join("+"),
      candidatesTried: diagnostics.length,
      files,
      diagnostics,
      sources: successSources
    };
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
