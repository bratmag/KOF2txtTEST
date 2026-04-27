(() => {
  "use strict";

  const CONFIG = {
    DEBUG: false,
    CONNECT_TIMEOUT_MS: 30000,
    TOKEN_WAIT_MS: 30000,
    PROXY_URL: "/.netlify/functions/tc-proxy",
    APP_TITLE: "KOF2TXT-TEST",
    MENU_MAIN_COMMAND: "KOF2TXT_TEST_MAIN",
    MENU_OPEN_COMMAND: "KOF2TXT_TEST_OPEN"
  };

  const state = {
    api: null,
    accessToken: null,
    project: null,
    selectedFile: null,
    fileList: [],
    tokenWaiters: [],
    isEmbedded: false,
    lastResult: null,
    busy: false,
    explorerApi: null,
    explorerVisible: false,
    lastDownloadName: null,
    lastAutoRefreshAt: 0
  };

  let ui = {};

  function log(...args) { console.log(...args); }
  function debug(...args) { if (CONFIG.DEBUG) console.log(...args); }

  function setStatus(message, kind = "neutral") {
    log(`[STATUS] ${message}`);
    if (!ui.status) return;
    ui.status.textContent = message;
    ui.status.className = `status ${kind === "neutral" ? "" : kind}`;
    if (state.api?.extension?.setStatusMessage) {
      state.api.extension.setStatusMessage(message).catch(() => {});
    }
  }

  function setDebug(data) {
    if (!ui.debugOutput) return;
    ui.debugOutput.textContent = typeof data === "string" ? data : JSON.stringify(data, null, 2);
  }

  function setBusy(busy) {
    state.busy = busy;
    if (ui.refreshBtn) ui.refreshBtn.disabled = busy;
    if (ui.localUploadBtn) ui.localUploadBtn.disabled = busy;
    if (ui.projectUploadBtn) ui.projectUploadBtn.disabled = busy || !canOpenProjectUpload();
    if (ui.convertSelectedBtn) ui.convertSelectedBtn.disabled = busy || !state.selectedFile;
    if (ui.convertAllBtn) ui.convertAllBtn.disabled = busy || !state.fileList.length;
  }

  function shortText(text, len = 1500) {
    if (typeof text !== "string") return text;
    return text.length > len ? text.slice(0, len) + "..." : text;
  }

  function safeJsonParse(text) {
    try { return JSON.parse(text); } catch { return null; }
  }

  function withTimeout(promise, ms, label) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms} ms`)), ms);
      promise.then((v) => { clearTimeout(timer); resolve(v); })
             .catch((e) => { clearTimeout(timer); reject(e); });
    });
  }

  function triggerDownload(filename, text) {
    const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function getTxtFilename(filename) {
    const name = String(filename || "output.kof").trim() || "output.kof";
    return /\.kof$/i.test(name) ? name.replace(/\.kof$/i, ".txt") : `${name}.txt`;
  }

  function getUploadTargetFile() {
    return state.lastResult?.file || state.selectedFile || null;
  }

  function getUploadTargetFolderId() {
    return getUploadTargetFile()?.parentId || null;
  }

  function canOpenProjectUpload() {
    return !!(state.project && getUploadTargetFile());
  }

  function getUploadPanelSummary() {
    const targetFile = getUploadTargetFile();
    const folderId = getUploadTargetFolderId();
    const projectName = state.project?.name || state.project?.id || "";
    const sourceName = targetFile?.name || null;
    const suggestedName = state.lastDownloadName || null;

    return {
      folderId,
      projectName,
      sourceName,
      suggestedName,
      locationText: folderId
        ? `Samme mappe som ${sourceName || "valgt fil"}`
        : "Prosjektets rotmappe"
    };
  }

  function resolveTokenWaiters(token) {
    const waiters = [...state.tokenWaiters];
    state.tokenWaiters = [];
    for (const resolve of waiters) { try { resolve(token); } catch {} }
  }

  function waitForToken(ms = CONFIG.TOKEN_WAIT_MS) {
    if (state.accessToken) return Promise.resolve(state.accessToken);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        state.tokenWaiters = state.tokenWaiters.filter((fn) => fn !== wrappedResolve);
        reject(new Error(`Ventet for lenge på access token (${ms} ms)`));
      }, ms);
      function wrappedResolve(token) { clearTimeout(timer); resolve(token); }
      state.tokenWaiters.push(wrappedResolve);
    });
  }

  function buildUi() {
    const app = document.getElementById("app");
    if (!app) throw new Error("Fant ikke #app i index.html");
    app.innerHTML = "";

    const titleCard = el("div", "card");
    titleCard.appendChild(el("div", "card-header", [
      el("h2", null, CONFIG.APP_TITLE)
    ]));
    titleCard.appendChild(el("div", "subtitle", "Konverter .kof-filer til tekstformat"));

    const projectCard = el("div", "card");
    projectCard.appendChild(el("div", "label", "Prosjekt"));
    const projectValue = el("div", "project-value", "Venter på tilkobling...");
    projectCard.appendChild(projectValue);

    const filesCard = el("div", "card");

    const filesHeader = el("div", "card-header", [
      el("div", null, [
        el("div", "label", "KOF-filer")
      ])
    ]);
    const fileCount = el("div", "file-count", "");
    filesHeader.appendChild(fileCount);
    filesCard.appendChild(filesHeader);

    const btnRow = el("div", "btn-row");
    const refreshBtn = el("button", null, "Oppdater liste");
    const convertSelectedBtn = el("button", "primary", "Konverter valgt");
    const convertAllBtn = el("button", null, "Konverter alle");
    const localUploadBtn = el("button", null, "Last opp lokal fil");
    const projectUploadBtn = el("button", null, "Last opp til prosjekt");
    const localFileInput = document.createElement("input");
    localFileInput.type = "file";
    localFileInput.accept = ".kof,text/plain";
    localFileInput.style.display = "none";
    convertSelectedBtn.disabled = true;
    convertAllBtn.disabled = true;
    projectUploadBtn.disabled = true;
    btnRow.appendChild(refreshBtn);
    btnRow.appendChild(convertSelectedBtn);
    btnRow.appendChild(convertAllBtn);
    btnRow.appendChild(localUploadBtn);
    btnRow.appendChild(projectUploadBtn);
    btnRow.appendChild(localFileInput);
    filesCard.appendChild(btnRow);

    const fileList = el("div", "file-list");
    fileList.id = "fileList";
    filesCard.appendChild(fileList);

    const explorerCard = el("div", "card embed-card");
    explorerCard.style.display = "none";
    const explorerHeader = el("div", "card-header", [
      el("div", null, [
        el("div", "label", "Last opp til prosjekt"),
        el("div", "subtitle", "Trimble Connects egen opplastingsvisning, åpnet i riktig prosjektmappe")
      ])
    ]);
    const closeExplorerBtn = el("button", null, "Lukk");
    explorerHeader.appendChild(closeExplorerBtn);
    explorerCard.appendChild(explorerHeader);
    const explorerTarget = el("div", "embed-meta", "");
    explorerCard.appendChild(explorerTarget);
    const explorerFrame = document.createElement("iframe");
    explorerFrame.className = "explorer-frame";
    explorerFrame.title = "Trimble Connect File Explorer";
    explorerFrame.hidden = true;
    explorerCard.appendChild(explorerFrame);

    const statusCard = el("div", "card");
    const status = el("div", "status", "Starter...");
    status.id = "statusBox";
    statusCard.appendChild(status);

    const hint = el("div", "hint");
    hint.style.display = "none";
    hint.id = "hintBox";
    statusCard.appendChild(hint);

    const debugDetails = el("details", "debug");
    const debugSummary = el("summary", null, "Vis tekniske detaljer");
    debugDetails.appendChild(debugSummary);
    const debugOutput = el("pre", null, "");
    debugDetails.appendChild(debugOutput);

    app.appendChild(titleCard);
    app.appendChild(projectCard);
    app.appendChild(filesCard);
    app.appendChild(explorerCard);
    app.appendChild(statusCard);
    app.appendChild(debugDetails);

    ui = {
      projectValue,
      fileCount,
      refreshBtn,
      convertSelectedBtn,
      convertAllBtn,
      localUploadBtn,
      projectUploadBtn,
      localFileInput,
      fileList,
      explorerCard,
      closeExplorerBtn,
      explorerTarget,
      explorerFrame,
      status,
      hint,
      debugOutput
    };
  }

  function el(tag, className, content) {
    const e = document.createElement(tag);
    if (className) e.className = className;
    if (content != null) {
      if (typeof content === "string") e.textContent = content;
      else if (Array.isArray(content)) content.forEach((c) => e.appendChild(c));
      else e.appendChild(content);
    }
    return e;
  }

  function renderFileList() {
    if (!ui.fileList) return;
    ui.fileList.innerHTML = "";
    if (ui.projectUploadBtn) ui.projectUploadBtn.disabled = state.busy || !canOpenProjectUpload();

    ui.fileCount.textContent = state.fileList.length
      ? `${state.fileList.length} fil${state.fileList.length === 1 ? "" : "er"}`
      : "";

    if (!state.fileList.length) {
      const empty = el("div", "empty-state", "Trykk \"Oppdater liste\" for å hente KOF-filer fra prosjektet.");
      ui.fileList.appendChild(empty);
      return;
    }

    for (const file of state.fileList) {
      const isSelected = state.selectedFile?.id === file.id;
      const row = el("label", `file-item${isSelected ? " selected" : ""}`);

      const radio = document.createElement("input");
      radio.type = "radio";
      radio.name = "kofFile";
      radio.value = file.id;
      radio.checked = isSelected;
      radio.addEventListener("change", () => {
        state.selectedFile = file;
        renderFileList();
        setBusy(state.busy);
      });

      const info = el("div", "file-info");
      info.appendChild(el("div", "file-name", file.name || "(uten navn)"));
      if (file.path) info.appendChild(el("div", "file-meta", file.path));

      row.appendChild(radio);
      row.appendChild(info);
      ui.fileList.appendChild(row);
    }
  }

  function showHint(message, show = true) {
    if (!ui.hint) return;
    if (!show || !message) {
      ui.hint.style.display = "none";
      ui.hint.innerHTML = "";
      return;
    }
    ui.hint.style.display = "block";
    ui.hint.innerHTML = `<span class="hint-icon">💡</span>${message}`;
  }

  async function connectWorkspace() {
    setStatus("Kobler til Trimble Connect...");
    if (!window.TrimbleConnectWorkspace?.connect) {
      throw new Error("TrimbleConnectWorkspace ikke funnet.");
    }
    const api = await TrimbleConnectWorkspace.connect(
      window.parent, onWorkspaceEvent, CONFIG.CONNECT_TIMEOUT_MS
    );
    state.api = api;
    state.isEmbedded = window.parent && window.parent !== window;
    debug("API keys:", Object.keys(api || {}));
    return api;
  }

  async function ensureMenu() {
    if (!state.api?.ui?.setMenu) return false;
    try {
      await state.api.ui.setMenu({
        title: CONFIG.APP_TITLE,
        icon: `${window.location.origin}/icon.png`,
        command: CONFIG.MENU_MAIN_COMMAND
      });
      await state.api.ui.setActiveMenuItem(CONFIG.MENU_MAIN_COMMAND).catch(() => {});
      return true;
    } catch (err) {
      debug("setMenu feilet:", err);
      return false;
    }
  }

  async function requestAccessToken() {
    if (state.accessToken) return state.accessToken;
    setStatus("Ber om tilgang...", "working");

    if (!state.api?.extension?.requestPermission) {
      throw new Error("extension.requestPermission mangler.");
    }
    const result = await state.api.extension.requestPermission("accesstoken");
    debug("requestPermission svar:", result);

    if (typeof result === "string" && result && result !== "pending" && result !== "denied") {
      state.accessToken = result;
      resolveTokenWaiters(result);
      return result;
    }
    if (result === "denied") throw new Error("Tilgang avslått.");
    if (result === "pending" || !result) {
      const token = await waitForToken(CONFIG.TOKEN_WAIT_MS);
      state.accessToken = token;
      return token;
    }
    throw new Error(`Uventet svar: ${String(result)}`);
  }

  async function getProject() {
    if (state.project) return state.project;
    setStatus("Henter prosjektinfo...", "working");

    const getProjectFn = state.api?.project?.getCurrentProject || state.api?.project?.getProject;
    if (!getProjectFn) throw new Error("Fant ingen getProject-metode.");

    const project = await getProjectFn.call(state.api.project);
    if (!project?.id) throw new Error("Fant ikke aktivt prosjekt.");

    state.project = project;
    debug("Project:", project);

    if (ui.projectValue) {
      const regionLabel = project.location === "europe" ? "Europa" :
                          project.location === "asia" ? "Asia" :
                          project.location === "northAmerica" ? "Nord-Amerika" :
                          project.location || "ukjent";
      ui.projectValue.innerHTML = `${escapeHtml(project.name || "-")} <span class="badge">${escapeHtml(regionLabel)}</span>`;
    }
    setBusy(state.busy);
    return project;
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    }[c]));
  }

  async function ensureReady() {
    if (!state.api) throw new Error("Ikke koblet til Workspace API.");
    if (!state.accessToken) await requestAccessToken();
    if (!state.project) await getProject();
  }

  function showExplorerPanel(show) {
    state.explorerVisible = !!show;
    if (!ui.explorerCard) return;
    ui.explorerCard.style.display = show ? "block" : "none";
    if (ui.explorerFrame) ui.explorerFrame.hidden = !show;
  }

  async function waitForFrameLoad(frame) {
    if (!frame) throw new Error("Fant ikke explorer-iframe.");
    if (frame.contentWindow && frame.dataset.loaded === "true") return;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Explorer iframe lastet ikke i tide.")), 30000);
      frame.addEventListener("load", () => {
        frame.dataset.loaded = "true";
        clearTimeout(timer);
        resolve();
      }, { once: true });
    });
  }

  async function ensureExplorerApi() {
    if (state.explorerApi) return state.explorerApi;
    if (!ui.explorerFrame) throw new Error("Explorer-iframe mangler i UI.");
    if (!window.TrimbleConnectWorkspace?.getConnectEmbedUrl) {
      throw new Error("getConnectEmbedUrl er ikke tilgjengelig.");
    }

    if (!ui.explorerFrame.src) {
      ui.explorerFrame.src = TrimbleConnectWorkspace.getConnectEmbedUrl();
    }

    await waitForFrameLoad(ui.explorerFrame);

    state.explorerApi = await TrimbleConnectWorkspace.connect(
      ui.explorerFrame,
      async (event) => {
        if (event === "extension.sessionInvalid") {
          const token = await requestAccessToken().catch(() => null);
          if (token && state.explorerApi?.embed?.setTokens) {
            state.explorerApi.embed.setTokens({ accessToken: token }).catch(() => {});
          }
        }
      },
      CONFIG.CONNECT_TIMEOUT_MS
    );

    return state.explorerApi;
  }

  async function openProjectUploadExplorer() {
    try {
      setBusy(true);
      showHint(null, false);
      await ensureReady();

      const folderId = getUploadTargetFolderId();
      const targetFile = getUploadTargetFile();
      const explorerApi = await ensureExplorerApi();
      const summary = getUploadPanelSummary();

      await explorerApi.embed.setTokens({ accessToken: state.accessToken });
      await explorerApi.embed.initFileExplorer({
        projectId: state.project.id,
        folderId: summary.folderId || undefined,
        enableUploadFiles: true,
        enableAdd: true,
        enableCreateFolder: false,
        enableExplorerKebabMenu: false,
        enableExplorerAllProjects: false,
        enableSelect: true
      });

      if (ui.explorerTarget) {
        const suggestedText = summary.suggestedName
          ? `Last opp <strong>${escapeHtml(summary.suggestedName)}</strong> via <strong>Legg til</strong>.`
          : `Bruk <strong>Legg til</strong> for å laste opp den konverterte TXT-filen.`;
        ui.explorerTarget.innerHTML = `${escapeHtml(summary.locationText)} <span class="badge">${escapeHtml(summary.projectName)}</span><br>${suggestedText}`;
      }

      showExplorerPanel(true);
      setStatus("Prosjektmappen for opplasting er åpnet", "success");
      setDebug({
        action: "openProjectUploadExplorer",
        projectId: state.project.id,
        folderId: folderId || null,
        targetFile
      });
    } catch (err) {
      console.error(err);
      showExplorerPanel(false);
      setStatus(`Feil: ${err?.message || String(err)}`, "error");
      setDebug({ error: err?.message || String(err), stack: err?.stack, action: "openProjectUploadExplorer" });
    } finally {
      setBusy(false);
    }
  }

  async function callProxy(action, payload) {
    const res = await withTimeout(
      fetch(CONFIG.PROXY_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action, ...payload })
      }),
      60000, `Proxy ${action}`
    );
    const text = await res.text();
    const json = safeJsonParse(text);
    return { ok: res.ok, status: res.status, text, json };
  }

  function convertKofToTxt(kofText) {
    const points = parseKofPoints(kofText);
    if (!points.length) {
      return [
        "Punktnavn,Nord,Øst,Høyde",
        "# Fant ingen punkter i KOF-fila",
        "# Første 1000 tegn:",
        ...String(kofText || "").slice(0, 1000).split(/\r?\n/)
      ].join("\n");
    }
    const lines = ["Punktnavn,Nord,Øst,Høyde"];
    for (const p of points) {
      lines.push([
        csvEscape(p.name || ""),
        formatNumberForTxt(p.north),
        formatNumberForTxt(p.east),
        formatNumberForTxt(p.height)
      ].join(","));
    }
    return lines.join("\n");
  }

  function parseKofPoints(kofText) {
    const text = String(kofText || "");
    const lines = text.split(/\r?\n/);
    const points = [];
    let current = {};

    for (const rawLine of lines) {
      const line = rawLine.trim();
      if (!line) continue;

      if (/^OBJ/i.test(line) || /^PUNKT/i.test(line) || /^POINT/i.test(line) || /^BEGIN/i.test(line)) {
        if (isCompletePoint(current)) points.push(normalizePoint(current));
        current = {};
        continue;
      }
      if (/^END/i.test(line) || /^SLUTT/i.test(line)) {
        if (isCompletePoint(current)) points.push(normalizePoint(current));
        current = {};
        continue;
      }

      const kv = line.match(/^([^=:]+)\s*[:=]\s*(.+)$/);
      if (kv) {
        const key = normalizeKey(kv[1]);
        const value = kv[2].trim();
        if (!current.name && isNameKey(key)) current.name = cleanValue(value);
        if (current.north == null && isNorthKey(key)) current.north = parseNumber(value);
        if (current.east == null && isEastKey(key)) current.east = parseNumber(value);
        if (current.height == null && isHeightKey(key)) current.height = parseNumber(value);
        continue;
      }

      const free = tryParseFreePointLine(line);
      if (free) {
        if (isCompletePoint(current)) points.push(normalizePoint(current));
        current = free;
      }
    }
    if (isCompletePoint(current)) points.push(normalizePoint(current));
    return dedupePoints(points);
  }

  function tryParseFreePointLine(line) {
    const s = String(line || "").trim();

    let m = s.match(/^05\s+([^\s]+)\s+([^\s]+)\s+(-?\d+(?:[.,]\d+)?)\s+(-?\d+(?:[.,]\d+)?)\s+(-?\d+(?:[.,]\d+)?)\s*$/);
    if (m) return { name: m[1], north: parseNumber(m[3]), east: parseNumber(m[4]), height: parseNumber(m[5]) };

    m = s.match(/^05\s+([^\s]+)\s+(-?\d+(?:[.,]\d+)?)\s+(-?\d+(?:[.,]\d+)?)\s+(-?\d+(?:[.,]\d+)?)\s*$/);
    if (m) return { name: m[1], north: parseNumber(m[2]), east: parseNumber(m[3]), height: parseNumber(m[4]) };

    m = s.match(/^05\s+([^\s]+)\s+(-?\d+(?:[.,]\d+)?)\s+(-?\d+(?:[.,]\d+)?)\s*$/);
    if (m) return { name: m[1], north: parseNumber(m[2]), east: parseNumber(m[3]), height: null };

    m = s.match(/^([^\s,;]+)[\s,;]+(-?\d+(?:[.,]\d+)?)[\s,;]+(-?\d+(?:[.,]\d+)?)[\s,;]+(-?\d+(?:[.,]\d+)?)\s*$/);
    if (m) return { name: m[1], north: parseNumber(m[2]), east: parseNumber(m[3]), height: parseNumber(m[4]) };

    return null;
  }

  function isCompletePoint(p) { return !!p && p.name && p.north != null && p.east != null; }
  function normalizePoint(p) {
    return {
      name: String(p.name || "").trim(),
      north: p.north != null ? Number(p.north) : null,
      east: p.east != null ? Number(p.east) : null,
      height: p.height != null ? Number(p.height) : null
    };
  }

  function dedupePoints(points) {
    const seen = new Set();
    const out = [];
    for (const p of points) {
      const key = `${p.name}|${p.north}|${p.east}|${p.height}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(p);
    }
    return out;
  }

  function normalizeKey(key) {
    return String(key || "").trim().toLowerCase()
      .replace(/[æøå]/g, (c) => ({ "æ": "ae", "ø": "o", "å": "a" }[c]))
      .replace(/[^a-z0-9]/g, "");
  }

  function cleanValue(value) { return String(value || "").trim().replace(/^"|"$/g, ""); }

  function parseNumber(value) {
    if (value == null) return null;
    const s = String(value).trim().replace(/\s+/g, "").replace(",", ".");
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }

  function formatNumberForTxt(n) {
    if (n == null || !Number.isFinite(n)) return "";
    return String(n);
  }

  function csvEscape(value) {
    const s = String(value ?? "");
    if (/[",;\n]/.test(s)) return `"${s.replace(/"/g, "\"\"")}"`;
    return s;
  }

  function isNameKey(key) {
    return ["punktnavn", "punktnummer", "punktnr", "punktid", "punkt", "navn", "name", "id", "label"].includes(key);
  }

  function isNorthKey(key) { return ["n", "nord", "north", "northing", "y"].includes(key); }
  function isEastKey(key) { return ["e", "ost", "east", "easting", "x"].includes(key); }
  function isHeightKey(key) { return ["h", "z", "hoyde", "height", "elev", "elevation", "kote"].includes(key); }

  async function refreshKofList() {
    try {
      setBusy(true);
      showHint(null, false);
      await ensureReady();
      setStatus("Henter KOF-filer fra prosjektet...", "working");

      const proxyRes = await callProxy("listProjectKofFiles", {
        token: state.accessToken,
        projectId: state.project.id,
        projectLocation: state.project.location
      });

      if (!proxyRes.ok || !proxyRes.json) {
        setStatus(`Feil: Proxy svarte med HTTP ${proxyRes.status}`, "error");
        setDebug({ step: "listProxyHttp", status: proxyRes.status, preview: shortText(proxyRes.text, 1500) });
        return;
      }

      const result = proxyRes.json;
      if (!result.ok) {
        setStatus("Kunne ikke hente filliste", "error");
        setDebug(result);
        return;
      }

      state.fileList = Array.isArray(result.files) ? result.files : [];
      if (!state.fileList.length) {
        state.selectedFile = null;
      } else if (!state.selectedFile || !state.fileList.some((f) => f.id === state.selectedFile.id)) {
        state.selectedFile = state.fileList[0];
      }

      renderFileList();

      if (state.fileList.length === 0) {
        setStatus("Ingen KOF-filer funnet i prosjektet", "neutral");
      } else {
        setStatus(`Fant ${state.fileList.length} KOF-fil${state.fileList.length === 1 ? "" : "er"}`, "success");
      }

      setDebug({
        action: "listProjectKofFiles",
        fileCount: state.fileList.length,
        candidatesTried: result.candidatesTried,
        source: result.source
      });
    } catch (err) {
      console.error(err);
      setStatus(`Feil: ${err?.message || String(err)}`, "error");
      setDebug({ error: err?.message || String(err), stack: err?.stack });
    } finally {
      setBusy(false);
    }
  }

  async function refreshKofListOnOpen(reason = "open") {
    const now = Date.now();
    if (state.busy) return;
    if (now - state.lastAutoRefreshAt < 1500) return;
    state.lastAutoRefreshAt = now;

    debug("Auto-refreshing KOF list", { reason });
    await refreshKofList();
  }

  async function downloadAndConvertFile(file) {
    const proxyRes = await callProxy("downloadKofFile", {
      token: state.accessToken,
      projectId: state.project.id,
      projectLocation: state.project.location,
      fileId: file.id,
      fileName: file.name
    });

    if (!proxyRes.ok || !proxyRes.json) throw new Error(`Proxy svarte med HTTP ${proxyRes.status}`);
    const result = proxyRes.json;
    if (!result.ok) throw new Error(result.error || result.step || "Kunne ikke laste ned KOF-fil");

    const txt = convertKofToTxt(result.text || "");
    const outName = getTxtFilename(result.file?.name || file.name || "output.kof");
    return { outName, txt, result };
  }

  async function processSelectedFile() {
    try {
      setBusy(true);
      showHint(null, false);
      await ensureReady();

      const file = state.selectedFile;
      if (!file?.id) {
        setStatus("Velg en fil fra listen først", "error");
        return;
      }

      setStatus(`Konverterer ${file.name}...`, "working");
      const converted = await downloadAndConvertFile(file);
      state.lastResult = converted.result;
      state.lastDownloadName = converted.outName;

      triggerDownload(converted.outName, converted.txt);
      setStatus(`Ferdig: ${converted.outName} er lastet ned lokalt`, "success");
      showHint(`Filen er lagret lokalt på maskinen din. Bruk <strong>Last opp til prosjekt</strong> for å åpne riktig mappe og laste opp <strong>${escapeHtml(converted.outName)}</strong>.`);

      setDebug({
        action: "processSelectedFile",
        sourceFile: converted.result.file,
        convertedFile: { name: converted.outName, size: converted.txt.length },
        preview: shortText(converted.result.text || "", 300)
      });
    } catch (err) {
      console.error(err);
      setStatus(`Feil: ${err?.message || String(err)}`, "error");
      setDebug({ error: err?.message || String(err), stack: err?.stack });
    } finally {
      setBusy(false);
    }
  }

  async function processAllFiles() {
    try {
      setBusy(true);
      showHint(null, false);
      await ensureReady();

      if (!state.fileList.length) {
        setStatus("Ingen filer i listen - trykk Oppdater liste først", "error");
        return;
      }

      const summary = [];
      let count = 0;

      for (const file of state.fileList) {
        count += 1;
        setStatus(`Konverterer ${count}/${state.fileList.length}: ${file.name}...`, "working");

        try {
          const converted = await downloadAndConvertFile(file);
          triggerDownload(converted.outName, converted.txt);
          summary.push({ ok: true, file: file.name, outName: converted.outName });
        } catch (err) {
          summary.push({ ok: false, file: file.name, error: err?.message || String(err) });
        }
      }

      const okCount = summary.filter((x) => x.ok).length;
      const failCount = summary.length - okCount;
      state.lastDownloadName = okCount === 1 ? summary.find((x) => x.ok)?.outName || null : null;

      if (failCount === 0) {
        setStatus(`Ferdig! ${okCount} fil${okCount === 1 ? "" : "er"} konvertert og lastet ned`, "success");
        showHint(
          okCount === 1
            ? `Filen er lagret lokalt. Bruk <strong>Last opp til prosjekt</strong> for å åpne riktig mappe og laste opp <strong>${escapeHtml(state.lastDownloadName || "den konverterte filen")}</strong>.`
            : `Filene er lagret lokalt. Bruk <strong>Last opp til prosjekt</strong> for å åpne prosjektmappen og laste opp dem du vil sende tilbake.`
        );
      } else {
        setStatus(`Fullført med ${failCount} feil (${okCount} OK, ${failCount} feilet)`, "error");
      }

      setDebug({ action: "convertAll", total: summary.length, okCount, failCount, files: summary });
    } catch (err) {
      console.error(err);
      setStatus(`Feil: ${err?.message || String(err)}`, "error");
      setDebug({ error: err?.message || String(err), stack: err?.stack });
    } finally {
      setBusy(false);
    }
  }

  async function processLocalFile(file) {
    try {
      setBusy(true);
      showHint(null, false);

      if (!file) return;

      setStatus(`Konverterer lokal fil ${file.name}...`, "working");
      const kofText = await file.text();
      const txt = convertKofToTxt(kofText || "");
      const outName = getTxtFilename(file.name || "output.kof");
      state.lastDownloadName = outName;

      triggerDownload(outName, txt);
      setStatus(`Ferdig: ${outName} er lastet ned lokalt`, "success");
      showHint(`Lokal fil er konvertert direkte fra maskinen din. Resultatet <strong>${escapeHtml(outName)}</strong> er lastet ned lokalt.`);

      setDebug({
        action: "processLocalFile",
        sourceFile: { name: file.name, size: file.size, type: file.type },
        convertedFile: { name: outName, size: txt.length },
        preview: shortText(kofText || "", 300)
      });
    } catch (err) {
      console.error(err);
      setStatus(`Feil: ${err?.message || String(err)}`, "error");
      setDebug({ error: err?.message || String(err), stack: err?.stack });
    } finally {
      if (ui.localFileInput) ui.localFileInput.value = "";
      setBusy(false);
    }
  }

  function onWorkspaceEvent(event, args) {
    debug("[TC EVENT]", event, args);
    if (event === "extension.accessToken") {
      const token = args?.data;
      if (typeof token === "string" && token && token !== "pending" && token !== "denied") {
        state.accessToken = token;
        resolveTokenWaiters(token);
      }
      return;
    }
    if (event === "extension.command") {
      const command = args?.data || null;
      if (command === CONFIG.MENU_MAIN_COMMAND || command === CONFIG.MENU_OPEN_COMMAND) {
        setStatus(`${CONFIG.APP_TITLE} åpnet fra meny`, "neutral");
        refreshKofListOnOpen("menu").catch(() => {});
      }
      return;
    }
  }

  function wireUi() {
    ui.refreshBtn.addEventListener("click", refreshKofList);
    ui.convertSelectedBtn.addEventListener("click", processSelectedFile);
    ui.convertAllBtn.addEventListener("click", processAllFiles);
    ui.localUploadBtn.addEventListener("click", () => ui.localFileInput.click());
    ui.localFileInput.addEventListener("change", (event) => processLocalFile(event.target.files?.[0]));
    ui.projectUploadBtn.addEventListener("click", openProjectUploadExplorer);
    ui.closeExplorerBtn.addEventListener("click", () => showExplorerPanel(false));
  }

  async function init() {
    try {
      buildUi();
      wireUi();
      renderFileList();

      setStatus("Starter...", "working");
      await connectWorkspace();
      await ensureMenu();

      setStatus("Klar - laster liste automatisk...", "working");
      setTimeout(() => {
        refreshKofListOnOpen("init").catch(() => {});
      }, 0);

      window.kof2txt = {
        state,
        refreshKofList,
        refreshKofListOnOpen,
        processSelectedFile,
        processAllFiles,
        processLocalFile,
        openProjectUploadExplorer,
        inspectApi() {
          if (!state.api) return "Ikke koblet";
          const r = {};
          for (const k of Object.keys(state.api)) {
            const sub = state.api[k];
            r[k] = sub && typeof sub === "object" ? Object.keys(sub) : typeof sub;
          }
          return r;
        }
      };
    } catch (err) {
      console.error(err);
      setStatus(`Feil: ${err?.message || String(err)}`, "error");
      setDebug({ error: err?.message || String(err), stack: err?.stack });
    }
  }

  window.addEventListener("load", init);
})();
