(() => {
  "use strict";

  const CONFIG = {
    DEBUG: false,
    CONNECT_TIMEOUT_MS: 30000,
    TOKEN_WAIT_MS: 30000,
    PROXY_URL: "/.netlify/functions/tc-proxy",
    MENU_MAIN_COMMAND: "KOF2TXT_MAIN",
    MENU_OPEN_COMMAND: "KOF2TXT_OPEN"
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
    busy: false
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

  // ─── UI Konstruksjon ────────────────────────────────────────────────────
  function buildUi() {
    const app = document.getElementById("app");
    if (!app) throw new Error("Fant ikke #app i index.html");
    app.innerHTML = "";

    const titleCard = el("div", "card");
    titleCard.appendChild(el("div", "card-header", [
      el("h2", null, "KOF2TXT")
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
    convertSelectedBtn.disabled = true;
    convertAllBtn.disabled = true;
    btnRow.appendChild(refreshBtn);
    btnRow.appendChild(convertSelectedBtn);
    btnRow.appendChild(convertAllBtn);
    filesCard.appendChild(btnRow);

    const fileList = el("div", "file-list");
    fileList.id = "fileList";
    filesCard.appendChild(fileList);

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
    app.appendChild(statusCard);
    app.appendChild(debugDetails);

    ui = {
      projectValue,
      fileCount,
      refreshBtn,
      convertSelectedBtn,
      convertAllBtn,
      fileList,
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

  // ─── Workspace API ──────────────────────────────────────────────────────
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
        title: "KOF2TXT",
        icon: `${window.location.origin}/icon.png`,
        command: CONFIG.MENU_MAIN_COMMAND,
        subMenus: [{ title: "Konverter KOF", command: CONFIG.MENU_OPEN_COMMAND }]
      });
      await state.api.ui.setActiveMenuItem(CONFIG.MENU_OPEN_COMMAND).catch(() => {});
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

  // ─── KOF-parser ─────────────────────────────────────────────────────────
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

    // KOF 05-variant: 05 Punktnavn Kode Nord Øst Høyde
    // Eksempel: 05 p1 4051 6694974.582 297294.438 36.131
    // Her ignorerer vi kodefeltet, fordi TXT-formatet bare skal ha punktnavn,nord,øst,høyde.
    let m = s.match(/^05\s+([^\s]+)\s+([^\s]+)\s+(-?\d+(?:[.,]\d+)?)\s+(-?\d+(?:[.,]\d+)?)\s+(-?\d+(?:[.,]\d+)?)\s*$/);
    if (m) return { name: m[1], north: parseNumber(m[3]), east: parseNumber(m[4]), height: parseNumber(m[5]) };

    // KOF 05-variant: 05 Punktnavn Nord Øst Høyde
    // Eksempel: 05 11 6643083.229 592220.933 10.000
    m = s.match(/^05\s+([^\s]+)\s+(-?\d+(?:[.,]\d+)?)\s+(-?\d+(?:[.,]\d+)?)\s+(-?\d+(?:[.,]\d+)?)\s*$/);
    if (m) return { name: m[1], north: parseNumber(m[2]), east: parseNumber(m[3]), height: parseNumber(m[4]) };

    // KOF 05-variant: 05 Punktnavn Nord Øst, uten høyde
    // Eksempel: 05 G1 6627190.530 620539.580
    m = s.match(/^05\s+([^\s]+)\s+(-?\d+(?:[.,]\d+)?)\s+(-?\d+(?:[.,]\d+)?)\s*$/);
    if (m) return { name: m[1], north: parseNumber(m[2]), east: parseNumber(m[3]), height: null };

    // Generisk variant uten 05: Punktnavn Nord Øst Høyde
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
    if (/[",;\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  }

  function isNameKey(key) {
    return ["punktnavn", "punktnummer", "punktnr", "punktid", "punkt", "navn", "name", "id", "label"].includes(key);
  }

  function isNorthKey(key) { return ["n", "nord", "north", "northing", "y"].includes(key); }
  function isEastKey(key) { return ["e", "ost", "east", "easting", "x"].includes(key); }
  function isHeightKey(key) { return ["h", "z", "hoyde", "height", "elev", "elevation", "kote"].includes(key); }

  // ─── Hovedfunksjoner ────────────────────────────────────────────────────
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
    const outName = String(result.file?.name || file.name || "output.kof").replace(/\.kof$/i, ".txt");
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

      triggerDownload(converted.outName, converted.txt);
      setStatus(`Ferdig: ${converted.outName} er lastet ned lokalt`, "success");
      showHint(`Filen er lagret lokalt på maskinen din. Dra og slipp <strong>${escapeHtml(converted.outName)}</strong> tilbake inn i Trimble Connect-mappen for å laste den opp.`);

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
        setStatus("Ingen filer i listen — trykk Oppdater liste først", "error");
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

      if (failCount === 0) {
        setStatus(`Ferdig! ${okCount} fil${okCount === 1 ? "" : "er"} konvertert og lastet ned`, "success");
        showHint(`Alle filer er lagret lokalt. Dra og slipp dem tilbake inn i Trimble Connect-mappen for å laste dem opp.`);
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

  // ─── Event Handlers ─────────────────────────────────────────────────────
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
      if (command === CONFIG.MENU_OPEN_COMMAND) {
        setStatus("KOF2TXT åpnet fra meny", "neutral");
      }
      return;
    }
  }

  function wireUi() {
    ui.refreshBtn.addEventListener("click", refreshKofList);
    ui.convertSelectedBtn.addEventListener("click", processSelectedFile);
    ui.convertAllBtn.addEventListener("click", processAllFiles);
  }

  async function init() {
    try {
      buildUi();
      wireUi();
      renderFileList();

      setStatus("Starter...", "working");
      await connectWorkspace();
      await ensureMenu();

      setStatus("Klar — trykk \"Oppdater liste\" for å se KOF-filer", "neutral");

      window.kof2txt = {
        state,
        refreshKofList,
        processSelectedFile,
        processAllFiles,
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

