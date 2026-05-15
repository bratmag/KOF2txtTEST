(() => {
  "use strict";

  const CONFIG = {
    DEBUG: false,
    CONNECT_TIMEOUT_MS: 30000,
    TOKEN_WAIT_MS: 30000,
    PROXY_URL: "/.netlify/functions/tc-proxy",
    APP_TITLE: "Geomatikksenter",
    APP_BUILD: "20260515-jxl-nn2000-ecef",
    JXL_ECEF_NN2000_GEOID_OFFSET_M: 40.3703,
    AUTO_CONVERT_ON_OPEN: true,
    IFC_POINT_OBJECT_HEIGHT_M: 1,
    IFC_FALLBACK_LINE_RADIUS_M: 0.05,
    IFC_REFERENCE_LINE_RADIUS_M: 0.015,
    IFC_REFERENCE_POINT_SIZE_M: 0.08,
    MENU_MAIN_COMMAND: "KOF2TXT_TEST_MAIN",
    MENU_OPEN_COMMAND: "KOF2TXT_TEST_OPEN"
  };

  const state = {
    api: null,
    accessToken: null,
    project: null,
    selectedFile: null,
    fileList: [],
    activeView: "converter",
    jxlSources: [],
    selectedJxlSource: null,
    tokenWaiters: [],
    isEmbedded: false,
    lastResult: null,
    busy: false,
    conversionInProgress: false,
    cancelConversionRequested: false,
    manualSelectionMode: false,
    manualSelectedFileIds: new Set(),
    explorerApi: null,
    explorerVisible: false,
    lastDownloadName: null,
    lastAutoRefreshAt: 0,
    autoConvertInProgress: false,
    lastUploadResult: null
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
    if (ui.refreshBtn) {
      ui.refreshBtn.textContent = state.manualSelectionMode ? "Oppdater liste" : "Oppdater og konverter";
      ui.refreshBtn.disabled = busy;
    }
    if (ui.stopBtn) {
      ui.stopBtn.style.display = state.conversionInProgress ? "" : "none";
      ui.stopBtn.disabled = !state.conversionInProgress || state.cancelConversionRequested;
    }
    if (ui.convertManualBtn) {
      ui.convertManualBtn.style.display = state.manualSelectionMode ? "" : "none";
      ui.convertManualBtn.disabled = busy || state.manualSelectedFileIds.size === 0;
    }
    if (ui.localUploadBtn) ui.localUploadBtn.disabled = busy;
    if (ui.jxlRefreshBtn) ui.jxlRefreshBtn.disabled = busy;
    if (ui.jxlConvertBtn) ui.jxlConvertBtn.disabled = busy || !state.selectedJxlSource;
    if (ui.projectUploadBtn) ui.projectUploadBtn.disabled = busy || !canOpenProjectUpload();
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
    const mimeType = /\.xml$/i.test(String(filename || ""))
      ? "application/xml;charset=utf-8"
      : /\.ifc$/i.test(String(filename || ""))
        ? "application/x-step;charset=utf-8"
        : "text/plain;charset=utf-8";
    const blob = new Blob([text], { type: mimeType });
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

  function getXmlFilename(filename) {
    const name = String(filename || "output.kof").trim() || "output.kof";
    return /\.(kof|sos|sosi|gml|jxl)$/i.test(name) ? name.replace(/\.(kof|sos|sosi|gml|jxl)$/i, ".xml") : `${name}.xml`;
  }

  function getIfcFilename(filename) {
    const name = String(filename || "output.gml").trim() || "output.gml";
    return /\.(gml|jxl)$/i.test(name) ? name.replace(/\.(gml|jxl)$/i, ".ifc") : `${name}.ifc`;
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
    titleCard.appendChild(el("div", "subtitle", "KOF/SOSI/GML konverteres automatisk. Avanserte verktøy ligger ett klikk unna."));

    const tabsCard = el("div", "card tabs-card");
    const converterTabBtn = el("button", "primary", "KOF-konvertering");
    const jxlTabBtn = el("button", null, "Avanserte verktøy");
    tabsCard.appendChild(el("div", "btn-row", [converterTabBtn, jxlTabBtn]));

    const projectCard = el("div", "card");
    projectCard.appendChild(el("div", "label", "Prosjekt"));
    const projectValue = el("div", "project-value", "Venter på tilkobling...");
    projectCard.appendChild(projectValue);

    const filesCard = el("div", "card");
    const filesHeader = el("div", "card-header", [
      el("div", null, [
        el("div", "label", "KOF/SOSI/GML-filer")
      ])
    ]);
    const fileCount = el("div", "file-count", "");
    filesHeader.appendChild(fileCount);
    filesCard.appendChild(filesHeader);

    const btnRow = el("div", "btn-row");
    const refreshBtn = el("button", "primary", "Oppdater og konverter");
    const stopBtn = el("button", "danger", "Stopp konvertering");
    const convertManualBtn = el("button", "primary", "Konverter valgte");
    const localUploadBtn = el("button", null, "Konverter lokal fil");
    const projectUploadBtn = el("button", null, "Trimble Connect datautforsker");
    const localFileInput = document.createElement("input");
    localFileInput.type = "file";
    localFileInput.accept = ".kof,.sos,.sosi,.gml,.jxl,text/plain,application/gml+xml,application/xml";
    localFileInput.style.display = "none";
    stopBtn.style.display = "none";
    convertManualBtn.style.display = "none";
    projectUploadBtn.disabled = true;
    btnRow.appendChild(refreshBtn);
    btnRow.appendChild(stopBtn);
    btnRow.appendChild(convertManualBtn);
    btnRow.appendChild(localUploadBtn);
    btnRow.appendChild(localFileInput);
    filesCard.appendChild(btnRow);

    const fileList = el("div", "file-list");
    fileList.id = "fileList";
    filesCard.appendChild(fileList);

    const jxlCard = el("div", "card");
    jxlCard.style.display = "none";
    const jxlHeader = el("div", "card-header", [
      el("div", null, [
        el("div", "label", "Avanserte verktøy"),
        el("div", "subtitle", "Manuell JXL til IFC fra Connect Explorer og Field Data.")
      ])
    ]);
    const jxlCount = el("div", "file-count", "");
    jxlHeader.appendChild(jxlCount);
    jxlCard.appendChild(jxlHeader);
    const jxlBtnRow = el("div", "btn-row");
    const jxlRefreshBtn = el("button", "primary", "Oppdater JXL-liste");
    const jxlConvertBtn = el("button", null, "Konverter valgt JXL");
    jxlConvertBtn.disabled = true;
    jxlBtnRow.appendChild(jxlRefreshBtn);
    jxlBtnRow.appendChild(jxlConvertBtn);
    jxlBtnRow.appendChild(projectUploadBtn);
    jxlCard.appendChild(jxlBtnRow);
    const jxlList = el("div", "file-list");
    jxlCard.appendChild(jxlList);

    const explorerCard = el("div", "card embed-card");
    explorerCard.style.display = "none";
    const explorerHeader = el("div", "card-header", [
      el("div", null, [
        el("div", "label", "Trimble Connect datautforsker"),
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

    const statusCard = el("div", "card status-card");
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
    app.appendChild(tabsCard);
    app.appendChild(projectCard);
    app.appendChild(statusCard);
    app.appendChild(filesCard);
    app.appendChild(jxlCard);
    app.appendChild(explorerCard);
    app.appendChild(debugDetails);

    ui = {
      projectValue,
      converterTabBtn,
      jxlTabBtn,
      fileCount,
      refreshBtn,
      stopBtn,
      convertManualBtn,
      localUploadBtn,
      projectUploadBtn,
      localFileInput,
      fileList,
      jxlCard,
      jxlCount,
      jxlRefreshBtn,
      jxlConvertBtn,
      jxlList,
      filesCard,
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
      const empty = el("div", "empty-state", "Trykk \"Oppdater liste\" for å hente KOF/SOSI/GML-filer fra prosjektet.");
      ui.fileList.appendChild(empty);
      return;
    }

    for (const file of state.fileList) {
      const isSelected = state.selectedFile?.id === file.id;
      const isManualSelected = state.manualSelectedFileIds.has(file.id);
      const conversionState = getFileConversionState(file);
      const row = el("label", `file-item ${conversionState.className}${isSelected || isManualSelected ? " selected" : ""}`);

      const selector = document.createElement("input");
      selector.type = state.manualSelectionMode ? "checkbox" : "radio";
      selector.name = state.manualSelectionMode ? "manualKofFile" : "kofFile";
      selector.value = file.id;
      selector.checked = state.manualSelectionMode ? isManualSelected : isSelected;
      selector.addEventListener("change", () => {
        if (state.manualSelectionMode) {
          if (selector.checked) state.manualSelectedFileIds.add(file.id);
          else state.manualSelectedFileIds.delete(file.id);
        } else {
          state.selectedFile = file;
        }
        renderFileList();
        setBusy(state.busy);
      });

      const info = el("div", "file-info");
      info.appendChild(el("div", "file-name", file.name || "(uten navn)"));
      if (file.path) info.appendChild(el("div", "file-meta", file.path));

      const statusBadge = el("span", `file-status ${conversionState.className}`, conversionState.label);

      row.appendChild(selector);
      row.appendChild(info);
      row.appendChild(statusBadge);
      ui.fileList.appendChild(row);
    }
  }

  function switchView(view) {
    state.activeView = view === "jxl" ? "jxl" : "converter";
    if (ui.filesCard) ui.filesCard.style.display = state.activeView === "converter" ? "" : "none";
    if (ui.jxlCard) ui.jxlCard.style.display = state.activeView === "jxl" ? "" : "none";
    if (ui.converterTabBtn) ui.converterTabBtn.className = state.activeView === "converter" ? "primary" : "";
    if (ui.jxlTabBtn) ui.jxlTabBtn.className = state.activeView === "jxl" ? "primary" : "";
    setBusy(state.busy);
  }

  function renderJxlList() {
    if (!ui.jxlList) return;
    ui.jxlList.innerHTML = "";
    ui.jxlCount.textContent = state.jxlSources.length
      ? `${state.jxlSources.length} JXL-kilde${state.jxlSources.length === 1 ? "" : "r"}`
      : "";

    if (!state.jxlSources.length) {
      ui.jxlList.appendChild(el("div", "empty-state", "Trykk \"Oppdater JXL-liste\" for å hente JXL fra Connect Explorer og Field Data."));
      setBusy(state.busy);
      return;
    }

    for (const source of state.jxlSources) {
      const isSelected = state.selectedJxlSource?.id === source.id && state.selectedJxlSource?.sourceType === source.sourceType;
      const row = el("label", `file-item pending${isSelected ? " selected" : ""}`);
      const radio = document.createElement("input");
      radio.type = "radio";
      radio.name = "jxlSource";
      radio.value = `${source.sourceType}:${source.id}`;
      radio.checked = isSelected;
      radio.addEventListener("change", () => {
        state.selectedJxlSource = source;
        renderJxlList();
      });

      const info = el("div", "file-info");
      info.appendChild(el("div", "file-name", source.name || "(uten navn)"));
      const meta = [
        source.sourceType === "field-data" ? "Field Data" : "Connect Explorer",
        source.path || "",
        source.modifiedOn || ""
      ].filter(Boolean).join(" · ");
      if (meta) info.appendChild(el("div", "file-meta", meta));
      const badge = el("span", "file-status pending", source.sourceType === "field-data" ? "Field Data" : "Fil");

      row.appendChild(radio);
      row.appendChild(info);
      row.appendChild(badge);
      ui.jxlList.appendChild(row);
    }
    setBusy(state.busy);
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

  function formatIfcStats(stats) {
    if (!stats || typeof stats !== "object") return "";
    const parts = [
      ["ror", stats.pipes],
      ["referanselinjer", stats.annot_lines ?? stats.referenceGeometry],
      ["solider", stats.solids],
      ["punkt", stats.points],
      ["kurver", stats.curves],
      ["IFC-entiteter", stats.entities]
    ].filter(([, value]) => Number.isFinite(Number(value)));
    return parts.map(([label, value]) => `${Number(value)} ${label}`).join(", ");
  }

  function buildConversionHint(converted, uploadResult) {
    const statsText = converted?.format === "ifc" ? formatIfcStats(converted.stats) : "";
    const statsSuffix = statsText ? `<br><strong>IFC-oppsummering:</strong> ${escapeHtml(statsText)}.` : "";
    if (uploadResult?.ok) {
      return `Den konverterte filen ble automatisk lastet opp tilbake til samme prosjektmappe i Trimble Connect.${statsSuffix}`;
    }
    return `Automatisk opplasting kom ikke helt i mal. Bruk <strong>Last opp til prosjekt</strong> for a apne riktig mappe og laste opp <strong>${escapeHtml(converted?.outName || "den konverterte filen")}</strong>.${statsSuffix}`;
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
          : `Bruk <strong>Legg til</strong> for å laste opp den konverterte filen.`;
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

  async function uploadConvertedTxtToProject({ sourceFile, outName, txt }) {
    const parentId = sourceFile?.parentId || null;
    if (!parentId) {
      return {
        ok: false,
        skipped: true,
        error: "Fant ikke prosjektmappe for automatisk opplasting."
      };
    }

    const proxyRes = await callProxy("uploadConvertedTxt", {
      token: state.accessToken,
      projectId: state.project.id,
      projectLocation: state.project.location,
      parentId,
      fileName: outName,
      text: txt
    });

    if (!proxyRes.ok || !proxyRes.json) {
      return {
        ok: false,
        error: `Proxy svarte med HTTP ${proxyRes.status}`,
        httpStatus: proxyRes.status,
        preview: shortText(proxyRes.text, 400)
      };
    }

    return proxyRes.json;
  }

  function convertKofToTxt(kofText) {
    const points = parseKofPoints(kofText);
    if (!points.length) {
      return [
        "Punktnavn,Nord,Øst,Høyde,Kode",
        "# Fant ingen punkter i KOF-fila",
        "# Første 1000 tegn:",
        ...String(kofText || "").slice(0, 1000).split(/\r?\n/)
      ].join("\n");
    }
    const lines = ["Punktnavn,Nord,Øst,Høyde,Kode"];
    for (const p of points) {
      lines.push([
        csvEscape(p.name || ""),
        formatNumberForTxt(p.north),
        formatNumberForTxt(p.east),
        formatNumberForTxt(p.height),
        csvEscape(p.code || "")
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
    const parsed = parseKof05Record(line);
    if (parsed) {
      return {
        name: parsed.rawName,
        north: parsed.n,
        east: parsed.e,
        height: parsed.h,
        code: parsed.code
      };
    }

    const s = String(line || "").trim();
    const m = s.match(/^([^\s,;]+)[\s,;]+(-?\d+(?:[.,]\d+)?)[\s,;]+(-?\d+(?:[.,]\d+)?)[\s,;]+(-?\d+(?:[.,]\d+)?)\s*$/);
    if (m) return { name: m[1], north: parseNumber(m[2]), east: parseNumber(m[3]), height: parseNumber(m[4]) };

    return null;
  }

  function isCompletePoint(p) { return !!p && p.name && p.north != null && p.east != null; }

  function normalizePoint(p) {
    return {
      name: String(p.name || "").trim(),
      north: p.north != null ? Number(p.north) : null,
      east: p.east != null ? Number(p.east) : null,
      height: p.height != null ? Number(p.height) : null,
      code: p.code != null ? String(p.code).trim() : ""
    };
  }

  function dedupePoints(points) {
    const seen = new Set();
    const out = [];
    for (const p of points) {
      const key = `${p.name}|${p.north}|${p.east}|${p.height}|${p.code || ""}`;
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

  function shouldConvertToLandXml(kofText) {
    return /^\s*09(?:_|\s+)91\b/im.test(String(kofText || ""));
  }

  function detectSourceFileType(sourceText, fileName) {
    const name = String(fileName || "");
    const text = String(sourceText || "");

    if (/\.jxl$/i.test(name)) return "jxl";
    if (/<(?:\w+:)?(JOBFile|PointRecord|LivePolylineRecord)\b/i.test(text)) return "jxl";
    if (/\.gml$/i.test(name)) return "gml";
    if (/<(?:\w+:)?FeatureCollection\b/i.test(text) || /<(?:\w+:)?(LineString|Point|Polygon)\b/i.test(text)) return "gml";
    if (/\.(sos|sosi)$/i.test(name)) return "sosi";
    if (/^\s*\.(HODE|PUNKT|KURVE)\b/im.test(text)) return "sosi";
    return "kof";
  }

  function convertKofFile(kofText, fileName, options = {}) {
    const sourceText = String(kofText || "");
    const sourceType = detectSourceFileType(sourceText, fileName);

    if (sourceType === "sosi") {
      return {
        format: "xml",
        outName: getXmlFilename(fileName),
        text: sosiToLandXml(sourceText, { fileName })
      };
    }

    if (sourceType === "gml") {
      try {
        const ifc = gmlToIfc(sourceText, {
          fileName,
          projectName: state.project?.name || state.project?.id || "Prosjekt",
          coordSys: "EPSG:5972"
        });
        return {
          format: "ifc",
          outName: getIfcFilename(fileName),
          text: ifc.text,
          stats: ifc.stats
        };
      } catch (err) {
        console.warn("GML til IFC feilet, bruker LandXML fallback:", err);
        return {
          format: "xml",
          outName: getXmlFilename(fileName),
          text: gmlToLandXml(sourceText, { fileName }),
          fallbackFrom: "ifc",
          fallbackError: err?.message || String(err)
        };
      }
    }

    if (sourceType === "jxl") {
      const ifc = jxlToIfc(sourceText, {
        fileName,
        projectName: state.project?.name || state.project?.id || "Prosjekt",
        coordSys: "EPSG:5972"
      });
      if ((ifc.stats?.geom || 0) === 0 && /<[^>]*LivePolylineRecord\b/i.test(sourceText)) {
        throw new Error(`JXL ble lest, men ingen IFC-geometri ble laget. Parserdiagnostikk: ${JSON.stringify(jxlTextDiagnostics(sourceText))}`);
      }
      return {
        format: "ifc",
        outName: getIfcFilename(fileName),
        text: ifc.text,
        stats: ifc.stats
      };
    }

    if (shouldConvertToLandXml(sourceText)) {
      return {
        format: "xml",
        outName: getXmlFilename(fileName),
        text: kofToLandXml(sourceText, { fileName })
      };
    }

    return {
      format: "txt",
      outName: getTxtFilename(fileName),
      text: convertKofToTxt(sourceText)
    };
  }

  function parseKof05Record(line) {
    const body = String(line || "").trim().replace(/^05\s+/, "");
    if (!body) return null;

    const tokens = body.split(/\s+/).filter(Boolean);
    if (tokens.length < 3) return null;

    let coordStart = -1;
    let h = null;

    if (tokens.length >= 4 && isLikelyCoordinate(tokens[tokens.length - 3])) {
      coordStart = tokens.length - 3;
      h = parseNumber(tokens[tokens.length - 1]);
    } else if (isLikelyCoordinate(tokens[tokens.length - 2])) {
      coordStart = tokens.length - 2;
    }

    if (coordStart < 1) return null;

    const descriptorTokens = tokens.slice(0, coordStart);
    const codeCandidate = descriptorTokens[descriptorTokens.length - 1] || "";
    const hasCode = descriptorTokens.length > 1 && /^\d{3,}$/.test(codeCandidate);
    const rawName = (hasCode ? descriptorTokens.slice(0, -1) : descriptorTokens).join(" ");

    return {
      rawName: String(rawName || "").trim(),
      code: hasCode ? codeCandidate : "",
      n: parseNumber(tokens[coordStart]),
      e: parseNumber(tokens[coordStart + 1]),
      h
    };
  }

  function isLikelyCoordinate(value) {
    const n = parseNumber(value);
    return n != null && Math.abs(n) >= 10000;
  }

  function kofToLandXml(kofText, options = {}) {
    return buildLandXmlDocument(parseKofForLandXml(kofText), {
      fileName: options.fileName,
      fallbackName: "KOF",
      layerPrefix: "Kof",
      author: "kof2xml"
    });
  }

  function sosiToLandXml(sosiText, options = {}) {
    return buildLandXmlDocument(parseSosiForLandXml(sosiText), {
      fileName: options.fileName,
      fallbackName: "SOSI",
      layerPrefix: "Sos",
      author: "sosi2xml",
      useLineCodeLayers: true,
      planFeatureNamePrefix: "Plan feature",
      lineColor: "0,0,255"
    });
  }

  function gmlToLandXml(gmlText, options = {}) {
    return buildLandXmlDocument(parseGmlForLandXml(gmlText), {
      fileName: options.fileName,
      fallbackName: "GML",
      layerPrefix: "Gml",
      author: "gml2xml",
      useLineCodeLayers: true,
      planFeatureNamePrefix: "Plan feature",
      lineColor: "0,0,255"
    });
  }

  function gmlToIfc(gmlText, options = {}) {
    const objects = parseGmlForIfc(gmlText);
    const outName = getIfcFilename(options.fileName || "output.gml");
    const result = buildIfc(objects, {
      version: options.version || "IFC4",
      projectName: options.projectName || "Prosjekt",
      coordSys: options.coordSys || "EPSG:5972",
      solidMode: options.solidMode !== false,
      extrusionHeight: Number.isFinite(options.extrusionHeight) ? options.extrusionHeight : 3,
      outputFile: outName
    });
    return result;
  }

  function jxlToIfc(jxlText, options = {}) {
    const objects = parseJxlForIfc(jxlText);
    const outName = getIfcFilename(options.fileName || "output.jxl");
    return buildIfc(objects, {
      version: options.version || "IFC4",
      projectName: options.projectName || "Prosjekt",
      coordSys: options.coordSys || "EPSG:5972",
      solidMode: options.solidMode !== false,
      extrusionHeight: Number.isFinite(options.extrusionHeight) ? options.extrusionHeight : 3,
      outputFile: outName,
      sourceLabel: "JXL",
      propertySetName: "JXL_Properties"
    });
  }

  const IFC_PIPE_TYPES = new Set([
    "DR", "SP", "OV", "AF", "VL", "TL", "LL",
    "LETREUKAB", "LETREUVANN", "LETREUDREN",
    "KAB", "EL", "TEL",
    "LEKU", "OVS", "SPP", "SPS", "LESPUNT", "LESTIKKB",
    "LETRA", "LETRE", "LETREMKAB", "VLU", "VLP", "VLSPR", "LEVAR"
  ]);

  const IFC_ANNOTATION_LINE_TYPES = new Set([
    "FET"
  ]);

  const IFC_POINT_PRODUCT_TYPES = {
    ANB: "IFCPIPEFITTING",
    BFD: "IFCTANK",
    DIV: "IFCDISTRIBUTIONCHAMBERELEMENT",
    FORAKONSTR: "IFCBUILDINGELEMENTPROXY",
    GRN: "IFCPIPEFITTING",
    GUT: "IFCDISTRIBUTIONCHAMBERELEMENT",
    HYD: "IFCFIRESUPPRESSIONTERMINAL",
    INB: "IFCDISTRIBUTIONCHAMBERELEMENT",
    INR: "IFCDISTRIBUTIONCHAMBERELEMENT",
    INT: "IFCDISTRIBUTIONCHAMBERELEMENT",
    KONSTROMRIS: "IFCBUILDINGELEMENTPROXY",
    KOTREKUM: "IFCDISTRIBUTIONCHAMBERELEMENT",
    KRN: "IFCVALVE",
    KUM: "IFCDISTRIBUTIONCHAMBERELEMENT",
    LOK: "IFCCOVERING",
    OVL: "IFCDISTRIBUTIONCHAMBERELEMENT",
    PMK: "IFCDISTRIBUTIONCHAMBERELEMENT",
    RED: "IFCDISTRIBUTIONCHAMBERELEMENT",
    SAN: "IFCDISTRIBUTIONCHAMBERELEMENT",
    SANI: "IFCDISTRIBUTIONCHAMBERELEMENT",
    SEP: "IFCTANK",
    SLG: "IFCDISTRIBUTIONCHAMBERELEMENT",
    SLI: "IFCDISTRIBUTIONCHAMBERELEMENT",
    SLS: "IFCDISTRIBUTIONCHAMBERELEMENT",
    SLU: "IFCDISTRIBUTIONCHAMBERELEMENT",
    SPR: "IFCFIRESUPPRESSIONTERMINAL",
    TNK: "IFCTANK",
    TOKSTVL: "IFCPUMP",
    TOP: "IFCANNOTATION",
    UTS: "IFCDISTRIBUTIONCHAMBERELEMENT",
    VPK: "IFCVALVE"
  };

  function buildIfc(objects, options = {}) {
    const schema = options.version === "IFC2X3" || options.version === "IFC4X3" ? options.version : "IFC4";
    const sourceLabel = options.sourceLabel || "GML";
    const propertySetName = options.propertySetName || `${sourceLabel}_Properties`;
    const timestamp = new Date().toISOString().slice(0, 19);
    const lines = [];
    let counter = 1;
    const elementRefs = [];

    const addRaw = (line) => lines.push(line);
    const addEntity = (body) => {
      const id = counter;
      counter += 1;
      lines.push(`#${id}=${body}`);
      return id;
    };

    addRaw("ISO-10303-21;");
    addRaw("HEADER;");
    addRaw("FILE_DESCRIPTION(('ViewDefinition [CoordinationView]'),'2;1');");
    addRaw(`FILE_NAME('${ifcString(fileBaseName(options.outputFile || "output.ifc"))}','${timestamp}',('${ifcString(sourceLabel)} til IFC Konverter'),(''),'','','');`);
    addRaw(`FILE_SCHEMA(('${schema}'));`);
    addRaw("ENDSEC;");
    addRaw("DATA;");

    const org = addEntity(`IFCORGANIZATION($,'${ifcString(sourceLabel)}-IFC Converter',$,$,$);`);
    const app = addEntity(`IFCAPPLICATION(#${org},'1.0','${ifcString(sourceLabel)} to IFC Converter','${ifcString(sourceLabel)}2IFC');`);
    const person = addEntity("IFCPERSON($,'Bruker',$,$,$,$,$,$);");
    const pao = addEntity(`IFCPERSONANDORGANIZATION(#${person},#${org},$);`);
    const owner = addEntity(`IFCOWNERHISTORY(#${pao},#${app},$,.ADDED.,$,$,$,0);`);
    const lengthUnit = addEntity("IFCSIUNIT(*,.LENGTHUNIT.,$,.METRE.);");
    const areaUnit = addEntity("IFCSIUNIT(*,.AREAUNIT.,$,.SQUARE_METRE.);");
    const volumeUnit = addEntity("IFCSIUNIT(*,.VOLUMEUNIT.,$,.CUBIC_METRE.);");
    const angleUnit = addEntity("IFCSIUNIT(*,.PLANEANGLEUNIT.,$,.RADIAN.);");
    const units = addEntity(`IFCUNITASSIGNMENT((#${lengthUnit},#${areaUnit},#${volumeUnit},#${angleUnit}));`);
    const origin = addEntity("IFCCARTESIANPOINT((0.,0.,0.));");
    const zAxis = addEntity("IFCDIRECTION((0.,0.,1.));");
    const xAxis = addEntity("IFCDIRECTION((1.,0.,0.));");
    const axis3d = addEntity(`IFCAXIS2PLACEMENT3D(#${origin},#${zAxis},#${xAxis});`);
    const extrusionDir = addEntity("IFCDIRECTION((0.,0.,1.));");
    const context = addEntity(`IFCGEOMETRICREPRESENTATIONCONTEXT($,'Model',3,1.E-05,#${axis3d},$);`);
    const project = addEntity(`IFCPROJECT('${ifcGuid()}',#${owner},'${ifcString(options.projectName || "Prosjekt")}',$,$,$,$,(#${context}),#${units});`);
    const site = addEntity(`IFCSITE('${ifcGuid()}',#${owner},'${ifcString(sourceLabel)} Site','CRS: ${ifcString(options.coordSys || "EPSG:5972")}',$,#${axis3d},$,.ELEMENT.,$,$,$,$,$);`);
    const building = addEntity(`IFCBUILDING('${ifcGuid()}',#${owner},'${ifcString(sourceLabel)} Building',$,$,#${axis3d},$,$,.ELEMENT.,$,$,$);`);
    const storey = addEntity(`IFCBUILDINGSTOREY('${ifcGuid()}',#${owner},'Storey 0',$,$,#${axis3d},$,$,.ELEMENT.,0.);`);
    addEntity(`IFCRELAGGREGATES('${ifcGuid()}',#${owner},$,$,#${project},(#${site}));`);
    addEntity(`IFCRELAGGREGATES('${ifcGuid()}',#${owner},$,$,#${site},(#${building}));`);
    addEntity(`IFCRELAGGREGATES('${ifcGuid()}',#${owner},$,$,#${building},(#${storey}));`);

    const stats = { points: 0, pointObjects: 0, curves: 0, solids: 0, pipes: 0, referenceGeometry: 0, geom: 0, entities: 0 };

    for (const [index, object] of (Array.isArray(objects) ? objects : []).entries()) {
      const guid = ifcGuid();
      const name = ifcString(object.props?.name || object.props?.Name || object.id || `Objekt_${index + 1}`).slice(0, 255);
      const desc = ifcString(object.props?.description || object.props?.class || object.type || "").slice(0, 255);
      const coords = (object.coords || []).slice(0, 200);
      const placement = addEntity(`IFCLOCALPLACEMENT($,#${axis3d});`);
      let shapeRef = "$";
      let element;
      let addReferenceAnnotation = false;

      if (object.geom === "point" && coords.length) {
        const pointDims = getIfcPointObjectDims(object.props || {}, { defaultHeightM: object.source === "jxl" ? 2 : CONFIG.IFC_POINT_OBJECT_HEIGHT_M });
        const productType = getIfcPointProductType(object);
        if (pointDims && productType !== "IFCANNOTATION") {
          element = buildIfcPointObject(addEntity, context, placement, owner, guid, name, desc, coords[0], pointDims, productType, zAxis, xAxis, extrusionDir);
          stats.pointObjects += 1;
          stats.solids += 1;
          addReferenceAnnotation = true;
        } else {
          element = buildIfcPointAnnotation(addEntity, context, owner, guid, name, desc, placement, coords[0]);
        }
        stats.points += 1;
        stats.geom += 1;
      } else if (object.geom === "curve" && isIfcAnnotationLineCandidate(object)) {
        element = buildIfcCurveAnnotation(addEntity, context, owner, guid, name, desc, placement, coords);
        stats.curves += 1;
        stats.geom += 1;
      } else if (object.geom === "curve" && isIfcPipeCandidate(object)) {
        const dims = getIfcPipeDims(object.props || {}, { defaultShape: object.source === "jxl" ? "circle" : "rect" });
        if (dims) {
          const offset = getIfcZOffsetToCenter(object.props || {}, dims);
          const adjusted = coords.map((coord) => [coord[0], coord[1], coord[2] + offset]);
          if (dims.shape === "circle") {
            const pointRefs = adjusted.map((coord) => `#${addEntity(`IFCCARTESIANPOINT((${formatIfcNumber(coord[0])},${formatIfcNumber(coord[1])},${formatIfcNumber(coord[2])}));`)}`);
            const polyline = addEntity(`IFCPOLYLINE((${pointRefs.join(",")}));`);
            const solid = addEntity(`IFCSWEPTDISKSOLID(#${polyline},${formatIfcNumber(dims.odM / 2)},${formatIfcNumber(dims.idM / 2)},$,$);`);
            const rep = addEntity(`IFCSHAPEREPRESENTATION(#${context},'Body','SweptSolid',(#${solid}));`);
            const shape = addEntity(`IFCPRODUCTDEFINITIONSHAPE($,$,(#${rep}));`);
            shapeRef = `#${shape}`;
            element = addEntity(`IFCPIPESEGMENT('${guid}',#${owner},'${name}','${desc}',$,#${placement},${shapeRef},$,.NOTDEFINED.);`);
          } else {
            const solidRefs = buildIfcRectangularPipeSolids(addEntity, adjusted, dims);
            if (solidRefs.length) {
              const rep = addEntity(`IFCSHAPEREPRESENTATION(#${context},'Body','SweptSolid',(${solidRefs.join(",")}));`);
              const shape = addEntity(`IFCPRODUCTDEFINITIONSHAPE($,$,(#${rep}));`);
              shapeRef = `#${shape}`;
            }
            element = addEntity(`IFCCABLESEGMENT('${guid}',#${owner},'${name}','${desc}',$,#${placement},${shapeRef},$,.NOTDEFINED.);`);
          }
          stats.pipes += 1;
          stats.solids += 1;
          stats.geom += 1;
          addReferenceAnnotation = true;
        } else {
          element = buildIfcCurveAnnotation(addEntity, context, owner, guid, name, desc, placement, coords);
          stats.curves += 1;
          stats.geom += 1;
        }
      } else if (object.geom === "curve" && coords.length >= 2) {
        element = buildIfcCurveFallbackSolid(addEntity, context, placement, owner, guid, name, desc, coords, CONFIG.IFC_FALLBACK_LINE_RADIUS_M);
        stats.solids += 1;
        stats.geom += 1;
        addReferenceAnnotation = true;
      } else if (object.geom === "polygon" && coords.length >= 4) {
        if (options.solidMode !== false) {
          element = buildIfcPolygonSolid(addEntity, context, placement, owner, guid, name, desc, coords, options.extrusionHeight || 3, zAxis, xAxis, extrusionDir);
          stats.solids += 1;
          stats.geom += 1;
          addReferenceAnnotation = true;
        } else {
          element = buildIfcCurveAnnotation(addEntity, context, owner, guid, name, desc, placement, coords);
          stats.curves += 1;
          stats.geom += 1;
        }
      } else {
        element = addEntity(`IFCBUILDINGELEMENTPROXY('${guid}',#${owner},'${name}','${desc}',$,#${placement},$,$,.ELEMENT.);`);
      }

      elementRefs.push(`#${element}`);
      addIfcProperties(addEntity, owner, element, object.props || {}, propertySetName);
      if (addReferenceAnnotation) {
        const refPlacement = addEntity(`IFCLOCALPLACEMENT($,#${axis3d});`);
        const refName = ifcString(`${object.props?.name || object.props?.Name || object.id || `Objekt_${index + 1}`} referanse`).slice(0, 255);
        const refElement = buildIfcReferenceGeometry(addEntity, context, refPlacement, owner, ifcGuid(), refName, `Original ${sourceLabel}-geometri`, object.geom, coords, zAxis, xAxis, extrusionDir);
        elementRefs.push(`#${refElement}`);
        stats.referenceGeometry += 1;
      }
    }

    if (elementRefs.length) {
      addEntity(`IFCRELCONTAINEDINSPATIALSTRUCTURE('${ifcGuid()}',#${owner},'Innhold',$,(${elementRefs.join(",")}),#${storey});`);
    }

    addRaw("ENDSEC;");
    addRaw("END-ISO-10303-21;");
    stats.entities = counter - 1;
    stats.annot_lines = stats.referenceGeometry;
    return { text: lines.join("\n"), stats };
  }

  function buildIfcPointAnnotation(addEntity, context, owner, guid, name, desc, placement, coord) {
    const point = addEntity(`IFCCARTESIANPOINT((${formatIfcNumber(coord[0])},${formatIfcNumber(coord[1])},${formatIfcNumber(coord[2])}));`);
    const gset = addEntity(`IFCGEOMETRICSET((#${point}));`);
    const rep = addEntity(`IFCSHAPEREPRESENTATION(#${context},'Annotation','GeometricSet',(#${gset}));`);
    const shape = addEntity(`IFCPRODUCTDEFINITIONSHAPE($,$,(#${rep}));`);
    return addEntity(`IFCANNOTATION('${guid}',#${owner},'${name}','${desc}','SurveyPoint',#${placement},#${shape});`);
  }

  function buildIfcReferenceGeometry(addEntity, context, placement, owner, guid, name, desc, geom, coords, zAxis, xAxis, extrusionDir) {
    if (geom === "point" && coords.length) {
      return buildIfcPointAnnotation(addEntity, context, owner, guid, name, desc, placement, coords[0]);
    }
    if ((geom === "curve" || geom === "polygon") && coords.length >= 2) {
      return buildIfcCurveAnnotation(addEntity, context, owner, guid, name, desc, placement, coords);
    }
    return addEntity(`IFCANNOTATION('${guid}',#${owner},'${name}','${desc}','Original survey data',#${placement},$);`);
  }

  function buildIfcReferencePointSolid(addEntity, context, placement, owner, guid, name, desc, coord, zAxis, xAxis, extrusionDir) {
    const half = CONFIG.IFC_REFERENCE_POINT_SIZE_M / 2;
    const profile = addEntity(`IFCRECTANGLEPROFILEDEF(.AREA.,$,$,${formatIfcNumber(CONFIG.IFC_REFERENCE_POINT_SIZE_M)},${formatIfcNumber(CONFIG.IFC_REFERENCE_POINT_SIZE_M)});`);
    const basePoint = addEntity(`IFCCARTESIANPOINT((${formatIfcNumber(coord[0])},${formatIfcNumber(coord[1])},${formatIfcNumber(coord[2] - half)}));`);
    const solidAxis = addEntity(`IFCAXIS2PLACEMENT3D(#${basePoint},#${zAxis},#${xAxis});`);
    const solid = addEntity(`IFCEXTRUDEDAREASOLID(#${profile},#${solidAxis},#${extrusionDir},${formatIfcNumber(CONFIG.IFC_REFERENCE_POINT_SIZE_M)});`);
    const rep = addEntity(`IFCSHAPEREPRESENTATION(#${context},'Body','SweptSolid',(#${solid}));`);
    const shape = addEntity(`IFCPRODUCTDEFINITIONSHAPE($,$,(#${rep}));`);
    return addEntity(`IFCBUILDINGELEMENTPROXY('${guid}',#${owner},'${name}','${desc}',$,#${placement},#${shape},$,.ELEMENT.);`);
  }

  function buildIfcPointObject(addEntity, context, placement, owner, guid, name, desc, coord, dims, productType, zAxis, xAxis, extrusionDir) {
    const profile = buildIfcPointObjectProfile(addEntity, dims);
    const baseZ = getIfcPointObjectBaseZ(dims.props || {}, coord[2], dims.heightM);
    const basePoint = addEntity(`IFCCARTESIANPOINT((${formatIfcNumber(coord[0])},${formatIfcNumber(coord[1])},${formatIfcNumber(baseZ)}));`);
    const solidAxis = addEntity(`IFCAXIS2PLACEMENT3D(#${basePoint},#${zAxis},#${xAxis});`);
    const solid = addEntity(`IFCEXTRUDEDAREASOLID(#${profile},#${solidAxis},#${extrusionDir},${formatIfcNumber(dims.heightM)});`);
    const rep = addEntity(`IFCSHAPEREPRESENTATION(#${context},'Body','SweptSolid',(#${solid}));`);
    const shape = addEntity(`IFCPRODUCTDEFINITIONSHAPE($,$,(#${rep}));`);
    return addIfcProductElement(addEntity, productType, guid, owner, name, desc, placement, `#${shape}`);
  }

  function buildIfcPointObjectProfile(addEntity, dims) {
    if (dims.shape === "circle") {
      if (dims.thkM > 0) {
        return addEntity(`IFCCIRCLEHOLLOWPROFILEDEF(.AREA.,$,$,${formatIfcNumber(dims.outerDiameterM / 2)},${formatIfcNumber(dims.thkM)});`);
      }
      return addEntity(`IFCCIRCLEPROFILEDEF(.AREA.,$,$,${formatIfcNumber(dims.outerDiameterM / 2)});`);
    }
    if (dims.thkM > 0) {
      return addEntity(`IFCRECTANGLEHOLLOWPROFILEDEF(.AREA.,$,$,${formatIfcNumber(dims.outerWidthM)},${formatIfcNumber(dims.outerLengthM)},${formatIfcNumber(dims.thkM)},$,$);`);
    }
    return addEntity(`IFCRECTANGLEPROFILEDEF(.AREA.,$,$,${formatIfcNumber(dims.outerWidthM)},${formatIfcNumber(dims.outerLengthM)});`);
  }

  function addIfcProductElement(addEntity, productType, guid, owner, name, desc, placement, shapeRef) {
    if (productType === "IFCBUILDINGELEMENTPROXY") {
      return addEntity(`IFCBUILDINGELEMENTPROXY('${guid}',#${owner},'${name}','${desc}',$,#${placement},${shapeRef},$,.ELEMENT.);`);
    }
    if (productType === "IFCANNOTATION") {
      return addEntity(`IFCANNOTATION('${guid}',#${owner},'${name}','${desc}','SurveyPoint',#${placement},${shapeRef});`);
    }
    return addEntity(`${productType}('${guid}',#${owner},'${name}','${desc}',$,#${placement},${shapeRef},$,.NOTDEFINED.);`);
  }

  function buildIfcCurveAnnotation(addEntity, context, owner, guid, name, desc, placement, coords) {
    const pointRefs = coords.map((coord) => `#${addEntity(`IFCCARTESIANPOINT((${formatIfcNumber(coord[0])},${formatIfcNumber(coord[1])},${formatIfcNumber(coord[2])}));`)}`);
    const polyline = addEntity(`IFCPOLYLINE((${pointRefs.join(",")}));`);
    const rep = addEntity(`IFCSHAPEREPRESENTATION(#${context},'Annotation','Curve3D',(#${polyline}));`);
    const shape = addEntity(`IFCPRODUCTDEFINITIONSHAPE($,$,(#${rep}));`);
    return addEntity(`IFCANNOTATION('${guid}',#${owner},'${name}','${desc}','Annotation curve',#${placement},#${shape});`);
  }

  function buildIfcCurveFallbackSolid(addEntity, context, placement, owner, guid, name, desc, coords, radiusM) {
    const pointRefs = coords.map((coord) => `#${addEntity(`IFCCARTESIANPOINT((${formatIfcNumber(coord[0])},${formatIfcNumber(coord[1])},${formatIfcNumber(coord[2])}));`)}`);
    const polyline = addEntity(`IFCPOLYLINE((${pointRefs.join(",")}));`);
    const solid = addEntity(`IFCSWEPTDISKSOLID(#${polyline},${formatIfcNumber(radiusM)},$,$,$);`);
    const rep = addEntity(`IFCSHAPEREPRESENTATION(#${context},'Body','SweptSolid',(#${solid}));`);
    const shape = addEntity(`IFCPRODUCTDEFINITIONSHAPE($,$,(#${rep}));`);
    return addEntity(`IFCBUILDINGELEMENTPROXY('${guid}',#${owner},'${name}','${desc}',$,#${placement},#${shape},$,.ELEMENT.);`);
  }

  function buildIfcPolygonSolid(addEntity, context, placement, owner, guid, name, desc, coords, extrusionHeight, zAxis, xAxis, extrusionDir) {
    const ring = coords.slice(0, -1);
    const pointRefs = ring.map((coord) => `#${addEntity(`IFCCARTESIANPOINT((${formatIfcNumber(coord[0])},${formatIfcNumber(coord[1])}));`)}`);
    pointRefs.push(pointRefs[0]);
    const polyline = addEntity(`IFCPOLYLINE((${pointRefs.join(",")}));`);
    const profile = addEntity(`IFCARBITRARYCLOSEDPROFILEDEF(.AREA.,$,#${polyline});`);
    const baseZ = Math.min(...ring.map((coord) => Number(coord[2]) || 0));
    const basePoint = addEntity(`IFCCARTESIANPOINT((0.,0.,${formatIfcNumber(baseZ)}));`);
    const solidAxis = addEntity(`IFCAXIS2PLACEMENT3D(#${basePoint},#${zAxis},#${xAxis});`);
    const solid = addEntity(`IFCEXTRUDEDAREASOLID(#${profile},#${solidAxis},#${extrusionDir},${formatIfcNumber(extrusionHeight)});`);
    const rep = addEntity(`IFCSHAPEREPRESENTATION(#${context},'Body','SweptSolid',(#${solid}));`);
    const shape = addEntity(`IFCPRODUCTDEFINITIONSHAPE($,$,(#${rep}));`);
    return addEntity(`IFCBUILDINGELEMENTPROXY('${guid}',#${owner},'${name}','${desc}',$,#${placement},#${shape},$,.ELEMENT.);`);
  }

  function buildIfcRectangularPipeSolids(addEntity, coords, dims) {
    const profile = addEntity(`IFCRECTANGLEHOLLOWPROFILEDEF(.AREA.,$,$,${formatIfcNumber(dims.heightM)},${formatIfcNumber(dims.widthM)},${formatIfcNumber(dims.thkM)},$,$);`);
    const solidRefs = [];
    for (let index = 0; index < coords.length - 1; index += 1) {
      const p0 = coords[index];
      const p1 = coords[index + 1];
      const dx = p1[0] - p0[0];
      const dy = p1[1] - p0[1];
      const dz = p1[2] - p0[2];
      const length = Math.sqrt(dx * dx + dy * dy + dz * dz);
      if (length < 1e-9) continue;
      const zx = dx / length;
      const zy = dy / length;
      const zz = dz / length;
      const baseAxis = Math.abs(zz) < 0.9 ? [0, 0, 1] : [1, 0, 0];
      const dot = baseAxis[0] * zx + baseAxis[1] * zy + baseAxis[2] * zz;
      let ex = baseAxis[0] - dot * zx;
      let ey = baseAxis[1] - dot * zy;
      let ez = baseAxis[2] - dot * zz;
      const en = Math.sqrt(ex * ex + ey * ey + ez * ez) || 1;
      ex /= en; ey /= en; ez /= en;
      const segOrigin = addEntity(`IFCCARTESIANPOINT((${formatIfcNumber(p0[0])},${formatIfcNumber(p0[1])},${formatIfcNumber(p0[2])}));`);
      const segZ = addEntity(`IFCDIRECTION((${formatIfcNumber(zx, 8)},${formatIfcNumber(zy, 8)},${formatIfcNumber(zz, 8)}));`);
      const segX = addEntity(`IFCDIRECTION((${formatIfcNumber(ex, 8)},${formatIfcNumber(ey, 8)},${formatIfcNumber(ez, 8)}));`);
      const segAxis = addEntity(`IFCAXIS2PLACEMENT3D(#${segOrigin},#${segZ},#${segX});`);
      const extDir = addEntity("IFCDIRECTION((0.,0.,1.));");
      const solid = addEntity(`IFCEXTRUDEDAREASOLID(#${profile},#${segAxis},#${extDir},${formatIfcNumber(length)});`);
      solidRefs.push(`#${solid}`);
    }
    return solidRefs;
  }

  function addIfcProperties(addEntity, owner, element, props, propertySetName = "GML_Properties") {
    const entries = Object.entries(props || {}).filter(([key]) => key !== "gml:id");
    if (!entries.length) return;
    const propertyRefs = [];
    for (const [key, value] of entries) {
      const property = addEntity(`IFCPROPERTYSINGLEVALUE('${ifcString(key).slice(0, 255)}',$,IFCLABEL('${ifcString(value).slice(0, 255)}'),$);`);
      propertyRefs.push(`#${property}`);
    }
    const pset = addEntity(`IFCPROPERTYSET('${ifcGuid()}',#${owner},'${ifcString(propertySetName).slice(0, 255)}',$,(${propertyRefs.join(",")}));`);
    addEntity(`IFCRELDEFINESBYPROPERTIES('${ifcGuid()}',#${owner},$,$,(#${element}),#${pset});`);
  }

  function getIfcPointProductType(object) {
    const code = normalizeIfcCode(object?.type || getIfcProp(object?.props || {}, ["OBJTYPE", "Type", "Navn"]));
    return IFC_POINT_PRODUCT_TYPES[code] || (getIfcPointObjectDims(object?.props || {}) ? "IFCBUILDINGELEMENTPROXY" : "IFCANNOTATION");
  }

  function getIfcPointObjectDims(props, options = {}) {
    const kumform = normalizeIfcToken(getIfcProp(props, ["Kumform"]));
    const widthM = pointDimensionToMeters(getIfcProp(props, ["Bredde"]));
    const lengthM = pointDimensionToMeters(getIfcProp(props, ["Lengde"]));
    if (!kumform || (widthM == null && lengthM == null)) return null;

    const thkM = pointThicknessToMeters(getIfcProp(props, ["Tykkelse"]));
    const insideOutside = normalizeIfcToken(getIfcProp(props, ["InnvendigUtvendig"]));
    const usesInside = insideOutside.startsWith("ID") || insideOutside.includes("INNVENDIG");
    const shape = kumform.startsWith("R") ? "circle" : "rect";
    const baseWidthM = Math.max(0, widthM ?? lengthM ?? 0);
    const baseLengthM = Math.max(0, lengthM ?? baseWidthM);
    if (baseWidthM <= 0 || baseLengthM <= 0) return null;

    const outerWidthM = usesInside ? baseWidthM + 2 * thkM : baseWidthM;
    const outerLengthM = usesInside ? baseLengthM + 2 * thkM : baseLengthM;
    return {
      shape,
      outerDiameterM: outerWidthM,
      outerWidthM,
      outerLengthM: (kumform === "FK" || kumform === "F") ? outerWidthM : outerLengthM,
      thkM,
      heightM: getIfcPointObjectHeight(props, options),
      props
    };
  }

  function getIfcPointObjectHeight(props, options = {}) {
    const heightM = pointDimensionToMeters(getIfcProp(props, ["Høyde", "Hoyde", "VertikalDimensjon", "Vertikal dimensjon"]));
    if (heightM != null && heightM > 0) return heightM;
    return options.defaultHeightM || CONFIG.IFC_POINT_OBJECT_HEIGHT_M;
  }

  function pointDimensionToMeters(value) {
    const number = firstNumber(value);
    if (number == null || number <= 0) return null;
    return number > 50 ? number / 1000 : number;
  }

  function pointThicknessToMeters(value) {
    const number = firstNumber(value);
    if (number == null || number <= 0) return 0;
    return number > 5 ? number / 100 : number;
  }

  function getIfcPointObjectBaseZ(props, measuredZ, heightM) {
    const reference = normalizeIfcToken(getIfcProp(props, ["Høydereferanse", "Hoydereferanse"]));
    const slabValue = firstNumber(getIfcProp(props, ["Avst_BunnInnvUnderUtv"])) || 0;
    const slabM = slabValue > 5 ? slabValue / 100 : slabValue;
    if (reference.includes("TOPP") || reference.includes("OVERKANT")) return measuredZ - heightM;
    if (reference.includes("SENTER")) return measuredZ - heightM / 2;
    if (reference.includes("BUNN_INNVENDIG")) return measuredZ - slabM;
    return measuredZ;
  }

  function getIfcPipeDims(props, options = {}) {
    const dimMm = firstNumber(getIfcProp(props, ["Dimensjon"]));
    if (dimMm == null || dimMm <= 0) return null;
    const thkMm = firstNumber(getIfcProp(props, ["Tykkelse"])) || 0;
    const verticalDimMm = firstNumber(getIfcProp(props, ["VertikalDimensjon", "Vertikal dimensjon"]));
    const insideOutside = String(getIfcProp(props, ["InnvendigUtvendig"]) || "").toUpperCase();
    const usesInsideDiameter = insideOutside.startsWith("ID") || insideOutside.includes("INNVENDIG");
    const pipeShape = String(getIfcProp(props, ["Rørform", "Rorform"]) || "").toUpperCase();
    const shapeToken = pipeShape.split(/\s+/)[0];
    const shape = shapeToken === "F" || pipeShape.includes("FIRKANT")
      ? "rect"
      : shapeToken === "S" || pipeShape.includes("SIRK")
        ? "circle"
        : options.defaultShape || "rect";
    const innerWidthMm = usesInsideDiameter ? dimMm : Math.max(0, dimMm - 2 * thkMm);
    const outerWidthMm = usesInsideDiameter ? dimMm + 2 * thkMm : dimMm;
    const inputHeightMm = verticalDimMm != null && verticalDimMm > 0 ? verticalDimMm : dimMm;
    const innerHeightMm = usesInsideDiameter ? inputHeightMm : Math.max(0, inputHeightMm - 2 * thkMm);
    const outerHeightMm = usesInsideDiameter ? inputHeightMm + 2 * thkMm : inputHeightMm;
    if (!shape) return null;
    return {
      odM: outerWidthMm / 1000,
      idM: innerWidthMm / 1000,
      widthM: outerWidthMm / 1000,
      heightM: outerHeightMm / 1000,
      innerWidthM: innerWidthMm / 1000,
      innerHeightM: innerHeightMm / 1000,
      thkM: thkMm / 1000,
      shape
    };
  }

  function getIfcZOffsetToCenter(props, dims) {
    const reference = String(getIfcProp(props, ["Høydereferanse", "Hoydereferanse"]) || "").toUpperCase();
    const outerVerticalM = dims.shape === "rect" ? dims.heightM : dims.odM;
    const innerVerticalM = dims.shape === "rect" ? dims.innerHeightM : dims.idM;
    if (reference.includes("BUNN_INNVENDIG") || reference.includes("UNDERKANT_INNVENDIG")) return innerVerticalM / 2;
    if (reference.includes("BUNN_UTVENDIG") || reference.includes("UNDERKANT_UTVENDIG")) return outerVerticalM / 2;
    if (reference.includes("TOPP_INNVENDIG") || reference.includes("OVERKANT_INNVENDIG")) return -innerVerticalM / 2;
    if (reference.includes("TOPP_UTVENDIG") || reference.includes("OVERKANT_UTVENDIG")) return -outerVerticalM / 2;
    if (reference.includes("PÅ_BAKKEN") || reference.includes("PA_BAKKEN")) return outerVerticalM / 2;
    return 0;
  }

  function isIfcAnnotationLineCandidate(object) {
    const code = normalizeIfcCode(object?.type || "");
    return IFC_ANNOTATION_LINE_TYPES.has(code);
  }

  function isIfcPipeCandidate(object) {
    const code = normalizeIfcCode(object?.type || "");
    if (IFC_ANNOTATION_LINE_TYPES.has(code)) return false;
    if (IFC_PIPE_TYPES.has(code)) return true;
    const props = object?.props || {};
    return getIfcProp(props, ["Dimensjon"]) != null &&
      getIfcProp(props, ["Rørform", "Rorform"]) != null;
  }

  function getIfcProp(props, aliases) {
    const normalizedAliases = new Set((Array.isArray(aliases) ? aliases : [aliases]).map(normalizeIfcPropKey));
    for (const [key, value] of Object.entries(props || {})) {
      if (normalizedAliases.has(normalizeIfcPropKey(key))) return value;
    }
    return undefined;
  }

  function normalizeIfcCode(value) {
    return normalizeIfcToken(value).replace(/\s+.*/, "");
  }

  function normalizeIfcToken(value) {
    return repairUtf8Mojibake(String(value || ""))
      .toUpperCase()
      .replace(/Ã†/g, "AE")
      .replace(/Ã˜/g, "O")
      .replace(/Ã…/g, "A")
      .replace(/Æ/g, "AE")
      .replace(/Ø/g, "O")
      .replace(/Å/g, "A")
      .replace(/[?\ufffd]/g, "O")
      .replace(/[^A-Z0-9_]+/g, " ")
      .trim();
  }

  function normalizeIfcPropKey(key) {
    return repairUtf8Mojibake(String(key || ""))
      .toLowerCase()
      .replace(/Ãƒâ€ /g, "ae")
      .replace(/ÃƒËœ/g, "o")
      .replace(/Ãƒâ€¦/g, "a")
      .replace(/Ã†/g, "ae")
      .replace(/Ã˜/g, "o")
      .replace(/Ã…/g, "a")
      .replace(/[Ã¦Ã¸Ã¥]/g, (char) => ({ "Ã¦": "ae", "Ã¸": "o", "Ã¥": "a" }[char]))
      .replace(/[æøå]/g, (char) => ({ "æ": "ae", "ø": "o", "å": "a" }[char]))
      .replace(/[?\ufffd]/g, "o")
      .replace(/[^a-z0-9]/g, "");
  }

  function firstNumber(value) {
    const match = String(value || "").match(/-?\d+(?:[.,]\d+)?/);
    return match ? Number(match[0].replace(",", ".")) : null;
  }

  function ifcGuid() {
    const chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_$";
    let hex = "";
    if (globalThis.crypto?.getRandomValues) {
      const bytes = new Uint8Array(16);
      globalThis.crypto.getRandomValues(bytes);
      hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
    } else {
      hex = `${Date.now().toString(16)}${Math.random().toString(16).slice(2).padEnd(20, "0")}`.slice(0, 32);
    }
    let value = BigInt(`0x${hex}`);
    let result = "";
    for (let index = 0; index < 22; index += 1) {
      result += chars[Number(value % 64n)];
      value /= 64n;
    }
    return result;
  }

  function formatIfcNumber(value, decimals = 6) {
    const number = Number(value);
    if (!Number.isFinite(number)) return "0.";
    const fixed = number.toFixed(decimals).replace(/0+$/g, "").replace(/\.$/, ".");
    return fixed === "-0." ? "0." : fixed;
  }

  function ifcString(value) {
    return String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  }

  function fileBaseName(path) {
    return String(path || "output.ifc").split(/[\\/]/).pop() || "output.ifc";
  }

  function buildLandXmlDocument(parsed, options = {}) {
    const fileName = String(options.fileName || "")
      .replace(/\.(kof|sos|sosi|gml|jxl)$/i, "") || options.fallbackName || "KOF";
    const layerPrefix = options.layerPrefix || "Kof";
    const author = options.author || "kof2xml";
    const lineLayers = buildLandXmlLayerNames(parsed.lines, fileName, options);
    const now = new Date();
    const date = now.toISOString().slice(0, 10);
    const time = now.toISOString().slice(11, 19);

    return [
      "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
      `<LandXML xmlns="http://www.landxml.org/schema/LandXML-1.2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.landxml.org/schema/LandXML-1.2 http://www.landxml.org/schema/LandXML-1.2/LandXML-1.2.xsd" version="1.2" date="${date}" time="${time}">`,
      "  <Project name=\"\" desc=\"\">",
      "    <Feature code=\"trimbleLayers\">",
      ...(parsed.points.length ? [
        [
          "      <Feature code=\"trimbleLayer\">",
          "        <Property label=\"name\" value=\"Punkter\" />",
          "        <Property label=\"color\" value=\"255,255,255\" />",
          "        <Property label=\"lineStyleName\" value=\"Gjennomgående\" />",
          "        <Property label=\"lineWeight\" value=\"0\" />",
          "      </Feature>"
        ].join("\n")
      ] : []),
      ...lineLayers.map((layerName) => [
        "      <Feature code=\"trimbleLayer\">",
        `        <Property label="name" value="${escapeXml(layerName)}" />`,
        "        <Property label=\"color\" value=\"255,255,255\" />",
        "        <Property label=\"lineStyleName\" value=\"Gjennomgående\" />",
        "        <Property label=\"lineWeight\" value=\"0\" />",
        "      </Feature>"
      ].join("\n")),
      "    </Feature>",
      "  </Project>",
      "  <Units>",
      "    <Metric linearUnit=\"meter\" widthUnit=\"meter\" heightUnit=\"meter\" diameterUnit=\"meter\" areaUnit=\"squareMeter\" volumeUnit=\"cubicMeter\" temperatureUnit=\"celsius\" pressureUnit=\"HPA\" angularUnit=\"radians\" directionUnit=\"radians\" elevationUnit=\"meter\" velocityUnit=\"kilometersPerHour\" />",
      "  </Units>",
      `  <Application name="${escapeXml(CONFIG.APP_TITLE)}" manufacturer="" version="1.0" timeStamp="${date}T${time}">`,
      `    <Author createdBy="${escapeXml(author)}" timeStamp="${date}T${time}" />`,
      "  </Application>",
      "  <FeatureDictionary name=\"ISO15143-4\" />",
      buildLandXmlCgPoints(parsed.points),
      buildLandXmlPlanFeatures(parsed.lines, fileName, options),
      "</LandXML>"
    ].filter(Boolean).join("\n");
  }

  function parseKofForLandXml(kofText) {
    const lines = String(kofText || "").split(/\r?\n/);
    const points = [];
    const lineFeatures = [];
    let inLine = false;
    let currentLinePoints = [];

    for (const rawLine of lines) {
      const line = String(rawLine || "").trim();
      if (!line) continue;

      if (/^09(?:_|\s+)91\b/i.test(line)) {
        inLine = true;
        currentLinePoints = [];
        continue;
      }

      if (/^09(?:_|\s+)99\b/i.test(line)) {
        if (inLine && currentLinePoints.length >= 2) {
          lineFeatures.push({ pts: currentLinePoints });
        }
        inLine = false;
        currentLinePoints = [];
        continue;
      }

      const point = parseKof05Record(line);
      if (!point) continue;

      if (inLine) {
        currentLinePoints.push(point);
      } else {
        points.push(point);
      }
    }

    return {
      points: dedupeLandXmlPointNames(points),
      lines: lineFeatures
    };
  }

  function parseSosiForLandXml(sosiText) {
    const lines = String(sosiText || "").split(/\r?\n/);
    const points = [];
    const lineFeatures = [];
    let unit = 1;
    let current = null;
    let coordMode = false;
    let pointIndex = 0;
    let curveIndex = 0;
    let lastLineFeature = null;
    let lastCompletedObjectType = null;

    function finishObject() {
      if (!current) return;

      const coords = buildSosiCoordinates(current.coordValues, current.coordKey, unit);
      let producedLine = false;
      if (current.type === "PUNKT" && coords.length) {
        pointIndex += 1;
        const name = buildSosiPointName(current, pointIndex);
        points.push({
          rawName: name,
          name,
          n: coords[0].n,
          e: coords[0].e,
          h: coords[0].h,
          code: getSosiObjectCode(current),
          attributes: getSosiObjectAttributes(current)
        });
      }

      if (current.type === "KURVE" && coords.length >= 2 && hasUsableLineGeometry(coords)) {
        curveIndex += 1;
        const linePoints = coords.map((coord, index) => ({
          rawName: `K${curveIndex}_${String(index + 1).padStart(3, "0")}`,
          name: `K${curveIndex}_${String(index + 1).padStart(3, "0")}`,
          n: coord.n,
          e: coord.e,
          h: coord.h,
          code: getSosiObjectCode(current)
        }));
        lastLineFeature = {
          pts: linePoints,
          name: getSosiObjectName(current),
          code: getSosiObjectCode(current),
          attributes: getSosiObjectAttributes(current)
        };
        lineFeatures.push(lastLineFeature);
        producedLine = true;
      } else if (current.renamePreviousLine && lastLineFeature && getSosiObjectName(current)) {
        lastLineFeature.name = getSosiObjectName(current);
        lastLineFeature.attributes = getSosiObjectAttributes(current);
      } else if (current.type === "KURVE") {
        lastLineFeature = null;
      }

      lastCompletedObjectType = producedLine ? "KURVE" : current.type;
      current = null;
      coordMode = false;
    }

    for (const rawLine of lines) {
      const line = String(rawLine || "").trim();
      if (!line) continue;

      const unitMatch = line.match(/^\.\.+ENHET\s+(.+)$/i);
      if (unitMatch) {
        const parsedUnit = parseNumber(unitMatch[1]);
        if (parsedUnit != null && parsedUnit > 0) unit = parsedUnit;
        continue;
      }

      const objectMatch = line.match(/^\.(?!\.)(\S+)\s*([^.]*)$/);
      if (objectMatch) {
        finishObject();
        const objectType = objectMatch[1].toUpperCase();
        current = /^(PUNKT|KURVE|FLATE|TEKST|SYMBOL)$/i.test(objectType)
          ? {
              type: objectType,
              id: cleanSosiObjectId(objectMatch[2]),
              attrs: {},
              attrList: [],
              coordKey: null,
              coordValues: [],
              renamePreviousLine: /^(FLATE|TEKST|SYMBOL)$/i.test(objectType) && lastCompletedObjectType === "KURVE"
            }
          : null;
        continue;
      }

      if (!current) continue;

      const attrMatch = line.match(/^\.\.+([^\s]+)\s*(.*)$/);
      if (attrMatch) {
        const key = attrMatch[1].toUpperCase();
        const value = String(attrMatch[2] || "").trim();
        coordMode = isSosiCoordinateKey(key);

        if (coordMode) {
          current.coordKey = key;
          current.coordValues.push(...extractSosiCoordinateNumbers(value));
        } else {
          current.attrs[key] = value;
          current.attrList.push({
            label: attrMatch[1].trim(),
            value
          });
        }
        continue;
      }

      if (coordMode) {
        current.coordValues.push(...extractSosiCoordinateNumbers(line));
      }
    }

    finishObject();

    return {
      points: dedupeLandXmlPointNames(points),
      lines: lineFeatures
    };
  }

  function parseGmlForLandXml(gmlText) {
    const xml = repairUtf8Mojibake(String(gmlText || ""));
    const points = [];
    const lineFeatures = [];
    let pointIndex = 0;
    let lineIndex = 0;

    const members = extractGmlFeatureMembers(xml);
    const featureBlocks = members.length ? members : [{ type: "GML", body: xml }];

    for (const feature of featureBlocks) {
      const featureType = feature.type || "GML";
      const featureAttributes = getGmlFeatureAttributes(feature);
      const lineBlocks = [
        ...extractXmlBlocks(feature.body, "LineString"),
        ...extractXmlBlocks(feature.body, "LinearRing")
      ];

      for (const block of lineBlocks) {
        const coordinateSets = extractGmlCoordinateSets(block);
        for (const coords of coordinateSets) {
          if (coords.length < 2 || !hasUsableLineGeometry(coords)) continue;
          lineIndex += 1;
          lineFeatures.push({
            pts: coords.map((coord, index) => ({
              rawName: `G${lineIndex}_${String(index + 1).padStart(3, "0")}`,
              name: `G${lineIndex}_${String(index + 1).padStart(3, "0")}`,
              n: coord.n,
              e: coord.e,
              h: coord.h,
              code: featureType
            })),
            code: featureType,
            attributes: featureAttributes
          });
        }
      }

      const pointBlocks = extractXmlBlocks(feature.body, "Point");
      for (const block of pointBlocks) {
        const coordinateSets = extractGmlCoordinateSets(block);
        for (const coords of coordinateSets) {
          if (!coords.length) continue;
          pointIndex += 1;
          const name = `${featureType}_${pointIndex}`;
          points.push({
            rawName: name,
            name,
            n: coords[0].n,
            e: coords[0].e,
            h: coords[0].h,
            code: featureType,
            attributes: featureAttributes
          });
        }
      }
    }

    return {
      points: dedupeLandXmlPointNames(points),
      lines: lineFeatures
    };
  }

  function parseGmlForIfc(gmlText) {
    const xml = repairUtf8Mojibake(String(gmlText || ""));
    const members = extractGmlFeatureMembers(xml);
    const featureBlocks = members.length ? members : [{ type: "GML", attrsText: "", body: xml }];
    const objects = [];

    for (const feature of featureBlocks) {
      const props = gmlAttributesToProps(getGmlFeatureAttributes(feature));
      const featureType = feature.type || "GML";
      const featureId = props["gml:id"] || props.id || props.S_OBJID || `obj_${objects.length + 1}`;
      let produced = false;

      const lineBlocks = [
        ...extractXmlBlocks(feature.body, "LineString"),
        ...extractXmlBlocks(feature.body, "LinearRing")
      ];
      for (const block of lineBlocks) {
        const coordinateSets = extractGmlCoordinateSets(block);
        for (const coords of coordinateSets) {
          if (coords.length < 2) continue;
          const ifcCoords = coords.map((coord) => [coord.e, coord.n, coord.h]);
          objects.push({
            id: featureId,
            type: featureType,
            coords: ifcCoords,
            props,
            geom: detectIfcGeometryType(ifcCoords)
          });
          produced = true;
        }
      }

      const pointBlocks = extractXmlBlocks(feature.body, "Point");
      for (const block of pointBlocks) {
        const coordinateSets = extractGmlCoordinateSets(block);
        for (const coords of coordinateSets) {
          if (!coords.length) continue;
          const coord = coords[0];
          objects.push({
            id: featureId,
            type: featureType,
            coords: [[coord.e, coord.n, coord.h]],
            props,
            geom: "point"
          });
          produced = true;
        }
      }

      if (!produced && Object.keys(props).length) {
        objects.push({
          id: featureId,
          type: featureType,
          coords: [],
          props,
          geom: "none"
        });
      }
    }

    return objects;
  }

  function parseJxlForIfc(jxlText) {
    const xml = repairUtf8Mojibake(String(jxlText || ""));
    const doc = new DOMParser().parseFromString(xml, "application/xml");
    const parserError = doc.querySelector("parsererror");
    if (parserError) {
      throw new Error(`Kunne ikke lese JXL som XML: ${shortText(parserError.textContent || "", 300)}`);
    }

    const pointCoords = new Map();
    const pointRecords = [];
    for (const point of xmlElements(doc, "PointRecord")) {
      if (jxlDirectText(point, "Deleted").toLowerCase() === "true") continue;
      const name = cleanJxlPointName(jxlDirectText(point, "Name"));
      const coord = jxlRecordCoordinate(point);
      if (!name || !coord) continue;
      registerJxlPointCoord(pointCoords, name, coord);
      pointRecords.push({ point, name, coord });
    }

    for (const point of xmlElements(doc, "Point")) {
      const name = cleanJxlPointName(jxlDirectText(point, "Name"));
      const coord = jxlRecordCoordinate(point);
      if (!name || !coord) continue;
      registerJxlPointCoord(pointCoords, name, coord);
    }

    const objects = [];
    for (const { point, name, coord } of pointRecords) {
      const code = jxlDirectText(point, "Code");
      if (!code) continue;
      objects.push({
        id: name,
        type: code,
        coords: [coord],
        props: jxlSurveyPointProps(point, coord),
        geom: "point",
        source: "jxl"
      });
    }

    for (const polyline of xmlElements(doc, "LivePolylineRecord")) {
      if (jxlDirectText(polyline, "Deleted").toLowerCase() === "true") continue;
      const name = jxlDirectText(polyline, "Name");
      const code = jxlDirectText(polyline, "Code");
      if (!name || !code) continue;

      const pointNames = [];
      const start = jxlPointRefText(firstXmlChild(polyline, "StartPoint"));
      if (start) pointNames.push(start);
      const parts = firstXmlChild(polyline, "Parts");
      for (const endPoint of xmlElements(parts || polyline, "EndPoint")) {
        const value = jxlPointRefText(endPoint);
        if (value) pointNames.push(value);
      }

      const coords = pointNames.map((pointName) => pointCoords.get(cleanJxlPointName(pointName))).filter(Boolean);
      if (coords.length < 2) coords.push(...jxlInlineCoordinates(polyline));
      if (coords.length < 2) continue;
      objects.push({
        id: name,
        type: code,
        coords,
        props: jxlFeatureProps(polyline),
        geom: detectIfcGeometryType(coords),
        source: "jxl"
      });
    }

    if (!objects.some((object) => object.geom === "curve" || object.geom === "polygon") && /<[^>]*LivePolylineRecord\b/i.test(xml)) {
      const fallbackObjects = parseJxlForIfcTextFallback(xml);
      if (fallbackObjects.length) return fallbackObjects;
    }

    return objects;
  }

  function parseJxlForIfcTextFallback(xml) {
    const pointCoords = new Map();
    for (const block of [...xmlBlockContents(xml, "PointRecord"), ...xmlBlockContents(xml, "Point")]) {
      if (xmlFirstText(block, "Deleted").toLowerCase() === "true") continue;
      const name = cleanJxlPointName(xmlFirstText(block, "Name"));
      const coord = jxlCoordinateFromXmlBlock(block);
      if (name && coord) registerJxlPointCoord(pointCoords, name, coord);
    }

    const objects = [];
    for (const block of xmlBlockContents(xml, "LivePolylineRecord")) {
      if (xmlFirstText(block, "Deleted").toLowerCase() === "true") continue;
      const name = xmlFirstText(block, "Name");
      const code = xmlFirstText(block, "Code");
      if (!name || !code) continue;

      const pointNames = [
        cleanJxlPointName(xmlFirstText(block, "StartPoint")),
        ...xmlAllTexts(block, "EndPoint").map(cleanJxlPointName)
      ].filter(Boolean);
      let coords = pointNames.map((pointName) => pointCoords.get(pointName)).filter(Boolean);
      if (coords.length < 2 && pointCoords.size >= 2) {
        coords = Array.from(pointCoords.values());
      }
      if (coords.length < 2) continue;

      objects.push({
        id: name,
        type: code,
        coords,
        props: jxlFeaturePropsFromXmlBlock(block),
        geom: detectIfcGeometryType(coords),
        source: "jxl"
      });
    }
    return objects;
  }

  function xmlBlockContents(xml, tagName) {
    const escaped = tagName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const pattern = new RegExp(`<(?:[A-Za-z_][\\w.-]*:)?${escaped}\\b[^>]*>([\\s\\S]*?)<\\/(?:[A-Za-z_][\\w.-]*:)?${escaped}>`, "gi");
    return Array.from(String(xml || "").matchAll(pattern), (match) => match[1] || "");
  }

  function jxlTextDiagnostics(xml) {
    const pointBlocks = [...xmlBlockContents(xml, "PointRecord"), ...xmlBlockContents(xml, "Point")];
    const lineBlocks = xmlBlockContents(xml, "LivePolylineRecord");
    const pointCoords = new Map();
    for (const block of pointBlocks) {
      const name = cleanJxlPointName(xmlFirstText(block, "Name"));
      const coord = jxlCoordinateFromXmlBlock(block);
      if (name && coord) registerJxlPointCoord(pointCoords, name, coord);
    }
    const firstLine = lineBlocks[0] || "";
    const refs = [
      cleanJxlPointName(xmlFirstText(firstLine, "StartPoint")),
      ...xmlAllTexts(firstLine, "EndPoint").map(cleanJxlPointName)
    ].filter(Boolean);
    return {
      length: String(xml || "").length,
      hasJobFile: /<[^>]*JOBFile\b/i.test(xml),
      pointRecordBlocks: xmlBlockContents(xml, "PointRecord").length,
      reductionsPointBlocks: xmlBlockContents(xml, "Point").length,
      computedGridBlocks: xmlBlockContents(xml, "ComputedGrid").length,
      gridBlocks: xmlBlockContents(xml, "Grid").length,
      livePolylineBlocks: lineBlocks.length,
      firstLineName: xmlFirstText(firstLine, "Name"),
      firstLineCode: xmlFirstText(firstLine, "Code"),
      firstLineRefs: refs,
      resolvedPointNames: Array.from(pointCoords.keys()).slice(0, 20),
      resolvedPointCount: pointCoords.size,
      matchedRefs: refs.filter((ref) => pointCoords.has(ref))
    };
  }

  function xmlFirstText(xml, tagName) {
    return xmlAllTexts(xml, tagName)[0] || "";
  }

  function xmlAllTexts(xml, tagName) {
    const escaped = tagName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const pattern = new RegExp(`<(?:[A-Za-z_][\\w.-]*:)?${escaped}\\b[^>]*>([\\s\\S]*?)<\\/(?:[A-Za-z_][\\w.-]*:)?${escaped}>`, "gi");
    return Array.from(String(xml || "").matchAll(pattern), (match) => stripXmlText(match[1] || ""));
  }

  function stripXmlText(value) {
    return String(value || "")
      .replace(/<[^>]+>/g, " ")
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, "\"")
      .replace(/&apos;/g, "'")
      .replace(/\s+/g, " ")
      .trim();
  }

  function jxlCoordinateFromXmlBlock(block) {
    for (const gridTag of ["ComputedGrid", "Grid"]) {
      for (const grid of xmlBlockContents(block, gridTag)) {
        const north = firstNumber(xmlFirstText(grid, "North"));
        const east = firstNumber(xmlFirstText(grid, "East"));
        const elevation = firstNumber(xmlFirstText(grid, "Elevation")) ?? 0;
        if (north != null && east != null) return [east, north, elevation];
      }
    }
    const wgs84 = jxlWgs84CoordinateFromXmlBlock(block);
    if (wgs84) return wgs84;
    const ecef = jxlEcefCoordinateFromXmlBlock(block);
    if (ecef) return ecef;
    return null;
  }

  function jxlWgs84CoordinateFromXmlBlock(block) {
    for (const wgs of xmlBlockContents(block, "WGS84")) {
      const lat = firstNumber(xmlFirstText(wgs, "Latitude"));
      const lon = firstNumber(xmlFirstText(wgs, "Longitude"));
      const height = firstNumber(xmlFirstText(wgs, "Height")) ?? firstNumber(xmlFirstText(wgs, "Elevation")) ?? 0;
      if (lat != null && lon != null) return latLonToUtm32(lat, lon, ellipsoidToNn2000Height(lat, lon, height));
    }
    return null;
  }

  function jxlEcefCoordinateFromXmlBlock(block) {
    for (const ecef of xmlBlockContents(block, "ECEF")) {
      const x = firstNumber(xmlFirstText(ecef, "X"));
      const y = firstNumber(xmlFirstText(ecef, "Y"));
      const z = firstNumber(xmlFirstText(ecef, "Z"));
      if (x != null && y != null && z != null) {
        const llh = ecefToWgs84(x, y, z);
        return latLonToUtm32(llh.lat, llh.lon, ellipsoidToNn2000Height(llh.lat, llh.lon, llh.height));
      }
    }
    return null;
  }

  function ecefToWgs84(x, y, z) {
    const a = 6378137.0;
    const e2 = 6.6943799901413165e-3;
    const b = a * Math.sqrt(1 - e2);
    const ep2 = (a * a - b * b) / (b * b);
    const p = Math.sqrt(x * x + y * y);
    const theta = Math.atan2(z * a, p * b);
    const sinTheta = Math.sin(theta);
    const cosTheta = Math.cos(theta);
    const lon = Math.atan2(y, x);
    const lat = Math.atan2(
      z + ep2 * b * sinTheta * sinTheta * sinTheta,
      p - e2 * a * cosTheta * cosTheta * cosTheta
    );
    const n = a / Math.sqrt(1 - e2 * Math.sin(lat) * Math.sin(lat));
    const height = p / Math.cos(lat) - n;
    return { lat: lat * 180 / Math.PI, lon: lon * 180 / Math.PI, height };
  }

  function ellipsoidToNn2000Height(_latDeg, _lonDeg, ellipsoidHeight) {
    return (Number(ellipsoidHeight) || 0) - CONFIG.JXL_ECEF_NN2000_GEOID_OFFSET_M;
  }

  function latLonToUtm32(latDeg, lonDeg, height = 0) {
    const a = 6378137.0;
    const f = 1 / 298.257223563;
    const e2 = f * (2 - f);
    const ep2 = e2 / (1 - e2);
    const k0 = 0.9996;
    const lat = latDeg * Math.PI / 180;
    const lon = lonDeg * Math.PI / 180;
    const lon0 = 9 * Math.PI / 180;
    const sinLat = Math.sin(lat);
    const cosLat = Math.cos(lat);
    const tanLat = Math.tan(lat);
    const n = a / Math.sqrt(1 - e2 * sinLat * sinLat);
    const t = tanLat * tanLat;
    const c = ep2 * cosLat * cosLat;
    const aa = cosLat * (lon - lon0);
    const m = a * (
      (1 - e2 / 4 - 3 * e2 * e2 / 64 - 5 * e2 * e2 * e2 / 256) * lat
      - (3 * e2 / 8 + 3 * e2 * e2 / 32 + 45 * e2 * e2 * e2 / 1024) * Math.sin(2 * lat)
      + (15 * e2 * e2 / 256 + 45 * e2 * e2 * e2 / 1024) * Math.sin(4 * lat)
      - (35 * e2 * e2 * e2 / 3072) * Math.sin(6 * lat)
    );
    const east = k0 * n * (aa + (1 - t + c) * aa ** 3 / 6 + (5 - 18 * t + t * t + 72 * c - 58 * ep2) * aa ** 5 / 120) + 500000;
    const north = k0 * (m + n * tanLat * (aa * aa / 2 + (5 - t + 9 * c + 4 * c * c) * aa ** 4 / 24 + (61 - 58 * t + t * t + 600 * c - 330 * ep2) * aa ** 6 / 720));
    return [east, north, height || 0];
  }

  function jxlFeaturePropsFromXmlBlock(block) {
    const props = {};
    for (const attribute of xmlBlockContents(block, "Attribute")) {
      const name = xmlFirstText(attribute, "Name");
      const value = xmlFirstText(attribute, "Value");
      if (name && value) props[name] = value;
    }
    return props;
  }

  function registerJxlPointCoord(pointCoords, name, coord) {
    const key = cleanJxlPointName(name);
    if (!key || !coord) return;
    const existing = pointCoords.get(key);
    if (!existing || (isZeroCoord(existing) && !isZeroCoord(coord)) || (!isLikelyJxlGridCoord(existing) && isLikelyJxlGridCoord(coord))) {
      pointCoords.set(key, coord);
    }
  }

  function jxlSurveyPointProps(record, coord) {
    const props = jxlFeatureProps(record);
    for (const key of ["Name", "Code", "Method", "SurveyMethod", "Classification"]) {
      const value = jxlDirectText(record, key);
      if (value) props[key] = value;
    }
    props.OriginalX = formatIfcNumber(coord[0]);
    props.OriginalY = formatIfcNumber(coord[1]);
    props.OriginalZ = formatIfcNumber(coord[2]);
    props.CoordinateOrder = "X=East, Y=North, Z=Elevation";
    return props;
  }

  function jxlFeatureProps(record) {
    const props = {};
    const features = firstXmlChild(record, "Features");
    const feature = firstXmlChild(features || record, "Feature");
    if (!feature) return props;

    for (const attribute of xmlElements(feature, "Attribute")) {
      const name = jxlDirectText(attribute, "Name");
      const value = jxlDirectText(attribute, "Value");
      if (name && value) props[name] = value;
    }
    return props;
  }

  function jxlRecordCoordinate(record) {
    const candidates = [];
    if (["Grid", "ComputedGrid", "Local", "Coordinate", "Coordinates"].includes(xmlLocalName(record))) {
      const coord = jxlCoordinateFromElement(record);
      if (coord) candidates.push(coord);
    }
    for (const child of Array.from(record?.children || [])) {
      if (["Grid", "ComputedGrid", "Local", "Coordinate", "Coordinates"].includes(xmlLocalName(child))) {
        const coord = jxlCoordinateFromElement(child);
        if (coord) candidates.push(coord);
      }
    }
    for (const tagName of ["ComputedGrid", "Grid"]) {
      for (const grid of xmlElements(record, tagName)) {
        const coord = jxlCoordinateFromElement(grid);
        if (coord) candidates.push(coord);
      }
    }
    return candidates.find(isLikelyJxlGridCoord) || candidates.find((coord) => !isZeroCoord(coord)) || candidates[0] || null;
  }

  function jxlCoordinateFromElement(element) {
    if (!element) return null;
    const north = firstNumberFromXml(element, ["North", "N", "Y"]);
    const east = firstNumberFromXml(element, ["East", "E", "X"]);
    const elevation = firstNumberFromXml(element, ["Elevation", "Elev", "Height", "H", "Z"]) ?? 0;
    if (north != null && east != null) return [east, north, elevation];

    const nums = String(element.textContent || "").match(/-?\d+(?:[.,]\d+)?/g) || [];
    if (nums.length >= 2) {
      const parsed = nums.map((value) => parseNumber(value)).filter((value) => value != null);
      if (parsed.length >= 2) return [parsed[1], parsed[0], parsed[2] ?? 0];
    }
    return null;
  }

  function firstNumberFromXml(root, names) {
    const wanted = new Set(names);
    for (const node of [root, ...xmlElements(root, "*")]) {
      if (wanted.has(xmlLocalName(node))) {
        const value = parseNumber(node.textContent);
        if (value != null) return value;
      }
    }
    return null;
  }

  function jxlInlineCoordinates(record) {
    const coords = [];
    for (const tagName of ["ComputedGrid", "Grid"]) {
      for (const element of xmlElements(record, tagName)) {
        const coord = jxlCoordinateFromElement(element);
        if (coord && !coords.some((existing) => sameCoord(existing, coord))) coords.push(coord);
      }
    }
    return coords;
  }

  function jxlPointRefText(element) {
    for (const key of ["Point", "PointName", "Name", "EndPoint"]) {
      const value = jxlDirectText(element, key);
      if (value) return value;
    }
    return cleanJxlPointName(element?.textContent || "");
  }

  function cleanJxlPointName(value) {
    const text = String(value || "").trim();
    if (!text) return "";
    return text.split(/\s+/)[0].replace(/^["']|["']$/g, "");
  }

  function isLikelyJxlGridCoord(coord) {
    return Array.isArray(coord) &&
      Math.abs(Number(coord[0]) || 0) > 1000 &&
      Math.abs(Number(coord[1]) || 0) > 1000;
  }

  function isZeroCoord(coord) {
    return Array.isArray(coord) && coord.every((value) => Math.abs(Number(value) || 0) < 1e-9);
  }

  function sameCoord(a, b) {
    return Array.isArray(a) && Array.isArray(b) &&
      Math.abs((a[0] || 0) - (b[0] || 0)) < 1e-6 &&
      Math.abs((a[1] || 0) - (b[1] || 0)) < 1e-6 &&
      Math.abs((a[2] || 0) - (b[2] || 0)) < 1e-6;
  }

  function xmlElements(root, localName) {
    if (!root) return [];
    return Array.from(root.getElementsByTagName("*")).filter((node) => localName === "*" || xmlLocalName(node) === localName);
  }

  function firstXmlChild(root, localName) {
    if (!root) return null;
    return Array.from(root.children || []).find((node) => xmlLocalName(node) === localName) || null;
  }

  function jxlDirectText(root, localName) {
    const child = firstXmlChild(root, localName);
    return child ? String(child.textContent || "").trim() : "";
  }

  function xmlLocalName(node) {
    return node?.localName || String(node?.nodeName || "").replace(/^.*:/, "");
  }

  function gmlAttributesToProps(attributes) {
    const props = {};
    for (const attribute of Array.isArray(attributes) ? attributes : []) {
      const label = String(attribute?.label || "").trim();
      if (!label) continue;
      props[label] = String(attribute?.value ?? "").trim();
    }
    return props;
  }

  function detectIfcGeometryType(coords) {
    if (!Array.isArray(coords) || !coords.length) return "none";
    if (coords.length === 1) return "point";
    const first = coords[0];
    const last = coords[coords.length - 1];
    const closed = Math.abs(first[0] - last[0]) < 1e-4 &&
      Math.abs(first[1] - last[1]) < 1e-4 &&
      Math.abs(first[2] - last[2]) < 1e-4;
    return closed && coords.length >= 4 ? "polygon" : "curve";
  }

  function extractGmlFeatureMembers(xml) {
    const members = [];
    for (const memberMatch of String(xml || "").matchAll(/<(?:[A-Za-z_][\w.-]*:)?featureMember\b[^>]*>([\s\S]*?)<\/(?:[A-Za-z_][\w.-]*:)?featureMember>/gi)) {
      const memberBody = memberMatch[1] || "";
      const featureMatch = memberBody.match(/^\s*<((?:[A-Za-z_][\w.-]*:)?([A-Za-z_][\w.-]*))\b([^>]*)>([\s\S]*?)<\/\1>\s*$/);
      if (featureMatch) {
        members.push({
          type: localXmlName(featureMatch[2]),
          attrsText: featureMatch[3] || "",
          body: featureMatch[4] || ""
        });
      } else {
        members.push({ type: "GML", body: memberBody });
      }
    }
    return members;
  }

  function getGmlFeatureAttributes(feature) {
    const attributes = [];
    const idMatch = String(feature?.attrsText || "").match(/\b(?:gml:)?id\s*=\s*["']([^"']+)["']/i);
    if (idMatch) {
      attributes.push({
        label: "gml:id",
        value: decodeXmlText(idMatch[1])
      });
    }

    for (const childMatch of String(feature?.body || "").matchAll(/<((?:[^\s<>/:]+:)?([^\s<>/]+))(?=[\s>/])[^>]*>([\s\S]*?)<\/\1>/g)) {
      const rawValue = childMatch[3] || "";
      if (/<[^>]+>/.test(rawValue)) continue;
      const value = decodeXmlText(rawValue);
      if (!value) continue;
      attributes.push({
        label: localXmlName(childMatch[2]),
        value
      });
    }

    return attributes;
  }

  function extractXmlBlocks(xml, localName) {
    const escapedName = localName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const pattern = new RegExp(`<(?:[A-Za-z_][\\w.-]*:)?${escapedName}\\b[^>]*>[\\s\\S]*?<\\/(?:[A-Za-z_][\\w.-]*:)?${escapedName}>`, "gi");
    return String(xml || "").match(pattern) || [];
  }

  function extractGmlCoordinateSets(block) {
    const coordinateSets = [];
    const dimensions = getGmlSrsDimension(block);

    for (const posListMatch of String(block || "").matchAll(/<(?:[A-Za-z_][\w.-]*:)?posList\b([^>]*)>([\s\S]*?)<\/(?:[A-Za-z_][\w.-]*:)?posList>/gi)) {
      const posListDimensions = getGmlSrsDimension(posListMatch[1] || "") || dimensions;
      const coords = buildGmlCoordinates(extractSosiNumbers(stripXmlTags(posListMatch[2])), posListDimensions || dimensions);
      if (coords.length) coordinateSets.push(coords);
    }

    const withoutPosLists = String(block || "").replace(/<(?:[A-Za-z_][\w.-]*:)?posList\b[^>]*>[\s\S]*?<\/(?:[A-Za-z_][\w.-]*:)?posList>/gi, "");
    const posCoords = [];
    for (const posMatch of withoutPosLists.matchAll(/<(?:[A-Za-z_][\w.-]*:)?pos\b([^>]*)>([\s\S]*?)<\/(?:[A-Za-z_][\w.-]*:)?pos>/gi)) {
      const posDimensions = getGmlSrsDimension(posMatch[1] || "") || dimensions;
      const coords = buildGmlCoordinates(extractSosiNumbers(stripXmlTags(posMatch[2])), posDimensions || dimensions);
      if (coords.length) posCoords.push(coords[0]);
    }
    if (posCoords.length) coordinateSets.push(posCoords);

    return coordinateSets;
  }

  function getGmlSrsDimension(text) {
    const match = String(text || "").match(/\bsrsDimension\s*=\s*["']?(\d+)/i);
    const dimension = match ? Number(match[1]) : null;
    return Number.isFinite(dimension) && dimension >= 2 ? dimension : null;
  }

  function buildGmlCoordinates(numbers, dimensions = null) {
    const coords = [];
    const inferredDimensions = dimensions || (numbers.length % 3 === 0 ? 3 : 2);
    const stride = inferredDimensions >= 3 ? 3 : 2;
    for (let index = 0; index + stride - 1 < numbers.length; index += stride) {
      const e = Number(numbers[index]);
      const n = Number(numbers[index + 1]);
      const h = stride === 3 ? Number(numbers[index + 2]) : 0;
      if (!Number.isFinite(n) || !Number.isFinite(e)) continue;
      coords.push({
        n,
        e,
        h: Number.isFinite(h) ? h : 0
      });
    }
    return coords;
  }

  function stripXmlTags(value) {
    return String(value || "").replace(/<[^>]*>/g, " ");
  }

  function decodeXmlText(value) {
    return repairUtf8Mojibake(String(value || ""))
      .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
      .replace(/&#x([0-9a-f]+);/gi, (_match, hex) => String.fromCodePoint(parseInt(hex, 16)))
      .replace(/&#(\d+);/g, (_match, number) => String.fromCodePoint(parseInt(number, 10)))
      .replace(/&quot;/g, "\"")
      .replace(/&apos;/g, "'")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&amp;/g, "&")
      .replace(/\u00a0/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function localXmlName(name) {
    return String(name || "").split(":").pop() || "";
  }

  function cleanSosiObjectId(value) {
    return String(value || "").replace(/[:\s]+$/g, "").trim();
  }

  function isSosiCoordinateKey(key) {
    const normalized = normalizeSosiKey(key);
    return normalized === "NOH" || normalized === "NO";
  }

  function normalizeSosiKey(key) {
    return repairUtf8Mojibake(String(key || ""))
      .toUpperCase()
      .replace(/\u00c6/g, "AE")
      .replace(/\u00d8/g, "O")
      .replace(/\u00c5/g, "A")
      .replace(/[?\ufffd]/g, "O")
      .replace(/[^A-Z0-9]/g, "");
  }

  function extractSosiNumbers(value) {
    return String(value || "")
      .split(/\s+/)
      .filter((part) => String(part || "").trim())
      .map((part) => parseNumber(part))
      .filter((number) => number != null);
  }

  function extractSosiCoordinateNumbers(value) {
    const coordinatePart = String(value || "").replace(/\s+\.\.\..*$/u, "");
    return extractSosiNumbers(coordinatePart);
  }

  function buildSosiCoordinates(values, coordKey, unit) {
    const numbers = Array.isArray(values) ? values : [];
    const normalizedKey = normalizeSosiKey(coordKey);
    const dimensions = normalizedKey === "NO" ? 2 : 3;
    const coords = [];

    for (let index = 0; index + dimensions - 1 < numbers.length; index += dimensions) {
      coords.push({
        n: scaleSosiCoordinate(numbers[index], unit),
        e: scaleSosiCoordinate(numbers[index + 1], unit),
        h: dimensions === 3 ? scaleSosiCoordinate(numbers[index + 2], unit) : 0
      });
    }

    return coords;
  }

  function hasUsableLineGeometry(coords) {
    return coords.some((coord, index) =>
      index > 0 && distance2d(coords[index - 1], coord) > 0
    );
  }

  function scaleSosiCoordinate(value, unit) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 0;
    return n * (Number.isFinite(unit) && unit > 0 ? unit : 1);
  }

  function buildSosiPointName(object, fallbackIndex) {
    const id = cleanSosiObjectId(object?.id);
    return id || String(fallbackIndex);
  }

  function getSosiObjectCode(object) {
    const attrs = object?.attrs || {};
    return attrs.OBJTYPE || attrs.LTEMA || attrs.TEMA || "";
  }

  function getSosiObjectName(object) {
    return String(object?.attrs?.OBJTYPE || "").trim();
  }

  function getSosiObjectAttributes(object) {
    return (Array.isArray(object?.attrList) ? object.attrList : [])
      .filter((attribute) => attribute && attribute.label)
      .map((attribute) => ({
        label: String(attribute.label || "").trim(),
        value: String(attribute.value ?? "").trim()
      }));
  }

  function dedupeLandXmlPointNames(points) {
    const totals = {};
    for (const point of points) {
      const base = point.rawName || "Point";
      totals[base] = (totals[base] || 0) + 1;
    }

    const seen = {};
    return points.map((point) => {
      const base = point.rawName || "Point";
      seen[base] = (seen[base] || 0) + 1;
      return {
        ...point,
        name: totals[base] === 1 ? base : `${base}_${seen[base]}`
      };
    });
  }

  function buildLandXmlCgPoints(points) {
    if (!points.length) return "  <CgPoints />";

    const inner = points.map((point) => {
      const coords = `${formatLandXmlNumber(point.n)} ${formatLandXmlNumber(point.e)} ${formatLandXmlNumber(point.h)}`;
      const name = escapeXml(point.name);
      const attributesXml = buildLandXmlTrimbleCadFeature(point.attributes, "      ");

      if (!attributesXml) {
        return `    <CgPoint name="${name}" desc="${name}" featureRef="Punkter">${coords}</CgPoint>`;
      }

      return [
        `    <CgPoint name="${name}" desc="${name}" featureRef="Punkter">`,
        `      ${coords}`,
        attributesXml,
        "    </CgPoint>"
      ].join("\n");
    }).join("\n");

    return `  <CgPoints>\n${inner}\n  </CgPoints>`;
  }

  function buildLandXmlPropertyRows(attributes, indent = "") {
    return (Array.isArray(attributes) ? attributes : [])
      .filter((attribute) => attribute && attribute.label)
      .map((attribute) =>
        `${indent}  <Property label="${escapeXml(attribute.label)}" value="${escapeXml(attribute.value ?? "")}" />`
      );
  }

  function buildLandXmlTrimbleCadFeature(attributes, indent = "", leadingRows = []) {
    const rows = [
      ...(Array.isArray(leadingRows) ? leadingRows : []),
      ...buildLandXmlPropertyRows(attributes, indent)
    ];
    if (!rows.length) return "";

    return [
      `${indent}<Feature code="trimbleCADProperties">`,
      ...rows,
      `${indent}</Feature>`
    ].join("\n");
  }

  function buildLandXmlLayerNames(lines, fileName, options = {}) {
    const layerPrefix = options.layerPrefix || "Kof";
    const useLineCodeLayers = !!options.useLineCodeLayers;
    const fallbackLayer = `${layerPrefix}_${fileName}_`;
    const names = [];

    for (const line of Array.isArray(lines) ? lines : []) {
      const code = String(line?.code || "").trim();
      const layerName = useLineCodeLayers && code
        ? `${layerPrefix}_${fileName}_${code}`
        : fallbackLayer;
      if (!names.includes(layerName)) names.push(layerName);
    }

    return names.length ? names : [fallbackLayer];
  }

  function buildLandXmlPlanFeatures(lines, fileName, options = {}) {
    if (!lines.length) return "";
    const layerPrefix = options.layerPrefix || "Kof";
    const useLineCodeLayers = !!options.useLineCodeLayers;
    const namePrefix = options.planFeatureNamePrefix || "";
    const lineColor = options.lineColor || "144,238,144";
    const featureNameCounts = {};

    const features = lines.map((line, index) => {
      const rawCode = String(line?.code || "").trim();
      const name = buildLandXmlPlanFeatureName(line, index, fileName, namePrefix, featureNameCounts);
      const rawLayer = useLineCodeLayers && rawCode
        ? `${layerPrefix}_${fileName}_${rawCode}`
        : `${layerPrefix}_${fileName}_`;
      const layer = escapeXml(rawLayer);
      const trimbleCadXml = buildLandXmlTrimbleCadFeature(line.attributes, "      ", [
        `        <Property label="layer" value="${layer}" />`,
        `        <Property label="color" value="${escapeXml(lineColor)}" />`
      ]);
      return [
        `    <PlanFeature name="${name}">`,
        "      <CoordGeom>",
        buildLandXmlCoordGeom(line.pts),
        "      </CoordGeom>",
        trimbleCadXml,
        "    </PlanFeature>"
      ].filter(Boolean).join("\n");
    }).join("\n");

    return `  <PlanFeatures>\n${features}\n  </PlanFeatures>`;
  }

  function buildLandXmlPlanFeatureName(line, index, fileName, namePrefix, featureNameCounts) {
    const explicitName = String(line?.name || "").trim();
    if (explicitName) {
      featureNameCounts[explicitName] = (featureNameCounts[explicitName] || 0) + 1;
      const count = featureNameCounts[explicitName];
      return count === 1
        ? escapeXml(explicitName)
        : `${escapeXml(explicitName)}(${count})`;
    }

    return namePrefix
      ? `${escapeXml(namePrefix)} ${index + 1}`
      : `${escapeXml(fileName)}_${index + 1}`;
  }

  function buildLandXmlCoordGeom(points) {
    const segments = [];
    let station = 0;

    for (let index = 1; index < points.length; index += 1) {
      const start = points[index - 1];
      const end = points[index];
      const length = distance2d(start, end);

      segments.push([
        `        <Line length="${formatLandXmlNumber(length)}" staStart="${formatLandXmlNumber(station)}">`,
        `          <Start>${formatLandXmlNumber(start.n)} ${formatLandXmlNumber(start.e)} ${formatLandXmlNumber(start.h)}</Start>`,
        `          <End>${formatLandXmlNumber(end.n)} ${formatLandXmlNumber(end.e)} ${formatLandXmlNumber(end.h)}</End>`,
        "        </Line>"
      ].join("\n"));

      station += length;
    }

    return segments.join("\n");
  }

  function distance2d(a, b) {
    const dn = (b?.n || 0) - (a?.n || 0);
    const de = (b?.e || 0) - (a?.e || 0);
    return Math.sqrt((dn * dn) + (de * de));
  }

  function formatLandXmlNumber(value, decimals = 5) {
    if (value == null || !Number.isFinite(value)) return "0.00000";
    return Number(value).toFixed(decimals);
  }

  function escapeXml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&apos;");
  }

  function isNameKey(key) {
    return ["punktnavn", "punktnummer", "punktnr", "punktid", "punkt", "navn", "name", "id", "label"].includes(key);
  }

  function isNorthKey(key) { return ["n", "nord", "north", "northing", "y"].includes(key); }
  function isEastKey(key) { return ["e", "ost", "east", "easting", "x"].includes(key); }
  function isHeightKey(key) { return ["h", "z", "hoyde", "height", "elev", "elevation", "kote"].includes(key); }

  function parseFileTimestamp(value) {
    if (!value) return null;
    const time = Date.parse(String(value));
    return Number.isFinite(time) ? time : null;
  }

  function isConvertedOutputCurrent(file) {
    const outputs = Array.isArray(file?.existingOutputs) ? file.existingOutputs : [];
    if (!outputs.length) return false;
    const expectedExtensions = getExpectedOutputExtensions(file);
    const matchingOutputs = outputs.filter((output) =>
      expectedExtensions.some((extension) => String(output?.name || "").toLowerCase().endsWith(extension))
    );
    if (!matchingOutputs.length) return false;

    const sourceTime = parseFileTimestamp(file.modifiedOn);
    if (!sourceTime) return true;

    return matchingOutputs.some((output) => {
      const outputTime = parseFileTimestamp(output.modifiedOn);
      return outputTime != null && outputTime + 1000 >= sourceTime;
    });
  }

  function getExpectedOutputExtensions(file) {
    const name = String(file?.name || "").toLowerCase();
    if (name.endsWith(".gml") || name.endsWith(".jxl")) {
      return [".ifc"];
    }
    if (name.endsWith(".sos") || name.endsWith(".sosi")) return [".xml"];
    return [".txt", ".xml"];
  }

  function getFileConversionState(file) {
    if (isConvertedOutputCurrent(file)) {
      return { className: "converted", label: "Konvertering OK" };
    }

    if (Array.isArray(file?.existingOutputs) && file.existingOutputs.length > 0) {
      return { className: "outdated", label: "Ny versjon" };
    }

    return { className: "pending", label: "Venter" };
  }

  function markFileConverted(file, outName) {
    if (!file || !outName) return;
    const outputs = Array.isArray(file.existingOutputs) ? file.existingOutputs : [];
    file.existingOutputs = [
      ...outputs.filter((output) => output.name !== outName),
      {
        name: outName,
        modifiedOn: new Date().toISOString(),
        localConversionCurrent: true
      }
    ];
  }

  function getPendingKofFiles(files = state.fileList) {
    return (Array.isArray(files) ? files : []).filter((file) => !isConvertedOutputCurrent(file));
  }

  async function refreshKofList() {
    try {
      setBusy(true);
      showHint(null, false);
      await ensureReady();
      setStatus("Henter KOF/SOSI/GML-filer fra prosjektet...", "working");

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
        setStatus("Ingen KOF/SOSI/GML-filer funnet i prosjektet", "neutral");
      } else {
        const pendingCount = getPendingKofFiles().length;
        const convertedCount = state.fileList.length - pendingCount;
        const suffix = convertedCount
          ? `, ${pendingCount} mangler konvertering`
          : "";
        setStatus(`Fant ${state.fileList.length} KOF/SOSI/GML-fil${state.fileList.length === 1 ? "" : "er"}${suffix}`, "success");
      }

      const pendingFiles = getPendingKofFiles();
      const debugPayload = {
        action: "listProjectKofFiles",
        fileCount: state.fileList.length,
        pendingCount: pendingFiles.length,
        candidatesTried: result.candidatesTried,
        source: result.source,
        sources: result.sources || null,
        files: state.fileList.map((f) => ({
          name: f.name,
          path: f.path || "",
          versionId: f.versionId || null,
          revision: f.revision || null,
          modifiedOn: f.modifiedOn || null,
          conversionPending: !isConvertedOutputCurrent(f),
          existingOutputs: (f.existingOutputs || []).map((output) => ({
            name: output.name,
            versionId: output.versionId || null,
            revision: output.revision || null,
            modifiedOn: output.modifiedOn || null
          }))
        }))
      };

      if (result.source !== "folder-tree" || state.fileList.length === 0) {
        debugPayload.diagnostics = result.diagnostics || null;
      }

      setDebug(debugPayload);
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

    const pendingFiles = getPendingKofFiles();
    if (!CONFIG.AUTO_CONVERT_ON_OPEN || state.manualSelectionMode || state.autoConvertInProgress || !pendingFiles.length) {
      return;
    }

    state.autoConvertInProgress = true;
    try {
      setStatus(`Starter automatisk konvertering av ${pendingFiles.length} KOF/SOSI/GML-fil${pendingFiles.length === 1 ? "" : "er"}...`, "working");
      await processAllFiles({ source: "auto-open", files: pendingFiles });
    } finally {
      state.autoConvertInProgress = false;
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
    if (!result.ok) throw new Error(result.error || result.step || "Kunne ikke laste ned kildefil");

    const converted = convertKofFile(result.text || "", result.file?.name || file.name || "output.kof");
    return { ...converted, result };
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

      const uploadResult = await uploadConvertedTxtToProject({
        sourceFile: converted.result.file,
        outName: converted.outName,
        txt: converted.text
      });
      state.lastUploadResult = uploadResult;

      if (uploadResult.ok) {
        markFileConverted(file, converted.outName);
        renderFileList();
        setStatus(`Ferdig: ${converted.outName} er lastet opp til prosjektet`, "success");
        showHint(buildConversionHint(converted, uploadResult));
      } else {
        triggerDownload(converted.outName, converted.text);
        setStatus(`Ferdig: ${converted.outName} er lastet ned lokalt`, "success");
        showHint(buildConversionHint(converted, uploadResult));
      }

      setDebug({
        action: "processSelectedFile",
        sourceFile: converted.result.file,
        convertedFile: { name: converted.outName, format: converted.format, size: converted.text.length, stats: converted.stats || null },
        uploadResult,
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

  function requestStopConversion() {
    if (!state.conversionInProgress) return;
    state.cancelConversionRequested = true;
    state.manualSelectionMode = true;
    state.manualSelectedFileIds.clear();
    setStatus("Stopper etter pågående fil...", "working");
    showHint("Konverteringen stoppes når filen som behandles akkurat nå er ferdig. Deretter kan du velge filer manuelt og trykke <strong>Konverter valgte</strong>.");
    renderFileList();
    setBusy(state.busy);
  }

  async function processManualSelectedFiles() {
    const selectedFiles = state.fileList.filter((file) => state.manualSelectedFileIds.has(file.id));
    if (!selectedFiles.length) {
      setStatus("Velg minst en KOF/SOSI/GML-fil først", "error");
      return;
    }
    await processAllFiles({ source: "manual-selected", files: selectedFiles, skipExisting: false });
  }

  async function refreshJxlSources() {
    try {
      setBusy(true);
      showHint(null, false);
      await ensureReady();
      setStatus("Henter JXL fra Connect Explorer og Field Data...", "working");

      const proxyRes = await callProxy("listJxlSources", {
        token: state.accessToken,
        projectId: state.project.id,
        projectLocation: state.project.location
      });

      if (!proxyRes.ok || !proxyRes.json) {
        setStatus(`Feil: Proxy svarte med HTTP ${proxyRes.status}`, "error");
        setDebug({ action: "listJxlSources", status: proxyRes.status, preview: shortText(proxyRes.text, 1500) });
        return;
      }

      const result = proxyRes.json;
      state.jxlSources = Array.isArray(result.sources) ? result.sources : [];
      state.selectedJxlSource = state.jxlSources[0] || null;
      renderJxlList();

      if (state.jxlSources.length) {
        setStatus(`Fant ${state.jxlSources.length} JXL-kilde${state.jxlSources.length === 1 ? "" : "r"}`, "success");
      } else {
        setStatus("Fant ingen JXL-kilder i Connect Explorer eller Field Data", "neutral");
      }
      setDebug(result);
    } catch (err) {
      console.error(err);
      setStatus(`Feil: ${err?.message || String(err)}`, "error");
      setDebug({ action: "refreshJxlSources", error: err?.message || String(err), stack: err?.stack });
    } finally {
      setBusy(false);
    }
  }

  async function processSelectedJxlSource() {
    const source = state.selectedJxlSource;
    if (!source) {
      setStatus("Velg en JXL-kilde først", "error");
      return;
    }

    if (source.sourceType === "connect-file") {
      await processConnectJxlSource(source);
      return;
    }

    await processFieldDataJxl(source.job?.name || source.name || "JXL", source.job?.id || source.id);
  }

  async function processConnectJxlSource(source) {
    try {
      setBusy(true);
      showHint(null, false);
      await ensureReady();
      const file = source.file || source;
      setStatus(`Konverterer JXL-fil: ${file.name}...`, "working");
      const converted = await downloadAndConvertFile(file);
      const uploadResult = await uploadConvertedTxtToProject({
        sourceFile: converted.result.file,
        outName: converted.outName,
        txt: converted.text
      });

      state.lastResult = converted.result;
      state.lastDownloadName = converted.outName;
      state.lastUploadResult = uploadResult;
      if (uploadResult.ok) {
        setStatus(`Ferdig: ${converted.outName} er lastet opp til prosjektet`, "success");
        showHint(buildConversionHint(converted, uploadResult));
      } else {
        triggerDownload(converted.outName, converted.text);
        setStatus(`Ferdig: ${converted.outName} er lastet ned lokalt`, "success");
        showHint(buildConversionHint(converted, uploadResult));
      }
      setDebug({ action: "processConnectJxlSource", source, convertedFile: { name: converted.outName, stats: converted.stats || null }, uploadResult });
    } catch (err) {
      console.error(err);
      setStatus(`Feil: ${err?.message || String(err)}`, "error");
      setDebug({ action: "processConnectJxlSource", error: err?.message || String(err), stack: err?.stack });
    } finally {
      setBusy(false);
    }
  }

  async function processFieldDataJxl(jobName = "JXL to IFC", jobTrn = null) {
    try {
      setBusy(true);
      showHint(null, false);
      await ensureReady();

      setStatus(`Søker Field Data etter "${jobName}"...`, "working");
      const proxyRes = await callProxy("getFieldDataJxl", {
        token: state.accessToken,
        projectId: state.project.id,
        projectLocation: state.project.location,
        jobName,
        jobTrn
      });

      if (!proxyRes.ok || !proxyRes.json) {
        setStatus(`Feil: Proxy svarte med HTTP ${proxyRes.status}`, "error");
        setDebug({ action: "getFieldDataJxl", status: proxyRes.status, preview: shortText(proxyRes.text, 1500) });
        return;
      }

      const result = proxyRes.json;
      if (!result.ok) {
        setStatus(result.error || "Fant ikke Field Data JXL", "error");
        setDebug(result);
        return;
      }

      const jxlName = result.jxlFile?.fileName || `${jobName}.jxl`;
      setStatus(`Konverterer Field Data JXL: ${jxlName}...`, "working");
      const converted = convertKofFile(result.text || "", jxlName);
      const outName = getIfcFilename(jxlName);
      state.lastDownloadName = outName;

      const sourceFile = {
        id: result.jxlFile?.id || result.job?.id || "field-data-jxl",
        name: jxlName,
        parentId: result.uploadParentId || null,
        path: "Field Data"
      };
      state.lastResult = { file: sourceFile, text: result.text || "" };

      let uploadResult = { ok: false, skipped: true, error: "Fant ikke prosjektmappe for automatisk opplasting." };
      if (result.uploadParentId) {
        uploadResult = await uploadConvertedTxtToProject({
          sourceFile,
          outName,
          txt: converted.text
        });
      }
      state.lastUploadResult = uploadResult;

      if (uploadResult.ok) {
        setStatus(`Ferdig: ${outName} er lastet opp til prosjektet`, "success");
        showHint(buildConversionHint({ ...converted, outName }, uploadResult));
      } else {
        triggerDownload(outName, converted.text);
        setStatus(`Ferdig: ${outName} er lastet ned lokalt`, "success");
        showHint(buildConversionHint({ ...converted, outName }, uploadResult));
      }

      setDebug({
        action: "processFieldDataJxl",
        job: result.job,
        jxlFile: result.jxlFile,
        uploadParentId: result.uploadParentId,
        uploadParentSource: result.uploadParentSource,
        appBuild: CONFIG.APP_BUILD,
        convertedFile: { name: outName, format: converted.format, size: converted.text.length, stats: converted.stats || null },
        uploadResult,
        diagnostics: result.diagnostics
      });
    } catch (err) {
      console.error(err);
      setStatus(`Feil: ${err?.message || String(err)}`, "error");
      setDebug({ action: "processFieldDataJxl", error: err?.message || String(err), stack: err?.stack });
    } finally {
      setBusy(false);
    }
  }

  async function processAllFiles(options = {}) {
    try {
      setBusy(true);
      showHint(null, false);
      await ensureReady();

      if (!state.fileList.length) {
        setStatus("Ingen filer i listen - trykk Oppdater liste først", "error");
        return;
      }

      const candidateFiles = Array.isArray(options.files) ? options.files : state.fileList;
      const filesToProcess = options.skipExisting === false
        ? candidateFiles
        : getPendingKofFiles(candidateFiles);
      const skippedCount = candidateFiles.length - filesToProcess.length;

      if (!filesToProcess.length) {
        setStatus("Alle KOF/SOSI/GML-filer har allerede en konvertert fil i samme mappe", "success");
        showHint("Ingen filer ble konvertert pÃ¥ nytt. Slett eksisterende TXT/XML/IFC i Trimble Connect hvis du vil tvinge en ny konvertering.");
        setDebug({
          action: options.source === "auto-open" ? "autoConvertAllOnOpen" : "convertAll",
          total: 0,
          skippedCount,
          skippedFiles: candidateFiles.map((file) => ({
            file: file.name,
            versionId: file.versionId || null,
            revision: file.revision || null,
            modifiedOn: file.modifiedOn || null,
            existingOutputs: (file.existingOutputs || []).map((output) => ({
              name: output.name,
              versionId: output.versionId || null,
              revision: output.revision || null,
              modifiedOn: output.modifiedOn || null
            }))
          }))
        });
        return;
      }

      state.conversionInProgress = true;
      state.cancelConversionRequested = false;
      setBusy(true);

      const summary = [];
      let count = 0;
      let cancelled = false;

      for (const file of filesToProcess) {
        if (state.cancelConversionRequested) {
          cancelled = true;
          break;
        }

        count += 1;
        setStatus(`Konverterer ${count}/${filesToProcess.length}: ${file.name}...`, "working");

        try {
          const converted = await downloadAndConvertFile(file);
          const uploadResult = await uploadConvertedTxtToProject({
            sourceFile: converted.result.file,
            outName: converted.outName,
            txt: converted.text
          });

          if (!uploadResult.ok) {
            triggerDownload(converted.outName, converted.text);
          } else {
            markFileConverted(file, converted.outName);
          }

          summary.push({
            ok: true,
            file: file.name,
            outName: converted.outName,
            format: converted.format,
            uploadOk: !!uploadResult.ok,
            uploadResult
          });
        } catch (err) {
          summary.push({ ok: false, file: file.name, error: err?.message || String(err) });
        }

        if (state.cancelConversionRequested) {
          cancelled = true;
          break;
        }
      }

      const okCount = summary.filter((x) => x.ok).length;
      const failCount = summary.length - okCount;
      const uploadOkCount = summary.filter((x) => x.ok && x.uploadOk).length;
      const localDownloadCount = summary.filter((x) => x.ok && !x.uploadOk).length;
      state.lastDownloadName = okCount === 1 ? summary.find((x) => x.ok)?.outName || null : null;
      state.lastUploadResult = okCount === 1 ? summary.find((x) => x.ok)?.uploadResult || null : null;
      if (!cancelled && options.source === "manual-selected") {
        state.manualSelectedFileIds.clear();
      }
      renderFileList();

      if (cancelled) {
        state.manualSelectionMode = true;
        setStatus(`Stoppet etter ${summary.length} av ${filesToProcess.length} fil${filesToProcess.length === 1 ? "" : "er"}`, "working");
        showHint("Velg en eller flere filer i listen og trykk <strong>Konverter valgte</strong> for å fortsette kontrollert.");
      } else if (failCount === 0) {
        if (uploadOkCount === okCount) {
          setStatus(`Ferdig! ${okCount} fil${okCount === 1 ? "" : "er"} konvertert og lastet opp${skippedCount ? ` (${skippedCount} hoppet over)` : ""}`, "success");
          showHint(
            skippedCount
              ? `Alle nye filer ble lastet opp. ${skippedCount} KOF/SOSI/GML-fil${skippedCount === 1 ? "" : "er"} hadde allerede TXT/XML/IFC i samme mappe og ble ikke konvertert pÃ¥ nytt.`
              : "Alle konverterte filer ble automatisk lastet opp tilbake til samme prosjektmapper i Trimble Connect."
          );
        } else {
          setStatus(`Ferdig! ${okCount} fil${okCount === 1 ? "" : "er"} konvertert og lastet ned${skippedCount ? ` (${skippedCount} hoppet over)` : ""}`, "success");
          showHint(
            okCount === 1
              ? `Automatisk opplasting kom ikke helt i mål. Bruk <strong>Last opp til prosjekt</strong> for å åpne riktig mappe og laste opp <strong>${escapeHtml(state.lastDownloadName || "den konverterte filen")}</strong>.`
              : "Noen automatiske opplastinger kom ikke helt i mål. Bruk <strong>Last opp til prosjekt</strong> for å åpne prosjektmappen og laste opp dem som mangler."
          );
        }
      } else {
        setStatus(`Fullført med ${failCount} feil (${okCount} OK, ${failCount} feilet)`, "error");
      }

      setDebug({
        action: options.source === "auto-open" ? "autoConvertAllOnOpen" : options.source === "manual-selected" ? "convertSelectedManual" : "convertAll",
        total: summary.length,
        cancelled,
        skippedCount,
        okCount,
        failCount,
        uploadOkCount,
        localDownloadCount,
        files: summary
      });
    } catch (err) {
      console.error(err);
      setStatus(`Feil: ${err?.message || String(err)}`, "error");
      setDebug({ error: err?.message || String(err), stack: err?.stack });
    } finally {
      state.conversionInProgress = false;
      state.cancelConversionRequested = false;
      setBusy(false);
    }
  }

  async function processLocalFile(file) {
    try {
      setBusy(true);
      showHint(null, false);

      if (!file) return;

      setStatus(`Konverterer lokal fil ${file.name}...`, "working");
      const kofText = await readSourceFileText(file);
      const converted = convertKofFile(kofText || "", file.name || "output.kof");
      const outName = converted.outName;
      state.lastDownloadName = outName;
      state.lastUploadResult = null;

      triggerDownload(outName, converted.text);
      setStatus(`Ferdig: ${outName} er lastet ned lokalt`, "success");
      showHint(`Lokal fil er konvertert direkte fra maskinen din. Resultatet <strong>${escapeHtml(outName)}</strong> er lastet ned lokalt.`);

      setDebug({
        action: "processLocalFile",
        sourceFile: { name: file.name, size: file.size, type: file.type },
        convertedFile: { name: outName, format: converted.format, size: converted.text.length, stats: converted.stats || null },
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

  async function readSourceFileText(file) {
    if (!file?.arrayBuffer) return file?.text ? await file.text() : "";
    const buffer = await file.arrayBuffer();
    return decodeSourceText(buffer);
  }

  function decodeSourceText(buffer) {
    const utf8 = new TextDecoder("utf-8").decode(buffer);
    if (!utf8.includes("\uFFFD")) return utf8;

    try {
      return repairUtf8Mojibake(new TextDecoder("windows-1252").decode(buffer));
    } catch (_err) {
      return utf8;
    }
  }

  function repairUtf8Mojibake(text) {
    return String(text || "")
      .replace(/\u00c3[\u0098\u02dc]/g, "Ø")
      .replace(/\u00c3\u00b8/g, "ø")
      .replace(/\u00c3[\u0085\u2026]/g, "Å")
      .replace(/\u00c3\u00a5/g, "å")
      .replace(/\u00c3\u0086/g, "Æ")
      .replace(/\u00c3\u00a6/g, "æ")
      .replace(/Ã˜/g, "Ø")
      .replace(/Ã¸/g, "ø")
      .replace(/Ã…/g, "Å")
      .replace(/Ã¥/g, "å")
      .replace(/Ã†/g, "Æ")
      .replace(/Ã¦/g, "æ");
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
    ui.converterTabBtn.addEventListener("click", () => switchView("converter"));
    ui.jxlTabBtn.addEventListener("click", () => {
      switchView("jxl");
      if (!state.jxlSources.length) refreshJxlSources().catch(() => {});
    });
    ui.refreshBtn.addEventListener("click", () => refreshKofListOnOpen("manual-refresh"));
    ui.stopBtn.addEventListener("click", requestStopConversion);
    ui.convertManualBtn.addEventListener("click", processManualSelectedFiles);
    ui.jxlRefreshBtn.addEventListener("click", refreshJxlSources);
    ui.jxlConvertBtn.addEventListener("click", processSelectedJxlSource);
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
      renderJxlList();
      switchView("converter");

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
        processManualSelectedFiles,
        refreshJxlSources,
        processSelectedJxlSource,
        processFieldDataJxl,
        processAllFiles,
        requestStopConversion,
        processLocalFile,
        parseJxlForIfc,
        jxlToIfc,
        openProjectUploadExplorer,
        uploadConvertedTxtToProject,
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
