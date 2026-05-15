const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");
const crypto = require("crypto");

const SERVER_ID = process.env.SERVER_ID || "js_server_1";
const BACKEND_ENDPOINT = process.env.BACKEND_ENDPOINT || "tcp://broker:5556";
const PUBSUB_PROXY_IN_ENDPOINT = process.env.PUBSUB_PROXY_IN_ENDPOINT || "tcp://pubsub_proxy:5557";
const PUBSUB_PROXY_OUT_ENDPOINT = process.env.PUBSUB_PROXY_OUT_ENDPOINT || "tcp://pubsub_proxy:5558";
const REFERENCE_ENDPOINT = process.env.REFERENCE_ENDPOINT || "tcp://reference_service:5560";
const DATA_FILE = process.env.DATA_FILE || "/data/state.json";

const REFERENCE_TIMEOUT_MS = Number(process.env.REFERENCE_TIMEOUT_MS || "5000");
const HEARTBEAT_EVERY_CLIENT_MESSAGES = Number(process.env.HEARTBEAT_EVERY_CLIENT_MESSAGES || "10");
const CLOCK_SYNC_EVERY_MESSAGES = Number(process.env.CLOCK_SYNC_EVERY_MESSAGES || "15");
const SERVER_RPC_TIMEOUT_MS = Number(process.env.SERVER_RPC_TIMEOUT_MS || "2500");
const SERVER_RPC_PORT = Number(process.env.SERVER_RPC_PORT || "5570");
const STATE_SYNC_INITIAL_DELAY_SECONDS = Number(process.env.STATE_SYNC_INITIAL_DELAY_SECONDS || "1.5");
const STATE_SYNC_MAX_ATTEMPTS = Number(process.env.STATE_SYNC_MAX_ATTEMPTS || "3");

const SERVER_ORDER = (process.env.SERVER_ORDER || "js_server_1,js_server_2,py_server_1,py_server_2")
  .split(",")
  .map((item) => item.trim())
  .filter(Boolean);

const SERVER_ENDPOINTS = Object.fromEntries(
  SERVER_ORDER.map((serverId) => [serverId, `tcp://${serverId}:${SERVER_RPC_PORT}`])
);

const USERNAME_REGEX = /^[A-Za-z0-9_-]{3,20}$/;
const CHANNEL_REGEX = /^[A-Za-z0-9_-]{3,30}$/;

const CLIENT_REQUEST_TYPES = new Set(["LOGIN", "LIST_CHANNELS", "CREATE_CHANNEL", "PUBLISH_MESSAGE"]);
const BROKER_REQUEST_TYPES = new Set(["LOGIN", "LIST_CHANNELS", "CREATE_CHANNEL", "PUBLISH_MESSAGE", "SYNC_PUBLICATION"]);
const SERVERS_TOPIC = "servers";

let logicalClock = 0;
let physicalClockOffsetMs = 0;
let serverRank = null;
let clientMessageCount = 0;
let exchangedMessageCount = 0;
let currentCoordinatorId = SERVER_ORDER[0] || SERVER_ID;

function tickLogicalClock() {
  logicalClock += 1;
  return logicalClock;
}

function updateLogicalClockFromMessage(message) {
  if (!message || typeof message !== "object") {
    return logicalClock;
  }

  const received = Number(message.logical_clock);

  if (Number.isInteger(received)) {
    logicalClock = Math.max(logicalClock, received);
  }

  return logicalClock;
}

function correctedNowDate() {
  return new Date(Date.now() + physicalClockOffsetMs);
}

function correctedNowIso() {
  return correctedNowDate().toISOString();
}

function correctedEpochMs() {
  return correctedNowDate().getTime();
}

function serverRankFor(serverId) {
  const index = SERVER_ORDER.indexOf(serverId);
  return index >= 0 ? index + 1 : 9999;
}

function getCurrentCoordinator() {
  return currentCoordinatorId;
}

function setCurrentCoordinator(coordinatorId, source) {
  const normalizedCoordinatorId = String(coordinatorId || "").trim();
  if (!normalizedCoordinatorId) {
    return;
  }

  const previous = currentCoordinatorId;
  currentCoordinatorId = normalizedCoordinatorId;

  if (previous !== normalizedCoordinatorId) {
    console.log(
      `[${SERVER_ID}][COORDINATOR_UPDATE] coordenador=${normalizedCoordinatorId} anterior=${previous} origem=${source}`
    );
  }
}

function updatePhysicalClockFromCoordinator(coordinatorEpochMs, coordinatorId) {
  const epoch = Number(coordinatorEpochMs);

  if (!Number.isInteger(epoch)) {
    return;
  }

  physicalClockOffsetMs = epoch - Date.now();
  console.log(`[${SERVER_ID}][CLOCK_SYNC_FROM_COORDINATOR] coordenador=${coordinatorId} offset_fisico_ms=${physicalClockOffsetMs}`);
}

function logMessage(direction, message) {
  console.log(`[${SERVER_ID}][${direction}] ${JSON.stringify(message)}`);
}

function ensureParent(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function defaultState() {
  return {
    server_id: SERVER_ID,
    server_rank: serverRank,
    current_coordinator_id: getCurrentCoordinator(),
    users: [],
    logins: [],
    channels: ["geral"],
    requests: [],
    publications: [],
    heartbeats: [],
    clock_syncs: [],
    elections: [],
    state_syncs: []
  };
}

function saveState(state) {
  ensureParent(DATA_FILE);
  state.server_id = SERVER_ID;
  state.server_rank = serverRank;
  state.current_coordinator_id = getCurrentCoordinator();

  const tempFile = `${DATA_FILE}.tmp`;
  fs.writeFileSync(tempFile, JSON.stringify(state, null, 2), "utf8");
  fs.renameSync(tempFile, DATA_FILE);
}

function loadState() {
  ensureParent(DATA_FILE);

  if (!fs.existsSync(DATA_FILE)) {
    const initialState = defaultState();
    saveState(initialState);
    return initialState;
  }

  try {
    const raw = fs.readFileSync(DATA_FILE, "utf8");
    const state = JSON.parse(raw);

    state.server_id = state.server_id || SERVER_ID;
    state.server_rank = state.server_rank || serverRank;
    state.current_coordinator_id = state.current_coordinator_id || getCurrentCoordinator();
    state.users = Array.isArray(state.users) ? state.users : [];
    state.logins = Array.isArray(state.logins) ? state.logins : [];
    state.channels = Array.isArray(state.channels) ? state.channels : ["geral"];
    state.requests = Array.isArray(state.requests) ? state.requests : [];
    state.publications = Array.isArray(state.publications) ? state.publications : [];
    state.heartbeats = Array.isArray(state.heartbeats) ? state.heartbeats : [];
    state.clock_syncs = Array.isArray(state.clock_syncs) ? state.clock_syncs : [];
    state.elections = Array.isArray(state.elections) ? state.elections : [];
    state.state_syncs = Array.isArray(state.state_syncs) ? state.state_syncs : [];

    if (!state.channels.includes("geral")) {
      state.channels.push("geral");
    }

    return state;
  } catch (error) {
    const corruptFile = `${DATA_FILE}.corrupt-${Date.now()}`;
    fs.renameSync(DATA_FILE, corruptFile);
    console.log(`[${SERVER_ID}][STATE] state.json corrompido. Backup criado em ${corruptFile}. Novo estado iniciado.`);

    const initialState = defaultState();
    saveState(initialState);
    return initialState;
  }
}

function persistRequest(state, request, processedAt) {
  state.requests.push({
    request_id: request.request_id,
    request_type: request.type,
    username: request.username,
    channel: request.channel,
    message: request.message,
    origin: request.origin,
    client_timestamp: request.timestamp,
    client_logical_clock: request.logical_clock,
    server_processed_at: processedAt,
    server_logical_clock_after_receive: logicalClock,
    current_coordinator_id: getCurrentCoordinator()
  });
}

function publicationExists(state, publicationId) {
  return state.publications.some((publication) => publication.publication_id === publicationId);
}

function persistPublication(state, publication) {
  if (publication.publication_id && publicationExists(state, publication.publication_id)) {
    return;
  }

  state.publications.push(publication);
}

function buildStateSnapshot(state) {
  return {
    source_server_id: SERVER_ID,
    source_server_rank: serverRank,
    generated_at: correctedNowIso(),
    users: Array.from(state.users || []),
    channels: Array.from(state.channels || []),
    logins: Array.from(state.logins || []),
    publications: Array.from(state.publications || [])
  };
}

function mergeUniqueItemsByKey(localItems, remoteItems, keyName) {
  const merged = Array.isArray(localItems) ? [...localItems] : [];
  const existingKeys = new Set(
    merged
      .filter((item) => item && typeof item === "object" && item[keyName])
      .map((item) => item[keyName])
  );

  let addedCount = 0;

  for (const item of Array.isArray(remoteItems) ? remoteItems : []) {
    if (!item || typeof item !== "object") {
      continue;
    }

    const itemKey = item[keyName];
    if (!itemKey || existingKeys.has(itemKey)) {
      continue;
    }

    merged.push(item);
    existingKeys.add(itemKey);
    addedCount += 1;
  }

  return { merged, addedCount };
}

function mergeLogins(localLogins, remoteLogins) {
  const merged = Array.isArray(localLogins) ? [...localLogins] : [];
  const existingKeys = new Set(
    merged
      .filter((item) => item && typeof item === "object")
      .map((item) => `${item.username || ""}|${item.timestamp || ""}|${item.server_processed_at || ""}`)
  );

  let addedCount = 0;

  for (const item of Array.isArray(remoteLogins) ? remoteLogins : []) {
    if (!item || typeof item !== "object") {
      continue;
    }

    const itemKey = `${item.username || ""}|${item.timestamp || ""}|${item.server_processed_at || ""}`;
    if (existingKeys.has(itemKey)) {
      continue;
    }

    merged.push(item);
    existingKeys.add(itemKey);
    addedCount += 1;
  }

  return { merged, addedCount };
}

function mergeStateSnapshot(localState, snapshot) {
  const remoteUsers = Array.isArray(snapshot.users) ? snapshot.users : [];
  const remoteChannels = Array.isArray(snapshot.channels) ? snapshot.channels : [];
  const remoteLogins = Array.isArray(snapshot.logins) ? snapshot.logins : [];
  const remotePublications = Array.isArray(snapshot.publications) ? snapshot.publications : [];

  const usersBefore = Array.isArray(localState.users) ? localState.users.length : 0;
  const channelsBefore = Array.isArray(localState.channels) ? localState.channels.length : 0;

  localState.users = Array.from(new Set([...(localState.users || []), ...remoteUsers])).sort();
  localState.channels = Array.from(new Set([...(localState.channels || []), ...remoteChannels, "geral"])).sort();

  const loginMerge = mergeLogins(localState.logins || [], remoteLogins);
  localState.logins = loginMerge.merged;

  const publicationMerge = mergeUniqueItemsByKey(
    localState.publications || [],
    remotePublications,
    "publication_id"
  );
  localState.publications = publicationMerge.merged;

  return {
    added_users: localState.users.length - usersBefore,
    added_channels: localState.channels.length - channelsBefore,
    added_logins: loginMerge.addedCount,
    added_publications: publicationMerge.addedCount
  };
}

async function synchronizeReplicatedStateFromPeers() {
  // Parte 5: quando um servidor sobe ou retorna após falha, ele consulta os outros
  // servidores ativos e mescla usuários, canais e publicações. Isso reforça a
  // replicação feita pelo broker via SYNC_PUBLICATION.
  await new Promise((resolve) => setTimeout(resolve, STATE_SYNC_INITIAL_DELAY_SECONDS * 1000));

  for (let attempt = 1; attempt <= STATE_SYNC_MAX_ATTEMPTS; attempt += 1) {
    let totalAddedPublications = 0;
    const syncedFrom = [];

    for (const otherServerId of SERVER_ORDER) {
      if (otherServerId === SERVER_ID) {
        continue;
      }

      try {
        const response = await serverRpcRequest(otherServerId, "STATE_SNAPSHOT_REQUEST", {
          reason: `state_sync_attempt:${attempt}`
        });

        if (response.status !== "OK" || !response.snapshot || typeof response.snapshot !== "object") {
          continue;
        }

        const state = loadState();
        const mergeResult = mergeStateSnapshot(state, response.snapshot);

        state.state_syncs.push({
          timestamp: correctedNowIso(),
          logical_clock: logicalClock,
          attempt,
          source_server_id: response.snapshot.source_server_id || otherServerId,
          merge_result: mergeResult
        });

        saveState(state);

        totalAddedPublications += mergeResult.added_publications;
        syncedFrom.push(otherServerId);

        console.log(
          `[${SERVER_ID}][STATE_SYNC] origem=${otherServerId} publicacoes_adicionadas=${mergeResult.added_publications}`
        );
      } catch (error) {
        console.log(
          `[${SERVER_ID}][STATE_SYNC] tentativa=${attempt} servidor=${otherServerId} indisponível motivo=${error.message}`
        );
      }
    }

    if (syncedFrom.length > 0) {
      console.log(
        `[${SERVER_ID}][STATE_SYNC_DONE] tentativa=${attempt} sincronizado_de=${JSON.stringify(syncedFrom)} novas_publicacoes=${totalAddedPublications}`
      );
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  console.log(`[${SERVER_ID}][STATE_SYNC_DONE] nenhum snapshot disponível no momento`);
}

async function referenceRequest(type, extra = {}) {
  const socket = new zmq.Request();
  socket.connect(REFERENCE_ENDPOINT);

  const request = {
    type,
    request_id: crypto.randomUUID(),
    server_id: SERVER_ID,
    timestamp: correctedNowIso(),
    logical_clock: tickLogicalClock(),
    ...extra
  };

  logMessage("REFERENCE_SEND", request);
  await socket.send(encode(request));

  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => reject(new Error(`Timeout ao consultar reference_service para ${type}.`)), REFERENCE_TIMEOUT_MS);
  });

  const receivePromise = (async () => {
    const [payload] = await socket.receive();
    return decode(payload);
  })();

  try {
    const response = await Promise.race([receivePromise, timeoutPromise]);
    updateLogicalClockFromMessage(response);
    logMessage("REFERENCE_RECV", response);
    socket.close();
    return response;
  } catch (error) {
    socket.close();
    throw error;
  }
}

async function registerRankWithReference() {
  for (let attempt = 1; attempt <= 10; attempt += 1) {
    try {
      const response = await referenceRequest("GET_RANK");

      if (response.status === "OK") {
        serverRank = Number(response.rank);
        console.log(`[${SERVER_ID}] Rank recebido do serviço de referência: ${serverRank}`);
        return serverRank;
      }
    } catch (error) {
      console.log(`[${SERVER_ID}] Tentativa ${attempt}/10 falhou ao obter rank: ${error.message}`);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }

  throw new Error("Não foi possível obter rank no serviço de referência.");
}

async function serverRpcRequest(targetServerId, type, extra = {}) {
  const endpoint = SERVER_ENDPOINTS[targetServerId];

  if (!endpoint) {
    throw new Error(`Endpoint não configurado para servidor '${targetServerId}'.`);
  }

  const socket = new zmq.Request();
  socket.connect(endpoint);

  const request = {
    type,
    request_id: crypto.randomUUID(),
    server_id: SERVER_ID,
    server_rank: serverRank,
    timestamp: correctedNowIso(),
    logical_clock: tickLogicalClock(),
    ...extra
  };

  logMessage(`SERVER_RPC_SEND:${targetServerId}`, request);
  await socket.send(encode(request));

  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => reject(new Error(`Timeout ao consultar servidor ${targetServerId} para ${type}.`)), SERVER_RPC_TIMEOUT_MS);
  });

  const receivePromise = (async () => {
    const [payload] = await socket.receive();
    return decode(payload);
  })();

  try {
    const response = await Promise.race([receivePromise, timeoutPromise]);
    updateLogicalClockFromMessage(response);
    logMessage(`SERVER_RPC_RECV:${targetServerId}`, response);
    socket.close();
    return response;
  } catch (error) {
    socket.close();
    throw error;
  }
}

async function publishCoordinatorAnnouncement(pubSocket, coordinatorId, reason) {
  const announcement = {
    type: "COORDINATOR_ANNOUNCEMENT",
    coordinator_id: coordinatorId,
    coordinator_rank: serverRankFor(coordinatorId),
    announcer_id: SERVER_ID,
    announcer_rank: serverRank,
    reason,
    timestamp: correctedNowIso(),
    logical_clock: tickLogicalClock()
  };

  await pubSocket.send([Buffer.from(SERVERS_TOPIC), encode(announcement)]);
  logMessage("PUB:servers", announcement);
}

async function sendHeartbeatIfNeeded() {
  if (clientMessageCount === 0) {
    return;
  }

  if (clientMessageCount % HEARTBEAT_EVERY_CLIENT_MESSAGES !== 0) {
    return;
  }

  try {
    const response = await referenceRequest("HEARTBEAT", {
      processed_client_messages: clientMessageCount,
      current_coordinator_id: getCurrentCoordinator()
    });

    const state = loadState();
    state.heartbeats.push({
      timestamp: correctedNowIso(),
      logical_clock: logicalClock,
      processed_client_messages: clientMessageCount,
      reference_status: response.status,
      active_servers: response.active_servers || [],
      current_coordinator_id: getCurrentCoordinator()
    });
    saveState(state);
  } catch (error) {
    console.log(`[${SERVER_ID}][HEARTBEAT] Falha ao enviar heartbeat: ${error.message}`);
  }
}

function internalOkResponse(request, extra = {}) {
  return {
    type: "SERVER_INTERNAL_REPLY",
    request_id: request.request_id,
    request_type: request.type,
    status: "OK",
    server_id: SERVER_ID,
    server_rank: serverRank,
    current_coordinator_id: getCurrentCoordinator(),
    timestamp: correctedNowIso(),
    logical_clock: tickLogicalClock(),
    ...extra
  };
}

function internalErrorResponse(request, error) {
  return {
    type: "SERVER_INTERNAL_REPLY",
    request_id: request.request_id,
    request_type: request.type,
    status: "ERROR",
    server_id: SERVER_ID,
    server_rank: serverRank,
    current_coordinator_id: getCurrentCoordinator(),
    timestamp: correctedNowIso(),
    logical_clock: tickLogicalClock(),
    error
  };
}

function handleInternalRequest(request) {
  updateLogicalClockFromMessage(request);
  exchangedMessageCount += 1;

  switch (request.type) {
    case "CLOCK_REQUEST":
      if (getCurrentCoordinator() !== SERVER_ID) {
        return internalErrorResponse(
          request,
          `Servidor '${SERVER_ID}' não é o coordenador atual. Coordenador conhecido: ${getCurrentCoordinator()}`
        );
      }

      return internalOkResponse(request, {
        coordinator_id: SERVER_ID,
        coordinator_epoch_ms: correctedEpochMs()
      });

    case "ELECTION_REQUEST":
      return internalOkResponse(request, {
        election_response: "OK"
      });

    case "COORDINATOR_NOTIFICATION": {
      const coordinatorId = String(request.coordinator_id || "").trim();
      if (coordinatorId) {
        setCurrentCoordinator(coordinatorId, `direct_notification_from_${request.server_id}`);
      }
      return internalOkResponse(request, { coordinator_id: getCurrentCoordinator() });
    }

    case "STATE_SNAPSHOT_REQUEST": {
      const state = loadState();
      return internalOkResponse(request, { snapshot: buildStateSnapshot(state) });
    }

    default:
      return internalErrorResponse(request, `Tipo de requisição interna inválido: ${request.type}`);
  }
}

async function startInternalRpcServer() {
  const socket = new zmq.Reply();
  await socket.bind(`tcp://*:${SERVER_RPC_PORT}`);

  console.log(`[${SERVER_ID}][SERVER_RPC] Escutando em tcp://*:${SERVER_RPC_PORT}`);

  for await (const [payload] of socket) {
    const request = decode(payload);
    logMessage("SERVER_RPC_RECV", request);

    const response = handleInternalRequest(request);
    logMessage("SERVER_RPC_SEND", response);
    await socket.send(encode(response));
  }
}

async function startServersSubscription() {
  const subscriber = new zmq.Subscriber();
  subscriber.connect(PUBSUB_PROXY_OUT_ENDPOINT);
  subscriber.subscribe(SERVERS_TOPIC);

  console.log(`[${SERVER_ID}][SUBSCRIBE] Inscrito no tópico '${SERVERS_TOPIC}'`);

  for await (const [topicBuffer, payload] of subscriber) {
    const message = decode(payload);
    updateLogicalClockFromMessage(message);
    logMessage("PUBSUB_RECV:servers", message);

    if (message.type === "COORDINATOR_ANNOUNCEMENT") {
      const coordinatorId = String(message.coordinator_id || "").trim();
      if (coordinatorId) {
        setCurrentCoordinator(coordinatorId, `pubsub_from_${message.announcer_id}`);
      }
    }
  }
}

function startBackgroundServices() {
  startInternalRpcServer().catch((error) => {
    console.error(`[${SERVER_ID}][SERVER_RPC] Erro fatal:`, error);
    process.exit(1);
  });

  startServersSubscription().catch((error) => {
    console.error(`[${SERVER_ID}][SUBSCRIBE] Erro fatal:`, error);
    process.exit(1);
  });
}

async function electCoordinator(pubSocket, reason) {
  const candidates = [[SERVER_ID, serverRank || serverRankFor(SERVER_ID)]];

  console.log(`[${SERVER_ID}][ELECTION_START] motivo=${reason}`);

  for (const otherServerId of SERVER_ORDER) {
    if (otherServerId === SERVER_ID) {
      continue;
    }

    try {
      const response = await serverRpcRequest(otherServerId, "ELECTION_REQUEST", {
        reason,
        known_coordinator_id: getCurrentCoordinator()
      });

      if (response.status === "OK") {
        candidates.push([
          String(response.server_id || otherServerId),
          Number(response.server_rank || serverRankFor(otherServerId))
        ]);
      }
    } catch (error) {
      console.log(`[${SERVER_ID}][ELECTION] servidor=${otherServerId} indisponível motivo=${error.message}`);
    }
  }

  const [electedId, electedRank] = candidates.sort((a, b) => a[1] - b[1])[0];
  setCurrentCoordinator(electedId, "election_result");
  await publishCoordinatorAnnouncement(pubSocket, electedId, reason);

  const state = loadState();
  state.elections.push({
    timestamp: correctedNowIso(),
    logical_clock: logicalClock,
    reason,
    candidates: candidates.map((item) => ({ server_id: item[0], rank: item[1] })),
    elected_id: electedId,
    elected_rank: electedRank
  });
  saveState(state);

  console.log(`[${SERVER_ID}][ELECTION_RESULT] coordenador=${electedId} rank=${electedRank}`);
  return electedId;
}

async function synchronizeClockWithCoordinator(pubSocket, reason) {
  const coordinatorId = getCurrentCoordinator();

  if (coordinatorId === SERVER_ID) {
    console.log(`[${SERVER_ID}][BERKELEY_COORDINATOR] servidor_atual_eh_coordenador mensagem=${reason}`);
    return;
  }

  try {
    const response = await serverRpcRequest(coordinatorId, "CLOCK_REQUEST", { reason });

    if (response.status !== "OK") {
      throw new Error(response.error || "Resposta inválida do coordenador.");
    }

    const coordinatorEpochMs = Number(response.coordinator_epoch_ms);

    if (!Number.isInteger(coordinatorEpochMs)) {
      throw new Error("Coordenador não retornou coordinator_epoch_ms inteiro.");
    }

    updatePhysicalClockFromCoordinator(coordinatorEpochMs, coordinatorId);

    const state = loadState();
    state.clock_syncs.push({
      timestamp: correctedNowIso(),
      logical_clock: logicalClock,
      reason,
      coordinator_id: coordinatorId,
      coordinator_epoch_ms: coordinatorEpochMs,
      status: "OK"
    });
    saveState(state);

    console.log(`[${SERVER_ID}][BERKELEY_SYNC] coordenador=${coordinatorId} status=OK`);
  } catch (error) {
    console.log(`[${SERVER_ID}][COORDINATOR_UNAVAILABLE] coordenador=${coordinatorId} motivo=${error.message}`);
    const electedId = await electCoordinator(pubSocket, `coordenador_indisponivel:${coordinatorId}`);

    if (electedId !== SERVER_ID) {
      try {
        const response = await serverRpcRequest(electedId, "CLOCK_REQUEST", {
          reason: `apos_eleicao:${reason}`
        });

        const coordinatorEpochMs = Number(response.coordinator_epoch_ms);
        if (response.status === "OK" && Number.isInteger(coordinatorEpochMs)) {
          updatePhysicalClockFromCoordinator(coordinatorEpochMs, electedId);
          console.log(`[${SERVER_ID}][BERKELEY_SYNC] coordenador=${electedId} status=OK`);
        }
      } catch (syncError) {
        console.log(`[${SERVER_ID}][BERKELEY_SYNC] falha_apos_eleicao coordenador=${electedId} motivo=${syncError.message}`);
      }
    }
  }
}

async function maybeSyncClock(pubSocket) {
  if (exchangedMessageCount === 0) {
    return;
  }

  if (exchangedMessageCount % CLOCK_SYNC_EVERY_MESSAGES !== 0) {
    return;
  }

  await synchronizeClockWithCoordinator(pubSocket, `mensagens_trocadas:${exchangedMessageCount}`);
}

function okResponse(request, extra = {}) {
  return {
    type: "SERVER_RESULT",
    request_id: request.request_id,
    timestamp: correctedNowIso(),
    logical_clock: tickLogicalClock(),
    status: "OK",
    server_id: SERVER_ID,
    server_rank: serverRank,
    current_coordinator_id: getCurrentCoordinator(),
    request_type: request.type,
    ...extra
  };
}

function errorResponse(request, error) {
  return {
    type: "SERVER_RESULT",
    request_id: request.request_id,
    timestamp: correctedNowIso(),
    logical_clock: tickLogicalClock(),
    status: "ERROR",
    server_id: SERVER_ID,
    server_rank: serverRank,
    current_coordinator_id: getCurrentCoordinator(),
    request_type: request.type,
    error
  };
}

function handleLogin(request) {
  const username = String(request.username || "").trim();

  if (!USERNAME_REGEX.test(username)) {
    return errorResponse(
      request,
      "Nome de usuário inválido. Use 3 a 20 caracteres com letras, números, _ ou -."
    );
  }

  const state = loadState();
  const processedAt = correctedNowIso();
  persistRequest(state, request, processedAt);

  if (state.users.includes(username)) {
    saveState(state);
    return errorResponse(request, `Usuário '${username}' já existe.`);
  }

  state.users = Array.from(new Set([...state.users, username])).sort();
  state.logins.push({
    username,
    timestamp: request.timestamp || processedAt,
    client_logical_clock: request.logical_clock,
    server_processed_at: processedAt,
    server_logical_clock: logicalClock
  });

  saveState(state);
  return okResponse(request, { username });
}

function handleListChannels(request) {
  const state = loadState();
  const processedAt = correctedNowIso();
  persistRequest(state, request, processedAt);
  saveState(state);

  const channels = Array.from(new Set(state.channels)).sort();
  return okResponse(request, { channels });
}

function handleCreateChannel(request) {
  const channel = String(request.channel || "").trim();

  if (!CHANNEL_REGEX.test(channel)) {
    return errorResponse(
      request,
      "Nome de canal inválido. Use 3 a 30 caracteres com letras, números, _ ou -."
    );
  }

  const state = loadState();
  const processedAt = correctedNowIso();
  persistRequest(state, request, processedAt);

  if (state.channels.includes(channel)) {
    saveState(state);
    return errorResponse(request, `Canal '${channel}' já existe.`);
  }

  state.channels = Array.from(new Set([...state.channels, channel])).sort();
  saveState(state);

  return okResponse(request, { channel });
}

function validatePublicationRequest(state, request) {
  const username = String(request.username || "").trim();
  const channel = String(request.channel || "").trim();
  const messageText = String(request.message || "").trim();

  if (!state.users.includes(username)) {
    return `Usuário '${username}' não está cadastrado no servidor.`;
  }

  if (!state.channels.includes(channel)) {
    return `Canal '${channel}' não existe.`;
  }

  if (!messageText) {
    return "A mensagem não pode ser vazia.";
  }

  return "";
}

function buildPublication(request, publishedAt) {
  return {
    type: "CHANNEL_MESSAGE",
    publication_id: request.request_id,
    channel: request.channel,
    message: request.message,
    username: request.username,
    origin: request.origin,
    timestamp: publishedAt,
    logical_clock: tickLogicalClock(),
    client_timestamp: request.timestamp,
    client_logical_clock: request.logical_clock,
    server_id: SERVER_ID,
    server_rank: serverRank,
    current_coordinator_id: getCurrentCoordinator()
  };
}

async function handlePublishMessage(request, pubSocket) {
  const state = loadState();
  const processedAt = correctedNowIso();
  persistRequest(state, request, processedAt);

  const validationError = validatePublicationRequest(state, request);
  if (validationError) {
    saveState(state);
    return errorResponse(request, validationError);
  }

  const publication = buildPublication(request, processedAt);
  persistPublication(state, publication);
  saveState(state);

  await pubSocket.send([Buffer.from(String(publication.channel)), encode(publication)]);
  logMessage("PUB", publication);

  return okResponse(request, {
    channel: publication.channel,
    message: publication.message,
    publication
  });
}

function handleSyncPublication(request) {
  const publication = request.publication;

  if (!publication || typeof publication !== "object") {
    return errorResponse(request, "Publicação inválida para sincronização.");
  }

  updateLogicalClockFromMessage(publication);

  const state = loadState();
  const processedAt = correctedNowIso();
  persistRequest(state, request, processedAt);
  persistPublication(state, publication);
  saveState(state);

  return okResponse(request, { publication_id: publication.publication_id });
}

async function handleRequest(request, pubSocket) {
  updateLogicalClockFromMessage(request);

  if (BROKER_REQUEST_TYPES.has(request.type)) {
    exchangedMessageCount += 1;
  }

  if (CLIENT_REQUEST_TYPES.has(request.type)) {
    clientMessageCount += 1;
    await sendHeartbeatIfNeeded();
  }

  switch (request.type) {
    case "LOGIN":
      return handleLogin(request);
    case "LIST_CHANNELS":
      return handleListChannels(request);
    case "CREATE_CHANNEL":
      return handleCreateChannel(request);
    case "PUBLISH_MESSAGE":
      return handlePublishMessage(request, pubSocket);
    case "SYNC_PUBLICATION":
      return handleSyncPublication(request);
    default:
      return errorResponse(request, `Tipo de requisição inválido: ${request.type}`);
  }
}

async function main() {
  await registerRankWithReference();
  startBackgroundServices();
  await synchronizeReplicatedStateFromPeers();

  const rpcSocket = new zmq.Dealer({ routingId: SERVER_ID });
  rpcSocket.connect(BACKEND_ENDPOINT);

  const pubSocket = new zmq.Publisher();
  pubSocket.connect(PUBSUB_PROXY_IN_ENDPOINT);

  await new Promise((resolve) => setTimeout(resolve, 500));

  if (serverRank === 1) {
    setCurrentCoordinator(SERVER_ID, "initial_rank_1");
    await publishCoordinatorAnnouncement(pubSocket, SERVER_ID, "initial_rank_1");
  }

  const registerMessage = {
    type: "REGISTER_SERVER",
    server_id: SERVER_ID,
    server_rank: serverRank,
    current_coordinator_id: getCurrentCoordinator(),
    timestamp: correctedNowIso(),
    logical_clock: tickLogicalClock()
  };

  logMessage("SEND", registerMessage);
  await rpcSocket.send(encode(registerMessage));

  for await (const [payload] of rpcSocket) {
    const request = decode(payload);
    logMessage("RECV", request);

    const response = await handleRequest(request, pubSocket);
    logMessage("SEND", response);
    await rpcSocket.send(encode(response));

    await maybeSyncClock(pubSocket);
  }
}

main().catch((error) => {
  console.error(`[${SERVER_ID}] Erro fatal:`, error);
  process.exit(1);
});