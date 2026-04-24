const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");
const crypto = require("crypto");

const SERVER_ID = process.env.SERVER_ID || "js_server_1";
const BACKEND_ENDPOINT = process.env.BACKEND_ENDPOINT || "tcp://broker:5556";
const PUBSUB_PROXY_IN_ENDPOINT = process.env.PUBSUB_PROXY_IN_ENDPOINT || "tcp://pubsub_proxy:5557";
const REFERENCE_ENDPOINT = process.env.REFERENCE_ENDPOINT || "tcp://reference_service:5560";
const DATA_FILE = process.env.DATA_FILE || "/data/state.json";

const REFERENCE_TIMEOUT_MS = Number(process.env.REFERENCE_TIMEOUT_MS || "5000");
const HEARTBEAT_EVERY_CLIENT_MESSAGES = Number(process.env.HEARTBEAT_EVERY_CLIENT_MESSAGES || "10");

const USERNAME_REGEX = /^[A-Za-z0-9_-]{3,20}$/;
const CHANNEL_REGEX = /^[A-Za-z0-9_-]{3,30}$/;

const CLIENT_REQUEST_TYPES = new Set(["LOGIN", "LIST_CHANNELS", "CREATE_CHANNEL", "PUBLISH_MESSAGE"]);

let logicalClock = 0;
let physicalClockOffsetMs = 0;
let serverRank = null;
let clientMessageCount = 0;

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

function updatePhysicalClockFromReference(response) {
  const referenceMs = Number(response.reference_timestamp_epoch_ms);

  if (!Number.isInteger(referenceMs)) {
    return;
  }

  physicalClockOffsetMs = referenceMs - Date.now();
  console.log(`[${SERVER_ID}][CLOCK_SYNC] offset_fisico_ms=${physicalClockOffsetMs}`);
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
    users: [],
    logins: [],
    channels: ["geral"],
    requests: [],
    publications: [],
    heartbeats: []
  };
}

function saveState(state) {
  ensureParent(DATA_FILE);
  state.server_id = SERVER_ID;
  state.server_rank = serverRank;

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
    state.users = Array.isArray(state.users) ? state.users : [];
    state.logins = Array.isArray(state.logins) ? state.logins : [];
    state.channels = Array.isArray(state.channels) ? state.channels : ["geral"];
    state.requests = Array.isArray(state.requests) ? state.requests : [];
    state.publications = Array.isArray(state.publications) ? state.publications : [];
    state.heartbeats = Array.isArray(state.heartbeats) ? state.heartbeats : [];

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
    server_logical_clock_after_receive: logicalClock
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
    updatePhysicalClockFromReference(response);
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

async function sendHeartbeatIfNeeded() {
  if (clientMessageCount === 0) {
    return;
  }

  if (clientMessageCount % HEARTBEAT_EVERY_CLIENT_MESSAGES !== 0) {
    return;
  }

  try {
    const response = await referenceRequest("HEARTBEAT", {
      processed_client_messages: clientMessageCount
    });

    const state = loadState();
    state.heartbeats.push({
      timestamp: correctedNowIso(),
      logical_clock: logicalClock,
      processed_client_messages: clientMessageCount,
      reference_status: response.status,
      active_servers: response.active_servers || []
    });
    saveState(state);
  } catch (error) {
    console.log(`[${SERVER_ID}][HEARTBEAT] Falha ao enviar heartbeat: ${error.message}`);
  }
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
    server_rank: serverRank
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

  const rpcSocket = new zmq.Dealer({ routingId: SERVER_ID });
  rpcSocket.connect(BACKEND_ENDPOINT);

  const pubSocket = new zmq.Publisher();
  pubSocket.connect(PUBSUB_PROXY_IN_ENDPOINT);

  const registerMessage = {
    type: "REGISTER_SERVER",
    server_id: SERVER_ID,
    server_rank: serverRank,
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
  }
}

main().catch((error) => {
  console.error(`[${SERVER_ID}] Erro fatal:`, error);
  process.exit(1);
});