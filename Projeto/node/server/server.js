const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const SERVER_ID = process.env.SERVER_ID || "js_server_1";
const BACKEND_ENDPOINT = process.env.BACKEND_ENDPOINT || "tcp://broker:5556";
const PUBSUB_PROXY_IN_ENDPOINT = process.env.PUBSUB_PROXY_IN_ENDPOINT || "tcp://pubsub_proxy:5557";
const DATA_FILE = process.env.DATA_FILE || "/data/state.json";

const USERNAME_REGEX = /^[A-Za-z0-9_-]{3,20}$/;
const CHANNEL_REGEX = /^[A-Za-z0-9_-]{3,30}$/;

function nowIso() {
  return new Date().toISOString();
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
    users: [],
    logins: [],
    channels: ["geral"],
    requests: [],
    publications: []
  };
}

function saveState(state) {
  ensureParent(DATA_FILE);
  fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2), "utf8");
}

function loadState() {
  ensureParent(DATA_FILE);

  if (!fs.existsSync(DATA_FILE)) {
    const initialState = defaultState();
    saveState(initialState);
    return initialState;
  }

  const raw = fs.readFileSync(DATA_FILE, "utf8");
  const state = JSON.parse(raw);
  state.server_id = state.server_id || SERVER_ID;
  state.users = Array.isArray(state.users) ? state.users : [];
  state.logins = Array.isArray(state.logins) ? state.logins : [];
  state.channels = Array.isArray(state.channels) ? state.channels : ["geral"];
  state.requests = Array.isArray(state.requests) ? state.requests : [];
  state.publications = Array.isArray(state.publications) ? state.publications : [];
  if (!state.channels.includes("geral")) {
    state.channels.push("geral");
  }
  return state;
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
    server_processed_at: processedAt
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

function okResponse(request, extra = {}) {
  return {
    type: "SERVER_RESULT",
    request_id: request.request_id,
    timestamp: nowIso(),
    status: "OK",
    server_id: SERVER_ID,
    request_type: request.type,
    ...extra
  };
}

function errorResponse(request, error) {
  return {
    type: "SERVER_RESULT",
    request_id: request.request_id,
    timestamp: nowIso(),
    status: "ERROR",
    server_id: SERVER_ID,
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
  const processedAt = nowIso();
  persistRequest(state, request, processedAt);

  if (state.users.includes(username)) {
    saveState(state);
    return errorResponse(request, `Usuário '${username}' já existe.`);
  }

  state.users = Array.from(new Set([...state.users, username])).sort();
  state.logins.push({
    username,
    timestamp: request.timestamp || processedAt,
    server_processed_at: processedAt
  });
  saveState(state);
  return okResponse(request, { username });
}

function handleListChannels(request) {
  const state = loadState();
  const processedAt = nowIso();
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
  const processedAt = nowIso();
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
    client_timestamp: request.timestamp,
    server_id: SERVER_ID
  };
}

async function handlePublishMessage(request, pubSocket) {
  const state = loadState();
  const processedAt = nowIso();
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

  const state = loadState();
  const processedAt = nowIso();
  persistRequest(state, request, processedAt);
  persistPublication(state, publication);
  saveState(state);
  return okResponse(request, { publication_id: publication.publication_id });
}

async function handleRequest(request, pubSocket) {
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
  const rpcSocket = new zmq.Dealer({ routingId: SERVER_ID });
  rpcSocket.connect(BACKEND_ENDPOINT);

  const pubSocket = new zmq.Publisher();
  pubSocket.connect(PUBSUB_PROXY_IN_ENDPOINT);

  const registerMessage = {
    type: "REGISTER_SERVER",
    server_id: SERVER_ID,
    timestamp: nowIso()
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