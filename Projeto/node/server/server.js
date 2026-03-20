const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const SERVER_ID = process.env.SERVER_ID || "js_server_1";
const BACKEND_ENDPOINT = process.env.BACKEND_ENDPOINT || "tcp://broker:5556";
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

function saveState(state) {
  ensureParent(DATA_FILE);
  fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2), "utf8");
}

function loadState() {
  ensureParent(DATA_FILE);

  if (!fs.existsSync(DATA_FILE)) {
    const initialState = {
      server_id: SERVER_ID,
      users: [],
      logins: [],
      channels: ["geral"]
    };
    saveState(initialState);
    return initialState;
  }

  const raw = fs.readFileSync(DATA_FILE, "utf8");
  const state = JSON.parse(raw);
  state.server_id = state.server_id || SERVER_ID;
  state.users = Array.isArray(state.users) ? state.users : [];
  state.logins = Array.isArray(state.logins) ? state.logins : [];
  state.channels = Array.isArray(state.channels) ? state.channels : ["geral"];
  if (!state.channels.includes("geral")) {
    state.channels.push("geral");
  }
  return state;
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
  if (state.users.includes(username)) {
    return errorResponse(request, `Usuário '${username}' já existe.`);
  }

  state.users = Array.from(new Set([...state.users, username])).sort();
  state.logins.push({
    username,
    timestamp: request.timestamp || nowIso(),
    server_processed_at: nowIso()
  });
  saveState(state);
  return okResponse(request, { username });
}

function handleListChannels(request) {
  const state = loadState();
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
  if (state.channels.includes(channel)) {
    return errorResponse(request, `Canal '${channel}' já existe.`);
  }

  state.channels = Array.from(new Set([...state.channels, channel])).sort();
  saveState(state);
  return okResponse(request, { channel });
}

function handleRequest(request) {
  switch (request.type) {
    case "LOGIN":
      return handleLogin(request);
    case "LIST_CHANNELS":
      return handleListChannels(request);
    case "CREATE_CHANNEL":
      return handleCreateChannel(request);
    default:
      return errorResponse(request, `Tipo de requisição inválido: ${request.type}`);
  }
}

async function main() {
  const socket = new zmq.Dealer({ routingId: SERVER_ID });
  socket.connect(BACKEND_ENDPOINT);

  const registerMessage = {
    type: "REGISTER_SERVER",
    server_id: SERVER_ID,
    timestamp: nowIso()
  };

  logMessage("SEND", registerMessage);
  await socket.send(encode(registerMessage));

  for await (const [payload] of socket) {
    const request = decode(payload);
    logMessage("RECV", request);

    const response = handleRequest(request);
    logMessage("SEND", response);
    await socket.send(encode(response));
  }
}

main().catch((error) => {
  console.error(`[${SERVER_ID}] Erro fatal:`, error);
  process.exit(1);
});
