const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");
const crypto = require("crypto");

const CLIENT_NAME = process.env.CLIENT_NAME || "js_client_1";
const USERNAME = process.env.USERNAME || "carol_js";
const TARGET_CHANNEL = process.env.TARGET_CHANNEL || "devops";
const BROKER_ENDPOINT = process.env.BROKER_ENDPOINT || "tcp://broker:5555";
const STARTUP_DELAY_SECONDS = Number(process.env.STARTUP_DELAY_SECONDS || "5");
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || "8000");

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function logMessage(direction, message) {
  console.log(`[${CLIENT_NAME}][${direction}] ${JSON.stringify(message)}`);
}

function makeRequest(type, extra = {}) {
  return {
    type,
    request_id: crypto.randomUUID(),
    timestamp: nowIso(),
    origin: CLIENT_NAME,
    ...extra
  };
}

async function sendRequest(socket, message) {
  logMessage("SEND", message);
  await socket.send(encode(message));

  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => reject(new Error("Timeout aguardando resposta do broker.")), REQUEST_TIMEOUT_MS);
  });

  const receivePromise = (async () => {
    const [payload] = await socket.receive();
    const response = decode(payload);
    logMessage("RECV", response);
    return response;
  })();

  return Promise.race([receivePromise, timeoutPromise]);
}

async function main() {
  await sleep(STARTUP_DELAY_SECONDS * 1000);

  const socket = new zmq.Request();
  socket.connect(BROKER_ENDPOINT);

  let activeUsername = USERNAME;
  let loggedIn = false;

  for (let attempt = 1; attempt <= 10; attempt += 1) {
    const candidate = attempt === 1 ? activeUsername : `${USERNAME}_${attempt}`;
    const response = await sendRequest(socket, makeRequest("LOGIN", { username: candidate }));

    if (response.status === "OK") {
      activeUsername = candidate;
      loggedIn = true;
      break;
    }

    console.log(`[${CLIENT_NAME}] Falha no login com '${candidate}': ${response.error}`);
    await sleep(1000);
  }

  if (!loggedIn) {
    throw new Error(`${CLIENT_NAME} não conseguiu efetuar login.`);
  }

  const listResponse = await sendRequest(
    socket,
    makeRequest("LIST_CHANNELS", { username: activeUsername })
  );

  const channels = listResponse.status === "OK" ? listResponse.channels || [] : [];

  if (!channels.includes(TARGET_CHANNEL)) {
    const createResponse = await sendRequest(
      socket,
      makeRequest("CREATE_CHANNEL", {
        username: activeUsername,
        channel: TARGET_CHANNEL
      })
    );

    if (createResponse.status !== "OK") {
      console.log(
        `[${CLIENT_NAME}] Não foi possível criar canal '${TARGET_CHANNEL}': ${createResponse.error}`
      );
    }
  }

  const finalList = await sendRequest(
    socket,
    makeRequest("LIST_CHANNELS", { username: activeUsername })
  );

  console.log(
    `[${CLIENT_NAME}] Fluxo finalizado. Usuário='${activeUsername}' | canais=${JSON.stringify(finalList.channels || [])}`
  );
}

main().catch((error) => {
  console.error(`[${CLIENT_NAME}] Erro fatal:`, error);
  process.exit(1);
});
