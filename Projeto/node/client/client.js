const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");
const crypto = require("crypto");

const CLIENT_NAME = process.env.CLIENT_NAME || "js_client_1";
const USERNAME = process.env.USERNAME || "carol_js";
const TARGET_CHANNEL = process.env.TARGET_CHANNEL || "devops";
const BROKER_ENDPOINT = process.env.BROKER_ENDPOINT || "tcp://broker:5555";
const PUBSUB_PROXY_OUT_ENDPOINT = process.env.PUBSUB_PROXY_OUT_ENDPOINT || "tcp://pubsub_proxy:5558";
const STARTUP_DELAY_SECONDS = Number(process.env.STARTUP_DELAY_SECONDS || "5");
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || "8000");
const MINIMUM_CHANNELS = Number(process.env.MINIMUM_CHANNELS || "5");
const MINIMUM_SUBSCRIPTIONS = Number(process.env.MINIMUM_SUBSCRIPTIONS || "3");
const MESSAGES_PER_BATCH = Number(process.env.MESSAGES_PER_BATCH || "10");
const MESSAGE_INTERVAL_SECONDS = Number(process.env.MESSAGE_INTERVAL_SECONDS || "1");
const MAX_BATCHES = Number(process.env.MAX_BATCHES || "0");

const MESSAGE_TEMPLATES = [
  "Atualização do canal {channel} enviada por {user}",
  "Mensagem automática #{counter} no canal {channel}",
  "Bot {user} reportando atividade distribuída em {channel}",
  "Evento sincronizado {counter} para o tópico {channel}",
  "Heartbeat do bot {user} no canal {channel}"
];

let logicalClock = 0;

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
    logical_clock: tickLogicalClock(),
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
    updateLogicalClockFromMessage(response);
    logMessage("RECV", response);
    return response;
  })();

  return Promise.race([receivePromise, timeoutPromise]);
}

async function listChannels(socket, username) {
  const response = await sendRequest(socket, makeRequest("LIST_CHANNELS", { username }));

  if (response.status !== "OK") {
    throw new Error(response.error || "Falha ao listar canais.");
  }

  return Array.isArray(response.channels) ? response.channels : [];
}

async function login(socket) {
  for (let attempt = 1; attempt <= 10; attempt += 1) {
    const candidate = attempt === 1 ? USERNAME : `${USERNAME}_${attempt}`;
    const response = await sendRequest(socket, makeRequest("LOGIN", { username: candidate }));

    if (response.status === "OK") {
      return candidate;
    }

    console.log(`[${CLIENT_NAME}] Falha no login com '${candidate}': ${response.error}`);
    await sleep(1000);
  }

  throw new Error(`${CLIENT_NAME} não conseguiu efetuar login.`);
}

async function ensureSingleChannelCreation(socket, username, channels) {
  if (channels.length >= MINIMUM_CHANNELS) {
    return channels;
  }

  const createResponse = await sendRequest(
    socket,
    makeRequest("CREATE_CHANNEL", {
      username,
      channel: TARGET_CHANNEL
    })
  );

  if (createResponse.status !== "OK") {
    console.log(`[${CLIENT_NAME}] Não foi possível criar canal '${TARGET_CHANNEL}': ${createResponse.error}`);
  }

  return listChannels(socket, username);
}

async function startPubSubListener(subscriber) {
  (async () => {
    for await (const [topicBuffer, payload] of subscriber) {
      const publication = decode(payload);
      updateLogicalClockFromMessage(publication);

      const receivedMessage = {
        channel: topicBuffer.toString(),
        message: publication.message,
        sent_timestamp: publication.timestamp,
        sent_logical_clock: publication.logical_clock,
        received_timestamp: nowIso(),
        received_logical_clock: logicalClock,
        username: publication.username,
        server_id: publication.server_id,
        server_rank: publication.server_rank,
        publication_id: publication.publication_id
      };

      logMessage("PUBSUB_RECV", receivedMessage);
    }
  })().catch((error) => {
    console.error(`[${CLIENT_NAME}] Erro no listener Pub/Sub:`, error);
  });
}

function maybeSubscribeMore(subscriber, subscribedChannels, channels) {
  const availableCandidates = channels.filter((channel) => !subscribedChannels.has(channel));

  if (subscribedChannels.size >= MINIMUM_SUBSCRIPTIONS || availableCandidates.length === 0) {
    return;
  }

  const selectedChannel = availableCandidates[Math.floor(Math.random() * availableCandidates.length)];
  subscriber.subscribe(selectedChannel);
  subscribedChannels.add(selectedChannel);
  console.log(`[${CLIENT_NAME}][SUBSCRIBE] Inscrito no canal '${selectedChannel}'`);
}

function randomMessage(channel, username, counter) {
  const template = MESSAGE_TEMPLATES[Math.floor(Math.random() * MESSAGE_TEMPLATES.length)];

  return template
    .replaceAll("{channel}", channel)
    .replaceAll("{user}", username)
    .replaceAll("{counter}", String(counter));
}

async function publishBatch(socket, username, channel, batchNumber) {
  for (let messageCounter = 1; messageCounter <= MESSAGES_PER_BATCH; messageCounter += 1) {
    const absoluteCounter = ((batchNumber - 1) * MESSAGES_PER_BATCH) + messageCounter;
    const messageText = randomMessage(channel, username, absoluteCounter);

    const response = await sendRequest(
      socket,
      makeRequest("PUBLISH_MESSAGE", {
        username,
        channel,
        message: messageText
      })
    );

    if (response.status !== "OK") {
      console.log(`[${CLIENT_NAME}] Falha ao publicar no canal '${channel}': ${response.error}`);
    }

    await sleep(MESSAGE_INTERVAL_SECONDS * 1000);
  }
}

async function main() {
  await sleep(STARTUP_DELAY_SECONDS * 1000);

  const requestSocket = new zmq.Request();
  requestSocket.connect(BROKER_ENDPOINT);

  const subscriber = new zmq.Subscriber();
  subscriber.connect(PUBSUB_PROXY_OUT_ENDPOINT);
  await startPubSubListener(subscriber);

  const subscribedChannels = new Set();

  const activeUsername = await login(requestSocket);
  let channels = await listChannels(requestSocket, activeUsername);
  channels = await ensureSingleChannelCreation(requestSocket, activeUsername, channels);
  maybeSubscribeMore(subscriber, subscribedChannels, channels);

  let batchNumber = 0;

  while (true) {
    channels = await listChannels(requestSocket, activeUsername);
    maybeSubscribeMore(subscriber, subscribedChannels, channels);

    if (channels.length === 0) {
      await sleep(1000);
      continue;
    }

    batchNumber += 1;

    const selectedChannel = channels[Math.floor(Math.random() * channels.length)];
    await publishBatch(requestSocket, activeUsername, selectedChannel, batchNumber);

    if (MAX_BATCHES > 0 && batchNumber >= MAX_BATCHES) {
      console.log(`[${CLIENT_NAME}] Execução encerrada após ${batchNumber} lote(s).`);
      break;
    }
  }
}

main().catch((error) => {
  console.error(`[${CLIENT_NAME}] Erro fatal:`, error);
  process.exit(1);
});
