// ============================================================
// MqttClient.js
// 역할: 클라우드(PNT/CHAI) MQTT broker 연결을 담당하는 공용 client 모듈.
//   단일 client 인스턴스를 생성하여 모든 핸들러가 공유(getClient)하고,
//   subscribe/publish/disconnect 유틸 함수를 제공한다.
// 부가: MQTT 연결 오류(인터넷 장애) 시 경고 음성(internet_error.mp3)을
//   20초 간격으로 반복 재생하고, 재연결 성공 시 중단한다.
// ============================================================
const mqtt = require("mqtt");
const config = require("../../config/key");
const { exec } = require("child_process");
const os = require("os");
const path = require("path");

let client = null;

// MQTT client 생성 및 연결: 접속 옵션 설정, 연결/재연결/오류 이벤트 로깅,
// 연결 오류 시 인터넷 장애 안내 음성 반복 재생
function createMqttClient() {
  if (!config.mqttURL) throw new Error("Missing config.mqttURL");
  if (!config.mqttID) throw new Error("Missing config.mqttID");
  if (!config.mqttPW) throw new Error("Missing config.mqttPW");

  // OS별 커맨드(afplay/mpg123)로 mp3 파일 재생
  function playMp3(filePath) {
    const platform = os.platform(); // 'darwin', 'linux', 'win32'
    let cmd;
    // macOS
    if (platform === "darwin") {cmd = `afplay "${filePath}"`;}
    else if (platform === "linux") {
      cmd = `mpg123 -f 32768 "${filePath}"`; // 100%
    }
    else {
      console.warn("[AUDIO] Unsupported OS:", platform);
      return;
    }
    exec(cmd, (err) => {
      if (err) {
        console.error("[AUDIO] play failed:", err.message);
      }
    });
  }

  // 인터넷(MQTT 연결) 오류 안내 음성 재생
  function playInternetErrorVoice() {
    const audioPath = path.resolve(__dirname, '../Sounds/internet_error.mp3');
    playMp3(audioPath);
    console.log("[VOICE] Edge internet error (play audio)");
  }

  const deviceIdx = config.deviceIdx

  const clientId = deviceIdx

  // broker가 사설 인증서(TLS)라서 self-signed 이면 rejectUnauthorized:false가 필요할 수 있음(테스트 용도)
  const options = {
    clientId,
    username: config.mqttID,
    password: config.mqttPW,
    clean: false,
    keepalive: 30,
    connectTimeout: 10000,

    // 재연결 주기(ms). 운영에선 1000~5000 정도 권장
    reconnectPeriod: 2000,

    // TLS 관련 필요 시:
    // rejectUnauthorized: true,
  };

  client = mqtt.connect(config.mqttURL, options);

  let internetErrorVoiceInterval = null;

  client.on("connect", () => {
    if (internetErrorVoiceInterval) {
      clearInterval(internetErrorVoiceInterval);
      internetErrorVoiceInterval = null;
    }
    console.log(`[MQTT] connected (${config.mqttURL}) clientId=${clientId}`);
  });
  client.on("reconnect", () => console.log("[MQTT] reconnecting..."));
  client.on("offline", () => console.log("[MQTT] offline"));
  client.on("close", () => console.log("[MQTT] close"));
  client.on("end", () => console.log("[MQTT] end"));
  // client.on("error", (err) => console.error("[MQTT] error:", err?.message || err));
  client.on("error", (err) => { console.error("[MQTT] error:", err?.message || err);
    if (!internetErrorVoiceInterval) {
      playInternetErrorVoice();

      internetErrorVoiceInterval = setInterval(() => {
        playInternetErrorVoice();
      }, 20000); // 20초마다 반복
    }
  });

  // 공통 수신 로깅(원하면 여기서 topic 라우팅도 가능)
  client.on("message", (topic, payload) => {
    console.log(`[MQTT] topic=${topic} payload=${payload.toString()}`);
  });

  return client;
}

// 공유 MQTT client 반환 (없으면 새로 생성 - singleton 패턴)
function getClient() {
  if (!client) return createMqttClient();
  return client;
}

// MQTT 연결 완료를 대기 (timeout 시 reject)
function waitForConnect(c, timeoutMs = 8000) {
  if (c.connected) return Promise.resolve(true);

  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error("MQTT connect timeout")), timeoutMs);

    c.once("connect", () => {
      clearTimeout(t);
      resolve(true);
    });
    c.once("error", (e) => {
      clearTimeout(t);
      reject(e);
    });
  });
}

// 연결 보장 후 MQTT topic(단일/배열) subscribe (QoS 1 고정)
async function subscribe(topics, qos = 0) {
  const c = getClient();
  const list = Array.isArray(topics) ? topics : [topics];

  await waitForConnect(c); // 연결 보장

  return new Promise((resolve, reject) => {
    // QoS는 요구사항에 맞게 조절 (0/1/2)
    c.subscribe(list, { qos: 1 }, (err, granted) => {
      if (err) return reject(err);
      console.log("[MQTT] subscribed:", granted);
      resolve(granted);
    });
  });
}

// MQTT topic으로 payload publish (객체는 JSON 문자열로 변환, QoS 1)
function publish(topic, payload, opts = {}) {
  const c = getClient();
  const message = typeof payload === "string" ? payload : JSON.stringify(payload);
  console.log('payload', payload)

  const options = {
    qos: 1,
    retain: false,
  };

  return new Promise((resolve, reject) => {
    c.publish(topic, message, options, (err) => {
      if (err) return reject(err);
      resolve(true);
    });
  });
}

// MQTT 연결 종료 및 client 인스턴스 해제
function disconnect() {
  if (!client) return Promise.resolve(true);

  return new Promise((resolve) => {
    client.end(true, () => {
      console.log("[MQTT] disconnected");
      client = null;
      resolve(true);
    });
  });
}

module.exports = {
  getClient,
  subscribe,
  publish,
  disconnect,
};