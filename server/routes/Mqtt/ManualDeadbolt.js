// ============================================================
// ManualDeadbolt.js
// 역할: 관리자 수동 door 제어 명령(MQTT topic:
//   chai/device/{deviceIdx}/cmd/door/manual)을 수신하여 deadbolt를
//   open/close하고, 처리 결과 ack(IF_03)를
//   chai/device/{deviceIdx}/ack/door/manual 로 publish한다.
// 연동: DeadboltApiService(callApiToControlDeadbolt) -> IO board API.
// ============================================================
const { v4: uuidv4 } = require("uuid");
const { getClient } = require("./MqttClient");
const config = require("../../config/key");

// 분리한 API 서비스 모듈을 가져옵니다.
const { callApiToControlDeadbolt } = require("./DeadboltApiService");

// IF_DATE 형식(yyyyMMddHHmmss)의 timestamp 문자열 생성
function formatIfDate(d = new Date()) {
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}`
         + `${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

// =========================================================
// [메인 로직: MQTT 메시지 처리]
// =========================================================

// 수동 door 제어 진입점: cmd topic subscribe -> deadbolt 제어 API 호출 ->
// 결과 검증 후 ack(IF_03) publish (실패 시 result_cd "F"로 응답)
async function ManualDeadbolt() {
  const deviceIdx = config.deviceIdx;
  const divisionIdx = config.divisionIdx;

  if (!deviceIdx || !divisionIdx) {
    console.error("[DEADBOLT] Missing deviceIdx/divisionIdx in config");
    return;
  }

  const manualDoorSub = `chai/device/${deviceIdx}/cmd/door/manual`;
  const manualDoorPub = `chai/device/${deviceIdx}/ack/door/manual`;

  const client = getClient();

  client.on("connect", () => {
    console.log("[DEADBOLT] MQTT Connected");
    client.subscribe(manualDoorSub, { qos: 1 }, (err, granted) => {
      if (err) console.error("[DEADBOLT] subscribe error:", err.message);
      else console.log("[DEADBOLT] subscribed:", granted);
    });
  });

  client.on("message", async (topic, payloadBuf) => {
    if (topic !== manualDoorSub) return;

    let msg;
    try {
      msg = JSON.parse(payloadBuf.toString());
    } catch (e) {
      console.error("[DEADBOLT] invalid JSON:", payloadBuf.toString());
      return;
    }

    const targetState = msg?.DATA?.door_state; // 'OPEN' or 'CLOSE'
    const ifSysId = msg?.HEADER?.IF_SYSID || uuidv4();

    console.log(`[DEADBOLT] MQTT CMD Received. ID=${ifSysId}, Target=${targetState}`);

    if (targetState !== "OPEN" && targetState !== "CLOSE") {
      console.error("[DEADBOLT] Invalid deadbolt_state:", targetState);
      return;
    }

    // ---------------------------------------------------------
    // [API 통신 및 결과 처리]
    // ---------------------------------------------------------
    let finalState = "";
    let resultCd = "S";
    let resultMsg = "";

    try {
      // 1. 분리된 함수 호출 (API 제어 요청)
      // callApiToControlDeadbolt는 정규화된 "UNLOCK" 또는 "LOCK"만 반환한다
      // (요청 상태와 불일치 시 내부에서 throw).
      const apiResultState = await callApiToControlDeadbolt(targetState);
      console.log('apiResultState', apiResultState)

      // 2. 결과 검증: UNLOCK -> OPEN, LOCK -> CLOSE 로 ack 상태 매핑
      if (apiResultState === "UNLOCK" || apiResultState === "LOCK") {
        finalState = apiResultState === "UNLOCK" ? "OPEN" : "CLOSE";
        resultMsg = finalState === "OPEN" ? "Door is opened" : "Door is closed";
      } else {
        throw new Error(`Unexpected API response: ${apiResultState}`);
      }

    } catch (err) {
      console.error("[DOOR] API Control Failed:", err.message);
      resultCd = "F";
      resultMsg = "API Error: " + err.message;
      // 실패 시 요청의 반대 상태(제어 미반영으로 가정)로 응답
      finalState = targetState === "OPEN" ? "CLOSE" : "OPEN";
    }

    // ---------------------------------------------------------
    // [MQTT ACK 전송]
    // ---------------------------------------------------------
    
    const ackPayload = JSON.stringify({
      HEADER: {
        IF_ID: "IF_03",
        IF_SYSID: ifSysId,
        IF_HOST: "CRKPNTCHAI",
        IF_DATE: formatIfDate(),
      },
      DATA: {
        device_idx: deviceIdx,
        division_idx: divisionIdx,
        door_state: finalState,
        result_cd: resultCd,
        result_msg: resultMsg,
      }
    });

    client.publish(manualDoorPub, ackPayload, { qos: 1, retain: false }, (e) => {
      if (e) console.error("[DEADBOLT] Publish Error:", e.message);
      else console.log(`[DEADBOLT] ACK Sent. Result=${resultCd}, State=${finalState}`);
    });
  });

  // 이미 연결되어 있는 경우 구독 처리
  if (client.connected) {
    client.subscribe(manualDoorSub, { qos: 1 });
  }
}

module.exports = { ManualDeadbolt };