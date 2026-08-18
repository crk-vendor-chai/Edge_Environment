// ============================================================
// DoorCollect.js
// 역할: 클라우드(PNT/CHAI)의 door collect 명령(MQTT topic:
//   chai/device/{deviceIdx}/cmd/door/collect)을 수신하여 deadbolt를
//   제어하고, door 상태와 health check 결과를 ack topic
//   (chai/device/{deviceIdx}/ack/door/collect, IF_04)으로 publish한다.
// 연동: IO board API(SSE /sse?streams=doors, POST /deadbolt),
//   camera/deadbolt/loadcell health check API,
//   door CLOSE 시 AI 서버(/v1/events/product/created)로 이벤트 통지.
// ============================================================
// server/routes/Mqtt/DoorCollect.js
const { v4: uuidv4 } = require("uuid");
const { EventSource } = require("eventsource");
const axios = require('axios');
const config = require("../../config/key");
const { callApiToControlDeadbolt } = require("./DeadboltApiService");
const {
  DeadboltStatusAPI,
  LoadcellStatusAPI,
  CameraStatusAPI,
} = require("./HealthMqtt");
const { getClient } = require("./MqttClient");

let latestCollectOption = {
  hasLoadcell: null,
  storageType: null,
  doorState: null
};

/**
 * IF06에서 받은 학습 대상 장비 정보
 *
 * IF04 장비 정보와 구분하기 위해 별도로 저장한다.
 */
let latestTrainingTarget = null;

function setLatestTrainingTarget({
  productIdx,
  divisionIdx,
  deviceIdx,
  storageType,
}) {
  if (!divisionIdx) {
    throw new Error(
      "[DoorCollect] Training target divisionIdx is required"
    );
  }

  latestTrainingTarget = {
    productIdx: productIdx != null
      ? String(productIdx)
      : null,

    divisionIdx: String(divisionIdx),

    deviceIdx: deviceIdx != null
      ? String(deviceIdx)
      : null,

    storageType,

    updatedAt: new Date(),
  };

  console.log(
    "[DoorCollect] Training target saved:",
    latestTrainingTarget
  );

  return latestTrainingTarget;
}

function getLatestTrainingTarget() {
  return latestTrainingTarget;
}

function clearLatestTrainingTarget() {
  console.log(
    "[DoorCollect] Training target cleared:",
    latestTrainingTarget
  );

  latestTrainingTarget = null;
}

// 가장 최근 collect 명령의 옵션(hasLoadcell/storageType/doorState) snapshot을 반환
function getLatestCollectOption() {
  return latestCollectOption;
}

// IF_DATE 형식(yyyyMMddHHmmss)의 timestamp 문자열 생성
function makeIFDate(d = new Date()) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const HH = String(d.getHours()).padStart(2, "0");
  const MM = String(d.getMinutes()).padStart(2, "0");
  const SS = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}${HH}${MM}${SS}`;
}

// IO board의 SSE 스트림(door.update)에서 현재 door(deadbolt) 상태를 1회 조회
// 3초 timeout, 실패/파싱 오류 시 "UNKNOWN" 반환
async function fetchCurrentDoorState() {
  return new Promise((resolve) => {
    const url = `${config.ioboardApi}/sse?streams=doors`;

    let evtSource;
    try {
      evtSource = new EventSource(url);
    } catch (err) {
      console.error("[DoorCollect] EventSource Error:", err.message);
      resolve("UNKNOWN");
      return;
    }

    const timer = setTimeout(() => {
      evtSource.close();
      resolve("UNKNOWN");
    }, 3000);

    evtSource.addEventListener("door.update", (event) => {
      try {
        const data = JSON.parse(event.data || "{}");
        const raw = String(data.deadbolt || "").toUpperCase();

        const closeStates = ["LOCK", "LOCKED", "CLOSE", "CLOSED"];
        const openStates = ["UNLOCK", "UNLOCKED", "OPEN", "OPENED"];

        let state = "UNKNOWN";
        if (closeStates.includes(raw)) state = "CLOSE";
        if (openStates.includes(raw)) state = "OPEN";

        clearTimeout(timer);
        evtSource.close();
        resolve(state);
      } catch {
        clearTimeout(timer);
        evtSource.close();
        resolve("UNKNOWN");
      }
    });

    evtSource.onerror = () => {
      clearTimeout(timer);
      evtSource.close();
      resolve("UNKNOWN");
    };
  });
}

// 목표 door 상태(targetState)가 될 때까지 300ms 간격으로 polling 대기
// timeout이 지나면 마지막으로 조회한 상태를 그대로 반환
async function waitForDoorState(targetState, timeoutMs = 5000) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const state = await fetchCurrentDoorState();

    if (state === targetState) return state;

    await new Promise((resolve) => setTimeout(resolve, 300));
  }

  return await fetchCurrentDoorState();
}

// camera/deadbolt/loadcell health check 결과를 "1"(정상)/"0"(오류) 코드로 변환
// hasLoadcell !== "Y" 이면 loadcell 조회를 생략하고 정상 코드로 처리
async function getHealthStatus(hasLoadcell) {
  const cameraRaw = await CameraStatusAPI();
  const deadboltRaw = await DeadboltStatusAPI();

  const useLoadcell = hasLoadcell === "Y"
  let loadcellRaw = "29";

  if (useLoadcell) {
    loadcellRaw = await LoadcellStatusAPI();
  }

  return {
    camera_status: cameraRaw === "09" ? "1" : "0",
    deadbolt_status: deadboltRaw === "19" ? "1" : "0",
    // loadcell_status: useLoadcell ? (loadcellRaw === "29" ? "1" : "0") : "9",
    loadcell_status: loadcellRaw === "29" ? "1" : "0",
  };
}

// door collect 처리 결과 ack(IF_04)를 MQTT topic으로 publish
function publishDoorAck({
  client,
  topic,
  ifSysId,
  doorState,
  storageType,
  hasLoadcell,
  cameraStatus,
  deadboltStatus,
  loadcellStatus,
  resultCd,
  resultMsg,
}) {
  const timestamp = makeIFDate();

  const responsePayload = {
    HEADER: {
      IF_ID: "IF_04",
      IF_SYSID: uuidv4(),
      IF_HOST: "CRKPNTCHAI",
      IF_DATE: timestamp,
    },
    DATA: {
      division_idx: config.divisionIdx,
      device_idx: config.deviceIdx,
      door_state: doorState,
      storage_type: storageType,
      has_loadcell: hasLoadcell,
      camera_status: cameraStatus,
      deadbolt_status: deadboltStatus,
      loadcell_status: loadcellStatus,
      result_cd: resultCd,
      result_msg: resultMsg,
    },
  };

  console.log("[DoorCollect] PUB Topic:", topic);
  console.log("[DoorCollect] PUB Payload:", responsePayload);

  client.publish(topic, JSON.stringify(responsePayload), { qos: 1, retain: false }, (e) => {
    if (e) {
      console.error("[DoorCollect] Publish Error:", e.message);
      return;
    }

    console.log(`[DoorCollect] ACK Sent. Result=${resultCd}, State=${doorState}`);
  });
}

// door collect 명령 subscribe 및 처리 진입점
// 흐름: deadbolt 제어 -> door 상태 확인 -> health check -> ack publish
// door CLOSE 시 AI 서버로 product created 이벤트(IF_EDGE_01)를 추가 전송
async function DoorCollect() {
  const subTopic = `chai/device/${config.deviceIdx}/cmd/door/collect`;
  const pubTopic = `chai/device/${config.deviceIdx}/ack/door/collect`;

  const client = getClient();

  client.subscribe(subTopic, { qos: 1 }, (err, granted) => {
    if (err) {
      console.error("[DoorCollect] Subscribe Error:", err.message);
      return;
    }

    console.log("[DoorCollect] Subscribe granted:", granted);
    // console.log(`[DoorCollect] Subscribed: ${subTopic}`);
  });

  client.on("message", async (topic, message) => {
    if (topic !== subTopic) return;

    let reqData = {};
    let ifSysId = ''

    try {
      const payload = JSON.parse(message.toString());
      console.log('[Request] payload.DATA', payload.HEADER)
      ifSysId = payload.HEADER.IF_SYSID
      reqData = payload.DATA || {};

      const {
        device_idx: deviceIdx,
        division_idx: divisionIdx,
        door_state: reqDoorState,
        storage_type: storageType,
        has_loadcell: hasLoadcell,
      } = reqData;

      console.log("[DoorCollect] Request:", reqData);

      await callApiToControlDeadbolt(reqDoorState);

      const finalState = await waitForDoorState(reqDoorState, 5000);
      const health = await getHealthStatus(hasLoadcell);

      const isDoorOk = finalState === reqDoorState;

      latestCollectOption = {
        hasLoadcell,
        storageType,
        doorState: reqDoorState,
      };

      console.log("[DoorCollect] latestCollectOption:", latestCollectOption);

      const isHealthOk =
        health.camera_status === "1" &&
        health.deadbolt_status === "1" &&
        (health.loadcell_status === "1" || health.loadcell_status === "9");

      const resultCd = isDoorOk && isHealthOk ? "S" : "F";

      const resultMsg = resultCd === "S" ? "status access" : "status error";

      await publishDoorAck({
        client,
        topic: pubTopic,
        ifSysId: ifSysId,
        deviceIdx: config.deviceIdx,
        divisionIdx: config.divisionIdx,
        doorState: finalState,
        storageType: reqData.storage_type,
        hasLoadcell: reqData.has_loadcell,
        cameraStatus: health.camera_status,
        deadboltStatus: health.deadbolt_status,
        loadcellStatus: health.loadcell_status,
        resultCd,
        resultMsg,
      });

      console.log('[PNT DOOR REQ] status of doorState: ', latestCollectOption.doorState)
      if (latestCollectOption.doorState === 'CLOSE') {
        const trainingTarget = getLatestTrainingTarget();
        const aiServer = `${config.aiServerApi}/v1/events/product/created`
        const now = new Date();
        const formattedDate = makeIFDate(now)
        const sysidDate = now.toISOString().replace(/[-:T]/g, "").slice(0, 8);
        const sysidTime = now.toISOString().replace(/[-:T]/g, "").slice(8, 14);
        const aiStorageType = (latestCollectOption.storageType == 'C' ? 'True' : 'False');
    
        const payload = {
            HEADER: {
                IF_ID   : "IF_EDGE_01",
                IF_SYSID: `EDGEPC-${sysidDate}-${sysidTime}`,
                IF_HOST : "EDGEPC",
                IF_DATE : formattedDate
            },
            DATA: {
                /*
                * IF04의 reqData.division_idx가 아니라
                * IF06에서 미리 저장한 학습 대상 division_idx
                */
                division_idx: trainingTarget.divisionIdx,
                // True: 냉장(Cold) / False: 냉동(Frozen)
                is_cold: aiStorageType
            },
        };
    
        console.log("[EDGE->AI] url:", aiServer);
        console.log("[EDGE->AI] payload:", JSON.stringify(payload, null, 2));
        
        try {
          const response = await axios.post(aiServer, payload, {
            headers: {
              "Content-Type": "application/json",
            },
            timeout: 10000,
          });
    
          console.log("[EDGE->AI] response:", response.data);
          /*
          * AI 요청이 성공한 경우에만 삭제한다.
          * 실패하면 다음 CLOSE 요청에서 재시도 가능하다.
          */
          clearLatestTrainingTarget();
        } catch (err) {
          console.error("[EDGE->AI] status:", err.response?.status);
          console.error("[EDGE->AI] data:", err.response?.data);
          console.error("[EDGE->AI] message:", err.message);
        }
      }

    } catch (error) {
      console.error("[DoorCollect] Error:", error.message);

      const health = await getHealthStatus(reqData.has_loadcell).catch(() => ({
        camera_status: "0",
        deadbolt_status: "0",
        loadcell_status: "0",
      }));

      await publishDoorAck({
        client,
        topic: pubTopic,
        ifSysId: ifSysId,
        deviceIdx: config.deviceIdx,
        divisionIdx: config.divisionIdx,
        doorState: reqData.door_state,
        storageType: reqData.storage_type,
        hasLoadcell: reqData.has_loadcell,
        cameraStatus: health.camera_status,
        deadboltStatus: health.deadbolt_status,
        loadcellStatus: health.loadcell_status,
        resultCd: "F",
        resultMsg: error?.message || String(error),
      });

      console.log("[DoorCollect] latestCollectOption:", latestCollectOption);
    }
  });

  client.on("error", (err) => {
    console.error("[DoorCollect] MQTT Error:", err.message);
  });

  client.on("close", () => {
    console.warn("[DoorCollect] MQTT Closed");
  });
}

module.exports = {
  DoorCollect,
  fetchCurrentDoorState,
  getLatestCollectOption,

  setLatestTrainingTarget,
  getLatestTrainingTarget,
  clearLatestTrainingTarget,
};