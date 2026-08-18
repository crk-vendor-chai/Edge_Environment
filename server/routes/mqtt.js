// ============================================================
// mqtt.js
// 역할: MQTT 관련 모듈의 진입점(라우터 + 초기화).
//   init()에서 HealthMqtt / RebootMqtt / ManualDeadbolt / Repayment /
//   DoorCollect / AckCollect 핸들러를 순서대로 초기화하여
//   각 MQTT topic subscribe 및 처리 루프를 올린다.
//   REST 엔드포인트(POST /api/publish)로 임의 MQTT topic publish도 지원한다.
// ============================================================
require("dotenv").config();

const express = require("express");
const { HealthMqtt } = require("./Mqtt/HealthMqtt");
const { RebootMqtt } = require('./Mqtt/RebootMqtt');
const { publish } = require("./Mqtt/MqttClient");
const { ManualDeadbolt } = require('./Mqtt/ManualDeadbolt');
const { Repayment } = require('./Mqtt/Repayment');
const { DoorCollect } = require("./Mqtt/DoorCollect");
const { AckCollect } = require("./Mqtt/AckCollect");

const router = express.Router();
// freeze(냉동고)에서 검증된 설정: /api/publish body 한도 1mb 유지
// (express.json 기본 한도는 100kb — main 2cb7f5f에서 제거됐으나 freeze 검증 구성을 우선함)
router.use(express.json({ limit: "1mb" }));

// REST -> MQTT publish 예시
// body의 topic/payload를 받아 MQTT broker로 publish하는 REST 엔드포인트
router.post("/api/publish", async (req, res) => {
  try {
    const { topic, payload, qos, retain } = req.body;
    if (!topic) return res.status(400).json({ error: "topic is required" });

    await publish(topic, payload ?? {}, { qos, retain });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// HealthMqtt는 서버 시작 시 호출
// 서버 기동 시 1회 호출: 모든 MQTT 핸들러 초기화 (실패 시 에러 전파)
async function init() {
  try {
    await HealthMqtt();
    await RebootMqtt();
    await ManualDeadbolt();
    await Repayment();
    await DoorCollect();
    await AckCollect();
    console.log("[APP] MQTT init done");
  } catch (e) {
    console.error("[APP] MQTT init failed:", e?.message || e);
    throw e;
  }
}

module.exports = {
  router,
  init,
};