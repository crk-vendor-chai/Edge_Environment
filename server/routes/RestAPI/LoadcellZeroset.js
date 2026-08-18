// ============================================================
// LoadcellZeroset.js
// 역할: 로드셀 영점(zeroset) 자동화 — IO board의 POST /calibrate 관리.
//  - 로드셀 측정 보증 시간(30분)을 넘기지 않도록, 마지막 영점 후
//    경과 시간이 임계값을 넘으면 유휴 상태에서 1회 실행 (경과 기준)
//  - 결제 세션 종료(카메라 OFF 직후)마다 1회 실행 → 같은 타이머 리셋
//  - 서버 기동 직후 1회 실행 (재시작 후 영점 보증 공백 방지)
//  - calibrate는 IO board 시리얼 버스를 3~4초 점유하므로(그동안 모든
//    시리얼 명령 대기), 세션 시작·health 발행은 waitForIdle()로 진행 중
//    calibrate 완료를 기다린 뒤 진입한다
// 실행 가드: 결제 세션 중 / 상품 수집 세션 중 / 문 열림·불명 상태에서는
//    실행하지 않고 다음 점검(1분)으로 미룬다
// ============================================================
require("dotenv").config();
const axios = require("axios");
const config = require("../../config/key");
const { getProcessing } = require("./PaymentProcessing");

// 측정 보증 30분보다 여유를 둔 임계값 — 세션 중 skip이 몇 번 이어져도
// 상한 전에 재시도할 시간이 남도록 한다
const ELAPSED_THRESHOLD_MS = 25 * 60 * 1000;
const CHECK_INTERVAL_MS = 60 * 1000;
// calibrate 자체 3~4초 + IO board 시리얼 재시도 여유
const CALIBRATE_TIMEOUT_MS = 10 * 1000;
// 기동 직후 IO board 서비스가 뜰 때까지 대기
const BOOT_DELAY_MS = 20 * 1000;

let lastCalibratedAt = 0; // epoch ms. 0 = 기동 후 성공한 영점 없음
let inFlight = null; // 진행 중 calibrate promise (중복 발사 방지 + waitForIdle)
let lastSkipReason = null;

// AckCollect는 HealthMqtt를 거쳐 순환 참조가 생길 수 있어 호출 시점에 lazy require
function collectSessionActive() {
  try {
    const { hasActiveCollectSession } = require("../Mqtt/AckCollect");
    return hasActiveCollectSession();
  } catch {
    return false;
  }
}

async function doorClosed() {
  try {
    const { fetchCurrentDoorState } = require("../Mqtt/AckCollect");
    return (await fetchCurrentDoorState()) === "CLOSE";
  } catch {
    return false; // 확인 불가면 보수적으로 skip
  }
}

// calibrate 1회 실행. 이미 진행 중이면 그 promise를 반환(중복 발사 방지).
// 반환 promise는 절대 reject하지 않는다 — 실패는 로그만 남기고
// lastCalibratedAt을 갱신하지 않아 다음 점검(1분)에서 자연 재시도된다.
function calibrateOnce(reason) {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const elapsedMin = lastCalibratedAt
      ? ((Date.now() - lastCalibratedAt) / 60000).toFixed(1)
      : "n/a";
    try {
      console.log(`[ZEROSET] calibrate start (reason=${reason}, elapsed=${elapsedMin}min)`);
      await axios.post(`${config.ioboardApi}/calibrate`, {}, { timeout: CALIBRATE_TIMEOUT_MS });
      lastCalibratedAt = Date.now();
      console.log(`[ZEROSET] calibrate done (reason=${reason})`);
    } catch (e) {
      console.error(`[ZEROSET] calibrate failed (reason=${reason}):`, e?.message || e);
    } finally {
      inFlight = null;
    }
  })();
  return inFlight;
}

// 주기 점검: 경과 시간이 임계값을 넘었고 가드를 통과하면 발사
async function fireIfDue() {
  if (inFlight) return;
  if (lastCalibratedAt && Date.now() - lastCalibratedAt < ELAPSED_THRESHOLD_MS) return;

  let skip = null;
  if (getProcessing()) skip = "payment-session";
  else if (collectSessionActive()) skip = "collect-session";
  else if (!(await doorClosed())) skip = "door-open-or-unknown";

  if (skip) {
    // 세션이 길어지면 매분 반복되므로 같은 사유는 한 번만 로그
    if (skip !== lastSkipReason) {
      console.log(`[ZEROSET] due but skipped (${skip}) — will retry`);
      lastSkipReason = skip;
    }
    return;
  }
  lastSkipReason = null;
  await calibrateOnce("periodic");
}

// 결제 세션 종료 시 호출 (Payments.js — 카메라 OFF 직후).
// 문 닫힘·녹화/카메라 종료가 보장된 지점이라 결제 세션 가드는 건너뛴다.
// fire-and-forget: 실패해도 결제 흐름과 무관하고, 주기 점검이 백스톱.
function fireAfterSession() {
  if (collectSessionActive()) {
    console.log("[ZEROSET] post-session skipped (collect-session)");
    return;
  }
  calibrateOnce("post-session");
}

// 진행 중 calibrate가 있으면 완료까지 대기 (없으면 즉시 반환).
// calibrate가 시리얼을 점유한 채 health check(GET /health, timeout 5s)에
// 들어가면 타임아웃으로 오판될 수 있어, 세션 시작·health 발행 전에 호출한다.
async function waitForIdle() {
  if (inFlight) await inFlight; // calibrateOnce의 promise는 reject하지 않음
}

function init() {
  console.log(`[ZEROSET] init (threshold=${ELAPSED_THRESHOLD_MS / 60000}min, check=${CHECK_INTERVAL_MS / 1000}s)`);
  // 기동 직후 1회 — 재시작 후 마지막 영점 시각을 알 수 없으므로 즉시 보증 확보
  setTimeout(fireIfDue, BOOT_DELAY_MS);
  setInterval(fireIfDue, CHECK_INTERVAL_MS);
}

module.exports = { init, fireAfterSession, waitForIdle };
