// ============================================================
// Payments.js
// 역할: 결제(payment) 핵심 흐름 전체를 담당하는 모듈.
//  - card terminal SSE로 카드/삼성페이/RFID 토큰 수신 → 결제 세션 시작
//  - health check(card terminal, deadbolt, loadcell, camera) 통과 시
//    deadbolt OPEN → 카메라 녹화 시작 → 모델 서버 추론(polling)
//  - deadbolt CLOSE 감지 시 녹화 종료 + 엣지 watermark(expected_triggers)
//    를 CLOSE payload에 첨부 → 추론 결과 금액으로 카드 승인
//  - 결제 결과/실패를 PNT 클라우드(IF_08)로 전송 (PaymentStore.sendToPNT)
// 주의: 이 파일은 결제 핵심 로직이므로 동작 변경 금지.
// ============================================================
require("dotenv").config();
const axios = require("axios");
const config = require("../../config/key");
const express = require("express");
const router = express.Router();
const { CardTerminalStatusAPI, DeadboltStatusAPI, LoadcellStatusAPI, CameraStatusAPI, CardTerminalErrorState } = require('../Mqtt/HealthMqtt')
const { ProductList } = require("./ProductList");
const fs = require("fs");
const path = require("path");
const { callApiToControlDeadbolt } = require('../Mqtt/DeadboltApiService');
const { getProcessing, setProcessing } = require("./PaymentProcessing");
const LoadcellZeroset = require("./LoadcellZeroset");
const { EventSource } = require('eventsource');
const { sendToPNT } = require("./PaymentStore");
const { ModelBrunchCheck } = require("./ModelBrunchCheck");
const { exec } = require("child_process");
const os = require("os");

const { v4: uuidv4 } = require("uuid");

// PNT 인터페이스용 날짜 문자열 생성 (YYYYMMDDHHmmss)
function formatIfDate(d = new Date()) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}`
       + `${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

// deadbolt "닫힘"으로 간주하는 상태값 목록
const closedStates = ["LOCK", "LOCKED", "CLOSE", "CLOSED"];

// 카드 승인 실패 시 PNT 클라우드로 결제 실패(IF_08, result_cd:'F') 전송
async function sendCardErrorToPNT(errorCode, token, CardMethod, state = "1") {
  try {
    const external = axios.create({
      baseURL: config.restApi,
      timeout: 10000,
    });

    const payload = {
      HEADER: {
        IF_ID: "IF_08",
        IF_SYSID: uuidv4(),
        IF_HOST: "CRKPNTCHAI",
        IF_DATE: formatIfDate(),
      },
      DATA: {
        device_idx: config.deviceIdx,
        division_idx: config.divisionIdx,
        token_id: token,
        payment_at: formatIfDate(),
        approve_at: '000000',
        approve_type: CardMethod === "R" ? "2" : CardMethod === "S" ? "1" : "0",
        approve_result: Number(errorCode),
        approve_price: 0,
        approve_no: "",
        approve_card_issuer: "",
        approve_card_num: "",
        approve_card_json: JSON.stringify({
          error_code: String(errorCode),
        }),
        provider: "chai",
        state, 
        result_cd: 'F',
        result_msg: 'Failed in Card Terminal'
      }
    };

    const jwtToken = config.jwtToken;

    console.log("[EDGE->PNT] CARD ERROR PAYLOAD", payload);

    const response = await external.post(
      "/chai/payment/store",
      payload,
      {
        headers: {
          Authorization: `Bearer ${jwtToken}`,
          "Content-Type": "application/json",
        },
        timeout: 30000,
      }
    );

    console.log("[PNT] Card error sent:", response.data);
    return true;
  } catch (err) {
    console.error("[PNT] Card error send failed:", err.response?.data || err.message);
    return false;
  }
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// OS별 명령으로 mp3 음성 재생 (macOS: afplay, Linux: mpg123)
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

// 1분 이상 문 열림 시 음성 출력
function playDoorOpenVoice() {
    const audioPath = path.resolve(__dirname, '../Sounds/door_open.mp3');
    playMp3(audioPath);
    console.log("[VOICE] Door is still open over 1 minute. (play audio)");
}

// 장비가 동작중일 때 음성 출력
function playDeviceRunningVoice() {
    const audioPath = path.resolve(__dirname, '../Sounds/device_is_running.mp3');
    playMp3(audioPath);
}

// 사원증(RFID) 결제 완료 시 음성 출력
function playRFIDVoice() {
    const audioPath = path.resolve(__dirname, '../Sounds/RFID_payment.mp3');
    playMp3(audioPath);
}

// 0원 추론 시 음성 출력
function playZeroPayVoice() {
    const audioPath = path.resolve(__dirname, '../Sounds/Zero_payment.mp3');
    playMp3(audioPath);
}

// 문 열림(door open) 모니터 타이머 상태
let graceTimer = null;   // 1분 후 시작용 (setTimeout)
let repeatTimer = null;  // 1분마다 반복용 (setInterval)
let startedAt = null;

// 문 열림 모니터 중지 (타이머 해제)
function stopDoorOpenMonitor(reason = "") {
  if (graceTimer) clearTimeout(graceTimer);
  if (repeatTimer) clearInterval(repeatTimer);

  graceTimer = null;
  repeatTimer = null;
  startedAt = null;

  if (reason) console.log("[DOOR] monitor stopped:", reason);
}

// 문 열림 모니터 시작: 1분 경과 시부터 1분마다 음성 안내
function startDoorOpenMonitor(openedAt = Date.now()) {
  // 중복 방지 (OPEN이 연속 호출될 수 있으니)
  stopDoorOpenMonitor("restart");
  startedAt = openedAt;

  console.log("[DOOR] monitor started at", new Date(openedAt).toISOString());

  // 1분 뒤부터 알람 시작
  graceTimer = setTimeout(() => {
    playDoorOpenVoice(); // 1분 경과 시 즉시 1회
    repeatTimer = setInterval(playDoorOpenVoice, 60_000); // 이후 1분마다
  }, 60_000);
}

// 녹화 폴더명용 타임스탬프 생성 (YYYYMMDD_HHMMSS)
function makeTimestampFolderName(d = new Date()) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const HH = String(d.getHours()).padStart(2, "0");
  const MM = String(d.getMinutes()).padStart(2, "0");
  const SS = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}_${HH}${MM}${SS}`;
}

// 결제 세션별 카메라 녹화 루트 폴더 생성 (cwd 하위 타임스탬프 폴더)
function MakeCameraFolder() {
  const folderName = makeTimestampFolderName();
  const folderPath = path.join(process.cwd(), `${folderName}`)
  fs.mkdirSync(folderPath, { recursive: true });
  return { folderName, folderPath };
}

/**
 * 엣지 워터마크 (CRK-model-HG issue #8): 문 닫힘 시점에 이 세션에서 생성된
 * 존별 녹화 디렉토리 수를 센다 — CLOSE payload의 expected_triggers로 보내면
 * 모델 서버가 "그 수만큼 /trigger가 도착할 때까지" 확정을 보류한다.
 * (카메라 AVI 업로드가 CLOSE보다 늦어 0원 확정 + event rejected로 매출이
 * 누락되던 레이스의 인과적 해결 — 모델 쪽 시간 유예 3s 휴리스틱을 대체)
 *
 * 디렉토리 계약: <folderPath>/inference/zone_<N>/<timestamp>/{top,side}.avi
 * 녹화 시작 시점에 <timestamp> 디렉토리가 생기므로, 문이 닫힌 순간 존재하는
 * 디렉토리 수 = 이 세션의 트리거 총수 (아직 파일을 쓰는 중이어도 포함해야 함
 * — 그 트리거가 곧 도착한다는 것이 워터마크의 존재 이유).
 *
 * @param {string} folderPath 이 세션의 녹화 루트 (MakeCameraFolder 산출)
 * @returns {Object|null} {"4": 2, "5": 1} 형태. 녹화가 없거나 읽기 실패면
 *   null — 필드를 생략하면 모델이 시간 유예(MODEL__CLOSE__GRACE_S)로 폴백.
 */
function countZoneRecordings(folderPath) {
  try {
    const inferenceDir = path.join(folderPath, "inference");
    const counts = {};
    for (const entry of fs.readdirSync(inferenceDir, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const m = /^zone_(\d+)$/.exec(entry.name);
      if (!m) continue;
      const recordings = fs
        .readdirSync(path.join(inferenceDir, entry.name), { withFileTypes: true })
        .filter((e) => e.isDirectory()).length;
      if (recordings > 0) counts[m[1]] = recordings;
    }
    return Object.keys(counts).length > 0 ? counts : null;
  } catch (e) {
    // inference 디렉토리 없음(트리거 0건) 또는 읽기 실패 — 워터마크 생략(모델 유예 폴백)
    return null;
  }
}


// 모델 서버(/api/judge/multi-zone)에 추론 요청.
//  - checkOnly=true: 문 열기 전 모델 서버 상태를 1회만 확인 (400이면 BLOCK)
//  - checkOnly=false: success=true 응답이 올 때까지 intervalMs 주기로 polling
//  - productData(requestPayload)는 호출부와 공유되는 객체로, 폴링 중
//    session_id/expected_triggers(watermark)가 갱신되어 다음 요청에 반영된다.
async function modelPooling(productData, opts = {}) {
  const { intervalMs = 10_000, timeoutMs = 5 * 60_000, checkOnly = false } = opts;
  const started = Date.now();

  /**
   * 모델 서버 요청 스키마 (참고):
   * class MultiZoneRequest(BaseModel):
    """Multi-Zone 판단 요청."""

    session_id: str = Field(..., description="세션 ID (zone_{zone}_{YYMMDD}_{HHMMSS})")
    products: List[ProductInfo] = Field(
        default_factory=list,
        description="상품 목록 (선택, 무게 검증용)",
    )
   */

  // [모드 1] 검증 모드 (checkOnly: true)
  // 문 열기 전에 1번만 실행해서 400 에러인지 확인하는 용도
  if (checkOnly) {
      try {
          console.log("[Validation] Checking model server status...");
          // 타임아웃을 짧게(3초) 설정해 빠르게 확인
          const res = await axios.post(`${config.modelApi}/api/judge/multi-zone`, productData, { timeout: 3000 });
          
          if (res.data.status === 400) {
              throw new Error("400 Bad Request");
          }
          return true; // 통과
      } catch (error) {
          // 400 에러면 명확하게 에러 던짐
          if (error.response && error.response.status === 400) {
              throw new Error("BLOCK_400");
          }
          // 통신 에러 등은 일단 경고만 하고 통과시킬지, 막을지 정책 결정 (여기선 false 리턴)
          console.warn("[Validation] Network Error (Ignored):", error.message);
          return false; 
      }
  }

  // [모드 2] 반복 추론 모드 (기존 로직)
  while (true) {
    if (Date.now() - started > timeoutMs) throw new Error("Model inference timeout");

    try {
      const res = await axios.post(`${config.modelApi}/api/judge/multi-zone`, productData, { timeout: 30_000 });
      const data = res.data;
      // console.log('[MODEL-RESPONSE]', data);

      if (data.success === true) {
        return data;
      }
      // 반복 중 400이 뜨면 중단
      if (data.status === 400) { 
           throw new Error("Model rejected request (Status 400)");
      }
    } catch (e) {
      console.error("[Model] Request failed (Network/System):", e?.message || e);
      // 반복 중 에러 발생 시 재시도하거나 종료 (여기선 에러 로그만 찍고 재시도)
    }
    await delay(intervalMs);
  }
}


// 카메라 서버에 녹화 시작 요청 (save_path 하위에 영상 저장)
async function requestTopCameraON({ save_path }) {
  try {
    const request = await axios.post(`${config.cameraApi}/recording/start`, { save_path: save_path });
    // 성공 시 응답 데이터 반환
    if (request.status === 200) {
        console.log("Recording Started with Path:", request.data);
        return request.data;
    }
  } catch (error) {
    if (error.response) {
      console.error(`API Error (requestTopCameraON):`, error.response.data);
    } else {
      console.error(`Request Error (requestTopCameraON):`, error.message);
    }
    return false;
  }
}

// 카메라 서버에 녹화 종료 요청 (5초 대기 후 stop — 마지막 프레임 유실 방지)
async function requestCameraOFF() {
  try {
    await delay(5000);
    const response = await axios.post(`${config.cameraApi}/recording/stop`);
    if (response && response.status === 200) {;
      return response.data; // 저장된 파일 경로 정보 반환 
    } else {
      console.error("Stop Recording Unexpected Response:", response && response.data);
      return false;
    }
  } catch (error) {
    if (error.response) {
      console.error("Stop Recording API Error:", error.response.data);
    } else {
      console.error("Stop Recording Request Error:", error.message);
    }
    return false;
  }
}


// IO board SSE(doors 스트림)를 구독해 deadbolt가 닫힐 때까지 대기
function waitForDeadboltClose() {
    return new Promise((resolve, reject) => {
        const deadboltSource = new EventSource(`${config.ioboardApi}/sse?streams=doors`);

        const statusHandler = (event) => {
            if (!event.data) return;
            try {
                const data = JSON.parse(event.data);
                const currentState = data.deadbolt ? data.deadbolt.toUpperCase() : "";
                console.log(`Current Deadbolt State: ${currentState}`)
                
                if (closedStates.includes(currentState)) { 
                    deadboltSource.close(); // 리스너 해제 및 연결 종료
                    resolve({ 
                        state: currentState, 
                    });
                }
            } catch (err) {
                console.error("[Door] Event parsing error:", err);
            }
        };

        deadboltSource.addEventListener('door.update', statusHandler);

        deadboltSource.onerror = (err) => {
            console.error("[Door] SSE Error:", err);
            deadboltSource.close();
            reject(new Error("SSE connection error"));
        };
    });
}


// 결제 세션 전역 변수 초기화 (다음 결제와의 데이터 오염 방지)
function resetGlobalTokens() {
    paymentToken = null;
    samsungpayToken = null;
    rfidToken = null;
    preAmount = null;
    preAuthNum = null;
    preAuthDate = null;
    CardMethod = null;
    paymentResponse = null;
    inferenceResult = null;
}

let isProcessing = false;

// 결제 진입점: card terminal SSE를 구독하고
// 카드/삼성페이/RFID 토큰 수신 시 startProcess()로 결제 세션 시작
async function init() {
    // --- 1. 이벤트 리스너 설정 ---
    const TokenHandler = new EventSource(`${config.cardTerminalApi}/sse`);
    console.log("[Card Terminal] Listening for card tokens...");

    // 일반 카드 토큰 수신
    TokenHandler.addEventListener('tx_token_generate', (event) => {
        // e {"status": "Y", "vankey_hash": "0027057596824048aafeea42", "card_info": {"SERIAL_NUMBER": "", "ACQUIRER_ID": "003", "ACQUIRER_NAME": "\ud558\ub098\uce74\ub4dc", "ISSUER_ID": "003", "ISSUER_NAME": "\ud1a0\uc2a4\ubc45\ud06c\uce74\ub4dc", "MERCHANT_ID": "00915100663"}, "response_code": 0, "message": ""}
        try {
            const payload = JSON.parse(event.data);
            console.log('e', payload)
            paymentToken = payload.vankey_hash;
            // const CardMethod = token.startsWith("SPAYKEY") ? "S" : "N"
            CardMethod = 'N'
            // S = 삼성페이, N = 일반카드
            console.log('[CardToken] Token received:', paymentToken);
            
            // 토큰을 받으면 프로세스 시작 (비동기 호출)
            startProcess(paymentToken, CardMethod); 
        } catch (err) {
            console.error('[CardToken] Error parsing token:', err);
            setCardTerminalErrorState("30");
        }
    });

    /**
     {
        "amount": "000005000",
        "authorization_type": "PURCHASE",
        "display_message": "삼성페이 결제"
    }
     */
    // 삼성페이 토큰 수신
    TokenHandler.addEventListener('samsung_pay_init', async (event) => {
      console.log('samsungpay::::', event.data);
      if (event.data) {
        try {
          const response = await axios.post(`${config.cardTerminalApi}/payment/samsung-pay/approve`,
            {
              amount: "5",
              authorization_type: "PRE_AUTH",
              items: null,
            }
          );
          console.log('Samsung Pay Approval Response:', response.data);

          if (response.data.status == 'Y') {
            preAuthNum = response.data.authorization_number;
            preAuthDate = response.data.authorization_date;
            samsungpayToken = response.data.vankey;
            preAmount = '5';
            CardMethod = 'S';

            console.log('[Samsungpay-Token] Token received:', samsungpayToken);
            startProcess(samsungpayToken, CardMethod);

          } else if (response.data.status == 'N') {
            console.log('[Samsungpay-Token] Failed response_code:', response.data.response_code);
            const cardState = await CardTerminalStatusAPI(response.data.response_code);
            CardTerminalErrorState(cardState);
          }
        } catch (error) {
          console.error('Samsung Pay Approval Error:', error);
          CardTerminalErrorState("30");
        }
      }
    });

    // 사원증(RFID) 토큰 수신 — payload 예: {"data": "1763193013"}
    TokenHandler.addEventListener('rfid_init', async (event) => {
      try {
        const checkRFID = await ModelBrunchCheck({
          division_idx: config.divisionIdx,
          device_idx: config.deviceIdx || null,
          productIdx: null,
        });
        console.log('checkRFID.DATA ======>', checkRFID.DATA)
        const paymentType = checkRFID.DATA.device_list[0].payment_type;
        console.log('paymentType ======>', paymentType);
        if (paymentType == 'POINT') {
          const payload = JSON.parse(event.data);
          console.log('rfid::::', payload); // {"data": "1763193013"}
          const token = config.jwtToken
          if (payload.data) {
              // PNT한테 사원증 토큰 전송 후 유효성 검사하기
              rfidToken = payload.data;
              CardMethod = 'R' // R = RFID
              console.log('[RFID-Token] Token received:', rfidToken);
              await axios.get(`${config.restApi}/employee-uid/validate/${rfidToken}/${config.divisionIdx}`, {
                headers: {
                  Authorization: `Bearer ${token}`,
                },
              })
                  .then((response) => {
                    console.log('[PAYMENT] RFID Response:', response.data);
                    if (response.data.result == '0') {
                      console.log('[RFID-Token] Token received:', response.data.message);
                      startProcess(rfidToken, CardMethod); 
                    } else {
                      console.log('[RFID-Token] Token received:', response.data.message);
                    }
                  })
          }
        } else {
          console.log('[RFID-Token] This is not RFID Device');
        }
      } catch (error) {
        console.error('RFID Approval Error:', error);
        return; // 에러 시 중단
      }
    });
}

// --- 2. 프로세스 시작 및 상태 체크 ---
// health check(card terminal/deadbolt/loadcell/camera) 통과 시 Payments() 실행.
// getProcessing/setProcessing으로 동시 결제 진입을 차단(lock)한다.
async function startProcess(token, CardMethod) {
    // 1. 프로세스가 이미 실행 중이면 요청 무시 (busy check)
    if (getProcessing()) {
        console.warn('[SYSTEM] Device is busy. Ignoring new request');
        playDeviceRunningVoice()
        return;
    }
    // 2. 프로세스 잠금 (lock)
    setProcessing(true);
    try {
      // 진행 중인 로드셀 영점(calibrate)이 있으면 완료까지 대기 —
      // calibrate가 IO board 시리얼을 점유한 채 health check(GET /health,
      // timeout 5s)에 들어가면 타임아웃으로 세션이 거절될 수 있다
      await LoadcellZeroset.waitForIdle();
      const CameraStatus = await CameraStatusAPI()
      // let CardTerminalStatus = await CardTerminalStatusAPI()
      const CardTerminalStatus = '39'
      let DeadboltStatus = await DeadboltStatusAPI()
      const LoadcellStatus = await LoadcellStatusAPI()

      if (CardTerminalStatus == '39' && DeadboltStatus == '19' && LoadcellStatus == '29' && CameraStatus == '09') {
          console.log('[PAYMENT] Health check passed. Starting Payments process...');
          try {
              await Payments(token, CardMethod);
          } catch (error) {
              console.error("[PAYMENT] Process failed:", error);
          }
      } else {
          console.error('[PAYMENT] Health status is bad. Cannot run.');
          console.log('card method', CardMethod)
          if (CardMethod === 'S') {
            if (samsungpayToken || token) {
              await axios.post(`${config.cardTerminalApi}/payment/samsung-pay/cancel`,{
                  amount: preAmount,
                  original_authorization_date: preAuthDate.substring(0, 6),
                  original_authorization_number: preAuthNum,
                  vankey: samsungpayToken
              }).then((response) => {
                console.log('canceled samsung-pay : ', response.data)
              })
            }
          }
          return;
      }
    } catch (error) {
        console.error("[PAYMENT] Process failed:", error);
      } finally {
          // 3. 프로세스 잠금 해제 (unlock) — 성공/실패와 무관하게 반드시 실행
          setProcessing(false);

          // 다음 결제를 위해 전역 토큰 변수 초기화
          resetGlobalTokens();

          console.log("[SYSTEM] Process finished. Ready for next user.");
      }
}

let paymentResponse = null;
let inferenceResult = null;

// --- 3. 결제 및 제어 로직 ---
// 결제 세션 본체: 상품 조회 → 모델 서버 검증 → deadbolt OPEN → 녹화 시작
// → 추론 polling → deadbolt CLOSE 감지(watermark 첨부) → 녹화 종료
// → 추론 금액으로 카드 승인 → PNT로 결제 결과 전송
async function Payments(token, CardMethod) {
    const divisionIdx = config.divisionIdx;
    const deviceIdx = config.deviceIdx;

    // [3] 상품정보 조회
    const productList = await ProductList({
        division_idx: divisionIdx,
        // device_idx: null
        device_idx: deviceIdx, /// 성능평가용 장비 한정 테스트
    });
    // console.log("[ProductList] Data Loading Complete:", productList);


    // 상품 정보 추출
    let productData = []
    if (productList) { productData = productList.DATA.product_list }
    console.log(productData)
    // 2. API 스펙(ProductInfo)에 맞춰 매핑 (Mapping)
    const formattedProducts = productData.map((item) => {
      return {
        product_idx: item.product_idx,      // 매칭: P17355176366172772
        product_name: item.product_eng_name,    // 매칭: 광동) 제주 삼다수 500ml
        sale_price: parseInt(item.sale_price),        // 매칭: 1500 (Integer)
        product_weight: item.product_loadcell_weight, // 매칭: '530' (String, API 정의와 일치)
        has_loadcell: item.has_loadcell == 'Y' ? "true" : item.has_loadcell == 'N' ? "false" : 'null',
        stock_qty: parseInt(item.stock_qty)
      };
    });

    // 3. 최종 전송 데이터 구성 (MultiZoneRequest)
    const requestPayload = {
      session_id: 'OPEN',
      products: formattedProducts
    };

    try {
        // checkOnly: true 옵션을 줘서 1번만 찔러봄
        await modelPooling(requestPayload, { checkOnly: true });
        console.log("[Validation] Model server success");
    } catch (error) {
        if (error.message === "BLOCK_400") {
            console.error("[BLOCK] Model server access denined");
            return; // 여기서 함수 완전 종료 -> 문 안 열림
        }
        console.log("Validation Warning:", error.message);
    }
  
    // [4] 카메라 폴더 생성
    // const LOCAL_ROOT = path.resolve(process.cwd()); 
    const { folderName, folderPath } = MakeCameraFolder();
    console.log("[Camera] Folder Created:", folderPath, 'name:', folderName);

    // [5] 문 열기 (OPEN)
    const openResult = await callApiToControlDeadbolt("OPEN");
    if (openResult !== "OPEN" && openResult !== 'UNLOCK') throw new Error(`Failed to open door. Status: ${openResult}`) 

    // 모델 서버 추론 polling 시작 (비동기 — 문이 닫힌 뒤 결과를 await)
    const inferencePromise = modelPooling(requestPayload, { intervalMs: 10_000 });

    // 문 열림 알림 시작 (1분 경과 시부터 음성 안내)
    startDoorOpenMonitor(Date.now());

    // [6] 상단 카메라 ON 요청
    await requestTopCameraON({ save_path: folderPath});

    // [7] 로드셀 무게 정보 실시간 전달
    
    // [8] 로드셀 무게 변화 감지

    // [9] 데드볼트 상태 (close) (sensor → node) + (상단 카메라 off + folder snapshot) 저장 (node → camera python)
    try {
      // 1. deadbolt가 닫힐 때까지 대기
      const closeEventData = await waitForDeadboltClose();
      const state = closeEventData.state;
      if (state && closedStates.includes(state)) {
          // 엣지 워터마크 (CRK-model-HG issue #8): 문 닫힌 순간의 존별 녹화 수를
          // CLOSE payload에 실어 모델이 late trigger를 인과적으로 기다리게 한다.
          // 주의: 폴러(modelPooling)가 requestPayload 공유 객체를 매 주기 읽으므로
          // session_id='CLOSE'보다 반드시 먼저 설정해야 첫 CLOSE에 실린다 —
          // 모델은 최초 CLOSE(ACTIVE→PENDING_CLOSE)에서만 이 필드를 읽는다.
          const expectedTriggers = countZoneRecordings(folderPath);
          if (expectedTriggers) {
            requestPayload.expected_triggers = expectedTriggers;
            console.log("[WATERMARK] expected_triggers:", expectedTriggers);
          }
          requestPayload.session_id = 'CLOSE';
          stopDoorOpenMonitor("deadbolt closed");
          console.log("[DEADBOLT] Door closed detected:", closeEventData.state);
        // IO board 측 loadcell 기록 종료 (실패해도 결제 흐름은 계속)
        try {
          await axios.post(`${config.ioboardApi}/recording/stop`, {});
          await delay(5000);
        } catch (error) {
          // recording stop 실패는 무시 (결제 흐름 지속)
        }
        try {
          await requestCameraOFF();
        } catch (e) {
          console.error("[CAM] OFF failed:", e?.message || e);
        }
        // 로드셀 영점 1회 (fire-and-forget) — 문 닫힘·녹화/카메라 종료로
        // 로드셀 소비자가 없는 지점이며, calibrate 3~4초는 이후 추론 대기~
        // 결제 승인 구간에 흡수된다. 측정 보증(30분) 타이머도 여기서 리셋.
        LoadcellZeroset.fireAfterSession();
      } else {
        console.log("[DEADBOLT] Close event received but state not closed:", state);
      }
    } catch (error) {
        console.error("[Process Error] Door/Camera Sequence:", error);
        return; // 에러 시 중단
    }

    // [10] 모델 서버 추론 결과 수신 후 결제 승인 처리
    // product_idx 기준 상품 마스터 map 생성 (승인 items 구성용)
    const productMap = new Map(
        productData.map(p => [
            String(p.product_idx),
            p
        ])
    );

    try {
        inferenceResult = await inferencePromise;
        console.log("[Model] Inference Result:", inferenceResult);
        console.log('card method', CardMethod)
        // stopPolling = true;
        if (inferenceResult.success == false || inferenceResult.status == 'error'){
          console.error("[PAYMENT] Model inference failed or error occurred. Process aborted.");
          return;
        }
        if (inferenceResult.success == true && inferenceResult.totalPrice == 0) {
          // 0원일 때 선결제 취소되게 하기
          if (CardMethod === "S") {
            await axios.post(`${config.cardTerminalApi}/payment/samsung-pay/cancel`,{
                amount: preAmount,
                original_authorization_date: preAuthDate.substring(0, 6),
                original_authorization_number: preAuthNum,
                vankey: samsungpayToken
            }).then((response) => {
              console.log('canceled samsung-pay : ', response.data)
            })
          }
          console.log('total price is 0, running end')
          playZeroPayVoice();
          return;
        }
        if (inferenceResult.success == true && inferenceResult.products){
          // 결제 승인 요청
          const finalAmount = inferenceResult.totalPrice;
          // console.log('inferenceResult', inferenceResult)
          const products = inferenceResult.products
          // const items = products.map(product => ({
          //   name: product.name.slice(0, 5),
          //   quantity: Number(product.count),
          //   total_price: Number(product.price) * Number(product.count || 1)
          // }));
          const items = products.map(product => {
            const master = productMap.get(String(product.productIdx));
            if (!master) {
                console.warn(
                    `[PNT] Product master not found: productIdx=${product.productIdx}`
                );
            }
            return {
                name: (master.product_name || product.name || "").slice(0, 5),
                quantity: Number(product.count),
                total_price: Number(product.price) * Number(product.count || 1)
            };
          });
          // 삼성 페이
          if (CardMethod === "S") {
            // paymentResponse = await axios.post(`${config.cardTerminalApi}/payment/samsung-pay/approve`, {
            //         // amount: string(finalAmount),
            //         amount: '5',
            //         authorization_type: "PURCHASE",
            //         items,
            //         // display_message: "SamsungPay Payment"
            // });
            try {
              paymentResponse = await axios.post(
                `${config.cardTerminalApi}/payment/samsung-pay/approve`,
                {
                  // amount: string(finalAmount),
                  amount: '5',
                  items,
                  authorization_type: "PURCHASE",
                },
                // {
                //   timeout: 60000
                // }
              );
            } catch (error) {
              const pubCode = 'payment error'
              console.log('samsung-pay error::::', error)

              await sendCardErrorToPNT(
                pubCode,
                token,
                CardMethod,
                "1"
              );

              return;
            }
            await axios.post(`${config.cardTerminalApi}/payment/samsung-pay/cancel`,{
                amount: preAmount,
                original_authorization_date: preAuthDate.substring(0, 6),
                original_authorization_number: preAuthNum,
                vankey: samsungpayToken
            }).then((response) => {
              console.log('canceled samsung-pay : ', response.data)
            })
          }
          // 일반 카드
          else if (CardMethod === "N"){
            // paymentResponse = await axios.post(`${config.cardTerminalApi}/payment/token/approve`, {
            //         // amount: String(finalAmount),
            //         amount: '5',
            //         items,
            //         vankey_hash: String(paymentToken || token)
            // });
            try {
              paymentResponse = await axios.post(
                `${config.cardTerminalApi}/payment/token/approve`,
                {
                  // amount: string(finalAmount),
                  amount: '5',
                  items,
                  vankey_hash: String(paymentToken || token)
                }
              );
            } catch (error) {
              const pubCode = '1';

              await sendCardErrorToPNT(
                pubCode,
                token,
                CardMethod
              );

              console.error(
                "[CARD] Approval failed:",
                pubCode,
                error.response?.data || error.message
              );

              return;
            }
          }
          // RFID 사원증
          else if (CardMethod === "R"){
            paymentResponse = {
                    // amount: String(finalAmount),
                    amount: '5',
                    items
            };
          }
          else {  
            console.log("Undefined Card Method Detected.")
            return;
          }
          // 결제 결과 처리
          if (paymentResponse && paymentResponse.status === 200) {
              // const paymentAt = new Date()
              // 형식: "payment_at": "2026-02-21T00:46:59.000",
              const paymentAt = new Date().toISOString().replace("Z", "");
              console.log("[PAYMENT] Success:", paymentResponse.data, token);
              
              await sendToPNT(
                paymentResponse.data,
                inferenceResult,
                folderPath,
                paymentAt,
                CardMethod,
                productData,
                token
              )
          } else if (paymentResponse && CardMethod === "R") {
            // RFID 결제 정보 전송
            const paymentAt = new Date().toISOString().replace("Z", "");
            console.log("[PAYMENT] Success:", paymentResponse);
            playRFIDVoice()
            await sendToPNT(
              paymentResponse,
              inferenceResult,
              folderPath,
              paymentAt,
              CardMethod,
              productData,
              token
            )
          } else {
              console.error("[PAYMENT] Failed:", paymentResponse.data);

          }
        } else {
          console.log('inferenceResult not found')
        }
    } catch (error) {
        console.error("[MODEL/PAYMENT] Inference Request Failed:", error.message);
        return;
    }
}

module.exports = { Payments, router, init };