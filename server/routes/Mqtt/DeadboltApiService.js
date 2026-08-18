// ============================================================
// DeadboltApiService.js
// 역할: IO board Python 서버의 deadbolt 제어 API(POST {ioboardApi}/deadbolt)를
//   호출하여 door open/close를 수행하고, 응답 상태를 정규화·검증한다.
//   요청한 상태로 전환되지 않으면 health check용 deadbolt 오류 코드("11")를
//   설정(DeadboltTerminalErrorState)하고 에러를 throw한다.
// 사용처: DoorCollect.js, ManualDeadbolt.js
// ============================================================
const axios = require("axios");
const config = require("../../config/key");

const API_HOST = config.ioboardApi; // 기본값 설정
const API_ENDPOINT = "/deadbolt"; // 상세 경로

const { DeadboltTerminalErrorState } = require("./HealthMqtt");

// 다양한 표기의 deadbolt 상태 문자열을 "UNLOCK"/"LOCK"으로 정규화
function normalizeDeadboltState(state) {
  const s = String(state || "").toUpperCase();

  if (["UNLOCK", "UNLOCKED", "OPEN", "OPENED"].includes(s)) {
    return "UNLOCK";
  }

  if (["LOCK", "LOCKED", "CLOSE", "CLOSED"].includes(s)) {
    return "LOCK";
  }

  return s;
}

/**
 * API 서버에 도어 제어 요청을 보냅니다.
 * Python 서버의 스펙: POST /deadbolt, Body: { "state": "OPEN" | "CLOSE" }
 * @param {string} targetState - "OPEN" or "CLOSE"
 * @returns {Promise<string>} - 서버가 반환한 최종 상태 ("OPEN" or "CLOSE")
 */
async function callApiToControlDeadbolt(targetState) {
  const DEADBOLT_API_URL = `${API_HOST}${API_ENDPOINT}`;
  try {
    console.log(`[API] Sending Request to ${DEADBOLT_API_URL} (state: ${targetState})...`);

    // POST 요청 전송
    const response = await axios.post(DEADBOLT_API_URL, {
      action: targetState 
    }, {
      timeout: 5000 // 5초 타임아웃
    });
    // console.log(response)

    // API 응답 확인
    const rawFinalState = response.data.state;
    const finalState = normalizeDeadboltState(rawFinalState);
    const expectedState = targetState === "OPEN" ? "UNLOCK" : "LOCK";
    console.log(`[API] Response Received. Final State: ${finalState}`);

    console.log("targetState =", targetState);
    console.log("rawFinalState =", rawFinalState);
    console.log("finalState =", finalState);
    console.log("expectedState =", expectedState);

    // 데드볼트 작동 불량인 경우 --> 요청한 값에서 변화가 없는 경우 error 처리
    if (finalState !== expectedState) {
      DeadboltTerminalErrorState("11");
      throw new Error(
        `Deadbolt operation failed. target=${targetState}, expected=${expectedState}, final=${finalState}`
      );}
    
    return finalState;

  } catch (error) {
    // 에러 발생 시 상세 내용 처리
    if (error.response) {
      throw new Error(`Server Error (${error.response.status}): ${JSON.stringify(error.response.data)}`);
    } else if (error.request) {
      throw new Error("No response from server (Network Error)");
    } else {
      throw new Error(error.message);
    }
  }
}

module.exports = { callApiToControlDeadbolt };