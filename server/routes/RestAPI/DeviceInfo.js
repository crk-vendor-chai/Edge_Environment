// ============================================================
// DeviceInfo.js
// 역할: 클라우드(PNT/CHAI) REST API IF_13(장비 정보 조회) 호출 모듈.
//  - /chai/device/info 에 division_idx / device_idx 를 담아 POST 하고,
//    성공(result_cd === "S") 시 device_list 배열을 반환한다.
//  - 인증은 config.jwtToken(Bearer) 사용.
// ============================================================
const axios = require("axios");
const config = require("../../config/key");
const { v4: uuidv4 } = require("uuid");

// IF 규격(YYYYMMDDHHMMSS)의 날짜 문자열 생성
function formatIfDate(d = new Date()) {
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}`
         + `${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

const external = axios.create({
  baseURL: config.restApi, // https://apichaidev.atcrk.co.kr/api/v1
  timeout: 10000,
  headers: { "Content-Type": "application/json" },
});

/**
 * [IF_13] 장비 정보 조회 서비스
 * @param {string} divisionIdx - 매장코드 
 * @param {string} deviceIdx - 장비코드 
 */
async function DeviceInfo() {
    const token = config.jwtToken;
    if (!token) {
        throw new Error("JWT_TOKEN not set");
    }
    try {
        // 1. 정의서상 URL 경로 반영 (오타 수정) 
        const targetUrl = `${config.restApi}/chai/device/info`;

        // 현재 시간을 정의서 규격(YYYYMMDDHHMMSS)으로 변환
        const now = new Date();
        const formattedDate = now.toISOString().replace(/[-:T]/g, "").slice(0, 14);

        const payload = {
            HEADER: {
                IF_ID   : "IF_13",
                IF_SYSID: uuidv4(),
                IF_HOST : "CRKPNTCHAI",
                IF_DATE : formatIfDate()
            },
            DATA: {
                division_idx    : config.divisionIdx,
                device_idx      : config.deviceIdx,
            },
        };
        
        const response = await external.post("/chai/device/info", payload, {
            headers: {
            Authorization: `Bearer ${token}`,
            },
        });

        // 4. 응답 처리 [cite: 2, 3]
        if (response.status === 200 && response.data) {
            const { result_cd, result_msg } = response.data.DATA;

            if (result_cd === "S") {
                console.log(`[IF13] 조회 성공: ${result_msg}`);
                return response.data.DATA.device_list; // 장비 리스트 배열 반환 [cite: 3]
            } else {
                console.error(`[IF13] 조회 실패 (서버 에러): ${result_msg}`);
                return null;
            }
        } else {
            throw new Error(`HTTP 응답 오류: ${response.status}`);
        }

    } catch (error) {
        console.error(`[IF13] 통신 실패: ${error.message}`);
        throw error; 
    }
}

module.exports = { DeviceInfo };