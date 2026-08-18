// ============================================================
// ModelBrunchCheck.js
// 역할: 클라우드(PNT/CHAI) REST API IF_13 호출로 장비의 model brunch
//  정보를 확인(check)하는 모듈.
//  - /chai/device/info 에 division_idx / device_idx / product_idx 를 담아
//    POST 하고 응답 전체를 반환한다. 인증은 config.jwtToken(Bearer) 사용.
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

// 외부 API 호출 함수
// [IF_13] 장비 정보 조회를 통해 model brunch 상태를 확인
async function ModelBrunchCheck({
  divisionIdx = config.divisionIdx,
  deviceIdx = null,
  productIdx = null,
} = {}) {
  const token = config.jwtToken;
  if (!token) {
    throw new Error("JWT_TOKEN not set");
  }

  const payload = {
    HEADER: {
      IF_ID: "IF_13",
      IF_SYSID: uuidv4(),
      IF_HOST: "CRKPNTCHAI",
      IF_DATE: formatIfDate(),
    },
    DATA: {
      division_idx: divisionIdx,
      device_idx: deviceIdx,
      product_idx: productIdx,
    },
  };

  const r = await external.post("/chai/device/info", payload, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  return r.data;
}

module.exports = { ModelBrunchCheck };

// if (require.main === module) {
//   (async () => {
//     try {
//       console.log("[RestAPIClient] standalone start");
      
//       process.env.JWT_TOKEN = token;
//       process.env.JWT_TOKEN_AT = Date.now().toString();
//       console.log("[RestAPIClient] JWT_TOKEN set");

//       // REST API 호출
//       const data = await ModelBrunchEdit({
//         division_idx: config.divisionIdx,
//       });

//       console.log("[RestAPIClient] response:");
//       console.dir(data, { depth: null });
//     } catch (err) {
//       console.error("[RestAPIClient] error:");
//       console.error(err.message);
//       if (err.response) {
//         console.error(err.response.data);
//       }
//     }
//   })();
// }
