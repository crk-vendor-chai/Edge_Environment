// ============================================================
// TrainingStore.js
// 역할: 클라우드(PNT/CHAI) REST API IF_07(학습 진행 상태 전달) 호출 모듈.
//  - product 별 training 상태(productMap + trainingStatus)를 product_list
//    배열로 변환하여 /chai/training/store 에 POST 한다.
//  - 인증은 config.jwtToken(Bearer) 사용.
// ============================================================
const axios = require("axios");
const config = require("../../config/dev");
const { v4: uuidv4 } = require("uuid");
// const { divisionIdx } = require("../../config/prod");

let currentTrainingStatus = null;

// 현재 저장된 training 상태 반환
function getTrainingStatus() {
  return currentTrainingStatus;
}

const external = axios.create({
  baseURL: config.restApi, // https://apichaidev.atcrk.co.kr/api/v1
  timeout: 10000,
  headers: { "Content-Type": "application/json" },
});

/**
 * [IF_07] 학습 진행 상태 전달
 */
async function TrainingStore(productMap, trainingStatus) {
    console.log(
        "MAKE TRAINING STORE FOR IF07::::",
        productMap, trainingStatus
    );
    // console.log(`MAKE TRAINING STORE FOR IF07:::: ${productIdx}, ${product_eng_name}, ${training_status}`)

    const normalizedTrainingStatus = String(trainingStatus);

    // const productList = Array.from(productMap.values()).map(
    //         (product) => ({
    //             division_idx: config.divisionIdx,
    //             device_idx: config.deviceIdx,
    //             product_idx: String(product.product_idx),
    //             product_eng_name:product.product_eng_name,
    //             training_status:normalizedTrainingStatus,
    //         })
    //     );

    try {
         /*
        * Map은 JSON으로 직접 전송할 수 없으므로 배열로 변환합니다.
        * division_idx, device_idx는 현재 서버 config가 아닌
        * 실제 학습 대상 장비 정보를 사용합니다.
        */
        const productList = Array.from(productMap.values()).map((product) => {
            const trainingDivisionIdx = product.division_idx ?? product.divisionIdx;
            const trainingDeviceIdx = product.device_idx ?? product.deviceIdx;

            if (!trainingDivisionIdx) {
            throw new Error(`[IF07] training division_idx is required: product_idx=${product.product_idx}`);
            }

            if (!trainingDeviceIdx) {
            throw new Error(`[IF07] training device_idx is required: product_idx=${product.product_idx}`);
            }

            return {
            division_idx: String(trainingDivisionIdx),
            device_idx: String(trainingDeviceIdx),
            product_idx: String(product.product_idx),
            product_eng_name: product.product_eng_name,
            training_status: normalizedTrainingStatus,
            };
        });
        const token = config.jwtToken;
        if (!token) {
            throw new Error("JWT_TOKEN not set");
        }

        // 1. 정의서상 URL 경로 반영 (오타 수정) 
        const targetUrl = `${config.restApi}/chai/training/store`;

        // 현재 시간을 정의서 규격(YYYYMMDDHHMMSS)으로 변환
        const now = new Date();
        const formattedDate = now.toISOString().replace(/[-:T]/g, "").slice(0, 14);

        const payload = {
            HEADER: {
                IF_ID   : "IF_07",
                IF_SYSID: uuidv4(),
                IF_HOST : "CRKPNTCHAI",
                IF_DATE : formattedDate
            },
            DATA: {
                product_list: productList,
                // product_list: [{
                //     product_idx: String(productIdx),
                //     product_eng_name: product_eng_name,
                //     training_status: String(training_status)
                // }],
            }
        };

        console.log("[IF07] request payload:", JSON.stringify(payload, null, 2));
        
        const response = await external.post(targetUrl, payload, {
            headers: {
                Authorization: `Bearer ${token}`,
            },
        });
        currentTrainingStatus = normalizedTrainingStatus;
        return response.data;

    } catch (error) {
        console.error(`[IF07] 통신 실패: ${error.message}`);
        console.error("[IF07] 통신 실패 status:", error.response?.status);
        console.error("[IF07] 통신 실패 data:", error.response?.data);
        throw error;
    }
}

module.exports = { TrainingStore, getTrainingStatus };