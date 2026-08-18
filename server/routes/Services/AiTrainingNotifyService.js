// ============================================================
// AiTrainingNotifyService.js
// 역할: collect(데이터 수집) 완료 후 클라우드(PNT/CHAI)에 상품별 학습 상태를
//       통보하는 서비스. RestAPI/TrainingStore(IF07)를 호출해 training_status를
//       전달하고, 응답 result_cd 기반의 success 여부로 정규화해 반환한다.
// 사용처: routes/Mqtt/AckCollect.js (collect ack 흐름에서 호출)
// ============================================================
require("dotenv").config();
const { TrainingStore } = require("../../routes/RestAPI/TrainingStore");

// 단일 상품의 training_status(기본 "2": 데이터 수집 완료)를 TrainingStore(IF07)로 통보.
// productIdx 필수. 내부적으로 Map 형태(productMap)로 감싸 TrainingStore에 전달하며,
// 응답의 result_cd === "S" 여부를 success로 정규화해 { success, raw } 형태로 반환한다.
async function notifyTrainingStore({
  productIdx,
  productEngName,
  divisionIdx,
  deviceIdx,
  trainingStatus = "2",
} = {}) {
  if (!productIdx) throw new Error("productIdx is required");
  if (!divisionIdx) throw new Error("divisionIdx is required");
  if (!deviceIdx) throw new Error("deviceIdx is required");

  // const res = await TrainingStore({
  //   productIdx,
  //   product_eng_name: productEngName,
  //   training_status: trainingStatus,
  // });

  const productMap = new Map();

  // productMap.set(String(productIdx), {
  //   product_idx: productIdx,
  //   product_eng_name: productEngName,
  // });

  // IF07에 전달할 상품 및 학습 대상 장비 정보 구성
  productMap.set(String(productIdx), {
    product_idx: productIdx,
    product_eng_name: productEngName,
    division_idx: divisionIdx,
    device_idx: deviceIdx,
  });

  const res = await TrainingStore(productMap, trainingStatus);

  console.log('CHECK IF07 res::::', res)

  return {
    success: res?.result_cd === "S" || res?.DATA?.result_cd === "S",
    raw: res,
  };
}

// 여러 상품을 순차(직렬)로 notifyTrainingStore 호출해 통보하고,
// 상품별 { productIdx, productEngName, success, raw } 결과 배열을 반환한다.
async function notifyTrainingStoreMany(products = []) {
  const results = [];

  for (const p of products) {
    const result = await notifyTrainingStore({
      productIdx: p.productIdx,
      productEngName: p.productEngName,
      divisionIdx: p.divisionIdx,
      deviceIdx: p.deviceIdx,
      trainingStatus: p.trainingStatus || "2",
    });

    results.push({
      productIdx: p.productIdx,
      productEngName: p.productEngName,
      divisionIdx: p.divisionIdx,
      deviceIdx: p.deviceIdx,
      success: result.success,
      raw: result.raw,
    });
  }

  return results;
}

module.exports = {
  notifyTrainingStore,
  notifyTrainingStoreMany,
};