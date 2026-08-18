// ============================================================
// PaymentProcessing.js
// 역할: payment 진행 중 여부를 나타내는 in-memory flag 관리 모듈.
//  - 결제 flow 가 동시에 중복 실행되지 않도록 getProcessing / setProcessing
//    으로 상태를 읽고 설정하는 단순 lock 역할을 한다.
// ============================================================
let isProcessing = false;

// 현재 payment 진행 중 여부 반환
function getProcessing() {
  return isProcessing;
}

// payment 진행 중 여부 설정 (true = 진행 중)
function setProcessing(v) {
  isProcessing = v;
}

// export default { getProcessing, setProcessing };
module.exports = { getProcessing, setProcessing };