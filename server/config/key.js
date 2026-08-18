// ============================================================
// key.js
// 역할: NODE_ENV에 따라 prod.js / dev.js 설정을 선택하는 진입점.
//  - 서버 코드에서는 반드시 require("config/key")로 접근할 것
//    (config/dev, config/prod 직접 require 금지)
// ============================================================
if (process.env.NODE_ENV === 'production') {
    module.exports = require('./prod'); // 운영 환경 설정
} else {
    module.exports = require('./dev'); // 개발 환경 설정
}