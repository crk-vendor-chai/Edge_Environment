// ============================================================
// prod.js
// 역할: 운영 환경(NODE_ENV === 'production')용 설정값.
//  - 모든 값을 환경변수(.env)에서 주입받는다.
//  - dev.js와 키 이름을 동일하게 유지할 것 (config/key.js에서 분기)
// ============================================================
module.exports = {
    mongoURI:process.env.MONGO_URI,

    mqttURL: process.env.MQTT_URL,
    mqttID: process.env.MQTT_USER,
    mqttPW: process.env.MQTT_PASS,
    
    divisionIdx: process.env.DIVISION_IDX,
    deviceIdx: process.env.DEVICE_IDX,
    
    restApi: process.env.REST_API,
    userId: process.env.USER_ID,
    userPassword: process.env.USER_PASSWORD,
    jwtToken: process.env.JWT_TOKEN,
    jwtTokenAt: process.env.JWT_TOKEN_AT,
    
    minioAccessKey: process.env.MINIO_ACCESS_KEY,
    minioSecretKey: process.env.MINIO_SECRET_KEY,
    minioURL: process.env.MINIO_URL,
    minioBucket: process.env.MINIO_BUCKET,
    
    ioboardApi: process.env.IOBOARD_API,
    cameraApi: process.env.CAMERA_API,
    cardTerminalApi: process.env.CARD_TERMINAL_API,
    modelApi: process.env.MODEL_API,

    modelVersion: process.env.MODEL_VERSION,

    // AI 학습 서버 API 주소 (dev.js의 aiServerApi와 키 일치)
    aiServerApi: process.env.AI_SERVER_API,

    // RebootMqtt: 모델 코드/모델 저장 경로 및 engine 빌드 env 파일 경로
    // (env 미설정 시 기존 하드코딩 경로를 기본값으로 사용 -> 동작 불변)
    modelCodesDir: process.env.MODEL_CODES_DIR || '/home/chai/Desktop/Codes/CRK-model',
    modelDir: process.env.MODEL_DIR || '/home/chai/Desktop/Codes/CRK-model/models',
    engineBuildEnvFile: process.env.ENGINE_BUILD_ENV_FILE || '/home/chai/Desktop/crk-model-build.txt',
}