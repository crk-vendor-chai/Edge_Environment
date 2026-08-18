// ============================================================
// dev.js
// 역할: 개발 환경(NODE_ENV !== 'production')용 설정값.
//  - MongoDB / MinIO / MQTT 브로커 접속 정보
//  - 매장(divisionIdx) / 장비(deviceIdx) 식별자
//  - 클라우드 REST API 및 로컬 서비스 API 주소
//    (ioboardApi, cameraApi, cardTerminalApi, modelApi, aiServerApi)
// 주의: prod.js와 키 이름을 동일하게 유지할 것 (config/key.js에서 분기)
// ============================================================
module.exports = {
    mongoURI: 'mongodb://admin:%40crkchai2025@139.150.81.182:27017/CHAI?authSource=admin', // MongoDB로 연결
    //MINIO 연결
    minioAccessKey: 'admin',
    minioSecretKey: 'CrkMinio2026',
    minioURL: '139.150.8.82',
    minioBucket: 'chaiimage',
    // MQTT 브로커 접속
    mqttURL: 'mqtt://chaidev.atcrk.co.kr:1883',
    // 브로커 계정 정보는 고유해야함 -> 테스트 장비 매장
    mqttID: 'pnt',
    mqttPW: 'chai',

    // 사원증(RFID) 매장 브로커ID

    // 냉장 매장 고유번호
    // divisionIdx: 'DI17647205538493077',
    // 테스트용
    // divisionIdx: 'DI17790813642907755', // 냉장 학습용 (여의도점)
    // divisionIdx: 'DI17798460900133031', // 냉장 통테용 (부산점)
    // divisionIdx: 'DI17790813642907755', // 냉동 테스트용
    // divisionIdx: 'DI17846216186054023', // 냉장 테스트용 (한양대점)
    divisionIdx: 'DI17866730866133136', // 냉장 테스트용 (ITBT점)
    
    
    // 냉장 장비 고유번호
    // deviceIdx: 'DE17560868094789999',
    // deviceIdx: 'DE17683631997086480',
    // 테스트용
    // deviceIdx: 'DE17790815108130388', // 냉장 학습용
    // deviceIdx: 'DE17798461293792881', // 냉장 통테용
    // deviceIdx: 'DE17815818605453073', // 냉동 테스트용 
    // deviceIdx: 'DE17846226165932307',  // 냉장 테스트용 (한양대점)
    deviceIdx: 'DE17866760140506643',  // 냉장 테스트용 (ITBT점)
    
    // PNT RestAPI 연결
    restApi: 'https://apichaidev.atcrk.co.kr/api/v1',
    userId: 'chaitest',   // 냉장용
    // userId: 'chaitest2',   // 냉동용
    userPassword: 'iljin123!',
    get jwtToken() {
        return process.env.JWT_TOKEN;
    },
    get jwtTokenAt() {
        return process.env.JWT_TOKEN_AT;
    },
    ioboardApi: 'http://localhost:8000',
    // ioboardApi: 'http://192.168.0.20:8000',
    // 카메라 서버 API 설정
    cameraApi: "http://localhost:8003",
    // 카드단말기 서버 API 설정
    cardTerminalApi: "http://localhost:8001",
    // cardTerminalApi: "http://192.168.0.20:8001",
    // 모델 서버 API 설정
    modelApi: "http://localhost:8002",

    // 임베딩 모델 버전
    get modelVersion() {
        return process.env.MODEL_VERSION;
    },
    // modelVersion: "v1.0.0",
    // storageType: "C",
    // hasLoadcell: "Y",
    aiServerApi: 'http://139.150.8.82:2140',

    // RebootMqtt: 모델 코드/모델 저장 경로 및 engine 빌드 env 파일 경로
    // (env 미설정 시 기존 하드코딩 경로를 기본값으로 사용 -> 동작 불변)
    modelCodesDir: process.env.MODEL_CODES_DIR || '/home/chai/Desktop/Codes/CRK-model',
    modelDir: process.env.MODEL_DIR || '/home/chai/Desktop/Codes/CRK-model/models',
    engineBuildEnvFile: process.env.ENGINE_BUILD_ENV_FILE || '/home/chai/Desktop/crk-model-build.txt',

}