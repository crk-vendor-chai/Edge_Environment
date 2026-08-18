// ============================================================
// index.js
// 역할: 자판기 엣지 서버(Node.js/Express)의 엔트리 포인트.
//  - MongoDB / MinIO 연결 초기화
//  - Express 미들웨어 및 REST 라우트(payment, product) 등록
//  - MQTT 모듈(routes/mqtt.js) 초기화: health check, collect,
//    deadbolt 제어, repayment 등 MQTT 흐름 기동
//  - /health 엔드포인트 및 graceful shutdown(SIGINT/SIGTERM) 처리
// ============================================================
const express = require("express");
const app = express();
const path = require("path");
const cors = require('cors')
const axios = require('axios');

const bodyParser = require("body-parser");
const cookieParser = require("cookie-parser");

require("dotenv").config({ path: path.resolve(__dirname, "../.env") });

const config = require("./config/key");

// ============================================
// MongoDB 연결
// ============================================
const mongoose = require("mongoose");
if (config.mongoURI) {
    mongoose.connect(config.mongoURI)
        .then(() => console.log('[DB] MongoDB Connected'))
        .catch(err => console.error('[DB] MongoDB connection error:', err.message));
} else {
    console.log('[DB] MongoDB URI not configured, skipping connection');
}

// ============================================
// MinIO 연결
// ============================================
const Minio = require("minio");

const minioClient = new Minio.Client({
  endPoint: config.minioURL,
  port: 9000,
  useSSL: false,
  accessKey: config.minioAccessKey,
  secretKey: config.minioSecretKey,
});

app.locals.minioClient = minioClient;
app.locals.minioBucket = "chaiimage"; // 또는 config로

(async () => {
  try {
    const buckets = await minioClient.listBuckets();
    console.log("[MINIO] Buckets:", buckets);
  } catch (err) {
    console.error("[MINIO] error:", err);
  }
})();

// ============================================
// Middleware 설정
// ============================================
app.use(cors())
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(cookieParser());

// 요청 로깅 (개발용)
app.use(function (req, res, next) {
    if (process.env.LOG_LEVEL === 'debug') {
        console.log(`[HTTP] ${req.method} ${req.path}`);
    }
    return next();
});

// ============================================
// Health Check Endpoint
// ============================================
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        service: 'node_server',
        timestamp: new Date().toISOString(),
        mongodb: mongoose.connection.readyState === 1 ? 'connected' : 'disconnected',
        minio: minioClient ? 'configured' : 'not configured'
    });
});

// ============================================
// Routes 설정
// ============================================

// Auth 모듈 (라우터로 마운트하지 않고 devAutoLogin만 사용)
const authModule = require("./routes/auth");

// 서버 기동 시 클라우드 REST API 자동 로그인 (JWT 토큰 확보)
(async () => {
    try {
        await authModule.devAutoLogin();
    } catch (e) {
        console.error("[APP] dev auto login failed:", e?.message || e);
    }
})();

// Payment 모듈 (라우터로 마운트하지 않고 init()으로 MQTT 기반 결제 흐름만 기동)
const paymentRouter = require("./routes/RestAPI/Payments");

(async () => {
  try {
    await paymentRouter.init();
  } catch (e) {
    console.error("[APP] dev auto payment failed:", e?.message || e);
  }
})();

// Product 라우트 (상품 이미지 업로드/조회 REST API)
const productRouter = require("./routes/AIServer/Products");
app.use("/api", productRouter);
app.use('/products', express.static('uploads'));
app.use('/uploads/images', express.static('images'));

// Serve static assets if in production
if (process.env.NODE_ENV === "production") {

  // Set static folder   
  // All the javascript and css files will be read and served from this folder
  app.use(express.static("client/build"));

  // index.html for all page routes    html or routing and naviagtion
  app.get("*", (req, res) => {
    res.sendFile(path.resolve(__dirname, "../client", "build", "index.html"));
  });
}

// MQTT 라우트
const mqttModule = require("./routes/mqtt");
const { disconnect } = require("./routes/Mqtt/MqttClient");
app.use('/', mqttModule.router);
console.log('[APP] MQTT module loaded', mqttModule);

// MQTT 초기화
mqttModule.init().catch((e) => {
    console.error('[APP] MQTT init during server start failed:', e?.message || e);
});

// 로드셀 영점(zeroset) 자동화 초기화
//  - 측정 보증 30분 경과 기준 주기 실행 + 기동 직후 1회
//  - 세션 종료 후 1회는 Payments.js가 직접 발사
require("./routes/RestAPI/LoadcellZeroset").init();

// ============================================
// 서버 시작
// ============================================
const port = process.env.PORT || 8888

app.listen(port, () => {
    console.log(`[APP] Server Listening on ${port}`)
});

// ============================================
// Graceful Shutdown
// ============================================
process.on("SIGINT", async () => {
    console.log("\n[APP] SIGINT received. Shutting down...");
    try { await disconnect(); } catch (e) { console.error(e); }
    if (mongoose.connection.readyState === 1) {
        await mongoose.connection.close();
        console.log('[DB] MongoDB disconnected');
    }
    process.exit(0);
});

process.on("SIGTERM", async () => {
    console.log("\n[APP] SIGTERM received. Shutting down...");
    try { await disconnect(); } catch (e) { console.error(e); }
    if (mongoose.connection.readyState === 1) {
        await mongoose.connection.close();
        console.log('[DB] MongoDB disconnected');
    }
    process.exit(0);
});