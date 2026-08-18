// server/routes/Mqtt/AckCollect.js
// ============================================================
// AckCollect.js
// 역할: 클라우드(PNT/CHAI)의 상품 수집(collect) 명령을 MQTT로 수신하여
//       2단계 카메라 촬영(camera 2 -> camera 0)과 loadcell 무게 기록을 수행하고,
//       촬영 이미지를 MinIO에 업로드한 뒤 MongoDB 메타데이터를 동기화하고
//       IF_06 ack 발행 및 AI 학습 notify(IF07)까지 처리하는 모듈.
// MQTT topic: 구독 chai/device/{deviceIdx}/cmd/collect
//             발행 chai/device/{deviceIdx}/ack/collect (IF_06 ack)
// 외부 연동: IO board API(deadbolt 제어, loadcell recording, SSE door 상태),
//            camera API(/sampling/start,stop), MinIO, MongoDB, AiTrainingNotifyService
// ============================================================
require("dotenv").config();

const path = require("path");
const axios = require("axios");
const fs = require("fs");
const Minio = require("minio");
const mongoose = require("mongoose");
const { EventSource } = require("eventsource");
const { getClient } = require("./MqttClient");
const config = require("../../config/dev");
const {
  DeadboltStatusAPI,
  LoadcellStatusAPI,
  CameraStatusAPI,
} = require("./HealthMqtt");
const { callApiToControlDeadbolt } = require("./DeadboltApiService");
const { ProductList } = require("../RestAPI/ProductList");

const { ProductUpload } = require("../../model/ProductUpload");
const { DivisionUpload } = require("../../model/DivisionUpload");
const { DeviceTypeUpload } = require("../../model/DeviceTypeUpload");

const { getLatestCollectOption, setLatestTrainingTarget } = require("./DoorCollect");
const { syncAnnotationLabels } = require("../Services/AnnotationLabelSyncService");
const aiNotifyService = require("../Services/AiTrainingNotifyService");

const SUB_TOPIC = `chai/device/${config.deviceIdx}/cmd/collect`;
const PUB_TOPIC = `chai/device/${config.deviceIdx}/ack/collect`;

let client = null;
let chain = Promise.resolve();
let mongoConnectPromise = null;

const collectSessions = new Map();

const minioClient = new Minio.Client({
  endPoint: config.minioURL,
  port: 9000,
  useSSL: false,
  accessKey: config.minioAccessKey,
  secretKey: config.minioSecretKey,
});

// MongoDB 연결을 보장한다. 이미 연결돼 있으면 즉시 반환하고,
// 연결 중이면 동일한 connect Promise를 재사용해 중복 연결을 막는다.
async function ensureMongoConnected() {
  if (mongoose.connection.readyState === 1) return;

  if (!mongoConnectPromise) {
    mongoConnectPromise = mongoose.connect(config.mongoURI);
  }

  await mongoConnectPromise;
}

// 폴더명/객체 key에 쓸 수 없는 문자를 "_"로 치환해 안전한 문자열로 만든다.
function safe(s) {
  return String(s || "").replace(/[^a-zA-Z0-9._-]/g, "_");
}

// 수집 폴더명에 붙일 타임스탬프 문자열(YYYYMMDD_HHMMSS)을 생성한다.
function makeTimestampFolderName(d = new Date()) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const HH = String(d.getHours()).padStart(2, "0");
  const MM = String(d.getMinutes()).padStart(2, "0");
  const SS = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}_${HH}${MM}${SS}`;
}

// ack HEADER의 IF_DATE 필드용 타임스탬프(YYYYMMDDHHMMSS)를 생성한다.
function makeIFDate(d = new Date()) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const HH = String(d.getHours()).padStart(2, "0");
  const MM = String(d.getMinutes()).padStart(2, "0");
  const SS = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}${HH}${MM}${SS}`;
}

// collectSessions Map의 key(productIdx 문자열)를 생성한다.
function makeSessionKey(productIdx) {
  return String(productIdx);
}

// 수집 결과 ack payload를 MQTT topic(ack/collect)으로 발행한다.
// MQTT client가 없거나 연결이 끊긴 경우에는 에러 로그만 남기고 발행을 생략한다.
function publishAck(payload) {
  if (!client) {
    console.error("[AckCollect] MQTT client is null");
    return;
  }
  if (!client.connected) {
    console.error("[AckCollect] MQTT client is not connected. Cannot publish ACK.");
    return;
  }
  // 수집 시작 - 수집 종료에 대해서 pub 해줘야함
  client.publish(PUB_TOPIC, JSON.stringify(payload), { qos: 1 }, (err) => {
    if (err) {
      console.error("[AckCollect] ACK publish failed:", err);
      return;
    }

    console.log("[AckCollect] ACK published:", PUB_TOPIC);
    console.log('[IF06] payload:', payload)
  });
}

// IF_06 규격의 ack payload(HEADER + DATA)를 생성한다.
// 요청의 IF_SYSID를 그대로 되돌려주고, 수집 상태/상품 정보/result 코드를 담는다.
function makeAckPayload({
  reqSysid,
  device_idx,
  division_idx,
  collectState,
  productIdx,
  productEngName,
  categoryIdx,
  isNew,
  productLoadcellWeight,
  resultCd = "S",
  resultMsg = "success",
  health = {},
  extraData = {},
}) {
  return {
    HEADER: {
      IF_ID: "IF_06",
      IF_SYSID: reqSysid,
      IF_HOST: "CRKPNTCHAI",
      IF_DATE: makeIFDate(),
    },

    DATA: {
      device_idx: device_idx,
      division_idx: division_idx,
      product_idx: productIdx,
      collect_state: collectState,
      product_eng_name: productEngName,
      category_idx: categoryIdx,
      is_new: isNew,
      product_loadcell_weight: productLoadcellWeight,
      result_cd: String(resultCd ?? "F"),
      result_msg: String(resultMsg ?? "New product collect is failed"),
      ...extraData,
    },
  };
}

// IO board의 SSE(/sse?streams=doors)로 deadbolt 상태를 1회 조회하여
// "OPEN"/"CLOSE"/"UNKNOWN" 중 하나로 반환한다. 3초 timeout 시 "UNKNOWN".
async function fetchCurrentDoorState() {
  return new Promise((resolve) => {
    const url = `${config.ioboardApi}/sse?streams=doors`;

    let evtSource;

    try {
      evtSource = new EventSource(url);
    } catch (err) {
      console.error("[DoorCheck] EventSource create failed:", err.message);
      resolve("UNKNOWN");
      return;
    }

    const timeout = setTimeout(() => {
      evtSource.close();
      console.warn("[DoorCheck] Timeout");
      resolve("UNKNOWN");
    }, 3000);

    evtSource.addEventListener("door.update", (event) => {
      if (!event.data) return;

      try {
        const data = JSON.parse(event.data);
        const rawState = data.deadbolt ? String(data.deadbolt).toUpperCase() : "";
        const closedStates = ["LOCK", "LOCKED", "CLOSE", "CLOSED"];
        const finalState = closedStates.includes(rawState) ? "CLOSE" : "OPEN";

        clearTimeout(timeout);
        evtSource.close();
        resolve(finalState);
      } catch {
        clearTimeout(timeout);
        evtSource.close();
        resolve("UNKNOWN");
      }
    });

    evtSource.onerror = () => {
      clearTimeout(timeout);
      evtSource.close();
      resolve("UNKNOWN");
    };
  });
}

// camera/deadbolt/loadcell health check와 현재 door 상태를 병렬로 조회하고,
// 세 장치가 모두 정상 코드(09/19/29)인지 여부(isSuccess)를 함께 반환한다.
async function ProductCollectionHealth() {
  // const CameraStatus = await CameraStatusAPI();
  // const DeadboltHealth = await DeadboltStatusAPI();
  // const LoadcellHealth = await LoadcellStatusAPI();
  // const CurrentDoorState = await fetchCurrentDoorState();

  const [
    CameraStatus,
    DeadboltHealth,
    LoadcellHealth,
    CurrentDoorState,
  ] = await Promise.all([
    CameraStatusAPI(),
    DeadboltStatusAPI(),
    LoadcellStatusAPI(),
    fetchCurrentDoorState(),
  ]);

  const isHealthOk =
    CameraStatus === "09" &&
    DeadboltHealth === "19" &&
    LoadcellHealth === "29";

  console.log('[ACK-CHECK] isHealthOk: ', isHealthOk)

  return {
    CameraStatus,
    DeadboltHealth,
    LoadcellHealth,
    CurrentDoorState,
    isSuccess: isHealthOk,
    resultMsg: isHealthOk ? "status access" : "status error",
  };
}

// camera API에 촬영(sampling) 시작을 요청한다. 저장 폴더를 먼저 만들고
// 지정한 camera index 목록으로 recording을 시작한다.
async function cameraStartSampling(savePath, cameraIndices = [0, 2]) {
  // 폴더 먼저 생성
  fs.mkdirSync(savePath, { recursive: true });
  const url = `${config.cameraApi}/sampling/start`;

  const body = {
    save_path: savePath,
    cameras: cameraIndices,
  };

  console.log(`[Sampling] Starting... Path: ${savePath}`);

  const response = await axios.post(url, body);

  if (response.status === 200 && response.data?.status === "recording started") {
    console.log("[Sampling] Successfully started");
    return response.data;
  }

  throw new Error(`[Sampling] Start unexpected response: ${JSON.stringify(response.data)}`);
}

// camera API에 촬영(sampling) 종료를 요청한다.
async function cameraStopSampling() {
  const url = `${config.cameraApi}/sampling/stop`;

  console.log("[Sampling] Stopping...");

  const response = await axios.post(url);

  if (response.status === 200 && response.data?.status === "recording stopped") {
    console.log("[Sampling] Successfully stopped");
    return response.data;
  }

  throw new Error(`[Sampling] Stop unexpected response: ${JSON.stringify(response.data)}`);
}

// IO board API에 loadcell 시계열 recording 시작을 요청한다.
async function startLoadcellRecording() {
  const response = await axios.post(`${config.ioboardApi}/recording/start`);

  if (response.status === 200) {
    console.log("[Loadcell] Recording started");
    return response.data;
  }

  throw new Error(`[Loadcell] Start failed: ${JSON.stringify(response.data)}`);
}

// 1) 녹화 종료 (응답은 무게가 아님, 그냥 "끝났다" 신호로만 사용)
async function stopLoadcellRecording() {
  const response = await axios.post(`${config.ioboardApi}/recording/stop`);
  if (response.status === 200) {
    console.log("[Loadcell] Recording stopped");
    return true;
  }
  throw new Error(`[Loadcell] Stop failed: ${JSON.stringify(response.data)}`);
}

// 2) 기록된 데이터 조회 → 무게 계산
async function fetchRecordedLoadcellData() {
  const response = await axios.get(`${config.ioboardApi}/recording/data`);
  if (response.status !== 200) {
    throw new Error(`[Loadcell] Data fetch failed: ${JSON.stringify(response.data)}`);
  }
  return response.data?.logs || [];
}

// 3) 시계열에서 최종 무게 산출 (정책에 따라 골라야 함, 아래는 한 가지 예)
function computeProductWeight(logs) {
  //
  if (!Array.isArray(logs) || logs.length === 0) return 0;

  const CHANNEL_INDEX = 2;      // 3번째 로드셀
  const OFFSET_MS = 4000;       // startLoadcellRecording 이후 4초
  const WINDOW_MS = 3200;       // IO-BOARD 샘플링 0.8s 기준 최소 4샘플 확보

  const validLogs = logs
    .filter(snap => snap?.timestamp && Array.isArray(snap?.loadcells))
    .map(snap => ({
      timestamp: new Date(snap.timestamp).getTime(),
      value: parseInt(snap.loadcells[CHANNEL_INDEX], 10),
      raw: snap,
    }))
    .filter(x => Number.isFinite(x.timestamp) && !Number.isNaN(x.value))
    .sort((a, b) => a.timestamp - b.timestamp);

  if (validLogs.length === 0) return 0;

  // 기록 데이터가 startLoadcellRecording 호출 시점부터 쌓인다는 전제
  const recordingStartTime = validLogs[0].timestamp;
  const windowStartTime = recordingStartTime + OFFSET_MS;
  const windowEndTime = windowStartTime + WINDOW_MS;

  const values = validLogs
    .filter(x => x.timestamp >= windowStartTime && x.timestamp < windowEndTime)
    .map(x => x.value);

  if (values.length === 0) {
    console.warn("[Loadcell] No values found in target window");
    return 0;
  }

  const avg = values.reduce((sum, v) => sum + v, 0) / values.length;

  // console.log("[Loadcell] weight window:", {
  //   recordingStartTime: new Date(recordingStartTime).toISOString(),
  //   windowStartTime: new Date(windowStartTime).toISOString(),
  //   windowEndTime: new Date(windowEndTime).toISOString(),
  //   channelIndex: CHANNEL_INDEX,
  //   sampleCount: values.length,
  //   min: Math.min(...values),
  //   max: Math.max(...values),
  //   avg,
  // });

  // 센서 보증 분해능 5g에 정합 (모델 판정 tolerance도 5g 기준)
  return Math.round(avg / 5) * 5;
}

// 지정 폴더 아래의 모든 파일 경로를 재귀적으로 수집해 배열로 반환한다.
function getAllFiles(dirPath, arrayOfFiles = []) {
  if (!fs.existsSync(dirPath)) return arrayOfFiles;

  const files = fs.readdirSync(dirPath);

  for (const file of files) {
    const fullPath = path.join(dirPath, file);

    if (fs.statSync(fullPath).isDirectory()) {
      getAllFiles(fullPath, arrayOfFiles);
    } else {
      arrayOfFiles.push(fullPath);
    }
  }

  return arrayOfFiles;
}

// 업로드 후 남은 빈 하위 폴더를 재귀적으로 제거한다.
function removeEmptyDirs(dir) {
  if (!fs.existsSync(dir)) return;

  const entries = fs.readdirSync(dir);

  for (const entry of entries) {
    const fullPath = path.join(dir, entry);

    if (fs.existsSync(fullPath) && fs.statSync(fullPath).isDirectory()) {
      removeEmptyDirs(fullPath);
    }
  }

  if (fs.existsSync(dir) && fs.readdirSync(dir).length === 0) {
    fs.rmdirSync(dir);
  }
}

// 로컬 파일 하나를 MinIO bucket의 지정 objectKey로 업로드한다(Promise 래핑).
function putMinioObject(bucket, objectKey, filePath) {
  return new Promise((resolve, reject) => {
    minioClient.fPutObject(bucket, objectKey, filePath, {}, (err, etag) => {
      if (err) return reject(err);
      resolve(etag);
    });
  });
}

// 로컬 수집 폴더 전체를 MinIO의 productImg/<foldername>/ 아래로 업로드한다.
// 업로드 성공 시 로컬 파일을 삭제(deleteAfterUpload)하고 빈 폴더를 정리한다.
async function uploadFolderToMinio({
  foldername,
  localPath,
  deleteAfterUpload = true,
}) {
  const BUCKET = config.minioBucket;

  if (!localPath || !fs.existsSync(localPath)) {
    return {
      success: false,
      message: `Local folder not found: ${localPath}`,
      filelength: 0,
      objects: [],
    };
  }

  // const foldername = `${safe(productIdx)}_${safe(productEngName)}_${safe(timestamp)}`;
  // const basePrefix = `productImg/${foldername}`;
  const basePrefix = `productImg/${safe(foldername)}`;
  const folderpath = `s3://${BUCKET}/${basePrefix}/`;

  const filesToUpload = getAllFiles(localPath);

  if (!filesToUpload.length) {
    return {
      success: false,
      message: `No files found in folder: ${localPath}`,
      // foldername,
      basePrefix,
      folderpath,
      filelength: 0,
      objects: [],
    };
  }

  const uploaded = [];

  for (const filePath of filesToUpload) {
    const relativePath = path.relative(localPath, filePath).replace(/\\/g, "/");
    const objectKey = `${basePrefix}/${relativePath}`;

    await putMinioObject(BUCKET, objectKey, filePath);

    uploaded.push({
      key: objectKey,
      localPath: filePath,
    });

    if (deleteAfterUpload) {
      try {
        fs.unlinkSync(filePath);
      } catch (err) {
        console.warn(`[MinIO] local file delete failed: ${filePath}`, err.message);
      }
    }
  }

  if (deleteAfterUpload) {
    removeEmptyDirs(localPath);
  }

  return {
    success: true,
    bucket: BUCKET,
    foldername,
    folderpath,
    filelength: uploaded.length,
    objects: uploaded,
  };
}

async function getNextTrainProductIdx(storageType) {
  const normalizedStorageType = normalizeStorageType(storageType);
  const range = TRAIN_PRODUCT_IDX_RANGE[normalizedStorageType];

  if (!range) {
    throw new Error(
      `[AckCollect] Invalid storageType for trainProductIdx: ${storageType}`
    );
  }

  const trainProductIdxCondition = {
    $gte: range.start,
  };

  // 냉장은 100000까지만 사용
  if (range.end !== null) {
    trainProductIdxCondition.$lte = range.end;
  }

  const last = await ProductUpload.findOne(
    {
      trainProductIdx: trainProductIdxCondition,
    },
    {
      trainProductIdx: 1,
    }
  )
    .sort({ trainProductIdx: -1 })
    .lean();

  const nextTrainProductIdx = last
    ? Number(last.trainProductIdx) + 1
    : range.start;

  if (
    range.end !== null &&
    nextTrainProductIdx > range.end
  ) {
    throw new Error(
      `[AckCollect] ${normalizedStorageType} trainProductIdx range exceeded`
    );
  }

  return nextTrainProductIdx;
}

const TRAIN_PRODUCT_IDX_RANGE = Object.freeze({
  COLD: {
    start: 1,
    end: 100000,
  },
  FROZEN: {
    start: 100001,
    end: null,
  },
});

// 수집 완료된 상품의 메타데이터(폴더 경로, 파일 수, storageType,
// loadcell 무게 등)를 ProductUpload 컬렉션에 upsert로 동기화한다.
async function syncProductMetadata({
  productIdx,
  productEngName,
  categoryIdx,
  isNew,
  foldername,
  folderpath,
  filelength,
  storageType,
  productLoadcellWeight,
  trainProductIdx,
}) {
  await ensureMongoConnected();

  const existing = await ProductUpload.findOne(
    { productIdx, productEngName },
    { trainProductIdx: 1 }
  ).lean();

  const now = new Date();
  console.log('[MONGODB]storageType', storageType)
  const setOnInsert = {};

  // if (!existing) {
  //   setOnInsert.trainProductIdx = await getNextTrainProductIdx();
  //   setOnInsert.createDate = now;
  //   setOnInsert.eventPromotion = [];
  // }
  if (!existing) {
    setOnInsert.trainProductIdx = trainProductIdx;
    setOnInsert.createDate = now;
    setOnInsert.eventPromotion = [];
  }

  await ProductUpload.updateOne(
    { productIdx, productEngName },
    {
      $set: {
        productIdx,
        productEngName,
        categoryIdx: categoryIdx ?? "null",
        isNew,
        trainingStatus: "2",
        storageType,
        productLoadcellWeight,
        foldername,
        folderpath,
        filelength: Number(filelength || 0),
        updateDate: now,
      },
      ...(Object.keys(setOnInsert).length ? { $setOnInsert: setOnInsert } : {}),
    },
    { upsert: true }
  );

  const updated = await ProductUpload.findOne({ productIdx, productEngName }).lean();

  console.log(`[MongoDB] Product metadata synced: ${productIdx}`);

  return updated;
}

// 수집 완료 상품을 AI 학습 서버에 notify한다(IF07 연계).
// AiTrainingNotifyService의 notifyTrainingStore(단건) 또는
// notifyTrainingStoreMany(배열)를 상황에 맞게 호출한다.
async function notifyAiTrainingStore(product) {
  console.log(
    '[notifyAiTrainingStore] product ======>',
    JSON.stringify(product, null, 2)
  );
  if (typeof aiNotifyService.notifyTrainingStore === "function") {
    // console.log(
    //   '[notifyAiTrainingStore] input:',
    //   JSON.stringify(product, null, 2)
    // );
    // console.log(
    //   "[aiNotifyService keys]",
    //   Object.keys(aiNotifyService)
    // );

    // console.log(
    //   "[aiNotifyService.notifyTrainingStore source]",
    //   aiNotifyService.notifyTrainingStore.toString()
    // );

    const payload = {
      productIdx: product.productIdx,
      productEngName: product.productEngName,
      divisionIdx: product.divisionIdx,
      deviceIdx: product.deviceIdx,
      trainingStatus: product.trainingStatus || "2",
    };

    if (!payload.divisionIdx) {
      console.log(`[IF07] training divisionIdx is required: productIdx=${payload.productIdx}`);
    }

    if (!payload.deviceIdx) {
      console.log(`[IF07] training deviceIdx is required: productIdx=${payload.productIdx}`);
    }

    console.log("[notifyAiTrainingStore -> service payload]", payload);

    return aiNotifyService.notifyTrainingStore(payload);
    // return aiNotifyService.notifyTrainingStore({
    //   productIdx: product.productIdx,
    //   productEngName: product.productEngName,
    //   trainingStatus: product.trainingStatus || "2",
    // });
  }

  if (typeof aiNotifyService.notifyTrainingStoreMany === "function") {
    const result = await aiNotifyService.notifyTrainingStoreMany([
      {
        productIdx: product.productIdx,
        productEngName: product.productEngName,
        divisionIdx: product.divisionIdx,
        deviceIdx: product.deviceIdx,
        trainingStatus: product.trainingStatus || "2",
      },
    ]);

    return result?.[0] ?? result;
  }

  throw new Error(
    "AiTrainingNotifyService must export notifyTrainingStore or notifyTrainingStoreMany"
  );
}

// storageType 표기("C"/"F"/"COLD"/"FROZEN")를 "COLD"/"FROZEN"으로 정규화한다.
function normalizeStorageType(storageType) {
  if (storageType === "C") return "COLD";
  if (storageType === "F") return "FROZEN";
  if (storageType === "COLD") return "COLD";
  if (storageType === "FROZEN") return "FROZEN";
  return "UNKNOWN";
}

// brunchName 접미사(C/F)를 storageType으로부터 생성한다.
function brunchSuffixFromStorageType(storageType) {
  if (storageType === "COLD") return "C";
  if (storageType === "FROZEN") return "F";
  return "U";
}

// 수집 완료 후 DivisionUpload / DeviceTypeUpload 컬렉션을 갱신한다.
// 기존 매핑 + 이번 수집 상품 + ProductList(IF11) 조회 결과를 병합해
// trainingStatus=2인 상품들을 brunchName 단위로 매핑한다.
async function syncDivisionAndDeviceTypeMapping({
  divisionIdx,
  deviceIdx,
  storageType,
  currentProductIdxList = [],
}) {

  await ensureMongoConnected();
  const DivisionStorageType = normalizeStorageType(storageType);
  const now = new Date();

  const brunchName =
    `${divisionIdx}_${brunchSuffixFromStorageType(DivisionStorageType)}`;

  const deviceTypeDoc = await DeviceTypeUpload.findOne({ brunchName })
    .populate("products.product")
    .lean();

  const existingProductIdxList =
    (deviceTypeDoc?.products || [])
      .map((x) => x?.product?.productIdx)
      .filter(Boolean);

  // ProductList에서 해당 매장 상품 전체 product_idx를 받아서 DeviceTypeUpload 업데이트
  const productListResp = await ProductList({
    division_idx: divisionIdx,
    device_idx: deviceIdx,
  });

  const storeProductIdxList =
    (productListResp?.DATA?.product_list || [])
      .map(p => String(p.product_idx))
      .filter(Boolean);


  const mergedProductIdxList = Array.from(
    new Set([
      ...existingProductIdxList,
      ...currentProductIdxList,
      ...storeProductIdxList,
    ].filter(Boolean))
  );

  /**
   * ProductUpload 전체 조회
   * trainingStatus=2 인 상품들만 매핑
   */
  const productDocs = await ProductUpload.find(
    {
      trainingStatus: "2",
      storageType: DivisionStorageType,
      productIdx: {
        $in: mergedProductIdxList,
      },
    },
    { _id: 1, productIdx: 1 }
  ).lean();

  const productMappings = productDocs.map((x) => ({
    product: x._id,
    training_status: "2",
  }));

  /**
   * DivisionUpload 갱신
   */
  const divisionDoc = await DivisionUpload.findOne({
    divisionIdx,
  }).lean();

  const deviceIdxArr = Array.from(
    new Set([
      ...(divisionDoc?.deviceIdx || []),
      deviceIdx,
    ].filter(Boolean))
  );

  await DivisionUpload.updateOne(
    { divisionIdx },
    {
      $set: {
        divisionIdx,
        deviceIdx: deviceIdxArr,
        // products: productMappings,
      },
    },
    { upsert: true }
  );

  /**
   * DeviceTypeUpload 갱신
   */
  const deviceTypeDeviceIdxArr = Array.from(
    new Set([
      ...(deviceTypeDoc?.deviceIdx || []),
      deviceIdx,
    ].filter(Boolean))
  );

  await DeviceTypeUpload.updateOne(
    {
      // divisionIdx,
      // storageType: normalizedStorageType,
      brunchName
    },
    {
      $set: {
        divisionIdx,
        storageType: DivisionStorageType,
        brunchName,
        deviceIdx: deviceTypeDeviceIdxArr,
        products: productMappings,
        trainingStatus: "2",
        trainingDate: now,
        retrainingDate: null,
      },

      $setOnInsert: {
        modelVersion: null,
      },
    },
    { upsert: true }
  );

  console.log(
    "[MongoDB] Division/DeviceType mapping synced"
  );

  return {
    divisionProductCount: productMappings.length,
    deviceTypeProductCount: productMappings.length,
    brunchName,
  };
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * 문이 닫힐 때까지 계속 확인한다.
 *
 * 수집 도중 사용자가 문을 닫는 시간은 제한하지 않는다.
 * 따라서 전체 타임아웃을 두지 않고 CLOSE 상태가 확인될 때까지 대기한다.
 */
async function waitUntilDoorClosed({ intervalMs = 500 } = {}) {
  while (true) {
    const state = await fetchCurrentDoorState();

    if (state === "CLOSE") {
      return;
    }

    await delay(intervalMs);
  }
}

/**
 * 지정된 시간 동안 문이 열렸는지 확인한다.
 *
 * 이 함수는 최초 2번 카메라 수집 전에 문을 OPEN 상태로 만드는 용도로만 사용한다.
 * 2번 카메라에서 0번 카메라로 전환할 때는 사용하지 않는다.
 */
async function waitUntilDoorOpen({ timeoutMs = 10000, intervalMs = 500 } = {}) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const state = await fetchCurrentDoorState();

    if (state === "OPEN") {
      return true;
    }

    await delay(intervalMs);
  }

  return false;
}

// collect_state=START 처리: 수집 세션을 생성하고 deadbolt를 OPEN한 뒤
// 1단계(camera 2) 촬영과 loadcell recording을 시작하고 START ack를 발행한다.
// 첫 번째 문 닫힘 감지 후 2단계(camera 0) 촬영으로 전환한다.
async function handleStartCollect(reqData, reqSysid) {
  const {
    device_idx,
    division_idx,
    product_idx,
    collect_state,
    product_eng_name,
    category_idx,
    is_new,
    product_loadcell_weight,
  } = reqData;

  const sessionKey = makeSessionKey(product_idx);

  if (collectSessions.has(sessionKey)) {
    throw new Error(
      `Collect session already exists for product_idx=${product_idx}`
    );
  }

  const option = getLatestCollectOption();
  const hasLoadcell = option.hasLoadcell;
  const storageType = option.storageType;
  const normalizedStorageType = normalizeStorageType(storageType);
  const useLoadcell = hasLoadcell === "Y";

  console.log("[AckCollect] hasLoadcell:", hasLoadcell);
  console.log("[AckCollect] storageType:", storageType);
  console.log("[AckCollect] START collect:", product_idx);

  await ensureMongoConnected();

  const healthBefore = await ProductCollectionHealth();

  /*
   * 1단계 수집은 문이 열린 상태에서 시작해야 한다.
   * 문이 닫혀 있으면 데드볼트를 열고 실제 OPEN 상태를 확인한 뒤 진행한다.
   */
  if (healthBefore.CurrentDoorState !== "OPEN") {
    await callApiToControlDeadbolt("OPEN");

    const opened = await waitUntilDoorOpen({
      timeoutMs: 10000,
      intervalMs: 500,
    });

    if (!opened) {
      throw new Error("Door open timeout before camera 2 collection.");
    }
  }

  const productDoc = await ProductUpload.findOne(
    {
      productIdx: product_idx,
      productEngName: product_eng_name,
    },
    {
      trainProductIdx: 1,
    }
  ).lean();

  let trainProductIdx;

  if (!productDoc) {
    trainProductIdx = await getNextTrainProductIdx(normalizedStorageType);
    console.log(`[AckCollect] new trainProductIdx: ${trainProductIdx}`);
  } else {
    trainProductIdx = productDoc.trainProductIdx;
    console.log(`[AckCollect] original trainProductIdx: ${trainProductIdx}`);
  }

  const timestamp = makeTimestampFolderName();
  const foldername =
    `${safe(trainProductIdx)}_${safe(product_eng_name)}_${timestamp}`;

  const baseProductPath = path.resolve(process.cwd(), "productImg");
  const productFolder = path.join(baseProductPath, foldername);

  // 카메라별 결과가 섞이지 않도록 서로 다른 하위 폴더에 저장한다.
  // const camera2Folder = path.join(productFolder, "camera_2");
  // const camera0Folder = path.join(productFolder, "camera_0");

  const session = {
    timestamp,
    foldername,
    trainProductIdx,
    productFolder,
    // camera2Folder,
    // camera0Folder,
    productIdx: product_idx,
    productEngName: product_eng_name,
    categoryIdx: category_idx,
    isNew: is_new,
    hasLoadcell,
    storageType: normalizedStorageType,
    // IF06에서 받은 학습 장비 정보
    deviceIdx: device_idx,
    divisionIdx: division_idx,
    productLoadcellWeight: product_loadcell_weight,
  };

  collectSessions.set(sessionKey, session);

  /**
   * DoorCollect.js가 마지막 IF04 CLOSE에서 사용할 수 있도록
   * IF06 학습 대상 장비 정보를 미리 저장한다.
   */
  setLatestTrainingTarget({
    productIdx: product_idx,
    divisionIdx: division_idx,
    deviceIdx: device_idx,
    storageType: normalizedStorageType,
  });

  try {
    /*
     * 1단계
     * 문이 열린 상태에서 2번 카메라만 촬영한다.
     */
    let camera_idx = 2
    await cameraStartSampling(productFolder, [camera_idx]);
    console.log(`[Collect] ${camera_idx}번 수집 시작`);

    const testLoadcellWeight = await startLoadcellRecording();
    console.log('testLoadcellWeight', testLoadcellWeight)

    const health = await ProductCollectionHealth();

    publishAck(
    makeAckPayload({
      reqSysid: reqSysid,
      device_idx: device_idx,
      division_idx: division_idx,
      collectState: collect_state,
      productIdx: product_idx,
      productEngName: product_eng_name,
      categoryIdx: category_idx,
      isNew: is_new,
      productLoadcellWeight: 'null',
      resultCd: health.isSuccess ? "S" : "F",
      resultMsg: health.resultMsg,
      health,
      extraData: {
        collection_timestamp: timestamp,
        local_path: productFolder,
      },
    })
  );

    /*
     * 사용자가 문을 닫아 데드볼트가 내려갈 때까지 2번 카메라 촬영을 유지한다.
     * 사용자가 상품을 배치하는 시간을 제한하지 않기 위해 타임아웃을 두지 않는다.
     */
    await waitUntilDoorClosed({ intervalMs: 500 });

    await cameraStopSampling();

    await callApiToControlDeadbolt("OPEN");

    camera_idx = 0
    await cameraStartSampling(productFolder, [camera_idx]);
    console.log(`[Collect] ${camera_idx}번 수집 시작`);

    // const health = await ProductCollectionHealth();

    publishAck(
    makeAckPayload({
      reqSysid: reqSysid,
      device_idx: device_idx,
      division_idx: division_idx,
      collectState: collect_state,
      productIdx: product_idx,
      productEngName: product_eng_name,
      categoryIdx: category_idx,
      isNew: is_new,
      productLoadcellWeight: 'null',
      resultCd: health.isSuccess ? "S" : "F",
      resultMsg: health.resultMsg,
      health,
      extraData: {
        collection_timestamp: timestamp,
        local_path: productFolder,
      },
    })
  );

  await waitUntilDoorClosed({ intervalMs: 500 });

  await cameraStopSampling();

  } catch (error) {
    // 단계 전환 실패 시 남아 있는 장치 동작을 가능한 범위에서 정리한다.
    await cameraStopSampling().catch(() => {});

    if (session.loadcellRecording) {
      await stopLoadcellRecording().catch(() => {});
      session.loadcellRecording = false;
    }

    collectSessions.delete(sessionKey);

    throw error;
  }
}

// collect_state=END 처리: 2단계(camera 0) 촬영을 종료하고 문 닫힘을 확인한 뒤
// loadcell 무게 산출, MinIO 업로드, MongoDB 동기화(Product/Division/DeviceType),
// annotation label 동기화, END ack 발행, AI 학습 notify까지 마무리한다.
async function handleEndCollect(reqData, reqSysid) {
  const {
      device_idx,
      division_idx,
      product_idx,
      collect_state,
      product_name,
      product_eng_name,
      category_idx,
      is_new,
      product_loadcell_weight,
      has_loadcell,
  } = reqData;

  // 학습 대상 : device_idx / division_idx
  console.log("[AckCollect] END collect:", product_idx);
  console.log('[COLLECT] end: ', reqData)

  // DoorCollect(IF04) 기준 설정값 조회
  const option = getLatestCollectOption();
  const doorState = option.doorState;
  console.log('option -------- ', option)

  // 학습 대상 storage type
  // const hasLoadcell = option.hasLoadcell;
  // const storageType = option.storageType;

  const session = collectSessions.get(String(product_idx));
  console.log('SESSION: ', session)

  if (!session) {
    throw new Error(`No active collect session found for product_idx=${product_idx}`);
  }
  const storageType = session.storageType;
  const finalStorageType = normalizeStorageType(storageType);

  // END 메시지는 두 번째 단계인 0번 카메라 수집을 종료한다.
  await cameraStopSampling();
  session.phase = "CAMERA_0_COMPLETED";

  // 문이 닫힐 때까지 타임아웃 없이 대기한다.
  await waitUntilDoorClosed({ intervalMs: 500 });

  const useLoadcell = session.hasLoadcell === "Y";
  console.log('session.has_loadcell', session.hasLoadcell)
  let updateLoadcellWeight = ''

  if (useLoadcell) {
    console.log('useLoadcell', useLoadcell)

    await stopLoadcellRecording();   // 끝났다는 신호만

    try {
      const logs = await fetchRecordedLoadcellData();
      const weight = computeProductWeight(logs);
      updateLoadcellWeight = String(weight);
      updateLoadcellWeight = Math.abs(updateLoadcellWeight)
      console.log(`[Loadcell] computed weight: ${updateLoadcellWeight} (snapshots=${logs.length})`);
    } catch (err) {
      console.error('[Loadcell] weight calculation failed:', err.message);
      updateLoadcellWeight = '';
    }
  }

  console.log('Loadcell Weight: ', updateLoadcellWeight)

  // const loadcellWeight = await stopLoadcellRecording();

  // const uploadResult = await uploadFolderToMinio({
  //   localPath: session.productFolder,
  //   productIdx: product_idx,
  //   productEngName: product_eng_name,
  //   timestamp: session.timestamp,
  //   deleteAfterUpload: true,
  // });

  // productFolder 아래 camera_2와 camera_0 두 폴더를 재귀적으로 업로드한다.
  // MinIO 구조:
  // productImg/<foldername>/camera_2/...
  // productImg/<foldername>/camera_0/...
  const uploadResult = await uploadFolderToMinio({
    foldername: session.foldername,
    localPath: session.productFolder,
    timestamp: session.timestamp,
    deleteAfterUpload: true,
  });

  if (!uploadResult.success) {
    throw new Error(uploadResult.message || "MinIO upload failed");
  }

  const finalLoadcellWeight = updateLoadcellWeight || product_loadcell_weight;

  const productDoc = await syncProductMetadata({
    productIdx: product_idx,
    productEngName: product_eng_name,
    // productName: product_name,
    categoryIdx: category_idx,
    isNew: is_new,
    foldername: uploadResult.foldername,
    folderpath: uploadResult.folderpath,
    filelength: uploadResult.filelength,
    storageType: finalStorageType,
    // productLoadcellWeight: product_loadcell_weight == null ? updateLoadcellWeight : product_loadcell_weight,
    // productLoadcellWeight: String(product_loadcell_weight == null ? updateLoadcellWeight : product_loadcell_weight),
    productLoadcellWeight: finalLoadcellWeight,
    trainProductIdx: session.trainProductIdx,
  });

  /**
   * DivisionUpload / DeviceTypeUpload 매핑
   */
  const mappingResult = await syncDivisionAndDeviceTypeMapping({
      divisionIdx: division_idx,
      deviceIdx: device_idx,
      storageType: finalStorageType,
      currentProductIdxList: [product_idx],
  });

  /**
   * AnnotationLabel 동기화
   */
  // const annotationResult =
  //   await syncAnnotationLabels({
  //     productModel: ProductUpload,
  //     deleteMissing: false,
  //   });

  let annotationResult = null;

  try {
    annotationResult = await syncAnnotationLabels({
        productModel: ProductUpload,
        deleteMissing: false,
      });
    console.log("[AnnotationLabel] synced");
  } catch (err) {
    console.error("[AnnotationLabel] sync failed:", err);
  }

  const health = await ProductCollectionHealth();

  publishAck(
    makeAckPayload({
      reqSysid: reqSysid,
      device_idx: reqData.device_idx,
      division_idx: reqData.division_idx,
      collectState: collect_state,
      productIdx: product_idx,
      productEngName: product_eng_name,
      categoryIdx: category_idx,
      isNew: is_new,
      productLoadcellWeight: finalLoadcellWeight,
      resultCd: health.isSuccess ? "S" : "F",
      resultMsg: health.resultMsg,
      health,
      extraData: {
        collection_timestamp: session.timestamp,
        foldername: uploadResult.foldername,
        folderpath: uploadResult.folderpath,
        camera_2_folderpath: `${uploadResult.folderpath}camera_2/`,
        camera_0_folderpath: `${uploadResult.folderpath}camera_0/`,
        filelength: uploadResult.filelength,
        train_product_idx: productDoc?.trainProductIdx,
      },
    })
  );

  /**
   * AI 서버 notify
   */
  try {
  const notifyResult = await notifyAiTrainingStore({
      productIdx: product_idx,
      productEngName: product_eng_name,
      divisionIdx: session.divisionIdx,
      deviceIdx: session.deviceIdx,
      trainingStatus: "2",
    });

    console.log("[AckCollect] sending to PNT:", notifyResult);
  } catch (err) {
    console.error("[IF07] notify failed:", err.message);
  }

  collectSessions.delete(String(product_idx));
}

// cmd/collect 수신 메시지 진입점. collect_state(START/END)에 따라 분기하고,
// 처리 중 에러가 나면 result_cd="F"인 실패 ack를 발행한다.
async function handleCollectMessage(message) {
  let reqData = {};
  let reqSysid = ''

  try {
    const reqPayload = JSON.parse(message.toString());
    reqData = reqPayload.DATA;
    reqSysid = reqPayload.HEADER.IF_SYSID

    const {
      device_idx,
      division_idx,
      product_idx,
      collect_state,
      product_eng_name,
      category_idx,
      is_new,
      product_loadcell_weight,
    } = reqData;

    console.log("[AckCollect] Request DATA:", reqData);

    // if (String(config.deviceIdx) !== String(device_idx)) {
    //   console.warn("[AckCollect] device_idx mismatch:", {
    //     configDeviceIdx: config.deviceIdx,
    //     requestDeviceIdx: device_idx,
    //   });
    //   return;
    // }

    // if (String(config.divisionIdx) !== String(division_idx)) {
    //   console.warn("[AckCollect] division_idx mismatch:", {
    //     configDivisionIdx: config.divisionIdx,
    //     requestDivisionIdx: division_idx,
    //   });
    //   return;
    // }

    if (collect_state === "START") {
      await handleStartCollect(reqData, reqSysid);
      return;
    }

    if (collect_state === "END") {
      await handleEndCollect(reqData, reqSysid);
      return;
    }

    if (collect_state === "training") {
      await handleTrainingCollect(reqData, reqSysid);
      return;
    }

    throw new Error(`Unsupported collect_state: ${collect_state}`);
  } catch (error) {
    // local folder not found가 뜸
    console.error("[AckCollect] Processing Error:", error.message);

    const health = await ProductCollectionHealth().catch(() => ({}));

    publishAck(
      makeAckPayload({
        reqSysid: reqSysid,
        device_idx: reqData.device_idx,
        division_idx: reqData.division_idx,
        collectState: reqData.collect_state,
        productIdx: reqData.product_idx,
        productEngName: reqData.product_eng_name,
        categoryIdx: reqData.category_idx,
        isNew: reqData.is_new,
        resultCd: "F",
        resultMsg: error.message,
        health,
      })
    );
  }
}

async function handleTrainingCollect(reqData, reqSysid) {
  const {
    device_idx,
    division_idx,
    product_idx,
    collect_state,
    product_eng_name,
    category_idx,
    is_new,
    product_loadcell_weight,
  } = reqData;

  console.log("[AckCollect] TRAINING request:", {
    device_idx,
    division_idx,
    product_idx,
    product_eng_name,
  });

  await ensureMongoConnected();

  /*
   * IF04에서 미리 받은 저장 타입 사용
   */
  const option = getLatestCollectOption();
  const normalizedStorageType =
    normalizeStorageType(option.storageType);

  if (normalizedStorageType === "UNKNOWN") {
    throw new Error(
      `[TRAINING] Invalid storageType: ${option.storageType}`
    );
  }

  if (!product_idx || !product_eng_name) {
    throw new Error(
      "[TRAINING] product_idx and product_eng_name are required"
    );
  }

  /*
   * 1. 기존 학습 상품 확인
   * 신규 ProductUpload는 생성하지 않는다.
   */
  let productDoc = await ProductUpload.findOne(
    {
      productIdx: product_idx,
      productEngName: product_eng_name,
    },
    {
      _id: 1,
      productIdx: 1,
      productEngName: 1,
      trainProductIdx: 1,
      trainingStatus: 1,
      storageType: 1,
    }
  ).lean();

  if (!productDoc) {
    throw new Error(
      `[TRAINING] Existing product not found: ` +
      `productIdx=${product_idx}, ` +
      `productEngName=${product_eng_name}`
    );
  }

  /*
   * 기존 상품의 냉장/냉동 타입 검증
   */
  const productStorageType =
    normalizeStorageType(productDoc.storageType);

  if (
    productStorageType !== "UNKNOWN" &&
    productStorageType !== normalizedStorageType
  ) {
    throw new Error(
      `[TRAINING] Product storageType mismatch: ` +
      `product=${productStorageType}, ` +
      `request=${normalizedStorageType}`
    );
  }

  /*
   * syncDivisionAndDeviceTypeMapping()가
   * trainingStatus="2" 상품만 조회하기 때문에 상태 보정
   *
   * 기존 데이터가 항상 "2"임이 보장되면 이 블록은 생략 가능
   */
  if (String(productDoc.trainingStatus) !== "2") {
    await ProductUpload.updateOne(
      {
        _id: productDoc._id,
      },
      {
        $set: {
          trainingStatus: "2",
          updateDate: new Date(),
        },
      }
    );

    productDoc = {
      ...productDoc,
      trainingStatus: "2",
    };
  }

  /*
   * 2. DivisionUpload / DeviceTypeUpload 상품 매핑
   *
   * DeviceTypeUpload.products[]에 다음 형태로 반영
   * {
   *   product: productDoc._id,
   *   training_status: "2"
   * }
   */
  const mappingResult =
    await syncDivisionAndDeviceTypeMapping({
      divisionIdx: division_idx,
      deviceIdx: device_idx,
      storageType: normalizedStorageType,
      currentProductIdxList: [
        String(product_idx),
      ],
    });

  /*
   * 실제 DeviceTypeUpload.products[] 반영 여부 검증
   */
  const mappedDeviceType =
    await DeviceTypeUpload.findOne(
      {
        brunchName: mappingResult.brunchName,
        products: {
          $elemMatch: {
            product: productDoc._id,
            training_status: "2",
          },
        },
      },
      {
        _id: 1,
        brunchName: 1,
      }
    ).lean();

  if (!mappedDeviceType) {
    throw new Error(
      `[TRAINING] DeviceType product mapping failed: ` +
      `productIdx=${product_idx}, ` +
      `brunchName=${mappingResult.brunchName}`
    );
  }

  /*
   * 3. 다음 IF04 CLOSE 요청에서 사용할
   * 학습 대상 장비 정보 저장
   */
  setLatestTrainingTarget({
    productIdx: product_idx,
    divisionIdx: division_idx,
    deviceIdx: device_idx,
    storageType: normalizedStorageType,
  });

  /*
   * notifyAiTrainingStore()는 호출하지 않는다.
   * IF06 ACK만 발행
   */
  publishAck(
    makeAckPayload({
      reqSysid,
      device_idx,
      division_idx,
      collectState: collect_state,
      productIdx: product_idx,
      productEngName: product_eng_name,
      categoryIdx: category_idx,
      isNew: is_new,
      productLoadcellWeight:
        product_loadcell_weight ?? null,
      resultCd: "S",
      resultMsg: "training target mapped",
      extraData: {
        train_product_idx:
          productDoc.trainProductIdx,
        brunch_name:
          mappingResult.brunchName,
      },
    })
  );

  console.log(
    "[AckCollect] TRAINING completed:",
    {
      productIdx: product_idx,
      productOid: productDoc._id,
      trainProductIdx:
        productDoc.trainProductIdx,
      brunchName:
        mappingResult.brunchName,
    }
  );
}

// 모듈 진입점. 공용 MQTT client로 cmd/collect topic을 구독하고,
// 수신 메시지를 Promise chain으로 직렬 처리해 수집 명령이 겹치지 않게 한다.
async function AckCollect() {

  client = getClient();
  // console.log("[AckCollect] BROKER_URL:", BROKER_URL);
  // console.log("[AckCollect] CMD_TOPIC:", CMD_TOPIC);
  // console.log("[AckCollect] ACK_TOPIC:", ACK_TOPIC);

  // client.on("connect", () => {
  //   client.subscribe(SUB_TOPIC);
  // });
  client.subscribe(SUB_TOPIC, { qos: 1 }, (err, granted) => {
    if (err) {
      console.error("[ACK-COLLECT] Subscribe Error:", err.message);
      return;
    }

    console.log("[ACK-COLLECT] Subscribed:", granted);
    // console.log(`[ACK-COLLECT] Subscribed: ${granted}`);
  });

  client.on("message", (topic, message) => {
    console.log("[AckCollect] message received topic:", topic);

    if (topic !== SUB_TOPIC) return;

    chain = chain
      .then(() => handleCollectMessage(message))
      .catch((err) => {
        console.error("[AckCollect] chain error:", err);
      });
  });

  client.on("error", (err) => {
    console.error("[AckCollect] MQTT error:", err.message);
  });

  client.on("close", () => {
    console.warn("[AckCollect] MQTT closed");
  });
}

// 진행 중인 상품 수집 세션이 있는지 — 수집 중 로드셀 영점(calibrate)이
// 실행되면 수집 데이터가 오염되므로 LoadcellZeroset 가드에서 사용한다
function hasActiveCollectSession() {
  return collectSessions.size > 0;
}

module.exports = {
  AckCollect,
  fetchCurrentDoorState,
  ProductCollectionHealth,
  notifyAiTrainingStore,
  hasActiveCollectSession,
};