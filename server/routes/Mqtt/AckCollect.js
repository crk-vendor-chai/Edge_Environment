// server/routes/Mqtt/AckCollect.js
require("dotenv").config();

const mqtt = require("mqtt");
const path = require("path");
const axios = require("axios");
const fs = require("fs");
const Minio = require("minio");
const mongoose = require("mongoose");
const { v4: uuidv4 } = require("uuid");
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

async function ensureMongoConnected() {
  if (mongoose.connection.readyState === 1) return;

  if (!mongoConnectPromise) {
    mongoConnectPromise = mongoose.connect(config.mongoURI);
  }

  await mongoConnectPromise;
}

function safe(s) {
  return String(s || "").replace(/[^a-zA-Z0-9._-]/g, "_");
}

function makeTimestampFolderName(d = new Date()) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const HH = String(d.getHours()).padStart(2, "0");
  const MM = String(d.getMinutes()).padStart(2, "0");
  const SS = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}_${HH}${MM}${SS}`;
}

function makeIFDate(d = new Date()) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const HH = String(d.getHours()).padStart(2, "0");
  const MM = String(d.getMinutes()).padStart(2, "0");
  const SS = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}${HH}${MM}${SS}`;
}

function makeSessionKey(productIdx) {
  return String(productIdx);
}

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

// function makeAckPayload({
//   collectState,
//   productIdx,
//   productEngName,
//   categoryIdx,
//   isNew,
//   resultCd = "S",
//   resultMsg = "success",
//   health = {},
//   extraData = {},
// }) {
//   return {
//     HEADER: {
//       IF_ID: "IF_06",
//       IF_SYSID: uuidv4(),
//       IF_HOST: "EDGEPC01",
//       IF_DATE: makeIFDate(),
//     },
//     DATA: {
//       device_idx: config.deviceIdx,
//       division_idx: config.divisionIdx,
//       product_idx: productIdx,
//       collect_state: collectState,
//       product_eng_name: productEngName,
//       category_idx: categoryIdx,
//       is_new: isNew,
//       camera_status: health.CameraStatus === "09" ? "1" : "0",
//       deadbolt_status: health.DeadboltHealth === "19" ? "1" : "0",
//       loadcell_status: health.LoadcellHealth === "29" ? "1" : "0",
//       result_cd: resultCd,
//       result_msg: resultMsg,
//       ...extraData,
//     },
//   };
// }

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
  const OFFSET_MS = 4000;       // startLoadcellRecording 이후 5초
  const WINDOW_MS = 2000;       // 3초 동안

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

  return Math.round(avg);
}

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

function putMinioObject(bucket, objectKey, filePath) {
  return new Promise((resolve, reject) => {
    minioClient.fPutObject(bucket, objectKey, filePath, {}, (err, etag) => {
      if (err) return reject(err);
      resolve(etag);
    });
  });
}

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

// async function getNextTrainProductIdx() {
//   const last = await ProductUpload.findOne({}, { trainProductIdx: 1 })
//     .sort({ trainProductIdx: -1 })
//     .lean();

//   return Number(last?.trainProductIdx ?? 0) + 1;
// }

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

  // // 1. productIdx만 같은 기존 상품 있는지 먼저 확인
  // const existingByProductIdx = await ProductUpload.findOne({ productIdx }).lean();

  // // const existingByProductIdx = await ProductUpload.findOne(
  // //   { productIdx },
  // //   { _id: 1, productIdx: 1, productEngName: 1, eventPromotion: 1, trainProductIdx: 1 }
  // // ).lean();

  // // 2. productIdx는 같고 productEngName이 다르면 eventPromotion에 추가
  // if (
  //   existingByProductIdx &&
  //   existingByProductIdx.productEngName !== productEngName
  // ) {
  //   const now = new Date();

  //   await ProductUpload.updateOne(
  //     { _id: existingByProductIdx._id },
  //     {
  //       $addToSet: {
  //         eventPromotion: {
  //           productIdx,
  //           productEngName,
  //           categoryIdx: categoryIdx ?? "null",
  //           isNew,
  //           trainingStatus: "2",
  //           storageType,
  //           productLoadcellWeight,
  //           foldername,
  //           folderpath,
  //           filelength: Number(filelength || 0),
  //           createDate: now,
  //           updateDate: now,
  //         },
  //       },
  //       $set: {
  //         updateDate: now,
  //       },
  //     }
  //   );

  //   console.log(`[MongoDB] Product added to eventPromotion: ${productIdx}`);

  //   return ProductUpload.findOne({ _id: existingByProductIdx._id }).lean();
  // }

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

// async function syncDivisionAndDeviceTypeMapping({
//   divisionIdx,
//   deviceIdx,
//   storageType,
// }) {

//   await ensureMongoConnected();

//   const normalizedStorageType = normalizeStorageType(storageType);

//   const now = new Date();

//   /**
//    * DivisionUpload
//    */
//   const divisionDoc = await DivisionUpload.findOne({
//       divisionIdx,
//     }).lean();

//   const deviceIdxArr = Array.from(
//     new Set([
//       ...(divisionDoc?.deviceIdx || []),
//       deviceIdx,
//     ].filter(Boolean))
//   );

//   await DivisionUpload.updateOne(
//     { divisionIdx },
//     {
//       $set: {
//         divisionIdx,
//         deviceIdx: deviceIdxArr,
//       },
//     },
//     { upsert: true }
//   );

//   /**
//    * storageType별 ProductUpload 조회
//    */
//   const productDocs =
//     await ProductUpload.find(
//       {
//         storageType: normalizedStorageType,
//       },
//       { _id: 1 }
//     ).lean();

//   const productMappings =
//     productDocs.map((x) => ({
//       product: x._id,
//       training_status: "2",
//     }));

//   /**
//    * DeviceTypeUpload
//    */
//   const brunchName = `${divisionIdx}_${brunchSuffixFromStorageType(normalizedStorageType)}`;

//   const deviceTypeDoc =
//     await DeviceTypeUpload.findOne({
//       divisionIdx,
//       storageType: normalizedStorageType,
//     }).lean();

//   const deviceTypeDeviceIdxArr =
//     Array.from(
//       new Set([
//         ...(deviceTypeDoc?.deviceIdx || []),
//         deviceIdx,
//       ].filter(Boolean))
//     );

//   await DeviceTypeUpload.updateOne(
//     {
//       divisionIdx,
//       storageType: normalizedStorageType,
//     },
//     {
//       $set: {
//         storageType: normalizedStorageType,
//         divisionIdx,
//         brunchName,
//         deviceIdx: deviceTypeDeviceIdxArr,
//         products: productMappings,
//         trainingStatus: "2",
//         trainingDate: now,
//         retrainingDate: null,
//       },
//     },
//     { upsert: true }
//   );

//   console.log(`[DeviceTypeUpload] synced division=${divisionIdx} storageType=${normalizedStorageType}`);

//   return {
//     divisionIdx,
//     storageType: normalizedStorageType,
//     brunchName,
//     productCount: productMappings.length,
//   };
// }

// 수집 완료 상품의 학습 상태와 실제 학습 대상 장비 정보를 IF07 서비스에 전달
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

    // const payload = {
    //   productIdx: product.productIdx,
    //   productEngName: product.productEngName,
    //   trainingStatus: product.trainingStatus || "2",
    // };

    // config 장비 정보가 아닌 수집 세션의 학습 대상 장비 정보를 전달
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

function normalizeStorageType(storageType) {
  if (storageType === "C") return "COLD";
  if (storageType === "F") return "FROZEN";
  if (storageType === "COLD") return "COLD";
  if (storageType === "FROZEN") return "FROZEN";
  return "UNKNOWN";
}

function brunchSuffixFromStorageType(storageType) {
  if (storageType === "COLD") return "C";
  if (storageType === "FROZEN") return "F";
  return "U";
}

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
  // const productDocs = await ProductUpload.find(
  //   { trainingStatus: "2" },
  //   { _id: 1 }
  // ).lean();

  // const productDocs = await ProductUpload.find(
  //   {
  //     trainingStatus: "2",
  //     storageType: DivisionStorageType,
  //   },
  //   { _id: 1 }
  // ).lean();

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
        // 냉장 TRAINING 요청
        // → DivisionUpload.products = 냉장 상품 목록
        // 냉동 TRAINING 요청
        // → DivisionUpload.products = 냉동 상품 목록으로 교체 문제 발생 가능 -> productMappings를 그대로 넣으면 냉장/냉동 섞여서 들어감
        // products: productMappings,
      },
    },
    { upsert: true }
  );

  /**
   * DeviceTypeUpload 갱신
   */
  // const brunchNameStorageType = brunchSuffixFromStorageType(DivisionStorageType)
  // const brunchName = `${divisionIdx}_${brunchNameStorageType}`;
  // const brunchName = `${division_idx}_${brunchSuffixFromStorageType(DivisionStorageType)}`;

  // const deviceTypeDoc = await DeviceTypeUpload.findOne({
  //     // divisionIdx,
  //     // storageType: normalizedStorageType,
  //     brunchName
  //   }).lean();

  // const deviceTypeDoc =
  //   await DeviceTypeUpload.findOne({
  //     brunchName,
  //   })
  //   .populate("products.product")
  //   .lean();

  // const existingProductIdxList =
  //   (deviceTypeDoc?.products || [])
  //     .map(x => x?.product?.productIdx)
  //     .filter(Boolean);

  ///deviceIdx 중복 제거
  // const deviceTypeDeviceIdxArr = Array.from(
  //   new Set([
  //     ...(deviceTypeDoc?.deviceIdx || []),
  //     deviceIdx,
  //   ].filter(Boolean))
  // );
  // const deviceTypeDeviceIdxArr = Array.from(
  //   new Set([
  //     ...existingProductIdxList,
  //     product_idx,
  //   ])
  // );
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

// async function handleStartCollect(reqData) {
//   const {
//     device_idx,
//     division_idx,
//     product_idx,
//     collect_state,
//     product_eng_name,
//     product_name,
//     category_idx,
//     is_new,
//     has_loadcell,
//   } = reqData;

//   if (String(config.deviceIdx) !== String(device_idx)) return;
//   if (String(config.divisionIdx) !== String(division_idx)) return;

//   const healthBefore = await ProductCollectionHealth();

//   const currentDoorState = healthBefore.CurrentDoorState;

//   if (currentDoorState === "CLOSE") {
//     await callApiToControlDeadbolt("OPEN");
//   }

//   const timestamp = makeTimestampFolderName();
//   const productFolder = path.join(
//     process.cwd(),
//     "productImg",
//     `${safe(product_idx)}_${safe(product_eng_name)}_${timestamp}`
//   );

//   const sessionKey = makeSessionKey(product_idx);

//   collectSessions.set(sessionKey, {
//     timestamp,
//     productFolder,
//     productIdx: product_idx,
//     productEngName: product_eng_name,
//     productName: product_name,
//     categoryIdx: category_idx,
//     isNew: is_new,
//     hasLoadcell: has_loadcell,
//     startedAt: new Date(),
//   });

//   await cameraStartSampling(productFolder, [0, 2]);

//   const useLoadcell =
//     has_loadcell === true ||
//     has_loadcell === "1" ||
//     has_loadcell === 1 ||
//     has_loadcell === "Y";

//   if (useLoadcell) {
//     await startLoadcellRecording();
//   }

//   const health = await ProductCollectionHealth();

//   publishAck(
//     makeAckPayload({
//       collectState: collect_state,
//       productIdx: product_idx,
//       productEngName: product_eng_name,
//       categoryIdx: category_idx,
//       isNew: is_new,
//       resultCd: health.isSuccess ? "S" : "F",
//       resultMsg: health.resultMsg,
//       health,
//       extraData: {
//         collection_timestamp: timestamp,
//         local_path: productFolder,
//       },
//     })
//   );
// }

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

  const option = getLatestCollectOption();
  const hasLoadcell = option.hasLoadcell;
  const storageType = option.storageType;
  const normalizedStorageType = normalizeStorageType(storageType);

  console.log("[AckCollect] hasLoadcell:", hasLoadcell);
  console.log("[AckCollect] storageType:", storageType);

  console.log("[AckCollect] START collect:", reqData.product_idx);

  const healthBefore = await ProductCollectionHealth();

  if (healthBefore.CurrentDoorState === "CLOSE") {
    await callApiToControlDeadbolt("OPEN");
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

  const foldername = `${trainProductIdx}_${product_eng_name}_${timestamp}`;

  const BASE_PRODUCT_PATH = path.resolve(
    process.cwd(),
    "productImg"
  );

  const productFolder = path.join(
    BASE_PRODUCT_PATH,
    foldername
  );

  // const timestamp = makeTimestampFolderName();

  // const productFolder = path.join(
  //   process.cwd(),
  //   "productImg",
  //   `${safe(product_idx)}_${safe(product_eng_name)}_${timestamp}`
  // );

  collectSessions.set(String(product_idx), {
    timestamp,
    foldername,
    trainProductIdx,
    productFolder,
    productIdx: product_idx,
    productEngName: product_eng_name,
    categoryIdx: category_idx,
    isNew: is_new,
    hasLoadcell: hasLoadcell,
    storageType: normalizedStorageType,
    deviceIdx: device_idx,
    divisionIdx: division_idx,
    productLoadcellWeight: product_loadcell_weight,
  });

  setLatestTrainingTarget({
    productIdx: product_idx,
    divisionIdx: division_idx,
    deviceIdx: device_idx,
    storageType: normalizedStorageType,
  });

  // await cameraStartSampling(productFolder, [0, 2]);
  await cameraStartSampling(productFolder, [0, 1]);

  const useLoadcell = hasLoadcell === "Y";
  console.log('Loadcell is', useLoadcell)

  // if (useLoadcell) { 
  //   await startLoadcellRecording();
  // }

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
}

async function waitUntilDoorClosed({ timeoutMs = 60000, intervalMs = 500 } = {}) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const state = await fetchCurrentDoorState();

    if (state === "CLOSE") return true;

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  return false;
}

// async function handleEndCollect(reqData) {
//   const {
//     device_idx,
//     division_idx,
//     product_idx,
//     collect_state,
//     product_eng_name,
//     product_name,
//     category_idx,
//     is_new,
//     has_loadcell,
//   } = reqData;

//   if (String(config.deviceIdx) !== String(device_idx)) return;
//   if (String(config.divisionIdx) !== String(division_idx)) return;

//   const sessionKey = makeSessionKey(product_idx);
//   const session = collectSessions.get(sessionKey);

//   if (!session) {
//     throw new Error(`No active collect session found for product_idx=${product_idx}`);
//   }

//   const closed = await waitUntilDoorClosed();

//   if (!closed) {
//     throw new Error("Door close timeout. Product collection cannot be finalized.");
//   }

//   await cameraStopSampling();

//   const useLoadcell =
//     session.hasLoadcell === true ||
//     session.hasLoadcell === "1" ||
//     session.hasLoadcell === 1 ||
//     session.hasLoadcell === "Y" ||
//     has_loadcell === true ||
//     has_loadcell === "1" ||
//     has_loadcell === 1 ||
//     has_loadcell === "Y";

//   if (useLoadcell) {
//     await stopLoadcellRecording();
//   }

//   const uploadResult = await uploadFolderToMinio({
//     localPath: session.productFolder,
//     productIdx: product_idx,
//     productEngName: product_eng_name || session.productEngName,
//     timestamp: session.timestamp,
//     deleteAfterUpload: true,
//   });

//   if (!uploadResult.success) {
//     throw new Error(uploadResult.message || "MinIO upload failed");
//   }

//   const productDoc = await syncProductMetadata({
//     productIdx: product_idx,
//     productEngName: product_eng_name || session.productEngName,
//     productName: product_name || session.productName,
//     categoryIdx: category_idx || session.categoryIdx,
//     isNew: is_new ?? session.isNew,
//     foldername: uploadResult.foldername,
//     folderpath: uploadResult.folderpath,
//     filelength: uploadResult.filelength,
//   });

//   const annotationResult = await syncAnnotationLabels({
//     productModel: ProductUpload,
//     deleteMissing: false,
//   });

//   const notifyResult = await notifyAiTrainingStore({
//     productIdx: product_idx,
//     productEngName: product_eng_name || session.productEngName,
//     trainingStatus: "2",
//   });

//   const health = await ProductCollectionHealth();

//   publishAck(
//     makeAckPayload({
//       collectState: collect_state,
//       productIdx: product_idx,
//       productEngName: product_eng_name || session.productEngName,
//       categoryIdx: category_idx || session.categoryIdx,
//       isNew: is_new ?? session.isNew,
//       resultCd: "S",
//       resultMsg: "collection completed",
//       health,
//       extraData: {
//         collection_timestamp: session.timestamp,
//         foldername: uploadResult.foldername,
//         folderpath: uploadResult.folderpath,
//         filelength: uploadResult.filelength,
//         train_product_idx: productDoc?.trainProductIdx,
//         annotation: annotationResult,
//         training_notify: notifyResult,
//       },
//     })
//   );

//   collectSessions.delete(sessionKey);
// }

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

  await cameraStopSampling();

  const closed = await waitUntilDoorClosed();

  if (!closed) {
    throw new Error("Door close timeout. Product collection cannot be finalized.");
  }

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

  // console.log('[PNT DOOR REQ] status of doorState: ', doorState)
  // if (doorState === 'CLOSE') {
  //   const aiServer = `${config.aiServerApi}/v1/events/product/created`
  //   const now = new Date();
  //   const formattedDate = makeIFDate(now)
  //   const sysidDate = now.toISOString().replace(/[-:T]/g, "").slice(0, 8);
  //   const sysidTime = now.toISOString().replace(/[-:T]/g, "").slice(8, 14);
  //   const aiStorageType = storageType == 'COLD' ? 'True' : 'False';

  //   const payload = {
  //       HEADER: {
  //           IF_ID   : "IF_EDGE_01",
  //           IF_SYSID: `EDGEPC-${sysidDate}-${sysidTime}`,
  //           IF_HOST : "EDGEPC",
  //           IF_DATE : formattedDate
  //       },
  //       DATA: {
  //           division_idx: session.divisionIdx,
  //           // True: 냉장(Cold) / False: 냉동(Frozen)
  //           is_cold: aiStorageType
  //       },
  //   };

  //   console.log("[EDGE->AI] url:", aiServer);
  //   console.log("[EDGE->AI] payload:", JSON.stringify(payload, null, 2));
    
  //   try {
  //     const response = await axios.post(aiServer, payload, {
  //       headers: {
  //         "Content-Type": "application/json",
  //       },
  //       timeout: 10000,
  //     });

  //     console.log("[EDGE->AI] response:", response.data);
  //   } catch (err) {
  //     console.error("[EDGE->AI] status:", err.response?.status);
  //     console.error("[EDGE->AI] data:", err.response?.data);
  //     console.error("[EDGE->AI] message:", err.message);
  //   }
  // }

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
        filelength: uploadResult.filelength,
        train_product_idx: productDoc?.trainProductIdx,
      },
    })
  );

  /**
   * AI 서버 notify
   */
  // const notifyResult =
  //   await notifyAiTrainingStore({
  //     productIdx: product_idx,
  //     productEngName: product_eng_name,
  //     trainingStatus: "2",
  //   });

  try {
  const notifyResult = await notifyAiTrainingStore({
      // productIdx: product_idx,
      // productEngName: product_eng_name,
      // trainingStatus: "2",
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

  // publishAck(
  //   makeAckPayload({
  //     collectState: collect_state,
  //     productIdx: product_idx,
  //     productEngName: product_eng_name || session.productEngName,
  //     categoryIdx: category_idx || session.categoryIdx,
  //     isNew: is_new ?? session.isNew,
  //     resultCd: "S",
  //     resultMsg: "collection completed",
  //     health,
  //     extraData: {
  //       collection_timestamp: session.timestamp,
  //       foldername: uploadResult.foldername,
  //       folderpath: uploadResult.folderpath,
  //       filelength: uploadResult.filelength,
  //       train_product_idx: productDoc?.trainProductIdx,
  //     },
  //   })
  // );

  collectSessions.delete(String(product_idx));
}

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

// async function handleCollectMessage(message) {
//   let reqPayload = {};
//   let reqData = {};

//   try {
//     reqPayload = JSON.parse(message.toString());
//     reqData = reqPayload.DATA || {};

//     const collectState = reqData.collect_state;

//     console.log(`[AckCollect] Request Received: ${collectState}`);

//     if (collectState === "START") {
//       await handleStartCollect(reqData);
//       return;
//     }

//     if (collectState === "END") {
//       await handleEndCollect(reqData);
//       return;
//     }

//     throw new Error(`Unsupported collect_state: ${collectState}`);
//   } catch (error) {
//     console.error("[AckCollect] Processing Error:", error);

//     const health = await ProductCollectionHealth().catch(() => ({}));

//     publishAck(
//       makeAckPayload({
//         collectState: reqData.collect_state || "UNKNOWN",
//         productIdx: reqData.product_idx,
//         productEngName: reqData.product_eng_name,
//         categoryIdx: reqData.category_idx,
//         isNew: reqData.is_new,
//         resultCd: "F",
//         resultMsg: error?.message || String(error),
//         health,
//       })
//     );
//   }
// }

// async function AckCollect() {
//   if (client) return;

//   client = mqtt.connect(BROKER_URL);

//   client.on("connect", () => {
//     client.subscribe(CMD_TOPIC, (err) => {
//       if (err) {
//         console.error("[AckCollect] MQTT subscribe failed:", err);

//         publishAck(
//           makeAckPayload({
//             collectState: "SUBSCRIBE",
//             resultCd: "F",
//             resultMsg: `MQTT subscribe failed: ${err.message}`,
//           })
//         );

//         return;
//       }

//       console.log(`[AckCollect] subscribed: ${CMD_TOPIC}`);
//     });
//   });

//   client.on("message", (topic, message) => {
//     if (topic !== CMD_TOPIC) return;

//     chain = chain
//       .then(() => handleCollectMessage(message))
//       .catch((err) => {
//         console.error("[AckCollect] chain error:", err);
//       });
//   });

//   client.on("error", (err) => {
//     console.error("[AckCollect] MQTT client error:", err);
//   });

//   client.on("close", () => {
//     console.warn("[AckCollect] MQTT client closed");
//   });
// }

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

// const mqtt = require('mqtt');
// const path = require('path');
// const config = require("../../config/key");
// const { DeadboltStatusAPI, LoadcellStatusAPI, CameraStatusAPI } = require('../Mqtt/HealthMqtt');
// const { callApiToControlDeadbolt } = require('../Mqtt/DeadboltApiService');
// const { ProductUpload } = require("../../model/ProductUpload");

// const { v4: uuidv4 } = require("uuid");
// const axios = require('axios');
// const fs = require("fs");
// const Minio = require("minio");
// const glob = require("glob"); // 패턴 매칭으로 파일을 찾기 위해 권장 (npm install glob)
// const { tr } = require('zod/locales');

// const minioClient = new Minio.Client({
//   endPoint: config.minioURL,
//   port: 9000,
//   useSSL: false,
//   accessKey: config.minioAccessKey,
//   secretKey: config.minioSecretKey,
// });

// const BROKER_URL = `${config.mqttURL}`;
// const DEVICE_IDX = `${config.deviceIdx}`;

// const CMD_TOPIC = `chai/device/${DEVICE_IDX}/cmd/collect`;
// const ACK_TOPIC = `chai/device/${DEVICE_IDX}/ack/collect`;

// let client = null;
// let chain = Promise.resolve();  // IF06 순차처리용 Promise 체인
// let isDoorClosed = false;

// function makeTimestampFolderName(d = new Date()) {
//   const yyyy = d.getFullYear();
//   const mm = String(d.getMonth() + 1).padStart(2, "0");
//   const dd = String(d.getDate()).padStart(2, "0");
//   const HH = String(d.getHours()).padStart(2, "0");
//   const MM = String(d.getMinutes()).padStart(2, "0");
//   const SS = String(d.getSeconds()).padStart(2, "0");
//   return `${yyyy}${mm}${dd}_${HH}${MM}${SS}`;
// }

// function makeTimestamp(d = new Date()) {
//   const yyyy = d.getFullYear();
//   const mm = String(d.getMonth() + 1).padStart(2, "0");
//   const dd = String(d.getDate()).padStart(2, "0");
//   const HH = String(d.getHours()).padStart(2, "0");
//   const MM = String(d.getMinutes()).padStart(2, "0");
//   const SS = String(d.getSeconds()).padStart(2, "0");
//   return `${yyyy}${mm}${dd}${HH}${MM}${SS}`;
// }

// function fetchCurrentDoorState() {
//     return new Promise((resolve, reject) => {
//         const url = `${config.ioboardApi}/sse?streams=doors`;
//         const evtSource = new EventSource(url);
//         const timeout = setTimeout(() => {
//             evtSource.close();
//             console.warn("[DoorCheck] Timeout");
//             resolve("UNKNOWN");
//         }, 3000);

//         evtSource.addEventListener('door.update', (event) => {
//             if (!event.data) return;
//             try {
//                 const data = JSON.parse(event.data);
//                 const rawState = data.deadbolt ? data.deadbolt.toUpperCase() : "";
//                 const closedStates = ["LOCK", "LOCKED", "CLOSE", "CLOSED"];
//                 const finalState = closedStates.includes(rawState) ? "CLOSE" : "OPEN";

//                 clearTimeout(timeout);
//                 evtSource.close();
//                 resolve(finalState);
//             } catch (err) {
//                 clearTimeout(timeout);
//                 evtSource.close();
//                 resolve("UNKNOWN");
//             }
//         });
//         evtSource.onerror = (err) => {
//             clearTimeout(timeout);
//             evtSource.close();
//             resolve("UNKNOWN");
//         };
//     });
// }

// async function ProductCollectionHealth() {
//     const CameraStatus = await CameraStatusAPI(); // 예: '09'는 정상이라고 가정
//     const DeadboltHealth = await DeadboltStatusAPI();
//     const LoadcellHealth = await LoadcellStatusAPI();
//     const CurrentDoorState = await fetchCurrentDoorState();
    
//     const isHealthOk = (CameraStatus === '09' && DeadboltHealth === '19' && LoadcellHealth === '29');
//     isSuccess = isHealthOk;
//     resultMsg = (isHealthOk) ? "status access" : "status error";

//     return {CameraStatus, DeadboltHealth, LoadcellHealth, CurrentDoorState, isSuccess, resultMsg}
// }

// // function ensureClientOnce() {
// //   if (client) return;

// //   client = mqtt.connect(BROKER_URL);
// //   client.on('connect', () => client.subscribe(CMD_TOPIC));

// //   client.on('message', (topic, message) => {
// //     if (topic !== CMD_TOPIC) return;

// //     chain = chain
// //       .then(() => handleCollectMessage(message))
// //       .catch(err => console.error('[Collect] 처리 실패:', err));
// //   });
// // }

// async function AckCollect() {
//   // ensureClientOnce();
//   if (client) return;

//   client = mqtt.connect(BROKER_URL);
//   client.on('connect', () => client.subscribe(CMD_TOPIC));

//   client.on('message', (topic, message) => {
//     if (topic !== CMD_TOPIC) return;

//     chain = chain
//       .then(() => handleCollectMessage(message))
//       .catch(err => console.error('[Collect] 처리 실패:', err));
//   });
// }

// async function handleCollectMessage(message) {
//   const ReqPayload = JSON.parse(message.toString());
//   const ReqData = ReqPayload.DATA;

//   const {
//     device_idx, division_idx, product_idx, 
//     collect_state, product_eng_name, category_idx,is_new,
//   } = ReqData;

//   if ((config.deviceIdx != device_idx) || (config.divisionIdx != division_idx)) return;

//   const timestamp = makeTimestampFolderName();
//   const productFolder = path.join(process.cwd(), "productImg", `${product_idx}_${product_eng_name}_${timestamp}`); // 카메라 저장 경로 설정 --> product_idx 말고 mongodb의 trainProductIdx로 변경

//   if (collect_state == "START") { // 픽앤탁이 EdgePC로 "수집 시작해~" 라는 내용을 PUB 하면

//     const CurrentDoorState = await fetchCurrentDoorState();
//     if (CurrentDoorState === "CLOSE") {
//         // IF04에서 한번 열었지만 9초가 지나서 다시 닫힐 수 있으니
//         // 현재 문이 닫혀있는 상태이면
//         const openResult = await callApiToControlDeadbolt("OPEN"); // 열기
//     }

//     // 수집 제어 (시작)
//     await CamerastartSampling(productFolder, [0, 2]); // 카메라 키고
//     await startLoadcellRecording(); // 카메라 끄고

//     const health = await ProductCollectionHealth(); // EdgecPC가 픽앤탁으로 "나 수집 시작할게~" 하는 내용을 PUB
//     const ackPayload = {
//         HEADER: {
//         IF_ID: "IF_06",
//         IF_SYSID: uuidv4(),
//         IF_HOST: "EDGEPC01",
//         IF_DATE: makeTimestamp(),
//         },
//         DATA: {
//         device_idx: config.deviceIdx,
//         division_idx: config.divisionIdx,
//         product_idx: product_idx,
//         collect_state: collect_state,
//         product_eng_name: product_eng_name,
//         category_idx: category_idx,
//         is_new: is_new,
//         camera_status: (health.CameraStatus === '09') ? "1" : "0",
//         deadbolt_status: (health.DeadboltHealth === '19') ? "1" : "0",
//         loadcell_status: (health.LoadcellHealth === '29') ? "1" : "0",
//         result_cd: health.isSuccess ? "S" : "F",
//         result_msg: health.resultMsg,
//         }
//     };

//     client.publish(ACK_TOPIC, JSON.stringify(ackPayload)); 
//     // 문 열은 후에, "나 이제 수집 시작할게"라는 내용을 EdgePC가 픽앤탁으로 PUB
//     }

//     else if (collect_state == "END"){ // 픽앤탁이 EdgePC로 "수집 끝~" 라는 내용을 PUB 하면
//         while (!isDoorClosed) {
//             await new Promise(r => setTimeout(r, 500)); // 0.5초 간격으로 
//             const checkState = await fetchCurrentDoorState(); // 현재 진짜 문이 닫혀있는 상황인지 보고
//             if (checkState === "CLOSE") isDoorClosed = true; // 진짜 문이 닫혀있는 상황이면 무한 루프를 빠져나와
//         }

//         await CamerastopSampling(); // 카메라 끄고
//         await stopLoadcellRecording(); // 로드셀 끄고
//         // const openResult = await callApiToControlDeadbolt("CLOSE"); // 데드볼트도 제어 (닫기)


//         // 3. MinIO 업로드 & MongoDB 업로드 시작
//         if (productFolder && fs.existsSync(productFolder)) {
//         console.log(`[MinIO] Upload starting: ${productFolder}`);
//         try { // MinIO로 상위 폴더의 경로를 입력받아 이미지 데이터 업로드
//             await uploadFolderToMinio(productFolder, product_idx);
//         } catch (err) {
//             console.error(`[MinIO] Upload failed:`, err);
//         }
//         try{
//             await syncProductMetadata({ 
//                 // [질문] 몽고디비로 메타데이터를 전달하는데
//                 // 현재는 다음과 같은 4개의 데이터를 전달하는데
//                 // 추가적으로 어떠한 데이터를 전송하면 되는지?
//                 // 그리고 엑셀 파일에는 training_status가 아니라 status라고 적혀있는데
//                 // status로 받아오면 되는건지?
//                 // 그리고 픽앤탁 서버에서 status 값을 보내주지 않아서 현재는 EdgePC도 보내지 않도록 하고 있는데
//                 // 그래도 괜찮은지?
//                 product_idx,
//                 product_eng_name,
//                 category_idx,
//                 is_new,
//             });
//         } catch (err) {
//             console.error(`[MongoDB] Sync failed:`, err);
//         }
//         }
//         const health = await ProductCollectionHealth(); // EdgecPC가 픽앤탁으로 "나 수집 마칠게~" 하는 내용을 PUB
//         const ackPayload = {
//             HEADER: {
//             IF_ID: "IF_06",
//             IF_SYSID: uuidv4(),
//             IF_HOST: "EDGEPC01",
//             IF_DATE: makeTimestamp(),
//             },
//             DATA: {
//             device_idx: config.deviceIdx,
//             division_idx: config.divisionIdx,
//             product_idx: product_idx,
//             collect_state: collect_state,
//             product_eng_name: product_eng_name,
//             category_idx: category_idx,
//             is_new: is_new,
//             camera_status: (health.CameraStatus === '09') ? "1" : "0",
//             deadbolt_status: (health.DeadboltHealth === '19') ? "1" : "0",
//             loadcell_status: (health.LoadcellHealth === '29') ? "1" : "0",
//             result_cd: health.isSuccess ? "S" : "F",
//             result_msg: health.resultMsg,
//             }
//         };

//         client.publish(ACK_TOPIC, JSON.stringify(ackPayload));
//     }
// }

// async function CamerastartSampling(savePath, cameraIndices) {
//     try {
//         const url = `${config.cameraApi}/sampling/start`;
//         const body = {
//             save_path: savePath,
//             cameras: cameraIndices
//         };

//         console.log(`[Sampling] Starting... Path: ${savePath}`);
//         const response = await axios.post(url, body);

//         if (response.status === 200 && response.data.status === "recording started") {
//             console.log("[Sampling] Successfully started");
//             return response.data;
//         } else {
//             throw new Error(`Unexpected response: ${JSON.stringify(response.data)}`);
//         }
//     } catch (error) {
//         console.error("[Sampling] Start failed:", error.response?.data || error.message);
//         throw error;
//     }
// }

// async function startLoadcellRecording() {
//     try {
//         const response = await axios.post(`${config.ioboardApi}/start`);
//         if (response.status === 200) {
//             console.log("[신규 상품 등록] 로드셀 기록이 시작되었습니다.");
//         }
//     } catch (error) {
//         console.error("기록 시작 실패:", error.message);
//     }
// }

// async function CamerastopSampling() {
//     try {
//         const url = `${config.cameraApi}/sampling/stop`;
        
//         console.log("[Sampling] Stopping...");
//         const response = await axios.post(url);


//         if (response.status === 200 && response.data.status === "recording stopped") {
//             console.log("[Sampling] Successfully stopped");
//             return response.data;
//         } else {
//             throw new Error(`Unexpected response: ${JSON.stringify(response.data)}`);
//         }
//     } catch (error) {
//         console.error("[Sampling] Stop failed:", error.response?.data || error.message);
//         throw error;
//     }
// }

// async function stopLoadcellRecording() {
//     try {
//         const response = await axios.post(`${config.ioboardApi}/stop`);
//         if (response.status === 200) {
//             console.log("[신규 상품 등록] 로드셀 기록이 중지되었습니다.");
//         }
//     } catch (error) {
//         console.error("기록 중지 실패:", error.message);
//     }
// }

// async function uploadFolderToMinio(localPath, productIdx) {
//   const BUCKET = config.minioBucket;
//   const folderName = path.basename(localPath);
//   const basePrefix = `productImg/${safe(productIdx)}_${folderName.split('_').pop()}`;

//   // 1. 모든 파일 목록 가져오기
//   const getAllFiles = (dirPath, arrayOfFiles = []) => {
//     const files = fs.readdirSync(dirPath);
//     files.forEach(file => {
//       const fullPath = path.join(dirPath, file);
//       if (fs.statSync(fullPath).isDirectory()) {
//         arrayOfFiles = getAllFiles(fullPath, arrayOfFiles);
//       } else {
//         arrayOfFiles.push(fullPath);
//       }
//     });
//     return arrayOfFiles;
//   };

//   const filesToUpload = getAllFiles(localPath);

//   // 2. 순차 업로드
//   for (const filePath of filesToUpload) {
//     const relativePath = path.relative(localPath, filePath);
//     const objectKey = `${basePrefix}/${relativePath.replace(/\\/g, '/')}`;

//     await new Promise((resolve, reject) => {
//       minioClient.fPutObject(BUCKET, objectKey, filePath, {}, (err, etag) => {
//         if (err) return reject(err);
//         resolve(etag);
//       });
//     });

//     fs.unlinkSync(filePath);
//   }

//   const removeEmptyDirs = (dir) => {
//     const files = fs.readdirSync(dir);
//     if (files.length > 0) {
//       files.forEach(file => {
//         const fullPath = path.join(dir, file);
//         if (fs.statSync(fullPath).isDirectory()) removeEmptyDirs(fullPath);
//       });
//     }
//     if (fs.readdirSync(dir).length === 0) fs.rmdirSync(dir);
//   };

//   removeEmptyDirs(localPath);
// }

// async function syncProductMetadata(p) {
//   try {
//     const last = await ProductUpload.findOne({}, { trainProductIdx: 1 })
//       .sort({ trainProductIdx: -1 })
//       .lean();
//     let seq = Number(last?.trainProductIdx ?? 0);

//     const now = new Date();
//     const trainProductIdx = ++seq;

//     await ProductUpload.updateOne(
//       { productIdx: p.product_idx },
//       {
//         $set: {
//           categoryIdx: p.category_idx ?? "null",
//           isNew: p.is_new,
//           trainingStatus: "COLLECTED", // 수집 완료 상태로 기록
//           updateDate: now,
//         },
//         $setOnInsert: {
//           productIdx: p.product_idx,
//           productName: p.product_name, // API에서 받아온 이름이 있다면 할당
//           productEngName: p.product_eng_name,
//           trainProductIdx: trainProductIdx,
//           createDate: now,
//           // MinIO에 올린 경로와 일치하도록 설정
//           folderpath: `/productImg/${p.product_idx}_${p.product_eng_name}_${makeTimestampFolderName(now)}/`,
//           eventPromotion: [],
//         },
//       },
//       { upsert: true }
//     );
//     console.log(`[DB] Product ${p.product_idx} metadata uploaded.`);
//   } catch (error) {
//     console.error("[DB Error] Failed to sync product metadata:", error);
//   }
// }

// module.exports = { AckCollect };
