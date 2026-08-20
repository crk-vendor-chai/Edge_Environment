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

  const basePrefix = `productImg/${safe(foldername)}`;
  const folderpath = `s3://${BUCKET}/${basePrefix}/`;

  const filesToUpload = getAllFiles(localPath);

  if (!filesToUpload.length) {
    return {
      success: false,
      message: `No files found in folder: ${localPath}`,
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

// 수집 완료 상품의 학습 상태와 실제 학습 대상 장비 정보를 IF07 서비스에 전달
async function notifyAiTrainingStore(product) {
  console.log(
    '[notifyAiTrainingStore] product ======>',
    JSON.stringify(product, null, 2)
  );
  if (typeof aiNotifyService.notifyTrainingStore === "function") {

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
      },
    },
    { upsert: true }
  );

  const deviceTypeDeviceIdxArr = Array.from(
    new Set([
      ...(deviceTypeDoc?.deviceIdx || []),
      deviceIdx,
    ].filter(Boolean))
  );

  await DeviceTypeUpload.updateOne(
    {
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

  await cameraStartSampling(productFolder, [0, 1]);

  const useLoadcell = hasLoadcell === "Y";
  console.log('Loadcell is', useLoadcell)

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
    categoryIdx: category_idx,
    isNew: is_new,
    foldername: uploadResult.foldername,
    folderpath: uploadResult.folderpath,
    filelength: uploadResult.filelength,
    storageType: finalStorageType,
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
        filelength: uploadResult.filelength,
        train_product_idx: productDoc?.trainProductIdx,
      },
    })
  );

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

async function AckCollect() {

  client = getClient();

  client.subscribe(SUB_TOPIC, { qos: 1 }, (err, granted) => {
    if (err) {
      console.error("[ACK-COLLECT] Subscribe Error:", err.message);
      return;
    }

    console.log("[ACK-COLLECT] Subscribed:", granted);
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