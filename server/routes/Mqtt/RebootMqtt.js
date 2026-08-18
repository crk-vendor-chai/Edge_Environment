// ============================================================
// RebootMqtt.js
// 역할: 클라우드(PNT/CHAI)의 재부팅 명령(MANUAL/EMBEDDING)을 MQTT로 수신하여
//       시스템 재부팅을 수행하는 모듈. EMBEDDING 모드에서는 재부팅 전에
//       MinIO trained_model/에서 새 모델(.pt/.onnx)을 내려받아 engine을 빌드하고
//       .env의 MODEL_VERSION을 갱신한다. 재부팅 후에는 reboot.flag를 기반으로
//       자가 진단(service/JWT/MQTT health check)과 IF07 배포 완료 notify,
//       COMPLETED ack 발행까지 처리한다.
// MQTT topic: 구독 chai/device/{deviceIdx}/cmd/reboot
//             발행 chai/device/{deviceIdx}/ack/reboot (IF_01 ack)
// 외부 연동: MinIO(trained_model 다운로드), systemctl(재부팅, engine 빌드 서비스),
//            REST API IF_13(DeviceInfo) / IF_11(ProductList) / IF_07(TrainingStore)
// ============================================================
const path = require("path");
const fs = require("fs/promises");
const { createWriteStream } = require("fs");
const { v4: uuidv4 } = require("uuid");
const { exec } = require("child_process");
const { pipeline } = require("stream/promises");
const Minio = require("minio");

const { getClient, publish } = require("./MqttClient");
const config = require("../../config/key");

const { DeviceInfo } = require("../RestAPI/DeviceInfo");
const { ProductList } = require("../RestAPI/ProductList");
const { TrainingStore } = require("../RestAPI/TrainingStore");
const { getProcessing } = require("../RestAPI/PaymentProcessing");

// const {
//   fetchCurrentDoorState,
// } = require("./AckCollect");

// const {
//   DeadboltStatusAPI,
//   LoadcellStatusAPI,
//   CameraStatusAPI,
// } = require("./HealthMqtt");

const ENV_FILE_PATH = path.resolve(__dirname, "../../.env");
const REBOOT_FLAG = path.resolve(__dirname, "../../log/reboot.flag");

const MINIO_BUCKET = config.minioBucket || "chaiimage";
const TRAINED_MODEL_PREFIX = "trained_model/";

// 모델 코드/모델 저장 경로 및 engine 빌드 env 파일 경로 (config에서 주입,
// env 미설정 시 기존 /home/chai/Desktop/... 경로가 기본값)
const LOCAL_MODEL_CODES_DIR = config.modelCodesDir;

const LOCAL_MODEL_DIR = config.modelDir;

const ENGINE_BUILD_ENV_FILE = config.engineBuildEnvFile;

let rebooting = false;

const minioClient = new Minio.Client({
  endPoint: config.minioURL,
  port: Number(config.minioPort || 9000),
  useSSL: Boolean(config.minioUseSSL || false),
  accessKey: config.minioAccessKey,
  secretKey: config.minioSecretKey,
});

// 지정한 시간(ms)만큼 대기하는 Promise를 반환한다.
function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

// 파일 존재 여부를 확인한다(stat 실패 시 false).
async function fileExists(filePath) {
  try {
    await fs.stat(filePath);
    return true;
  } catch {
    return false;
  }
}

// .env 파일에서 JWT_TOKEN 값을 다시 읽어 process.env에 반영한다.
// 유효한 토큰(길이 10 초과)이 없으면 null을 반환한다.
async function reloadJwtTokenFromEnvFile() {
  try {
    if (!(await fileExists(ENV_FILE_PATH))) {
      return null;
    }

    const envContent = await fs.readFile(
      ENV_FILE_PATH,
      "utf-8"
    );

    const match = envContent.match(
      /^JWT_TOKEN=(.*)$/m
    );

    if (!match) {
      return null;
    }

    const token = String(match[1] || "")
      .trim()
      .replace(/^["']|["']$/g, "");

    if (token.length > 10) {
      process.env.JWT_TOKEN = token;
      return token;
    }

    return null;
  } catch (error) {
    console.warn(
      "[JWT] .env reload failed:",
      error.message
    );

    return null;
  }
}

// 재부팅 직후 JWT Access Token이 발급될 때까지 polling으로 대기한다.
// 환경변수에 없으면 .env 재로딩을 시도하고, timeout 시 false를 반환한다.
async function waitForJwtToken({
  timeoutMs = 120000,
  intervalMs = 3000,
} = {}) {
  const startedAt = Date.now();
  let attempt = 0;

  while (
    Date.now() - startedAt < timeoutMs
  ) {
    attempt += 1;
    
    let token = process.env.JWT_TOKEN;

    if (
      !token ||
      typeof token !== "string" ||
      token.length <= 10
    ) {
      token =
        await reloadJwtTokenFromEnvFile();
    }

    if (
      token &&
      typeof token === "string" &&
      token.length > 10
    ) {
      console.log(
        `[JWT] Access Token 확인 완료. ` +
        `attempt=${attempt}`
      );

      return true;
    }

    console.log(
      `[JWT] Access Token 발급 대기 중... ` +
      `attempt=${attempt}`
    );

    await sleep(intervalMs);
  }

  console.error(
    `[JWT] Access Token 대기 시간 초과: ` +
    `${timeoutMs}ms`
  );

  return false;
}

/**
 * IF_DATE 생성
 */
function makeIFDate() {
  const date = new Date();

  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const HH = String(date.getHours()).padStart(2, "0");
  const MM = String(date.getMinutes()).padStart(2, "0");
  const SS = String(date.getSeconds()).padStart(2, "0");

  return `${yyyy}${mm}${dd}${HH}${MM}${SS}`;
}

/**
 * 재부팅 ACK Payload 생성
 */
function makeRebootAckPayload({
  ifSysId,
  deviceIdx,
  divisionIdx,
  resultCd,
  resultMsg,
  rebootState,
}) {
  return {
    HEADER: {
      IF_ID: "IF_01",
      IF_SYSID: ifSysId || uuidv4(),
      IF_HOST: "CRKPNTCHAI",
      IF_DATE: makeIFDate(),
    },
    DATA: {
      device_idx: deviceIdx,
      division_idx: divisionIdx,
      reboot_state: rebootState,
      result_cd: resultCd,
      result_msg: resultMsg,
    },
  };
}

/**
 * 재부팅 ACK 전송
 */
async function publishRebootAck({
  deviceIdx,
  divisionIdx,
  ifSysId,
  rebootState,
  resultCd,
  resultMsg,
}) {
  const rebootTopic =
    `chai/device/${deviceIdx}/ack/reboot`;

  const payload = makeRebootAckPayload({
    ifSysId,
    deviceIdx,
    divisionIdx,
    rebootState,
    resultCd,
    resultMsg,
  });

  await publish(rebootTopic, payload, {
    qos: 1,
    retain: false,
  });

  console.log(
    `[REBOOT] ACK published: ` +
    `topic=${rebootTopic}, ` +
    `state=${rebootState}, ` +
    `result=${resultCd}`
  );
}

/**
 * 재부팅 완료 ACK 전송
 *
 * COMPLETED ACK 전송에 성공했을 때만
 * reboot.flag를 삭제합니다.
 */
async function publishRebootAckOnce(
  deviceIdx,
  divisionIdx
) {
  if (!(await fileExists(REBOOT_FLAG))) {
    return;
  }

  const raw = await fs.readFile(REBOOT_FLAG, "utf-8");
  const flag = JSON.parse(raw);

  const ifSysId = flag?.ifSysId || uuidv4();

  await publishRebootAck({
    deviceIdx,
    divisionIdx,
    ifSysId,
    rebootState: "COMPLETED",
    resultCd: "S",
    resultMsg: "reboot completed",
  });

  await fs.unlink(REBOOT_FLAG);

  console.log(
    "[REBOOT] COMPLETED ACK 전송 및 reboot.flag 삭제 완료"
  );
}

/**
 * MQTT 재부팅 명령 토픽 구독
 */
function subscribeRebootTopic(client, rebootSub) {
  return new Promise((resolve, reject) => {
    client.subscribe(
      rebootSub,
      { qos: 1 },
      (error, granted) => {
        if (error) {
          console.error(
            "[MQTT] reboot subscribe error:",
            error.message
          );

          reject(error);
          return;
        }

        console.log(
          "[MQTT-REBOOT] subscribed:",
          granted
        );

        resolve(granted);
      }
    );
  });
}

/**
 * edge-environment.service 상태 확인
 */
function checkServiceStatus() {
  return new Promise((resolve) => {
    exec(
      "systemctl is-active edge-environment.service",
      (error, stdout) => {
        const status = stdout
          ? stdout.trim()
          : "unknown";

        if (error || status !== "active") {
          resolve({
            ok: false,
            msg:
              status ||
              error?.message ||
              "unknown",
          });

          return;
        }

        resolve({
          ok: true,
          msg: status,
        });
      }
    );
  });
}

/**
 * JWT Access Token 존재 여부 확인
 *
 * 현재는 JWT_TOKEN 환경변수에 일정 길이 이상의
 * 문자열이 존재하는지만 확인합니다.
 */
function checkJwtToken() {
  const token = process.env.JWT_TOKEN;

  return Boolean(
    token &&
    typeof token === "string" &&
    token.length > 10
  );
}

/**
 * MQTT Connect, Subscribe, Publish, Receive 확인
 */
function checkMqttPubSub(client, deviceIdx) {
  return new Promise((resolve) => {
    const testTopic =
      `chai/device/${deviceIdx}/health/test`;

    const testPayload =
      `ping_${Date.now()}_${uuidv4()}`;

    let completed = false;
    let timeout = null;

    const finish = (result) => {
      if (completed) {
        return;
      }

      completed = true;

      if (timeout) {
        clearTimeout(timeout);
      }

      client.removeListener(
        "message",
        testListener
      );

      client.unsubscribe(
        testTopic,
        () => {}
      );

      resolve(result);
    };

    const testListener = (topic, payload) => {
      if (
        topic === testTopic &&
        payload.toString() === testPayload
      ) {
        finish(true);
      }
    };

    client.on("message", testListener);

    timeout = setTimeout(() => {
      console.error(
        "[MQTT-DIAGNOSTIC] Pub/Sub test timeout"
      );

      finish(false);
    }, 5000);

    client.subscribe(
      testTopic,
      { qos: 1 },
      (subscribeError) => {
        if (subscribeError) {
          console.error(
            "[MQTT-DIAGNOSTIC] Subscribe failed:",
            subscribeError.message
          );

          finish(false);
          return;
        }

        client.publish(
          testTopic,
          testPayload,
          { qos: 1 },
          (publishError) => {
            if (publishError) {
              console.error(
                "[MQTT-DIAGNOSTIC] Publish failed:",
                publishError.message
              );

              finish(false);
            }
          }
        );
      }
    );
  });
}

/**
 * 재부팅 후 자가 진단
 */
async function runStartupDiagnostics(
  client,
  deviceIdx
) {
  console.log(
    "\n========== [Reboot Diagnostics] =========="
  );

  console.log(
    "재부팅 완료 감지됨. 시스템 자가 진단을 시작합니다."
  );

  const serviceResult =
    await checkServiceStatus();

  console.log(
    `[Diagnostic] 1. edge-environment.service: ` +
    `${serviceResult.ok ? "OK" : "FAIL"} ` +
    `(${serviceResult.msg})`
  );

  const jwtOk =
    await waitForJwtToken({
      timeoutMs: 120000,
      intervalMs: 3000,
    });

  console.log(
    `[Diagnostic] 2. JWT Access Token: ` +
    `${jwtOk ? "OK" : "FAIL"}`
  );

  if (!jwtOk) {
    console.log(
      "==========================================\n"
    );

    return false;
  }

  const mqttOk =
    await checkMqttPubSub(client, deviceIdx);

  console.log(
    `[Diagnostic] 3. MQTT Pub/Sub: ` +
    `${mqttOk ? "OK" : "FAIL"}`
  );

  console.log(
    "==========================================\n"
  );

  return serviceResult.ok && jwtOk && mqttOk;
}

// EMBEDDING 재부팅 후속 처리: IF11(ProductList)로 매장 상품 전체를 조회하여
// 유효한 상품만 Map으로 구성한 뒤, IF07(TrainingStore)로
// training_status=1(배포 완료)을 한 번에 전송한다.
async function notifyDeployCompleteForAllProducts(
  divisionIdx,
  deviceIdx
) {
  /*
   * IF11 상품 목록 조회
   */
  const productResponse = await ProductList({
    division_idx: divisionIdx,
    device_idx: deviceIdx,
  });

  console.log(
    "[ProductList in RebootMqtt] response:",
    JSON.stringify(productResponse, null, 2)
  );

  /*
   * ProductList 응답 구조:
   *
   * {
   *   DATA: {
   *     product_list: [...]
   *   }
   * }
   */
  const products = productResponse.DATA.product_list;

  if (
    !Array.isArray(products) ||
    products.length === 0
  ) {
    throw new Error(
      "IF11 DATA.product_list is empty"
    );
  }

  /*
  * product_idx를 Key로 하는 Map 생성
  *
  * Key   : String(product_idx)
  * Value : 원본 상품 정보
  */
  const productMap = new Map(
    products
      .filter((product) => {
        const productIdx = product?.product_idx;
        const productEngName = product?.product_eng_name;

        const isValid =
          productIdx !== undefined &&
          productIdx !== null &&
          Boolean(productEngName);

        if (!isValid) {
          console.warn(
            "[IF07] 상품 정보 누락으로 제외:",
            {
              product_idx: productIdx,
              product_eng_name: productEngName,
            }
          );
        }

        return isValid;
      })
      .map((product) => [
        String(product.product_idx),
        {
          product_idx: String(product.product_idx),
          product_eng_name: product.product_eng_name,
        },
      ])
  );

  if (productMap.size === 0) {
    throw new Error(
      "IF07 전송 가능한 상품이 없습니다."
    );
  }

  console.log(
    `[IF07] 전송 대상 상품 수: ${productMap.size}`
  );

  console.log(
    "[IF07] 전송 대상 상품:",
    Array.from(productMap.values())
  );

  /*
  * 상품 전체를 한 번의 IF07 요청으로 전달
  */
  const result = await TrainingStore(
    productMap,
    "1"
  );

  console.log(
    "[IF07] 배포 완료 전송 결과:",
    result
  );

  return result;
}

/**
 * .env MODEL_VERSION 변경
 */
async function updateEnvModelVersion(newVersion) {
  const normalizedVersion = String(newVersion || "").trim();

  if (!normalizedVersion) {
    throw new Error(
      "MODEL_VERSION is empty"
    );
  }

  if (
    !/^[A-Za-z0-9._-]+$/.test(
      normalizedVersion
    )
  ) {
    throw new Error(
      `Invalid MODEL_VERSION: ${normalizedVersion}`
    );
  }

  let envContent = "";

  if (await fileExists(ENV_FILE_PATH)) {
    envContent = await fs.readFile(
      ENV_FILE_PATH,
      "utf-8"
    );
  }

  const regex = /^MODEL_VERSION=.*$/m;

  if (regex.test(envContent)) {
    envContent = envContent.replace(
      regex,
      `MODEL_VERSION=${normalizedVersion}`
    );
  } else {
    envContent +=
      `\nMODEL_VERSION=${normalizedVersion}\n`;
  }

  const temporaryPath =
    `${ENV_FILE_PATH}.tmp`;

  await fs.writeFile(
    temporaryPath,
    envContent,
    "utf-8"
  );

  await fs.rename(
    temporaryPath,
    ENV_FILE_PATH
  );

  process.env.MODEL_VERSION =
    normalizedVersion;

  console.log(
    `[ENV] MODEL_VERSION 업데이트 완료: ` +
    `${normalizedVersion}`
  );
}

/**
 * MinIO trained_model/ 하위 폴더 조회
 */
function listTrainedModelFolders() {
  return new Promise((resolve, reject) => {
    const folders = new Set();

    const stream =
      minioClient.listObjectsV2(
        MINIO_BUCKET,
        TRAINED_MODEL_PREFIX,
        false
      );

    stream.on("data", (objectInfo) => {
      if (objectInfo.prefix) {
        const folderName =
          objectInfo.prefix
            .replace(
              TRAINED_MODEL_PREFIX,
              ""
            )
            .replace(/\/$/, "");

        if (folderName) {
          folders.add(folderName);
        }
      }

      if (objectInfo.name) {
        const rest =
          objectInfo.name.replace(
            TRAINED_MODEL_PREFIX,
            ""
          );

        const firstDepth =
          rest.split("/")[0];

        if (
          firstDepth &&
          rest.includes("/")
        ) {
          folders.add(firstDepth);
        }
      }
    });

    stream.on("error", (error) => {
      console.error(
        "[MINIO] trained_model folder " +
        "list failed:",
        error.message
      );

      reject(error);
    });

    stream.on("end", () => {
      const result =
        [...folders].sort();

      console.log(
        "[MINIO] trained_model folders:",
        result
      );

      resolve(result);
    });
  });
}

/**
 * MinIO 객체 존재 여부 확인
 */
async function minioObjectExists(objectKey) {
  try {
    await minioClient.statObject(
      MINIO_BUCKET,
      objectKey
    );

    return true;
  } catch (error) {
    if (
      error.code === "NotFound" ||
      error.code === "NoSuchKey" ||
      error.statusCode === 404
    ) {
      return false;
    }

    throw error;
  }
}

/**
 * Shell 명령 실행
 */
function runShellCommand(command, label) {
  return new Promise((resolve) => {
    console.log(
      `[CLEANUP] ${label}: ${command}`
    );

    exec(
      command,
      {
        maxBuffer: 10 * 1024 * 1024,
      },
      (error, stdout, stderr) => {
        if (stdout) {
          console.log(
            `[CLEANUP][${label}] stdout:\n` +
            stdout
          );
        }

        if (stderr) {
          console.warn(
            `[CLEANUP][${label}] stderr:\n` +
            stderr
          );
        }

        if (error) {
          console.warn(
            `[CLEANUP][${label}] failed: ` +
            error.message
          );

          resolve(false);
          return;
        }

        resolve(true);
      }
    );
  });
}

/**
 * 디스크 여유 공간 확인
 */
function getAvailableDiskMB(
  targetPath = "/"
) {
  return new Promise((resolve, reject) => {
    exec(
      `df -Pm "${targetPath}" | ` +
      `awk 'NR==2 {print $4}'`,
      (error, stdout, stderr) => {
        if (error) {
          reject(error);
          return;
        }

        const availableMB =
          Number(String(stdout).trim());

        if (Number.isNaN(availableMB)) {
          reject(
            new Error(
              "failed to parse df output: " +
              (stdout || stderr)
            )
          );

          return;
        }

        resolve(availableMB);
      }
    );
  });
}

/**
 * 모델 다운로드 전 캐시 정리
 */
async function cleanupCachesBeforeModelDownload() {
  console.log(
    "[CLEANUP] 모델 다운로드 전 " +
    "캐시 정리를 시작합니다."
  );

  const beforeMB =
    await getAvailableDiskMB("/")
      .catch(() => null);

  if (beforeMB !== null) {
    console.log(
      `[CLEANUP] before available disk: ` +
      `${beforeMB} MB`
    );
  }

  await runShellCommand(
    "uv cache clean || true",
    "uv cache clean"
  );

  await runShellCommand(
    "python3 -m pip cache purge || true",
    "pip cache purge"
  );

  await runShellCommand(
    "rm -rf /home/chai/.cache/uv/* || true",
    "remove uv cache"
  );

  await runShellCommand(
    "rm -rf /home/chai/.cache/pip/* || true",
    "remove pip cache"
  );

  await runShellCommand(
    `find "${LOCAL_MODEL_DIR}" ` +
    `-maxdepth 1 ` +
    `\\( -name "*.part.minio" ` +
    `-o -name "*.download" \\) ` +
    `-type f -delete || true`,
    "remove partial files"
  );

  const afterMB =
    await getAvailableDiskMB("/")
      .catch(() => null);

  if (afterMB !== null) {
    console.log(
      `[CLEANUP] after available disk: ` +
      `${afterMB} MB`
    );
  }

  console.log(
    "[CLEANUP] 모델 다운로드 전 캐시 정리 완료"
  );
}

/**
 * MinIO 객체 다운로드
 */
async function downloadMinioObject(
  objectKey,
  localPath
) {
  await fs.mkdir(
    path.dirname(localPath),
    { recursive: true }
  );

  const directory =
    path.dirname(localPath);

  const baseName =
    path.basename(localPath);

  const temporaryPath =
    `${localPath}.download`;

  const existingFiles =
    await fs.readdir(directory)
      .catch(() => []);

  for (const file of existingFiles) {
    if (
      file.startsWith(baseName) &&
      (
        file.endsWith(".part.minio") ||
        file.endsWith(".download")
      )
    ) {
      const stalePath =
        path.join(directory, file);

      console.warn(
        `[MINIO] remove stale file: ` +
        `${stalePath}`
      );

      await fs.unlink(stalePath)
        .catch(() => {});
    }
  }

  const objectStat =
    await minioClient.statObject(
      MINIO_BUCKET,
      objectKey
    );

  const expectedSize =
    Number(objectStat.size || 0);

  if (expectedSize <= 0) {
    throw new Error(
      `MinIO object size invalid: ` +
      `${objectKey}, size=${expectedSize}`
    );
  }

  const availableMB =
    await getAvailableDiskMB(directory)
      .catch(() => null);

  const requiredMB =
    Math.ceil(
      expectedSize / 1024 / 1024
    ) + 500;

  if (availableMB !== null) {
    console.log(
      `[MINIO] available disk: ` +
      `${availableMB} MB`
    );

    console.log(
      `[MINIO] required disk: ` +
      `${requiredMB} MB`
    );

    if (availableMB < requiredMB) {
      throw new Error(
        `Not enough disk space. ` +
        `available=${availableMB}MB, ` +
        `required=${requiredMB}MB`
      );
    }
  }

  console.log(
    `[MINIO] download start: ` +
    `${MINIO_BUCKET}/${objectKey}`
  );

  const objectStream =
    await minioClient.getObject(
      MINIO_BUCKET,
      objectKey
    );

  await pipeline(
    objectStream,
    createWriteStream(temporaryPath)
  );

  const temporaryStat =
    await fs.stat(temporaryPath)
      .catch(() => null);

  if (
    !temporaryStat ||
    temporaryStat.size <= 0
  ) {
    throw new Error(
      `Downloaded file is empty: ` +
      temporaryPath
    );
  }

  if (
    temporaryStat.size !==
    expectedSize
  ) {
    throw new Error(
      `Downloaded size mismatch: ` +
      `expected=${expectedSize}, ` +
      `actual=${temporaryStat.size}`
    );
  }

  await fs.rename(
    temporaryPath,
    localPath
  );

  const finalStat =
    await fs.stat(localPath)
      .catch(() => null);

  if (
    !finalStat ||
    finalStat.size !== expectedSize
  ) {
    throw new Error(
      `Final file verification failed: ` +
      localPath
    );
  }

  console.log(
    `[MINIO] download complete: ` +
    `${localPath}, size=${finalStat.size}`
  );
}

/**
 * brunch 폴더에서 PT, ONNX 다운로드
 */
async function downloadModelFilesFromBrunchFolder(
  brunchName,
  modelVersion
) {
  if (!brunchName) {
    throw new Error(
      "brunchName is empty"
    );
  }

  if (!modelVersion) {
    throw new Error(
      "modelVersion is empty"
    );
  }

  const folderPrefix =
    `${TRAINED_MODEL_PREFIX}` +
    `${brunchName}/`;

  const ptObjectKey =
    `${folderPrefix}${modelVersion}.pt`;

  const onnxObjectKey =
    `${folderPrefix}${modelVersion}.onnx`;

  const localPtPath =
    path.join(
      LOCAL_MODEL_DIR,
      `${modelVersion}.pt`
    );

  const localOnnxPath =
    path.join(
      LOCAL_MODEL_DIR,
      `${modelVersion}.onnx`
    );

  if (
    !(await minioObjectExists(ptObjectKey))
  ) {
    throw new Error(
      `MinIO object not found: ` +
      ptObjectKey
    );
  }

  if (
    !(await minioObjectExists(onnxObjectKey))
  ) {
    throw new Error(
      `MinIO object not found: ` +
      onnxObjectKey
    );
  }

  await downloadMinioObject(
    ptObjectKey,
    localPtPath
  );

  await downloadMinioObject(
    onnxObjectKey,
    localOnnxPath
  );

  return {
    pt: localPtPath,
    onnx: localOnnxPath,
  };
}

/**
 * Engine 생성 후 다운로드한 PT, ONNX 파일 삭제
 */
async function deleteDownloadedModelFiles(downloadedFiles) {
  const targets = [
    {
      type: "PT",
      filePath: downloadedFiles?.pt,
    },
    {
      type: "ONNX",
      filePath: downloadedFiles?.onnx,
    },
  ];

  for (const target of targets) {
    if (!target.filePath) {
      console.warn(
        `[CLEANUP] ${target.type} 파일 경로가 없습니다.`
      );
      continue;
    }

    try {
      await fs.unlink(target.filePath);

      console.log(
        `[CLEANUP] ${target.type} 파일 삭제 완료: ` +
        target.filePath
      );
    } catch (error) {
      if (error.code === "ENOENT") {
        console.warn(
          `[CLEANUP] ${target.type} 파일이 이미 없습니다: ` +
          target.filePath
        );
        continue;
      }

      console.error(
        `[CLEANUP] ${target.type} 파일 삭제 실패: ` +
        `${target.filePath}, error=${error.message}`
      );

      throw error;
    }
  }
}

/**
 * Engine 빌드 환경 파일 생성
 */
async function writeEngineBuildTxt(
  modelVersion
) {
  const content =
`MODEL_DIR=${LOCAL_MODEL_CODES_DIR}
VENV_ACTIVATE=${LOCAL_MODEL_CODES_DIR}/.engine_build_env/bin/activate
MODELS_DIR=${LOCAL_MODEL_DIR}
PT_FILE=${LOCAL_MODEL_DIR}/${modelVersion}.pt
ENGINE_FILE=${LOCAL_MODEL_DIR}/${modelVersion}.engine
`;

  try {
    await fs.writeFile(
      ENGINE_BUILD_ENV_FILE,
      content,
      "utf-8"
    );

    console.log(
      `[ENGINE-BUILD] 설정 파일 작성 완료: ` +
      ENGINE_BUILD_ENV_FILE
    );
  } catch (error) {
    console.error(
      "[ENGINE-BUILD] 설정 파일 작성 실패:",
      error.message
    );

    throw error;
  }
}

/**
 * Engine 빌드 서비스 실행
 */
function startCrkModelBuildServiceWithLogs() {
  return new Promise((resolve, reject) => {
    const serviceName =
      "crk-model-build.service";

    const startCommand =
      `sudo -n systemctl start ` +
      serviceName;

    console.log(
      `[SYSTEMD] 실행: ${startCommand}`
    );

    exec(
      startCommand,
      {
        maxBuffer: 20 * 1024 * 1024,
      },
      (startError, stdout, stderr) => {
        if (stdout) {
          console.log(
            `[SYSTEMD] stdout:\n${stdout}`
          );
        }

        if (stderr) {
          console.warn(
            `[SYSTEMD] stderr:\n${stderr}`
          );
        }

        if (startError) {
          console.error(
            `[SYSTEMD] ${serviceName} ` +
            `실행 실패: ${startError.message}`
          );

          reject(startError);
          return;
        }

        const logCommand =
          `journalctl -u ${serviceName} ` +
          `--no-pager -n 100 -o cat`;

        exec(
          logCommand,
          {
            maxBuffer: 20 * 1024 * 1024,
          },
          (logError, logStdout, logStderr) => {
            if (logStdout) {
              console.log(
                `[SYSTEMD] ${serviceName} logs:\n` +
                logStdout
              );
            }

            if (logStderr) {
              console.warn(
                `[SYSTEMD] journalctl stderr:\n` +
                logStderr
              );
            }

            if (logError) {
              console.warn(
                "[SYSTEMD] journalctl 조회 실패:",
                logError.message
              );
            }

            console.log(
              `[SYSTEMD] ${serviceName} 실행 완료`
            );

            resolve(true);
          }
        );
      }
    );
  });
}

/**
 * Camera, Deadbolt, Loadcell 상태 확인 후 전체 정상 여부(boolean)를 반환
 * (AckCollect.js의 ProductCollectionHealth와 달리 상태 객체가 아닌 boolean 반환)
 */
async function isDeviceSensorsHealthy() {
  const [
    cameraStatus,
    deadboltStatus,
    loadcellStatus,
    currentDoorState,
  ] = await Promise.all([
    CameraStatusAPI(),
    DeadboltStatusAPI(),
    LoadcellStatusAPI(),
    fetchCurrentDoorState(),
  ]);

  const isHealthOk =
    cameraStatus === "09" &&
    deadboltStatus === "19" &&
    loadcellStatus === "29";

  console.log(
    "[ACK-CHECK] CameraStatus:",
    cameraStatus
  );

  console.log(
    "[ACK-CHECK] DeadboltStatus:",
    deadboltStatus
  );

  console.log(
    "[ACK-CHECK] LoadcellStatus:",
    loadcellStatus
  );

  console.log(
    "[ACK-CHECK] CurrentDoorState:",
    currentDoorState
  );

  console.log(
    "[ACK-CHECK] isHealthOk:",
    isHealthOk
  );

  return isHealthOk;
}

/**
 * 시스템 재부팅
 */
function runReboot() {
  if (process.platform === "darwin") {
    console.log(
      "[REBOOT] 개발 환경에서는 재부팅을 생략합니다."
    );

    return Promise.resolve(false);
  }

  if (process.platform !== "linux") {
    return Promise.reject(
      new Error(
        `Unsupported platform: ` +
        process.platform
      )
    );
  }

  console.log(
    "[REBOOT] 시스템 재부팅을 요청합니다."
  );

  return new Promise((resolve, reject) => {
    exec(
      "sleep 2 && sudo -n systemctl reboot",
      (error, stdout, stderr) => {
        if (stdout) {
          console.log(
            "[REBOOT] stdout:",
            stdout
          );
        }

        if (stderr) {
          console.warn(
            "[REBOOT] stderr:",
            stderr
          );
        }

        if (error) {
          reject(error);
          return;
        }

        resolve(true);
      }
    );
  });
}

/**
 * MQTT 재부팅 처리
 */
async function RebootMqtt() {
  const deviceIdx =
    config.deviceIdx;

  const divisionIdx =
    config.divisionIdx;

  if (!deviceIdx || !divisionIdx) {
    console.error(
      "[REBOOT] Missing deviceIdx/divisionIdx"
    );

    return;
  }

  const rebootSub =
    `chai/device/${deviceIdx}/cmd/reboot`;

  const client = getClient();

  /**
   * 재부팅 명령 수신
   */
  client.on(
    "message",
    async (topic, payload) => {
      if (topic !== rebootSub) {
        return;
      }

      if (rebooting) {
        console.warn(
          "[REBOOT] reboot already in progress. " +
          "ignore duplicated command."
        );

        return;
      }

      rebooting = true;

      let message;

      try {
        message = JSON.parse(
          payload.toString()
        );
      } catch (parseError) {
        console.error(
          "[REBOOT] Invalid JSON payload:",
          parseError.message
        );

        rebooting = false;
        return;
      }

      const ifSysId =
        message?.HEADER?.IF_SYSID ||
        uuidv4();

      const rebootMode =
        String(
          message?.DATA?.reboot_mode ||
          "MANUAL"
        )
          .trim()
          .toUpperCase();

      console.log(
        `[MQTT] reboot command received: ` +
        `IF_SYSID=${ifSysId}, ` +
        `mode=${rebootMode}`
      );

      let flagWritten = false;

      try {
        /**
         * 결제 진행 중에는 재부팅 금지
         * ① 재부팅 명령 수신 직후
         * → 이미 결제 중이면 바로 거절
         */
        if (getProcessing()) {
          console.warn(
            "[REBOOT] Payment is processing. Reboot rejected."
          );

          await publishRebootAck({
            deviceIdx,
            divisionIdx,
            ifSysId,
            rebootState: "REJECTED",
            resultCd: "F",
            resultMsg: "Payment is processing",
          });

          rebooting = false;
          return;
        }
        if (
          !["MANUAL", "EMBEDDING"]
            .includes(rebootMode)
        ) {
          await publishRebootAck({
            deviceIdx,
            divisionIdx,
            ifSysId,
            rebootState: "REJECTED",
            resultCd: "F",
            resultMsg:
              `Unsupported reboot mode: ` +
              rebootMode,
          });

          rebooting = false;
          return;
        }

        /**
         * EMBEDDING일 때만 모델 업데이트 수행
         */
        if (rebootMode === "EMBEDDING") {
          console.log(
            "[ModelEmbedding] 모델 업데이트 절차 시작"
          );

          const deviceList =
            await DeviceInfo();

          console.log(
            "[ModelEmbedding] IF_13 result:",
            deviceList
          );

          if (
            !Array.isArray(deviceList) ||
            deviceList.length === 0
          ) {
            throw new Error(
              "IF_13 장비 정보가 없습니다."
            );
          }

          const myDeviceInfo =
            deviceList.find((item) => {
              const itemDeviceIdx =
                item.device_idx ??
                item.deviceIdx;

              return (
                String(itemDeviceIdx) ===
                String(deviceIdx)
              );
            }) || deviceList[0];

          const modelVersion =
            String(
              myDeviceInfo.model_version ||
              ""
            ).trim();

          const brunchName =
            String(
              myDeviceInfo.brunch_name ||
              ""
            ).trim();

          if (!modelVersion) {
            throw new Error(
              "IF_13 model_version이 없습니다."
            );
          }

          if (!brunchName) {
            throw new Error(
              "IF_13 brunch_name이 없습니다."
            );
          }

          const currentModelVersion =
            String(
              process.env.MODEL_VERSION ||
              ""
            ).trim();

          console.log(
            `[ModelEmbedding] current=` +
            `${currentModelVersion}, ` +
            `new=${modelVersion}`
          );

          console.log(
            `[ModelEmbedding] brunch=` +
            brunchName
          );

          if (
            currentModelVersion ===
            modelVersion
          ) {
            await publishRebootAck({
              deviceIdx,
              divisionIdx,
              ifSysId,
              rebootState: "REJECTED",
              resultCd: "F",
              resultMsg:
                "This Division Model Version is SAME",
            });

            rebooting = false;
            return;
          }

          /*
           * 상태가 정상이 아닐 때 업데이트를
           * 중단하려면 아래 코드를 활성화합니다.
           */
          // const healthOk =
          //   await isDeviceSensorsHealthy();

          // console.log(
          //   `[ModelEmbedding] health result: ` +
          //   healthOk
          // );

          // if (!healthOk) {
          //   throw new Error(
          //     "Product collection health check failed"
          //   );
          // }

          const trainedModelFolders =
            await listTrainedModelFolders();

          if (
            !trainedModelFolders
              .includes(brunchName)
          ) {
            throw new Error(
              `MinIO trained_model/` +
              `${brunchName}/ 폴더가 없습니다.`
            );
          }

          await cleanupCachesBeforeModelDownload();

          const downloadedFiles =
            await downloadModelFilesFromBrunchFolder(
              brunchName,
              modelVersion
            );

          console.log(
            "[ModelEmbedding] 다운로드 완료:",
            downloadedFiles
          );

          await writeEngineBuildTxt(
            modelVersion
          );

          await startCrkModelBuildServiceWithLogs();

          const enginePath =
            path.join(
              LOCAL_MODEL_DIR,
              `${modelVersion}.engine`
            );

          const engineStat =
            await fs.stat(enginePath)
              .catch(() => null);

          if (
            !engineStat ||
            engineStat.size <= 0
          ) {
            throw new Error(
              `Engine file verification failed: ` +
              enginePath
            );
          }

          console.log(
            `[ModelEmbedding] Engine 확인 완료: ` +
            `${enginePath}, ` +
            `size=${engineStat.size}`
          );

          await deleteDownloadedModelFiles(
            downloadedFiles
          );

          await updateEnvModelVersion(
            modelVersion
          );

          console.log(
            "[ModelEmbedding] 모델 업데이트 완료"
          );
        }

        /**
         * 실제 재부팅 직전 결제 상태 재확인
         * ② 실제 reboot.flag / ACCEPTED 직전
         * → EMBEDDING 작업 도중 결제가 시작됐으면 거절
         */
        if (getProcessing()) {
          console.warn(
            "[REBOOT] Payment started during reboot preparation. Reboot rejected."
          );

          await publishRebootAck({
            deviceIdx,
            divisionIdx,
            ifSysId,
            rebootState: "REJECTED",
            resultCd: "F",
            resultMsg: "Payment is processing",
          });

          rebooting = false;
          return;
        }
        /**
         * MANUAL과 EMBEDDING 공통 처리
         */
        await fs.mkdir(
          path.dirname(REBOOT_FLAG),
          { recursive: true }
        );

        await fs.writeFile(
          REBOOT_FLAG,
          JSON.stringify(
            {
              requestedAt: Date.now(),
              ifSysId,
              rebootMode,
            },
            null,
            2
          ),
          "utf-8"
        );

        flagWritten = true;

        console.log(
          `[REBOOT] flag saved: ` +
          `mode=${rebootMode}`
        );

        await publishRebootAck({
          deviceIdx,
          divisionIdx,
          ifSysId,
          rebootState: "ACCEPTED",
          resultCd: "S",
          resultMsg: "reboot accepted",
        });

        await runReboot();
      } catch (error) {
        rebooting = false;

        console.error(
          "[REBOOT] handling failed:",
          error?.message || error
        );

        if (flagWritten) {
          await fs.unlink(REBOOT_FLAG)
            .catch(() => {});
        }

        await publishRebootAck({
          deviceIdx,
          divisionIdx,
          ifSysId,
          rebootState: "FAILED",
          resultCd: "F",
          resultMsg:
            error?.message ||
            String(error),
        }).catch((ackError) => {
          console.error(
            "[REBOOT] FAILED ACK error:",
            ackError?.message ||
            ackError
          );
        });
      }
    }
  );

  /**
   * MQTT 연결 후 처리
   */
  const onReady = async () => {
    console.log(
      "[MQTT] connected (reboot)"
    );

    /*
     * 먼저 reboot command 토픽을 구독합니다.
     * 플래그 후속 처리에 실패하더라도
     * 새로운 명령을 받을 수 있습니다.
     */
    try {
      await subscribeRebootTopic(
        client,
        rebootSub
      );
    } catch (subscribeError) {
      console.error(
        "[MQTT] reboot topic " +
        "subscribe failed:",
        subscribeError.message
      );

      return;
    }

    if (
      !(await fileExists(REBOOT_FLAG))
    ) {
      console.log(
        "[RebootMqtt] reboot.flag 없음"
      );

      return;
    }

    let rebootFlag;

    try {
      const raw =
        await fs.readFile(
          REBOOT_FLAG,
          "utf-8"
        );

      rebootFlag = JSON.parse(raw);
    } catch (flagError) {
      console.error(
        "[RebootMqtt] reboot.flag " +
        "읽기 실패:",
        flagError.message
      );

      await fs.unlink(REBOOT_FLAG)
        .catch(() => {});

      return;
    }

    const rebootMode =
      String(
        rebootFlag?.rebootMode ||
        "MANUAL"
      )
        .trim()
        .toUpperCase();

    console.log(
      `[RebootMqtt] 재부팅 후 처리: ` +
      `mode=${rebootMode}`
    );

    /**
     * 일반 재부팅
     */
    if (rebootMode !== "EMBEDDING") {
      try {
        await publishRebootAckOnce(
          deviceIdx,
          divisionIdx
        );

        console.log(
          "[RebootMqtt] 일반 재부팅 " +
          "완료 처리 성공"
        );
      } catch (error) {
        console.error(
          "[RebootMqtt] 일반 재부팅 " +
          "완료 처리 실패:",
          error.message
        );
      }

      return;
    }

    /**
     * EMBEDDING 재부팅
     */
    console.log(
      "[RebootMqtt] EMBEDDING 재부팅 감지"
    );

    const systemHealthy =
      await runStartupDiagnostics(
        client,
        deviceIdx
      );

    if (!systemHealthy) {
      console.warn(
        "[RebootMqtt] 자가 진단 실패. " +
        "IF07을 전송하지 않습니다."
      );

      /*
       * 플래그를 유지하므로 프로세스가 다시
       * 시작되면 후속 처리를 재시도할 수 있습니다.
       */
      return;
    }

    try {
      const results =
        await notifyDeployCompleteForAllProducts(
          divisionIdx,
          deviceIdx
        );

      const hasFailure =
        results.some((item) => {
          const resultCd =
            item.result?.result_cd ??
            item.result?.DATA?.result_cd ??
            item.result?.body?.result_cd;

          return resultCd !== "S";
        });

      if (hasFailure) {
        console.error(
          "[IF07] 일부 상품 배포 완료 " +
          "전송 실패"
        );

        return;
      }

      console.log(
        "[IF07] training_status=1 " +
        "전송 성공"
      );

      await publishRebootAckOnce(
        deviceIdx,
        divisionIdx
      );

      console.log(
        "[RebootMqtt] EMBEDDING 재부팅 " +
        "후속 처리 완료"
      );
    } catch (error) {
      console.error(
        "[IF07] 배포 완료 처리 실패:",
        error.message
      );
    }
  };

  if (client.connected) {
    void onReady();
  } else {
    client.once(
      "connect",
      () => void onReady()
    );
  }
}

module.exports = {
  RebootMqtt,
  REBOOT_FLAG,
};