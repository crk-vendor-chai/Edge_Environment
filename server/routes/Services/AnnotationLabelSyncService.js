// ============================================================
// AnnotationLabelSyncService.js
// 역할: ProductsList(상품 메타)를 기준으로 annotation label 컬렉션
//       (AnnotationLabel)을 동기화하는 서비스. id=0 "hand" label을 고정
//       색상(#F08CB3)으로 예약 upsert하고, 각 상품의 trainProductIdx/
//       productEngName을 label(id/name)로 upsert하며 신규 label에는 중복
//       없는 랜덤 HEX color를 배정한다. deleteMissing 시 잔여 label 정리.
// 사용처: routes/Mqtt/AckCollect.js (collect ack 흐름에서 호출)
// ============================================================
require("dotenv").config();
const { AnnotationLabel } = require("../../model/AnnotationLabel");

// hand label(id=0) 전용으로 예약된 고정 색상. 일반 label 색상 생성 시 제외된다.
const RESERVED_HAND_COLOR = "#F08CB3";

// 대문자 6자리 랜덤 HEX color 문자열("#RRGGBB")을 생성한다.
function randomHexColor() {
  const n = Math.floor(Math.random() * 0xffffff);
  return `#${n.toString(16).padStart(6, "0").toUpperCase()}`;
}

// usedColors에 없고 RESERVED_HAND_COLOR와도 다른 색상을 뽑을 때까지
// 최대 10000회 재시도. 성공 시 usedColors에 등록 후 반환, 실패 시 throw.
function uniqueColor(usedColors) {
  let c = randomHexColor();
  let guard = 0;

  while (
    (usedColors.has(c) || c === RESERVED_HAND_COLOR) &&
    guard < 10000
  ) {
    c = randomHexColor();
    guard += 1;
  }

  if (guard >= 10000) {
    throw new Error("Failed to generate unique HEX color");
  }

  usedColors.add(c);
  return c;
}

// productModel(ProductUpload 등)의 전체 상품을 annotation label로 동기화한다.
// 1) id=0 "hand" label 고정 upsert  2) 상품별 trainProductIdx(id)/productEngName(name) upsert
//    (신규 insert 시에만 uniqueColor로 색상 배정)  3) deleteMissing=true면
//    desiredIds에 없는 label deleteMany. 결과 통계 { totalProducts, desiredLabels,
//    upserted, skipped, deleted }를 반환한다.
async function syncAnnotationLabels({
  productModel,
  deleteMissing = false,
} = {}) {
  if (!productModel) {
    throw new Error("productModel is required");
  }

  // hand label 고정 생성
  await AnnotationLabel.updateOne(
    { id: 0 },
    {
      $set: {
        name: "hand",
      },
      $setOnInsert: {
        color: RESERVED_HAND_COLOR,
        attributes: [],
        type: "any",
      },
    },
    { upsert: true }
  );

  const products = await productModel
    .find({}, { trainProductIdx: 1, productEngName: 1 })
    .lean();

  const existing = await AnnotationLabel.find(
    {},
    { id: 1, color: 1 }
  ).lean();

  const usedColors = new Set(
    existing.map((x) => x.color).filter(Boolean)
  );

  // hand color 예약
  usedColors.add(RESERVED_HAND_COLOR);

  const existingById = new Map(
    existing.map((x) => [Number(x.id), x])
  );

  const desiredIds = new Set([0]);

  let upserted = 0;
  let skipped = 0;

  for (const p of products) {
    const id = Number(p.trainProductIdx);
    const name = p.productEngName
      ? String(p.productEngName).trim()
      : "";

    if (!Number.isFinite(id) || !name) {
      skipped += 1;
      continue;
    }

    desiredIds.add(id);

    const existed = existingById.get(id);

    const setOnInsert = existed
      ? {}
      : {
          color: uniqueColor(usedColors),
          attributes: [],
          type: "any",
        };

    await AnnotationLabel.updateOne(
      { id },
      {
        $set: { name },
        ...(Object.keys(setOnInsert).length
          ? { $setOnInsert: setOnInsert }
          : {}),
      },
      { upsert: true }
    );

    upserted += 1;
  }

  let deleted = 0;

  if (deleteMissing) {
    const delRes = await AnnotationLabel.deleteMany({
      id: { $nin: Array.from(desiredIds) },
    });

    deleted = delRes?.deletedCount ?? 0;
  }

  return {
    totalProducts: products.length,
    desiredLabels: desiredIds.size,
    upserted,
    skipped,
    deleted,
  };
}

module.exports = {
  syncAnnotationLabels,
};
