// ============================================================
// Products.js (AIServer)
// 역할: model 학습용 상품 이미지 업로드 / 메타데이터 저장 라우터.
//  - POST /uploads/images: multer 로 수신한 이미지들을 MinIO(기본 bucket:
//    chaiimage)의 productImg/{productIdx}_{rootName}/ 경로에 업로드한다.
//  - POST /products: 업로드된 폴더 정보와 상품 메타데이터(loadcell weight,
//    training 상태 등)를 MongoDB(ProductUpload 컬렉션)에 upsert 한다.
// ============================================================
const express = require("express");
const router = express.Router();
const multer = require("multer");
const { ProductUpload } = require("../../model/ProductUpload");
const crypto = require("crypto");
const path = require("path");

// 파일 buffer 의 sha1 해시 계산 (object key 중복 방지용 suffix 로 사용)
const sha1 = (buffer) => crypto.createHash("sha1").update(buffer).digest("hex");

//=================================
//        Product Upload
//=================================

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    files: 2000,
    fileSize: 100 * 1024 * 1024,
  },
}).array("files", 2000);

// 파일/폴더명에 사용할 수 없는 문자를 '_' 로 치환
function safe(s) {
  return String(s || "").replace(/[^a-zA-Z0-9._-]/g, "_");
}

// MinIO putObject(callback 방식)를 Promise 로 감싼 helper
function putObjectAsync(minioClient, bucket, key, buffer, meta) {
  return new Promise((resolve, reject) => {
    minioClient.putObject(bucket, key, buffer, meta, (err, etag) => {
      if (err) return reject(err);
      resolve(etag);
    });
  });
}

//=================================
//       MinIO Image Upload
//=================================
// 상품 이미지 다건 업로드: relPaths 기준 경로를 유지하며 MinIO 에 저장하고
// 업로드된 object key / etag / size 목록을 반환한다.
router.post("/uploads/images", (req, res) => {
  upload(req, res, async (err) => {
    try {
      if (err) return res.status(400).json({ success: false, err: String(err) });

      if (!req.files || req.files.length === 0) {
        return res.status(400).json({ success: false, err: "No files uploaded" });
      }

      const minioClient = req.app.locals.minioClient;
      const BUCKET = req.app.locals.minioBucket || "chaiimage";

      if (!minioClient) {
        return res.status(500).json({
          success: false,
          err: "minioClient not initialized in index.js",
        });
      }

      const productIdx = req.body.productIdx;
      const divisionIdx = req.body.divisionIdx;

      if (!productIdx || !divisionIdx) {
        return res.status(400).json({
          success: false,
          err: "productIdx and divisionIdx are required",
        });
      }

      const rootName = req.body.rootName;
      if (!rootName) {
        return res.status(400).json({
          success: false,
          err: "rootName is required (e.g., 20260122_170612)",
        });
      }

      const relPaths = req.body.relPaths;
      const relPathArr = Array.isArray(relPaths)
        ? relPaths
        : typeof relPaths === "string"
          ? [relPaths]
          : [];

      if (relPathArr.length !== req.files.length) {
        return res.status(400).json({
          success: false,
          err: "relPaths is required and must match files count (e.g., images/cam_0/0001.jpg)",
        });
      }

      const foldername = `${safe(productIdx)}_${safe(rootName)}`;
      const basePrefix = `productImg/${foldername}`;
      const folderpath = `s3://${BUCKET}/${basePrefix}/`;

      const uploaded = await Promise.all(
        req.files.map(async (f, i) => {
          const p = String(relPathArr[i]).replace(/\\/g, "/");

          let rel = p.startsWith("images/") ? p.slice("images/".length) : p;

          rel = rel.replace(/^\/*/, "");
          rel = rel.replace(/\.\./g, "_");

          const ext = path.extname(rel) || path.extname(f.originalname || "") || ".jpg";
          const withoutExt = ext ? rel.slice(0, -ext.length) : rel;

          const key = `${basePrefix}/${withoutExt}_${sha1(f.buffer)}${ext.toLowerCase()}`;
          const meta = { "Content-Type": f.mimetype || "application/octet-stream" };

          const etag = await putObjectAsync(minioClient, BUCKET, key, f.buffer, meta);

          return { key, etag, size: f.size, mimeType: f.mimetype };
        })
      );

      return res.json({
        success: true,
        bucket: BUCKET,
        foldername,
        folderpath,
        filelength: uploaded.length,
        objects: uploaded.map((x) => ({
          key: x.key,
          etag: x.etag,
          size: x.size,
        })),
      });
    } catch (e) {
      return res.status(500).json({
        success: false,
        err: e?.message || String(e),
      });
    }
  });
});

//=================================
//      MongoDB Meta Save / Upsert
//=================================
// 상품 메타데이터 upsert: productIdx(+productEngName) 기준으로 MongoDB 에
// 저장하며, 신규 문서에는 createDate / eventPromotion 을 함께 세팅한다.
router.post("/products", async (req, res) => {
  try {
    const { productIdx, divisionIdx, productEngName, foldername, folderpath } = req.body;

    if (!productIdx || !divisionIdx) {
      return res.status(400).json({
        success: false,
        err: "productIdx and divisionIdx are required",
      });
    }

    if (!foldername || !folderpath) {
      return res.status(400).json({
        success: false,
        err: "foldername and folderpath are required",
      });
    }

    const filter = productEngName
      ? { productIdx, productEngName }
      : { productIdx };

    const updateSet = {
      divisionIdx,
      modelVersion: req.body.modelVersion,
      brunchName: req.body.brunchName,
      productName: req.body.productName,
      categoryIdx: req.body.categoryIdx,
      isNew: req.body.isNew,
      trainingStatus: req.body.trainingStatus,
      productEngName,
      productLoadcellWeight: req.body.productLoadcellWeight,
      productAnnotation: req.body.productAnnotation,
      foldername,
      folderpath,
      filelength: Number(req.body.filelength || 0),
      updateDate: new Date(),
    };

    Object.keys(updateSet).forEach((key) => {
      if (updateSet[key] === undefined) delete updateSet[key];
    });

    const result = await ProductUpload.updateOne(
      filter,
      {
        $set: updateSet,
        $setOnInsert: {
          productIdx,
          divisionIdx,
          createDate: new Date(),
          eventPromotion: Array.isArray(req.body.eventPromotion)
            ? req.body.eventPromotion
            : [],
        },
      },
      { upsert: true }
    );

    return res.json({
      success: true,
      matchedCount: result.matchedCount,
      modifiedCount: result.modifiedCount,
      upsertedId: result.upsertedId || null,
    });
  } catch (e) {
    return res.status(500).json({
      success: false,
      err: e?.message || String(e),
    });
  }
});

module.exports = router;
