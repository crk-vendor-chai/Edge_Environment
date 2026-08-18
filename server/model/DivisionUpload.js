// ============================================================
// DivisionUpload.js
// 역할: 매장(division) 단위 메타를 담는 Mongoose 모델. divisionIdx(unique index)와
//       소속 deviceIdx 배열, modelVersion, brunchName, 상품 참조(products),
//       학습 상태(trainingStatus)/학습일(trainingDate)/재학습일(retrainingDate)을 관리.
// 컬렉션명: DivisionList / 사용처: AckCollect.js, ProductMongoSyncService,
//       server/test/mongodbDataUpload.js, mongodbDivisionUpload.js
// ============================================================
const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const divisionUploadSchema = mongoose.Schema({
    //mongoDB 고유 id값
    divisionMetaIdx: mongoose.Schema.Types.ObjectId,
    //PNT에 등록된 매장 고유번호 (FK)
    divisionIdx: {
        type: String,
    },
    //매장에 소속된 장비 코드 목록
    deviceIdx: {
        type: [String]
    },
}, { versionKey: false }
)

divisionUploadSchema.index({ divisionIdx: 1 }, { unique: true });

const DivisionUpload = mongoose.model('DivisionUpload', divisionUploadSchema, 'DivisionList');

module.exports = { DivisionUpload }
