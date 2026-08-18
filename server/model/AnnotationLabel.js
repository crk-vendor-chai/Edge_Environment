// ============================================================
// AnnotationLabel.js
// 역할: annotation 도구(CVAT 등)에서 사용하는 label 정의 Mongoose 모델.
//       상품별 label의 id(trainProductIdx 기반)/name(productEngName)/color를
//       저장하며, id=0은 "hand" label로 예약된다. (name, id) 복합 unique index.
//       toJSON/toObject 변환 시 _id를 제거해 API 응답을 깔끔하게 만든다.
// 컬렉션명: AnnotationLabel / 사용처: AnnotationLabelSyncService,
//       server/test/mongoDBAnnotationUpload.js
// ============================================================
const mongoose = require('mongoose');
const Schema = mongoose.Schema;

// label 스키마: name(label 이름), id(숫자 label id), color(HEX),
// type(기본 "any"), attributes(기본 빈 배열)
const annotationLabelSchema = mongoose.Schema({
    name: {
        type: String,
    },
    id: {
        type: Number,
        unique: true,
    },
    color: {
        type: String
    },
    type: {
        type: String,
        default: "any"
    },
    attributes: {
        type: Object,
        default: []
    }
    }, { versionKey: false }
)

annotationLabelSchema.index({ name: 1, id: 1 }, { unique: true });

// ✅ API 응답에서 _id 제거 (doc.toJSON(), res.json(doc) 등에 적용)
function stripMongoId(doc, ret) {
  delete ret._id;
  return ret;
}

annotationLabelSchema.set("toJSON", { transform: stripMongoId });
annotationLabelSchema.set("toObject", { transform: stripMongoId });

// const ProductUpload = mongoose.model('ProductUpload', productUploadSchema);
const AnnotationLabel = mongoose.model('AnnotationLabel', annotationLabelSchema, 'AnnotationLabel');

module.exports = { AnnotationLabel }