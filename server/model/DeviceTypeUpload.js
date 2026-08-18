// ============================================================
// DeviceTypeUpload.js
// 역할: 매장(division) x 저장 타입(COLD/FROZEN) 단위의 장비 그룹 메타를 담는
//       Mongoose 모델. brunchName(모델 배포 브런치, unique index), deviceIdx 배열,
//       modelVersion, 상품별 training_status 매핑(products), 학습 상태/일자를 관리.
// 컬렉션명: DeviceTypeList / 사용처: AckCollect.js, ProductMongoSyncService,
//       server/test/mongodbDataUpload.js
// ============================================================
const mongoose = require('mongoose');
const Schema = mongoose.Schema;

// products 배열의 서브 스키마: ProductUpload 문서 참조(ref)와
// 해당 상품의 training_status(기본 "2")를 함께 보관한다.
const deviceTypeProductSchema = new Schema(
  {
    product: { type: Schema.Types.ObjectId, ref: "ProductUpload", required: true },
    training_status: { type: String, default: "2" },    
  },
  { _id: false }
);

const DeviceTypeUploadSchema = mongoose.Schema({
    //mongoDB 고유 id값
    deviceMetaIdx: mongoose.Schema.Types.ObjectId,
    storageType: {
        type: String
    },
    //PNT에 등록된 매장 고유번호 (FK)
    divisionIdx: {
        type: String,
    },
    //매장 별 모델 배포 브런치 정보(CI/CD)
    brunchName: {
        type: String
    },
    deviceIdx: {
        type: [String]
    },
    //배포 모델 버전 (매장별)
    modelVersion: {
        type: String
    },
    //PNT에 등록된 매장별 신규 상품 리스트
    // 해당 productIdx에 대해서 object 하나 더 넣어서 이거에 대한 status 값을 따로 관리할 수 있도록 { training_status }
    products: {
        type: [deviceTypeProductSchema], 
        default: [] 
    },
    //모델 학습 상태 정보 (장비별)
    //학습 상태 여부
    /**
        미학습: 0
        임베딩 완료: 1
        데이터 수집 완료(이미 AI 서버 내에 상품 데이터셋 존재): 2
        어노테이션 프로세스 진행 완료 : 3
        세그먼트 추출 완료 : 4
        매장별 이전 모델 내 학습 완료: 5
        매장별 이전 모델 내 검증/테스트 진행 완료: 6
        모델 학습/검증 완료 후 CI/CD 배포 완료: 7
        모델 검증 실패, 재학습(데이터셋 재수집) 필요: 8
    */
    trainingStatus: {
        type: String
    },
    //학습 시작일
    trainingDate: {
        type: Date,
    },
    //재학습 시작일
    retrainingDate: {
        type: Date,
        default: null
    },
    }, { versionKey: false }
)

DeviceTypeUploadSchema.index({ brunchName: 1 }, { unique: true });

// const ProductUpload = mongoose.model('ProductUpload', productUploadSchema);
const DeviceTypeUpload = mongoose.model('DeviceTypeUpload', DeviceTypeUploadSchema, 'DeviceTypeList');

module.exports = { DeviceTypeUpload }