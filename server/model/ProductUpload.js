// ============================================================
// ProductUpload.js
// 역할: PNT에 등록된 상품 메타를 담는 Mongoose 모델. productIdx(FK),
//       productEngName, storageType(COLD/FROZEN), productLoadcellWeight,
//       trainingStatus, snapshot 폴더 정보(foldername/folderpath/filelength),
//       학습용 순번 trainProductIdx(1부터 증가, unique index) 등을 관리한다.
//       (productIdx, productEngName) 복합 unique index로 문서를 식별한다.
// 컬렉션명: ProductsList / 사용처: AckCollect.js, AIServer/Products.js,
//       ProductMongoSyncService, server/test의 mongodb* 스크립트
// ============================================================
const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const productUploadSchema = mongoose.Schema({
    //mongoDB 고유 id값
    productMetaIdx: mongoose.Schema.Types.ObjectId,
    //PNT에 등록된 상품 고유번호 (FK)
    productIdx: {
        type: String,
    },
    // //PNT에 등록된 상품 이름(한글)
    // productName: {
    //     type: String,
    // },
    //PNT에 등록된 상품 카테고리
    categoryIdx: {
        type: String,
        default: 'null'
    },
    //해당 상품이 신규인지 기존에 갖던 정보(이미 학습 완료된)인지 여부
    isNew: {
        type: String,
    },
    //학습 상태 여부
    /**
        미학습: 0
        배포 완료: 1
        데이터 수집 완료(이미 AI 서버 내에 상품 데이터셋 존재): 2
        어노테이션 프로세스 진행 완료 : 3
        세그먼트 추출 완료 : 4
        모델 내 학습 완료: 5
        모델 내 검증/테스트 진행 완료: 6
        모델 학습/검증 완료 후 CI/CD 배포 완료: 7
        모델 검증 실패, 재학습(데이터셋 재수집) 필요: 8
    */
   // 제거
    trainingStatus: {
        type: String,
    },
    //상품 영문명
    productEngName: {
        type: String
    },
    //상품 로드셀 무게 정보
    productLoadcellWeight: {
        type: String,
        default: 'null'
    },
    storageType: {
        type: String
    },
    //이미지 스냅샷 폴더명
    foldername: {
        type: String
    },
    //이미지 스냅샷 폴더 경로 (MinIO)
    folderpath: {
        type: String
    },
    //이미지 스냅샷 전체 장 수
    filelength: {
        type: Number
    },
    //상품 등록일(이미지가 DB에 저장된 날)
    createDate: {
        type: Date,
    },
    updateDate: {
        type: Date,
        default: null
    },
    //상품은 동일하나 이벤트(예: 크리스마스, 뺴빼로데이)로 인해 포장지가 달라져 재학습이 필요한 경우
    eventPromotion: {
        type: Array,
        default: []
    },
    trainProductIdx: {
        type: Number, //1부터 순차적으로 시작
    },
    }, { versionKey: false, timestamps: false }
)

productUploadSchema.index({ productIdx: 1, productEngName: 1 }, { unique: true });
productUploadSchema.index({ trainProductIdx: 1 }, { unique: true });

// const ProductUpload = mongoose.model('ProductUpload', productUploadSchema);
const ProductUpload = mongoose.model('ProductUpload', productUploadSchema, 'ProductsList');

module.exports = { ProductUpload }