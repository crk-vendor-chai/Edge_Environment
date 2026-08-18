// ============================================================
// PaymentStore.js
// 역할: 클라우드(PNT) REST API IF_08(결제 정보 저장) 전송 모듈.
//  - 결제 승인 결과(paymentResponse), model 추론 결과(inferenceResult),
//    camera 촬영 영상(mp4)을 FormData 로 묶어 /chai/payment/store 에 업로드한다.
//  - CardMethod 에 따라 payload 분기: 'R'=RFID, 'S'=삼성페이, 그 외=일반 카드.
//  - 상품 단가는 IF_11 상품 마스터(productData)에서 조회하며,
//    인증은 config.jwtToken(Bearer) 사용.
// ============================================================
const FormData = require('form-data');
const config = require("../../config/dev");
const { v4: uuidv4 } = require("uuid");
const fs = require("fs");
const path = require("path");
const axios = require("axios");

// const dummyImg = path.join(__dirname, "../../log/dummyTestImg.png");

// IF 규격(YYYYMMDDHHMMSS)의 날짜 문자열 생성
function formatIfDate(d = new Date()) {
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}`
         + `${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

// IF 규격(YYYYMMDDHHMMSS)의 날짜 문자열 생성 (formatIfDate 와 동일 기능)
function makeIFDate(d = new Date()) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const HH = String(d.getHours()).padStart(2, "0");
  const MM = String(d.getMinutes()).padStart(2, "0");
  const SS = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}${HH}${MM}${SS}`;
}

// [IF_08] 결제 결과 + 결제 영상(mp4)을 PNT 클라우드로 전송한다.
// snapshot 폴더(folderPath/archival/cam_0)의 mp4 를 찾아 payment_file 로 첨부하며,
// 전송 성공 시 true, 실패 시 false 를 반환한다.
async function sendToPNT(paymentResponse, inferenceResult, folderPath, paymentAt, CardMethod, productData, token) {
    console.log("[PNT] Preparing IF_08 data transfer...");
    console.log('paymentResponse', paymentResponse)
    // console.log("[PNT] productData:", productData);
    console.log("[PNT] inferenceResult.products:", inferenceResult?.products);
    try {
        // 추후 카메라가 촬영한 영상으로 전송
        // const camFolderPath = path.join(folderPath, "archival", "cam_0");
        // const files = fs.readdirSync(camFolderPath);
        // const mp4 = files.find(f => f.toLowerCase().endsWith(".mp4"));
        // if (!mp4) {
        //     throw new Error("No mp4 file found in cam_0 folder");
        // }
        // const fullPath = path.join(camFolderPath, mp4);
        // const fileName = path.basename(fullPath);
        // const stat = fs.statSync(fullPath);

        // 영상
        const camFolderPath = path.join(folderPath, "archival", "cam_0");
        const files = fs.readdirSync(camFolderPath);
        const mp4 = files.find(f => f.toLowerCase().endsWith(".mp4"));

        if (!mp4) {
        throw new Error("No mp4 file found in cam_0 folder");
        }

        const fullPath = path.join(camFolderPath, mp4);
        const fileName = path.basename(fullPath);
        const stat = fs.statSync(fullPath);


        // 더미 이미지로 전송
        // if (!fs.existsSync(dummyImg)) {
        //     throw new Error(`[PNT] Dummy image not found: ${dummyImg}`);
        // }

        // const fileName = path.basename(dummyImg);
        // const stat = fs.statSync(dummyImg);

        const hasLowConfidence = inferenceResult.products.some(
            p => Number(p.confidence) < 0.2
        );

        const productMap = new Map(
            productData.map(p => [String(p.product_idx), p])
        );
        
        // if (fs.existsSync(camFolderPath)) {
        //     const files = fs.readdirSync(camFolderPath);
        //     const imageFiles = files.filter(file => /\.(jpg|jpeg|png)$/i.test(file)).slice(0, 2);

        //     paymentImgList = imageFiles.map(file => {
        //         const filePath = path.join(camFolderPath, file);
        //         const stats = fs.statSync(filePath);
                
        //         // 파일 스트림 첨부
        //         formData.append('files', fs.createReadStream(filePath), { filename: file });

        //         return {
        //             file_name: file,
        //             file_ext: path.extname(file).replace('.', ''),
        //             file_size: stats.size
        //         };
        //     });
        // } else {
        //     console.warn(`[PNT] Warning: Image folder not found at ${camFolderPath}`);
        // }


        // 결제 데이터 전달
        const external = axios.create({
          baseURL: config.restApi, // https://apichaidev.atcrk.co.kr/api/v1
          timeout: 10000,
        //   headers: { "Content-Type": "multipart/form-data" },
        });

        const now = new Date();
        const formattedDate = makeIFDate(now)
        // RFID 경로 approve_at: 카드/삼성 경로(단말 authorization_date)와 동일한
        // 12자리 YYMMDDHHmmss 포맷 (기존엔 getDate() 뒤 세미콜론 오타로 시분초가
        // 버려져 YYMMDD 6자리만 저장되던 버그 — PAYMENT-ISSUES.md §2-2)
        const rfidTime =
        String(now.getFullYear()).slice(2) +
        String(now.getMonth() + 1).padStart(2, "0") +
        String(now.getDate()).padStart(2, "0") +
        String(now.getHours()).padStart(2, "0") +
        String(now.getMinutes()).padStart(2, "0") +
        String(now.getSeconds()).padStart(2, "0");
        console.log(rfidTime);

        let payload = {}

        if (CardMethod === 'R') {
            payload = {
                HEADER: {
                    IF_ID: "IF_08",
                    IF_SYSID: uuidv4(),
                    IF_HOST: "CRKPNTCHAI", // 엑셀에는 EDGE라고 적혀있음
                    // IF_DATE: timestamp,
                    IF_DATE: formatIfDate(),
                },
                DATA:{
                    device_idx: config.deviceIdx,
                    division_idx: config.divisionIdx,
                    token_id: token,
                    payment_at: formattedDate,
                    approve_at: rfidTime,
                    approve_type: CardMethod === 'R' ? '2' : (CardMethod === 'S' ? '1' : '0'), // 0=일반카드, 1=삼성페이, 2=RFID
                    approve_result: 1,
                    approve_price: inferenceResult.totalPrice,
                    approve_no: token,
                    approve_card_issuer: 'POINT',
                    approve_card_num: token,
                    approve_card_json: JSON.stringify(paymentResponse),
                    provider: "chai",
                    state: inferenceResult.status === 'success' ? (hasLowConfidence ? '1' : '0') : '1',
                    product_list: inferenceResult.products.map(p => {
                        const master = productMap.get(String(p.productIdx));

                        if (!master) {
                            console.warn(
                                `[PNT] Product master not found in IF_11: productId=${p.productIdx}. ` +
                                `Falling back to unitPrice for sale_price, 0 for supply_price.`
                            );
                        }

                        return {
                            product_idx: String(p.productIdx),
                            product_count: Number(p.count),
                            supply_price: Number(master.supply_price),
                            sale_price: Number(master.sale_price),
                        };
                    }),
                    // product_idx: inferenceResult.products.map(p => p.productId).join(","),
                    // product_count: inferenceResult.products.map(p => p.count).join(","),
                    // 이미지
                    // payment_file_list: [{
                    //     file_name: fileName,
                    //     file_ext: 'png',
                    //     file_size: stat.size,
                    // }],
                    // 영상
                    payment_file_list: [{
                        file_name: fileName,
                        file_ext: 'mp4',
                        file_size: stat.size,
                    }],
                }
            }
        } else {
            payload = {
                HEADER: {
                    IF_ID: "IF_08",
                    IF_SYSID: uuidv4(),
                    IF_HOST: "CRKPNTCHAI", // 엑셀에는 EDGE라고 적혀있음
                    // IF_DATE: timestamp,
                    IF_DATE: formatIfDate(),
                },
                DATA:{
                    device_idx: config.deviceIdx,
                    division_idx: config.divisionIdx,
                    token_id: CardMethod === 'N' ? token : paymentResponse.vankey,
                    // token_id: token || paymentResponse.vankey_hash || paymentResponse.vankey,
                    payment_at: formattedDate, // 픽앤탁으로 전송하는 시간
                    approve_at: paymentResponse.authorization_date, // 카드결제가 이루어진 시간
                    approve_type: CardMethod === 'R' ? '2' : (CardMethod === 'S' ? '1' : '0'), // 0=일반카드, 1=삼성페이, 2=RFID
                    // 단말 서버 응답은 {status:"Y"/"N", ...} 객체 — 최상위 status로 판정
                    // (기존엔 객체 === "Y" 비교라 항상 1(실패)로 기록되던 버그 — PAYMENT-ISSUES.md §2-1)
                    approve_result: (paymentResponse.status === "Y") ? 0 : 1,
                    approve_price: inferenceResult.totalPrice,
                    approve_no: paymentResponse.authorization_number,
                    approve_card_issuer: paymentResponse.card_info.ISSUER_NAME,
                    approve_card_num: paymentResponse.card_info.SERIAL_NUMBER,
                    approve_card_json: JSON.stringify(paymentResponse),
                    provider: "chai",
                    state: inferenceResult.status === 'success' ? (hasLowConfidence ? '1' : '0') : '1',
                    product_list: inferenceResult.products.map(p => {
                        const master = productMap.get(String(p.productIdx));

                        if (!master) {
                            console.warn(
                                `[PNT] Product master not found in IF_11: productId=${p.productIdx}. ` +
                                `Falling back to unitPrice for sale_price, 0 for supply_price.`
                            );
                        }

                        return {
                            product_idx: String(p.productIdx),
                            product_count: Number(p.count),
                            supply_price: Number(master.supply_price),
                            sale_price: Number(master.sale_price),
                        };
                    }),
                    // 이미지
                    // payment_file_list: [{
                    //     file_name: fileName,
                    //     file_ext: 'png',
                    //     file_size: stat.size,
                    // }],
                    // 영상
                    payment_file_list: [{
                        file_name: fileName,
                        file_ext: 'mp4',
                        file_size: stat.size,
                    }],
                }
            }
        }
        // 3) FormData 구성 (payload + paymentFile)
        const form = new FormData();
        form.append("payload", JSON.stringify(payload), {
           contentType: "application/json"  // 추가
        });
        // 이미지
        // form.append("payment_file", fs.createReadStream(dummyImg), {
        //     filename: fileName,
        //     contentType: "image/png",
        // });
        // 영상
        form.append("payment_file", fs.createReadStream(fullPath), {
            filename: fileName,
            contentType: "video/mp4",
        });


        const jwtToken = config.jwtToken
        console.log('[EDGE->PNT] REQUEST PAYLOAD', payload)
        const response = await external.post("/chai/payment/store", form, {
            headers: {
                ...form.getHeaders(),
                Authorization: `Bearer ${jwtToken}`,
            }, 
            timeout: 30000,
            maxBodyLength: Infinity,
            maxContentLength: Infinity,
        });
        if (response.status === 200) {
            console.log("[PNT] Transfer Success:", response.data);
            return true;
        } else {
            console.error(`[PNT] Transfer Failed (Status: ${response.status})`);
            return false;
        }

    } catch (error) {
        console.error("[PNT] Error:", error.message);
        if (error.response) {
            console.error("[PNT] Server Response:", error.response.data);
        }
        return false;
    }
}

module.exports = { sendToPNT };