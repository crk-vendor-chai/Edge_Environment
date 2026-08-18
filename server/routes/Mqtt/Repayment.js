// ============================================================
// Repayment.js
// 역할: 클라우드에서 내려오는 payment 취소(CANCEL)/재결제(REPAY) 명령
//   (MQTT topic: chai/device/{deviceIdx}/cmd/payment)을 수신·처리하고,
//   결과 ack(IF_09)를 chai/device/{deviceIdx}/ack/payment 로 publish한다.
// 연동: card terminal API(/payment/token/approve, /payment/token/cancel,
//   /payment/samsung-pay/cancel), PaymentProcessing(카드 단말기 사용 중 여부 확인).
// 참고: REPAY는 신규 금액 승인 후 기존 결제를 vankey로 취소하는
//   "승인 후 취소" 방식이며 신용카드(CARD)만 지원한다.
// ============================================================
const axios = require("axios");
const config = require("../../config/key");
const { getClient } = require("./MqttClient");
const { getProcessing } = require("../RestAPI/PaymentProcessing");
const { v4: uuidv4 } = require("uuid");

// 토큰 prefix로 결제 타입 판단
// function getCardMethod(tokenId = "") {
//   if (tokenId.startsWith("SPAYKEY")) return "S"; // Samsung Pay
//   if (tokenId.startsWith("VANKEY")) return "N";  // Credit Card
// }

// IF_DATE 형식(yyyyMMddHHmmss)의 timestamp 문자열 생성
function formatIfDate(d = new Date()) {
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}`
         + `${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

// 서버 시작 시 1회만 호출해서 구독/처리 루프를 올리는 방식 추천
// payment 명령 처리 진입점: CANCEL(결제 취소)과 REPAY(재결제) 분기 처리
// 카드 단말기 사용 중이면 즉시 실패 ack 응답
function Repayment() {
  const deviceIdx = config.deviceIdx;
  const repaymentSub = `chai/device/${deviceIdx}/cmd/payment`;
  const repaymentPub = `chai/device/${deviceIdx}/ack/payment`;

  const client = getClient();

  client.subscribe(repaymentSub, { qos: 1 }, (err, granted) => {
    if (err) {
      console.error("[REPAY] Subscribe Error:", err.message);
      return;
    }

    console.log("[REPAY] Subscribe granted:", granted);
    // console.log(`[DoorCollect] Subscribed: ${subTopic}`);
  });

  // const client = getClient();

  // const subscribeRepayment = () => {
  //   console.log("[REPAY] subscribing:", repaymentSub);

  //   client.subscribe(repaymentSub, { qos: 1 }, (err, granted) => {
  //     if (err) {
  //       console.error("[REPAY] Subscribe Error:", err.message);
  //       return;
  //     }

  //     console.log("[REPAY] subscribed:", granted);
  //   });
  // };

  // if (client.connected) {
  //   subscribeRepayment();
  // } else {
  //   console.log("[REPAY] MQTT Connected");
  //   };


  client.on("message", async (topic, message) => {
    if (topic !== repaymentSub) return;

    let reqData = {};

    console.log("[REPAY] message received:", topic);
    const payload = JSON.parse(message.toString());
    console.log('[Request] reqData', payload.DATA)
    reqData = payload.DATA

    if (reqData.request_type == "CANCEL") {
      console.log("[CANCEL] response data:", payload);
      

      // 진행 중이면 취소 불가 ACK
      if (getProcessing()) {
        const ifSysId = (payload.HEADER && payload.HEADER.IF_SYSID) ? payload.HEADER.IF_SYSID : uuidv4();

        const ackPayload = JSON.stringify({
          HEADER: {
            IF_ID: "IF_09",
            IF_SYSID: ifSysId,
            IF_HOST: "CRKPNTCHAI",
            IF_DATE: formatIfDate(),
          },
          DATA: {
            device_idx: payload.device_idx,
            division_idx: payload.division_idx,
            payment_idx: payload.payment_idx,
            token_id: payload.token_id,
            payment_mode: payload.payment_mode,
            request_type: "CANCEL",
            result_cd: "F",
            result_msg: "[결제 취소] 현재 카드단말기가 이용중입니다. 잠시 후 다시 시도해주세요",
          },
        });

        client.publish(repaymentPub, ackPayload, { qos: 1, retain: false }, (e) => {
          if (e) console.error("[CANCEL] Publish Error:", e.message);
          else console.log("[CANCEL] Busy ACK sent");
        });
        return;
      }

      // token_id 로 카드 방식 결정 --> 불가능 다르지가 않음
      // const cardMethod = getCardMethod(reqData.token_id);

      let cancelEndpoint = "";
      let cancelPayload = {};

      if (reqData.payment_mode === "SAMSUNG") {
        cancelEndpoint = `${config.cardTerminalApi}/payment/samsung-pay/cancel`;
        cancelPayload = {
          // amount: String(reqData.approve_price),
          amount: '5',
          original_authorization_date: reqData.approve_at.substring(0, 6),
          original_authorization_number: reqData.approve_no,
          vankey: reqData.token_id,
        };
      } else if (reqData.payment_mode === "CARD") {
        cancelEndpoint = `${config.cardTerminalApi}/payment/token/cancel`;
        cancelPayload = {
          // amount: String(reqData.approve_price),
          amount: '5',
          original_authorization_date: reqData.approve_at.substring(0, 6),
          original_authorization_number: reqData.approve_no,
          vankey_hash: reqData.token_id,
        };
      } else {
        console.error("[CANCEL] Unknown Payment Method:", reqData.token_id);

        // ✅ Unknown 케이스도 실패 ACK (선택이지만 운영상 권장)
        const ifSysId = (payload.HEADER && payload.HEADER.IF_SYSID) ? payload.HEADER.IF_SYSID : uuidv4();

        const ackPayload = JSON.stringify({
          HEADER: {
            IF_ID: "IF_09",
            IF_SYSID: ifSysId,
            IF_HOST: "CRKPNTCHAI",
            IF_DATE: formatIfDate(),
          },
          DATA: {
            device_idx: reqData.device_idx,
            division_idx: reqData.division_idx,
            payment_idx: reqData.payment_idx,
            token_id: reqData.token_id,
            payment_mode: reqData.payment_mode,
            request_type: "CANCEL",
            result_cd: "F",
            result_msg: "[결제 취소] Unknown Payment Method (token_id prefix)",
          },
        });

        client.publish(repaymentPub, ackPayload, { qos: 1, retain: false }, () => {});
        return;
      }

      console.log(`[CANCEL] Sending Cancel Request to ${cancelEndpoint}`, cancelPayload);

      // ✅ 여기부터 “2번 보완”: 실패/예외도 ACK를 보냄
      let response;
      try {
        response = await axios.post(cancelEndpoint, cancelPayload);
      } catch (err) {
        console.error("[CANCEL] Cancel API error:", err.message, err.response?.data);

        const ifSysId = (payload.HEADER && payload.HEADER.IF_SYSID) ? payload.HEADER.IF_SYSID : uuidv4();

        const ackPayload = JSON.stringify({
          HEADER: {
            IF_ID: "IF_09",
            IF_SYSID: ifSysId,
            IF_HOST: "CRKPNTCHAI",
            IF_DATE: formatIfDate(),
          },
          DATA: {
            device_idx: reqData.device_idx,
            division_idx: reqData.division_idx,
            payment_idx: reqData.payment_idx,
            token_id: reqData.token_id,
            payment_mode: reqData.payment_mode,
            request_type: "CANCEL",
            result_cd: "F",
            result_msg: `[결제 취소] 취소 요청 실패: ${err.message}`,
            // 필요하면 단말 응답도 붙여서 디버깅
            terminal_response: err.response?.data ?? null,
          },
        });

        client.publish(repaymentPub, ackPayload, { qos: 1 }, (e) => {
          if (e) console.error("[CANCEL] Publish Error:", e.message);
          else console.log("[CANCEL] Fail ACK sent (exception)");
        });
        return;
      }

      const ok = response.data.response_code == 0 && response.data.status === "Y";

      if (ok) {
        console.log("[CANCEL] Cancellation Successful:", response.data);

        const ifSysId = (payload.HEADER && payload.HEADER.IF_SYSID) ? payload.HEADER.IF_SYSID : uuidv4();

        const ackPayload = JSON.stringify({
          HEADER: {
            IF_ID: "IF_09",
            IF_SYSID: ifSysId,
            IF_HOST: "CRKPNTCHAI",
            IF_DATE: formatIfDate(),
          },
          DATA: {
            device_idx: reqData.device_idx,
            division_idx: reqData.division_idx,
            payment_idx: reqData.payment_idx,
            token_id: reqData.token_id,
            org_token_id: "null",
            request_type: "CANCEL",
            payment_at: formatIfDate(),
            approve_at: reqData.approve_at,
            payment_mode: reqData.payment_mode,
            approve_price: parseInt(0),
            approve_no: reqData.approve_no,
            org_approve_at: "null",
            org_approve_price: "null",
            org_approve_no: "null",
            result_cd: "S",
            result_msg: "취소가 완료되었습니다",
          },
        });

        client.publish(repaymentPub, ackPayload, { qos: 1, retain: false }, (e) => {
          if (e) console.error("[CANCEL] Publish Error:", e.message);
          else console.log("[CANCEL] Success ACK sent");
        });
      } else {
        console.error("[CANCEL] Cancellation Failed:", response.data);

        const ifSysId = (payload.HEADER && payload.HEADER.IF_SYSID) ? payload.HEADER.IF_SYSID : uuidv4();

        const ackPayload = JSON.stringify({
          HEADER: {
            IF_ID: "IF_09",
            IF_SYSID: ifSysId,
            IF_HOST: "CRKPNTCHAI",
            IF_DATE: formatIfDate(),
          },
          DATA: {
            device_idx: reqData.device_idx,
            division_idx: reqData.division_idx,
            payment_idx: reqData.payment_idx,
            token_id: reqData.token_id,
            payment_mode: reqData.payment_mode,
            request_type: "CANCEL",
            result_cd: "F",
            result_msg: "[결제 취소] 취소 실패(단말 응답 오류)",
            terminal_response: response.data ?? null,
          },
        });

        client.publish(repaymentPub, ackPayload, { qos: 1 }, (e) => {
          if (e) console.error("[CANCEL] Publish Error:", e.message);
          else console.log("[CANCEL] Fail ACK sent success (rejected)");
        });
      }
    } else if (reqData.request_type == "REPAY") {
      // 재결제 기능 진행 --> 신용카드 (삼성 페이 X)
      // 재결제 기능이 들어오면 -> 새로운 금액으로 결제를 진행하고 -> 이전 결제는 vankey로 다시 취소 처리 필요
      console.log("[REPAY] response data:", reqData);

      let ifSysId = payload.HEADER.IF_SYSID || uuidv4();

      // 진행 중이면 취소 불가 ACK
      if (getProcessing()) {

        const ackPayload = JSON.stringify({
          HEADER: {
            IF_ID: "IF_09",
            IF_SYSID: ifSysId,
            IF_HOST: "CRKPNTCHAI",
            IF_DATE: formatIfDate(),
          },
          DATA: {
            device_idx: reqData.device_idx,
            division_idx: reqData.division_idx,
            payment_idx: reqData.payment_idx,
            token_id: reqData.org_token_id,
            payment_mode: reqData.payment_mode,
            request_type: "REPAY",
            result_cd: "F",
            result_msg: "[재결제] 현재 카드단말기가 이용중입니다. 잠시 후 다시 시도해주세요",
          },
        });

        client.publish(repaymentPub, ackPayload, { qos: 1, retain: false }, (e) => {
          if (e) console.error("[REPAY] Publish Error:", e.message);
          else console.log("[REPAY] Busy ACK sent");
        });
        return;
      }

      // token_id 로 신용카드가 맞는지 확인
      // const cardMethod = getCardMethod(reqData.token_id);
      //승인 후 취소 방식 채택
      if (reqData.payment_mode === "CARD") {
        const oldToken = reqData.org_token_id
        const oldApproveAt = reqData.approve_at.substring(0, 6);
        const oldApprovePrice = reqData.org_approve_price
        const oldApproveNo = reqData.org_approve_no

        const newApprovePrice = reqData.approve_price;

        try {
          const approveRes = await axios.post(
            `${config.cardTerminalApi}/payment/token/approve`,
            {
              // amount: String(newApprovePrice),
              amount: '5',
              // items: reqData.items,
              items: reqData.items.map(item => ({
                ...item,
                name: String(item.name || "").slice(0, 5),
              })),
              vankey_hash: oldToken,
            }
          );
          console.log('[REPAY-CANCEL] approveRes.data: ', approveRes.data)

          const approveOk =
            approveRes.data?.status === "Y" &&
            approveRes.data?.response_code == 0;

          if (!approveOk) {
            throw new Error(
              `[재결제] 승인 실패: ${JSON.stringify(approveRes.data)}`
            );
          }

          const newToken = approveRes.data.vankey;
          const newApproveNo = approveRes.data.authorization_number;
          const newApproveAt = approveRes.data.authorization_date;
          const paymentAt = formatIfDate();

          const cancelRes = await axios.post(
            `${config.cardTerminalApi}/payment/token/cancel`,
            {
              // amount: String(oldApprovePrice),
              amount: '5',
              original_authorization_date: oldApproveAt,
              original_authorization_number: oldApproveNo,
              vankey_hash: oldToken,
            }
          );

          const cancelOk =
            cancelRes.data?.status === "Y" &&
            cancelRes.data?.response_code == 0;

          if (!cancelOk) {
            throw new Error(
              `[재결제] 기존 결제 취소 실패: ${JSON.stringify(cancelRes.data)}`
            );
          }

          const ackPayload = JSON.stringify({
            HEADER: {
              IF_ID: "IF_09",
              IF_SYSID: ifSysId,
              IF_HOST: "CRKPNTCHAI",
              IF_DATE: formatIfDate(),
            },
            DATA: {
              device_idx: reqData.device_idx,
              division_idx: reqData.division_idx,
              payment_idx: reqData.payment_idx,
              token_id: newToken,
              org_token_id: oldToken,
              payment_at: paymentAt,
              request_type: "REPAY",
              payment_mode: reqData.payment_mode,
              approve_at: newApproveAt,
              approve_price: parseInt(newApprovePrice),
              approve_no: newApproveNo,
              org_approve_at: oldApproveAt,
              org_approve_price: parseInt(oldApprovePrice),
              org_approve_no: oldApproveNo,
              result_cd: "S",
              result_msg: "재결제가 완료되었습니다",
            },
          });

          client.publish(repaymentPub, ackPayload, { qos: 1, retain: false }, (e) => {
            if (e) console.error("[REPAY] Publish Error:", e.message);
            else console.log("[REPAY] Success ACK sent");
          });

        } catch (err) {
          console.error("[REPAY] Error:", err.message, err.response?.data);

          const failPayload = JSON.stringify({
            HEADER: {
              IF_ID: "IF_09",
              IF_SYSID: ifSysId,
              IF_HOST: "CRKPNTCHAI",
              IF_DATE: formatIfDate(),
            },
            DATA: {
              device_idx: reqData.device_idx,
              division_idx: reqData.division_idx,
              payment_idx: reqData.payment_idx,
              token_id: null,
              org_token_id: oldToken,
              payment_at: null,
              request_type: "REPAY",
              payment_mode: reqData.payment_mode,
              approve_at: null,
              approve_price: null,
              approve_no: null,
              org_approve_at: oldApproveAt,
              org_approve_price: parseInt(oldApprovePrice),
              org_approve_no: oldApproveNo,
              result_cd: "F",
              result_msg: err.response?.data?.detail || err.message,
              terminal_response: err.response?.data ?? null,
            },
          });

          client.publish(repaymentPub, failPayload, { qos: 1, retain: false }, (e) => {
            if (e) console.error("[REPAY] Fail Publish Error:", e.message);
            else console.log("[REPAY] Fail ACK sent");
          });
        }
      }
    }
  });
}

module.exports = { Repayment };