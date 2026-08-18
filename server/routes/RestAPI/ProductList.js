// ============================================================
// ProductList.js
// 역할: 클라우드(PNT/CHAI) REST API IF_11(상품 목록 조회) 호출 모듈.
//  - /chai/product/list 에 division_idx / device_idx 를 담아 POST 하고
//    상품 마스터 목록 응답(r.data)을 반환한다.
//  - 인증은 config.jwtToken(Bearer) 사용.
// ============================================================
require("dotenv").config();
const axios = require("axios");
const config = require("../../config/key");
const { v4: uuidv4 } = require("uuid");
const express = require("express");
const router = express.Router();

// IF 규격(YYYYMMDDHHMMSS)의 날짜 문자열 생성
function formatIfDate(d = new Date()) {
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}`
         + `${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

const external = axios.create({
  baseURL: config.restApi, // https://apichaidev.atcrk.co.kr/api/v1
  timeout: 10000,
  headers: { "Content-Type": "application/json" },
});

// 외부 API 호출 함수
// [IF_11] 매장(division) 기준 상품 목록을 클라우드에서 조회
async function ProductList({
  division_idx = config.divisionIdx,
  device_idx = null
} = {}) {
  const token = config.jwtToken
  console.log("[PRODUCT] JWT_TOKEN =", token);
  if (!token) {
    console.log("[DEBUG] config resolved =", require.resolve("../../config/key"));
    console.log("[DEBUG] jwtToken =", config.jwtToken);
    throw new Error("JWT_TOKEN not set");
  }

  const payload = {
    HEADER: {
      IF_ID: "IF_11",
      IF_SYSID: uuidv4(),
      IF_HOST: "CRKPNTCHAI",
      IF_DATE: formatIfDate(),
    },
    DATA: {
      division_idx: division_idx,
      device_idx: device_idx,
    },
  };

  const r = await external.post("/chai/product/list", payload, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  return r.data;
}

module.exports = { ProductList, router };