// ============================================================
// auth.js
// 역할: 클라우드(PNT/CHAI) REST API 인증(로그인) 모듈.
//  - devAutoLogin(): 개발 환경에서 /auth/login 으로 자동 로그인하여
//    accessToken(JWT)을 발급받고, 메모리 캐시와 process.env.JWT_TOKEN 에 저장한다.
//  - production 환경(NODE_ENV === "production")에서는 동작하지 않고 null 반환.
// ============================================================
// 파이썬에서 import하는 부분
const express = require("express");
const axios = require("axios");
const config = require("../config/key");

const router = express.Router();

const external = axios.create({
  baseURL: config.restApi,
  timeout: 10000,
  headers: { "Content-Type": "application/json" }, // 토큰 인증 시 사용
  withCredentials: false,
});

// dev에서 자동으로 받아둘 토큰(메모리 저장)
let cachedToken = ''; // 개발용 토큰
let cachedRaw = ''; // 응답 원본

// 개발용 자동 로그인: config 의 계정 정보로 클라우드에 로그인하여
// JWT 토큰을 발급받아 캐시 및 환경변수(JWT_TOKEN)에 저장 후 반환
async function devAutoLogin() {
  if (process.env.NODE_ENV === "production") return null;

  const userId = config.userId
  const userPassword = config.userPassword
  if (!userId || !userPassword) return null;

  const r = await external.post("/auth/login", {
    loginType: "chai",
    userId: userId,
    userPassword: userPassword,
    ipaddress: "",
  });

  const token = r.data.accessToken

  // console.log("[LOGIN] pid=", process.pid, "JWT_TOKEN set");

  if (!token) return null;
  else {
    cachedToken = token;
    cachedRaw = r.data;
    // console.log('Token', cachedToken);
    process.env.JWT_TOKEN = cachedToken;
    process.env.JWT_TOKEN_AT = Date.now().toString(); // (선택) 발급시각
    console.log('jwtToken', process.env.JWT_TOKEN);
    return cachedToken; // 토큰 반환
  }
}

module.exports = { router, devAutoLogin };
