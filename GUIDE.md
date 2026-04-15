# 아파트 실거래가 지도 — 구조 가이드

## 전체 구조 한눈에 보기

```
[사용자 브라우저]
      ↕ HTTP
[Vercel (FastAPI 서버)]
      ↕ HTTP
[공공 API / 네이버 API]   ←→   [Upstash Redis DB]
```

---

## 파일 구조

```
realestate/
├── app.py               ← 서버 (FastAPI)
├── templates/
│   └── index.html       ← 화면 (HTML + JavaScript)
├── static/
│   ├── favicon.png
│   └── manifest.json
├── requirements.txt     ← Python 패키지 목록
├── .env                 ← API 키 모음 (로컬 전용, git 제외)
└── vercel.json          ← Vercel 배포 설정 (없으면 자동 감지)
```

---

## 1. 서버 — app.py (FastAPI)

Python 웹 프레임워크. 브라우저 요청을 받아서 처리합니다.

### 주요 역할
- HTML 페이지 제공 (`GET /`)
- 아파트 데이터 API 제공 (`GET /api/markers`, `/api/apartments`)
- 공공 API 호출 후 가공
- Redis 캐시 읽기/쓰기

### 주요 API 엔드포인트

| 경로 | 설명 |
|---|---|
| `GET /` | 지도 HTML 페이지 반환 |
| `GET /api/districts` | 구 목록 반환 |
| `GET /api/markers?district=강남구` | 지도 마커 데이터 |
| `GET /api/apartments?district=강남구` | 아파트 상세 데이터 |
| `GET /api/naver-asking?key=...` | 네이버 호가 조회 |
| `POST /api/update-geocache` | 좌표 저장 |

---

## 2. 화면 — templates/index.html

서버가 브라우저에 보내는 HTML 파일. 안에 JavaScript도 같이 들어있습니다.

### 구성
- **네이버 지도 API**: 지도 표시
- **마커**: 아파트 위치에 가격 태그 표시
- **하단 시트**: 마커 클릭 시 상세정보 (거래이력, 차트)
- **네이버 매물 시트**: 네이버 부동산 링크

### 데이터 흐름
```
페이지 로드
  → /api/districts 호출 → 구 목록 표시
  → 구 선택
  → /api/markers?district=강남구 호출 → 마커 표시
  → 마커 클릭
  → /api/apt-trades 호출 → 거래이력 시트 표시
```

---

## 3. 외부 API

### 국토교통부 공공 API
- **용도**: 아파트 실거래가, 전월세 데이터 조회
- **키**: `TRADE_API_KEY` (.env)
- **주소**: `apis.data.go.kr`
- **제한**: 1회 최대 1000건, 월별 조회
- **속도**: 느림 (구당 12개월 × 2종 = 약 24회 호출)

### 네이버 지도 API
- **용도**: 지도 표시, 좌표 검색
- **키**: `NAVER_MAPS_KEY` (.env)

### 네이버 부동산 API
- **용도**: 호가 조회, 단지번호 검색
- **인증**: 쿠키 (`NAVER_COOKIE` in .env) + 자동 JWT 토큰

### 교육정보 포털 (NEIS)
- **용도**: 학교 정보 조회
- **키**: `NEIS_API_KEY` (.env)

---

## 4. Redis (Upstash)

파일 대신 DB에 캐시를 저장합니다. Vercel은 파일 저장이 안 되기 때문에 필수입니다.

### 저장되는 데이터

| Redis 키 | 내용 | 만료 |
|---|---|---|
| `apt:강남구` | 강남구 아파트 목록 전체 | 24시간 |
| `apt:서초구` | 서초구 아파트 목록 전체 | 24시간 |
| `geo_cache` | 전체 아파트 좌표 `{key: {lat, lng}}` | 없음 (영구) |
| `naver_complex_cache` | 아파트 → 네이버 단지번호 매핑 | 없음 (영구) |

### 동작 방식
```
사용자가 "강남구" 요청
  → 메모리에 있으면 → 즉시 반환 (가장 빠름)
  → Redis에 있으면 → 즉시 반환 (빠름, ~5ms)
  → 둘 다 없으면 → 공공 API 24회 호출 (60초)
                 → 결과를 Redis에 저장
                 → 다음 사용자는 즉시 반환
```

---

## 5. Vercel 배포

GitHub에 push하면 자동으로 배포됩니다.

### 배포 흐름
```
git push → GitHub → Vercel 자동 감지 → 빌드 → 배포
```

### 주의사항
- Vercel은 **서버리스** — 요청이 없으면 서버가 꺼짐
- 서버가 꺼지면 **메모리 캐시 소멸** (Redis는 유지)
- 파일 쓰기 **불가** (그래서 Redis 사용)
- 함수 실행 시간 제한: 기본 10초 (Pro: 300초)

### 환경변수 설정 위치
Vercel Dashboard → 프로젝트 → Settings → Environment Variables

| 변수명 | 설명 |
|---|---|
| `TRADE_API_KEY` | 공공 API 키 |
| `NAVER_MAPS_KEY` | 네이버 지도 키 |
| `NEIS_API_KEY` | 학교 정보 키 |
| `NAVER_COOKIE` | 네이버 쿠키 |
| `UPSTASH_REDIS_REST_URL` | Redis 주소 |
| `UPSTASH_REDIS_REST_TOKEN` | Redis 인증 토큰 |

---

## 6. 로컬 개발

```bash
# 서버 실행
uvicorn app:app --reload

# 접속
http://localhost:8000
```

환경변수는 `.env` 파일에서 자동으로 읽어옵니다.

---

## 데이터 흐름 요약

```
브라우저                 Vercel(app.py)              외부
  │                          │                        │
  │── GET / ──────────────→  │                        │
  │← index.html ────────────│                        │
  │                          │                        │
  │── GET /api/markers ────→ │── Redis 확인 ─────────→│
  │                          │  있으면 ←──────────────│
  │                          │  없으면 → 공공API 호출 →│
  │                          │          ←─ 데이터 ────│
  │                          │── Redis 저장           │
  │← JSON 마커 데이터 ───────│                        │
  │                          │                        │
  │── 마커 클릭 ───────────→ │                        │
  │── GET /api/naver-asking→ │── Redis 확인           │
  │                          │  없으면 → 네이버API ──→│
  │← complex_no ────────────│          ←─────────────│
```
