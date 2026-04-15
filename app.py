from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
import httpx
from datetime import datetime
import xml.etree.ElementTree as ET
import urllib.parse
import logging
import os
import json
import re
import html as html_lib
from zipfile import ZipFile
from dotenv import load_dotenv
import time
from pathlib import Path

# .env 파일에서 환경변수 로드
load_dotenv()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI(title="아파트 실거래가 지도")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
async def _startup_naver_token():
    """서버 시작 시 네이버 JWT 토큰 미리 가져오기 + 인기 구 프리로드"""
    import asyncio
    token = await _refresh_naver_token()
    if token:
        logger.info(f"[Naver] 시작 시 토큰 획득 성공: {token[:20]}...")
    else:
        logger.warning("[Naver] 시작 시 토큰 획득 실패 — 첫 요청 시 재시도")
    asyncio.create_task(_preload_popular_districts())


PRELOAD_DISTRICTS = ["강남구", "서초구", "송파구", "마포구", "용산구", "성동구", "영등포구", "분당구"]

async def _preload_popular_districts():
    """서버 시작 후 백그라운드에서 인기 구 순차 로드 (캐시 없는 구만)"""
    import asyncio
    for district in PRELOAD_DISTRICTS:
        if district in _cache["apartments"]:
            logger.info(f"[프리로드] {district} 캐시 있음 — 스킵")
            continue
        try:
            logger.info(f"[프리로드] {district} 시작")
            await get_apartments(district=district, dong="", quick=False)
            logger.info(f"[프리로드] {district} 완료")
        except Exception as e:
            logger.warning(f"[프리로드] {district} 실패: {e}")
        await asyncio.sleep(2)

@app.on_event("shutdown")
async def _shutdown_http_client():
    """서버 종료 시 공유 HTTP 클라이언트 정리"""
    global _shared_client
    if _shared_client and not _shared_client.is_closed:
        await _shared_client.aclose()

# ─── API 설정 ────────────────────────────────────────────────
TRADE_API_KEY = os.getenv("TRADE_API_KEY")
NAVER_MAPS_KEY = os.getenv("NAVER_MAPS_KEY")

TRADE_BASE = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
RENT_BASE  = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRentDev/getRTMSDataSvcAptRentDev"

# 지역별 법정동 코드
DISTRICTS: dict[str, list[str]] = {
    # ── 서울 ──────────────────────────────────────────────────
    "종로구":   ["11110"], "중구":    ["11140"], "용산구":   ["11170"],
    "성동구":   ["11200"], "광진구":  ["11215"], "동대문구": ["11230"],
    "중랑구":   ["11260"], "성북구":  ["11290"], "강북구":   ["11305"],
    "도봉구":   ["11320"], "노원구":  ["11350"], "은평구":   ["11380"],
    "서대문구": ["11410"], "마포구":  ["11440"], "양천구":   ["11470"],
    "강서구":   ["11500"], "구로구":  ["11530"], "금천구":   ["11545"],
    "영등포구": ["11560"], "동작구":  ["11590"], "관악구":   ["11620"],
    "서초구":   ["11650"], "강남구":  ["11680"], "송파구":   ["11710"],
    "강동구":   ["11740"],
    # ── 인천 ──────────────────────────────────────────────────
    "인천중구": ["28110"], "인천동구": ["28140"], "미추홀구": ["28177"],
    "연수구":   ["28185"], "남동구":   ["28200"], "부평구":   ["28237"],
    "계양구":   ["28245"], "인천서구": ["28260"],
    "강화군":   ["28710"], "옹진군":   ["28720"],
    # ── 경기 ──────────────────────────────────────────────────
    "장안구":   ["41111"],
    "권선구":   ["41113"],
    "팔달구":   ["41115"],
    "영통구":   ["41117"],
    "수정구":   ["41131"],
    "중원구":   ["41133"],
    "분당구":   ["41135"],
    "의정부시": ["41150"],
    "만안구":   ["41171"],
    "동안구":   ["41173"],
    "부천시":   ["41190"],
    "광명시":   ["41210"],
    "평택시":   ["41220"],
    "동두천시": ["41250"],
    "상록구":   ["41271"],
    "단원구":   ["41273"],
    "덕양구":   ["41281"],
    "일산동구": ["41285"],
    "일산서구": ["41287"],
    "과천시":   ["41290"],
    "구리시":   ["41310"],
    "남양주시": ["41360"],
    "오산시":   ["41370"],
    "시흥시":   ["41390"],
    "군포시":   ["41410"],
    "의왕시":   ["41430"],
    "하남시":   ["41450"],
    "처인구":   ["41461"],
    "기흥구":   ["41463"],
    "수지구":   ["41465"],
    "파주시":   ["41480"],
    "이천시":   ["41500"],
    "안성시":   ["41550"],
    "김포시":   ["41570"],
    "만세구":   ["41591"],  # 2024.09 화성시 구 신설
    "효행구":   ["41593"],
    "병점구":   ["41595"],
    "동탄구":   ["41597"],
    "광주시":   ["41610"],
    "양주시":   ["41630"],
    "포천시":   ["41650"],
    "여주시":   ["41670"],
    "연천군":   ["41800"],
    "가평군":   ["41820"],
    "양평군":   ["41830"],
}

# 구별 아파트 데이터 메모리 캐시
_cache: dict = {"apartments": {}}
# 추가 기간 원시 데이터 캐시: "district_YYYYMM_YYYYMM" → list[dict]
_raw_period_cache: dict = {}
# 단지 상세정보 캐시 (건축물대장)
_bldg_info_cache: dict = {}

# ─── 공유 HTTP 클라이언트 (커넥션 풀링으로 TCP 오버헤드 최소화) ──
_shared_client: httpx.AsyncClient | None = None

def _get_shared_client() -> httpx.AsyncClient:
    global _shared_client
    if _shared_client is None or _shared_client.is_closed:
        _shared_client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_connections=30, max_keepalive_connections=15, keepalive_expiry=60),
        )
    return _shared_client

# ─── 좌표 캐시 (파일 영속) ────────────────────────────────────
GEO_CACHE_FILE = "geo_cache.json"
_geo_cache: dict = {}

def _load_geo_cache():
    global _geo_cache
    try:
        if os.path.exists(GEO_CACHE_FILE):
            with open(GEO_CACHE_FILE, encoding="utf-8") as f:
                _geo_cache = json.load(f)
            logger.info(f"geo cache 로드: {len(_geo_cache)}개")
    except Exception as e:
        logger.warning(f"geo cache 로드 실패: {e}")

async def _save_geo_cache():
    try:
        with open(GEO_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(_geo_cache, f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"geo cache 저장 실패: {e}")

_load_geo_cache()

# ─── 아파트 데이터 파일 영속 캐시 ─────────────────────────────
APT_CACHE_FILE = "apt_cache.json"
APT_CACHE_TTL_HOURS = 24  # 하루 지나면 재조회

def _load_apt_cache():
    try:
        if os.path.exists(APT_CACHE_FILE):
            with open(APT_CACHE_FILE, encoding="utf-8") as f:
                stored = json.load(f)
            now = datetime.now()
            loaded = 0
            for district, entry in stored.items():
                saved_at = datetime.fromisoformat(entry.get("saved_at", "2000-01-01"))
                age_hours = (now - saved_at).total_seconds() / 3600
                if age_hours < APT_CACHE_TTL_HOURS:
                    _cache["apartments"][district] = entry["apartments"]
                    loaded += 1
            logger.info(f"apt cache 로드: {loaded}개 구 (TTL {APT_CACHE_TTL_HOURS}h)")
    except Exception as e:
        logger.warning(f"apt cache 로드 실패: {e}")

async def _save_apt_cache(district: str, apartments: list):
    try:
        stored = {}
        if os.path.exists(APT_CACHE_FILE):
            with open(APT_CACHE_FILE, encoding="utf-8") as f:
                stored = json.load(f)
        stored[district] = {"saved_at": datetime.now().isoformat(), "apartments": apartments}
        with open(APT_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(stored, f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"apt cache 저장 실패: {e}")

_load_apt_cache()

# 마커 기준 면적 순서: 84㎡ 우선, 없으면 59㎡, 없으면 가장 가까운 면적
PREFERRED_AREAS = [84, 59]


def get_recent_deal_yms(count: int = 3) -> list:
    """최근 N개월의 거래연월 리스트 반환"""
    now = datetime.now()
    y, m = now.year, now.month
    result = []
    for _ in range(count):
        result.append(f"{y}{m:02d}")
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return result


def aggregate_monthly_trade_counts(rows: list, recent_count: int = 12) -> dict:
    from collections import Counter

    monthly_counter: Counter = Counter()
    for row in rows:
        if row.get("_type") != "매매":
            continue
        ym = f"{row.get('dealYear', '')}{str(row.get('dealMonth', '')).zfill(2)}"
        if len(ym) == 6:
            monthly_counter[ym] += 1

    recent_yms = sorted(monthly_counter.keys(), reverse=True)[:recent_count]
    return {ym: monthly_counter[ym] for ym in recent_yms}


def xml_items(text: str) -> list:
    """API 응답 XML에서 <item> 목록을 파싱해 딕셔너리 리스트로 반환"""
    try:
        root = ET.fromstring(text)
        return [
            {c.tag: (c.text or "").strip() for c in item}
            for item in root.findall(".//item")
        ]
    except Exception as e:
        logger.warning(f"XML parse: {e}")
        return []


def find_main_area_key(area_map: dict) -> int:
    """84㎡ → 59㎡ → 거래 많은 면적 순으로 대표 면적 반환 (오차 5㎡ 허용)"""
    keys = sorted(area_map.keys())
    for pref in PREFERRED_AREAS:
        closest = min(keys, key=lambda a: abs(a - pref))
        if abs(closest - pref) <= 5:
            return closest
    return max(keys, key=lambda a: len(area_map[a]))


def trade_date_key(t: dict) -> tuple:
    """거래 데이터를 날짜 기준으로 정렬하기 위한 키 함수"""
    try:
        return (int(t.get("dealYear", 0)), int(t.get("dealMonth", 0)), int(t.get("dealDay", 0)))
    except (ValueError, TypeError):
        return (0, 0, 0)


def format_price(manwon: int) -> str:
    """만원 단위 금액을 '40.51억' 형식 문자열로 변환 (1억 미만은 '9,500만')"""
    if manwon <= 0:
        return "-"
    if manwon < 10000:
        return f"{manwon:,}만"
    eok = round(manwon / 10000, 2)
    if eok == int(eok):
        return f"{int(eok)}억"
    # 소수점 둘째 자리까지, 불필요한 0 제거
    s = f"{eok:.2f}".rstrip('0').rstrip('.')
    return f"{s}억"




async def _fetch(url: str, label: str) -> list:
    """공통 API 호출 (공유 클라이언트로 커넥션 재사용)"""
    try:
        client = _get_shared_client()
        r = await client.get(url)
        if r.status_code == 200:
            items = xml_items(r.text)
            logger.info(f"  {label} {len(items)}건")
            return items
        logger.warning(f"  {label} HTTP {r.status_code}")
    except Exception as e:
        logger.error(f"fetch error {label}: {e}")
    return []


async def _fetch_all_pages(base_url: str, label: str) -> list:
    """1000건 제한 우회 — 페이지네이션으로 전체 수집"""
    all_items = []
    page = 1
    while True:
        url = f"{base_url}&numOfRows=1000&pageNo={page}"
        items = await _fetch(url, f"{label} p{page}")
        all_items.extend(items)
        if len(items) < 1000:
            break
        page += 1
    return all_items


async def fetch_transactions(lawd_cd: str, deal_ym: str) -> list:
    """매매 실거래 조회 — 각 row에 trade_type='매매' 추가"""
    base = (f"{TRADE_BASE}?serviceKey={TRADE_API_KEY}"
            f"&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ym}")
    rows = await _fetch_all_pages(base, f"매매[{lawd_cd} {deal_ym}]")
    for r in rows:
        r["_type"] = "매매"
    return rows


async def fetch_rents(lawd_cd: str, deal_ym: str) -> list:
    """전월세 실거래 조회 — 각 row에 trade_type 추가 (월세금액 > 0 이면 월세, 아니면 전세)"""
    base = (f"{RENT_BASE}?serviceKey={TRADE_API_KEY}"
            f"&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ym}")
    rows = await _fetch_all_pages(base, f"전월세[{lawd_cd} {deal_ym}]")
    for r in rows:
        try:
            monthly = int(r.get("monthlyRent", "0") or "0")
        except ValueError:
            monthly = 0
        r["_type"] = "월세" if monthly > 0 else "전세"
    return rows


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse(request, "index.html", {
        "naver_maps_key": NAVER_MAPS_KEY,
    })


@app.get("/api/districts")
async def get_districts():
    return {"districts": list(DISTRICTS.keys())}


@app.get("/api/markers")
async def get_markers(
    district: str = Query(default="강남구"),
    quick:    bool = Query(default=False),
):
    """마커 전용 경량 API — area_types(거래이력) 제외 (캐시 객체 변경 안 함)"""
    result = await get_apartments(district=district, dong="", quick=quick)
    if hasattr(result, "body"):
        return result
    # 얕은 복사로 캐시 원본 보호 + 84㎡/59㎡ 호가 필드 계산
    markers = []
    for apt in result.get("apartments", []):
        m = {k: v for k, v in apt.items() if k != "area_types"}
        area_types = apt.get("area_types", [])
        naver_at = next((a for a in area_types if a.get("area_key") == 84), None)
        if not naver_at:
            naver_at = next((a for a in area_types if a.get("area_key") == 59), None)
        if naver_at:
            m["naver_price_display"] = naver_at.get("latest_price_display") or apt.get("price_display", "")
            m["naver_avg_display"]   = naver_at.get("district_avg_display") or apt.get("district_avg_display", "")
            m["naver_area_label"]    = naver_at.get("label") or apt.get("main_area_label", "")
        else:
            m["naver_price_display"] = apt.get("price_display", "")
            m["naver_avg_display"]   = apt.get("district_avg_display", "")
            m["naver_area_label"]    = apt.get("main_area_label", "")
        markers.append(m)
    return {
        "apartments": markers,
        "cached": result.get("cached"),
        "full": result.get("full"),
        "monthly_trades": result.get("monthly_trades", {}),
    }


BLDG_API_BASE = "https://apis.data.go.kr/1613000/BldRgstHubService/getBrBasisOulnInfo"

async def fetch_bldg_info(sgg_cd: str, umd_cd: str, jibun: str) -> dict:
    """건축물대장 표제부 API로 세대수·사용승인일·건폐율·용적률 조회"""
    if not sgg_cd or not jibun:
        return {}
    # 지번 파싱: "663-1" → bun=663, ji=1 / "산100-1" → platGbCd=1, bun=100, ji=1
    jibun = jibun.strip()
    if jibun.startswith("산"):
        plat_gb = "1"
        jibun = jibun[1:]
    else:
        plat_gb = "0"
    parts = jibun.split("-")
    bun = parts[0].strip().zfill(4)
    ji  = parts[1].strip().zfill(4) if len(parts) > 1 else "0000"

    # umdCd가 없으면 빈 문자열로 시도 (일부 API는 생략 허용)
    params = (
        f"?serviceKey={TRADE_API_KEY}"
        f"&sigunguCd={sgg_cd}"
        f"&bjdongCd={umd_cd}"
        f"&platGbCd={plat_gb}"
        f"&bun={bun}&ji={ji}"
        f"&numOfRows=10&pageNo=1"
    )
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(BLDG_API_BASE + params)
        items = xml_items(r.text) if r.status_code == 200 else []
        if not items:
            return {}
        it = items[0]
        raw_date = it.get("useAprDay", "")        # YYYYMMDD
        occ_year  = raw_date[:4] if len(raw_date) >= 4 else ""
        occ_month = raw_date[4:6] if len(raw_date) >= 6 else ""
        if occ_year and occ_month:
            elapsed = datetime.now().year - int(occ_year)
            occ_display = f"{occ_year}년 {int(occ_month)}월({elapsed}년차)"
        elif occ_year:
            elapsed = datetime.now().year - int(occ_year)
            occ_display = f"{occ_year}년({elapsed}년차)"
        else:
            occ_display = ""
        return {
            "hhld_cnt":    it.get("hhldCnt", ""),
            "occ_display": occ_display,
            "bc_rat":      it.get("bcRat", ""),
            "vl_rat":      it.get("vlRat", ""),
        }
    except Exception as e:
        logger.warning(f"건축물대장 조회 실패: {e}")
        return {}


@app.get("/api/apt-detail")
async def get_apt_detail(key: str = Query(...), district: str = Query(...)):
    """단지 상세정보 (세대수·입주년월·건폐율·용적률)"""
    if key in _bldg_info_cache:
        return _bldg_info_cache[key]
    if district not in _cache["apartments"]:
        await get_apartments(district=district, dong="", quick=False)
    apts = _cache["apartments"].get(district, [])
    apt  = next((a for a in apts if a["key"] == key), None)
    if not apt:
        return {}
    info = await fetch_bldg_info(apt.get("sgg_cd", ""), apt.get("umd_cd", ""), apt.get("jibun", ""))
    _bldg_info_cache[key] = info
    return info


@app.get("/api/apt-trades")
async def get_apt_trades(
    key:      str = Query(...),
    district: str = Query(...),
    months:   int = Query(default=12),
):
    """단지 클릭 시 거래이력 조회 — months 파라미터로 기간 지정 (기본 12개월)"""
    if district not in DISTRICTS:
        return JSONResponse({"error": "존재하지 않는 구"}, status_code=400)

    # 12개월 초과 — 캐시 12개월 데이터 + 추가 기간만 조회 후 병합
    if months > 12:
        import asyncio
        # 먼저 캐시된 12개월 데이터 확보
        if district not in _cache["apartments"]:
            await get_apartments(district=district, dong="", quick=False)
        apts = _cache["apartments"].get(district, [])
        apt_cached = next((a for a in apts if a["key"] == key), None)

        cached_area_types = apt_cached.get("area_types", []) if apt_cached else []

        # 캐시에 없는 기간(13~months개월)만 추가 조회
        all_yms    = get_recent_deal_yms(months)
        cached_yms = set(get_recent_deal_yms(12))
        extra_yms  = [ym for ym in all_yms if ym not in cached_yms]

        extra_by_area: dict = {}
        if extra_yms:
            lawd_codes = DISTRICTS[district]
            tasks = []
            for code in lawd_codes:
                for ym in extra_yms:
                    tasks.append(fetch_transactions(code, ym))
                    tasks.append(fetch_rents(code, ym))
            results = await asyncio.gather(*tasks)
            extra_raw = [row for batch in results for row in batch]

            for row in extra_raw:
                if f"{row.get('name','')}_{row.get('dong','')}" != key:
                    continue
                ak = round(float(row.get("area", 0)))
                extra_by_area.setdefault(ak, []).append(row)

        # 캐시된 area_types에 추가 trades 병합 (enriched 필드 유지)
        area_types = []
        for at in cached_area_types:
            ak = at["area_key"]
            extra_rows = extra_by_area.pop(ak, [])
            extra_trades = []
            for r in extra_rows:
                p = r.get("price", 0)
                ttype = r.get("trade_type", "매매")
                deposit = r.get("deposit", 0)
                monthly = r.get("monthly_rent", 0)
                if ttype == "매매":
                    price_str = format_price(p)
                elif monthly > 0:
                    price_str = f"{format_price(deposit)}/{monthly:,}"
                else:
                    price_str = format_price(deposit)
                extra_trades.append({
                    "year_month": f"{r.get('year','')}.{r.get('month','').zfill(2)}",
                    "day": r.get("day", ""),
                    "floor": r.get("floor", ""),
                    "building": r.get("building", ""),
                    "price": p if ttype == "매매" else deposit,
                    "price_display": price_str,
                    "trade_type": ttype,
                    "area": r.get("area", ""),
                })
            merged_at = dict(at)
            merged_at["trades"] = sorted(
                list(at.get("trades", [])) + extra_trades,
                key=lambda r: (r.get("year_month", ""), r.get("day", "")),
                reverse=True,
            )
            area_types.append(merged_at)

        # 캐시에 없던 면적(신규)도 추가
        for ak, rows in extra_by_area.items():
            trade_history = []
            for r in rows:
                p = r.get("price", 0)
                ttype = r.get("trade_type", "매매")
                deposit = r.get("deposit", 0)
                monthly = r.get("monthly_rent", 0)
                price_str = format_price(p) if ttype == "매매" else (
                    f"{format_price(deposit)}/{monthly:,}" if monthly > 0 else format_price(deposit))
                trade_history.append({
                    "year_month": f"{r.get('year','')}.{r.get('month','').zfill(2)}",
                    "day": r.get("day",""), "floor": r.get("floor",""),
                    "building": r.get("building",""),
                    "price": p if ttype == "매매" else deposit,
                    "price_display": price_str, "trade_type": ttype, "area": r.get("area",""),
                })
            trade_history.sort(key=lambda r: (r.get("year_month",""), r.get("day","")), reverse=True)
            area_types.append({"area_key": ak, "label": f"{ak}㎡", "trades": trade_history})

        return {"area_types": area_types}

    # 12개월 이하 — 캐시 사용
    if district not in _cache["apartments"]:
        await get_apartments(district=district, dong="", quick=False)
    apts = _cache["apartments"].get(district, [])
    apt  = next((a for a in apts if a["key"] == key), None)
    if not apt:
        return JSONResponse({"error": "단지를 찾을 수 없습니다"}, status_code=404)
    return {"area_types": apt.get("area_types", [])}


@app.get("/api/apartments")
async def get_apartments(
    district: str = Query(default="강남구"),
    dong:     str = Query(default=""),   # 특정 동 필터 (빠른 초기 로드용)
    quick:    bool = Query(default=False), # True: 3개월만 조회 (빠른 초기 응답)
):
    if district not in DISTRICTS:
        return JSONResponse({"error": "존재하지 않는 구"}, status_code=400)

    # 1차: 메모리 캐시 히트 → 즉시 반환
    if district in _cache["apartments"]:
        apts = _cache["apartments"][district]
        if dong:
            apts = [a for a in apts if a.get("dong") == dong]
        return {"apartments": apts, "cached": True, "full": True, "monthly_trades": {}}

    import asyncio
    lawd_codes = DISTRICTS[district]

    # quick=True: 1개월만 먼저 조회 (초기 응답 속도 최우선)
    months = 1 if quick else 12
    deal_yms = get_recent_deal_yms(months)

    tasks = []
    for code in lawd_codes:
        for ym in deal_yms:
            tasks.append(fetch_transactions(code, ym))
            tasks.append(fetch_rents(code, ym))
    results = await asyncio.gather(*tasks)
    all_raw = [row for batch in results for row in batch]
    logger.info(f"[{district}] 총 {len(all_raw)}건 (months={months})")

    if not all_raw:
        return {"apartments": [], "cached": False, "full": not quick, "monthly_trades": {}}

    # 월별 매매 거래건수 집계 (구 전체)
    from collections import Counter
    _monthly_counter: Counter = Counter()
    for _row in all_raw:
        if _row.get("_type") == "매매":
            _ym = f"{_row.get('dealYear','')}{str(_row.get('dealMonth','')).zfill(2)}"
            if len(_ym) == 6:
                _monthly_counter[_ym] += 1
    # 최근 12개월 반환
    _recent_yms = sorted(_monthly_counter.keys(), reverse=True)[:12]
    monthly_trades = {ym: _monthly_counter[ym] for ym in _recent_yms}

    # 단지명+동 기준으로 그룹핑, 면적(㎡ 반올림)별로 세분화
    apt_map: dict = {}
    for row in all_raw:
        try:
            area = float(row.get("excluUseAr", 0))
        except (ValueError, TypeError):
            continue
        if area <= 0:
            continue

        area_key = round(area / 3) * 3  # 3㎡ 단위로 묶어 59/60㎡ 쪼개짐 방지
        name = row.get("aptNm", "").strip()
        dong = row.get("umdNm", "").strip()
        if not name:
            continue

        apt_key = f"{name}_{dong}"
        if apt_key not in apt_map:
            apt_map[apt_key] = {
                "name": name, "dong": dong, "key": apt_key,
                "buildYear": row.get("buildYear", ""),
                "jibun": row.get("jibun", "").strip(),
                "sgg_cd": row.get("sggCd", "").strip(),
                "umd_cd": row.get("umdCd", "").strip(),
                "areas": {},
            }
        apt_map[apt_key]["areas"].setdefault(area_key, []).append(row)

    # 결과 조립
    candidates = []
    for apt in apt_map.values():
        area_groups = apt["areas"]
        if not area_groups:
            continue

        # 대표 면적(마커 표시용): 매매만 기준
        trade_only = {k: [r for r in v if r.get("_type") == "매매"]
                      for k, v in area_groups.items()}
        trade_only = {k: v for k, v in trade_only.items() if v}
        if not trade_only:
            continue
        main_key = find_main_area_key(trade_only)
        main_trades = sorted(trade_only[main_key], key=trade_date_key, reverse=True)
        latest = main_trades[0]
        try:
            price_val = int(latest.get("dealAmount", "0").replace(",", ""))
        except (ValueError, TypeError):
            price_val = 0
        if price_val <= 0:
            continue

        y = latest.get("dealYear", "")
        mo = str(latest.get("dealMonth", "")).zfill(2)
        d = str(latest.get("dealDay", "")).zfill(2)

        # 면적 타입별 거래 이력 (매매+전월세 합산, 날짜 역순)
        area_types = []
        for area_key in sorted(area_groups.keys()):
            all_trades = sorted(area_groups[area_key], key=trade_date_key, reverse=True)
            lt_매매 = next((t for t in all_trades if t.get("_type") == "매매"), None)
            lt_latest = all_trades[0]  # 타입 무관 최신 거래

            if lt_매매:
                try:
                    lt_price = int(lt_매매.get("dealAmount", "0").replace(",", ""))
                except (ValueError, TypeError):
                    lt_price = 0
                lt_y = lt_매매.get("dealYear", "")
                lt_mo = str(lt_매매.get("dealMonth", "")).zfill(2)
                lt_d = str(lt_매매.get("dealDay", "")).zfill(2)
            else:
                # 매매 없음 → 최신 전월세 기준
                try:
                    lt_price = int(lt_latest.get("deposit", "0").replace(",", ""))
                except (ValueError, TypeError):
                    lt_price = 0
                lt_y = lt_latest.get("dealYear", "")
                lt_mo = str(lt_latest.get("dealMonth", "")).zfill(2)
                lt_d = str(lt_latest.get("dealDay", "")).zfill(2)

            trade_history = []
            for t in all_trades:
                ttype = t.get("_type", "매매")
                ty = t.get("dealYear", "")[2:]   # 26 (2자리)
                tmo = str(t.get("dealMonth", "")).zfill(2)
                td = str(t.get("dealDay", "")).zfill(2)

                if ttype == "매매":
                    try:
                        p = int(t.get("dealAmount", "0").replace(",", ""))
                    except (ValueError, TypeError):
                        p = 0
                    price_str = format_price(p)
                else:
                    # 전세/월세: 보증금(deposit) + 월세(monthlyRent)
                    try:
                        deposit = int(t.get("deposit", "0").replace(",", ""))
                    except (ValueError, TypeError):
                        deposit = 0
                    try:
                        monthly = int(t.get("monthlyRent", "0") or "0")
                    except (ValueError, TypeError):
                        monthly = 0
                    if monthly > 0:
                        price_str = f"{format_price(deposit)}/{monthly:,}"
                    else:
                        price_str = format_price(deposit)
                    p = deposit

                # 정보: 계약취소(cdealType=O) 또는 거래유형(dealingGbn)
                cdeal = (t.get("cdealType") or "").strip()
                dealing = (t.get("dealingGbn") or "").strip()
                if cdeal == "O":
                    info = "취소"
                else:
                    info = cdeal if cdeal else dealing

                trade_history.append({
                    "year_month": f"{ty}.{tmo}",
                    "day": td,
                    "trade_type": ttype,
                    "price": p,
                    "price_display": price_str,
                    "info": info,
                    "apt_dong": (t.get("aptDong") or "").strip(),
                    "floor": t.get("floor", ""),
                    "area": t.get("excluUseAr", ""),
                })

            area_types.append({
                "label": f"{area_key}㎡",
                "area_key": area_key,
                "latest_price": lt_price,
                "latest_price_display": format_price(lt_price),
                "latest_date": f"{lt_y}.{lt_mo}.{lt_d}",
                "trades": trade_history,
            })

        # 전체 거래건수 (매매+전월세) — 단지 규모 추정에 사용
        total_trades = sum(len(v) for v in area_groups.values())

        candidates.append({
            "key": apt["key"],
            "name": apt["name"],
            "dong": apt["dong"],
            "jibun": apt["jibun"],
            "sgg_cd": apt.get("sgg_cd", ""),
            "umd_cd": apt.get("umd_cd", ""),
            "district": district,
            "region": ("경기" if any(c.startswith("41") for c in lawd_codes)
                       else "인천" if any(c.startswith("28") for c in lawd_codes)
                       else "서울"),
            "price": price_val,
            "price_display": format_price(price_val),
            "main_area_key": main_key,
            "main_area_label": f"{main_key}㎡",
            "built_year": apt.get("buildYear", ""),
            "latest_date": f"{y}.{mo}.{d}",
            "area_types": area_types,
            "total_trades": total_trades,
            "is_champion": False,  # 동 내 최고가 여부, 아래서 업데이트
        })

    district_avg = round(sum(apt["price"] for apt in candidates) / len(candidates)) if candidates else 0
    district_apt_count = len(candidates)
    district_area_prices: dict[int, list[int]] = {}
    for apt in candidates:
        for area_type in apt.get("area_types", []):
            latest_price = area_type.get("latest_price", 0)
            area_key = area_type.get("area_key")
            if area_key and latest_price:
                district_area_prices.setdefault(area_key, []).append(latest_price)

    district_area_avg = {
        area_key: round(sum(prices) / len(prices))
        for area_key, prices in district_area_prices.items()
        if prices
    }

    # 구(區) 전체 최고가 단 1개만 대장
    district_max_price = max((apt["price"] for apt in candidates), default=0)

    for apt in candidates:
        diff = apt["price"] - district_avg
        apt["is_champion"] = apt["price"] == district_max_price
        apt["district_avg"] = district_avg
        apt["district_avg_display"] = format_price(district_avg)
        apt["district_diff"] = diff
        apt["district_diff_display"] = format_price(abs(diff))
        apt["district_apt_count"] = district_apt_count
        for area_type in apt.get("area_types", []):
            area_avg = district_area_avg.get(area_type.get("area_key"))
            if not area_avg:
                continue
            area_type["district_avg"] = area_avg
            area_type["district_avg_display"] = format_price(area_avg)
            area_type["district_avg_count"] = len(district_area_prices.get(area_type.get("area_key"), []))

    # 서버 geo 캐시에서 좌표 주입
    apartments = candidates[:]
    apartments.sort(key=lambda x: x["price"], reverse=True)
    for apt in apartments:
        geo = _geo_cache.get(apt["key"])
        if geo:
            apt["lat"] = geo["lat"]
            apt["lng"] = geo["lng"]

    # 12개월 full 데이터만 캐시 (quick=3개월은 캐시 안 함)
    if not quick:
        _cache["apartments"][district] = apartments
        await _save_apt_cache(district, apartments)
    logger.info(f"[{district}] 마커: {len(apartments)}개 (months={months}, 좌표캐시: {sum(1 for a in apartments if 'lat' in a)}개)")

    if dong:
        apartments = [a for a in apartments if a.get("dong") == dong]
    return {"apartments": apartments, "cached": False, "full": not quick, "monthly_trades": monthly_trades}


# ─── KB 주간 아파트 매매가격 지수 ──────────────────────────────
KB_INDEX_URL = "https://data-api.kbland.kr/bfmstat/weekMnthlyHuseTrnd/priceIndex"
_kb_index_cache: dict | None = None
_kb_index_cached_at: datetime | None = None
KB_INDEX_TTL_HOURS = 12
_district_volume_cache: dict[str, dict] = {}
_district_volume_cached_at: dict[str, datetime] = {}
DISTRICT_VOLUME_TTL_HOURS = 12

# 지역명 정규화: KB 응답의 지역명 → 우리 district 키로 매핑
# 인천은 "중구/동구/서구"를 서울과 구분하기 위해 "인천중구/인천동구/인천서구"로 변환
KB_INCHEON_ALIAS = {
    "중구": "인천중구", "동구": "인천동구", "서구": "인천서구",
}

async def _fetch_kb_region(region_code: str) -> dict:
    """KB API 호출 → {지역명: {index, change, date}} 딕셔너리 반환

    응답 구조:
      dataBody.data = {
        "데이터리스트": [{지역코드, 지역명, dataList: [idx0, idx1, ..., idxN, change_rate]}, ...],
        "날짜리스트":   ["20240401", ..., "20260330"],  ← N개 (dataList보다 1개 적음)
      }
    dataList[-1] = 해당 주 주간 변동률(%)
    dataList[-2] = 최신 가격지수
    """
    try:
        params = {
            "월간주간구분코드": "02",
            "매물종별구분": "01",
            "매매전세코드": "01",
            "지역코드": region_code,
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(KB_INDEX_URL, params=params)
            if r.status_code != 200:
                return {}
            body = r.json()
            inner = body.get("dataBody", {}).get("data", {})
            if not isinstance(inner, dict):
                return {}
            items = inner.get("데이터리스트", [])
            dates = inner.get("날짜리스트", [])
            date_str = dates[-1] if dates else ""
            result = {}
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = (item.get("지역명") or "").strip()
                dl = item.get("dataList", [])
                if not name or len(dl) < 2:
                    continue
                # 마지막 값 = 변동률(%), 마지막에서 두 번째 = 최신 지수
                change = float(dl[-1])
                idx = float(dl[-2])
                result[name] = {"index": round(idx, 2), "change": round(change, 2), "date": date_str}
            return result
    except Exception as e:
        logger.warning(f"KB index fetch error ({region_code}): {e}")
    return {}


@app.get("/api/kb-index")
async def get_kb_index():
    """KB 주간 아파트 매매가격 지수 (구/시 레벨)"""
    global _kb_index_cache, _kb_index_cached_at
    now = datetime.now()
    if _kb_index_cache is not None and _kb_index_cached_at:
        age = (now - _kb_index_cached_at).total_seconds() / 3600
        if age < KB_INDEX_TTL_HOURS:
            return _kb_index_cache

    import asyncio
    maps = await asyncio.gather(
        _fetch_kb_region("11"),  # 서울 (25개 구 전체 포함)
        _fetch_kb_region("41"),  # 경기 (시/군/구 포함)
        _fetch_kb_region("28"),  # 인천 (군/구 포함)
    )

    seoul_map, gyeonggi_map, incheon_map = maps

    index_map: dict = {}
    # 서울/경기: 지역명 그대로 저장
    for name, info in {**seoul_map, **gyeonggi_map}.items():
        index_map[name] = info
        # "수원시 팔달구" → "팔달구" short형도 저장 (단 서울과 충돌 없는 경우만)
        parts = name.split()
        if len(parts) == 2:
            index_map.setdefault(parts[1], info)
    # 인천: 서울 "중구/서구/동구"와 충돌하므로 alias 적용
    for name, info in incheon_map.items():
        norm = KB_INCHEON_ALIAS.get(name, name)
        index_map[norm] = info

    logger.info(f"KB index 로드: {len(index_map)}개 지역")
    result = {"districts": index_map}
    _kb_index_cache = result
    _kb_index_cached_at = now
    return result


@app.get("/api/district-volume")
async def get_district_volume(
    district: str = Query(...),
):
    """구/시 단위 최근 60개월 매매 거래량"""
    if district not in DISTRICTS:
        return JSONResponse({"error": "존재하지 않는 구"}, status_code=400)

    now = datetime.now()
    cached = _district_volume_cache.get(district)
    cached_at = _district_volume_cached_at.get(district)
    if cached is not None and cached_at is not None:
        age = (now - cached_at).total_seconds() / 3600
        cached_months = len(cached.get("monthly_trades", {}))
        if age < DISTRICT_VOLUME_TTL_HOURS and cached_months >= 50:
            return cached

    import asyncio

    lawd_codes = DISTRICTS[district]
    deal_yms = get_recent_deal_yms(60)
    tasks = [fetch_transactions(code, ym) for code in lawd_codes for ym in deal_yms]
    results = await asyncio.gather(*tasks)
    all_raw = [row for batch in results for row in batch]

    result = {
        "district": district,
        "monthly_trades": aggregate_monthly_trade_counts(all_raw, recent_count=60),
    }
    _district_volume_cache[district] = result
    _district_volume_cached_at[district] = now
    return result


@app.post("/api/geocache")
async def update_geocache(request: Request):
    """클라이언트가 지오코딩한 좌표를 서버에 저장"""
    data = await request.json()
    key = (data.get("key") or "").strip()
    lat, lng = data.get("lat"), data.get("lng")
    if key and lat and lng:
        _geo_cache[key] = {"lat": float(lat), "lng": float(lng)}
        await _save_geo_cache()
    return {"ok": True}


@app.get("/api/more-trades")
async def get_more_trades(
    district:  str = Query(...),
    apt_name:  str = Query(...),
    apt_dong:  str = Query(...),
    area_key:  int = Query(...),
    offset:    int = Query(12),
    count:     int = Query(6),
):
    """특정 단지의 추가 거래 이력 (offset~offset+count 개월 전)"""
    if district not in DISTRICTS:
        return JSONResponse({"error": "존재하지 않는 구"}, status_code=400)

    all_yms    = get_recent_deal_yms(offset + count)
    period_yms = all_yms[offset: offset + count]
    if not period_yms:
        return {"trades": [], "has_more": False}

    cache_key = f"{district}_{'_'.join(period_yms)}"
    if cache_key not in _raw_period_cache:
        import asyncio
        lawd_codes = DISTRICTS[district]
        tasks = []
        for code in lawd_codes:
            for ym in period_yms:
                tasks.append(fetch_transactions(code, ym))
                tasks.append(fetch_rents(code, ym))
        results = await asyncio.gather(*tasks)
        _raw_period_cache[cache_key] = [r for batch in results for r in batch]
        logger.info(f"[{district}] more-trades {period_yms[0]}~{period_yms[-1]}: {len(_raw_period_cache[cache_key])}건")

    all_raw = _raw_period_cache[cache_key]

    # 해당 단지+면적만 필터
    target_area = round(area_key / 3) * 3
    rows = [
        r for r in all_raw
        if r.get("aptNm", "").strip() == apt_name
        and r.get("umdNm", "").strip() == apt_dong
        and abs(round(float(r.get("excluUseAr", 0)) / 3) * 3 - target_area) <= 3
    ]

    trade_history = []
    for t in sorted(rows, key=trade_date_key, reverse=True):
        ttype = t.get("_type", "매매")
        ty  = t.get("dealYear", "")[2:]
        tmo = str(t.get("dealMonth", "")).zfill(2)
        td  = str(t.get("dealDay", "")).zfill(2)
        if ttype == "매매":
            try:
                p = int(t.get("dealAmount", "0").replace(",", ""))
            except (ValueError, TypeError):
                p = 0
            price_str = format_price(p)
        else:
            try:
                deposit = int(t.get("deposit", "0").replace(",", ""))
                monthly = int(t.get("monthlyRent", "0") or "0")
            except (ValueError, TypeError):
                deposit = monthly = 0
            price_str = f"{format_price(deposit)}/{monthly:,}" if monthly > 0 else format_price(deposit)
            p = deposit
        cdeal = (t.get("cdealType") or "").strip()
        dealing = (t.get("dealingGbn") or "").strip()
        info = "취소" if cdeal == "O" else (cdeal or dealing)
        trade_history.append({
            "year_month": f"{ty}.{tmo}", "day": td,
            "trade_type": ttype, "price": p, "price_display": price_str,
            "info": info,
            "apt_dong": (t.get("aptDong") or "").strip(),
            "floor": t.get("floor", ""),
        })

    next_offset = offset + count
    # 데이터가 있고 최대 60개월(5년)까지 허용, 빈 결과면 종료
    has_more = bool(trade_history) and next_offset < 60
    return {"trades": trade_history, "has_more": has_more, "next_offset": next_offset}


NEIS_API_KEY = os.getenv("NEIS_API_KEY", "")  # https://open.neis.go.kr 에서 무료 등록
NEIS_BASE = "https://open.neis.go.kr/hub/schoolInfo"
# 시도명 매핑
SIDO_MAP = {
    "서울": "서울특별시", "경기": "경기도", "인천": "인천광역시",
}
DISTRICT_SIDO = {}
for _region, _data in [
    ("서울", ["종로구","중구","용산구","성동구","광진구","동대문구","중랑구","성북구","강북구",
              "도봉구","노원구","은평구","서대문구","마포구","양천구","강서구","구로구","금천구",
              "영등포구","동작구","관악구","서초구","강남구","송파구","강동구"]),
    ("경기", ["장안구","권선구","팔달구","영통구","수정구","중원구","분당구","의정부시","만안구",
              "동안구","부천시","광명시","평택시","동두천시","상록구","단원구","덕양구","일산동구",
              "일산서구","과천시","구리시","남양주시","오산시","시흥시","군포시","의왕시","하남시",
              "처인구","기흥구","수지구","파주시","이천시","안성시","김포시","만세구","효행구",
              "병점구","동탄구","광주시","양주시","포천시","여주시","연천군","가평군","양평군"]),
    ("인천", ["인천중구","인천동구","미추홀구","연수구","남동구","부평구","계양구","인천서구","강화군","옹진군"]),
]:
    for _d in _data:
        DISTRICT_SIDO[_d] = _region

_school_cache: dict = {}  # "sido_sigungu_type" → list[school]
_school_index_cache: dict | None = None  # (school_type, school_name) -> [schoolinfo_id]
_school_meta_cache: dict = {}            # schoolinfo_id -> {title, address}
_school_detail_cache: dict = {}          # district|type|name|address -> detail
_school_region_index_cache: dict = {}    # district|type -> {school_name: [schoolinfo_id]}
_snu_admission_cache: dict[str, dict] | None = None

SCHOOLINFO_INDEX_URL = "https://www.schoolinfo.go.kr/ei/ss/pneiss_a08_s0.do"
SCHOOLINFO_DETAIL_URL = "https://www.schoolinfo.go.kr/ei/ss/Pneiss_b01_s0.do"
SCHOOLINFO_CAREER_URL = "https://www.schoolinfo.go.kr/ei/pp/Pneipp_b06_s0p.do"
SCHOOLINFO_DISTRICT_ALIAS = {
    "인천중구": "중구",
    "인천동구": "동구",
    "인천서구": "서구",
}
SCHOOLINFO_SIDO_CODE = {
    "서울": "1100000000",
    "경기": "4100000000",
    "인천": "2800000000",
}
SNU_ADMISSION_FILE = r"C:\Users\PC\Downloads\2026_서울대_진학현황1.xlsx"
SNU_SCHOOL_ALIASES = {
    "외대부고": ["한국외국어대학교부설고등학교", "한국외국어대학교부속고등학교"],
    "단대부고": ["단국대학교사범대학부속고등학교", "단국대학교사범대학부설고등학교"],
    "경기외고": ["경기외국어고등학교"],
    "대일외고": ["대일외국어고등학교"],
    "대원외고": ["대원외국어고등학교"],
    "명덕외고": ["명덕외국어고등학교"],
    "한영외고": ["한영외국어고등학교"],
}


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    text = html_lib.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", "", text or "")


def _normalize_school_name(text: str) -> str:
    text = _normalize_space(text)
    text = re.sub(r"[()\-.,·]", "", text)
    text = text.replace("고등학교", "고")
    text = text.replace("부설", "부속")
    text = text.replace("여자", "여")
    text = text.replace("대학교", "대")
    text = text.replace("사범대학", "사대")
    return text


def _snu_match_keys(text: str) -> set[str]:
    raw = _normalize_space(text)
    keys = {_normalize_school_name(raw)}

    if raw.endswith("여고"):
        keys.add(_normalize_school_name(raw[:-2] + "여자고등학교"))
    if raw.endswith("외고"):
        keys.add(_normalize_school_name(raw[:-2] + "외국어고등학교"))
    if raw.endswith("국제고"):
        keys.add(_normalize_school_name(raw[:-3] + "국제고등학교"))
    if raw.endswith("과고"):
        keys.add(_normalize_school_name(raw[:-2] + "과학고등학교"))
    if raw.endswith("예고"):
        keys.add(_normalize_school_name(raw[:-2] + "예술고등학교"))
    if raw.endswith("체고"):
        keys.add(_normalize_school_name(raw[:-2] + "체육고등학교"))
    if raw.endswith("고") and not raw.endswith(("여고", "외고", "국제고", "과고", "예고", "체고")):
        keys.add(_normalize_school_name(raw + "등학교"))

    return {key for key in keys if key}


def _iter_xlsx_rows(path: str) -> list[dict[str, str]]:
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

    def col_letters(ref: str) -> str:
        letters = []
        for ch in ref:
            if ch.isalpha():
                letters.append(ch)
            else:
                break
        return "".join(letters)

    with ZipFile(path) as zf:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("a:si", ns):
                shared_strings.append("".join(node.text or "" for node in si.iterfind(".//a:t", ns)))

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
        first_sheet = workbook.find("a:sheets/a:sheet", ns)
        if first_sheet is None:
            return []

        rel_id = first_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id", "")
        target = "xl/" + rel_map.get(rel_id, "").lstrip("/")
        if not target or target not in zf.namelist():
            return []

        sheet = ET.fromstring(zf.read(target))
        rows: list[dict[str, str]] = []
        for row in sheet.findall(".//a:sheetData/a:row", ns):
            values: dict[str, str] = {}
            for cell in row.findall("a:c", ns):
                ref = cell.attrib.get("r", "")
                cell_type = cell.attrib.get("t")
                value = ""
                if cell_type == "inlineStr":
                    value = "".join(node.text or "" for node in cell.iterfind(".//a:t", ns))
                else:
                    raw = cell.find("a:v", ns)
                    if raw is not None:
                        value = raw.text or ""
                        if cell_type == "s":
                            try:
                                value = shared_strings[int(value)]
                            except (ValueError, IndexError):
                                value = ""
                values[col_letters(ref)] = value.strip()
            rows.append(values)
        return rows


def _load_snu_admission_cache() -> dict[str, dict]:
    global _snu_admission_cache
    if _snu_admission_cache is not None:
        return _snu_admission_cache

    result: dict[str, dict] = {}
    if not os.path.exists(SNU_ADMISSION_FILE):
        _snu_admission_cache = result
        return result

    try:
        rows = _iter_xlsx_rows(SNU_ADMISSION_FILE)
        total_col, early_col, regular_col = "C", "D", "E"

        for row in rows:
            if (row.get("B") or "").strip() != "고교명":
                continue
            if (row.get("D") or "").strip() == "수시" and (row.get("E") or "").strip() == "정시":
                total_col, early_col, regular_col = "C", "D", "E"
            elif (row.get("F") or "").strip() == "개" and (row.get("I") or "").strip() == "개":
                total_col, early_col, regular_col = "C", "F", "I"
            break

        for row in rows:
            school_name = (row.get("B") or "").strip()
            total = _pick_numeric_value(row, total_col)
            early = _pick_numeric_value(row, early_col, "D", "F")
            regular = _pick_numeric_value(row, regular_col, "E", "I")
            if not school_name or not total.isdigit():
                continue
            record = {
                "school_name": school_name,
                "early": int(early) if early.isdigit() else 0,
                "regular": int(regular) if regular.isdigit() else 0,
                "total": int(total),
            }
            for key in _snu_match_keys(school_name):
                result[key] = record
            for alias in SNU_SCHOOL_ALIASES.get(school_name, []):
                for key in _snu_match_keys(alias):
                    result[key] = record
        logger.info(f"[학교] 서울대 진학현황 로드: {len(result)}개 키")
    except Exception as e:
        logger.warning(f"서울대 진학현황 로드 실패: {e}")

    _snu_admission_cache = result
    return result


def _match_snu_admission(name: str) -> dict | None:
    records = _load_snu_admission_cache()
    if not records:
        return None

    for key in _snu_match_keys(name):
        if key in records:
            return records[key]
    return None


# ── 네이버 부동산 호가 ────────────────────────────────────────
NAVER_ASKING_TTL = 3600 * 4   # 호가 캐시 4시간
NAVER_TOKEN_TTL  = 3600 * 2   # JWT 토큰 캐시 2시간 (만료 3시간이므로 여유 있게)
_COMPLEX_CACHE_FILE = Path("naver_complex_no.json")

def _load_complex_cache() -> dict:
    try:
        if _COMPLEX_CACHE_FILE.exists():
            return json.loads(_COMPLEX_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

async def _save_complex_cache(cache: dict) -> None:
    try:
        _COMPLEX_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning(f"[Naver] complexNo 파일 저장 실패: {e}")

_naver_complex_cache: dict = _load_complex_cache()   # apt_key -> complex_no (영구)
_naver_asking_cache: dict  = {}   # apt_key -> {price_84, price_59, complex_no, ts, ...}
_naver_token_cache: dict   = {"token": None, "ts": 0}  # JWT Bearer 토큰

def _naver_base_headers() -> dict:
    """기본 Naver 요청 헤더"""
    h = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer":         "https://new.land.naver.com/",
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }
    cookie = os.getenv("NAVER_COOKIE", "").strip()
    if cookie:
        h["Cookie"] = cookie
    return h

def _naver_headers() -> dict:
    """Bearer 토큰 포함 헤더 (캐시된 토큰 사용)"""
    h = _naver_base_headers()
    token = _naver_token_cache.get("token")
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


async def _refresh_naver_token() -> str | None:
    """네이버 부동산 페이지에서 JWT 토큰 추출 (3시간 유효)"""
    import re as _re
    try:
        headers = _naver_base_headers()
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        async with httpx.AsyncClient(headers=headers, timeout=10.0, follow_redirects=True) as c:
            r = await c.get("https://new.land.naver.com/complexes")
        m = _re.search(r'"token"\s*:\s*"([A-Za-z0-9._-]+)"', r.text)
        if m:
            token = m.group(1)
            _naver_token_cache["token"] = token
            _naver_token_cache["ts"]    = time.time()
            logger.info("[Naver] JWT 토큰 갱신 성공")
            return token
    except Exception as e:
        logger.warning(f"[Naver] 토큰 갱신 실패: {e}")
    return None


async def _get_naver_token() -> str | None:
    """유효한 JWT 토큰 반환 (만료 시 자동 갱신)"""
    now = time.time()
    if _naver_token_cache["token"] and now - _naver_token_cache["ts"] < NAVER_TOKEN_TTL:
        return _naver_token_cache["token"]
    return await _refresh_naver_token()


async def _naver_get(url: str, params: dict) -> dict | None:
    """Naver API GET — Bearer 토큰 포함, 401 시 토큰 갱신 후 재시도"""
    import asyncio
    for attempt in range(2):
        token = await _get_naver_token()
        headers = _naver_base_headers()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            async with httpx.AsyncClient(headers=headers, timeout=7.0) as c:
                r = await c.get(url, params=params)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 401 and attempt == 0:
                logger.warning("[Naver] 401 — 토큰 만료, 갱신 후 재시도")
                _naver_token_cache["ts"] = 0  # 강제 갱신
                continue
            if r.status_code == 429 and attempt == 0:
                logger.warning(f"Naver 429 — 3초 후 재시도: {url}")
                await asyncio.sleep(3)
                continue
            logger.warning(f"Naver HTTP {r.status_code}: {url}")
            return None
        except Exception as e:
            logger.warning(f"Naver 요청 실패: {e}")
            return None
    return None


async def _search_naver_complex_no(name: str) -> str | None:
    """단지명 → complexNo 검색
    1차: land.naver.com 구형 검색 API
    2차: search.naver.com HTML 파싱
    ※ 이름 매칭 실패 시 None 반환 (잘못된 단지 캐싱 방지)
    """
    import re as _re

    def _name_match(query: str, candidate: str) -> bool:
        """단지명 유사도 검사 — 공백·특수문자 제거 후 포함 여부"""
        q = _re.sub(r'[\s\-·]', '', query)
        c = _re.sub(r'[\s\-·?]', '', candidate)
        return q in c or c in q

    # ── 1차: 구형 land.naver.com 검색 API
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Referer": "https://land.naver.com/",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ko-KR,ko;q=0.9",
        }
        cookie = os.getenv("NAVER_COOKIE", "").strip()
        if cookie:
            headers["Cookie"] = cookie
        async with httpx.AsyncClient(headers=headers, timeout=8.0) as c:
            r = await c.get(
                "https://land.naver.com/search/complexSearch.naver",
                params={"query": name},
            )
        if r.status_code == 200:
            text = r.content.decode("utf-8", errors="replace")
            pairs = _re.findall(
                r'"complexCode"\s*:\s*"(\d+)"[^}]*?"complexName"\s*:\s*"([^"]+)"',
                text,
            )
            for code, nm in pairs:
                if _name_match(name, nm):
                    logger.info(f"[Naver] '{name}' → complexCode {code} '{nm}' (구형 API)")
                    return code
            if pairs:
                logger.info(f"[Naver] '{name}' 구형 API 이름 매칭 실패 (후보: {[nm for _, nm in pairs[:3]]})")
    except Exception as e:
        logger.warning(f"[Naver] 구형 API 검색 실패: {e}")

    # ── 2차: search.naver.com HTML 파싱
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9",
        }
        async with httpx.AsyncClient(headers=headers, timeout=8.0, follow_redirects=True) as c:
            r = await c.get(
                "https://search.naver.com/search.naver",
                params={"query": name + " 아파트", "where": "nexearch"},
            )
        # complexNo 뒤에 나오는 단지명도 함께 추출해서 이름 검증
        hits = _re.findall(
            r'new\.land\.naver\.com/complexes/(\d+)[^"]*"[^"]*complexName[^"]*"[^"]*"([^"]+)"',
            r.text,
        )
        for no, nm in hits:
            if _name_match(name, nm):
                logger.info(f"[Naver] '{name}' → complexNo {no} '{nm}' (naver search)")
                return no
        # 이름 검증 없이 단순 추출 (complexNo만 있는 경우) — 1개만 있을 때만 신뢰
        nos = _re.findall(r'new\.land\.naver\.com/complexes/(\d+)', r.text)
        unique = list(dict.fromkeys(nos))
        if len(unique) == 1:
            logger.info(f"[Naver] '{name}' → complexNo {unique[0]} (naver search 단일결과)")
            return unique[0]
        if unique:
            logger.info(f"[Naver] '{name}' naver search 복수결과 매칭 실패 (후보 {len(unique)}개)")
    except Exception as e:
        logger.warning(f"[Naver] 단지 검색 실패: {e}")

    logger.info(f"[Naver] '{name}' complexNo 찾기 실패")
    return None


def _parse_naver_price(s: str) -> int | None:
    """'12억 5,000' 또는 '125000' → 만원 정수"""
    import re as _re
    s = s.replace(",", "").strip()
    m = _re.match(r'^(\d+)억\s*(\d+)?$', s)
    if m:
        return int(m.group(1)) * 10000 + (int(m.group(2)) if m.group(2) else 0)
    try:
        return int(s)
    except ValueError:
        return None


async def _fetch_naver_asking(complex_no: str, main_area: int = 0) -> dict:
    """complexNo → 매매 호가 최저가 (main_area 기준, 없으면 최소가 면적)"""
    data = await _naver_get(
        f"https://new.land.naver.com/api/articles/complex/{complex_no}",
        {"realEstateType": "APT", "tradeType": "A1", "page": "1", "pageSize": "50"},
    )
    if not data:
        return {}
    articles = data.get("articleList") or data.get("articles") or []
    # 첫 매물 필드 디버깅
    if articles:
        logger.debug(f"[네이버 호가] complex {complex_no} 첫 매물 keys: {list(articles[0].keys())[:15]}")
        logger.debug(f"[네이버 호가] complex {complex_no} 첫 매물 샘플: area2={articles[0].get('area2')} exclusiveArea={articles[0].get('exclusiveArea')} dealOrWarrantPrc={articles[0].get('dealOrWarrantPrc')} price={articles[0].get('price')}")

    prices: dict[int, list[int]] = {}
    for art in articles:
        area_str  = art.get("area2") or art.get("exclusiveArea") or ""
        price_str = str(art.get("dealOrWarrantPrc") or art.get("price") or "")
        try:
            area = int(float(str(area_str).replace(",", "")))
            price = _parse_naver_price(price_str)
            if area > 0 and price and price > 0:
                prices.setdefault(area, []).append(price)
        except (ValueError, TypeError):
            continue

    if not prices:
        logger.info(f"[네이버 호가] complex {complex_no}: 매물 없음")
        return {}

    # main_area 기준 ±5㎡ 내 최저가, 없으면 전체 최저가
    result: dict = {}
    target_areas = [a for a in prices if main_area and abs(a - main_area) <= 5]
    if not target_areas:
        target_areas = list(prices.keys())  # 모든 면적 중 최저가

    all_prices = [p for a in target_areas for p in prices[a]]
    if all_prices:
        mn = min(all_prices)
        result["price_display"] = format_price(mn)
        result["price"] = mn
        # 기존 호환성 유지
        result["price_84_display"] = result["price_display"]

    logger.info(f"[네이버 호가] complex {complex_no} (main_area={main_area}): {result.get('price_display')}")
    return result


@app.get("/api/naver-asking")
async def get_naver_asking(
    key:  str = Query(...),
    name: str = Query(...),
    area: int = Query(default=0),   # main_area_key (㎡)
):
    """단지 네이버 호가 + complexNo 반환 (캐시 4시간)"""
    now = time.time()
    cached = _naver_asking_cache.get(key)
    if cached and now - cached.get("ts", 0) < NAVER_ASKING_TTL:
        return cached

    complex_no = _naver_complex_cache.get(key)
    if not complex_no:
        complex_no = await _search_naver_complex_no(name)
        if complex_no:
            _naver_complex_cache[key] = complex_no
            await _save_complex_cache(_naver_complex_cache)

    if not complex_no:
        _naver_asking_cache[key] = {"error": "not_found", "ts": now - NAVER_ASKING_TTL + 1800}
        return {"error": "not_found"}

    prices = await _fetch_naver_asking(complex_no, main_area=area)
    result = {"complex_no": complex_no, "ts": now, **prices}
    _naver_asking_cache[key] = result
    return result


def _pick_numeric_value(row: dict[str, str], *cols: str) -> str:
    for col in cols:
        value = (row.get(col) or "").strip()
        if value.isdigit():
            return value
    return ""


def _school_detail_key(district: str, school_type: str, name: str, address: str) -> str:
    return f"{district}|{school_type}|{name}|{address}"


def _district_schoolinfo_gugun_code(district: str) -> str | None:
    lawd_codes = DISTRICTS.get(district) or []
    if not lawd_codes:
        return None
    if len(lawd_codes) == 1:
        return f"{lawd_codes[0]}00000"

    common = lawd_codes[0]
    for code in lawd_codes[1:]:
        i = 0
        max_i = min(len(common), len(code))
        while i < max_i and common[i] == code[i]:
            i += 1
        common = common[:i]
    root = common.ljust(5, "0")[:5]
    return f"{root}00000"


async def _load_school_index():
    global _school_index_cache
    if _school_index_cache is not None:
        return _school_index_cache

    index_map: dict[tuple[str, str], list[str]] = {}
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(SCHOOLINFO_INDEX_URL)
        text = r.text
        section_pattern = re.compile(
            r'<h4 class="tabtt">([^<]+)</h4>\s*<ul class="link_list">(.*?)</ul>',
            re.S,
        )
        link_pattern = re.compile(
            r"searchSchul\('([^']+)'\)\" title=\"([^\"]+?) 학교정보 새창\">([^<]+)</a>"
        )
        for section_name, section_html in section_pattern.findall(text):
            school_type = _strip_html(section_name)
            if school_type not in {"중학교", "고등학교"}:
                continue
            for school_id, _, school_name in link_pattern.findall(section_html):
                key = (school_type, _strip_html(school_name))
                index_map.setdefault(key, []).append(school_id)
        logger.info(f"[학교] schoolinfo 인덱스 로드: {sum(len(v) for v in index_map.values())}개")
    except Exception as e:
        logger.error(f"school index fetch error: {e}")

    _school_index_cache = index_map
    return _school_index_cache


async def _fetch_school_meta(school_id: str) -> dict:
    if school_id in _school_meta_cache:
        return _school_meta_cache[school_id]

    meta = {"title": "", "address": ""}
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(SCHOOLINFO_DETAIL_URL, params={"SHL_IDF_CD": school_id})
        text = r.text
        title_m = re.search(r"<title>([^<]+?) 학교정보", text)
        desc_m = re.search(r'<meta name="description" content="([^"]+)"', text)
        if title_m:
            meta["title"] = _strip_html(title_m.group(1))
        if desc_m:
            desc = html_lib.unescape(desc_m.group(1))
            addr_m = re.search(r"주소\s*:\s*([^,]+)", desc)
            if addr_m:
                meta["address"] = addr_m.group(1).strip()
    except Exception as e:
        logger.warning(f"school meta fetch error ({school_id}): {e}")

    _school_meta_cache[school_id] = meta
    return meta


async def _load_school_region_index(district: str, school_type: str) -> dict[str, list[str]]:
    cache_key = f"{district}|{school_type}"
    if cache_key in _school_region_index_cache:
        return _school_region_index_cache[cache_key]

    region = DISTRICT_SIDO.get(district, "서울")
    sido_code = SCHOOLINFO_SIDO_CODE.get(region)
    gugun_code = _district_schoolinfo_gugun_code(district)
    index_map: dict[str, list[str]] = {}
    if not sido_code or not gugun_code:
        _school_region_index_cache[cache_key] = index_map
        return index_map

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                SCHOOLINFO_INDEX_URL,
                data={"SIDO_CODE": sido_code, "GUGUN_CODE": gugun_code},
            )
        text = r.text
        section_pattern = re.compile(
            r'<h4 class="tabtt">([^<]+)</h4>\s*<ul class="link_list">(.*?)</ul>',
            re.S,
        )
        link_pattern = re.compile(
            r"searchSchul\('([^']+)'\)\" title=\"([^\"]+?) 학교정보 새창\">([^<]+)</a>"
        )
        for section_name, section_html in section_pattern.findall(text):
            current_type = _strip_html(section_name)
            if current_type != school_type:
                continue
            for school_id, _, school_name in link_pattern.findall(section_html):
                clean_name = _strip_html(school_name)
                index_map.setdefault(clean_name, []).append(school_id)
        logger.info(f"[학교] schoolinfo 지역 인덱스 로드: {district} {school_type} {len(index_map)}개")
    except Exception as e:
        logger.warning(f"school region index fetch error ({district}, {school_type}): {e}")

    _school_region_index_cache[cache_key] = index_map
    return index_map


async def _resolve_schoolinfo_id(name: str, school_type: str, district: str, address: str) -> str | None:
    regional_index = await _load_school_region_index(district, school_type)
    regional_candidates = regional_index.get(name, [])
    if regional_candidates:
        if len(regional_candidates) == 1:
            return regional_candidates[0]
        candidates = regional_candidates
    else:
        candidates = []

    index_map = await _load_school_index()
    if not candidates:
        candidates = index_map.get((school_type, name), [])
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    district_alias = SCHOOLINFO_DISTRICT_ALIAS.get(district, district)
    normalized_address = _normalize_space(address)
    for school_id in candidates:
        meta = await _fetch_school_meta(school_id)
        meta_addr = meta.get("address", "")
        normalized_meta_addr = _normalize_space(meta_addr)
        if district_alias and district_alias in meta_addr:
            return school_id
        if normalized_address and normalized_meta_addr:
            if normalized_address in normalized_meta_addr or normalized_meta_addr in normalized_address:
                return school_id
    return candidates[0]


async def _fetch_school_career_detail(name: str, school_type: str, district: str, address: str) -> dict:
    cache_key = _school_detail_key(district, school_type, name, address)
    if cache_key in _school_detail_cache:
        return _school_detail_cache[cache_key]

    school_id = await _resolve_schoolinfo_id(name, school_type, district, address)
    if not school_id:
        detail = {
            "name": name,
            "type": school_type,
            "available": False,
            "message": "학교알리미 학교 식별자를 찾지 못했습니다.",
        }
        _school_detail_cache[cache_key] = detail
        return detail

    year = str(datetime.now().year)
    params = {
        "SHL_IDF_CD": school_id,
        "HG_NM": name,
        "GS_BURYU_CD": "JG040",
        "GS_HANGMOK_CD": "06",
        "JG_BURYU_CD": "JG130",
        "JG_HANGMOK_CD": "52",
        "GS_HANGMOK_NO": "13-다",
        "GS_HANGMOK_NM": "졸업생의 진로 현황",
        "JG_YEAR": year,
        "JG_YEAR2": year,
        "GS_TYPE": "Y",
        "SORT": "BR",
        "CHOSEN_JG_YEAR": year,
        "PRE_JG_YEAR": year,
        "LOAD_TYPE": "single",
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(SCHOOLINFO_CAREER_URL, data=params)
        text = r.text
        table_m = re.search(
            r'<table class="box hidden_table">.*?<thead>(.*?)</thead>.*?<tbody>(.*?)</tbody>',
            text,
            re.S,
        )
        if not table_m:
            raise ValueError("진로현황 표를 찾지 못했습니다.")

        headers = [_strip_html(x) for x in re.findall(r"<th[^>]*>(.*?)</th>", table_m.group(1), re.S)]
        values = []
        for cell in re.findall(r"<td[^>]*>(.*?)</td>", table_m.group(2), re.S):
            raw = _strip_html(cell).replace(",", "")
            try:
                values.append(int(raw or "0"))
            except ValueError:
                values.append(0)
        stats = {k: v for k, v in zip(headers, values)}
        total = sum(stats.values())

        detail = {
            "name": name,
            "type": school_type,
            "available": True,
            "schoolinfo_id": school_id,
            "schoolinfo_url": f"{SCHOOLINFO_DETAIL_URL}?SHL_IDF_CD={school_id}",
            "counts": stats,
            "total": total,
            "achievement_rate": None,
        }

        if school_type == "중학교":
            science = stats.get("[특수목적고] 과학고", 0)
            foreign = stats.get("[특수목적고] 외고국제고", 0)
            special = science + foreign
            detail["science_high_count"] = science
            detail["foreign_lang_high_count"] = foreign
            detail["special_purpose_count"] = special
            detail["special_purpose_rate"] = round((special / total) * 100, 1) if total else None
            if detail["special_purpose_rate"] is not None:
                detail["summary_text"] = f"과고 {science} · 외고 {foreign} · {detail['special_purpose_rate']}%"
            else:
                detail["summary_text"] = f"과고 {science} · 외고 {foreign}"
            detail["message"] = None
        else:
            university = stats.get("대학", 0)
            detail["university_count"] = university
            detail["junior_college_count"] = stats.get("전문대학", 0)
            detail["overseas_count"] = stats.get("국외진학", 0)
            detail["seoul_national_count"] = None
            detail["medical_school_count"] = None
            snu = _match_snu_admission(name)
            if snu:
                detail["seoul_national_early_count"] = snu["early"]
                detail["seoul_national_regular_count"] = snu["regular"]
                detail["seoul_national_count"] = snu["total"]
                detail["summary_text"] = f"수시 {snu['early']}명 + 정시 {snu['regular']}명: 합계 {snu['total']}명"
                detail["message"] = None
            else:
                detail["summary_text"] = f"대학 {university} · 취업 {stats.get('취업자', 0)}"
                detail["message"] = "서울대 진학현황 파일과 자동 매칭되지 않아 학교알리미 진로현황으로 표시합니다."

        _school_detail_cache[cache_key] = detail
        return detail
    except Exception as e:
        logger.warning(f"school career detail fetch error ({name}): {e}")
        detail = {
            "name": name,
            "type": school_type,
            "available": False,
            "message": "학교 진로현황을 불러오지 못했습니다.",
        }
        _school_detail_cache[cache_key] = detail
        return detail

@app.get("/api/schools")
async def get_schools(
    district: str = Query(...),
    school_type: str = Query(default="중학교"),  # 중학교 | 고등학교
):
    """NEIS에서 학교 목록 조회 (이름+주소) — 프론트에서 geocoding"""
    region = DISTRICT_SIDO.get(district, "서울")
    sido = SIDO_MAP.get(region, "서울특별시")
    # 경기도는 시군구명으로 필터 (용인시, 수원시 등)
    cache_key = f"{sido}_{district}_{school_type}"
    if cache_key in _school_cache:
        return {"schools": _school_cache[cache_key]}

    try:
        # 페이지네이션으로 전체 수집 후 주소에 district 포함된 것만 필터
        all_items = []
        page = 1
        page_size = 100
        key_param = f"KEY={NEIS_API_KEY}&" if NEIS_API_KEY else ""
        sido_enc = urllib.parse.quote(sido)
        type_enc = urllib.parse.quote(school_type)
        async with httpx.AsyncClient(timeout=15.0) as client:
            while True:
                url = (f"{NEIS_BASE}?{key_param}Type=json&pIndex={page}&pSize={page_size}"
                       f"&SCHUL_KND_SC_NM={type_enc}&LCTN_SC_NM={sido_enc}")
                r = await client.get(url)
                data = r.json()
                rows = data.get("schoolInfo", [])
                # rows[0]=head, rows[1]=data — 마지막 페이지는 rows[1]이 없을 수 있음
                if len(rows) < 2:
                    break
                items = rows[1].get("row") or []
                all_items.extend(items)
                if len(items) < page_size:
                    break
                page += 1

        # 주소에 district 이름 포함된 학교만 필터
        schools = [
            {
                "name": s["SCHUL_NM"],
                "type": school_type,
                "address": s.get("ORG_RDNMA") or "",
            }
            for s in all_items
            if district in (s.get("ORG_RDNMA") or "")
        ]
        _school_cache[cache_key] = schools
        logger.info(f"[학교] {district} {school_type}: {len(schools)}개 / 전체 {len(all_items)}개")
        return {"schools": schools}
    except Exception as e:
        logger.error(f"school fetch error: {e}")
        return {"schools": []}


@app.get("/api/school-detail")
async def get_school_detail(
    district: str = Query(...),
    name: str = Query(...),
    school_type: str = Query(...),
    address: str = Query(default=""),
):
    return await _fetch_school_career_detail(
        name=name.strip(),
        school_type=school_type.strip(),
        district=district.strip(),
        address=address.strip(),
    )


@app.delete("/api/cache")
async def clear_cache():
    _cache["apartments"].clear()
    _raw_period_cache.clear()
    _district_volume_cache.clear()
    _district_volume_cached_at.clear()
    _school_detail_cache.clear()
    _school_meta_cache.clear()
    return {"message": "캐시 삭제"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 9000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=(port == 9000))
