from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
import httpx
import asyncio
import math
from datetime import datetime
import xml.etree.ElementTree as ET
import logging
import os
from dotenv import load_dotenv

# .env 파일에서 환경변수 로드
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="아파트 실거래가 지도")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ─── API 설정 ────────────────────────────────────────────────
# .env 파일에서 읽어옴
TRADE_API_KEY = os.getenv("TRADE_API_KEY")       # 국토교통부 실거래가 API 키
NAVER_MAPS_KEY = os.getenv("NAVER_MAPS_KEY")     # 네이버 지도 API 키

TRADE_BASE = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"

# 서울 25개 자치구 코드 (법정동 코드 앞 5자리)
DISTRICTS = {
    "종로구": "11110", "중구": "11140", "용산구": "11170",
    "성동구": "11200", "광진구": "11215", "동대문구": "11230",
    "중랑구": "11260", "성북구": "11290", "강북구": "11305",
    "도봉구": "11320", "노원구": "11350", "은평구": "11380",
    "서대문구": "11410", "마포구": "11440", "양천구": "11470",
    "강서구": "11500", "구로구": "11530", "금천구": "11545",
    "영등포구": "11560", "동작구": "11590", "관악구": "11620",
    "서초구": "11650", "강남구": "11680", "송파구": "11710",
    "강동구": "11740",
}

# 구별 아파트 데이터 메모리 캐시
_cache: dict = {"apartments": {}}

# 34평형 / 25평형 전용면적 범위 (㎡)
PYEONG_34 = (79.0, 89.0)
PYEONG_25 = (57.0, 65.0)


def get_recent_deal_yms(count: int = 3) -> list:
    """최근 N개월의 거래연월 리스트 반환 (예: ['202504', '202503', '202502'])"""
    now = datetime.now()
    y, m = now.year, now.month
    result = []
    for _ in range(count):
        result.append(f"{y}{m:02d}")
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return result


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


def classify_pyeong(area: float):
    """전용면적(㎡)으로 34평/25평 여부 판별, 해당 없으면 None"""
    if PYEONG_34[0] <= area <= PYEONG_34[1]:
        return "34평"
    if PYEONG_25[0] <= area <= PYEONG_25[1]:
        return "25평"
    return None


def trade_date_key(t: dict) -> tuple:
    """거래 데이터를 날짜 기준으로 정렬하기 위한 키 함수"""
    try:
        return (int(t.get("dealYear", 0)), int(t.get("dealMonth", 0)), int(t.get("dealDay", 0)))
    except (ValueError, TypeError):
        return (0, 0, 0)


def format_price(manwon: int) -> str:
    """만원 단위 금액을 '40.5억' 형식 문자열로 변환 (1억 미만은 '9,500만')"""
    if manwon <= 0:
        return "-"
    if manwon < 10000:
        return f"{manwon:,}만"
    eok = round(manwon / 10000, 1)
    return f"{int(eok)}억" if eok == int(eok) else f"{eok}억"


# 동(洞) 좌표 캐시 (Nominatim API 중복 호출 방지)
_geocode_cache: dict = {}

async def geocode_dong(district: str, dong: str):
    """동(洞) 이름으로 위도/경도 조회 (OpenStreetMap Nominatim 사용)
    아파트 단지 정확도보다 느슨하지만, 같은 동끼리 오프셋으로 구분"""
    key = f"{district}_{dong}"
    if key in _geocode_cache:
        return _geocode_cache[key]

    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": f"서울특별시 {district} {dong}", "format": "json", "countrycodes": "kr", "limit": 1}
    headers = {"User-Agent": "realestate-apt-map/1.0"}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params, headers=headers)
            if r.status_code == 200:
                results = r.json()
                if results:
                    coords = (float(results[0]["lat"]), float(results[0]["lon"]))
                    _geocode_cache[key] = coords
                    return coords
    except Exception as e:
        logger.warning(f"geocode [{district} {dong}]: {e}")

    _geocode_cache[key] = None
    return None


async def fetch_transactions(lawd_cd: str, deal_ym: str) -> list:
    """국토교통부 API에서 특정 구/월의 아파트 실거래 내역 조회"""
    url = (
        f"{TRADE_BASE}?serviceKey={TRADE_API_KEY}"
        f"&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ym}"
        f"&numOfRows=1000&pageNo=1"
    )
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(url)
            if r.status_code == 200:
                items = xml_items(r.text)
                logger.info(f"  [{lawd_cd} {deal_ym}] {len(items)}건")
                return items
            logger.warning(f"  [{lawd_cd} {deal_ym}] HTTP {r.status_code}")
    except Exception as e:
        logger.error(f"fetch error {lawd_cd} {deal_ym}: {e}")
    return []


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    # 네이버 지도 키를 템플릿 변수로 주입
    return templates.TemplateResponse(request, "index.html", {
        "naver_maps_key": NAVER_MAPS_KEY,
    })


@app.get("/api/districts")
async def get_districts():
    """서울 25개 자치구 목록 반환"""
    return {"districts": list(DISTRICTS.keys())}


@app.get("/api/apartments")
async def get_apartments(district: str = Query(default="강남구")):
    """특정 구의 아파트 실거래 목록 반환 (25평/34평 필터, 동 단위 좌표 포함)"""
    if district not in DISTRICTS:
        return JSONResponse({"error": "존재하지 않는 구"}, status_code=400)

    # 캐시 히트 시 바로 반환
    if district in _cache["apartments"]:
        return {"apartments": _cache["apartments"][district], "cached": True}

    lawd_cd = DISTRICTS[district]
    deal_yms = get_recent_deal_yms(3)

    # 최근 3개월 거래 데이터 수집
    all_raw = []
    for ym in deal_yms:
        rows = await fetch_transactions(lawd_cd, ym)
        all_raw.extend(rows)
    logger.info(f"[{district}] 총 {len(all_raw)}건")

    if not all_raw:
        return {"apartments": [], "cached": False}

    # 단지명+동 기준으로 그룹핑 (34평/25평 구분)
    apt_map: dict = {}
    for row in all_raw:
        try:
            area = float(row.get("excluUseAr", 0))
        except (ValueError, TypeError):
            continue
        ptype = classify_pyeong(area)
        if not ptype:
            continue
        name = row.get("aptNm", "").strip()
        dong = row.get("umdNm", "").strip()
        if not name:
            continue
        key = f"{name}_{dong}"
        if key not in apt_map:
            apt_map[key] = {
                "name": name, "dong": dong, "key": key,
                "34평": [], "25평": [],
                "buildYear": row.get("buildYear", ""),
            }
        apt_map[key][ptype].append(row)

    # 거래 3건 미만 단지 제외 후 최신 거래 기준으로 정보 조립
    candidates = []
    for apt in apt_map.values():
        trades_34 = sorted(apt["34평"], key=trade_date_key, reverse=True)
        trades_25 = sorted(apt["25평"], key=trade_date_key, reverse=True)
        use_trades = trades_34 if trades_34 else trades_25
        if not use_trades or len(use_trades) < 3:
            continue

        ptype = "34평" if trades_34 else "25평"
        latest = use_trades[0]
        try:
            price_val = int(latest.get("dealAmount", "0").replace(",", ""))
        except (ValueError, TypeError):
            price_val = 0
        if price_val <= 0:
            continue

        # 최근 10건 거래 이력 구성
        trade_history = []
        for t in use_trades[:10]:
            try:
                p = int(t.get("dealAmount", "0").replace(",", ""))
            except (ValueError, TypeError):
                p = 0
            y = t.get("dealYear", "")
            mo = str(t.get("dealMonth", "")).zfill(2)
            d = str(t.get("dealDay", "")).zfill(2)
            trade_history.append({
                "date": f"{y}.{mo}.{d}",
                "price": p,
                "price_display": format_price(p),
                "floor": t.get("floor", ""),
                "area": t.get("excluUseAr", ""),
            })

        y = latest.get("dealYear", "")
        mo = str(latest.get("dealMonth", "")).zfill(2)
        d = str(latest.get("dealDay", "")).zfill(2)
        candidates.append({
            "key": apt["key"],
            "name": apt["name"],
            "dong": apt["dong"],
            "district": district,
            "price": price_val,
            "price_display": format_price(price_val),
            "pyeong_type": ptype,
            "area": float(latest.get("excluUseAr", 0)),
            "built_year": apt.get("buildYear", ""),
            "latest_date": f"{y}.{mo}.{d}",
            "trades": trade_history,
        })

    # 동(洞) 단위 지오코딩 — 5개씩 배치 처리 (Nominatim 속도 제한 대응)
    unique_dongs = list({a["dong"] for a in candidates})
    for i in range(0, len(unique_dongs), 5):
        batch_dongs = unique_dongs[i:i+5]
        await asyncio.gather(*[geocode_dong(district, d) for d in batch_dongs])
        if i + 5 < len(unique_dongs):
            await asyncio.sleep(1.0)  # Nominatim 요청 간격 준수

    # 같은 동 내 아파트끼리 겹치지 않도록 나선형 오프셋 적용 (~200m 반경)
    dong_apt_counter: dict = {}
    apartments = []
    for apt in candidates:
        coords = _geocode_cache.get(f"{district}_{apt['dong']}")
        if not coords:
            continue
        lat, lng = coords
        idx = dong_apt_counter.get(apt["dong"], 0)
        dong_apt_counter[apt["dong"]] = idx + 1
        offset_r = 0.001 * (idx // 8 + 1)
        angle = (idx % 8) * (3.14159 / 4)
        lat += offset_r * math.cos(angle)
        lng += offset_r * math.sin(angle)
        apartments.append({**apt, "lat": round(lat, 6), "lng": round(lng, 6)})

    apartments.sort(key=lambda x: x["price"], reverse=True)
    _cache["apartments"][district] = apartments
    logger.info(f"[{district}] 마커: {len(apartments)}개")
    return {"apartments": apartments, "cached": False}


@app.delete("/api/cache")
async def clear_cache():
    """메모리 캐시 전체 삭제"""
    _cache["apartments"].clear()
    return {"message": "캐시 삭제"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=9000, reload=True)
