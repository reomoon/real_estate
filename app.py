from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
import httpx
from datetime import datetime
import xml.etree.ElementTree as ET
import logging
import os
import json
from dotenv import load_dotenv

# .env 파일에서 환경변수 로드
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="아파트 실거래가 지도")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ─── API 설정 ────────────────────────────────────────────────
TRADE_API_KEY = os.getenv("TRADE_API_KEY")
NAVER_MAPS_KEY = os.getenv("NAVER_MAPS_KEY")

TRADE_BASE = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
RENT_BASE  = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRentDev/getRTMSDataSvcAptRentDev"

# 지역별 법정동 코드 (값은 리스트 — 수원시처럼 여러 구를 가진 시는 복수 코드)
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
    "수원시":   ["41111","41113","41115","41117"],
    "성남시":   ["41131","41133","41135"],
    "의정부시": ["41150"],
    "안양시":   ["41171","41173"],
    "부천시":   ["41190"],
    "광명시":   ["41210"],
    "평택시":   ["41220"],
    "동두천시": ["41250"],
    "안산시":   ["41271","41273"],
    "고양시":   ["41281","41285","41287"],
    "과천시":   ["41290"],
    "구리시":   ["41310"],
    "남양주시": ["41360"],
    "오산시":   ["41370"],
    "시흥시":   ["41390"],
    "군포시":   ["41410"],
    "의왕시":   ["41430"],
    "하남시":   ["41450"],
    "용인시":   ["41461","41463","41465"],
    "파주시":   ["41480"],
    "이천시":   ["41500"],
    "안성시":   ["41550"],
    "김포시":   ["41570"],
    "화성시":   ["41590"],
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

def _save_geo_cache():
    try:
        with open(GEO_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(_geo_cache, f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"geo cache 저장 실패: {e}")

_load_geo_cache()

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
    """공통 API 호출"""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
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


@app.get("/api/apartments")
async def get_apartments(
    district: str = Query(default="강남구"),
    dong:     str = Query(default=""),   # 특정 동 필터 (빠른 초기 로드용)
    quick:    bool = Query(default=False), # True: 3개월만 조회 (빠른 초기 응답)
):
    if district not in DISTRICTS:
        return JSONResponse({"error": "존재하지 않는 구"}, status_code=400)

    # 12개월 캐시 히트 → 즉시 반환 (dong 필터 적용)
    if district in _cache["apartments"]:
        apts = _cache["apartments"][district]
        if dong:
            apts = [a for a in apts if a.get("dong") == dong]
        return {"apartments": apts, "cached": True, "full": True}

    import asyncio
    lawd_codes = DISTRICTS[district]

    # quick=True: 3개월만 먼저 조회 (초기 응답 속도 우선)
    months = 3 if quick else 12
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
        return {"apartments": [], "cached": False, "full": not quick}

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
            "district": district,
            "region": ("경기" if any(c.startswith("41") for c in lawd_codes)
                       else "인천" if any(c.startswith("28") for c in lawd_codes)
                       else "서울"),
            "price": price_val,
            "price_display": format_price(price_val),
            "main_area_label": f"{main_key}㎡",
            "built_year": apt.get("buildYear", ""),
            "latest_date": f"{y}.{mo}.{d}",
            "area_types": area_types,
            "total_trades": total_trades,
            "is_champion": False,  # 동 내 최고가 여부, 아래서 업데이트
        })

    # 동(洞)별 통계 계산: 평균가
    from collections import defaultdict
    dong_prices: dict = defaultdict(list)
    for apt in candidates:
        dong_prices[apt["dong"]].append(apt["price"])

    dong_avg: dict = {dong: round(sum(prices) / len(prices)) for dong, prices in dong_prices.items()}

    # 구(區) 전체 최고가 단 1개만 대장
    district_max_price = max((apt["price"] for apt in candidates), default=0)

    for apt in candidates:
        avg = dong_avg[apt["dong"]]
        diff = apt["price"] - avg
        apt["is_champion"] = apt["price"] == district_max_price
        apt["dong_avg"] = avg
        apt["dong_avg_display"] = format_price(avg)
        apt["dong_diff"] = diff
        apt["dong_diff_display"] = format_price(abs(diff))
        apt["dong_apt_count"] = len(dong_prices[apt["dong"]])

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
    logger.info(f"[{district}] 마커: {len(apartments)}개 (months={months}, 좌표캐시: {sum(1 for a in apartments if 'lat' in a)}개)")

    if dong:
        apartments = [a for a in apartments if a.get("dong") == dong]
    return {"apartments": apartments, "cached": False, "full": not quick}


@app.post("/api/geocache")
async def update_geocache(request: Request):
    """클라이언트가 지오코딩한 좌표를 서버에 저장"""
    data = await request.json()
    key = (data.get("key") or "").strip()
    lat, lng = data.get("lat"), data.get("lng")
    if key and lat and lng:
        _geo_cache[key] = {"lat": float(lat), "lng": float(lng)}
        _save_geo_cache()
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
    has_more = next_offset < 36  # 최대 3년치
    return {"trades": trade_history, "has_more": has_more, "next_offset": next_offset}


@app.delete("/api/cache")
async def clear_cache():
    _cache["apartments"].clear()
    _raw_period_cache.clear()
    return {"message": "캐시 삭제"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 9000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=(port == 9000))
