# -*- coding: utf-8 -*-
"""
CREW LIVE 기록 수집기 (여러 대회 · 여러 코스 · 여러 크루)

- 관리자 화면이 저장한 config.json 을 읽어, 오늘 열리는 대회의 모든 배번을 마이리저트에서 읽습니다.
- 마이리저트 선수 페이지의 구간표(구간명 / 통과시간 / 구간기록 / 누적기록)를 해석합니다.
- 결과는 대회별로 live/<대회ID>.json 에 저장됩니다. (지난 대회 파일은 지우지 않음 → 명예의 전당)

환경변수
  CONFIG_PATH  설정 파일 경로 (기본 config.json)
  OUT_DIR      결과 폴더 (기본 live)
  DEBUG_DIR    사이트 구조 확인용 샘플 저장 폴더 (비우면 저장 안 함)
  MODE         auto = 오늘 대회 중 수집 시간대인 것만 / once = 오늘(없으면 다가오는) 대회 1회 수집
  FORCE        1 이면 auto 모드에서도 시간대와 상관없이 오늘 대회를 수집
  RACE         특정 대회 ID 만 수집 (1회 테스트용)
  WORKERS      동시 접속 수 (기본 4 · 마이리저트에 한꺼번에 몰리지 않게 낮게)
명령
  python tracker.py          수집 실행 (종료코드 3 = 마이리저트 접속 제한 감지)
  python tracker.py --plan   오늘 수집 시작/종료 시각 출력 "START END" (없으면 "0 0")
"""
import html as htmlmod
import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))
CONFIG_PATH = os.environ.get("CONFIG_PATH", "config.json")
OUT_DIR = os.environ.get("OUT_DIR", "live")
DEBUG_DIR = os.environ.get("DEBUG_DIR", "")
MODE = os.environ.get("MODE", "auto").strip() or "auto"
FORCE = os.environ.get("FORCE", "") == "1"
ONLY_RACE = os.environ.get("RACE", "").strip()
WORKERS = max(1, min(24, int(os.environ.get("WORKERS", "4") or 4)))
FINISHED_RECHECK_SEC = 15 * 60   # 완주한 사람은 기록이 더 안 바뀌니 15분에 한 번만 다시 확인

HALF_KM = 21.0975
PAGE_URL = "https://myresult.co.kr/{event_id}/{bib}"
API_URLS = ["https://myresult.co.kr/api/event/{event_id}/player/{bib}"]
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/129.0 Safari/537.36"),
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9",
}
TIME_RE = re.compile(r"^(\d{1,2}):([0-5]\d)(?::([0-5]\d))?(?:\.\d+)?$")
_local = threading.local()


# ───────────────────────── 공통 ─────────────────────────
def session():
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update(HEADERS)
        _local.s = s
    return s


def is_valid_time(value):
    """'09:12:33', '1:45:20', '45:12' 같은 시간만 인정. '?', '-', '00:00:00' 은 무효."""
    if value is None:
        return False
    m = TIME_RE.match(str(value).strip())
    if not m:
        return False
    return any(int(g) for g in m.groups() if g)


def parse_hhmm(s, default=None):
    m = re.match(r"^\s*(\d{1,2}):(\d{2})", str(s or ""))
    if not m:
        return default
    return int(m.group(1)), int(m.group(2))


def race_epoch(race, hhmm):
    y, mo, d = [int(x) for x in race["date"].split("-")]
    h, mi = hhmm
    return int(datetime(y, mo, d, h, mi, tzinfo=KST).timestamp())


def race_window(race):
    """수집 시간대 (시작, 종료) epoch. 관리자가 정한 값이 없으면 출발 40분 전 ~ 마지막 출발 7시간 후."""
    starts = []
    for c in race.get("courses", []):
        st = parse_hhmm(c.get("start"))
        if st:
            starts.append(st)
        for g in c.get("groups", []) or []:
            gt = parse_hhmm(g.get("start"))
            if gt:
                starts.append(gt)
    if not starts:
        starts = [(9, 0)]
    first, last = min(starts), max(starts)
    col = race.get("collect") or {}
    s = parse_hhmm(col.get("start"))
    e = parse_hhmm(col.get("end"))
    start = race_epoch(race, s) if s else race_epoch(race, first) - 40 * 60
    end = race_epoch(race, e) if e else race_epoch(race, last) + 7 * 3600
    if end <= start:
        end = start + 8 * 3600
    return start, end


def load_config():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        return cfg if isinstance(cfg, dict) else {}
    except Exception as e:
        print(f"[WARN] 설정 파일을 읽지 못함: {e}", file=sys.stderr)
        return {}


def valid_races(cfg):
    out = []
    for r in cfg.get("races", []) or []:
        if not isinstance(r, dict) or r.get("hidden"):
            continue
        if not r.get("id") or not re.match(r"^\d{4}-\d{2}-\d{2}$", str(r.get("date", ""))):
            continue
        if not str(r.get("myresultId", "")).strip():
            continue
        out.append(r)
    return out


def races_to_collect(cfg, now):
    races = valid_races(cfg)
    if ONLY_RACE:
        return [r for r in races if r["id"] == ONLY_RACE]
    today = datetime.fromtimestamp(now, KST).strftime("%Y-%m-%d")
    todays = [r for r in races if r["date"] == today]
    if MODE == "once":
        if todays:
            return todays
        upcoming = sorted([r for r in races if r["date"] >= today], key=lambda r: r["date"])
        return upcoming[:3]
    if FORCE:
        return todays
    picked = []
    for r in todays:
        s, e = race_window(r)
        if s <= now <= e:
            picked.append(r)
    return picked


def plan(cfg, now):
    today = datetime.fromtimestamp(now, KST).strftime("%Y-%m-%d")
    wins = [race_window(r) for r in valid_races(cfg) if r["date"] == today]
    if not wins:
        return 0, 0
    return min(w[0] for w in wins), max(w[1] for w in wins)


# ───────────────────────── 페이지 해석 ─────────────────────────
def label_to_km(label, course_km, turn_km):
    """구간 이름 → km (코스마다 다름). 모르는 이름은 None."""
    if label is None:
        return None
    s = re.sub(r"\s+", "", str(label)).upper()
    if not s or len(s) > 12:
        return None
    if s in ("출발", "START"):
        return 0.0
    if s in ("도착", "FINISH", "골인", "완주", "결승"):
        return float(course_km)
    if s in ("반환점", "반환", "TURN"):
        return float(turn_km) if turn_km else None
    if s in ("HALF", "하프"):
        return HALF_KM if HALF_KM < course_km - 0.001 else (float(course_km) if abs(HALF_KM - course_km) < 0.01 else None)
    m = re.match(r"^(\d{1,2}(?:\.\d+)?)(K|KM)$", s)
    if m:
        v = float(m.group(1))
        if 0 < v < course_km - 0.001:
            return v
    return None


def html_to_lines(text):
    t = re.sub(r"(?is)<(script|style|noscript|svg)\b[^>]*>.*?</\1>", " ", text)
    t = re.sub(r"(?s)<!--.*?-->", " ", t)
    t = re.sub(r"<[^>]+>", "\n", t)
    t = htmlmod.unescape(t)
    return [ln.strip() for ln in t.split("\n") if ln.strip()]


NAME_STOP = {"출발", "도착", "반환점", "남자", "여자", "남성", "여성", "풀코스", "하프", "기록", "구간명", "통과시간",
             "구간기록", "누적기록", "순위", "완주", "개인기록", "기록증", "마라톤", "코스", "참가자"}


def find_name_lines(lines, bib):
    """페이지 글줄에서 배번 근처의 한글 이름(2~5글자)을 찾음. 못 찾으면 None"""
    bib = str(bib)
    pat = re.compile(r"(^|[^0-9])" + re.escape(bib) + r"([^0-9]|$)")
    for i, ln in enumerate(lines[:200]):
        if not pat.search(ln):
            continue
        for j in (i - 1, i - 2, i + 1, i - 3, i + 2):
            if 0 <= j < len(lines):
                c = lines[j].strip()
                m = re.fullmatch(r"([가-힣]{2,5})(\s*님)?", c)
                if m and m.group(1) not in NAME_STOP:
                    return m.group(1)
        # 같은 줄에 "홍길동 #1234" 처럼 붙은 경우
        m = re.search(r"([가-힣]{2,5})\s*[|·/]?\s*#?\s*" + re.escape(bib), ln)
        if m and m.group(1) not in NAME_STOP:
            return m.group(1)
    return None


def find_name_json(data):
    found = []

    def walk(o):
        if found:
            return
        if isinstance(o, dict):
            for k in ("name", "playerName", "runnerName", "userName", "korName", "nameKo", "player_name"):
                v = o.get(k)
                if isinstance(v, str) and re.fullmatch(r"[가-힣]{2,5}", v.strip()) and v.strip() not in NAME_STOP:
                    found.append(v.strip())
                    return
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for x in o:
                walk(x)

    walk(data)
    return found[0] if found else None


def finish(best, splits, start_time):
    if best is not None:
        best["start_time"] = start_time
        best["splits"] = sorted(splits.values(), key=lambda s: s[0])
    return best


def parse_html(text, course_km, turn_km):
    """반환: (가장 먼 통과 구간 or None, 찾은 구간 이름 수, 줄 목록)"""
    lines = html_to_lines(text)
    best, labels_found, start_time, splits = None, 0, None, {}
    for i, line in enumerate(lines):
        km = label_to_km(line, course_km, turn_km)
        if km is None:
            continue
        nxt = lines[i + 1:i + 4]
        if not nxt:
            continue
        labels_found += 1
        if not is_valid_time(nxt[0]):
            continue  # '?' → 아직 통과 전
        if km == 0.0:
            start_time = nxt[0]
        net = nxt[2] if len(nxt) >= 3 and is_valid_time(nxt[2]) else None
        if km > 0:
            splits[km] = [round(km, 4), net]
        if best is None or km >= best["km"]:
            best = {"km": km, "point": line, "pass_time": nxt[0], "net_time": net}
    return finish(best, splits, start_time), labels_found, lines


def parse_json(data, course_km, turn_km):
    state = {"best": None, "start": None}
    splits = {}

    def consider(label, t, net=None):
        km = label_to_km(label, course_km, turn_km)
        if km is None or not is_valid_time(t):
            return
        t = str(t).strip()
        net = str(net).strip() if is_valid_time(net) else None
        if km == 0.0:
            state["start"] = t
        if km > 0:
            splits[km] = [round(km, 4), net]
        if state["best"] is None or km >= state["best"]["km"]:
            state["best"] = {"km": km, "point": str(label), "pass_time": t, "net_time": net}

    def pick(o, keys):
        for k in keys:
            v = o.get(k)
            if isinstance(v, (str, int, float)) and str(v).strip():
                return v
        return None

    def walk(o):
        if isinstance(o, dict):
            label = pick(o, ("point", "pointName", "section", "sectionName", "name", "title", "label"))
            t = pick(o, ("passTime", "time", "clockTime", "record", "netTime", "chipTime"))
            net = pick(o, ("netTime", "cumulative", "totalTime", "accTime", "chipTime"))
            if label is not None and t is not None:
                consider(label, t, net)
            for k, v in o.items():
                if isinstance(v, (dict, list)):
                    walk(v)
                else:
                    consider(k, v)
        elif isinstance(o, list):
            for x in o:
                walk(x)

    walk(data)
    return finish(state["best"], splits, state["start"])


def fetch_runner(event_id, bib, course_km, turn_km):
    """ok=False 면 '이번엔 못 읽었음'(이전 값 유지). limited=True 면 접속 제한(429/403) 의심."""
    last_err, limited = "", False
    url = PAGE_URL.format(event_id=event_id, bib=bib)
    time.sleep(random.uniform(0.1, 0.4))   # 요청 사이에 살짝 간격 (사람이 조회하는 속도에 가깝게)
    for _ in range(2):
        try:
            res = session().get(url, timeout=8)
            if res.status_code == 200:
                text = res.text
                m = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', text, re.S)
                if m:
                    try:
                        data = json.loads(m.group(1))
                        best = parse_json(data, course_km, turn_km)
                        if best:
                            return {"ok": True, "best": best, "raw": text, "name": find_name_json(data)}
                    except Exception:
                        pass
                best, labels_found, lines = parse_html(text, course_km, turn_km)
                if labels_found > 0:
                    return {"ok": True, "best": best, "raw": text, "name": find_name_lines(lines, bib)}
                last_err = "구간표를 찾지 못함(배번이 없거나 사이트 구조 변경)"
                break
            if res.status_code in (403, 429):
                limited = True
            last_err = f"HTTP {res.status_code}"
        except Exception as e:
            last_err = str(e)[:120]
        time.sleep(1.0)
    for tpl in API_URLS:
        try:
            res = session().get(tpl.format(event_id=event_id, bib=bib), timeout=6)
            if res.status_code == 200:
                data = res.json()
                return {"ok": True, "best": parse_json(data, course_km, turn_km), "raw": res.text, "name": find_name_json(data)}
        except Exception:
            pass
    return {"ok": False, "error": last_err, "limited": limited}


# ───────────────────────── 대회 1개 수집 ─────────────────────────
def self_bibs(cfg, rid):
    """크루원이 앱에서 직접 등록한 배번 (Supabase odj_bibs). 못 읽으면 직전에 받아둔 목록을 씀."""
    st = (cfg or {}).get("settings") or {}
    if not st.get("selfReg"):
        return []
    b = st.get("board") or {}
    m = re.match(r"^(https://[\w.-]+)", str(b.get("url") or "").strip())
    key = str(b.get("key") or "").strip()
    cache = os.path.join(OUT_DIR, f"{rid}.self.json")
    if m and key:
        h = {"apikey": key}
        if key.startswith("eyJ"):
            h["Authorization"] = f"Bearer {key}"
        try:
            res = requests.get(f"{m.group(1)}/rest/v1/odj_bibs",
                               params={"select": "crew_id,bib,course_id", "race_id": f"eq.{rid}", "limit": "5000"},
                               headers=h, timeout=8)
            if res.status_code == 200 and isinstance(res.json(), list):
                rows = [x for x in res.json() if isinstance(x, dict) and x.get("bib")]
                os.makedirs(OUT_DIR, exist_ok=True)
                with open(cache, "w", encoding="utf-8") as f:
                    json.dump(rows, f, ensure_ascii=False, separators=(",", ":"))
                if rows:
                    print(f"[{rid}] 크루원이 직접 등록한 배번 {len(rows)}개")
                return rows
            print(f"[WARN] {rid} 직접 등록 배번 읽기 실패 HTTP {res.status_code} (직전 목록 사용)")
        except Exception as e:
            print(f"[WARN] {rid} 직접 등록 배번 읽기 실패: {str(e)[:80]} (직전 목록 사용)")
    try:
        with open(cache, "r", encoding="utf-8") as f:
            rows = json.load(f)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def race_bibs(race, extra=None):
    """배번 → (코스 km, 반환점 km). 같은 배번이 여러 크루에 있어도 한 번만 수집."""
    courses = {c.get("id"): c for c in race.get("courses", []) or [] if isinstance(c, dict)}
    first = next(iter(courses.values()), None)
    out = {}
    seen = set()
    entries = [(str(rn.get("b", "")).strip(), rn.get("c"))
               for crew in race.get("crews", []) or [] for rn in (crew.get("runners", []) or []) if isinstance(rn, dict)]
    entries += [(str(x.get("bib", "")).strip(), x.get("course_id")) for x in (extra or []) if isinstance(x, dict)]
    for bib, cid in entries:
        if not bib or bib.upper() in seen:
            continue
        seen.add(bib.upper())
        c = courses.get(cid) or first
        if not c:
            continue
        try:
            km = float(c.get("km") or 42.195)
        except (TypeError, ValueError):
            km = 42.195
        turn = c.get("turnKm")
        try:
            turn = float(turn) if turn not in (None, "") else None
        except (TypeError, ValueError):
            turn = None
        out[bib] = (km, turn)
    return out


def load_prev(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def collect_race(race, now, cfg=None):
    rid = race["id"]
    event_id = str(race["myresultId"]).strip()
    bibs = race_bibs(race, self_bibs(cfg, rid))
    path = os.path.join(OUT_DIR, f"{rid}.json")
    prev = load_prev(path)
    prev_r = prev.get("r") if isinstance(prev.get("r"), dict) else {}
    overrides = race.get("overrides") if isinstance(race.get("overrides"), dict) else {}

    # 완주해서 최근 15분 안에 확인한 사람은 이번엔 건너뜀 (이전 기록 그대로 유지)
    def resting(b, km):
        o = prev_r.get(b)
        if not isinstance(o, dict) or o.get("ov"):
            return False
        try:
            done = float(o.get("km") or 0) >= km - 0.01 and bool(o.get("nt"))
        except (TypeError, ValueError):
            return False
        return done and now - float(o.get("ck") or 0) < FINISHED_RECHECK_SEC
    skip = {b for b, (km, _t) in bibs.items() if resting(b, km)}
    todo = [b for b in bibs if b not in skip]
    random.shuffle(todo)
    if skip:
        print(f"[{rid}] 완주자 {len(skip)}명은 이번 회차 건너뜀 · 조회 {len(todo)}명")

    results = {b: {"ok": True, "skipped": True} for b in skip}
    if todo:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {b: ex.submit(fetch_runner, event_id, b, *bibs[b]) for b in todo}
            for b, f in futs.items():
                try:
                    results[b] = f.result()
                except Exception as e:
                    results[b] = {"ok": False, "error": str(e)[:120]}

    out, ok_count, errors, limited, sample = {}, 0, [], 0, None
    for bib, (course_km, _turn) in bibs.items():
        old = prev_r.get(bib) if isinstance(prev_r.get(bib), dict) else {}
        try:
            # 수동 보정이 있었던 기록은 '실제' 거리(rk)를 기준으로 이어감
            old_km = float((old.get("rk") if old.get("ov") else old.get("km")) or 0)
        except (TypeError, ValueError):
            old_km = 0.0
        if old.get("ov"):
            old = {k: v for k, v in old.items() if k not in ("pt", "st", "ov", "rk")}
        rec = {
            "km": old_km, "st": 1 if (old.get("st") or old_km > 0) else 0,
            "pt": old.get("pt"), "ps": old.get("ps"), "nt": old.get("nt"), "s0": old.get("s0"),
            "sp": old.get("sp") if isinstance(old.get("sp"), list) else [],
            "e": int(old.get("e") or now),
            "nm": old.get("nm"), "ck": old.get("ck"),
        }
        res = results.get(bib, {})
        if res.get("ok") and res.get("skipped"):
            ok_count += 1
        elif res.get("ok"):
            ok_count += 1
            if rec["km"] >= course_km - 0.01 or (res.get("best") or {}).get("km", 0) >= course_km - 0.01:
                rec["ck"] = int(now)
            if res.get("name"):
                rec["nm"] = res["name"]
            if sample is None:
                sample = (bib, res)
            best = res.get("best")
            if best and best["km"] >= old_km:
                km = best["km"]
                hours = max(0.0, now - rec["e"]) / 3600
                allowed = 10 + hours * 25
                if old_km > 0 and km - old_km > allowed:
                    print(f"[ALERT] {rid}/{bib} 비정상 점프 {old_km}→{km} 무시")
                else:
                    if km > old_km:
                        rec["e"] = int(now)
                    rec.update(km=km, st=1, pt=best["point"], ps=best["pass_time"],
                               nt=best["net_time"] or rec["nt"],
                               s0=best.get("start_time") or rec["s0"],
                               sp=best.get("splits") or rec["sp"])
        elif bib not in overrides:
            errors.append(bib)
            if res.get("limited"):
                limited += 1
        rec["km"] = round(min(rec["km"], course_km), 4)
        # 수동 보정: 실제 기록보다 멀리 있을 때만 화면에 보정 거리로 표시 (실제 기록이 따라잡으면 실제 우선)
        try:
            ov_km = round(min(float(overrides[bib]), course_km), 4) if bib in overrides else None
        except (TypeError, ValueError):
            ov_km = None
        if ov_km is not None and ov_km > rec["km"]:
            rec.update(rk=rec["km"], ov=1, km=ov_km, st=1, pt="수동 보정")
        out[bib] = {k: v for k, v in rec.items() if v not in (None, [], "")} | {"km": rec["km"]}

    kst = datetime.fromtimestamp(now, KST).strftime("%H:%M:%S")
    payload = {
        "v": 2, "race": rid,
        "updated_at": kst if ok_count > 0 else prev.get("updated_at", "--:--:--"),
        "updated_epoch": int(now) if ok_count > 0 else prev.get("updated_epoch", 0),
        "checked_at": kst, "checked_epoch": int(now),
        "ok": ok_count, "total": len(bibs), "errors": errors[:300],
        "r": out,
    }
    os.makedirs(OUT_DIR, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)

    if DEBUG_DIR and sample:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        b, res = sample
        with open(os.path.join(DEBUG_DIR, f"{rid}.txt"), "w", encoding="utf-8") as f:
            f.write(f"checked_at={kst} race={rid} event={event_id} bib={b}\n")
            f.write(f"parsed={json.dumps(res.get('best'), ensure_ascii=False)}\n")
            f.write("-" * 40 + "\n")
            f.write("\n".join(html_to_lines(res.get("raw", ""))[:250]))

    print(f"[DONE] {kst} {race.get('name', rid)} 성공 {ok_count}/{len(bibs)} 실패 {len(errors)}")
    return limited, len(bibs)


def main():
    cfg = load_config()
    now = time.time()
    if len(sys.argv) > 1 and sys.argv[1] == "--plan":
        s, e = plan(cfg, now)
        print(f"{s} {e}")
        return 0
    races = races_to_collect(cfg, now)
    if not races:
        print("[INFO] 지금 수집할 대회가 없습니다.")
        return 0
    limited_total, bib_total = 0, 0
    for r in races:
        try:
            lim, n = collect_race(r, now, cfg)
            limited_total += lim
            bib_total += n
        except Exception as e:  # 한 대회 오류가 다른 대회를 막지 않게
            print(f"[ERROR] {r.get('id')} 수집 오류: {e}")
    if bib_total and limited_total >= max(5, bib_total * 0.3):
        print(f"[WARN] 접속 제한 의심 ({limited_total}건)")
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
