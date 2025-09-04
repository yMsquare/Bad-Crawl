#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, csv, html, json, re, sys, time, random
from datetime import datetime
from typing import Dict, Any, List, Optional
import requests

SEARCH_API = "https://api.bilibili.com/x/web-interface/search/type"

MATCH_HINTS = ["比赛", "录像", "集锦", "全场", "决赛", "半决赛", "世锦赛", "奥运", "汤姆斯杯", "苏迪曼杯"]

UA_POOL = [
    # 一些常见桌面 UA，可自行增补
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

def clean_html(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = re.sub(r"<.*?>", "", text)
    return html.unescape(text).strip()

def to_datetime(ts: int) -> str:
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""

def ensure_list_tags(tag_field: Any) -> List[str]:
    if tag_field is None:
        return []
    if isinstance(tag_field, list):
        return [str(t).strip() for t in tag_field if str(t).strip()]
    s = str(tag_field).strip()
    if not s:
        return []
    return [p for p in re.split(r"[,\s]+", s) if p]

def looks_like_match(title: str, tags: List[str]) -> bool:
    return any(k in title for k in MATCH_HINTS) or any(k in "".join(tags) for k in MATCH_HINTS)

def has_shiyuqi(title: str, tags: List[str], author: str, desc: str) -> bool:
    key = "石宇奇"
    return key in " ".join([title, " ".join(tags), author, desc])

def make_session(cookie_str: Optional[str]) -> requests.Session:
    s = requests.Session()
    headers = {
        "User-Agent": random.choice(UA_POOL),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://www.bilibili.com/",
        "Origin": "https://www.bilibili.com",
        "Connection": "keep-alive",
    }
    if cookie_str:
        headers["Cookie"] = cookie_str.strip()
    s.headers.update(headers)
    # 合理的重试策略（网络波动/偶发 5xx）
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=0)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

def fetch_page(sess: requests.Session, keyword: str, page: int, page_size: int = 50, order: str = "pubdate") -> Dict[str, Any]:
    params = {
        "search_type": "video",
        "keyword": keyword,
        "page": page,
        "page_size": page_size,
        "order": order,
        # 加一点无害参数扰动，避免缓存特征过于固定
        "t": int(time.time() * 1000)
    }
    backoff = 2.0
    for attempt in range(6):
        r = sess.get(SEARCH_API, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        # 412/429/418 => 退避并随机化 UA
        if r.status_code in (412, 429, 418):
            print(f"[WARN] HTTP {r.status_code} on page {page}, backoff {backoff:.1f}s", file=sys.stderr)
            # 切换 UA，防止特征固定
            sess.headers["User-Agent"] = random.choice(UA_POOL)
            time.sleep(backoff + random.uniform(0, 0.8))
            backoff = min(backoff * 2.0, 16.0)
            continue
        r.raise_for_status()
    raise RuntimeError(f"Too many 412/429/418 on page {page}")

def parse_results(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(j, dict):
        return []
    data = j.get("data") or j.get("result")
    if not data:
        return []
    results = data.get("result") if isinstance(data, dict) else data
    if not isinstance(results, list):
        return []
    rows = []
    for it in results:
        title = clean_html(it.get("title", ""))
        author = it.get("author") or it.get("uname") or ""
        bvid = it.get("bvid") or ""
        aid = it.get("aid") or it.get("id") or ""
        arcurl = it.get("arcurl") or (f"https://www.bilibili.com/video/{bvid}" if bvid else "")
        pubdate = to_datetime(it.get("pubdate", 0))
        duration = it.get("duration", "")
        play = it.get("play", it.get("playcnt", ""))
        danmaku = it.get("video_review", it.get("dm", ""))
        desc = clean_html(it.get("description", it.get("desc", "")))
        tags = ensure_list_tags(it.get("tag"))
        rows.append({
            "title": title, "bvid": bvid, "aid": aid, "url": arcurl,
            "author": author, "pubdate": pubdate, "duration": duration,
            "play": play, "danmaku": danmaku, "tags": "|".join(tags), "desc": desc,
        })
    return rows

def crawl(keyword: str, max_pages: int, delay: float, cookie: Optional[str], require_match_hint: bool) -> List[Dict[str, Any]]:
    sess = make_session(cookie)
    all_rows: List[Dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        try:
            j = fetch_page(sess, keyword=keyword, page=page)
        except Exception as e:
            print(f"[WARN] request error on page {page}: {e}", file=sys.stderr)
            break
        if j.get("code", -1) != 0:
            print(f"[WARN] api code={j.get('code')} msg={j.get('message')}", file=sys.stderr)
            if j.get("code") == -412:
                print("[HINT] 仍被风控：请粘贴浏览器 Cookie 到 --cookie，或增大 --delay，或减少 --max-pages。", file=sys.stderr)
            break
        rows = parse_results(j)
        if not rows:
            print(f"[INFO] no results on page {page}, stop.")
            break
        for r in rows:
            title, tags, author, desc = r["title"], r["tags"].split("|") if r["tags"] else [], r["author"], r["desc"]
            if not has_shiyuqi(title, tags, author, desc):
                continue
            if require_match_hint and not looks_like_match(title, tags):
                continue
            all_rows.append(r)
        print(f"[INFO] page {page}: got {len(rows)} items, kept {len(all_rows)} total.")
        time.sleep(max(delay, 1.2) + random.uniform(0, 0.6))  # 最少 1.2s + 抖动
    # 去重
    uniq = {}
    for r in all_rows:
        key = r["bvid"] or r["aid"] or r["url"]
        uniq[key] = r
    return list(uniq.values())

def save_csv(rows: List[Dict[str, Any]], out: str):
    fields = ["title", "bvid", "aid", "url", "author", "pubdate", "duration", "play", "danmaku", "tags", "desc"]
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"[OK] saved {len(rows)} rows -> {out}")

def main():
    ap = argparse.ArgumentParser(description="Bilibili crawler anti-412 enhanced (Shi Yuqi matches).")
    ap.add_argument("--keyword", default="石宇奇 比赛 录像", help="搜索关键词")
    ap.add_argument("--max-pages", type=int, default=40, help="最多抓取页数")
    ap.add_argument("--out", default="shiyuqi_matches.csv", help="输出 CSV")
    ap.add_argument("--delay", type=float, default=1.3, help="每页间隔（秒），建议≥1.2")
    ap.add_argument("--cookie", default=None, help="从浏览器复制的 Cookie 字符串")
    ap.add_argument("--no-match-filter", action="store_true", help="关闭比赛相关关键词过滤")
    args = ap.parse_args()

    rows = crawl(
        keyword=args.keyword,
        max_pages=args.max_pages,
        delay=args.delay,
        cookie=args.cookie,
        require_match_hint=not args.no_match_filter,
    )
    save_csv(rows, args.out)

if __name__ == "__main__":
    main()