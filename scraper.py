"""
今天看啥 (jintiankansha) 专栏文章抓取器

两阶段解耦架构:
  fetch  — 从今天看啥 API 获取文章列表，存入队列文件（快，仅元数据）
  scrape — 从队列文件读取待处理文章，爬取微信公众号正文 + 下载图片（慢）

两个命令可同时运行：fetch 不断拉取新数据，scrape 同时消费队列爬取正文。

存储格式:
  {slug}_queue.jsonl  — 待爬取队列（API 元数据）
  {slug}.jsonl        — 完整数据（含正文 HTML + 本地图片路径）
  images/{slug}/      — 本地图片目录
"""

import hashlib
import json
import logging
import mimetypes
import os
import random
import re
import sys
import time
from urllib.parse import urlparse

import requests
from datetime import datetime
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# 是否使用数据库模式（命令行加 --db 启用）
USE_DB = "--db" in sys.argv
if "--db" in sys.argv:
    sys.argv.remove("--db")

# ============ 专栏配置 ============
# slug        : 今天看啥的专栏标识
# name        : 公众号名称（写入 sources 表，方便展示）
# pages_per_run: 每次运行最多消耗的 API 翻页次数（额度保护）
SOURCES = {
    "pJMG8ZXFLd": {"name": "机器之心", "pages_per_run": 100},
    "BmcmLZE1td": {"name": "智东西", "pages_per_run": 300},
    "fDZT3Q49Bk": {"name": "36氪", "pages_per_run": 100},
    "SZa7lGats4": {"name": "Andy730", "pages_per_run": 300},
    "tiKZRAcl3D": {"name": "新智元", "pages_per_run": 300},
    "8LA3hF4EoQ": {"name": "量子位", "pages_per_run": 100},
    "SaY55L9Ftq": {"name": "InfoQ", "pages_per_run": 300},
    "Jw0FKj6ccg": {"name": "赛博禅心", "pages_per_run": 300},
    "JihTVjLM5w": {"name": "芯东西", "pages_per_run": 300},
    "euZCfLlKpL": {"name": "数字生命卡兹克", "pages_per_run": 300},
    "fso2l9R6W4": {"name": "全球半导体观察", "pages_per_run": 300},
    "nGqairAU3Z": {"name": "晚点LatePost", "pages_per_run": 300},

    # "another_slug": {"name": "另一公众号", "pages_per_run": 5},
}
# 单 slug 兼容旧命令（取第一个，或从 .env 指定）
SLUG = os.getenv("JTKSHA_SLUG", next(iter(SOURCES)))

USER = os.getenv("JTKSHA_USER", "")
TOKEN = os.getenv("JTKSHA_TOKEN", "")
API_URL = "http://www.jintiankansha.me/api3/query/adv/get_topics_by_one_column"
PAGE_SIZE = 30          # 每页条数，最大30
API_INTERVAL = 3        # API 翻页间隔（秒）
SCRAPE_MIN_INTERVAL = 1
SCRAPE_MAX_INTERVAL = 3
IMG_DOWNLOAD_INTERVAL = 0.5
MAX_CONSECUTIVE_FAILURES = 5
OUTPUT_DIR = "data"
# =================================

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

IMG_HEADERS = {
    "User-Agent": HEADERS["User-Agent"],
    "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
    "Referer": "https://mp.weixin.qq.com/",
}

# logging 配置
log = logging.getLogger("scraper")
log.setLevel(logging.DEBUG)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
))
log.addHandler(_handler)


class APILimitReached(Exception):
    """API 调用次数耗尽"""


class ScrapeFatalError(Exception):
    """爬取连续失败过多，主动中止"""


def article_id(original_url):
    """根据 original_url 生成短 ID 作为文件夹名"""
    return hashlib.md5(original_url.encode()).hexdigest()[:10]


def get_output_path(slug):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return os.path.join(OUTPUT_DIR, f"{slug}.jsonl")


def get_queue_path(slug):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return os.path.join(OUTPUT_DIR, f"{slug}_queue.jsonl")


def get_progress_path(slug):
    return os.path.join(OUTPUT_DIR, f"{slug}_progress.json")


def get_image_dir(slug, art_id):
    """返回某篇文章的图片存储目录"""
    d = os.path.join(OUTPUT_DIR, "images", slug, art_id)
    os.makedirs(d, exist_ok=True)
    return d


def load_existing_urls(filepath):
    """读取已保存的 original_url 集合，用于去重"""
    urls = set()
    if not os.path.exists(filepath):
        return urls
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                url = record.get("article", {}).get("original_url")
                if url:
                    urls.add(url)
            except json.JSONDecodeError:
                continue
    return urls


def load_queue_urls(queue_path):
    """读取队列文件中所有 original_url，用于去重"""
    urls = set()
    if not os.path.exists(queue_path):
        return urls
    with open(queue_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                url = item.get("original_url")
                if url:
                    urls.add(url)
            except json.JSONDecodeError:
                continue
    return urls


def load_queue_items(queue_path):
    """读取队列文件中所有条目"""
    items = []
    if not os.path.exists(queue_path):
        return items
    with open(queue_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return items


def save_queue_items(queue_path, items):
    """将条目列表写回队列文件（覆盖）"""
    with open(queue_path, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def remove_from_queue(queue_path, url):
    """从队列文件中移除指定 URL 的条目。每次重新读取文件，不会丢失 fetch 新追加的数据。"""
    items = load_queue_items(queue_path)
    items = [it for it in items if it.get("original_url") != url]
    save_queue_items(queue_path, items)


def append_queue_items(queue_path, items):
    """追加条目到队列文件"""
    with open(queue_path, "a", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


# ========== 进度管理 ==========

def load_progress(slug):
    path = get_progress_path(slug)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "slug": slug,
        "last_completed_page": 0,
        "next_page": 1,
        "total_articles": 0,
        "last_updated": "",
        "stop_reason": "",
        "pages": {},
    }


def save_progress(progress):
    """保存进度到文件"""
    progress["last_updated"] = datetime.now().isoformat()
    path = get_progress_path(progress["slug"])
    with open(path, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def update_page_progress(progress, page, status, new_count, skip_count):
    """更新某一页的进度"""
    progress["pages"][str(page)] = {
        "status": status,
        "new": new_count,
        "skip": skip_count,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    progress["total_articles"] = sum(
        p.get("new", 0) + p.get("skip", 0) for p in progress["pages"].values()
    )
    if status == "done":
        progress["last_completed_page"] = max(progress["last_completed_page"], page)
        progress["next_page"] = progress["last_completed_page"] + 1


def print_progress_summary(progress):
    """打印进度摘要"""
    log.info(f"专栏: {progress['slug']}")
    log.info(f"已完成到第 {progress['last_completed_page']} 页，下次从第 {progress['next_page']} 页开始")
    log.info(f"已处理文章总数: {progress['total_articles']}")
    if progress.get("stop_reason"):
        log.info(f"上次停止原因: {progress['stop_reason']}")
    if progress.get("last_updated"):
        log.info(f"上次更新: {progress['last_updated']}")


def sleep_random():
    """随机等待，模拟人类行为，避免被微信检测"""
    delay = random.uniform(SCRAPE_MIN_INTERVAL, SCRAPE_MAX_INTERVAL)
    log.debug(f"等待 {delay:.1f}s")
    time.sleep(delay)


def guess_ext(url, content_type=""):
    """从 URL 或 Content-Type 猜测图片扩展名"""
    ct = content_type.lower()
    if "png" in ct:
        return ".png"
    if "gif" in ct:
        return ".gif"
    if "webp" in ct:
        return ".webp"
    if "svg" in ct:
        return ".svg"
    if "jpeg" in ct or "jpg" in ct:
        return ".jpg"

    m = re.search(r'wx_fmt=(\w+)', url)
    if m:
        fmt = m.group(1).lower()
        if fmt in ("png", "gif", "webp", "svg"):
            return f".{fmt}"
        if fmt in ("jpeg", "jpg"):
            return ".jpg"

    path = urlparse(url).path.lower()
    for ext in (".png", ".gif", ".webp", ".svg", ".jpg", ".jpeg"):
        if path.endswith(ext):
            return ext

    return ".jpg"


def download_image(url, save_path):
    """下载单张图片，返回是否成功"""
    try:
        resp = requests.get(url, headers=IMG_HEADERS, timeout=15, stream=True)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        if not os.path.splitext(save_path)[1]:
            save_path += guess_ext(url, ct)
        with open(save_path, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        return True, save_path
    except Exception as e:
        log.debug(f"    图片下载失败: {str(e)[:80]}")
        return False, save_path


def download_article_images(content_html, cover_url, slug, art_id):
    """
    下载文章的所有图片（封面 + 正文），替换 HTML 中的链接为本地路径。
    返回 (修改后的 html, 本地封面路径, 下载数, 失败数)
    """
    img_dir = get_image_dir(slug, art_id)
    url_prefix = f"/data/images/{slug}/{art_id}"

    downloaded = 0
    failed = 0
    local_cover = ""

    # 1. 下载封面图
    if cover_url:
        ext = guess_ext(cover_url)
        cover_filename = f"cover{ext}"
        cover_path = os.path.join(img_dir, cover_filename)
        if not os.path.exists(cover_path):
            ok, cover_path = download_image(cover_url, cover_path)
            if ok:
                downloaded += 1
                local_cover = f"{url_prefix}/{os.path.basename(cover_path)}"
                time.sleep(IMG_DOWNLOAD_INTERVAL)
            else:
                failed += 1
                local_cover = cover_url
        else:
            local_cover = f"{url_prefix}/{cover_filename}"

    # 2. 提取正文中所有图片 URL（data-src 和 src）
    img_pattern = re.compile(r'(<img[^>]*?)(data-src|src)="([^"]+)"', re.IGNORECASE)
    img_urls = []
    for match in img_pattern.finditer(content_html):
        img_urls.append(match.group(3))

    seen = set()
    unique_urls = []
    for u in img_urls:
        if u not in seen:
            seen.add(u)
            unique_urls.append(u)

    url_map = {}
    for idx, img_url in enumerate(unique_urls, 1):
        ext = guess_ext(img_url)
        filename = f"{idx}{ext}"
        local_path = os.path.join(img_dir, filename)

        if os.path.exists(local_path):
            url_map[img_url] = f"{url_prefix}/{filename}"
            continue

        ok, local_path = download_image(img_url, local_path)
        if ok:
            downloaded += 1
            url_map[img_url] = f"{url_prefix}/{os.path.basename(local_path)}"
        else:
            failed += 1
            url_map[img_url] = img_url

        if idx < len(unique_urls):
            time.sleep(IMG_DOWNLOAD_INTERVAL)

    # 3. 替换 HTML 中的图片链接
    def replace_img(match):
        prefix = match.group(1)
        attr = match.group(2)
        orig_url = match.group(3)
        local_url = url_map.get(orig_url, orig_url)
        if attr == "data-src":
            prefix = re.sub(r'\ssrc="[^"]*"', '', prefix)
            return f'{prefix}src="{local_url}"'
        return f'{prefix}src="{local_url}"'

    new_html = img_pattern.sub(replace_img, content_html)

    return new_html, local_cover, downloaded, failed


def fetch_article_list(slug, page, page_size=PAGE_SIZE):
    """调用 API 获取一页文章列表。返回空列表或抛 APILimitReached。"""
    params = {
        "token": TOKEN,
        "user": USER,
        "slug": slug,
        "page": page,
        "page_size": page_size,
    }
    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    status = data.get("status", "")
    if status != "success":
        body_preview = json.dumps(data, ensure_ascii=False)[:300]
        log.error(f"API 返回非 success: {body_preview}")
        raise APILimitReached(f"API 状态异常 ({status})，可能已达调用上限")

    items = data.get("data", [])
    if not isinstance(items, list):
        log.error(f"API data 字段不是列表: {type(items)}")
        raise APILimitReached("API 返回格式异常，可能已达调用上限")

    return items


def scrape_wechat_content(url):
    """爬取微信公众号文章页面，提取正文 HTML"""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    html = resp.text

    match = re.search(r'id="js_content"[^>]*>(.*?)</div>\s*<script', html, re.DOTALL)
    if match:
        return match.group(1).strip()

    match = re.search(r'class="rich_media_content[^"]*"[^>]*>(.*?)</div>\s*<script', html, re.DOTALL)
    if match:
        return match.group(1).strip()

    return ""


def build_record(item, content_html, local_cover, slug, art_id):
    """构造一条完整记录"""
    return {
        "article": {
            "title": item.get("title", ""),
            "name": item.get("name", ""),
            "author": item.get("author", ""),
            "publish_time": item.get("publish_time", ""),
            "original_url": item.get("original_url", ""),
            "image": item.get("image", ""),
            "image_local": local_cover,
            "content_html": content_html,
        },
        "meta": {
            "slug": slug,
            "article_id": art_id,
            "fetched_at": datetime.now().isoformat(),
        },
    }


def append_record(filepath, record):
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ========== fetch: 仅从 API 获取元数据，存入队列 ==========

def cmd_fetch(slug=SLUG, start_page=None):
    """
    从今天看啥 API 翻页获取文章列表，存入队列文件。
    只做 API 调用 + 去重 + 写入队列，不爬取微信正文。
    """
    queue_path = get_queue_path(slug)
    output_path = get_output_path(slug)
    progress = load_progress(slug)

    # 去重：已在主数据文件中的 + 已在队列中的
    existing_urls = load_existing_urls(output_path)
    queue_urls = load_queue_urls(queue_path)
    known_urls = existing_urls | queue_urls

    log.info(f"队列文件: {queue_path}（已有 {len(queue_urls)} 条待爬取）")
    log.info(f"主数据文件: {output_path}（已有 {len(existing_urls)} 条已完成）")
    print_progress_summary(progress)

    if start_page is None:
        start_page = progress.get("next_page", 1)
        log.info(f"自动续接，从第 {start_page} 页开始")
    else:
        log.info(f"手动指定，从第 {start_page} 页开始")

    page = start_page
    total_new = 0
    total_skip = 0
    consecutive_all_exist = 0
    stop_reason = "completed"

    while True:
        log.info(f"========== 第 {page} 页 ==========")
        try:
            items = fetch_article_list(slug, page)
        except APILimitReached as e:
            log.error(f"API 受限，停止翻页: {e}")
            stop_reason = "api_limit"
            break
        except requests.exceptions.RequestException as e:
            log.error(f"API 请求失败，停止翻页: {e}")
            stop_reason = "network_error"
            break

        if not items:
            log.info("本页无数据，已到末尾。")
            stop_reason = "completed"
            break

        # 过滤已存在的
        new_items = [it for it in items if it.get("original_url", "") not in known_urls]
        page_skip = len(items) - len(new_items)

        if new_items:
            append_queue_items(queue_path, new_items)
            for it in new_items:
                known_urls.add(it.get("original_url", ""))

        page_new = len(new_items)
        total_new += page_new
        total_skip += page_skip

        log.info(f"本页 {len(items)} 篇: 新增 {page_new} 到队列, 跳过 {page_skip} (已存在)")

        update_page_progress(progress, page, "done", page_new, page_skip)
        save_progress(progress)

        if page_new == 0:
            consecutive_all_exist += 1
            if consecutive_all_exist >= 2:
                log.info("连续 2 页全为已有数据，停止。")
                stop_reason = "all_exist"
                break
        else:
            consecutive_all_exist = 0

        if len(items) < PAGE_SIZE:
            log.info("本页不足 PAGE_SIZE，已到最后一页。")
            stop_reason = "completed"
            break

        page += 1
        log.info(f"翻页等待 {API_INTERVAL}s...")
        time.sleep(API_INTERVAL)

    progress["stop_reason"] = stop_reason
    save_progress(progress)

    log.info("=" * 40)
    log.info(f"[fetch] 本次: 新增 {total_new} 条到队列, 跳过 {total_skip} 条")
    log.info(f"[fetch] 停止原因: {stop_reason}")

    # 打印当前队列状态
    current_queue = load_queue_items(queue_path)
    log.info(f"[fetch] 队列中共 {len(current_queue)} 条待爬取")


# ========== scrape: 从队列读取，爬取微信正文 ==========

def cmd_scrape(slug=SLUG):
    """
    从队列文件读取待爬取文章，逐篇爬取微信正文 + 下载图片，
    完成后从队列中移除。

    采用循环读取模式：每轮处理完当前队列后，重新读取队列文件，
    如果 fetch 追加了新数据则继续处理，直到队列真正为空。
    """
    queue_path = get_queue_path(slug)
    output_path = get_output_path(slug)
    existing_urls = load_existing_urls(output_path)

    consecutive_fail = 0
    total_processed = 0

    while True:
        # 每轮重新读取队列，拿到 fetch 可能新追加的数据
        queue_items = load_queue_items(queue_path)
        if not queue_items:
            if total_processed == 0:
                log.info("队列为空，没有待爬取的文章。先运行 fetch 获取文章列表。")
            else:
                log.info("队列已清空，本轮结束。")
            break

        log.info(f"队列中 {len(queue_items)} 条待爬取，主数据文件已有 {len(existing_urls)} 条")

        round_processed = 0

        for i, item in enumerate(queue_items, 1):
            url = item.get("original_url", "")
            title = item.get("name") or item.get("title", "无标题")

            # 已完成的直接从队列移除
            if url in existing_urls:
                log.debug(f"[{i}/{len(queue_items)}] 跳过（已完成）: {title[:50]}")
                remove_from_queue(queue_path, url)
                continue

            art_id = article_id(url)
            content_html = ""
            local_cover = ""

            log.info(f"[{i}/{len(queue_items)}] 爬取: {title[:50]}")
            try:
                content_html = scrape_wechat_content(url)
                if content_html:
                    log.info(f"  -> 正文 OK ({len(content_html)} chars)")
                    consecutive_fail = 0

                    cover_url = item.get("image", "")
                    log.info(f"  -> 下载图片...")
                    content_html, local_cover, img_down, img_fail = \
                        download_article_images(content_html, cover_url, slug, art_id)
                    log.info(f"  -> 图片: {img_down} 下载, {img_fail} 失败")
                else:
                    log.warning(f"  -> 正文为空（页面可能需要验证或已被删除）")
                    consecutive_fail += 1
            except requests.exceptions.HTTPError as e:
                log.error(f"  -> HTTP 错误: {e}")
                consecutive_fail += 1
            except requests.exceptions.RequestException as e:
                log.error(f"  -> 网络错误: {e}")
                consecutive_fail += 1
            except Exception as e:
                log.error(f"  -> 未知错误: {e}")
                consecutive_fail += 1

            if consecutive_fail >= MAX_CONSECUTIVE_FAILURES:
                log.error(f"连续 {consecutive_fail} 次爬取失败，可能被限流，中止")
                break

            # 保存到主数据文件
            record = build_record(item, content_html, local_cover, slug, art_id)
            append_record(output_path, record)
            existing_urls.add(url)
            total_processed += 1
            round_processed += 1

            # 从队列移除这一条（重新读取文件，不会丢失 fetch 新追加的）
            remove_from_queue(queue_path, url)

            sleep_random()

        # 连续失败过多则退出外层循环
        if consecutive_fail >= MAX_CONSECUTIVE_FAILURES:
            break

        # 本轮没有处理任何新文章（全是跳过），直接结束
        if round_processed == 0:
            break

        # 本轮处理完了，回到 while 顶部重新读取队列，看 fetch 有没有追加新的
        log.info(f"本轮处理 {round_processed} 条，检查队列是否有新数据...")

    remaining = load_queue_items(queue_path)
    log.info("=" * 40)
    log.info(f"[scrape] 本次共处理: {total_processed} 条")
    log.info(f"[scrape] 队列剩余: {len(remaining)} 条")
    log.info(f"[scrape] 主数据文件: {len(existing_urls)} 条")


# ========== 兼容旧命令 ==========

def scrape_and_save_page(items, filepath, existing_urls, slug, scrape=True):
    """
    处理一页的文章列表：去重、爬取正文、下载图片、保存。
    返回 (new_count, skip_count)。
    """
    new_count = 0
    skip_count = 0
    consecutive_fail = 0

    for i, item in enumerate(items, 1):
        url = item.get("original_url", "")
        title = item.get("name") or item.get("title", "无标题")

        if url in existing_urls:
            log.debug(f"[{i}/{len(items)}] 跳过（已存在）: {title[:50]}")
            skip_count += 1
            continue

        art_id = article_id(url)
        content_html = ""
        local_cover = ""

        if scrape and url:
            log.info(f"[{i}/{len(items)}] 爬取: {title[:50]}")
            try:
                content_html = scrape_wechat_content(url)
                if content_html:
                    log.info(f"  -> 正文 OK ({len(content_html)} chars)")
                    consecutive_fail = 0

                    cover_url = item.get("image", "")
                    log.info(f"  -> 下载图片...")
                    content_html, local_cover, img_down, img_fail = \
                        download_article_images(content_html, cover_url, slug, art_id)
                    log.info(f"  -> 图片: {img_down} 下载, {img_fail} 失败")
                else:
                    log.warning(f"  -> 正文为空（页面可能需要验证或已被删除）")
                    consecutive_fail += 1
            except requests.exceptions.HTTPError as e:
                log.error(f"  -> HTTP 错误: {e}")
                consecutive_fail += 1
            except requests.exceptions.RequestException as e:
                log.error(f"  -> 网络错误: {e}")
                consecutive_fail += 1
            except Exception as e:
                log.error(f"  -> 未知错误: {e}")
                consecutive_fail += 1

            if consecutive_fail >= MAX_CONSECUTIVE_FAILURES:
                log.error(f"连续 {consecutive_fail} 次爬取失败，可能被限流，中止本轮")
                raise ScrapeFatalError(f"连续 {consecutive_fail} 次失败")

            if i < len(items):
                sleep_random()
        else:
            log.info(f"[{i}/{len(items)}] 仅保存元信息: {title[:50]}")

        record = build_record(item, content_html, local_cover, slug, art_id)
        append_record(filepath, record)
        existing_urls.add(url)
        new_count += 1

    return new_count, skip_count


def fetch_single_page(slug=SLUG, page=1, scrape=True):
    """抓取单页文章列表 + 爬取正文（旧的一体化模式）"""
    filepath = get_output_path(slug)
    existing_urls = load_existing_urls(filepath)
    progress = load_progress(slug)

    log.info(f"数据文件: {filepath}（已有 {len(existing_urls)} 条）")
    print_progress_summary(progress)
    log.info(f"请求第 {page} 页（page_size={PAGE_SIZE}）...")

    try:
        items = fetch_article_list(slug, page)
    except APILimitReached as e:
        log.error(f"API 受限: {e}")
        progress["stop_reason"] = "api_limit"
        save_progress(progress)
        return
    log.info(f"获取到 {len(items)} 篇文章")

    if not items:
        log.warning("本页无数据")
        return

    new_count = 0
    skip_count = 0
    try:
        new_count, skip_count = scrape_and_save_page(items, filepath, existing_urls, slug, scrape)
        update_page_progress(progress, page, "done", new_count, skip_count)
        progress["stop_reason"] = "page_done"
    except ScrapeFatalError:
        log.error("爬取中止，已保存的数据不受影响")
        update_page_progress(progress, page, "partial", new_count, skip_count)
        progress["stop_reason"] = "scrape_failure"

    save_progress(progress)
    log.info(f"本页完成: 新增 {new_count} 条，跳过 {skip_count} 条重复")
    log.info(f"进度已保存到 {get_progress_path(slug)}")


# ========== 数据库模式 (--db) ==========

def _init_db(slug=None):
    """初始化数据库连接，返回 (db, source)"""
    from database import Database, get_or_create_source
    if slug is None:
        slug = SLUG
    name = SOURCES.get(slug, {}).get("name", slug)
    db = Database()
    db.connect()
    db.init_schema()
    source = get_or_create_source(db, slug, name=name, platform="wechat")
    return db, source


def _guess_mime(url, ext):
    """根据扩展名猜测 MIME 类型"""
    mapping = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".gif": "image/gif",
        ".webp": "image/webp", ".svg": "image/svg+xml",
    }
    return mapping.get(ext, "image/jpeg")


def download_image_bytes(url):
    """下载图片，返回 (bytes, ext, mime) 或 (None, None, None)"""
    try:
        resp = requests.get(url, headers=IMG_HEADERS, timeout=15, stream=True)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        ext = guess_ext(url, ct)
        data = resp.content
        mime = _guess_mime(url, ext)
        return data, ext, mime
    except Exception as e:
        log.debug(f"    图片下载失败: {str(e)[:80]}")
        return None, None, None


def db_cmd_fetch(slug=SLUG, start_page=None):
    """
    数据库模式的 fetch：从 API 获取文章列表存入 scrape_queue 表。

    额度保护：每个 slug 在 SOURCES 里配置 pages_per_run，限制单次 API 调用次数。

    Gap 检测逻辑：
      - 遇到连续 2 页全为已有数据时，检查下一页是否曾经抓过（在 page_details 里）。
      - 若下一页从未抓过 → 可能存在历史缺口，继续探查（不计入 consecutive_all_exist）。
      - 若下一页已抓过且也是全已有 → 说明后续是连续已知区域，安全停止。
    """
    from database import (get_or_create_source, get_existing_urls, get_progress,
                          update_progress, enqueue_articles, count_queue, Database)

    db, source = _init_db(slug)
    source_id = source["id"]
    max_pages = SOURCES.get(slug, {}).get("pages_per_run", 10)

    existing_urls = get_existing_urls(db, source_id)
    pending_count = count_queue(db, source_id, "pending")
    progress = get_progress(db, source_id)

    log.info(f"[{slug}] 数据库已有 {len(existing_urls)} 篇，队列 {pending_count} 条待爬取，本次额度 {max_pages} 页")
    log.info(f"[{slug}] 已完成到第 {progress['last_completed_page']} 页")

    if start_page is None:
        start_page = 1  # 每次从最新开始，确保不遗漏新文章
        log.info(f"[{slug}] 从第 1 页（最新）开始")
    else:
        log.info(f"[{slug}] 手动指定从第 {start_page} 页开始")

    page = start_page
    pages_fetched = 0       # 本次实际消耗的 API 次数
    total_new = 0
    total_skip = 0
    consecutive_all_exist = 0
    stop_reason = "completed"
    page_details = progress.get("page_details") or {}

    while True:
        if pages_fetched >= max_pages:
            log.info(f"[{slug}] 已达本次额度上限 {max_pages} 页，停止（剩余页留下次继续）")
            stop_reason = "page_limit"
            break

        log.info(f"========== [{slug}] 第 {page} 页 ==========")
        try:
            items = fetch_article_list(slug, page)
        except APILimitReached as e:
            log.error(f"API 受限，停止翻页: {e}")
            stop_reason = "api_limit"
            break
        except requests.exceptions.RequestException as e:
            log.error(f"API 请求失败，停止翻页: {e}")
            stop_reason = "network_error"
            break

        pages_fetched += 1

        if not items:
            log.info("本页无数据，已到末尾。")
            stop_reason = "completed"
            break

        # 过滤已在文章表中的
        new_items = [it for it in items if it.get("original_url", "") not in existing_urls]
        page_skip = len(items) - len(new_items)
        page_new = enqueue_articles(db, source_id, new_items)
        for it in new_items:
            existing_urls.add(it.get("original_url", ""))

        total_new += page_new
        total_skip += page_skip
        log.info(f"本页 {len(items)} 篇: 新增 {page_new} 到队列, 跳过 {page_skip} (已存在)")

        page_details[str(page)] = {
            "status": "done", "new": page_new, "skip": page_skip,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        completed_page = max(progress.get("last_completed_page", 0), page)
        update_progress(db, source_id,
                        last_completed_page=completed_page,
                        next_page=completed_page + 1,
                        page_details=page_details)

        if page_new == 0:
            consecutive_all_exist += 1
            if consecutive_all_exist >= 2:
                # 检查下一页是否曾经抓过
                next_page_str = str(page + 1)
                if next_page_str in page_details:
                    # 下一页已知 → 后续是连续已知区域，可以安全停止
                    log.info(f"[{slug}] 连续 {consecutive_all_exist} 页全为已有数据且后续页已知，停止")
                    stop_reason = "all_exist"
                    break
                else:
                    # 下一页从未抓过 → 可能存在历史缺口，继续探查
                    log.info(f"[{slug}] 连续 {consecutive_all_exist} 页全为已有，但第 {page+1} 页未曾抓取，继续探查缺口...")
                    consecutive_all_exist = 0
        else:
            consecutive_all_exist = 0

        if len(items) < PAGE_SIZE:
            log.info("本页不足 PAGE_SIZE，已到最后一页。")
            stop_reason = "completed"
            break

        page += 1
        log.info(f"翻页等待 {API_INTERVAL}s...")
        time.sleep(API_INTERVAL)

    update_progress(db, source_id, stop_reason=stop_reason)
    log.info("=" * 40)
    log.info(f"[{slug}] fetch 完成: 消耗 {pages_fetched} 次 API，新增 {total_new} 条，跳过 {total_skip} 条")
    log.info(f"[{slug}] 停止原因: {stop_reason}")
    pending = count_queue(db, source_id, "pending")
    log.info(f"[{slug}] 队列中共 {pending} 条待爬取")
    db.close()


def db_cmd_scrape(slug=SLUG):
    """数据库模式的 scrape：从 scrape_queue 读取，爬取正文+图片存入数据库"""
    from database import (get_pending_queue, update_queue_status,
                          insert_article, insert_image, count_queue,
                          count_articles, Database)

    db, source = _init_db(slug)
    source_id = source["id"]

    pending = count_queue(db, source_id, "pending")
    if pending == 0:
        log.info("队列为空，没有待爬取的文章。先运行 fetch --db 获取文章列表。")
        db.close()
        return

    log.info(f"队列中 {pending} 条待爬取")
    consecutive_fail = 0
    processed = 0
    batch_size = 50

    while True:
        items = get_pending_queue(db, source_id, limit=batch_size)
        if not items:
            break

        for i, item in enumerate(items, 1):
            url = item["original_url"]
            title = item["title"] or "无标题"
            queue_id = item["id"]

            art_hash = article_id(url)
            content_html = ""

            log.info(f"[{processed + 1}] 爬取: {title[:50]}")
            update_queue_status(db, queue_id, "processing")

            try:
                content_html = scrape_wechat_content(url)
                if content_html:
                    log.info(f"  -> 正文 OK ({len(content_html)} chars)")
                    consecutive_fail = 0
                else:
                    log.warning(f"  -> 正文为空")
                    consecutive_fail += 1
            except requests.exceptions.HTTPError as e:
                log.error(f"  -> HTTP 错误: {e}")
                consecutive_fail += 1
            except requests.exceptions.RequestException as e:
                log.error(f"  -> 网络错误: {e}")
                consecutive_fail += 1
            except Exception as e:
                log.error(f"  -> 未知错误: {e}")
                consecutive_fail += 1

            if consecutive_fail >= MAX_CONSECUTIVE_FAILURES:
                log.error(f"连续 {consecutive_fail} 次爬取失败，可能被限流，中止")
                update_queue_status(db, queue_id, "failed")
                break

            # 插入文章
            db_article_id = insert_article(
                db, source_id, art_hash,
                title=title,
                author=item.get("author", ""),
                publish_time=item.get("publish_time", ""),
                original_url=url,
                cover_url=item.get("cover_url", ""),
                content_html=content_html,
                fetched_at=datetime.now(),
            )

            if db_article_id and content_html:
                # 下载并存储封面图
                cover_url = item.get("cover_url", "")
                img_down = 0
                img_fail = 0

                if cover_url:
                    data, ext, mime = download_image_bytes(cover_url)
                    if data:
                        insert_image(db, db_article_id, "cover", 0,
                                     cover_url, f"cover{ext}", mime, data)
                        img_down += 1
                        time.sleep(IMG_DOWNLOAD_INTERVAL)
                    else:
                        img_fail += 1

                # 下载正文图片
                img_pattern = re.compile(r'(<img[^>]*?)(data-src|src)="([^"]+)"', re.IGNORECASE)
                seen = set()
                idx = 0
                new_html = content_html

                for match in img_pattern.finditer(content_html):
                    img_url = match.group(3)
                    if img_url in seen:
                        continue
                    seen.add(img_url)
                    idx += 1

                    data, ext, mime = download_image_bytes(img_url)
                    if data:
                        filename = f"{idx}{ext}"
                        img_id = insert_image(db, db_article_id, "content", idx,
                                              img_url, filename, mime, data)
                        # 替换 HTML 中的链接为 API 路径
                        local_path = f"/api/images/{img_id}"
                        new_html = new_html.replace(img_url, local_path)
                        img_down += 1
                        time.sleep(IMG_DOWNLOAD_INTERVAL)
                    else:
                        img_fail += 1

                # 更新文章的 content_html（图片链接已替换）
                if img_down > 0:
                    def _update_html(conn):
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE articles SET content_html = %s WHERE id = %s",
                                (new_html, db_article_id)
                            )
                    db.execute_with_retry(_update_html)

                log.info(f"  -> 图片: {img_down} 下载, {img_fail} 失败")

            update_queue_status(db, queue_id, "done")
            processed += 1

            if i < len(items):
                sleep_random()

        if consecutive_fail >= MAX_CONSECUTIVE_FAILURES:
            break

    total_articles = count_articles(db, source_id)
    remaining = count_queue(db, source_id, "pending")
    log.info("=" * 40)
    log.info(f"[scrape-db] 本次处理: {processed} 条")
    log.info(f"[scrape-db] 队列剩余: {remaining} 条")
    log.info(f"[scrape-db] 数据库文章总数: {total_articles} 条")
    db.close()


def db_cmd_status(slug=SLUG):
    """数据库模式的 status"""
    from database import (get_progress, count_queue, count_articles, Database)

    db, source = _init_db(slug)
    source_id = source["id"]

    progress = get_progress(db, source_id)
    total = count_articles(db, source_id)
    pending = count_queue(db, source_id, "pending")
    done = count_queue(db, source_id, "done")
    failed = count_queue(db, source_id, "failed")

    log.info(f"专栏: {slug} (source_id={source_id})")
    log.info(f"已完成到第 {progress['last_completed_page']} 页，下次从第 {progress['next_page']} 页开始")
    log.info(f"数据库文章总数: {total}")
    log.info(f"队列: 待爬取={pending}, 已完成={done}, 失败={failed}")
    if progress.get("stop_reason"):
        log.info(f"上次停止原因: {progress['stop_reason']}")

    db.close()


# ========== 入口 ==========

if __name__ == "__main__":
    usage = """用法:
  python scraper.py fetch          从 API 获取文章列表存入队列（可持续运行）
  python scraper.py fetch 5        从第 5 页开始获取
  python scraper.py scrape         从队列读取并爬取微信正文（可与 fetch 同时运行）
  python scraper.py status         查看当前进度和队列状态
  python scraper.py                抓取第1页（一体化模式，测试用）
  python scraper.py 3              抓取第3页（一体化模式）
  python scraper.py list 2         仅获取第2页列表（不爬正文）

  加 --db 参数启用数据库模式:
  python scraper.py --db fetch              遍历 SOURCES 所有 slug，每个按 pages_per_run 限额抓取
  python scraper.py --db fetch pJMG8ZXFLd  仅抓取指定 slug
  python scraper.py --db scrape             从数据库队列爬取，正文+图片存入数据库
  python scraper.py --db scrape pJMG8ZXFLd 仅爬取指定 slug
  python scraper.py --db status             查看所有 slug 的进度
  python scraper.py --db status pJMG8ZXFLd 查看指定 slug 的进度

  多 slug 管理（在 scraper.py 顶部 SOURCES 字典中添加）:
  SOURCES = {
      "pJMG8ZXFLd": {"name": "机器之心", "pages_per_run": 10},
      "another_slug": {"name": "另一公众号", "pages_per_run": 5},
  }"""

    if USE_DB:
        # 数据库模式
        # 判断是否指定了 slug 参数（非数字的第二个参数视为 slug）
        def _get_slug_arg(pos=2):
            if len(sys.argv) > pos and not sys.argv[pos].isdigit() and sys.argv[pos] != "all":
                return sys.argv[pos]
            return None

        if len(sys.argv) < 2:
            print(usage)
        elif sys.argv[1] == "fetch":
            target_slug = _get_slug_arg(2)
            # 起始页：最后一个纯数字参数
            start = next((int(a) for a in reversed(sys.argv[2:]) if a.isdigit()), None)
            if target_slug:
                db_cmd_fetch(slug=target_slug, start_page=start)
            else:
                # 遍历 SOURCES 中所有 slug
                for s in SOURCES:
                    db_cmd_fetch(slug=s, start_page=start)
        elif sys.argv[1] == "scrape":
            target_slug = _get_slug_arg(2)
            if target_slug:
                db_cmd_scrape(slug=target_slug)
            else:
                for s in SOURCES:
                    db_cmd_scrape(slug=s)
        elif sys.argv[1] == "status":
            target_slug = _get_slug_arg(2)
            slugs = [target_slug] if target_slug else list(SOURCES.keys())
            for s in slugs:
                db_cmd_status(slug=s)
        else:
            print(usage)
    else:
        # 文件模式（原有逻辑）
        if len(sys.argv) < 2:
            fetch_single_page(page=1)
        elif sys.argv[1] == "fetch":
            start = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else None
            cmd_fetch(start_page=start)
        elif sys.argv[1] == "scrape":
            cmd_scrape()
        elif sys.argv[1] == "list":
            page = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 1
            fetch_single_page(page=page, scrape=False)
        elif sys.argv[1] == "status":
            progress = load_progress(SLUG)
            print_progress_summary(progress)
            queue_path = get_queue_path(SLUG)
            queue_items = load_queue_items(queue_path)
            log.info(f"待爬取队列: {len(queue_items)} 条")
            if progress["pages"]:
                log.info("--- 各页详情 ---")
                for p in sorted(progress["pages"].keys(), key=int):
                    info = progress["pages"][p]
                    log.info(f"  第 {p} 页: {info['status']}  新增={info['new']}  跳过={info['skip']}  时间={info['time']}")
        elif sys.argv[1].isdigit():
            fetch_single_page(page=int(sys.argv[1]))
        else:
            print(usage)
