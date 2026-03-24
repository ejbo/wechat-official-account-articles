"""
今天看啥 (jintiankansha) 专栏文章抓取器

两步抓取:
  1. 通过 API 获取文章列表（含 original_url），page_size=30
  2. 逐个访问 original_url 爬取微信公众号文章正文 HTML
  3. 下载正文及封面图片到本地，HTML 中图片链接替换为本地路径

存储格式: JSON Lines (.jsonl) + 本地图片目录
"""

import hashlib
import json
import logging
import os
import random
import re
import sys
import time
from urllib.parse import urlparse

import requests
from datetime import datetime

# ============ 配置区域 ============
SLUG = "pJMG8ZXFLd"  # 专栏标识，需要时修改这里
USER = "jzl19991121@gmail.com"
TOKEN = "Kb3ijGo95y"
API_URL = "http://www.jintiankansha.me/api3/query/adv/get_topics_by_one_column"
PAGE_SIZE = 30  # 每页条数，最大30
API_INTERVAL = 3  # API 翻页间隔（秒）
SCRAPE_MIN_INTERVAL = 3  # 爬取文章最小间隔（秒）
SCRAPE_MAX_INTERVAL = 6  # 爬取文章最大间隔（秒）
IMG_DOWNLOAD_INTERVAL = 0.5  # 图片下载间隔（秒），比文章短，因为是静态资源
MAX_CONSECUTIVE_FAILURES = 5  # 连续失败次数上限，超过则中止
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


# ========== 进度管理 ==========

def load_progress(slug):
    """
    加载进度文件，返回 dict:
    {
        "slug": "...",
        "last_completed_page": 3,
        "next_page": 4,
        "total_articles": 90,
        "last_updated": "...",
        "stop_reason": "user_interrupt / api_limit / completed / ...",
        "pages": {
            "1": {"status": "done", "new": 28, "skip": 2, "time": "..."},
            "2": {"status": "done", ...},
            "3": {"status": "partial", "new": 15, "skip": 0, "time": "..."},
        }
    }
    """
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
    # 从 Content-Type
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

    # 从 URL 参数 wx_fmt=
    m = re.search(r'wx_fmt=(\w+)', url)
    if m:
        fmt = m.group(1).lower()
        if fmt in ("png", "gif", "webp", "svg"):
            return f".{fmt}"
        if fmt in ("jpeg", "jpg"):
            return ".jpg"

    # 从路径
    path = urlparse(url).path.lower()
    for ext in (".png", ".gif", ".webp", ".svg", ".jpg", ".jpeg"):
        if path.endswith(ext):
            return ext

    return ".jpg"  # 默认


def download_image(url, save_path):
    """下载单张图片，返回是否成功"""
    try:
        resp = requests.get(url, headers=IMG_HEADERS, timeout=15, stream=True)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        # 如果 save_path 没有扩展名，根据响应补上
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
    # 本地路径前缀（相对于 server 根目录，供前端引用）
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
                local_cover = cover_url  # 回退用原始 URL
        else:
            local_cover = f"{url_prefix}/{cover_filename}"

    # 2. 提取正文中所有图片 URL（data-src 和 src）
    img_pattern = re.compile(r'(<img[^>]*?)(data-src|src)="([^"]+)"', re.IGNORECASE)
    img_urls = []
    for match in img_pattern.finditer(content_html):
        img_urls.append(match.group(3))

    # 去重但保持顺序
    seen = set()
    unique_urls = []
    for u in img_urls:
        if u not in seen:
            seen.add(u)
            unique_urls.append(u)

    # 建立 URL -> 本地路径 的映射
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
            url_map[img_url] = img_url  # 回退

        if idx < len(unique_urls):
            time.sleep(IMG_DOWNLOAD_INTERVAL)

    # 3. 替换 HTML 中的图片链接
    def replace_img(match):
        prefix = match.group(1)
        attr = match.group(2)   # data-src 或 src
        orig_url = match.group(3)
        local_url = url_map.get(orig_url, orig_url)
        # 统一用 src，去掉 data-src
        if attr == "data-src":
            # 先移除已有的 src=""（如果有）
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
            "is_first": item.get("is_first", 0),
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
        img_down = 0
        img_fail = 0

        if scrape and url:
            log.info(f"[{i}/{len(items)}] 爬取: {title[:50]}")
            try:
                content_html = scrape_wechat_content(url)
                if content_html:
                    log.info(f"  -> 正文 OK ({len(content_html)} chars)")
                    consecutive_fail = 0

                    # 下载图片
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

            # 不是最后一篇时等待随机间隔
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
    """抓取单页文章列表 + 爬取正文"""
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


def fetch_all_pages(slug=SLUG, start_page=None):
    """自动翻页抓取。start_page=None 时自动从上次断点继续。"""
    filepath = get_output_path(slug)
    existing_urls = load_existing_urls(filepath)
    progress = load_progress(slug)

    log.info(f"数据文件: {filepath}（已有 {len(existing_urls)} 条）")
    print_progress_summary(progress)

    # 自动续接：未指定起始页时，从进度文件的 next_page 继续
    if start_page is None:
        start_page = progress.get("next_page", 1)
        log.info(f"自动续接，从第 {start_page} 页开始")
    else:
        log.info(f"手动指定，从第 {start_page} 页开始")

    log.info(f"page_size={PAGE_SIZE}")

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

        log.info(f"本页 {len(items)} 篇文章")

        try:
            page_new, page_skip = scrape_and_save_page(items, filepath, existing_urls, slug)
            update_page_progress(progress, page, "done", page_new, page_skip)
        except ScrapeFatalError:
            log.error("爬取连续失败，中止。已保存的数据不受影响。")
            update_page_progress(progress, page, "partial", 0, 0)
            stop_reason = "scrape_failure"
            save_progress(progress)
            break

        total_new += page_new
        total_skip += page_skip
        log.info(f"本页结果: 新增 {page_new}, 跳过 {page_skip}")
        log.info(f"累计: 新增 {total_new}, 跳过 {total_skip}")

        # 每页完成后立即保存进度（断电也不丢）
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
    log.info(f"本次运行: 新增 {total_new} 条, 跳过 {total_skip} 条")
    log.info(f"停止原因: {stop_reason}")
    print_progress_summary(progress)
    log.info(f"进度已保存到 {get_progress_path(slug)}")


if __name__ == "__main__":
    usage = """用法:
  python scraper.py              抓取第1页（测试）
  python scraper.py 3            抓取第3页
  python scraper.py all          自动续接上次断点继续翻页抓取
  python scraper.py all 5        从第5页开始自动翻页抓取
  python scraper.py list 2       仅获取第2页列表（不爬正文）
  python scraper.py status       查看当前抓取进度"""

    if len(sys.argv) < 2:
        fetch_single_page(page=1)
    elif sys.argv[1] == "all":
        start = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else None
        fetch_all_pages(start_page=start)
    elif sys.argv[1] == "list":
        page = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 1
        fetch_single_page(page=page, scrape=False)
    elif sys.argv[1] == "status":
        progress = load_progress(SLUG)
        print_progress_summary(progress)
        if progress["pages"]:
            log.info("--- 各页详情 ---")
            for p in sorted(progress["pages"].keys(), key=int):
                info = progress["pages"][p]
                log.info(f"  第 {p} 页: {info['status']}  新增={info['new']}  跳过={info['skip']}  时间={info['time']}")
    elif sys.argv[1].isdigit():
        fetch_single_page(page=int(sys.argv[1]))
    else:
        print(usage)
