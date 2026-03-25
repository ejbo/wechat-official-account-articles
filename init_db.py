"""
数据库初始化 & 数据迁移工具

功能:
  1. 创建数据库表结构
  2. 将现有 JSONL 文件中的文章数据导入数据库
  3. 将本地图片文件读入数据库的 images 表

用法:
  python init_db.py                    仅建表
  python init_db.py migrate            建表 + 迁移现有数据
  python init_db.py migrate --images   建表 + 迁移数据 + 导入图片（耗时较长）
"""

import json
import os
import re
import sys
from datetime import datetime

from database import (
    Database, get_or_create_source, insert_article, insert_image,
    article_exists, count_articles
)

DATA_DIR = "data"
SLUG = "pJMG8ZXFLd"


def init_tables():
    """仅创建表"""
    db = Database()
    db.connect()
    db.init_schema()
    print("数据库表结构已创建/更新")
    return db


def migrate_articles(db, slug=SLUG, import_images=False):
    """将 JSONL 文件中的文章导入数据库"""
    source = get_or_create_source(db, slug, name="机器之心", platform="wechat")
    source_id = source["id"]

    filepath = os.path.join(DATA_DIR, f"{slug}.jsonl")
    if not os.path.exists(filepath):
        print(f"数据文件不存在: {filepath}")
        return

    existing_count = count_articles(db, source_id)
    print(f"数据库中已有 {existing_count} 篇文章")

    imported = 0
    skipped = 0
    img_imported = 0
    img_failed = 0

    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                print(f"  行 {line_num}: JSON 解析失败，跳过")
                continue

            article = record.get("article", {})
            meta = record.get("meta", {})

            original_url = article.get("original_url", "")
            if not original_url:
                skipped += 1
                continue

            # 插入文章
            art_hash = meta.get("article_id", "")
            fetched_at = meta.get("fetched_at", datetime.now().isoformat())

            db_article_id = insert_article(
                db, source_id, art_hash,
                title=article.get("title") or article.get("name", ""),
                author=article.get("author", ""),
                publish_time=article.get("publish_time", ""),
                original_url=original_url,
                cover_url=article.get("image", ""),
                content_html=article.get("content_html", ""),
                is_first=article.get("is_first", 0),
                fetched_at=fetched_at,
            )

            if db_article_id is None:
                skipped += 1
                if line_num % 500 == 0:
                    print(f"  进度: {line_num} 行, 导入={imported}, 跳过={skipped}")
                continue

            imported += 1

            # 导入图片
            if import_images and db_article_id:
                img_dir = os.path.join(DATA_DIR, "images", slug, art_hash)
                if os.path.isdir(img_dir):
                    files = sorted(os.listdir(img_dir))
                    for fname in files:
                        fpath = os.path.join(img_dir, fname)
                        if not os.path.isfile(fpath):
                            continue

                        try:
                            with open(fpath, "rb") as imgf:
                                data = imgf.read()

                            is_cover = fname.startswith("cover")
                            img_type = "cover" if is_cover else "content"
                            idx = 0
                            if not is_cover:
                                m = re.match(r"(\d+)", fname)
                                idx = int(m.group(1)) if m else 0

                            ext = os.path.splitext(fname)[1].lower()
                            mime_map = {
                                ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                                ".png": "image/png", ".gif": "image/gif",
                                ".webp": "image/webp", ".svg": "image/svg+xml",
                            }
                            mime = mime_map.get(ext, "image/jpeg")

                            insert_image(db, db_article_id, img_type, idx,
                                         "", fname, mime, data)
                            img_imported += 1
                        except Exception as e:
                            img_failed += 1

            if line_num % 200 == 0:
                print(f"  进度: {line_num} 行, 导入={imported}, 跳过={skipped}"
                      + (f", 图片={img_imported}" if import_images else ""))

    print(f"\n迁移完成:")
    print(f"  文章: 导入 {imported}, 跳过 {skipped}")
    if import_images:
        print(f"  图片: 导入 {img_imported}, 失败 {img_failed}")

    total = count_articles(db, source_id)
    print(f"  数据库文章总数: {total}")


if __name__ == "__main__":
    db = init_tables()

    if len(sys.argv) > 1 and sys.argv[1] == "migrate":
        import_images = "--images" in sys.argv
        if import_images:
            print("将导入文章数据和图片文件（这可能需要较长时间）...")
        else:
            print("将导入文章数据（不含图片）...")
            print("如需同时导入图片，请使用: python init_db.py migrate --images")
        migrate_articles(db, import_images=import_images)

    db.close()
