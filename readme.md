# jiqizhixin-scraper

今天看啥 (jintiankansha) 专栏文章抓取器，用于抓取微信公众号文章全文及图片到本地。

## 工作流程

1. 调用今天看啥 API 获取文章列表（每页最多 30 篇）
2. 逐篇访问微信原文链接，爬取正文 HTML
3. 下载正文及封面图片到本地，替换 HTML 中的图片链接为本地路径
4. 全量数据保存为 JSON Lines 格式，支持断点续爬

## 快速开始

```bash
# 激活虚拟环境（已有 requests 依赖）
# Windows
.venv\Scripts\activate

# 抓取第 1 页看效果
python scraper.py

# 自动续接上次断点，继续翻页抓取
python scraper.py all

# 查看当前进度
python scraper.py status

# 启动本地预览服务器
python server.py
# 浏览器打开 http://127.0.0.1:8080
```

## 命令一览

| 命令 | 说明 |
|------|------|
| `python scraper.py` | 抓取第 1 页（30 篇），用于测试 |
| `python scraper.py 3` | 抓取第 3 页 |
| `python scraper.py all` | 自动从上次断点继续翻页抓取全部 |
| `python scraper.py all 5` | 从第 5 页开始翻页抓取（手动指定起始页） |
| `python scraper.py list 2` | 仅获取第 2 页文章列表，不爬取正文和图片 |
| `python scraper.py status` | 查看当前抓取进度 |
| `python server.py` | 启动本地 Web 服务器预览文章 |

## 配置

编辑 `scraper.py` 顶部的配置区域：

```python
SLUG = "pJMG8ZXFLd"       # 专栏标识，切换专栏时修改这里
PAGE_SIZE = 30             # 每页条数，最大 30
API_INTERVAL = 2           # API 翻页间隔（秒）
SCRAPE_MIN_INTERVAL = 3    # 爬取文章最小间隔（秒）
SCRAPE_MAX_INTERVAL = 3    # 爬取文章最大间隔（秒）
IMG_DOWNLOAD_INTERVAL = 0.5 # 图片下载间隔（秒）
MAX_CONSECUTIVE_FAILURES = 5 # 连续失败次数上限，超过自动中止
```

## 数据存储结构

```
data/
  {slug}.jsonl                  # 文章数据（每行一条 JSON 记录）
  {slug}_progress.json          # 抓取进度
  images/{slug}/
    {article_id}/               # 文章 ID（URL 的 MD5 前 10 位）
      cover.jpg                 # 封面图
      1.png                     # 正文图片（按顺序编号）
      2.gif
      ...
```

### JSONL 记录结构

每行一条 JSON，结构如下：

```json
{
  "article": {
    "title": "文章标题",
    "name": "文章名称",
    "author": "专栏名称",
    "publish_time": "20260324093032",
    "original_url": "http://mp.weixin.qq.com/s?...",
    "image": "原始封面图 URL",
    "image_local": "/data/images/{slug}/{id}/cover.jpg",
    "is_first": 1,
    "content_html": "正文 HTML（图片链接已替换为本地路径）"
  },
  "meta": {
    "slug": "pJMG8ZXFLd",
    "article_id": "a3b2c1d4ef",
    "fetched_at": "2026-03-24T00:04:55.374600"
  }
}
```

### 进度文件结构

`{slug}_progress.json` 记录抓取进度，用于断点续爬：

```json
{
  "slug": "pJMG8ZXFLd",
  "last_completed_page": 3,
  "next_page": 4,
  "total_articles": 90,
  "last_updated": "2026-03-24T00:04:55",
  "stop_reason": "page_done",
  "pages": {
    "1": { "status": "done", "new": 30, "skip": 0, "time": "2026-03-24 00:00:00" },
    "2": { "status": "done", "new": 28, "skip": 2, "time": "2026-03-24 00:05:00" },
    "3": { "status": "done", "new": 30, "skip": 0, "time": "2026-03-24 00:10:00" }
  }
}
```

**stop_reason 取值：**

| 值 | 含义 |
|----|------|
| `page_done` | 单页正常完成 |
| `completed` | 全部抓取完成（已到最后一页） |
| `api_limit` | API 调用次数达上限 |
| `scrape_failure` | 微信文章连续爬取失败（可能被限流） |
| `network_error` | 网络错误 |
| `all_exist` | 连续 2 页文章全部已存在 |

## 防护机制

- **去重**：基于 `original_url` 自动跳过已抓取的文章
- **断点续爬**：每页完成后立即写入进度文件，`all` 模式自动从断点继续
- **API 限流保护**：API 返回异常时立即停止，不浪费请求次数
- **微信反检测**：每篇文章间隔 3~6 秒随机延迟，携带完整浏览器 Headers
- **连续失败熔断**：连续 5 次爬取失败自动中止

## 前端预览

`server.py` 提供本地 Web 服务器（默认端口 8080），功能包括：

- 文章列表卡片展示（封面图 + 标题 + 时间）
- 搜索过滤（按标题/作者）
- 时间排序（最新/最早）
- 点击查看文章全文（图片从本地加载）
- 查看原文链接 / 复制链接
- 深色/浅色模式切换
- 分页浏览
- 响应式布局（支持移动端）

## 后续用途

抓取的数据可用于：

1. 提供给大模型训练、分析、生成 skills
2. 全量导入数据库（JSONL 格式可直接 `pandas.read_json(path, lines=True)`）
3. 生成静态 HTML 页面或前端展示
