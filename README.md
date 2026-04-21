# Scrape Thousands of Web Pages in Parallel in Python

Scrape 1,000,000 URLs across 1,000 workers at the same time with retry, backoff, and polite per-worker rate limiting.

## The Problem

You need to scrape 1M product pages, news articles, or SERPs. `asyncio` on one box tops out around a few thousand concurrent connections and chokes on CPU (HTML parsing, DNS, TLS). A single machine also gives you one IP and one point of failure.

Scrapy Cluster, Crawlee, or Playwright on Kubernetes all work, but they're infrastructure. Running 1,000 VMs yourself means AMIs, a queue, a retry layer, and a deployer.

You want `scrape(url)` to run on 1,000 machines at the same time, with retries and a reasonable delay per worker.

## The Solution (Burla)

Chunk 1M URLs into 2,000 tasks of 500 URLs each. Set `max_parallelism=1000` so exactly 1,000 workers run at once. Each worker pulls its 500 URLs, scrapes them with `httpx` + `selectolax`, backs off on errors, and returns parsed rows. Stream results with `generator=True`.

No Scrapy setup, no Kubernetes, no queue.

## Example

```python
import random
from burla import remote_parallel_map

with open("urls.txt") as f:
    urls = [u.strip() for u in f if u.strip()]

CHUNK = 500
chunks = [urls[i : i + CHUNK] for i in range(0, len(urls), CHUNK)]
print(f"{len(urls):,} URLs in {len(chunks)} chunks")


def scrape_chunk(urls: list[str]) -> list[dict]:
    import random
    import time
    import httpx
    from selectolax.parser import HTMLParser

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; MyBot/1.0; +https://example.com/bot)",
        "Accept-Language": "en-US,en;q=0.9",
    }

    out = []
    with httpx.Client(
        http2=True, timeout=20.0, headers=HEADERS, follow_redirects=True
    ) as client:
        for url in urls:
            for attempt in range(4):
                try:
                    r = client.get(url)
                    if r.status_code in (429, 503):
                        time.sleep(2 ** attempt + random.random())
                        continue
                    r.raise_for_status()
                    tree = HTMLParser(r.text)
                    title = tree.css_first("title")
                    price = tree.css_first("meta[itemprop=price]")
                    out.append({
                        "url": url,
                        "status": r.status_code,
                        "title": title.text(strip=True) if title else None,
                        "price": price.attributes.get("content") if price else None,
                    })
                    break
                except (httpx.HTTPError, httpx.TimeoutException) as e:
                    if attempt == 3:
                        out.append({"url": url, "error": str(e)})
                    else:
                        time.sleep(2 ** attempt + random.random())
            time.sleep(0.5 + random.random() * 0.5)  # polite per-worker delay
    return out


# 2,000 chunks capped to 1,000 workers running in parallel
import json
with open("scraped.jsonl", "w") as f:
    for chunk_rows in remote_parallel_map(
        scrape_chunk,
        chunks,
        func_cpu=1,
        func_ram=2,
        max_parallelism=1000,
        generator=True,
    ):
        for row in chunk_rows:
            f.write(json.dumps(row) + "\n")
```

## Why This Is Better

**vs Scrapy / Scrapy Cluster** — Scrapy is great, but Scrapy Cluster is Redis + Kafka + ZooKeeper + custom spiders. Burla is a function and a list.

**vs Playwright on Kubernetes** — you maintain a cluster, a Helm chart, a work queue, and a worker image. For plain HTML (no JS), you don't need a browser at all.

**vs Ray / Dask** — both need a cluster up front and neither has a natural "cap concurrent workers at 1,000" primitive for fan-out.

**vs AWS Lambda fan-out** — per-account concurrency limits, 15-minute timeouts, and weird networking. Not built for 1M URLs across 1k workers.

## How It Works

You chunk the URL list. Burla holds 1,000 workers live at once via `max_parallelism`. Each worker processes its assigned chunks back-to-back, reusing one `httpx.Client` per worker (so connection pooling + HTTP/2 keep-alive work). `generator=True` streams results chunk-by-chunk.

## When To Use This

- Bulk scraping of static HTML across 10k-10M URLs.
- SERP scraping, product catalogs, news crawls, real-estate listings.
- One-off backfills of a site's entire archive.
- Scraping behind a rotating proxy (configure in `httpx.Client`).

## When NOT To Use This

- JavaScript-heavy sites requiring a full browser — use Playwright with `func_cpu=2, func_ram=4` per worker, and expect higher cost.
- Sites with aggressive anti-bot (Cloudflare Turnstile, PerimeterX) — you need residential proxies and a browser fingerprinting solution, not just parallelism.
- Always-on crawlers that need a scheduling graph and dedupe across runs — use Scrapy with a proper item pipeline.
