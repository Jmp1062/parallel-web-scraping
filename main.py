import random
import httpx  # noqa: F401 -- top-level import so Burla installs httpx on workers
import selectolax  # noqa: F401 -- top-level import so Burla installs selectolax on workers
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
        grow=True,
    ):
        for row in chunk_rows:
            f.write(json.dumps(row) + "\n")
