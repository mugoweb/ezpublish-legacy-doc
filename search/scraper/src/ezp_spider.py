from datetime import datetime
import asyncio
from typing import List
import httpx
from loguru import logger as log
from parsel import Selector
from urllib.parse import urljoin
import json
from lunr import lunr
import re


def parse(responses: List[httpx.Response]) -> List[dict]:
    """parse responses ofr index docs"""
    log.info(f"parsing docs from {len(responses)} responses")
    docs = []
    for resp in responses:
        sel = get_clean_html_tree(resp)

        titles = []
        sections = []
        for doc in sel.xpath("//div[contains(@class, 'content-view-full')]//div[contains(@class, 'template-object')]"):
            section = []
            #todo: h2 section
            for node in doc.xpath("*"):
                # separate page by <hX> nodes
                if re.search(r"h\d", node.root.tag):
                    titles.append(node);
                else:
                    section.append(node)
            if section:
                sections.append(section)

        page_title = sel.xpath("//h1/text()").get("").strip()
        for section in sections:
            data = {
                "title": f"{page_title} | "
                + " | ".join(s.xpath(".//text()").get("") for s in titles).strip(),
                "text": "".join(s.get() for s in section).strip(),
            }
            url_with_id_pointer = (
                str(resp.url) 
            )
            data["location"] = url_with_id_pointer
            docs.append(data)
    return docs

def build_index(docs: List[dict]):
    """build lunrjs index from provided list of documents"""
    log.info(f"building index from {len(docs)} documents")
    config = {
        "lang": ["en"],
        "min_search_length": 1,
    }
    page_dicts = {"docs": docs, "config": config}
    idx = lunr(
        ref="location",
        fields=("title", "text"),
        documents=docs,
        languages=["en"],
    )
    page_dicts["index"] = idx.serialize()
    return json.dumps(page_dicts, sort_keys=True, separators=(",", ":"), indent=2)

def find_urls(resp: httpx.Response, xpath: str) -> set:
    """find crawlable urls in a response from an xpath"""
    found = set()
    urls = Selector(text=resp.text).xpath(xpath).getall()
    for url in urls:
        url = httpx.URL(resp.url).join(url.split("#")[0])
        if url.host != resp.url.host:
            log.debug(f"skipping url of a different hostname: {url.host}")
            continue
        if url.path.find("Technical-manual/4.x") == -1:
            log.debug(f"skipping incorrect version")
            continue
        found.add(str(url))
    return found


async def crawl(url, follow_xpath: str, session: httpx.AsyncClient, max_depth=10) -> List[httpx.Response]:
    """crawl source with provided follow rules"""
    urls_seen = set()
    urls_to_crawl = [url]
    all_responses = []
    depth = 0
    while urls_to_crawl:
        # first we want to protect ourselfes from accidental infinite crawl loops
        if depth > max_depth:
            log.error(
                f"max depth reached with {len(urls_to_crawl)} urls left in the crawl queue")
            break
        log.info(f"scraping: {len(urls_to_crawl)} urls")
        responses = await asyncio.gather(*[session.get(url) for url in urls_to_crawl])
        found_urls = set()
        for resp in responses:
            all_responses.append(resp)
            found_urls = found_urls.union(find_urls(resp, xpath=follow_xpath))
        # find more urls to crawl that we haven't visited before:
        urls_to_crawl = found_urls.difference(urls_seen)
        urls_seen = urls_seen.union(found_urls)
        depth += 1
    log.info(f"found {len(all_responses)} responses")
    return all_responses


def get_clean_html_tree(
    resp: httpx.Response, remove_xpaths=(".//figure", ".//*[contains(@class,'carousel')]")
):
    """cleanup HTML tree from domain specific details like classes"""
    sel = Selector(text=resp.text)
    for remove_xp in remove_xpaths:
        for rm_node in sel.xpath(remove_xp):
            rm_node.remove()
    allowed_attributes = ["src", "href", "width", "height", "class", "id", "title"]
    for el in sel.xpath("//*"):
        for k in list(el.root.attrib):
            if k in allowed_attributes:
                continue
            el.root.attrib.pop(k)
        # turn all link to absolute
        if el.root.attrib.get("href"):
            el.root.attrib["href"] = urljoin(
                str(resp.url), el.root.attrib["href"])
        if el.root.attrib.get("src"):
            el.root.attrib["src"] = urljoin(
                str(resp.url), el.root.attrib["src"])
    return sel


async def run(url:str, header:str):
    limits = httpx.Limits(max_connections=3, keepalive_expiry=None)
    headers = {"User-Agent": header}
    async with httpx.AsyncClient(limits=limits, headers=headers, timeout=None) as session:
        responses = await crawl(
            # our starting point url
            url=url,
            follow_xpath="//div[contains(@id, 'main')]//a/@href",
            session=session,
            max_depth=10
        )
        docs = parse(responses)
        if len(docs) > 0:
            with open("ezp_index.json", "w") as f:
                f.write(build_index(docs))
        else:
            log.info('no docs found')

if __name__ == "__main__":
    now = datetime.now()
    now_str = f"{now:%Y-%m-%d %H:%M:%S%z}"
    asyncio.run(run(url="https://ezpublishdoc.mugo.ca/eZ-Publish/Technical-manual/4.x.html", header=now_str))
