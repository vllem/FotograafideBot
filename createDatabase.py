import asyncio
import httpx
import backoff
from bs4 import BeautifulSoup
import pandas as pd
import logging
import logging.config
import traceback
import re

LOGGING_CONFIG = {
    "version": 1,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "http",
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "http",
            "filename": "debug.log"
        }
    },
    "formatters": {
        "http": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }
    },
    'loggers': {
        'httpx': {
            'handlers': ['console', 'file'],
            'level': 'WARN',
        },
        'httpcore': {
            'handlers': ['console', 'file'],
            'level': 'WARN',
        },
        'root': {
            'handlers': ['console', 'file'],
            'level': 'INFO'
        }
    }
}

logging.config.dictConfig(LOGGING_CONFIG)

async def on_backoff(details):
    logging.warning(f"Backing off {details['wait']:0.1f} seconds after {details['tries']} tries calling function {details['target'].__name__} with args {details['args']} and kwargs {details['kwargs']}")

async def on_giveup(details):
    logging.error(f"Giving up calling function {details['target'].__name__} after {details['tries']} tries")

@backoff.on_exception(backoff.expo,
                      (httpx.ConnectTimeout, httpx.ReadTimeout),
                      max_tries=100,
                      on_backoff=on_backoff,
                      on_giveup=on_giveup,
                      giveup=lambda e: not isinstance(e, (httpx.ConnectTimeout, httpx.ReadTimeout)),
                      jitter=backoff.full_jitter)

async def fetch_with_backoff(client, url):
    response = await client.get(url, timeout=(30.0, 60.0))
    return response

def save_to_parquet(data_tuples):
    filename = f"photo_details.parquet"
    new_df = pd.DataFrame(data_tuples, columns=["Foto Kirjeldus", "Asukoht", "Kuupäev", "Fotograaf", "EFA_ID", "ImageHref", "PageNr"])
    if old_df is not None:
        df = pd.concat([old_df, new_df])
    else:
        df = new_df
        
    df.to_parquet(filename, compression='snappy', index=False)
    logging.info(f"Saved to {filename}.")

try:
    getPageCountURL = "https://www.ra.ee/fotis/index.php/et/photo/search?page=1"
    getPageCountResponse = httpx.get(getPageCountURL)
    getPageCountSoup = BeautifulSoup(getPageCountResponse.content, "lxml")
    li_last = getPageCountSoup.find('li', class_='last')
    pageCount = 36850
    if li_last:
        a_tag = li_last.find('a')
        if a_tag and 'href' in a_tag.attrs:
            href = a_tag['href']
            match = re.search(r'page=(\d+)', href)
            if match:
                pageCount = int(match.group(1))
except httpx.ReadTimeout:
    print("The request timed out while waiting for a response.")

async def fetch_page(session, nr):
    page_data = list()
    url = f"https://www.ra.ee/fotis/index.php/et/photo/search?page={nr}"
    response = await fetch_with_backoff(session, url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "lxml")
        rows = soup.find_all('tr', class_=['odd', 'even'])
        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 6 and row.find('a', class_='popup_image'):
                td_texts = [td.get_text(strip=True) for td in tds[:5]]
                popup_image_href = row.find('a', class_='popup_image')['href']
                td_texts = [None if x == "" else x for x in td_texts]
                page_data.append(tuple(td_texts + [popup_image_href, nr]))
            
    else:
        logging.error(f"Failed to fetch or process page {url}. Status Code: {response.status_code}")

    return page_data

async def iterate_page(queue, session, worker):
    global globalCount, all_data

    while True:

        t = await queue.get()

        try:
            
            page_data = await fetch_page(session, t)
            globalCount += 1
            logging.info(f"[{worker}][{globalCount}/{realPageCount}] Fetched page nr {t}")

            for single_info in page_data:
                all_data.append(single_info)

        except RuntimeError:
            break

        except httpx.ReadError:
            break

        except Exception as e:
            globalCount += 1
            logging.error(f"[{worker}][{globalCount}/{realPageCount}] An error occurred while fetching page {t}: {repr(e)}")

        finally:
            queue.task_done()

async def main():
    global globalCount, all_data, old_df, realPageCount

    #queue make
    queue = asyncio.Queue()
    tasks = []
    all_data = []
    globalCount = 0
    workerCount = 15
    unique_page_numbers = []

    try:

        async with httpx.AsyncClient() as session:

            # add page nr to queue, filter out already existing
            try:
                old_df = pd.read_parquet("photo_details.parquet", engine="pyarrow")
                unique_page_numbers = old_df["PageNr"].unique().tolist()
            except FileNotFoundError:
                old_df = None
            
            realPageCount = pageCount - len(unique_page_numbers)
            for i in range(pageCount):
                if not i in unique_page_numbers:
                    await queue.put(i)

            # create workers
            for i in range(workerCount):
                task = asyncio.create_task(iterate_page(queue, session, f'W{i}'))
                tasks.append(task)

            #asyncio task bs
            await queue.join()

    except Exception as e:
        traceback.print_exc()

    finally:
        for task in tasks:
            task.cancel()

        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)
        # save all to file
        save_to_parquet(all_data)

if __name__ == "__main__":
    asyncio.run(main())
