import asyncio
import httpx
import backoff
from bs4 import BeautifulSoup
import pandas as pd
import logging
import glob

logging.basicConfig(filename='debug_logs.log', filemode='w', level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

async def on_backoff(details):
    logging.warning(f"Backing off {details['wait']:0.1f} seconds after {details['tries']} tries calling function {details['target'].__name__} with args {details['args']} and kwargs {details['kwargs']}")

async def on_giveup(details):
    logging.error(f"Giving up calling function {details['target'].__name__} after {details['tries']} tries")

@backoff.on_exception(backoff.expo,
                      (httpx.ConnectTimeout, httpx.ReadTimeout),
                      max_tries=5,
                      on_backoff=on_backoff,
                      on_giveup=on_giveup,
                      giveup=lambda e: not isinstance(e, (httpx.ConnectTimeout, httpx.ReadTimeout)),
                      jitter=backoff.full_jitter)
async def fetch_with_backoff(client, url):
    response = await client.get(url, timeout=(10.0, 30.0))
    return response

async def fetch_photo_details(client, name, total_pages):
    all_tuples = []
    for page in range(1, total_pages + 1):
        try:
            url = f"https://www.ra.ee/fotis/index.php/et/photo/search?m_messages=1&search={name.replace(' ', '+')}&q=1&page={page}"
            response = await fetch_with_backoff(client, url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "lxml")
                rows = soup.find_all('tr', class_=['odd', 'even'])
                for row in rows:
                    tds = row.find_all('td')
                    if len(tds) >= 6 and row.find('a', class_='popup_image'):
                        td_texts = [td.get_text(strip=True) for td in tds[:5]]
                        popup_image_href = row.find('a', class_='popup_image')['href']
                        all_tuples.append(tuple(td_texts + [popup_image_href]))
            else:
                logging.error(f"Failed to fetch or process page {page} for {name}. Status Code: {response.status_code}")
        except Exception as e:
            logging.error(f"An error occurred while fetching page {page} for {name}: {str(e)}")
    return all_tuples

def save_batch_to_parquet(data_tuples, batch_number):
    filename = f"photo_details_batch_{batch_number}.parquet"
    df = pd.DataFrame(data_tuples, columns=["Foto Kirjeldus", "Asukoht", "Kuupäev", "Fotograaf", "EFA_ID", "ImageHref"])
    df.to_parquet(filename, compression='snappy', index=False)
    logging.info(f"Batch {batch_number} saved to {filename}.")

def concatenate_parquet_files():
    files = glob.glob('photo_details_batch_*.parquet')
    if files:
        dfs = [pd.read_parquet(file) for file in files]
        df_final = pd.concat(dfs, ignore_index=True)
        df_final.to_parquet('photo_details_final.parquet', compression='snappy', index=False)
        logging.info("All batches concatenated into photo_details_master.parquet.")

async def main():
    subjects = [
        ("Endel Veliste", 111),
        ("Gunnar Vaidla", 157),
        ("Voldemar Maask", 7),
        ("Gunnar Loss", 203),
        ("Dmitri Prants", 20),
        ("Ilmar Prooso", 56),
        ("Isi Trapido", 41),
        ("Jaan Riet", 2734),
        ("Benita Labi", 99),
        ("Theodor John", 148),
        ("Parikas", 193),
        ("Feodor Olop", 38),
        ("Rein Zobel", 58),
        ("E. Norman", 161),
        ("Kaljo Raud", 86),
        ("Georgi Tsvetkov", 504),
        ("Oskar Vihandi", 200),
        ("V. Gorbunov", 229),
        ("O. Koska", 32),
        ("Vatser, Ü.", 26),
        ("Samussenko, V.", 65),
        ("Pavel Kuznetsov", 45),
        ("Viktor Rudko", 35),
        ("K. Liiv", 63),
    ]
    all_data = []
    batch_number = 1
    batch_size = sum(map(lambda subject: subject[1], subjects))
    batch_size = batch_size // 25

    async with httpx.AsyncClient() as client:
        tasks = [fetch_photo_details(client, name, total_pages) for name, total_pages in subjects]
        results = await asyncio.gather(*tasks)
        for data in results:
            all_data.extend(data)
            while len(all_data) >= batch_size:
                save_batch_to_parquet(all_data[:batch_size], batch_number)
                all_data = all_data[batch_size:]
                batch_number += 1

    if all_data:
        save_batch_to_parquet(all_data, batch_number)

    concatenate_parquet_files()

if __name__ == "__main__":
    asyncio.run(main())
