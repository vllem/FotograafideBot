import asyncio
import json
import aiofiles
import pandas as pd
import httpx

async def read_credentials(filename):
    async with aiofiles.open(filename, 'r') as file:
        credentials = json.loads(await file.read())
    return credentials

async def get_img(session, href):
    url = "https://www.ra.ee" + href
    filename = href.split("=")[-1]
    response = await session.get(url)

    if response.status_code == 200:
        # get file format
        content_disposition = response.headers.get("Content-Disposition")
        suggested_format = "idk"
        if content_disposition:
            # Extract filename from Content-Disposition header
            parts = content_disposition.split(";")
            for part in parts:
                if part.strip().startswith("filename="):
                    suggested_format = part.split("=", 1)[1].strip().split(".")[-1].strip('"')
                    break

        async with aiofiles.open(f"imgs/{filename}.{suggested_format}", 'wb') as file:
            await file.write(response.content)
        print(f"Image downloaded successfully to {filename}.{suggested_format}")
    else:
        print(f"Failed to download image from {url}. Status code: {response.status_code}")

async def query_random_image(dataframe):
    # Sample a random row from the DataFrame
    random_row = dataframe.sample(n=1)
    imgref = random_row['ImageHref'].iloc[0]
    return imgref

async def main():
    credentials = await read_credentials('creds.json')
    df = pd.read_parquet("photo_details.parquet", engine="pyarrow")

    imgref = await query_random_image(df)  # Get a random image reference

    async with httpx.AsyncClient() as session:
        await get_img(session, imgref)

if __name__ == "__main__":
    asyncio.run(main())
