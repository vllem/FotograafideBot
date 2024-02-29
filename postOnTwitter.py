import asyncio
from twikit.twikit_async import Client
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
        print(f"Image downloaded successfully to {filename}")
    else:
        print(f"Failed to download image from {url}. Status code: {response.status_code}")


async def query_images(dataframe, description=None, author=None, location=None, date=None):
    filtered_df = dataframe.copy()
    if description:
        filtered_df = filtered_df[filtered_df['Foto Kirjeldus'].str.contains(description, case=False)]
    if author:
        filtered_df = filtered_df[filtered_df['Fotograaf'] == author]
    if location:
        filtered_df = filtered_df[filtered_df['Asukoht'] == location]
    if date:
        filtered_df = filtered_df[filtered_df['Kuupäev'] == date]

    # Get list of imgref values from filtered DataFrame
    imgref_list = filtered_df['ImageHref'].tolist()
    return imgref_list

async def main():
    credentials = await read_credentials('creds.json')
    df = pd.read_parquet("photo_details.parquet", engine="pyarrow")
    #await client.login(
    #    auth_info_1=credentials["username"],
    #    auth_info_2=credentials["email"],
    #    password=credentials["password"]
    #)
    #print(credentials)

    imgrefs = await query_images(df, description="Jõuga kääpa põhja-lõuna profiil.")
    print(imgrefs)

    async with httpx.AsyncClient() as session:
        for href in imgrefs:
            await get_img(session, href)

if __name__ == "__main__":
    asyncio.run(main())
