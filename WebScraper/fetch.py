import asyncio
from MaxReentriesReached.MaxReentriesReached import MaxReentriesReached

async def fetch(url, session):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
        except asyncio.TimeoutError:
            print(f"Timeout error for URL: {url}, retrying.....")
        await asyncio.sleep(2 ** attempt)
    else:
        # print(f"Max retries reached for URL: {url}")
        raise MaxReentriesReached(f"Max retries reached for URL: {url}")