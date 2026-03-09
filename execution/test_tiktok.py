import asyncio
from playwright.async_api import async_playwright

async def test_tiktok():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        url = "https://www.tiktok.com/tag/dubairealestate"
        await page.goto(url)
        await page.wait_for_timeout(5000)
        await page.screenshot(path=".tmp/tiktok_test.png")
        
        html = await page.content()
        print(f"TikTok Page loaded. HTML length: {len(html)}")
        
        links = await page.evaluate('''() => {
            return Array.from(document.querySelectorAll('a'))
                .map(a => a.href)
                .filter(href => href.includes('/video/'));
        }''')
        print(f"Found Tiktoks: {list(set(links))[:5]}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_tiktok())
