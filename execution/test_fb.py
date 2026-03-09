import asyncio
from playwright.async_api import async_playwright

async def test_fb():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # desktop User agent to avoid m.facebook limitations
        page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        url = "https://www.facebook.com/search/videos/?q=dubai%20real%20estate"
        await page.goto(url)
        await page.wait_for_timeout(5000)
        await page.screenshot(path=".tmp/fb_test.png")
        
        html = await page.content()
        print(f"FB Page loaded. HTML length: {len(html)}")
        
        links = await page.evaluate('''() => {
            return Array.from(document.querySelectorAll('a'))
                .map(a => a.href)
                .filter(href => href.includes('/watch') || href.includes('/video'));
        }''')
        print(f"Found FB links: {list(set(links))[:5]}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_fb())
