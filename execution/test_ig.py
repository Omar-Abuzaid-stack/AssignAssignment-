import asyncio
from playwright.async_api import async_playwright

async def test_ig():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        url = "https://www.instagram.com/explore/tags/dubairealestate/"
        await page.goto(url)
        await page.wait_for_timeout(5000)
        await page.screenshot(path=".tmp/ig_test.png")
        
        links = await page.evaluate('''() => {
            return Array.from(document.querySelectorAll('a'))
                .map(a => a.href)
                .filter(href => href.includes('/p/') || href.includes('/reel/'));
        }''')
        print(f"Found IG links: {list(set(links))[:5]}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_ig())
