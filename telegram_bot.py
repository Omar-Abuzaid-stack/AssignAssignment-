import os
import json
import logging
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

load_dotenv()
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hello! I am your Real Estate Auto-Bot. Send 'run report' to trigger the full pipeline end-to-end.")

async def run_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.lower().strip()
    if text == "run report":
        await update.message.reply_text("🚀 Starting pipeline... This takes a few minutes.\n\n(1/6) Collecting videos (yt-dlp, Instaloader, Playwright)...")
        
        try:
            proc = await asyncio.create_subprocess_exec("python3", "execution/collect_videos.py")
            await proc.wait()
            
            await update.message.reply_text("(2/6) Cleaning and deduplicating...")
            proc = await asyncio.create_subprocess_exec("python3", "execution/clean_videos.py")
            await proc.wait()
            
            await update.message.reply_text("(3/6) Evaluating virality scores...")
            proc = await asyncio.create_subprocess_exec("python3", "execution/evaluate_videos.py")
            await proc.wait()
            
            await update.message.reply_text("(4/6) Summarizing angles with Mistral AI...")
            proc = await asyncio.create_subprocess_exec("python3", "execution/summarize_videos.py")
            await proc.wait()
            
            await update.message.reply_text("(5/6) Generating PDF report...")
            proc = await asyncio.create_subprocess_exec("python3", "execution/distribute_report.py")
            await proc.wait()
            
            pdf_path = ".tmp/report.pdf"
            proc = await asyncio.create_subprocess_exec(
                "npx", "--yes", "markdown-pdf", ".tmp/report.md", "-o", pdf_path
            )
            await proc.wait()
            
            await update.message.reply_text("(6/6) Uploading PDF via Google Drive API...")
            proc = await asyncio.create_subprocess_exec(
                "python3", "execution/upload_to_gdrive.py",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            if proc.returncode != 0:
                err_text = stderr.decode()
                await update.message.reply_text(f"❌ Error uploading to Drive. Have you authenticated?\n{err_text}")
                return
                
            # The last line printed should be the link
            lines = stdout.decode().strip().split('\n')
            gdrive_link = lines[-1] if lines else "Link not found"
            
            # Count platforms
            platform_counts = {}
            if os.path.exists(".tmp/cleaned_videos.json"):
                with open(".tmp/cleaned_videos.json", "r") as f:
                    data = json.load(f)
                    for item in data:
                        p = item.get("platform", "unknown").capitalize()
                        platform_counts[p] = platform_counts.get(p, 0) + 1
            
            # Get top 3 videos
            top_3_text = ""
            if os.path.exists(".tmp/evaluated_videos.json"):
                with open(".tmp/evaluated_videos.json", "r") as f:
                    vids = json.load(f)
                    trending = [v for v in vids if v.get("is_trending")][:3]
                    for i, v in enumerate(trending, 1):
                        top_3_text += f"{i}. {v.get('url')}\n"
            
            # Check for errors
            error_note = ""
            if os.path.exists(".tmp/scrape_errors.log"):
                with open(".tmp/scrape_errors.log", "r") as f:
                    errors = f.read().strip()
                    if errors:
                        error_note = f"\n⚠️ Note: Some platforms had issues:\n{errors}\n"

            summary_text = "\n".join([f"- {p}: {c} videos" for p, c in platform_counts.items()])
            date_str = datetime.now().strftime("%Y-%m-%d")
            
            final_msg = (
                f"Real Estate Report Successfully Generated ({date_str})\n\n"
                f"Total Videos: {sum(platform_counts.values())}\n"
                f"{summary_text}\n\n"
                f"Top 3 Trending Videos:\n{top_3_text}\n"
                f"{error_note}\n"
                f"Google Drive Link:\n{gdrive_link}"
            )
            
            if update and update.message:
                await update.message.reply_text(final_msg)
            else:
                # This was a scheduled job, we need a chat_id to send to
                # For now we assume a bot owner ID or similar if available
                owner_id = os.getenv("TELEGRAM_OWNER_ID")
                if owner_id:
                    await context.bot.send_message(chat_id=owner_id, text=final_msg)
            
        except Exception as e:
            msg = f"❌ Pipeline encountered an error: {str(e)}"
            if update and update.message:
                await update.message.reply_text(msg)
            logger.error(msg)

async def scheduled_report(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Triggering scheduled weekly report...")
    # Mocking an update object for the run_report function
    class MockMessage:
        def __init__(self): self.text = "run report"
        async def reply_text(self, t): logger.info(f"Bot Status Update: {t}")
    class MockUpdate:
        def __init__(self): self.message = MockMessage()
    
    await run_report(MockUpdate(), context)

def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        print("Error: TELEGRAM_BOT_TOKEN not found in .env. Please add it.")
        return
        
    app = Application.builder().token(token).build()
    
    # Setup Scheduler
    scheduler = AsyncIOScheduler(timezone=pytz.timezone('Asia/Dubai'))
    # Every Monday at 9:00 AM UAE Time
    scheduler.add_job(scheduled_report, CronTrigger(day_of_week='mon', hour=9, minute=0), args=[ContextTypes.DEFAULT_TYPE])
    scheduler.start()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, run_report))
    
    print("Telegram Bot is active 24/7 on Railway. Monday 9am UAE reports scheduled.")
    app.run_polling()

if __name__ == '__main__':
    main()
