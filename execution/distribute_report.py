import os
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_markdown(videos, output_path):
    platforms = {}
    for v in videos:
        p = v.get("platform", "unknown").capitalize()
        if p not in platforms:
            platforms[p] = []
        platforms[p].append(v)
        
    date_str = datetime.now().strftime("%Y-%m-%d")
    md_content = f"# Trending Real Estate Videos in UAE ({date_str})\n\n"
    md_content += "This report contains the top trending UAE real estate videos (Dubai, Abu Dhabi, etc.) tracked across YouTube Shorts and TikTok.\n"
    md_content += "---\n\n"
    
    md_content = f"# Trending Real Estate Videos — {date_str}\n\n"
    md_content += "## 📈 Weekly Market Summary\n"
    md_content += f"This report analyzes trending content across TikTok, Instagram, Facebook, and YouTube Shorts focusing on the UAE Real Estate market. Total videos analyzed: {len(videos)}.\n\n"
    
    # Check for errors
    if os.path.exists(".tmp/scrape_errors.log"):
        with open(".tmp/scrape_errors.log", "r") as f:
            errors = f.read().strip()
            if errors:
                md_content += "### ⚠️ Platform Advisory\n"
                md_content += f"The following platforms encountered collection issues and may have limited data:\n{errors}\n\n"

    platforms = ["tiktok", "instagram", "facebook", "youtube"]
    
    for p in platforms:
        p_vids = [v for v in videos if v.get("platform") == p][:10]
        if not p_vids: continue
        
        md_content += f"## 🎥 {p.upper()} Top 10\n\n"
        
        for i, v in enumerate(p_vids):
            md_content += f"### {i+1}. {v.get('data', {}).get('title', 'Video Post')[:60]}...\n"
            md_content += f"- **Link:** [Watch here]({v.get('url')})\n"
            md_content += f"- **Creator:** @{v.get('creator_handle', 'Unknown')}\n"
            md_content += f"- **Engagement:** {v.get('view_count', 0):,} Views | {v.get('likes', 0):,} Likes | {v.get('comments', 0):,} Comments\n"
            
            summary = v.get("llm_summary", {})
            if summary:
                md_content += f"- **Target Audience:** {summary.get('target_audience', 'N/A')}\n"
                md_content += f"- **Hook Style:** {summary.get('hook_style', 'N/A')}\n"
                md_content += f"- **Video Format:** {summary.get('video_format', 'N/A')}\n"
                md_content += f"- **Content Angle:** {summary.get('content_angle', 'N/A')}\n"
                md_content += f"- **Success Breakdown:** {summary.get('virality_explanation', 'N/A')}\n"
            else:
                md_content +=f"- *Detailed analysis unavailable*\n"
                
            md_content += "\n---\n\n"

    # Add Content Opportunities
    if os.path.exists(".tmp/content_opportunities.json"):
        md_content += "## 💡 Content Opportunities (Strategic Gaps)\n"
        md_content += "Based on this week's trending data, here are 3 underserved areas your brand can exploit:\n\n"
        with open(".tmp/content_opportunities.json", "r") as f:
            gaps = json.load(f)
            for gap in gaps:
                md_content += f"### 🚀 {gap.get('title')}\n"
                md_content += f"{gap.get('description')}\n\n"
            
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(md_content)
        
    logger.info(f"Generated Markdown report at {output_path}")

def main():
    logger.info("Starting Final Report Distribution...")
    
    input_path = ".tmp/summarized_videos.json"
    output_path = ".tmp/report.md"
    
    if not os.path.exists(input_path):
        logger.error(f"Summarized videos not found at {input_path}")
        input_path = ".tmp/evaluated_videos.json"
        if not os.path.exists(input_path):
             logger.error("No valid input files found to generate report.")
             return
             
    with open(input_path, 'r', encoding='utf-8') as f:
        videos = json.load(f)
        
    if not videos:
        logger.warning("No data found to distribute.")
        return
        
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    generate_markdown(videos, output_path)
    logger.info(f"Markdown pre-report saved to {output_path}. Use `npx markdown-pdf` to convert this to PDF.")

if __name__ == "__main__":
    main()
