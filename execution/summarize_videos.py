import os
import sys
import json
import logging
from dotenv import load_dotenv

try:
    from mistralai.client import MistralClient
    from mistralai.models.chat_completion import ChatMessage
except ImportError:
    logging.error("mistralai package is not installed. Please run `pip install mistralai<1.0.0`.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_mistral_client():
    load_dotenv()
    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        logger.error("MISTRAL_API_KEY not found in .env. Required for summarize_videos.py.")
        sys.exit(1)
    return MistralClient(api_key=api_key)

def summarize_video(client, video_data):
    system_prompt = """
    You are an expert real estate social media analyst specializing in the UAE market.
    Your job is to analyze the metadata of a trending real estate video and provide a deep strategic breakdown.
    
    Provide a JSON summary containing exactly these keys:
    {
      "hook_style": "Exact tactic used in first 3 seconds.",
      "video_format": "The visual structure.",
      "content_angle": "The core message intent.",
      "target_audience": "Who is this specific video targeting (e.g. Investors, Luxury Seekers, First-time buyers)?",
      "virality_explanation": "A 2-3 sentence technical breakdown of why this video performed well (psychology, timing, production)."
    }
    Return ONLY pure JSON.
    """
    
    raw_data = video_data.get("data", {})
    description = raw_data.get("desc", raw_data.get("caption", raw_data.get("text", raw_data.get("title", "No text provided"))))
    
    user_prompt = f"""
    Platform: {video_data.get('platform')}
    Creator: {video_data.get('creator_handle')}
    Views: {video_data.get('view_count')}
    Likes: {video_data.get('likes')}
    Description / Text: {description}
    """

    try:
        response = client.chat(
            model="mistral-large-latest",
            response_format={"type": "json_object"},
            messages=[
                ChatMessage(role="system", content=system_prompt),
                ChatMessage(role="user", content=user_prompt)
            ],
            temperature=0.3
        )
        content = response.choices[0].message.content
        return json.loads(content)
    except Exception as e:
        logger.error(f"Error summarizing video: {e}")
        return {
            "hook_style": "Unknown",
            "video_format": "Unknown",
            "content_angle": "Unknown",
            "success_reason": "Failed to analyze."
        }

def main():
    logger.info("Starting Video Summarization...")
    
    input_path = ".tmp/evaluated_videos.json"
    output_path = ".tmp/summarized_videos.json"
    
    if not os.path.exists(input_path):
        logger.error(f"File not found: {input_path}")
        return
        
    with open(input_path, 'r', encoding='utf-8') as f:
        videos = json.load(f)
        
    if not videos:
        logger.warning("No trending videos to summarize.")
        return

    client = get_mistral_client()
    
    summarized_videos = []
    
    trending_videos = [v for v in videos if v.get("is_trending", False)]
    
    logger.info(f"Summarizing {len(trending_videos)} trending videos using Mistral LLM...")
    
    for i, v in enumerate(trending_videos):
        logger.info(f"Summarizing {i+1}/{len(trending_videos)}: {v.get('url')}...")
        summary = summarize_video(client, v)
        v["llm_summary"] = summary
        summarized_videos.append(v)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(summarized_videos, f, indent=2, ensure_ascii=False)
        
    logger.info(f"Saved summarized videos to {output_path}")
    
    # Identify Content Opportunities
    if summarized_videos:
        identify_content_opportunities(client, summarized_videos)

def identify_content_opportunities(client, summarized_videos):
    logger.info("Identifying content opportunities (market gaps)...")
    
    # Sample some summaries for the LLM
    summaries_text = ""
    for v in summarized_videos[:15]:
        s = v.get("llm_summary", {})
        summaries_text += f"- Title/Desc: {v.get('data', {}).get('title', 'N/A')} | Hook: {s.get('hook_style')} | Angle: {s.get('content_angle')}\n"
    
    prompt = f"""
    You are a strategic advisor for UAE Real Estate brands.
    Analyze these highly trending videos:
    {summaries_text}
    
    Identify 3 major content gaps or underserved 'Opportunities' that a brand could exploit this week (e.g., lack of authentic market data, missed target audience X, better visual format Y).
    Return ONLY a JSON list of 3 objects with keys 'title' and 'description'.
    """
    
    try:
        response = client.chat(
            model="mistral-large-latest",
            response_format={"type": "json_object"},
            messages=[ChatMessage(role="user", content=prompt)]
        )
        content = response.choices[0].message.content.strip()
        gaps = json.loads(content)
        # Handle if the model returned e.g. {"opportunities": [...]}
        if isinstance(gaps, dict) and "opportunities" in gaps:
            gaps = gaps["opportunities"]
        elif isinstance(gaps, dict) and "gaps" in gaps:
            gaps = gaps["gaps"]
            
        with open(".tmp/content_opportunities.json", "w") as f:
            json.dump(gaps, f, indent=2)
        logger.info("Content opportunities saved to .tmp/content_opportunities.json")
    except Exception as e:
        logger.error(f"Failed to identify content gaps: {e}")

if __name__ == "__main__":
    main()
