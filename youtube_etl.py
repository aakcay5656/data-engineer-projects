import pandas as pd
import json 
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


import googleapiclient.discovery

def run_etl():
    
    def process_comments(response_items):
    		comments = []
    		for comment in response_items:
    				author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
    				comment_text = comment['snippet']['topLevelComment']['snippet']['textOriginal']
    				publish_time = comment['snippet']['topLevelComment']['snippet']['publishedAt']
    				comment_info = {'author': author, 
    						'comment': comment_text, 'published_at': publish_time}
    				comments.append(comment_info)
    		print(f'Finished processing {len(comments)} comments.')
    		return comments
    
    def main():
        # Disable OAuthlib's HTTPS verification when running locally.
        # *DO NOT* leave this option enabled in production.
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    
        api_service_name = "youtube"
        api_version = "v3"
        DEVELOPER_KEY = os.environ["YOUTUBE_APİ_KEY"]
    
        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey = DEVELOPER_KEY)
    
        request = youtube.commentThreads().list(
            part="snippet, replies",
            videoId="SaVs9_I-rLs",
            maxResults=100 
        )
        response = request.execute()
    
        # print(response)
        
        comments = process_comments(response.get('items'))
        if comments:
            df = pd.DataFrame(comments)
            csv_file_path = "youtube_comments.csv"
            df.to_csv(csv_file_path,index=False,encoding="utf-8-sig")
        else:
            print("comments is empty")
    


if __name__ == "__main__":
    main()


