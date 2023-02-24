import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
from multiprocessing import Process

def download_video(direct_url: str, video_name: str):
    # Example direct download link: https://assets.mixkit.co/videos/download/mixkit-dog-catches-a-ball-in-a-river-1494.mp4
    data = requests.get(direct_url, headers={"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"})
    with open(f"./parsed/{video_name}.mp4", "wb") as fd:
        fd.write(data.content)
    print(f"Downloaded: {video_name}")

def process_chunk(chunk, pid):
    global global_df
    for video in chunk:
        download_video(video["url"], video["video_name"])
        #print(f"From: {pid}")
        #global_df = pd.concat([global_df, pd.DataFrame([{"video": f"{video['video_name']}.mp4", "description": video["description"]}])])
        #global_df.to_csv('captions.csv') # Checkpoint save after each new video

def parse_page(query: str, page_id: int = 1, url: str = "https://mixkit.co/free-stock-video", base_domain: str = "https://assets.mixkit.co/videos/download"):
    global parsed_counter, download_tasks, global_df
    page_html = requests.get(f"{url}/{query}?page={page_id}", headers={"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}).text
    soup = BeautifulSoup(page_html, "html.parser")
    pagination_items = soup.find_all("a", {"class": "pagination__link"})
    max_page = int(pagination_items[-1].text)
    video_divs = soup.find_all("div", {"class": "item-grid-item"}) 
    print(f"Fetched {len(video_divs)} from page: {page_id}")
    for video in video_divs:
        video_base = video.find_all("a", {"class": "item-grid-video-player__overlay-link"})[0]
        href = video_base.get("href")
        video_description = video_base.text.rstrip().lstrip()
        try: 
            video_description = video.find_all("p", {"class": "item-grid-card__description"})[0].text
        except IndexError: # No description for this video
            pass
        #print(href, video_description)
        #print(f"Processing video {parsed_counter}")
        url = href.replace("/free-stock-video/", "mixkit-")
        video_name = url[:-1]
        direct_url = f"{base_domain}/{url[:-1]}.mp4"
        #print(direct_url)
        download_tasks.append({"url": direct_url, "description": video_description, "video_name": video_name})
        global_df = pd.concat([global_df, pd.DataFrame([{"video": f"{video_name}.mp4", "description": video_description}])])
        #print(global_df)
        parsed_counter += 1
    return max_page

if __name__ == "__main__":
    KEYWORDS = ["dog", "transport", "food", "animal", "nature", "cloud", "fire", "woman", "computer", "lifestyle", "buisness", "party"] # Tags to parse
    PROCESSES = 100
    parsed_counter = 0
    download_tasks = []
    global_df = pd.DataFrame(columns=['video', 'description'])
    for keyword in KEYWORDS:
        print(f"Processing keyword: {keyword}")
        # Process urls
        max_page = parse_page(keyword)
        for page in range(2, max_page + 1):
            parse_page(keyword, page_id=page)
    print(f"Total tasks: {len(download_tasks)}")
    global_df.to_csv('captions.csv') # Save final captions
    # Split task for multiple processes
    chunks = np.array_split(download_tasks, PROCESSES)
    pid = 0
    for chunk in chunks:
        print("Spawning process")
        p = Process(target=lambda: process_chunk(chunk, pid))
        p.start()
        pid += 1
    
