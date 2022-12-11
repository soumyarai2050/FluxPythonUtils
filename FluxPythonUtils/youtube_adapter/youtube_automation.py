import logging
from typing import List, Optional, Tuple, Dict, Any
from pytube import YouTube
from youtubesearchpython import VideosSearch


class YoutubeAutomation:

    def __init__(self):
        # self.uploader = YoutubeUploader(client_id, client_secret)
        # self.uploader.authenticate(oauth_path="oauth.json")
        pass

    def download_video(self, links: List[str], save_path: str | None = None):
        for link in links:
            try:
                # object creation using YouTube
                youtube = YouTube(link)
            except Exception as e:
                logging.exception(f"Some error occurred while connecting the video: {link} : {e}")
                raise Exception(f"Some error occurred while connecting the video: {link} : {e}")

            video = youtube.streams.first()

            # get the video with the extension and
            # resolution passed in the get() function
            try:
                # downloading the video
                if save_path is not None:
                    video.download(save_path)
                else:
                    video.download()
            except Exception as e:
                logging.exception(f"Some error occurred while downloading the video: {link} : {e}")
                raise Exception(f"Some error occurred while downloading the video: {link} : {e}")

            logging.info(f"Video Downloaded Successfully: {link}")

    def search_video(self, search_string: str, limit: int | None = 10):
        results_list: List[Tuple] = list()
        videos_search = VideosSearch(search_string, limit=limit)

        results: Dict[str, Any] = videos_search.result()

        for result in results["result"]:
            title = result["title"]
            duration = result["duration"]
            published_time = result["publishedTime"]
            channel_name = result["channel"]["name"]
            link = result["link"]

            results_list.append((title, duration, published_time, channel_name, link))

        return results_list


if __name__ == "__main__":
    def test():
        api_key = "AIzaSyCnNHuKC9GvX07oRpzTHnFoUGeH3Hpm6Ks"
        client_id = "320334481556-7k0sd4sl95b6i77vnnfbm61ijaqpulmq.apps.googleusercontent.com"
        client_secret = "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"
        links = ["https://www.youtube.com/watch?v=x7X9w_GIm1s",
                 "https://www.youtube.com/watch?v=Y8Tko2YC5hA"
                 ]

        youtube_automation: YoutubeAutomation = YoutubeAutomation()
        # youtube_automation.download_video(links)
        print(youtube_automation.search_video("python"))

    test()

