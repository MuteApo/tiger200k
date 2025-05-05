import os
import sys
import time
from datetime import timedelta

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "bilibili-downloader"))

import config
from models.category import Category
from models.video import Video
from strategy.bilibili_executor import BilibiliDownloader, BilibiliExecutor
from strategy.default import DefaultStrategy

config.TEMP_PATH = config.OUTPUT_PATH = "videos/source"
config.COOKIE = ""
assert config.COOKIE != "", "Please set your cookie for bilibili!"


class TigerStrategy(DefaultStrategy):
    def get(self, video: Video) -> Video:
        bs = self.get_video_page(video.url)
        title = self.get_video_title(bs)
        json = self.get_video_json(bs)

        payload = json["data"]["dash"]["video"]

        video_dict_by_quality = {x["id"]: x for x in payload}

        for quality_id in [120, 112, 80]:  # 4K, 1080P+, 1080P
            if quality_id in video_dict_by_quality:
                video.set_title(title)
                video.set_quality(quality_id)
                video.set_video_url(video_dict_by_quality[quality_id]["baseUrl"])
                video.set_audio_url(video_dict_by_quality[quality_id]["baseUrl"])

                return video

        raise ValueError(f"No available resolution for {video.url}")


class TigerDownloader(BilibiliDownloader):
    def download_video(self, video) -> None:
        save_path = os.path.join(config.OUTPUT_PATH, video.bvid + ".mp4")
        self._download(video.video_url, save_path)


class TigerExecutor(BilibiliExecutor):
    def get(self, bvid: str) -> Video:
        url = f"https://www.bilibili.com/video/{bvid}/?spm_id_from=333.337.search-card.all.click"
        video = self.get_video(url)
        strategy = self._strategies[video.category]
        video = strategy.get(video)
        video.bvid = bvid

        return video


class BFacade:
    def __init__(self):
        self.downloader = TigerDownloader()
        self.crawler = TigerExecutor()
        self.crawler._strategies = {Category.default: TigerStrategy()}

    def download(self, bvid_list):
        os.makedirs(config.OUTPUT_PATH, exist_ok=True)
        for idx, bvid in enumerate(bvid_list):
            print(f"[{idx + 1}/{len(bvid_list)}] {bvid}")

            try:
                video = self.crawler.get(bvid)
                self.downloader.download_video(video)
            except Exception as e:
                print(f"[{bvid}] {e}")

            cur_time = time.time() - start_time
            eta_time = cur_time / (idx + 1) * (len(bvid_list) - idx)
            print(f"已用时 {timedelta(0, cur_time)} 剩余用时 {timedelta(0, eta_time)}")


if __name__ == "__main__":
    start_time = time.time()

    df = pd.read_csv("meta_csv/tiger200k_batchxxx.csv")
    BFacade().download(set(df["bvid"].tolist()))
