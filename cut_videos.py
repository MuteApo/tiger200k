import argparse
import json
import os
import subprocess

import pandas as pd
from tqdm import tqdm


def get_video_dimensions(video_path):
    """ get video width and hight """
    cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "json",
        video_path
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise ValueError(f"Error {result.stderr.decode()}")
    
    info = json.loads(result.stdout)
    return info["streams"][0]["width"], info["streams"][0]["height"]

def process_csv(csv_path, num_threads, thread_id):
    df = pd.read_csv(csv_path).iloc[thread_id::num_threads].reset_index(drop=True)
    for idx, row in tqdm(df.iterrows(), total=len(df), smoothing=0.01):
        # read meta info
        bvid = row["bvid"]
        scene_id = row["scene_id"]
        cut_id = row["cut_id"]
        start_time = row["cut_start_timecode"]
        end_time = row["cut_end_timecode"]
        crop_region = row["crop_region"].strip('[]"')

        # path for output
        output_dir = f"videos/clips/{bvid}"
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{bvid}_scene{scene_id}_cut{cut_id}.mp4"
        output_path = os.path.join(output_dir, output_file)
        if os.path.exists(output_path):
            continue

        # path for input
        input_video = f"videos/source/{bvid}.mp4"
        if not os.path.exists(input_video):
            print(f"Video Not Found: {input_video}")
            continue

        # determine ffmpeg cli args
        crop_values = list(map(float, crop_region.split(",")))
        if crop_values[0] > 0.0 or crop_values[1] < 1.0 or crop_values[2] > 0.0 or crop_values[3] < 1.0:
            try:
                video_width, video_height = get_video_dimensions(input_video)
            except Exception as e:
                print(f"Error [{input_video}]: {e}")
                continue

            ct, cb, cl, cr = (
                int(crop_values[0] * video_height) // 2 * 2,
                int(crop_values[1] * video_height) // 2 * 2,
                int(crop_values[2] * video_width) // 2 * 2,
                int(crop_values[3] * video_width) // 2 * 2,
            )

            cmd = [
                "ffmpeg",
                "-ss", start_time,
                "-to", end_time,
                "-i", input_video,
                "-vf", f"crop={cr-cl}:{cb-ct}:{cl}:{ct}",
                "-c:v", "libx264",
                "-crf", "19",
                "-preset", "fast",
                "-y", output_path,
            ]
        else: # bypass for quick *copy* process if no crop
            cmd = [
                "ffmpeg",
                "-ss", start_time,
                "-to", end_time,
                "-i", input_video,
                "-c", "copy",
                "-y", output_path,
            ]

        # ffmpeg cut
        try:
            subprocess.run(cmd, check=True, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"[{output_file}] {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preprocess Tiger200K Dataset")
    parser.add_argument("--meta-path", type=str, default="meta_csv/tiger200k_batch0.csv")
    parser.add_argument("--num_threads", type=int, default=1)
    parser.add_argument("--thread_id", type=int, default=0)

    args = parser.parse_args()

    process_csv(args.meta_path, args.num_threads, args.thread_id)
