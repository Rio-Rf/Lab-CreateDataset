import os
import zstandard as zstd
from datasets import load_dataset
import glob
import argparse
from tqdm import tqdm

def compress_file_with_zst(input_path, output_path):
    with open(input_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            cctx = zstd.ZstdCompressor()
            compressor = cctx.stream_writer(f_out)
            while True:
                chunk = f_in.read(65536)  # Read 64K at a time
                if not chunk:
                    break
                compressor.write(chunk)
            compressor.flush(zstd.FLUSH_FRAME)


def upload(input_file_path, hf_username, dataset_name):
    zst_file_name = os.path.basename(input_file_path)+'.zst'
    zst_file_path = f'./tmp/{zst_file_name}'    
    compress_file_with_zst(input_file_path, zst_file_path)
    
    # YOUR_HF_USERNAME = "if001"
    # YOUR_DATASET_NAME = "oscar_2023_filtered"    
    dataset = load_dataset(zst_file_path, split='train', save_infos=True)
    dataset.save_to_disk(f"datasets/{hf_username}/{dataset_name}/{zst_file_name}")

def get_args():
    parser = argparse.ArgumentParser() 
    parser.add_argument('--target_dir', type=str, required=True)
    parser.add_argument('--hf_username', type=str, required=True)
    parser.add_argument('--dataset_name', type=str, required=True)
    args = parser.parse_args()

    print(args.target_dir)
    print(args.hf_username)
    print(args.dataset_name)
    return args

def main():
    args = get_args()
    target_dir = f"{args.target_dir}/*.jsonl"
    filelist = glob.glob(target_dir)
    filelist = list(filelist)[:2]
    for file_path in tqdm(filelist, total=len(filelist)):
        upload(file_path, args.hf_username, args.dataset_name)


if __name__ == '__main__':
    main()