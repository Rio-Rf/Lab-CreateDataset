import os
import time
import zstandard as zstd
from huggingface_hub import upload_file
import argparse

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
    zst_dir_path = '/tmp/dataset'
    zst_file_path = f'{zst_dir_path}/{zst_file_name}'
    if not os.path.exists(zst_dir_path):
        os.mkdir(zst_dir_path)
    with open(zst_file_path, 'w') as _:
        pass
    try:
        compress_file_with_zst(input_file_path, zst_file_path)

        upload_file(
            path_or_fileobj=zst_file_path,
            path_in_repo=zst_file_name,
            repo_id=f"{hf_username}/{dataset_name}",
            repo_type="dataset"
        )
    except Exception as e:
        print('input_file_path: ', input_file_path)
        print('error ', e)

def get_args():
    parser = argparse.ArgumentParser() 
    parser.add_argument('--start', type=int, required=True)
    parser.add_argument('--end', type=int, required=True)
    parser.add_argument('--target_dir', type=str, required=True)
    parser.add_argument('--hf_username', type=str, required=True)
    parser.add_argument('--dataset_name', type=str, required=True)
    args = parser.parse_args()
    print(args.start)
    print(args.end)

    print(args.target_dir)
    print(args.hf_username)
    print(args.dataset_name)
    return args

def main():
    args = get_args()
    for i in range(args.start, args.end):
        file_path = f"{args.target_dir}/{i}.jsonl"
        upload(file_path, args.hf_username, args.dataset_name)        
        time.sleep(1)


if __name__ == '__main__':
    main()