import os
import glob
import gc
from os import PathLike
from typing import Any, Union
from tqdm import tqdm
import argparse

from hojichar import Compose, document_filters, deduplication, Parallel, Document
from hojichar.core.filter_interface import Filter
from hojichar.filters.deduplication import LSHDeduplicator
    
def read_yielder(input_file):
    with open(input_file) as fp:
        yield fp.readlines()

def run_debup(input_file, output_dir, cleaner):
    print('input_file:',input_file)

    with open(input_file, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file)
    gc.collect()

    output_file_name = os.path.basename(input_file)
    output_file = output_dir + '/' + output_file_name
    print('output_file: ', output_file)
    output_fp = open(output_file, 'w')
    
    with open(input_file) as fp:
        for line in tqdm(fp, total=total_lines):        
            result = cleaner(line)
            if result != "":
                output_fp.write(result + "\n")
            del result
        gc.collect()
    output_fp.close()


class Debug(Filter):
    def __init__(self, idx = "", *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.idx = idx

    def apply(self, document):
        # print(self.idx)
        # print(document.text)
        # print(document.is_rejected)        
        # print('**'*40)
        return document
    
class LSHDeduplicatorWith(LSHDeduplicator):
    def __init__(self, 
                online_dedup: bool = True,
                blacklist_path: Union[str, PathLike] = "",
                *args: Any, **kwargs: Any) -> None:
        f = open(blacklist_path, 'w')
        f.close()
        super().__init__(
                online_dedup,
                blacklist_path,
                store_blacklist = True,
                *args, **kwargs)
        self.blacklist_path = blacklist_path

    def save_black_list(self):
        if len(self.blacklist) == 0:
            return

        with open(self.blacklist_path, 'w') as fp:            
            fp.writelines([v+'\n' for v in self.blacklist])

    def apply(self, document):
        super().apply(document)
        self.save_black_list()
        return document
    pass


def get_cleaner():  
    cleaner = Compose([
        document_filters.JSONLoader(key='text'),        
        deduplication.GenerateDedupLSH(),
        LSHDeduplicatorWith(
            blacklist_path='./dedup_blacklist.txt',
            online_dedup=True,
        ),
        Debug(),
        document_filters.JSONDumper()
    ])
    return cleaner


def get_args():
    parser = argparse.ArgumentParser() 
    parser.add_argument('--target_dir', type=str, required=True)
    parser.add_argument('--output_dir', type=str, required=True)
    parser.add_argument('--blacklist_path', type=str, default='./output/blacklist.txt')
    args = parser.parse_args()
    return args


def main():
    args = get_args()    
    target_dir = f"{args.target_dir}/*.jsonl"
    output_dir = args.output_dir
 
    print('target', target_dir)
    
    cleaner = get_cleaner()
    filelist = glob.glob(target_dir)
    print('file list len', len(filelist))
    for input_file in filelist:
        run_debup(input_file, output_dir, cleaner)


def test():
    cleaner = get_cleaner()
    input_file = './sample.jsonl'    
    output_dir = './dedup'
    run_debup(input_file, output_dir, cleaner)

    input_file = './sample3.jsonl'
    run_debup(input_file, output_dir, cleaner)

if __name__ == '__main__':
    main()
    # test()