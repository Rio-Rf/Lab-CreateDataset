import os
import glob
import gc
from os import PathLike
from typing import Any, Union
from tqdm import tqdm
import argparse
import multiprocessing


from hojichar import Compose, document_filters, deduplication, Parallel, Document
from hojichar.core.filter_interface import Filter
from hojichar.filters.deduplication import LSHDeduplicator


def read_yielder(input_file):    
    with open(input_file) as fp:        
        for line in fp.readlines():
            yield Document(line)

def run_dedup_multi(input_file, output_dir, cleaner, num_jobs=10):
    print('run as multi')
    print('input_file:',input_file)

    with open(input_file, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file)
    gc.collect()
    
    with open(input_file, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file)

    output_file_name = os.path.basename(input_file)
    output_file = output_dir + '/' + output_file_name
    print('output_file: ', output_file)
    t = tqdm(total=total_lines)    
    with Parallel(cleaner, num_jobs=num_jobs) as pfilter, open(output_file, 'w') as output_fp:
        for doc in pfilter.imap_apply(read_yielder(input_file)):
            if not doc.is_rejected:                
                output_fp.write(doc.text + "\n")
            t.update(1)
    t.close()

def run_dedup(input_file, output_dir, cleaner):
    print('input_file:',input_file)
    print('output_dir', output_dir)  

    with open(input_file, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file)
    gc.collect()

    output_file_name = os.path.basename(input_file)
    output_file = output_dir + '/' + output_file_name
    print('output_file: ', output_file)

    with open(input_file) as read_fp, open(output_file, 'w') as output_fp:
        for line in read_fp.readlines():
            text = cleaner(line)
            if text != "":
                output_fp.write(text + "\n")
    # with tqdm(total=total_lines) as t, open(input_file) as read_fp, open(output_file, 'w') as output_fp:
    #     for line in read_fp.readlines():
    #         text = cleaner(line)
    #         if text != "":
    #             output_fp.write(text + "\n")
    #         t.update(1)


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
                blacklist_path: Union[str, PathLike],                
                recreate_blacklist_file: bool = False,
                *args: Any, **kwargs: Any) -> None:
        f = open(blacklist_path, 'w')
        f.close()
        super().__init__(
                online_dedup = True,
                blacklist_path = blacklist_path,
                store_blacklist = True,
                *args, **kwargs)
        
        if recreate_blacklist_file:
            recreate_empty_file(blacklist_path)
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
        
class SharedSet():
    def __init__(self, manager) -> None:
        self.shared_set = manager.list()
        self.lock = manager.Lock()

    def add(self, item):        
        with self.lock:
            if item not in self.shared_set:
                self.shared_set.append(item)

    def get(self):
        with self.lock:
            return list(self.shared_set)

def recreate_empty_file(file_path):    
    if os.path.exists(file_path):
        print('remove...', file_path)
        os.remove(file_path)

    with open(file_path, 'w') as f:        
        pass

class LSHDeduplicatorLockWith(LSHDeduplicator):
    def __init__(self,
                 share_seen,
                 shared_black_list,
                 blacklist_path: Union[str, PathLike],
                 recreate_blacklist_file: bool = False,
                 *args: Any, **kwargs: Any) -> None:        
        super().__init__(*args, **kwargs)
        self.blacklist_path = blacklist_path
        self.has_new_seen = False
        # self.seen = set()
        # self.seen = SharedSet()
        # self.blacklist = SharedSet()
        self.seen = share_seen
        self.blacklist = shared_black_list


        if recreate_blacklist_file:
            recreate_empty_file(blacklist_path)

        with open(blacklist_path) as fp:
            for line in fp:
                lsh = line.strip()
                # self.seen.add(lsh)
                self.blacklist.add(lsh)

    def save_black_list(self):
        if not self.has_new_seen:
            return
        with open(self.blacklist_path, 'w') as fp:            
            fp.writelines([v+'\n' for v in self.seen.get()])

    def apply(self, doc):        
        lshs = doc.dedup_lsh
        if len(lshs) == 0:
            assert ValueError(
                "LSHs for deduplication are not caluculated. Filter \
                    `GenerateDedupLSH` must be composed before this filter."
            )        
        for lsh in lshs:
            if lsh in self.seen.get():
            # if lsh in self.seen.get():
                doc.is_rejected = True
                self.has_new_seen = True
                self.blacklist.add(lsh)
            self.seen.add(lsh)
        # self.save_black_list()
        return doc



def get_cleaner(blacklist_file, recreate_blacklist_file, seen, blacklist):
    cleaner = Compose([
        document_filters.JSONLoader(key='text'),        
        deduplication.GenerateDedupLSH(),
        LSHDeduplicatorLockWith(
            seen,
            blacklist,
            blacklist_path = blacklist_file,
            recreate_blacklist_file = recreate_blacklist_file
        ),
        # LSHDeduplicatorWith(
        #     blacklist_file,
        #     recreate_blacklist_file = recreate_blacklist_file
        # ),
        # Debug(),
        document_filters.JSONDumper()
    ])
    return cleaner


def get_args():
    parser = argparse.ArgumentParser() 
    parser.add_argument('--target_dir', type=str, required=True)
    parser.add_argument('--output_dir', type=str, required=True)
    parser.add_argument('--blacklist_path', type=str, default='./output/blacklist.txt')
    parser.add_argument('--recreate_blacklist', action='store_true')
    args = parser.parse_args()
    return args


def main():
    args = get_args()    
    target_dir = f"{args.target_dir}/*.jsonl"
    output_dir = args.output_dir
 
    print('target', target_dir)
    
    filelist = glob.glob(target_dir)
    print('file list len', len(filelist))
    with multiprocessing.Manager() as manager:        
        seen = SharedSet(manager)
        blacklist = SharedSet(manager)
        cleaner = get_cleaner(
                blacklist_file='./blacklist.txt',
                recreate_blacklist_file=False,        
                seen=seen,
                blacklist=blacklist,
        )
        with multiprocessing.Pool(10) as pool:
            args = [(file, output_dir, cleaner) for file in filelist]
            t = tqdm(total=len(args))
            for _ in pool.starmap(run_dedup, args):
                t.update(1)
            t.close()                


def test():   
    input_file = './sample.jsonl'
    output_dir = './dedup'
    # run_dedup(input_file, output_dir, cleaner)
    # run_dedup_multi(input_file, output_dir, cleaner, num_jobs=4)

    input_file = './sample3.jsonl'
    # run_dedup(input_file, output_dir, cleaner)
    # run_dedup_multi(input_file, output_dir, cleaner, num_jobs=4)

    files = ['./sample.jsonl', './sample3.jsonl']        
    with multiprocessing.Manager() as manager:        
        seen = SharedSet(manager)
        blacklist = SharedSet(manager)
        cleaner = get_cleaner(
                blacklist_file='./blacklist.txt',
                recreate_blacklist_file=False,
                seen=seen,
                blacklist=blacklist,
            )
        with multiprocessing.Pool(4) as pool:
            args = [(file, output_dir, cleaner) for file in files]
            t = tqdm(total=len(args))
            for _ in pool.starmap(run_dedup, args):
                t.update(1)
            t.close()                


if __name__ == '__main__':
    main()
    # test()