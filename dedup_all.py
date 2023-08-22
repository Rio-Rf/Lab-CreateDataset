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

def run_dedup(input_file, output_dir):
    # print('input_file:',input_file)
    # print('output_dir', output_dir)    
    cleaner = get_cleaner(
                seen=SharedSet(),
                blacklist=SharedSet()
            )

    output_file_name = os.path.basename(input_file)
    output_file = output_dir + '/' + output_file_name
    # print('output_file: ', output_file)

    with open(input_file) as read_fp, open(output_file, 'w') as output_fp:        
        for line in tqdm(read_fp.readlines()):
            text = cleaner(line)
            if text != "":
                output_fp.write(text + "\n")


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

class SharedSet():
    def __init__(self):
        self.shared_set = set()

    def add(self, item):
        self.shared_set.add(item)

    def get(self):
        return list(self.shared_set)

class SharedSetLocked(SharedSet):
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
                 share_seen = set(),
                 shared_black_list = set(),
                 blacklist_path: Union[str, PathLike] = '',
                 recreate_blacklist_file: bool = False,
                 *args: Any, **kwargs: Any) -> None: 
        super().__init__(*args, **kwargs)
        self.blacklist_path = blacklist_path
        self.has_new_seen = False
        self.seen = share_seen
        self.blacklist = shared_black_list

        if recreate_blacklist_file:
            recreate_empty_file(blacklist_path)

        if blacklist_path != '':
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
                doc.is_rejected = True
                self.has_new_seen = True
                self.blacklist.add(lsh)
            self.seen.add(lsh)
        # self.save_black_list()
        return doc



def get_cleaner(seen, blacklist):
    cleaner = Compose([
        document_filters.JSONLoader(key='text'),        
        deduplication.GenerateDedupLSH(),
        LSHDeduplicatorLockWith(
            seen,
            blacklist,
        ),
        document_filters.JSONDumper()
    ])
    return cleaner

def dedup_in_file(filelist, output_dir, num_worker):
    print('run dedup in file...')
    print('output dir', output_dir)
    print('num worker', num_worker)
    with multiprocessing.Pool(num_worker) as pool:
        args = [(file, output_dir) for file in filelist]
        t = tqdm(total=len(args))
        for _ in pool.starmap(run_dedup, args):
            t.update(1)
        t.close()


def async_check_dedup(args):
    doc, target_file, cleaner  = args
    target_fp = open(target_file)

    # with open(target_file, 'r', encoding='utf-8') as file:
    #     total_lines = sum(1 for _ in file)

    for line in target_fp.readlines():
        target_doc = cleaner(line)
        
        lshs = doc.dedup_lsh
        target_lshs = target_doc.dedup_lsh

        if len(lshs) == 0:
            print('lshs empty..., may be error')
            return doc
        for lsh in lshs:
            if lsh in target_lshs:
                doc.is_rejected = True
                return doc
    return doc
    

def local_compose(line):
    doc = Document(line)
    doc = document_filters.JSONLoader(key='text').apply(doc)
    doc = deduplication.GenerateDedupLSH().apply(doc)    
    return doc

def dedup_between_files(input_file, filelist, output_dir, num_worker=5):
    output_file_name = os.path.basename(input_file)
    output_file = output_dir + '/' + output_file_name
    output_fp = open(output_file, 'w')
    
    with open(input_file, 'r', encoding='utf-8') as file:
        total_lines = sum(1 for _ in file)
    gc.collect()

    read_fp = open(input_file)
    for line in tqdm(read_fp.readlines(), total=total_lines):
        doc = local_compose(line)
        manager = multiprocessing.Manager()
        pool = multiprocessing.Pool(num_worker)
        # progress_dict = manager.dict()
        # blacklist = SharedSetLocked(manager)
        # seen = SharedSetLocked(manager)
        # remove_text = SharedSetLocked(manager)
        args = [(doc, target_file, local_compose) for target_file in filelist]
        is_reject = False
        t2 = tqdm(total=len(args))
        for result in pool.imap_unordered(async_check_dedup, args):
            ## 1つでもrejectならreject
            if not is_reject:                
                is_reject = result.is_rejected or result.text == ""            
            t2.update(1)
        t2.close()
        if not is_reject:
            text = {"text": doc.text}
            output_fp.write(str(text) + "\n")
        if is_reject:
            ## 削除対象はそこまで数が多くないと仮定して、削除対象はすべて保存
            removed_output_file = output_dir + '/removed.jsonl'        
            with open(removed_output_file, 'a') as remove_fp:
                text = {"text": doc.text}
                remove_fp.write(str(text)+'\n')
        manager.shutdown()
        pool.close()
    read_fp.close()
    
    
def get_args():
    parser = argparse.ArgumentParser() 
    parser.add_argument('--target_dir', type=str, required=True)
    parser.add_argument('--output_dir', type=str, required=True)
    parser.add_argument('--in_file', action='store_true')
    parser.add_argument('--between_file', action='store_true')
    parser.add_argument('--num_worker', type=int, default=4)

    # parser.add_argument('--blacklist_path', type=str, default='./output/blacklist.txt')
    # parser.add_argument('--recreate_blacklist', action='store_true')
    args = parser.parse_args()
    return args

def main():
    args = get_args()    
    target_dir = f"{args.target_dir}/*.jsonl"
    output_dir = args.output_dir
    num_worker = args.num_worker
 
    print('target', target_dir)
    filelist = glob.glob(target_dir)
    if args.in_file:
        print('in file')
        dedup_in_file(filelist, output_dir, num_worker=num_worker)
    
    if args.between_file:
        print('between file')
        for file_path in filelist:
            dedup_between_files(file_path, filelist, output_dir, num_worker=num_worker)


def test():
    num_worker = 4    
    input_file = './sample.jsonl'
    output_dir = './dedup2'
    # run_dedup(input_file, output_dir, cleaner)
    # run_dedup_multi(input_file, output_dir, cleaner, num_jobs=4)

    input_file = './sample3.jsonl'
    # run_dedup(input_file, output_dir, cleaner)
    # run_dedup_multi(input_file, output_dir, cleaner, num_jobs=4)

    files = ['./sample_input/sample.jsonl', './sample_input/sample3.jsonl']
    dedup_in_file(files, output_dir, num_worker=3)
    exit(0)

    input_file = './sample_input/sample.jsonl'
    filelist = ['./sample_input/sample3.jsonl', './sample_input/sample4.jsonl']
    output_dir = "dedup2"
    dedup_between_files(input_file, filelist, output_dir, num_worker=4)

    input_file = './sample_input/sample5.jsonl'
    output_dir = "dedup2"
    dedup_between_files(input_file, filelist, output_dir, num_worker=4)


if __name__ == '__main__':
    main()
    # test()