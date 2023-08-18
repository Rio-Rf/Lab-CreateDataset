import os
import glob
from tqdm import tqdm

from hojichar import Compose, document_filters, deduplication, Parallel, Document


    

def run_debup(file_name, save_file):
    cleaner = Compose([
        document_filters.JSONLoader(key='text'),
        # Debug(),
        deduplication.GenerateDedupLSH(),
        deduplication.LSHDeduplicator(
            online_dedup=True,        
            store_blacklist=True
        ),
        document_filters.JSONDumper()
    ])


def main():
    target_dir = "./output/*.jsonl"
    output_dir = ''

    filelist = glob.glob(target_dir)
    cleaner = Compose([
        document_filters.JSONLoader(key='text'),
        # Debug(),
        deduplication.GenerateDedupLSH(),
        deduplication.LSHDeduplicator(
            online_dedup=True,        
            store_blacklist=True
        ),
        document_filters.JSONDumper()
    ])

    t = tqdm(total=len(filelist))
    for input_file in filelist:
        print('input_file:',input_file)
        lines = []
        with open(input_file) as fp:
            lines = fp.readlines()
        output_file_name = os.path.basename(input_file)
        output_file = output_dir + '/' + output_file_name
        print('output_file: ', output_file)
        output_fp = open(output_file)
        t2 = tqdm(total=len(lines))
        for line in lines:
          result = cleaner(line)
          if result != "":
            output_fp.write(result + "\n")
          t2.update(1)
        output_fp.close()
        t2.close()
        t.update(1)
    t.close()

if __name__ == '__main__':
    main()