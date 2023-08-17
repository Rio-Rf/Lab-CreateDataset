import json
from typing import Any
import os
from tqdm import tqdm


from hojichar import Compose, document_filters, deduplication, Parallel, Document
from hojichar.filters.document_filters import JSONLoader
from hojichar.core.filter_interface import Filter

from huggingface_hub import hf_hub_download


class OscarDocument(Document):
      def __init__(self, *args, **kwargs):
          super().__init__(*args, **kwargs)          
          self.metadata = {}

class SpaceFilter(Filter):
    def apply(self, doc):        
        space_count = 20
        text = doc.text
        if(len(text) > 100):
            ## 半角スペース or 全角スペースを多く含む
            if(text.count(' ') > space_count or text.count('　') > space_count):
                doc.is_rejected = True

        doc.text = text
        return doc
        
class FilterByQualityWarnings(Filter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.quality_key='quality_warnings'

    def apply(self, doc: OscarDocument):        
        quality = doc.metadata[self.quality_key]
        # print('quality', quality)
        if 'header' in quality or 'footer' in quality or 'noisy' in quality:
            doc.is_rejected = True
        return doc    

class OscarJSONLoader(JSONLoader):
    def __init__(self, metadata_keys = [], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.meta = 'metadata'
        self.metadata_keys = metadata_keys

    def apply(self, document):
        try:
            data = json.loads(document.text)
            document.text = str(data[self.key])
            for k in self.metadata_keys:            
                document.metadata[k] = data[self.meta][k]                

        except Exception as e:
            if self.ignore:
                document.is_rejected = True
                return document
            else:
                raise e

        return document

class Debug(Filter):
    def apply(self, document):
        print(document)
        print('**'*40)
        return document

def extract_zst_file(input_file, output_file):
    import zstandard as zstd
    with open(input_file, 'rb') as compressed_file:
        decompressor = zstd.ZstdDecompressor()
        with decompressor.stream_reader(compressed_file) as reader, open(output_file, 'wb') as output:
            while True:
                chunk = reader.read(16384)  # 16K chunks
                if not chunk:
                    break
                output.write(chunk)

def clean(input_file, output_file):
    key = 'text'
    key = 'content'
    before_debup_file = './before_debup.jsonl'

    cleaner = Compose([
        OscarJSONLoader(key=key, metadata_keys=['quality_warnings']),        
        document_filters.DocumentLengthFilter(min_doc_len=100, max_doc_len=50000),
        document_filters.AcceptJapanese(),
        FilterByQualityWarnings(),
        SpaceFilter(),
        document_filters.NgWordsFilterJa(dict_path='./ng_word.txt'),
        document_filters.DiscardBBSComments(),
        document_filters.DiscardAds(),
        document_filters.DocumentNormalizer(),
        document_filters.MaskPersonalInformation(),
        document_filters.JSONDumper()
    ])
    

    input_doc_iter = [OscarDocument(line) for line in open(input_file)]
    print('data len ', len(input_doc_iter))
    with Parallel(cleaner, num_jobs=10) as pfilter:        
        out_doc_iter = pfilter.imap_apply(input_doc_iter)
        t = tqdm(total=len(input_doc_iter))
        with open(before_debup_file, "w") as fp:
            for doc in out_doc_iter:
                if not doc.is_rejected:
                    # print(doc.text)
                    fp.write(doc.text + "\n")
                t.update(1)
        t.close()

    ## dedupは並列でできなそう
    cleaner = Compose([
        JSONLoader(key='text'),
        # Debug(),
        deduplication.GenerateDedupLSH(),
        deduplication.LSHDeduplicator(
            online_dedup=True,        
            store_blacklist=True
        ),
        document_filters.JSONDumper()
    ])
    results = []
    for line in open(before_debup_file):
        a = cleaner(line)        
        results.append(a)
        
    with open(output_file, "w") as fp:
        for doc in results:            
            fp.write(doc + "\n")

def main():
    input_dir = './data'
    output_dir = './output'
    token = os.environ['HF_TOKEN']
    end = 119
    end = 2

    for i in range(1, end):
        url = f'https://huggingface.co/datasets/oscar-corpus/OSCAR-2301/resolve/main/ja_meta/ja_meta_part_{i}.jsonl.zst'
        print('get...', url)
        zst_file_name=os.path.basename(url)        
        hf_hub_download(repo_id='oscar-corpus/OSCAR-2301',
                        subfolder='ja_meta',
                        local_dir=input_dir,
                        filename=zst_file_name,
                        repo_type="dataset",
                        token=token
                        )
        input_ex_file = input_dir + '/ja_meta/' + zst_file_name
        jsonl_file = os.path.splitext(input_ex_file)[0]
        extract_zst_file(input_ex_file, jsonl_file)
        output_file = f'{output_dir}/{i}.jsonl'

        print('input...', jsonl_file)
        print('output...', output_file)
        clean(jsonl_file, output_file)

if __name__ == '__main__':
    main()