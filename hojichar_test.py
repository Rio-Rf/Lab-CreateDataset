import json
import argparse
from typing import Any

from hojichar import Compose, document_filters, deduplication, Parallel, Document
from hojichar.filters.document_filters import JSONLoader
from hojichar.core.filter_interface import Filter


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

def get_args():
    parser = argparse.ArgumentParser()    
    parser.add_argument(
        "--input_file",
        type=str,
        required=True,
        help="Path to input jsonl files or lmd archive(s) - if using multiple archives, put them in a comma separated "
        "list",
    )    
    parser.add_argument(
        "--workers", type=int, default=1, help="Number of worker processes to launch"
    )
    parser.add_argument(
        "--output_file", type=str, help="", required=True
    )
    args = parser.parse_args()
    return args

def main():
    args = get_args()
    key = 'text'
    key = 'content'
    before_debup_file = './before_debup.jsonl'

    cleaner = Compose([
        OscarJSONLoader(key=key, metadata_keys=['quality_warnings']),        
        document_filters.DocumentLengthFilter(min_doc_len=100, max_doc_len=50000),
        document_filters.AcceptJapanese(),
        FilterByQualityWarnings(),
        SpaceFilter(),
        document_filters.DocumentNormalizer(),
        document_filters.MaskPersonalInformation(),
        document_filters.JSONDumper()
    ])
    
    input_doc_iter = [OscarDocument(line) for line in open(args.input_file)]    
    with Parallel(cleaner, num_jobs=10) as pfilter:
        out_doc_iter = pfilter.imap_apply(input_doc_iter)
        with open(before_debup_file, "w") as fp:
            for doc in out_doc_iter:           
                if not doc.is_rejected:
                   # print(doc.text)
                   fp.write(doc.text + "\n")


    cleaner = Compose([
        JSONLoader(key='text'),
        Debug(),
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
        
    with open(args.output_file, "w") as fp:
        for doc in results:            
            fp.write(doc + "\n")

if __name__ == '__main__':
    main()