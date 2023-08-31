# readme
hojicharをoscarに対して動かしてみたサンプル

https://github.com/HojiChar/HojiChar

## run

oscarの認証用に環境変数を設定しておく (colab用)
`%env HF_TOKEN=`


### pre_filter
dedup処理以外を行う

```
python pre_filter.py --start START --end END --output OUTPUT --workers WORKERS
```

引数
- start、end: 処理するファイルのindexを指定。
- output: フィルタリングされたファイルが出力されるディレクトリ
- workers: workerの数


フィルターでは、以下の文章を取り出すようにする

- 100文字 < 文章 < 50000文字
- 日本語の文章であること
- oscarのmeta dataのうち、header, footer, noisy以外のもの
- 半角や全角のスペースが少ないこと
- 指定されたNG wordを含まないこと
- KenLMのスコア


`https://huggingface.co/datasets/oscar-corpus/OSCAR-2301/resolve/main/ja_meta/ja_meta_part_{i}.jsonl.zst`をダウンロード
iは 1から119まで

`./data` 以下にダウンロードしたzst、解凍後のjsonlが保存される
`./output` 以下に処理されたファイルが保存される


### dedup
pre_filterで処理したすべてのファイルを見て重複削除を行う

pythonだと処理に時間がかかりすぎるのでC++に移行

https://github.com/if001/dedup_sentence


### upload
hfにupload

```
python upload_to_hf.py [-h] --start START --end END --target_dir TARGET_DIR --hf_username HF_USERNAME --dataset_name DATASET_NAME
```

TARGET_DIRにあるファイルをuploadする。ファイルは1.jsonl、2.jsonl...を想定。
startとendでファイルを指定する。


## format
oscarのデータでは、以下の用にcontentに本文が入る

```json
{
	"content": "本文",
	"warc_headers": {
		"warc-refers-to": "**********",
		"content-type": "text/plain",
		"warc-date": "2022-12-07T03:09:41Z",
		"content-length": "414",
		"warc-target-uri": "https://****",
		"warc-type": "conversion",
		"warc-record-id": "<urn:uuid:00000000-0000-0000-0000-000000000000>",
		"warc-identified-content-language": "jpn,eng",
		"warc-block-digest": "sha1:DGDH6QXEPQ5EG3KJYFUYWMLFDJCOJNHJ"
	},
	"metadata": {
		"identification": {
			"label": "ja",
			"prob": 1.0000107
		},
		"harmful_pp": 267.7608,
		"tlsh": "tlsh:T14FD97CE06144B532E71071241655D31D4D0039515F14609944F8D755E53F060F9E0E3F4A736D918B9C651F001193A52E10FC232BDC4BE4B71335047F8038A660F5EE416CFD",
		"quality_warnings": [
			"tiny"
		],
		"categories": null,
		"sentence_identifications": [
			{
				"label": "ja",
				"prob": 1.0000107
			}
		]
	}
}
{
	"content": "本文",
	"warc_headers": {
		"warc-refers-to": "**********",
		"content-type": "text/plain",
		"warc-date": "2022-12-07T03:09:41Z",
		"content-length": "414",
		"warc-target-uri": "https://****",
		"warc-type": "conversion",
		"warc-record-id": "<urn:uuid:00000000-0000-0000-0000-000000000000>",
		"warc-identified-content-language": "jpn,eng",
		"warc-block-digest": "sha1:DGDH6QXEPQ5EG3KJYFUYWMLFDJCOJNHJ"
	},
	"metadata": {
		"identification": {
			"label": "ja",
			"prob": 1.0000107
		},
		"harmful_pp": 267.7608,
		"tlsh": "tlsh:T14FD97CE06144B532E71071241655D31D4D0039515F14609944F8D755E53F060F9E0E3F4A736D918B9C651F001193A52E10FC232BDC4BE4B71335047F8038A660F5EE416CFD",
		"quality_warnings": [
			"tiny"
		],
		"categories": null,
		"sentence_identifications": [
			{
				"label": "ja",
				"prob": 1.0000107
			}
		]
	}
}
```