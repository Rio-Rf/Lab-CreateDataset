# 概要
#### 人間が作成したテキスト(OSCAR)とLLM生成テキスト(Gpt-3.5 Turbo)から成る日本語データセットを作成した

https://github.com/if001/HojiChar_OSCAR_sample/tree/0.0.4

#### LLMで生成された日本語テキストの検出性能の検証のために作成した

https://github.com/Rio-Rf/Lab-AITextDetection

## 各種ファイルについて
### pre_filter.py
```
python pre_filter.py --start START --end END --output OUTPUT --workers WORKERS
```
引数
- start、end: 処理するファイルのindexを指定。
- output: フィルタリングされたファイルが出力されるディレクトリ
- workers: workerの数

フィルターでは、以下の文章を取り出すようにする

- 500文字 < 文章 < 50000文字
- 日本語の文章であること
- oscarのmeta dataのうち、header, footer, noisy以外のもの
- 半角や全角のスペースが少ないこと
- 指定されたNG wordを含まないこと
- KenLMのスコア
### oscar_generate_text.py
```
python oscar_generate_text.py
```
- OSCARコーパスの最初の50トークンの続きを作成する
- 500文字以上で出力するように試行錯誤したが実際は250文字以上程度になった
- APIの接続が切れて中断されることがあるので途中から実行するコードを用意した
### merge_jsonl.py
```
python merge_jsonl.py  
```
- 途中から実行した際にそれぞれの出力を結合する
### filter_jsonl.py
```
python filter_jsonl.py  
```
- field名を指定して最低文字数制限とキーワード制限をかける
