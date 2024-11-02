import os

output_path = "oscar.jsonl"

# ディレクトリが存在しない場合は作成する
#os.makedirs(os.path.dirname(output_path), exist_ok=True)

# 読み込みファイルのディレクトリを指定
input_dir = "result"

# 書き込みモードで出力ファイルを開く
with open(output_path, 'w') as outfile:
    # 1.jsonlから2.jsonlまでのファイルをループで処理
    for i in range(1, 3):
        # 各ファイルのパスをresultディレクトリ内で指定
        input_file = os.path.join(input_dir, f"{i}.jsonl")
        
        # 各入力ファイルを読み込みモードで開く
        with open(input_file, 'r') as infile:
            # 入力ファイルの内容を出力ファイルに追加
            for line in infile:
                outfile.write(line)

print(f"All files have been merged into {output_path}")
