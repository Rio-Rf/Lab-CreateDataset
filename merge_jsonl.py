import json

# 結合するファイルのリスト
file_paths = ["oscar_ai_added_1_3772.jsonl", "oscar_ai_added_3773_5015.jsonl", "oscar_ai_added_5016_20564.jsonl"]
# 出力するファイル名
output_file = "oscar_ai_added.jsonl"

# 結合したデータを格納するリスト
merged_data = []

# 各ファイルを1つずつ読み込む
for file_path in file_paths:
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            # JSONとして読み込んでリストに追加
            merged_data.append(json.loads(line))

# 新しいファイルに書き込む
with open(output_file, 'w', encoding='utf-8') as f:
    for item in merged_data:
        # JSON Lines形式で1行ずつ書き出す
        f.write(json.dumps(item, ensure_ascii=False) + '\n')

print(f"ファイルが正常に結合されました: {output_file}")