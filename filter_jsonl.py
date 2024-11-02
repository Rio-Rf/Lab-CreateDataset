import json

# 元のファイルと出力ファイルのパス
input_file = "ccc.jsonl"
output_file = "oscar_ai_added_filtered.jsonl"

# 除去したいキーワードのリスト
keywords = ["続きを作成します", "続きを作成いたします", "続きを生成", "申し訳ありませんが", "申し訳ございませんが", "500文字", "the", "作成中"]
#keywords = ["http", "選択してください"]

# チェックするフィールド名
field_name = "gpt-3.5-turbo_generated_text_wo_prompt"
#field_name = "text"

# フィルタリング処理
with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
    for line in infile:
        obj = json.loads(line)  # 1行ずつJSONとして読み込む
        field_value = obj.get(field_name, "")  # 指定したフィールドの値を取得
        
        # テキストが500文字未満か、キーワードが含まれているかをチェック
        if len(field_value) >= 500 and not any(keyword in field_value for keyword in keywords):
            # 条件を満たす場合のみ、新しいファイルに書き込む
            outfile.write(json.dumps(obj, ensure_ascii=False) + '\n')

print(f"フィルタリングされたファイルが出力されました: {output_file}")