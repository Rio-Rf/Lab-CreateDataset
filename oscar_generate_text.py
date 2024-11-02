# !pip install openai==0.28
# !pip install tiktoken
import openai
import tiktoken
import json
import time

# OpenAI APIキーの設定
# ここにAPIキーを設定

# 最初のNトークンを抽出する関数
def get_N_tokens(text, N):
    # gpt-3.5-turboのエンコーディングを取得
    enc = tiktoken.encoding_for_model("gpt-3.5-turbo")
    # テキストをトークン化
    tokens = enc.encode(text)
    # 最初のNトークンを取得
    first_N_tokens = tokens[:N]
    # トークンをデコードして文字列にする
    first_N_text = enc.decode(first_N_tokens)
    return first_N_text

# GPT-3.5を使ってテキストを生成する関数
def generate_text(prompt, retries=5, delay=5):
    user_prompt1 = (
        f"以下の()内の文章の続きを作成してください。"
        f"出力は与えられた文章部分は出力せず、作成した部分のみを出力してください。文字数は500文字で出力してください。\n"
        f"({prompt})"
    )
    user_prompt2 = "続きを500文字で作成してください。"

    for i in range(retries):
        try:
            response1 = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": user_prompt1},
                    {"role": "user", "content": user_prompt2},
                    {"role": "user", "content": user_prompt2}
                ],
                max_tokens=4096
            )
            return response1.choices[0].message['content'].strip()
        except openai.error.APIError as e:
            print(f"API error: {e} (attempt {i+1}/{retries})")
            if i < retries - 1:
                time.sleep(delay)
            else:
                raise e

# ファイルの読み込み
input_file_path = './oscar.jsonl'
output_file_path = './oscar_ai_added.jsonl'
"""
# 途中から実行するための設定
start_line = 2355  # ここに開始したい行番号を指定
"""
with open(input_file_path, 'r', encoding='utf-8') as infile, open(output_file_path, 'w', encoding='utf-8') as outfile:
    for line in infile:
        data = json.loads(line.strip())
        prompt = get_N_tokens(data['text'], 50)
        ai_text = generate_text(prompt)
        ai_text_512 = get_N_tokens(ai_text, 512)

        # 新しいフィールドを追加
        data['gpt-3.5-turbo_generated_text_wo_prompt'] = ai_text_512

        # JSON形式で出力
        json.dump(data, outfile, ensure_ascii=False)
        outfile.write('\n')
print(f"AI生成テキストが {output_file_path} に保存されました。")