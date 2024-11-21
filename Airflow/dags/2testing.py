import openai
import json
from tqdm.notebook import tqdm
import time

openai.api_base = 'http://localhost:1234/v1'
openai.api_key = ''

def get_completion(prompt, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = openai.ChatCompletion.create(
                model="local model",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            return response.choices[0].message["content"]
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2)
    return None

def clean_and_validate_response(response):
    if not response:
        return None
    try:
        return json.loads(response)
    except json.JSONDecodeError:
        try:
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                return json.loads(response[json_start:json_end])
        except:
            pass
    return None

def process_single_feedback(feedback):
    prompt = f'''### Instruction:
Analyze this feedback and extract aspects. Return ONLY a JSON object:
{json.dumps(feedback, ensure_ascii=False)}

Required format:
{{
    "GeneralFeedbackID": {feedback["GeneralFeedbackID"]},
    "ID": {feedback["ID"]},
    "Content": "{feedback["Content"]}",
    "Aspects": [
        {{
            "AspectCategory": "Về Sản Phẩm",
            "AspectTerms": []
        }},
        {{
            "AspectCategory": "Về Dịch Vụ", 
            "AspectTerms": []
        }}
    ]
}}
### Response:'''
    
    response = get_completion(prompt)
    if response:
        return clean_and_validate_response(response)
    return None

def load_json_data(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {str(e)}")
        return []

def save_json_data(data, file_path):
    with open(file_path, 'w', encoding='utf-8-sig') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    BATCH_SIZE = 5
    INPUT_FILE = 'warehouse/feedback/CustomerSatisfaction.json'
    OUTPUT_FILE = 'processed.json'
    FAILED_FILE = 'failed_entries.json'

    feedback_data = load_json_data(INPUT_FILE)
    print(f"Loaded {len(feedback_data)} feedback entries")

    processed_results = []
    failed_entries = []

    for idx, feedback in enumerate(tqdm(feedback_data)):
        try:
            processed = process_single_feedback(feedback)
            if processed and all(k in processed for k in ["GeneralFeedbackID", "ID", "Content", "Aspects"]):
                processed_results.append(processed)
                if (idx + 1) % BATCH_SIZE == 0:
                    save_json_data(processed_results, OUTPUT_FILE)
                    print(f"Saved {len(processed_results)} entries")
            else:
                failed_entries.append(feedback)
                print(f"Failed to process entry {idx + 1}")
            time.sleep(1)
        except Exception as e:
            print(f"Error processing entry {idx + 1}: {str(e)}")
            failed_entries.append(feedback)

    if processed_results:
        save_json_data(processed_results, OUTPUT_FILE)
    if failed_entries:
        save_json_data(failed_entries, FAILED_FILE)

    print(f"\nProcessing complete!")
    print(f"Successfully processed: {len(processed_results)} entries")
    print(f"Failed entries: {len(failed_entries)}")

    if processed_results:
        print("\nSample of results:")
        print(json.dumps(processed_results[:3], ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()