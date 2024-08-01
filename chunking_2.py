#!/usr/bin/env python3
#after combining the clean and otehr stuff /n
import pandas as pd
import re
from langchain.text_splitter import RecursiveCharacterTextSplitter

input_file_path = '/home/vas/Documents/llava3/yjc_ragscale/ragscale/src/processed/processed_texts.csv'
output_file_path = '/home/vas/Documents/llava3/yjc_ragscale/ragscale/src/processed/processed_texts2.csv'

# Initialize text splitter
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=300,
    chunk_overlap=20,
    length_function=len,
)

# Function to clean and strip text
def clean_text(text):
    cleaned_sentences = []
    sentences = text.split('.')
    for sentence in sentences:
        cleaned_sentence = re.sub('\s+', ' ', sentence).strip()
        cleaned_sentence = re.sub('\n+', ' ', cleaned_sentence).strip()
        if len(cleaned_sentence) > 0:
            cleaned_sentences.append(cleaned_sentence)
    return ' '.join(cleaned_sentences)

# Read input CSV
df = pd.read_csv(input_file_path)

# Fill NaN values with an empty string and convert to string type
df['text'] = df['text'].fillna('').astype(str)

# Split texts into chunks and retain file_path for each chunk
chunked_data = []

for index, row in df.iterrows():
    file_path = row['file_path']
    text = row['text']
    chunks = text_splitter.create_documents([text])
    for chunk in chunks:
        cleaned_chunk = clean_text(chunk.page_content)
        chunked_data.append({
            'file_path': file_path,
            'text_chunk': cleaned_chunk
        })

# Create DataFrame from chunked data
chunked_df = pd.DataFrame(chunked_data)
print(chunked_df)
# Save to output CSV
chunked_df.to_csv(output_file_path, index=False)

print(f"Processed texts saved to {output_file_path}")

