#!/usr/bin/env python3


#이게 더 퍼포먼스가 좋음. 
#더 자세하게 액터를 들어가가지고 만드는게 좋은것이 아님 
#일단 이걸로 하곡 move on to next.
import os
import ray
import fitz  # PyMuPDF
import pandas as pd
from ray.util.actor_pool import ActorPool

if ray.is_initialized():
    ray.shutdown()
ray.init(
    runtime_env={"pip": ["pymupdf", "ray", "pandas"]}
)

NUM_ACTORS = 10  # You can always increase the number of actors to scale

def read_pdf(file_path):
    try:
        with fitz.open(file_path) as doc:
            text = ""
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                text += page.get_text()
        return text
    except Exception as e:
        return str(e)

@ray.remote
class Processor:
    def __init__(self, read_pdf):
        self.read_pdf = read_pdf

    def process(self, file_path):
        result = self.read_pdf(file_path)
        return result

# Example usage:
input_directory = "/home/vas/Documents/llava3/yjc_ragscale/ragscale/src/data"
output_directory = "/home/vas/Documents/llava3/yjc_ragscale/ragscale/src/processed"
os.makedirs(output_directory, exist_ok=True)

# List PDF files
pdf_files = [os.path.join(input_directory, f) for f in os.listdir(input_directory) if f.endswith('.pdf')]

# Create Processor actors
actors = [Processor.remote(read_pdf) for _ in range(NUM_ACTORS)]
actor_pool = ActorPool(actors)

# Submit tasks to ActorPool
futures = [actor_pool.submit(lambda actor, file_path: actor.process.remote(file_path), pdf_file) for pdf_file in pdf_files]

# Collect results
results = []
while actor_pool.has_next():
    result = actor_pool.get_next()
    results.append(result)
    
# Create a DataFrame with the results
df = pd.DataFrame({
    'file_path': pdf_files,
    'text': results
})

# Print the DataFrame
print(df)

# Save the DataFrame to a CSV file if needed
output_csv_path = os.path.join(output_directory, "processed_texts.csv")
df.to_csv(output_csv_path, index=False, encoding="utf-8")

ray.shutdown()



