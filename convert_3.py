#!/usr/bin/env python3
import os
import ray
import pandas as pd
import numpy as np
from transformers import pipeline
from embeddings import LocalHuggingFaceEmbeddings

# Initialize Ray
if ray.is_initialized():
    ray.shutdown()
ray.init(
    runtime_env={"pip": ["ray", "pandas", "transformers"]}
)

# Define constants
NUM_ACTORS = 10  # Number of actors for parallel processing
BATCH_SIZE = 1000  # Number of records to process before saving intermediate results

# Define the paths
input_file_path = '/home/src/data/processed/processed_texts2.csv'
output_directory = '/home/src/data/processed'
os.makedirs(output_directory, exist_ok=True)

# Load the input CSV
df = pd.read_csv(input_file_path)

@ray.remote
class Embedder:
    def __init__(self, actor_id):
        # Initialize the embeddings model with MPS support
        self.device = 'cpu'
        self.embeddings_model = pipeline('feature-extraction', model='sentence-transformers/multi-qa-mpnet-base-dot-v1', device=self.device)
        #self.embeddings_model = LocalHuggingFaceEmbeddings("multi-qa-mpnet-base-dot-v1")
        self.actor_id = actor_id  # Unique ID for the actor

    def embed_and_save(self, batch, batch_number):
        # Embed the batch of text chunks
        embeddings = [self.embeddings_model(text_chunk)[0] for text_chunk in batch]
        
        # Create DataFrame for this batch
        batch_df = pd.DataFrame({
            'text_chunk': batch,
            'embedded': embeddings
        })
        
        # Save the batch to CSV
        batch_csv_path = os.path.join(output_directory, f'embedded_texts_batch_{self.actor_id}_{batch_number}.csv')
        batch_df.to_csv(batch_csv_path, index=False)
        
        return batch_csv_path

# Create Embedder actors
actors = [Embedder.remote(i) for i in range(NUM_ACTORS)]

# Helper function to create batches of text chunks
def create_batches(lst, batch_size):
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]

# Convert text chunks to list
text_chunks = df['text_chunk'].tolist()
batches = list(create_batches(text_chunks, BATCH_SIZE))

# Distribute batches to actors
futures = []
for i, batch in enumerate(batches):
    actor = actors[i % NUM_ACTORS]  # Distribute batches across available actors
    futures.append(actor.embed_and_save.remote(batch, i + 1))

# Collect results
csv_files = ray.get(futures)

print(f"All batches saved to {output_directory}")
print("CSV files:", csv_files)

ray.shutdown()

