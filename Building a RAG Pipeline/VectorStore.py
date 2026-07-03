# Libraries
import os
import sys
import subprocess
from typing import List
from langchain_core.documents import Document
from langchain_community.vectorstores import FAISS
from langchain_chroma import Chroma

# ---------------------------------------------------------
# 4. CREATE VECTOR STORE (USING FAISS) FOR THE EMBEDDINGS
# ---------------------------------------------------------
# class VectorStore:
#     def __init__(self, embeddings, db_dir: str = "./vectorstore/db_faiss"):
#         self.embeddings = embeddings
#         self.db_dir = db_dir
#         self.vector_store = None
        
#     def create_store(self, chunks: List[Document]):
#         print("--- Managing FAISS Vector Store ---")
        
#         if not os.path.exists(self.db_dir):
#             print("    No local database found. Building new index...")
#             self.vector_store = FAISS.from_documents(
#                 chunks,
#                 self.embeddings
#             )
#             self.vector_store.save_local(self.db_dir)
#             print(f"FAISS index successfully saved to {self.db_dir}.")
        
#         else:
#             print(f"    Local database found at '{self.db_dir}'. Loading into memory...")
#             try:
#                 self.vector_store = FAISS.load_local(
#                     self.db_dir,
#                     self.embeddings,
#                     allow_dangerous_deserialization=True
#                 )
#             except Exception as e:
#                 print(f"[ERROR] Problem loading the local FAISS database: {e}")
#                 print(f"Consider deleting the corrupted folder and running the script again.")
#                 sys.exit(1)
        
#         return self.vector_store
    
# ---------------------------------------------------------
# 4. CREATE VECTOR STORE (USING CHROMA) FOR THE EMBEDDINGS
# ---------------------------------------------------------
class VectorStore:
    def __init__(self, embeddings, db_dir: str = "./vectorstore/db_chroma") -> None:
        self.embeddings = embeddings
        self.db_dir = db_dir
        self.vector_store = None
        
    def add_to_chroma(self, chunks: List[Document]):
        print("--- Managing Chroma Vector Store ---")
        self.vector_store = Chroma(
            persist_directory = self.db_dir,
            embedding_function = self.embeddings
        )
        
        # Calculate Page IDs:
        chunks_with_ids = self.calculate_chunk_ids(chunks)
        
        # Add or Update the documents
        existing_items = self.vector_store.get(include=[])
        existing_ids = set(existing_items["ids"])
        print(f"Number of existing documents in DB: {len(existing_ids)}")
        
        # Only add documents that don't exist in the DB
        new_chunks = []
        for chunk in chunks_with_ids:
            if chunk.metadata["id"] not in existing_ids:
                new_chunks.append(chunk)
                
        if len(new_chunks):
            print(f"    Adding new documents: {len(new_chunks)}")
            new_chunks_ids = [chunk.metadata["id"] for chunk in new_chunks]
            self.vector_store.add_documents(new_chunks, ids = new_chunks_ids)
        else:
            print("    No new documents to add")
            
        return self.vector_store
        
    @staticmethod
    def calculate_chunk_ids(chunks: List[Document]):
        last_page_id = None
        current_chunk_index = 0
        
        for chunk in chunks:
            source = chunk.metadata.get("source")
            page = chunk.metadata.get("page")
            current_page_id = f"{source}:{page}"
            if current_page_id == last_page_id:
                current_chunk_index += 1
            else:
                current_chunk_index = 0
                
            chunk_id = f"{current_page_id}:{current_chunk_index}"
            last_page_id = current_page_id
            
            chunk.metadata["id"] = chunk_id
            
        return chunks