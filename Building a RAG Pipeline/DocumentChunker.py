#Libraries
from typing import List
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

# ---------------------------------------------------------
# 2. CREATE CHUNKS FROM DOCUMENTS
# ---------------------------------------------------------            
class DocumentChunker:
    def __init__(self, chunk_size: int = 256, chunk_overlap: int = 50):
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size = chunk_size,
            chunk_overlap = chunk_overlap
        )
         
    def create_chunks(self, docs: List[Document]) -> List[Document]:
        print("--- Creating chunks documents ---")
        chunks = self.splitter.split_documents(docs)
        print(f"   Created {len(chunks)} chunks from {len(docs)} documents")
        return chunks