# Libraries
import os
import sys
from typing import List
from langchain_core.documents import Document
from langchain_community.document_loaders import DirectoryLoader, PyPDFLoader

# ---------------------------------------------------------
# 1. LOAD DOCUMENTS FROM DIRECTORY
# ---------------------------------------------------------
class DocumentLoader:
    @staticmethod
    def load_pdfs(dir_path: str) -> List[Document]:
        """
        The function checks if there are any updated files in the source folder.
        It saves the list of PDF files in a JSON file called 'files.json'.
        The function loads the documents into a loader with DirectoryLoader and returns
        the List[Document].
        
        arg:
            dir_path: The directory path to the source folder
        
        return:
            List[Document]
        """
        print("--- Loading documents ---")
        try:
            pdf_files = [filename for filename in os.listdir(dir_path) if filename.lower().endswith('.pdf')]
            print(f"PDFs found: {pdf_files}")
            
            loader = DirectoryLoader(
                path = dir_path,
                glob = "**/*.pdf",
                loader_cls = PyPDFLoader, # type: ignore
            )
            docs = loader.load()
            print(f"   Successfully loaded {len(docs)} pages.")
            return docs
            
        except Exception as e:
            print(f"   error: Failed to load documents - {e}")
            sys.exit(1)