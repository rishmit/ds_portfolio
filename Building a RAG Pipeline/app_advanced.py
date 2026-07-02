# Libraries
import os
import sys
import subprocess
from typing import List
from langchain_community.document_loaders import DirectoryLoader, PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_ollama import OllamaEmbeddings, ChatOllama
from langchain_community.vectorstores import FAISS
from langchain_chroma import Chroma
from langchain_classic.chains import create_retrieval_chain
from langchain_classic.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.documents import Document

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
    
# ---------------------------------------------------------
# 3. CREATE EMBEDDINGS USING OLLAMAEMBEDDINGS
# ---------------------------------------------------------
class OllamaEmbedding:
    """Generate embeddings using Ollama's embedding model."""
    def __init__(self, model_name = "nomic-embed-text"):
        self.model_name = model_name
        self._verify_model()
        
    def _verify_model(self):
        """Check if model is available locally."""
        try:
            result = subprocess.run(
                ['ollama', 'list'],
                capture_output = True,
                text = True,
                check = True,
            )
            
            base_model_name = self.model_name.split(':')[0]
            
            if base_model_name not in result.stdout:
                raise RuntimeError(
                    f"Model '{self.model_name}' not found locally.\n"
                    f"Please download it first using:\n"
                    f"  ollama pull {self.model_name}\n"
                    f"This is a onr-time setup step that requires internet connection."
                )
                
            print(f"Found embedding model: {self.model_name}")
            
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Cannot connect to Ollama service.\n"
                f"Please ensure Ollama is installed and runnning.\n"
                f"Error: {e.returncode}"
            )
        except FileNotFoundError:
            raise RuntimeError(
                "Ollama not found on your system.\n"
                "Please install Ollama. This is a one-time step"
            )
            
    def get_embedding_function(self):
        """Initialize and return the configured LangChain OllamaEmbeddings instance."""
        print(f"--- Activating local embeddings engine ({self.model_name}) ---")
        return OllamaEmbeddings(
            model=self.model_name
        )
        
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
    
# ---------------------------------------------------------
# 5. TURN THE VECTOR STORE INTO A RETRIEVER
# ---------------------------------------------------------
class Retriever:
    """The Retriever class uses the FAISS to retrieve relevant documents based on a query"""
    
    @staticmethod
    def create_retriever(vector_store, k: int = 3):
        return vector_store.as_retriever(
            search_kwargs = {"k": k}
        )
        
# ---------------------------------------------------------
# 6. CREATE THE PROMPT TEMPLATE WITH CONTEXT AND INPUT
# ---------------------------------------------------------
class PromptBuilder:
    
    @staticmethod
    def create_prompt_template():
        system_prompt = (
            "You are a helpful assistant. Use ONLY the context below "
            "to answer the question. If the answer is not in the context, say 'I don\'t know '"
            "based on the provided documents."
            "Context:"
            "{context}"
            )        
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", "{input}")
        ])
        
        return prompt
        
# ---------------------------------------------------------
# 7. LLM INITIALIZATION
# ---------------------------------------------------------
class LLM:
    def __init__(self, model_name = "llama3.2:3b"):
        self.model_name = model_name
        self._verify_model()
        
    def _verify_model(self):
        """Check if model is available locally."""
        try:
            result = subprocess.run(
                ["ollama", "list"],
                check = True,
                capture_output = True,
                text = True
            )
            
            base_model_name = self.model_name.split(":")[0]
            
            if base_model_name not in result.stdout:
                raise RuntimeError(
                    f"Model '{self.model_name}' not found locally.\n"
                    f"Please download it first using:\n"
                    f"  ollama pull {self.model_name}\n"
                    f"This is a onr-time setup step that requires internet connection."
                )
                
            print(f"Found LLM model: {self.model_name}")
            
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Cannot connect to Ollama service.\n"
                f"Please ensure Ollama is installed and runnning.\n"
                f"Error: {e.returncode}"
            )
            
        except FileNotFoundError:
            raise RuntimeError(
                "Ollama not found on your system.\n"
                "Please install Ollama. This is a one-time step"
            )
            
    def llm(self):
        return ChatOllama(
            model = self.model_name,
            temperature = 0
        )
        
# ---------------------------------------------------------
# 8. RAG CHAIN
# ---------------------------------------------------------
class RAGChain:
    def __init__(self, llm, prompt, retriever):
        self.llm = llm
        self.prompt = prompt
        self.retriever = retriever
        
    def build_chain(self):
        question_answer_chain = create_stuff_documents_chain(self.llm, self.prompt)
        rag_chain = create_retrieval_chain(self.retriever, question_answer_chain)
        
        return rag_chain
        
# ---------------------------------------------------------
# 9. RAG PIPELINE
# ---------------------------------------------------------
class RAGPipeline:
    """RAG workflow from ingestion to execution."""
    def __init__(self, source_dir: str):
        self.source_dir = source_dir
        self.rag_chain = None
        
    def setup(self):
        """Initializes all components and builds the final chain"""
        print("\n=== Initializing System ===\n")
        
        # 1. Load documents
        docs = DocumentLoader.load_pdfs(self.source_dir)
        
        # 2. Chunk documents
        chunker = DocumentChunker(chunk_size = 600, chunk_overlap = 100)
        chunks = chunker.create_chunks(docs = docs)
        
        # 3. Create embedding engine
        embedding_wrapper = OllamaEmbedding(model_name = 'nomic-embed-text')
        embeddings = embedding_wrapper.get_embedding_function()
        
        # 4. Build Vector Store
        vector_store = VectorStore(embeddings).add_to_chroma(chunks)
        
        # 5. Create Retriever
        retriever = Retriever.create_retriever(vector_store = vector_store, k = 3)
        
        # 6. Create Prompt
        prompt = PromptBuilder.create_prompt_template()
        
        # 7. Initialize LLM (Using 'llama3.2:3b')
        llm = LLM(model_name = "llama3.2:3b").llm()
        
        # 8. Assemble Pipeline
        chain_builder = RAGChain(llm, prompt, retriever)
        self.rag_chain = chain_builder.build_chain()
        
        print("=== System Ready ===\n")
        
    def ask(self, query: str):
        """Executes the chain against a user query"""
        if not self.rag_chain:
            raise RuntimeError("Pipeline is not setup. Call .setup() first.")
        
        return self.rag_chain.invoke({"input": query})  
        
if __name__ == "__main__":
    directory = "./sources"
    
    if not os.path.exists(directory) or not os.listdir(directory):
        print(f"[ERROR] Directory '{directory}' is missing or empty")
        sys.exit(1)
        
    pipeline = RAGPipeline(source_dir = directory)
    pipeline.setup()
    
    print("="*60)
    print(" RAG ENGINE DEPLOYED")
    print(" Ask questions about your documents. Type 'quit' or 'exit' to end.")
    print("="*60)
    
    while True:
        try:
            # 1. Get the user's question
            user_query = input("You: ").strip()
            
            if user_query.lower() in ["quit", "exit"]:
                print("--- Exiting the RAG Pipeline ---\n")
                break
            
            if not user_query:
                continue
            
            print("\nSearching documents and generating response...")
            response = pipeline.ask(user_query)
            
            print(f"\nAI: {response['answer']}")
            seen_sources = set()
            for doc in response.get('context', []):
                source_file = os.path.basename(doc.metadata.get('source'))
                page_num = doc.metadata.get('page', 0) + 1
                source_tag = f"{source_file} (Page {page_num})"
                
                if source_tag not in seen_sources:
                    print(f"    {source_tag}")
                    seen_sources.add(source_tag)
            
            print("-"*30 + "\n")
                
        except KeyboardInterrupt:
            print("\n\nSession interrupted via terminal. Exiting gracefully.")
            break
        except Exception as e:
            print(f"[ERROR] An unexpected error occured: {e}\n")