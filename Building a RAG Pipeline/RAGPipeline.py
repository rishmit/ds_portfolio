# Libraries
import os
import sys
from DocumentLoader import DocumentLoader
from DocumentChunker import DocumentChunker
from OllamaEmbedding import OllamaEmbedding
from VectorStore import VectorStore
from Retriever import Retriever
from PromptBuilder import PromptBuilder
from LLM import LLM
from RAGChain import RAGChain

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