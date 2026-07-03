# Local RAG Document Q&A System

A fully local, privacy-first Retrieval-Augmented Generation (RAG) pipeline built with Python, LangChain, ChromaDB, and Ollama. This system allows you to drop PDF documents into a local folder and instantly chat with them using local LLMs, completely free of cloud APIs or data telemetry.

## Features
* **100% Local Processing:** Both document embeddings and LLM inference run entirely on your local machine using Ollama.
* **Incremental Indexing:** Uses ChromaDB with deterministic ID hashing (`source:page:chunk`) to automatically deduplicate files. You can safely rerun the pipeline without creating redundant database entries.
* **Source Tracking:** The AI doesn't just answer; it cites the exact source file and page number the information was extracted from.
* **Modular Architecture:** Built using clean, object-oriented design patterns (Facade, Dependency Injection) for easy maintenance and scalability.
* **Continuous Interactive CLI:** Features a persistent chat loop for continuous querying until the user exits.

## Prerequisites
1. **Python 3.9+** installed on your machine.
2. **Ollama** installed globally (download from [ollama.com](https://ollama.com/download)).

Once Ollama is installed, pull the required models by running these commands in your terminal:
```bash
# Pull the LLM for chat generation
ollama pull llama3.2:3b

# Pull the embedding model for vector search
ollama pull nomic-embed-text
```

## Installation

1. Clone this repository or create a new project folder.
2. Set up a Python virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```
3. Install the required dependencies:
```bash
pip install langchain-core langchain-text-splitters langchain-ollama langchain-chroma langchain-community langchain-classic pypdf
```

## Usage

1. Create a directory named `sources` in the root of your project.
2. Drop any number of PDF documents into the `sources` folder.
3. Run the main application:
```bash
python RAGPipeline.py
```
4. The system will scan the folder, chunk the text, generate embeddings, and save the vector index to a local `vectorstore/db_chroma` folder.
5. Once the pipeline is ready, type your questions into the terminal. Type `quit` or `exit` to shut down the engine.

## System Architecture

The pipeline is broken down into distinct, modular classes:

* **`DocumentLoader`:** Scans the target directory and parses raw text from PDFs using `PyPDFLoader`.
* **`DocumentChunker`:** Applies a `RecursiveCharacterTextSplitter` to break large documents into semantically meaningful 600-character chunks with a 100-character overlap.
* **`OllamaEmbedding`:** Verifies local model availability and initializes the `nomic-embed-text` engine.
* **`VectorStore_Chroma`:** Manages the persistent Chroma database. It calculates unique deterministic IDs for every chunk to prevent duplication during re-runs.
* **`Retriever`:** Wraps the vector store to perform similarity searches, pulling the top 3 most relevant chunks per query.
* **`PromptBuilder`:** Enforces strict systemic boundaries to prevent LLM hallucinations by forcing the model to rely *only* on retrieved context.
* **`LLM`:** Initializes the local `llama3.2:3b` text generation model.
* **`RAGChain`:** Uses LangChain Expression Language (LCEL) and classic chain routing to fuse the retriever, prompt, and LLM together.
* **`RAGPipeline`:** The orchestrator (Facade pattern) that initializes all subsystems and exposes a clean `.ask()` method for the interactive loop.
