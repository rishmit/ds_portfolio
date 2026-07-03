# Libraries
import os
import sys
import subprocess
from langchain_ollama import OllamaEmbeddings, ChatOllama

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