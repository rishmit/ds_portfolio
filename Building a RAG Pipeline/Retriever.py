

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