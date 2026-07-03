from langchain_classic.chains import create_retrieval_chain
from langchain_classic.chains.combine_documents import create_stuff_documents_chain

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