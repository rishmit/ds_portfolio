from langchain_core.prompts import ChatPromptTemplate

# ---------------------------------------------------------
# 6. CREATE THE PROMPT TEMPLATE WITH CONTEXT AND INPUT
# ---------------------------------------------------------
class PromptBuilder:
    
    @staticmethod
    def create_prompt_template():
        system_prompt = (
            "You are a helpful assistant. Use ONLY the context below "
            "to answer the question. If the answer is not in the context, say 'I don\'t know '"
            "based on the provided documents.\n\n"
            "Context:\n"
            "{context}"
            )        
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", "{input}")
        ])
        
        return prompt