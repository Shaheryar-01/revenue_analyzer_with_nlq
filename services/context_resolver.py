from openai import OpenAI
from config.settings import get_settings

settings = get_settings()



class ContextResolver:
    def __init__(self):

        self.client = OpenAI(api_key=settings.openai_api_key)
    def resolve(self, query: str, history: list) -> dict:
        """Determine if query depends on context. Return merged intent."""
        if not history:
            return {"contextual": False, "query": query}

        # Ask LLM: contextual or standalone?
        response = self.client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a query classifier."},
                {"role": "user", "content": f"Last user query: {history[-1]['user']}"},
                {"role": "user", "content": f"Last answer metadata: {history[-1].get('metadata')}"},
                {"role": "user", "content": f"New query: {query}. Is this contextual? If yes, merge with previous metadata."}
            ],
            temperature=0
        )
        return response.choices[0].message.content
