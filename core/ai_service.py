# core/ai_service.py
import requests
from typing import Dict, Optional
from config.settings import settings

class AIService:
    def __init__(self, api_key: str = settings.GROQ_API_KEY):
        self.api_key = api_key
        self.base_url = "https://api.groq.com/openai/v1/chat/completions"
                      

    def analyze_query(self, query: str, execution_time: Optional[float] = None) -> Dict:
        """Basic query analysis using GROQ AI"""
        prompt = f"""Analizza questa query SQL e suggerisci ottimizzazioni:
        {query}
        """
        return self._call_groq(prompt)

    def deep_analyze_query(self, query: str, execution_time: Optional[float] = None) -> Dict:
        """Provides a deep, detailed analysis of a SQL query"""
        context = f"\nTempo di esecuzione: {execution_time}ms" if execution_time else ""
        
        prompt = f"""Effettua un'analisi approfondita di questa query SQL:{context}

```sql
{query}
```

Fornisci un'analisi dettagliata che copra:

1. ANALISI STRUTTURALE:
- Tipo di query e operazioni principali
- Complessità delle join e loro impatto
- Qualità delle condizioni di filtro

2. PERFORMANCE:
- Potenziali colli di bottiglia
- Suggerimenti per gli indici
- Ottimizzazioni del piano di esecuzione

3. SUGGERIMENTI PRATICI:
- Riscrittura della query (se applicabile)
- Modifiche strutturali consigliate
- Best practices PostgreSQL specifiche

Organizza la risposta in sezioni chiare e fornisci esempi concreti dove possibile."""

        return self._call_groq(prompt)

    def _call_groq(self, prompt: str) -> Dict:
        """Make API call to GROQ"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": "mixtral-8x7b-32768",
            "messages": [
                {
                    "role": "system",
                    "content": "Sei un esperto DBA PostgreSQL che analizza e ottimizza query SQL."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.7,
            "max_tokens": 2000
        }
        
        try:
            response = requests.post(self.base_url, headers=headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}