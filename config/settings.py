# config/settings.py
from pydantic import BaseModel
from typing import Optional
from urllib.parse import urlparse
import streamlit as st

class Settings(BaseModel):
    DATABASE_URL: Optional[str] = None
    GROQ_API_KEY: str = st.secrets["GROQ_API_KEY"]
    
    @property
    def db_params(self):
        if not self.DATABASE_URL:
            raise ValueError("Database URL not set")
            
        url = urlparse(self.DATABASE_URL)
        return {
            'dbname': url.path[1:],  # Remove leading '/'
            'user': url.username,
            'password': url.password,
            'host': url.hostname,
            'port': url.port or 5432,
            'sslmode': 'require'  # Required for Neon.tech
        }
    
    # Redis settings (optional for caching)
    # REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
    # REDIS_PORT: int = int(os.getenv('REDIS_PORT', 6379))    

settings = Settings()


