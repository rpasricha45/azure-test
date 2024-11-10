import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Environment
ENV = os.getenv('ENV', 'development')

# OpenAI settings
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# File paths configuration
class FileConfig:
    TEST_FILES = {
        'arboria': BASE_DIR / 'data' / 'test' / 'Harbor Court September 30, 2019 Rent Roll.xlsx',
        'amazing_grace': BASE_DIR / 'data' / 'test' / '01-2024 TRERA Rent Roll.XLSX',
        'clarendale': BASE_DIR / 'data' / 'test' / 'MM-Rent Roll.xlsx',
        'west_chester': BASE_DIR / 'data' / 'test' / 'West Chester Rent Roll_6.26.19.xlsx'
    }
    OUTPUT_DIR = BASE_DIR / 'output'
    DEFAULT_OUTPUT = 'processed_rent_roll.csv'

    @classmethod
    def setup_directories(cls):
        """Ensure all necessary directories exist"""
        cls.OUTPUT_DIR.mkdir(parents=True, exist_ok=True) 