from src.config.settings import FileConfig

TEST_CONFIG = {
    'TEST_FILES': {
        name: str(path) for name, path in FileConfig.TEST_FILES.items()
    },
    'OUTPUT_DIR': str(FileConfig.OUTPUT_DIR),
    'DEFAULT_OUTPUT': FileConfig.DEFAULT_OUTPUT
}

def setup_test_env():
    """Create necessary test directories"""
    FileConfig.setup_directories()