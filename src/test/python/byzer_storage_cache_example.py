import os
import tempfile
import shutil
import time
import pathspec
from autocoder.common import AutoCoderArgs
from autocoder.rag.cache.local_byzer_storage_cache import LocalByzerStorageCache
from loguru import logger
from autocoder.utils.llms import get_single_llm
from autocoder.rag.variable_holder import VariableHolder
from tokenizers import Tokenizer    
import pkg_resources
import os
from autocoder.common import SourceCode
from autocoder.rag.token_counter import count_tokens

try:
    tokenizer_path = pkg_resources.resource_filename(
        "autocoder", "data/tokenizer.json"
    )
    VariableHolder.TOKENIZER_PATH = tokenizer_path
    VariableHolder.TOKENIZER_MODEL = Tokenizer.from_file(tokenizer_path)
except FileNotFoundError:
    tokenizer_path = None

def create_sample_files(temp_dir):
    """Create sample files for demonstration"""
    # Create a Python file
    python_file_path = os.path.join(temp_dir, "sample_code.py")
    with open(python_file_path, "w") as f:
        f.write("""
def hello_world():
    '''This function prints hello world'''
    print("Hello, World!")

class Calculator:
    '''A simple calculator class'''
    def add(self, a, b):
        '''Add two numbers'''
        return a + b
    
    def subtract(self, a, b):
        '''Subtract b from a'''
        return a - b
""")
    
    # Create a markdown file
    md_file_path = os.path.join(temp_dir, "README.md")
    with open(md_file_path, "w") as f:
        f.write("""
# Sample Project

This is a sample project to demonstrate the usage of LocalByzerStorageCache.

## Features
- Caching of file contents
- Vector search
- Text search
""")
    
    # Create a directory with a file to test recursion
    os.makedirs(os.path.join(temp_dir, "src", "utils"), exist_ok=True)
    utils_file_path = os.path.join(temp_dir, "src", "utils", "helpers.py")
    with open(utils_file_path, "w") as f:
        f.write("""
def format_string(text):
    '''Format a string with proper capitalization'''
    return text.strip().title()

def calculate_average(numbers):
    '''Calculate the average of a list of numbers'''
    return sum(numbers) / len(numbers) if numbers else 0
""")
    
    # Create a file that should be ignored
    ignore_dir = os.path.join(temp_dir, "node_modules")
    os.makedirs(ignore_dir, exist_ok=True)
    with open(os.path.join(ignore_dir, "ignore_me.js"), "w") as f:
        f.write("// This file should be ignored")
    
    return [python_file_path, md_file_path, utils_file_path]

def main():
    emb_llm = get_single_llm("emb2", product_mode="lite")
    # Create a temporary directory for our example
    temp_dir = tempfile.mkdtemp()
    try:
        logger.info(f"Created temporary directory: {temp_dir}")
        
        # Create sample files
        files = create_sample_files(temp_dir)
        logger.info(f"Created {len(files)} sample files")
        
        # Create .gitignore to specify files to ignore
        with open(os.path.join(temp_dir, ".gitignore"), "w") as f:
            f.write("node_modules\n__pycache__\n")
        
        # Parse the gitignore file
        with open(os.path.join(temp_dir, ".gitignore"), "r") as f:
            ignore_spec = pathspec.PathSpec.from_lines(
                pathspec.patterns.GitWildMatchPattern, f.readlines()
            )
        
        # Initialize LocalByzerStorageCache
        required_exts = [".py", ".md"]  # Only process Python and Markdown files
        args = AutoCoderArgs(
            rag_build_name="sample_cache",
            hybrid_index_max_output_tokens=10000
        )
        
        cache_manager = LocalByzerStorageCache(
            path=temp_dir,
            ignore_spec=ignore_spec,
            required_exts=required_exts,
            extra_params=args,
            emb_llm=emb_llm
        )        
        
        # Build the cache
        logger.info("Building cache...")
        cache_manager.build_cache()
        
        # Query the cache
        logger.info("Querying the cache...")
        
        # Text search example
        text_result = cache_manager.get_cache({"query": "calculator", "enable_vector_search": False})
        logger.info(f"Text search for 'calculator' found {len(text_result)} files")
        for file_path, data in text_result.items():
            logger.info(f"Found in file: {file_path}")
        
        # Vector search example
        vector_result = cache_manager.get_cache({"query": "How to calculate an average?"})
        logger.info(f"Vector search for 'How to calculate an average?' found {len(vector_result)} files")
        for file_path, data in vector_result.items():
            logger.info(f"Found in file: {file_path}")
        
        # Modify a file to test update mechanism
        logger.info("Modifying a file to test update mechanism...")
        with open(files[0], "a") as f:
            f.write("""
def multiply(a, b):
    '''Multiply two numbers'''
    return a * b
""")
        
        # Trigger update and query again
        logger.info("Triggering update...")
        cache_manager.trigger_update()
        time.sleep(1)  # Allow time for the background thread to process
        
        # Query again after update
        updated_result = cache_manager.get_cache({"query": "multiply"})
        logger.info(f"Search for 'multiply' after update found {len(updated_result)} files")
        for file_path, data in updated_result.items():
            logger.info(f"Found in file: {file_path}")
        
        # Create a new file
        logger.info("Creating a new file...")
        new_file_path = os.path.join(temp_dir, "new_file.py")
        with open(new_file_path, "w") as f:
            f.write("""
def divide(a, b):
    '''Divide a by b'''
    if b == 0:
        raise ValueError("Cannot divide by zero")
    return a / b
""")
        
        # Trigger update and query again
        logger.info("Triggering update for new file...")
        cache_manager.trigger_update()
        time.sleep(1)  # Allow time for the background thread to process
        
        # Query the new file
        new_file_result = cache_manager.get_cache({"query": "divide"})
        logger.info(f"Search for 'divide' found {len(new_file_result)} files")
        for file_path, data in new_file_result.items():
            logger.info(f"Found in file: {file_path}")
            
    finally:
        # Clean up
        logger.info("Cleaning up temporary directory...")
        shutil.rmtree(temp_dir)
        logger.info("Cleanup complete")

if __name__ == "__main__":
    main() 