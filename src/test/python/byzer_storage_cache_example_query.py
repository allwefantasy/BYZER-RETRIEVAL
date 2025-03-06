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



def main():
    emb_llm = get_single_llm("emb2", product_mode="lite")
    # Create a temporary directory for our example
    temp_dir = tempfile.mkdtemp()
    try:              
        # Initialize LocalByzerStorageCache
        required_exts = [".py", ".md"]  # Only process Python and Markdown files
        args = AutoCoderArgs(
            rag_build_name="sample_cache",
            hybrid_index_max_output_tokens=10000
        )
        
        cache_manager = LocalByzerStorageCache(
            path=temp_dir,
            ignore_spec=None,
            required_exts=required_exts,
            extra_params=args,
            emb_llm=emb_llm
        )        
                        
        # Query the cache
        logger.info("Querying the cache...")
        
        # Text search example
        text_result = cache_manager.get_cache({"query": "calculator", "enable_vector_search": False})
        logger.info(f"Text search for 'calculator' found {len(text_result)} files")
        for file_path, data in text_result.items():
            logger.info(f"Found in file: {file_path}")
            logger.info(f"Data: {data}")
        
        # Vector search example
        vector_result = cache_manager.get_cache({"query": "How to calculate an average?"})
        logger.info(f"Vector search for 'How to calculate an average?' found {len(vector_result)} files")
        for file_path, data in vector_result.items():
            logger.info(f"Found in file: {file_path}")
            logger.info(f"Data: {data}")
            
    finally:
        # Clean up
        logger.info("Cleaning up temporary directory...")
        shutil.rmtree(temp_dir)
        logger.info("Cleanup complete")

if __name__ == "__main__":
    main() 