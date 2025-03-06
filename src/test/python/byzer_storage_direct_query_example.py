import os
import tempfile
import json
from loguru import logger
from autocoder.utils.llms import get_single_llm
from byzerllm.apps.byzer_storage.local_simple_api import (
    LocalByzerStorage,
    DataType,
    FieldOption,
    SortOption,
)

def format_result(result):
    """Format search result for better readability"""
    return {
        "_id": result.get("_id", "N/A"),
        "file_path": result.get("file_path", "N/A"),
        "content_preview": result.get("raw_content", "N/A")[:100] + "..." if len(result.get("raw_content", "")) > 100 else result.get("raw_content", "N/A"),
        "score": result.get("_score", 0.0)
    }

def main():
    # Get the embedding model - same as used in the LocalByzerStorageCache
    emb_llm = get_single_llm("emb2", product_mode="lite")
    
    # Initialize LocalByzerStorage with the same parameters as LocalByzerStorageCache
    # This will connect to the same database and table
    storage = LocalByzerStorage(
        cluster_name="byzerai_store",
        database="rag_test",
        table="sample_cache",  # This matches the rag_build_name in the example
        host="127.0.0.1",
        port=33333,
        emb_llm=emb_llm
    )
    
    logger.info("Connected to Byzer Storage")
    
    # Demonstration 1: Simple text search
    logger.info("=== Text Search Demo ===")
    query_builder = storage.query_builder()
    query_builder.set_search_query("calculator", fields=["content"])
    text_results = query_builder.execute()
    
    logger.info(f"Text search for 'calculator' found {len(text_results)} results")
    for i, result in enumerate(text_results[:3], 1):  # Show first 3 results
        logger.info(f"Result {i}: {format_result(result)}")
    
    # Demonstration 2: Vector search
    logger.info("\n=== Vector Search Demo ===")
    query_builder = storage.query_builder()
    query_builder.set_vector_query("How to calculate an average?", fields=["vector"])
    vector_results = query_builder.execute()
    
    logger.info(f"Vector search for 'How to calculate an average?' found {len(vector_results)} results")
    for i, result in enumerate(vector_results[:3], 1):  # Show first 3 results
        logger.info(f"Result {i}: {format_result(result)}")
    
    # Demonstration 3: Combined search (vector + text)
    logger.info("\n=== Combined Search Demo ===")
    query_builder = storage.query_builder()
    query_builder.set_search_query("function", fields=["content"])
    query_builder.set_vector_query("mathematical operations", fields=["vector"])
    combined_results = query_builder.execute()
    
    logger.info(f"Combined search found {len(combined_results)} results")
    for i, result in enumerate(combined_results[:3], 1):  # Show first 3 results
        logger.info(f"Result {i}: {format_result(result)}")
    
    # Demonstration 4: Search with filters
    logger.info("\n=== Filtered Search Demo ===")
    query_builder = storage.query_builder()
    query_builder.set_search_query("code", fields=["content"])
    # Add a filter condition for a specific file path pattern
    and_filter = query_builder.and_filter()
    and_filter.add_condition("file_path", ".py")  # Filter for Python files
    and_filter.build()
    
    filtered_results = query_builder.execute()
    logger.info(f"Filtered search found {len(filtered_results)} results")
    for i, result in enumerate(filtered_results[:3], 1):  # Show first 3 results
        logger.info(f"Result {i}: {format_result(result)}")
    
    # Demonstration 5: Search with sorting
    logger.info("\n=== Sorted Search Demo ===")
    query_builder = storage.query_builder()
    query_builder.set_search_query("data", fields=["content"])
    query_builder.sort("mtime", SortOption.DESC)  # Sort by modification time, newest first
    
    sorted_results = query_builder.execute()
    logger.info(f"Sorted search found {len(sorted_results)} results")
    for i, result in enumerate(sorted_results[:3], 1):  # Show first 3 results
        logger.info(f"Result {i}: {format_result(result)}")
        if "mtime" in result:
            logger.info(f"  Modification time: {result['mtime']}")
    
    # Demonstration 6: Setting result limit
    logger.info("\n=== Limited Results Demo ===")
    query_builder = storage.query_builder()
    query_builder.set_search_query("function", fields=["content"])
    query_builder.set_limit(5)  # Only return top 5 results
    
    limited_results = query_builder.execute()
    logger.info(f"Limited search returned {len(limited_results)} results (max 5)")
    for i, result in enumerate(limited_results, 1):
        logger.info(f"Result {i}: {format_result(result)}")

if __name__ == "__main__":
    main() 