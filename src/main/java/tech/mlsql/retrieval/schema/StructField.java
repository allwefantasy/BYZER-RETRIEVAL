package tech.mlsql.retrieval.schema;

import tech.mlsql.retrieval.schema.types.DataType;

import java.util.Map;

/**
 * 10/7/23 WilliamZhu(allwefantasy@gmail.com)
 */
public record StructField(String name, DataType dataType, boolean analyze,boolean nullable, Map<String,Object> metadata) {
}




