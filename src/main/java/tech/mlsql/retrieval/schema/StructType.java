package tech.mlsql.retrieval.schema;

import tech.mlsql.retrieval.schema.types.DataType;

import java.util.List;

/**
 * 10/7/23 WilliamZhu(allwefantasy@gmail.com)
 */
public record StructType(List<StructField> fields) implements DataType {}
