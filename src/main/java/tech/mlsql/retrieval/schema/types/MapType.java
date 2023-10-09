package tech.mlsql.retrieval.schema.types;

/**
 * 10/7/23 WilliamZhu(allwefantasy@gmail.com)
 */
public record MapType(DataType keyType, DataType valueType) implements DataType {}
