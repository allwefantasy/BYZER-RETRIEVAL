package tech.mlsql.retrieval.schema;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import tech.mlsql.retrieval.Utils;
import tech.mlsql.retrieval.batchserver.ArrowTypesConverter;
import tech.mlsql.retrieval.schema.types.ArrayType;
import tech.mlsql.retrieval.schema.types.MapType;
import tech.mlsql.retrieval.schema.types.SingleType;


import java.util.List;

/**
 * 11/10/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class SchemaUtils {

//    public static org.apache.arrow.vector.types.pojo.Field toArrowVector(BufferAllocator allocator, StructField s) {
//        var field = ArrowTypesConverter.toArrowField(s);
////        if(field.getFieldType())
////        field.createVector(allocator);
//    }

    public static Field toLuceneField(StructField s, Object value) {
        if (s.dataType() instanceof SingleType m) {
            if (m.name().equals("string") && s.analyze()) {
                return new TextField(s.name(), (String) value, Field.Store.YES);
            } else if (m.name().equals("string")) {
                return new StringField(s.name(), (String) value, Field.Store.YES);
            } else if (m.name().equals("int")) {
                return new IntField(s.name(), (Integer) value, Field.Store.YES);
            } else if (m.name().equals("long")) {
                Long newValue = 0l;
                if (value instanceof Integer) {
                    newValue = ((Integer) value).longValue();
                } else {
                    newValue = (Long) value;
                }
                return new LongField(s.name(), newValue, Field.Store.YES);
            } else if (m.name().equals("double")) {
                return new DoubleField(s.name(), (Double) value, Field.Store.YES);
            } else if (m.name().equals("float")) {
                Float newValue = 0.0f;
                if (value instanceof Double) {
                    newValue = ((Double) value).floatValue();
                } else {
                    newValue = (Float) value;
                }
                return new FloatField(s.name(), newValue, Field.Store.YES);
            } else {
                throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
            }

        } else if (s.dataType() instanceof MapType m) {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else if (s.dataType() instanceof ArrayType m) {
            if (m.dt() instanceof SingleType && ((SingleType) m.dt()).name().equals("float")) {
                // in json format, we use double to represent float
                // so we need to convert double to float
                var floatArray = Utils.toFloatArray((List<Double>) value);
                return new KnnFloatVectorField(s.name(), floatArray, VectorSimilarityFunction.COSINE);
            }
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else if (s.dataType() instanceof StructType m) {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        }
    }

    public static StructField getStructField(String schemaStr, String name) {
        var schema = new SimpleSchemaParser().parse(schemaStr);
        return schema.fields().stream().filter(f -> f.name().equals(name)).findFirst().get();
    }

    public static StructType getSchema(String schemaStr) {
        var schema = new SimpleSchemaParser().parse(schemaStr);
        return schema;
    }

    public static SortField toSortField(StructField s, boolean reverse) {
        if (s.dataType() instanceof SingleType m) {
            if (m.name().equals("string")) {
                return new SortField(s.name(), SortField.Type.STRING, reverse);
            } else if (m.name().equals("int")) {
                return new SortField(s.name(), SortField.Type.INT, reverse);
            } else if (m.name().equals("long")) {
                return new SortField(s.name(), SortField.Type.LONG, reverse);
            } else if (m.name().equals("double")) {
                return new SortField(s.name(), SortField.Type.DOUBLE, reverse);
            } else if (m.name().equals("float")) {
                return new SortField(s.name(), SortField.Type.FLOAT, reverse);
            } else {
                throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
            }

        } else if (s.dataType() instanceof MapType m) {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else if (s.dataType() instanceof ArrayType m) {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else if (s.dataType() instanceof StructType m) {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        }
    }

    public static Query toLuceneQuery(StructField s, Object v1, Object v2) {
        if (s.dataType() instanceof SingleType m) {
            if (m.name().equals("string") && s.analyze()) {
                Query parsedQuery = new SimpleQueryParser(new WhitespaceAnalyzer(), s.name()).
                        parse(v1.toString());
                return parsedQuery;
            } else if (m.name().equals("string")) {
                return new TermQuery(new Term(s.name(), (String) v1));
            } else if (m.name().equals("int")) {
                if (v1 == null) {
                    return IntPoint.newExactQuery(s.name(), (Integer) v2);
                } else if (v2 == null) {
                    return IntPoint.newExactQuery(s.name(), (Integer) v1);
                } else {
                    return IntPoint.newRangeQuery(s.name(), (Integer) v1, (Integer) v2);
                }
            } else if (m.name().equals("long")) {
                if (v1 == null) {
                    return LongPoint.newExactQuery(s.name(), (Long) v2);
                } else if (v2 == null) {
                    return LongPoint.newExactQuery(s.name(), (Long) v1);
                } else {
                    return LongPoint.newRangeQuery(s.name(), (Long) v1, (Long) v2);
                }
            } else if (m.name().equals("double")) {
                if (v1 == null) {
                    return DoublePoint.newExactQuery(s.name(), (Double) v2);
                } else if (v2 == null) {
                    return DoublePoint.newExactQuery(s.name(), (Double) v1);
                } else {
                    return DoublePoint.newRangeQuery(s.name(), (Double) v1, (Double) v2);
                }
            } else if (m.name().equals("float")) {
                if (v1 == null) {
                    return FloatPoint.newExactQuery(s.name(), (Float) v2);
                } else if (v2 == null) {
                    return FloatPoint.newExactQuery(s.name(), (Float) v1);
                } else {
                    return FloatPoint.newRangeQuery(s.name(), (Float) v1, (Float) v2);
                }
            } else {
                throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
            }

        } else if (s.dataType() instanceof MapType m) {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else if (s.dataType() instanceof ArrayType m) {
            if (m.dt() instanceof SingleType && ((SingleType) m.dt()).name().equals("float")) {
                var floatArray = Utils.toFloatArray((List<Double>) v1);
                return new KnnFloatVectorQuery(s.name(), floatArray, 10);
            }
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else if (s.dataType() instanceof StructType m) {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        }
    }

}
