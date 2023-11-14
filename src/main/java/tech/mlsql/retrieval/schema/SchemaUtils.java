package tech.mlsql.retrieval.schema;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.lucene.document.*;
import org.apache.lucene.index.VectorSimilarityFunction;
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
                throw new RuntimeException();
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

}
