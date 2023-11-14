package tech.mlsql.retrieval.batchserver;


import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.index.IndexableField;
import tech.mlsql.retrieval.schema.StructField;
import tech.mlsql.retrieval.schema.StructType;
import tech.mlsql.retrieval.schema.types.ArrayType;
import tech.mlsql.retrieval.schema.types.MapType;
import tech.mlsql.retrieval.schema.types.SingleType;

import java.util.Collections;

/**
 * 11/9/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class ArrowTypesConverter {

    public static Field toArrowField(StructField s) {
        if (s.dataType() instanceof SingleType m) {
            if (m.name().equals("string") && s.analyze()) {
                return new Field(s.name(), FieldType.nullable(new ArrowType.Utf8()), null);
            } else if (m.name().equals("string")) {
                return new Field(s.name(), FieldType.nullable(new ArrowType.Utf8()), null);
            } else if (m.name().equals("int")) {
                return new Field(s.name(), FieldType.nullable(new ArrowType.Int(32, true)), null);
            } else if (m.name().equals("long")) {
                return new Field(s.name(), FieldType.nullable(new ArrowType.Int(64, true)), null);
            } else if (m.name().equals("double")) {
                return new Field(s.name(), FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            } else if (m.name().equals("float")) {
                return new Field(s.name(), FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
            } else {
                throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
            }
        } else if (s.dataType() instanceof MapType m) {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else if (s.dataType() instanceof ArrayType m) {
            if (m.dt() instanceof SingleType && ((SingleType) m.dt()).name().equals("float")) {
                Field floatField = new Field("float", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
                return new Field(s.name(), FieldType.nullable(new ArrowType.List()), Collections.singletonList(floatField));
            }
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else if (s.dataType() instanceof StructType m) {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        } else {
            throw new RuntimeException("{} {} is not support".formatted(s.name(), s.dataType()));
        }
    }


    private static Field convertLuceneFieldToArrowField(IndexableField luceneField) {
        String fieldName = luceneField.name();
        FieldType fieldType = null;

        switch (luceneField.fieldType().docValuesType()) {
            case NONE:
                // Handle full-text fields or fields without doc values.
                fieldType = new FieldType(true, new ArrowType.Utf8(), null);
                break;
            case NUMERIC:
                // Map numeric fields to an appropriate numeric type in Arrow.
                fieldType = new FieldType(true, new ArrowType.Int(64, true), null);
                break;
            case BINARY:
                // Binary fields can be mapped directly to an Arrow binary type.
                fieldType = new FieldType(true, new ArrowType.Binary(), null);
                break;
            case SORTED:
                // Sorted can often be strings, so Utf8 is used.
                fieldType = new FieldType(true, new ArrowType.Utf8(), null);
                break;
            case SORTED_NUMERIC:
                // Sorted numeric fields can be handled similar to numeric fields.
                fieldType = new FieldType(true, new ArrowType.Int(64, true), null);
                break;
            case SORTED_SET:
                // Sorted sets can be complex; often need to be flattened or handled as lists.
                fieldType = new FieldType(true, new ArrowType.Utf8(), null);
                break;
        }

        if (fieldType != null) {
            return new Field(fieldName, fieldType, null);
        } else {
            throw new RuntimeException("Unsupported field type: " + luceneField.fieldType().docValuesType());
        }
    }
}
