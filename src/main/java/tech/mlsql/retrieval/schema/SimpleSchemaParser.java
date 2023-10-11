package tech.mlsql.retrieval.schema;

import org.apache.lucene.document.*;
import org.apache.lucene.index.VectorSimilarityFunction;
import tech.mlsql.retrieval.Utils;
import tech.mlsql.retrieval.schema.containers.TPair;
import tech.mlsql.retrieval.schema.types.ArrayType;
import tech.mlsql.retrieval.schema.types.DataType;
import tech.mlsql.retrieval.schema.types.SingleType;
import tech.mlsql.retrieval.schema.types.MapType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 10/7/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class SimpleSchemaParser {


    // parse string like this: st(field(name,string),
    // field(name1,st(field(name2,array(string)))),
    // field(name2,map(string,string))
    // )
    // to StructType(List(StructField(name,string),StructField(name1,StructType(List(StructField(name2,ArrayType(StringType,true),true,Map()))),true,Map()))))
    public static StructType parse(String schemaStr) {
        StructType root = new StructType(new ArrayList<>());
        return (StructType) _parse(trimWhiteSpaceAndNewLine(schemaStr), root);
    }

    private static String trimWhiteSpaceAndNewLine(String input) {
        return input.trim().replaceAll("\n","");
    }

    public static DataType _parse(String _schemaStr, StructType structType) {
        var schemaStr = _schemaStr.trim();
        if (startWith(schemaStr, "boolean")) {
            return new SingleType(schemaStr);
        } else if (startWith(schemaStr, "byte")) {
            return new SingleType(schemaStr);
        } else if (startWith(schemaStr, "short")) {
            return new SingleType(schemaStr);
        } else if (startWith(schemaStr, "date")) {
            return new SingleType(schemaStr);
        } else if (startWith(schemaStr, "long")) {
            return new SingleType(schemaStr);
        } else if (startWith(schemaStr, "float")) {
            return new SingleType(schemaStr);
        } else if (startWith(schemaStr, "double")) {
            return new SingleType(schemaStr);
        } else if (startWith(schemaStr, "decimal")) {
            return new SingleType(schemaStr);
        } else if (startWith(schemaStr, "binary")) {
            return new SingleType(schemaStr);
        } else if (startWith(schemaStr, "string")) {
            return new SingleType(schemaStr);
        } else if (startWith(schemaStr, "array")) {
            var s = findInputInArrayBracket(schemaStr);
            return new ArrayType(_parse(s, structType));
        } else if (startWith(schemaStr, "map")) {
            var s = findKeyAndValue(findInputInArrayBracket(schemaStr));
            return new MapType(_parse(s.first(), structType), _parse(s.second(), structType));
        } else if (startWith(schemaStr, "st")) {
            StructType newStructType = new StructType(new ArrayList<>());
            if (structType.fields().isEmpty()) {
                newStructType = structType;
            }
            var value = findInputInArrayBracket(schemaStr);
            return _parse(value, newStructType);

        } else if (startWith(schemaStr, "field")) {
            List<String> fields = new ArrayList<>();
            findFieldArray(schemaStr, fields);
            for (var field : fields) {
                var pair = findKeyAndValue(findInputInArrayBracket(field));
                var name = pair.first();
                var value = pair.second();
                var analyze = false;
                if (pair.more().size() > 0) {
                    analyze = pair.more().get(0).equals("analyze");
                }
                structType.fields().add(new StructField(name, _parse(value, structType), analyze, true, new HashMap<String, Object>()));
            }
            return structType;
        } else {
            throw new RuntimeException("not support");
        }

    }

    private static void findFieldArray(String input, List<String> fields) {

        int max = input.length();
        int fBracketCount = 0;
        int position = 0;
        boolean stop = false;

        for (int i = 0; i < max; i++) {

            if (!stop) {

                char c = input.charAt(i);

                if (c == '(') {
                    fBracketCount++;
                } else if (c == ')') {
                    fBracketCount--;
                    if (i == max - 1 && fBracketCount == 0) {
                        fields.add(input.substring(0, max));
                    }
                } else if (c == ',' && fBracketCount == 0) {
                    position = i;
                    fields.add(input.substring(0, position));
                    findFieldArray(input.substring(position + 1), fields);
                    stop = true;
                } else if (i == max - 1 && fBracketCount == 0) {
                    fields.add(input.substring(0, max + 1));
                }

            }

        }

    }

    private static TPair findKeyAndValue(String input) {

        int max = input.length() - 1;
        int fBracketCount = 0;
        List<Integer> splits = new ArrayList<>();

        for (int i = 0; i <= max; i++) {
            char c = input.charAt(i);
            if (c == '(') {
                fBracketCount++;
            } else if (c == ')') {
                fBracketCount--;
            } else if (c == ',' && fBracketCount == 0) {
                splits.add(i);
            }
        }

        //use the position in splits to split the input string
        List<String> chunks = new ArrayList<>();
        for (int i = 0; i < splits.size(); i++) {
            int position = splits.get(i);
            if (i == 0) {
                chunks.add(input.substring(0, position));
            } else {
                chunks.add(input.substring(splits.get(i - 1) + 1, position));
            }

            if (i == splits.size() - 1) {
                chunks.add(input.substring(position + 1));
            }
        }

        return new TPair(chunks.get(0), chunks.get(1), chunks.subList(2, chunks.size()));
    }

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
                if (value instanceof Integer){
                    newValue = ((Integer) value).longValue();
                }else {
                    newValue = (Long) value;
                }
                return new LongField(s.name(),  newValue, Field.Store.YES);
            } else if (m.name().equals("double")) {
                return new DoubleField(s.name(), (Double) value, Field.Store.YES);
            } else if (m.name().equals("float")) {
                Float newValue = 0.0f;
                if (value instanceof Double){
                    newValue = ((Double) value).floatValue();
                }else {
                    newValue = (Float) value;
                }
                return new FloatField(s.name(),  newValue, Field.Store.YES);
            } else {
                throw new RuntimeException(STR. "\{ s.name() } \{ m.name() } is not support" );
            }

        } else if (s.dataType() instanceof MapType m) {
            throw new RuntimeException(STR. "\{ s.name() } \{ s.dataType() } is not support" );
        } else if (s.dataType() instanceof ArrayType m) {
            if (m.dt() instanceof SingleType(String name) && name.equals("float")){
                // in json format, we use double to represent float
                // so we need to convert double to float
                var floatArray = Utils.toFloatArray((List<Double>)value);
                return new KnnFloatVectorField(s.name(), floatArray , VectorSimilarityFunction.COSINE);
            }
            throw new RuntimeException(STR. "\{ s.name() } \{ s.dataType() } is not support" );
        } else if (s.dataType() instanceof StructType m) {
            throw new RuntimeException(STR. "\{ s.name() } \{ s.dataType() } is not support" );
        } else {
            throw new RuntimeException(STR. "\{ s.name() } \{ s.dataType() } is not support" );
        }
    }

    private static String findInputInArrayBracket(String input) {
        int max = input.length() - 1;
        StringBuilder rest = new StringBuilder();
        boolean firstS = false;
        int fBracketCount = 0;

        for (int i = 0; i <= max; i++) {
            char c = input.charAt(i);
            if (c == '(') {
                if (firstS) {
                    rest.append(c);
                    fBracketCount++;
                } else {
                    firstS = true;
                }
            } else if (c == ')') {
                fBracketCount--;
                if (fBracketCount < 0) {
                    firstS = false;
                } else {
                    rest.append(c);
                }
            } else {
                if (firstS) {
                    rest.append(c);
                }
            }
        }

        return rest.toString();
    }

    public static boolean startWith(String c, String token) {
        return c.startsWith(token) || c.startsWith(STR. "\{ token } " ) || c.startsWith(STR. "\{ token }(" );
    }
}
