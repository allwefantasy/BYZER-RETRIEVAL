package tech.mlsql.retrieval;

import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import io.ray.runtime.AbstractRayRuntime;
import io.ray.runtime.object.ObjectRefImpl;
import org.apache.lucene.document.Document;
import tech.mlsql.retrieval.records.ClusterSettings;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Utils {
    public static <T> T toRecord(String json, Class<T> recordClass) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        return mapper.readValue(json, recordClass);
    }

    public static <T> String toJson(T record) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        return mapper.writeValueAsString(record);
    }

    public static <T extends Number> float[] toFloatArray(List<T> value) {
        var newFloatArray = new float[value.size()];
        for (int i = 0; i < value.size(); i++) {
            newFloatArray[i] = value.get(i).floatValue();
        }
        return newFloatArray;
    }

    public static Map<String, Object> documentToMap(Document doc) {
        var result = new HashMap<String, Object>();
        for (var field : doc.getFields()) {
            if (field.numericValue() != null) {
                result.put(field.name(), field.numericValue());
            } else if (field.binaryValue() != null) {
                result.put(field.name(), field.binaryValue());
            } else if (field.stringValue() != null) {
                result.put(field.name(), field.stringValue());
            } else if (field.readerValue() != null) {
                result.put(field.name(), field.readerValue());
            } else {
                throw new RuntimeException(field.fieldType().toString() + "not supported field type");
            }
        }
        return result;
    }

    public static List<String> defaultJvmOptions() {
        var jvmSettings = new ArrayList<String>();

        for (String arg : List.of(
                "java.base/java.lang",
                "java.base/java.lang.annotation",
                "java.base/java.lang.invoke",
                "java.base/java.lang.module",
                "java.base/java.lang.ref",
                "java.base/java.lang.reflect",
                "java.base/java.util",
                "java.base/java.util.concurrent",
                "java.base/java.util.concurrent.atomic",
                "java.base/java.util.concurrent.locks",
                "java.base/java.util.function",
                "java.base/java.util.jar",
                "java.base/java.util.regex",
                "java.base/java.util.stream",
                "java.base/java.util.zip",
                "java.base/java.util.spi",
                "java.base/java.text",
                "java.base/java.math",
                "java.base/java.io",
                "java.base/java.nio",
                "java.base/java.net",
                "java.base/java.time",
                "java.base/sun.nio.ch"
        )) {
            jvmSettings.add("--add-opens");
            jvmSettings.add(arg + "=ALL-UNNAMED");
        }

        jvmSettings.add("--enable-preview");
        jvmSettings.add("--add-modules");
        jvmSettings.add("jdk.incubator.vector");

//        jvmSettings.add("--add-modules");
//        jvmSettings.add("jdk.incubator.foreign");
        return jvmSettings;
    }

    public static <T> ObjectRef<T> readBinaryAsObjectRef(byte[] obj, Class<T> clazz, byte[] ownerAddress) {
        ObjectId id = new ObjectId(obj);
        ObjectRefImpl<T> ref = new ObjectRefImpl<>(id, clazz, false);
        AbstractRayRuntime runtime = (AbstractRayRuntime) Ray.internal();
        runtime.getObjectStore().registerOwnershipInfoAndResolveFuture(
                id, null, ownerAddress
        );
        return ref;
    }

    public static <T> ObjectRefImpl<T> objectRefConvert(ObjectRef<T> obj) {
        if (obj instanceof ObjectRefImpl) {
            return (ObjectRefImpl<T>) obj;
        } else {
            throw new RuntimeException(obj.getClass() + " is not ObjectRefImpl");
        }
    }

    public static int route(Object id, int numWorkers) {
        Long shardId = 0l;
        if (id instanceof Long) {
            shardId = (long)id % numWorkers;
        } else {
            shardId = (long) (id.toString().hashCode() % numWorkers);
        }
        return shardId.intValue();
    }

    // for test
    public static void writeExceptionToFile(Exception e) {
        // write exception to file
        var uuid = UUID.randomUUID().toString();
        var exceptionFile = Paths.get(String.format("/tmp/exception-%s.txt", uuid));
        // convert exception to string
        var exceptionString = new StringBuilder();
        exceptionString.append(e.getMessage());
        exceptionString.append("\n");
        for (var stackTraceElement : e.getStackTrace()) {
            exceptionString.append(stackTraceElement.toString());
            exceptionString.append("\n");
        }
        try {
            Files.writeString(exceptionFile, exceptionString.toString());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
