package tech.mlsql.retrieval.records;

import java.io.Serializable;
import java.util.List;

/**
 * 10/8/23 WilliamZhu(allwefantasy@gmail.com)
 */
//public record JVMSettings(List<String> options) {
//}

public class JVMSettings implements Serializable {
    private  List<String> options;

    public JVMSettings() {
    }

    public List<String> getOptions() {
        return options;
    }

    public void setOptions(List<String> options) {
        this.options = options;
    }

    public JVMSettings(List<String> options) {
        this.options = options;
    }

    public List<String> options() {
        return options;
    }
}
