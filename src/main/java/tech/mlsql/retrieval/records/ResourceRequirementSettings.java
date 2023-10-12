package tech.mlsql.retrieval.records;

import java.io.Serializable;
import java.util.List;

/**
 * 10/11/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class ResourceRequirementSettings implements Serializable {

    public static final String CPU = "CPU";
    public static final String MEMORY = "MEMORY";
    public static final String GPU = "GPU";

    private List<ResourceRequirement> resourceRequirements;

    public ResourceRequirementSettings(List<ResourceRequirement> resourceRequirements) {
        this.resourceRequirements = resourceRequirements;
    }

    public ResourceRequirementSettings() {
    }

    public List<ResourceRequirement> getResourceRequirements() {
        return resourceRequirements;
    }

    public void setResourceRequirements(List<ResourceRequirement> resourceRequirements) {
        this.resourceRequirements = resourceRequirements;
    }
}
