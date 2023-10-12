package tech.mlsql.retrieval.records;

import java.io.Serializable;

/**
 * 10/12/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class ResourceRequirement implements Serializable {
    private String name;
    private double resourceQuantity;

    public ResourceRequirement(String name, double resourceQuantity) {
        this.name = name;
        this.resourceQuantity = resourceQuantity;
    }

    public ResourceRequirement() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getResourceQuantity() {
        return resourceQuantity;
    }

    public void setResourceQuantity(double resourceQuantity) {
        this.resourceQuantity = resourceQuantity;
    }
}
