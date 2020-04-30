package io.nosqlbench.engine.api.activityapi.core;

public class ParameterModel {
    private String name;
    private String description;
    private Object defaultValue;
    private boolean required;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public ParameterModel(String name, String description, Object defaultValue, boolean required) {
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
        this.required = required;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }
}
