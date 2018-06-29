package br.com.cursor.demo.entity;

public class DemoType {
    private String namespace;
    private String type;

    public DemoType(String namespace, String type) {
        this.namespace = namespace;
        this.type = type;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getType() {
        return type;
    }

    public static DemoType getDemoType(String composedType) {
        final String[] dividedType = composedType.split(" ");
        return new DemoType(dividedType[0], dividedType[1]);
    }
}
