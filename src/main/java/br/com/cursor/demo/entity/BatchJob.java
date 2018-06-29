package br.com.cursor.demo.entity;

public class BatchJob {
    private int batchId;
    private final DemoType demoType;
    private int[] ids;

    public BatchJob(final int batchId, final DemoType demoType, final int[] batchIds) {
        this.batchId = batchId;
        this.demoType = demoType;
        ids = batchIds;
    }

    public DemoType getDemoType() {
        return demoType;
    }

    public int getBatchId() {
        return batchId;
    }

    public int[] getIds() {
        return ids;
    }
}
