package br.com.cursor.demo.executor;

import br.com.cursor.demo.executor.task.ExporterTask;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TypeProcessorExecutorService {

    private AtomicInteger running;
    private AtomicInteger queued;
    private AtomicInteger totalProcessed;

    private ExecutorService executorService;
    private static TypeProcessorExecutorService typeExporterExecutorService;

    private TypeProcessorExecutorService() {
        this.running = new AtomicInteger();
        this.queued = new AtomicInteger();
        this.totalProcessed = new AtomicInteger();
        final int threadPoolSize = Runtime.getRuntime().availableProcessors() - 1 == 1 ? 1 : Runtime.getRuntime().availableProcessors();
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    public static TypeProcessorExecutorService getInstance() {
        if(typeExporterExecutorService == null) {
            typeExporterExecutorService = new TypeProcessorExecutorService();
        }
        return typeExporterExecutorService;
    }

    public void enqueueTask(final ExporterTask task) {
        this.executorService.execute(task);
        this.queued.incrementAndGet();
    }

    public void incrementRunningTasks() {
        this.queued.decrementAndGet();
        this.running.incrementAndGet();
    }

    public void incrementProcessedTasks() {
        this.running.decrementAndGet();
        this.totalProcessed.incrementAndGet();
    }

    public int getProcessedTasks() {
        return this.totalProcessed.get();
    }

    public void waitExecutionToFinish() {
        boolean isFinished;
        do {
            isFinished = isFinished();
        } while(!isFinished);
        this.shutdown();
    }

    private boolean isFinished() {
        return this.queued.get() == 0 && this.running.get() == 0;
    }

    public void shutdown() {
        executorService.shutdown();
    }
}
