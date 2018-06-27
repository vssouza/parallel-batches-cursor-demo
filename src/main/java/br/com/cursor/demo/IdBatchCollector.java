package br.com.cursor.demo;

import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class IdBatchCollector implements Collector<MongoCursor<Document>, List<List<Integer>>, List<List<Integer>>> {

    private int batchSize;

    public IdBatchCollector(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public Supplier<List<List<Integer>>> supplier() {
        return ArrayList::new;
    }

    @Override
    public BiConsumer<List<List<Integer>>, MongoCursor<Document>> accumulator() {
        return (list, val) -> {
            if(list.size() == 0 || list.get(list.size() - 1).size() == batchSize) {
                list.add(new ArrayList<>(batchSize));
            }
            list.get(list.size() - 1);
            while (val.hasNext()) {
                Document dc = val.next();
                if(list.get(list.size() - 1).size() >= batchSize) {
                    list.add(new ArrayList<>(batchSize));
                }
                list.get(list.size() - 1).add(dc.getInteger("id"));
            }
        };
    }

    @Override
    public BinaryOperator<List<List<Integer>>> combiner() {
        return (left, right) ->{
            left.addAll(right);
            return left;
        };
    }

    @Override
    public Function<List<List<Integer>>, List<List<Integer>>> finisher() {
        return Function.identity();
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }
}
