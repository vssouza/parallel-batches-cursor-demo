package br.com.cursor.demo.collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class FlattenListCollector<T> implements Collector<List<T>, List<T>, List<T>> {

    public Supplier<List<T>> supplier() {
        return ArrayList::new;
    }

    public BiConsumer<List<T>, List<T>> accumulator() {
        return (ts, t) -> ts.addAll(t);
    }

    public BinaryOperator<List<T>> combiner() {
        return (ts, ots) -> {
            ts.addAll(ots);
            return ts;
        };
    }

    public Function<List<T>, List<T>> finisher() {
        return Function.identity();
    }

    public Set<Characteristics> characteristics() {
        return Collections.singleton(Characteristics.UNORDERED);
    }
}
