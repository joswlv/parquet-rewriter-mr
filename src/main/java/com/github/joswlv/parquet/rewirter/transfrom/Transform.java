package com.github.joswlv.parquet.rewirter.transfrom;

@FunctionalInterface
public interface Transform<S, T> {

  T transform(S data);
}
