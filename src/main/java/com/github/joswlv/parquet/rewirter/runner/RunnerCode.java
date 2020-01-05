package com.github.joswlv.parquet.rewirter.runner;

public enum RunnerCode {
  SUCCESS(0),
  FAIL(1);

  private int code;

  RunnerCode(int code) {
    this.code = code;
  }

  public int code() {
    return code;
  }
}
