package com.github.lechandonga.durabletaskqueuejava.storage;

/**
 * 本地存储发生不可恢复的 I/O 错误（目录不可创建、写入失败等）时抛出。
 */
public class TaskStoreException extends RuntimeException {

    public TaskStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
