package com.github.lechandonga.durabletaskqueuejava.core;

import java.util.Map;

/**
 * journal 中的一条不可变事件。{@code data} 为事件负载（字段名稳定，构成磁盘格式）。
 */
public record JournalEvent(long seq, String type, Map<String, Object> data) {

    public static final String ENQUEUED = "ENQUEUED";
    public static final String LEASED = "LEASED";
    public static final String LEASE_RENEWED = "LEASE_RENEWED";
    public static final String LEASE_RELEASED = "LEASE_RELEASED";
    public static final String SUCCEEDED = "SUCCEEDED";
    public static final String RETRY_SCHEDULED = "RETRY_SCHEDULED";
    public static final String DEAD_LETTERED = "DEAD_LETTERED";
    /** 压缩后写出的任务快照，重放时等价于该任务此前全部事件的折叠。 */
    public static final String SNAPSHOT = "SNAPSHOT";
    public static final String COMPACTION_BOUNDARY = "COMPACTION_BOUNDARY";
}
