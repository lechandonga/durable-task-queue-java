package com.github.lechandonga.durabletaskqueuejava.web;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class TaskNotFoundException extends RuntimeException {
    public TaskNotFoundException(String identifier) {
        super("任务不存在: " + identifier);
    }
}
