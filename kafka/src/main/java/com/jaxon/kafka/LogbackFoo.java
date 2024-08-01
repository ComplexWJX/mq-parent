package com.jaxon.kafka;

import lombok.extern.slf4j.Slf4j;

/**
 * @Author rukawa
 * @Create 2023/3/12 0012 21:29
 * @Description todo
 * @Version V1.0
 */
@Slf4j
public class LogbackFoo {
    public void logInfo(String arg) {
        log.info("hello, {}", arg);
    }
}
