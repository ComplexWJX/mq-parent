package com.asiainfo.activeMq.message;

import lombok.Data;

import java.io.Serializable;

/**
 * @author WJX
 * @title: MessageBean
 * @projectName aif
 * @description: TODO
 * @date 2020/8/3 0003
 */
@Data
public class MessageBean implements Serializable {

    private static final long serialVersionUID = 628547460414769774L;
    private Long messageId;
    private String code;
    private String text;

    @Override
    public String toString() {
        return "MessageBean{" +
                "messageId=" + messageId +
                ", code='" + code + '\'' +
                ", text='" + text + '\'' +
                '}';
    }
}
