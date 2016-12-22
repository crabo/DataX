package com.alibaba.datax.plugin.writer.elasticwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum EsErrorCode implements ErrorCode {
	    //连接错误
	    ERROR("es-error-01","errors occured");

	private final String code;

    private final String description;

    private EsErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}

