package com.facebook.presto.plugin.oracle;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;

public class IgnoreFieldException extends PrestoException {
    public IgnoreFieldException(String s) {
        super(StandardErrorCode.GENERIC_INTERNAL_ERROR, s);
    }
}
