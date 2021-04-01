package com.hellion23.tuplediff.api.stream.sql;

import com.hellion23.tuplediff.api.model.TDColumn;

/**
 * Created by hleung on 5/30/2017.
 */
public class SQLTDColumn extends TDColumn {
    SQLTDStreamMetaData.Getter RSGetter;
    final int sqlType;

    public SQLTDColumn (String label, Class<?> clazz, int sqlType, SQLTDStreamMetaData.Getter RSGetter) {
        super(label, clazz, true);
        this.RSGetter = RSGetter;
        this.sqlType = sqlType;
    }


    public SQLTDStreamMetaData.Getter getter() {
        return RSGetter;
    }
}
