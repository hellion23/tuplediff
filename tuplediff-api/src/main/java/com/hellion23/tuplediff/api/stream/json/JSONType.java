package com.hellion23.tuplediff.api.stream.json;

import com.hellion23.tuplediff.api.compare.ComparableLinkedHashMap;
import com.hellion23.tuplediff.api.compare.ComparableLinkedList;

import java.math.BigDecimal;


public enum JSONType {
    STRING  (String.class                   ),
    NUMBER  (BigDecimal.class               ),
    ARRAY   (ComparableLinkedList.class     ),
    OBJECT  (ComparableLinkedHashMap.class  ),
    NULL    (Comparable.class               ),
    BOOLEAN (Boolean.class                  );

    private final Class javaClass;

    JSONType (Class javaClass) {
        this.javaClass = javaClass;
    }

    /**
     * The Java class representation of this JSON type.
     * @return
     */
    public Class getJavaClass () {
        return javaClass;
    }
}
