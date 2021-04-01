package com.hellion23.tuplediff.api.format;

import com.hellion23.tuplediff.api.model.TDSide;

/**
 * Wraps a TypeComparator that wraps the both the Formatter, the field fieldNames and the side that
 * it should be applied to.
 *
 * Created by hleung on 6/2/2017.
 */
public class FieldTypeFormatter <T, R extends Comparable> extends TypeFormatter<T, R>{
    TypeFormatter <T,R> formatter;
    TDSide side;
    MatchCriteria criteria;

    public FieldTypeFormatter (TypeFormatter <T,R> formatter, TDSide side, MatchCriteria criteria) {
//        this.fieldNames = new HashSet<>(Arrays.asList(fieldNames));
        this.criteria = criteria;
        this.side = side;
        this.formatter = formatter;
    }

    public TypeFormatter<T, R> getFormatter() {
        return formatter;
    }

    public MatchCriteria getMatchCriteria () {
        return criteria;
    }

    public TDSide getSide() {
        return side;
    }

    @Override
    public Class getType() {
        return formatter.getType();
    }

    @Override
    public R apply(T t) {
        return formatter.apply (t);
    }

    @Override
    public String toString() {
        return "FieldTypeFormatter{" +
                "formatter=" + formatter +
                ", matchCriteria=" + criteria +
                ", side=" + side +
                "} " + super.toString();
    }
}
