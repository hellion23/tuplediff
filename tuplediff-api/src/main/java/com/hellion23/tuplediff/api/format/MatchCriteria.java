package com.hellion23.tuplediff.api.format;

import com.hellion23.tuplediff.api.TDUtils;
import com.hellion23.tuplediff.api.model.TDColumn;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Match TDColumn based on whether the name matches or whether it is assignable to the provided classes,
 * or if both, then check both.
 *
 * Created by hleung on 8/4/2017.
 */
@EqualsAndHashCode
@ToString
@Slf4j
public class MatchCriteria implements Function<TDColumn, Boolean> {
    Set<String> names;
    Set<String> labels;
    Set<Class> classes;

    static enum BY {
        NAME, CLASS
    }

    public MatchCriteria() {}

    public static MatchCriteria forLabels (String ... labels) {
        return new MatchCriteria (labels, null);
    }

    public static MatchCriteria forClasses (Class ... classes) {
        return new MatchCriteria (null, classes);
    }

    public MatchCriteria (String [] labels, Class [] classes) {
        this.labels = labels != null ? new HashSet(Arrays.asList(labels)) : null;
        if (this.labels != null) {
            names = this.labels.stream().map(TDUtils::normalizeColumnName).collect(Collectors.toSet());
        }
        this.classes = classes != null ? new HashSet(Arrays.asList(classes)) : null;
    }

    public Set<String> getNames() {
        return names;
    }

    public Set<String> getLabels () { return labels; }

    public Set<Class> getClasses() {
        return classes;
    }

    public void setClasses(Set<Class> classes) {
        this.classes = classes;
    }

    /**
     * Match by Name or by Class?
     * @return
     */
    public BY getBy () {
        return names != null ? BY.NAME : BY.CLASS;
    }

    @Override
    public Boolean apply(TDColumn tdColumn) {
        if (names !=null) {
            return names.contains(tdColumn.getName());
        }
        if (classes != null) {
            for (Class clazz : classes) {
                if (clazz.isAssignableFrom(tdColumn.getColumnClass())) {
                    return true;
                }
            }
        }
        return false;
    }

}
