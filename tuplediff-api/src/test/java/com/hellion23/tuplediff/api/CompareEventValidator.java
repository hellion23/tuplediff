package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.compare.ComparableLinkedList;
import com.hellion23.tuplediff.api.compare.CompareEvent;
import com.hellion23.tuplediff.api.format.TypeFormatter;
import com.hellion23.tuplediff.api.format.TypeFormatterLibrary;
import com.hellion23.tuplediff.api.handler.CompareEventHandler;
import com.hellion23.tuplediff.api.model.TDColumn;
import com.hellion23.tuplediff.api.model.TDStreamMetaData;
import com.hellion23.tuplediff.api.model.TDTuple;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.*;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Used for JUnit testing. You can manually build the validation by the addXXX calls or read from file.
 * This is a CompareEventHandler intended to be injected into the TupleDiff and will compare and validate correctness
 * and run assertions on every comparison result.
 *
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Slf4j
public class CompareEventValidator implements CompareEventHandler {
    String name;
    private final static Logger LOG = LoggerFactory.getLogger(CompareEventValidator.class);
    static ObjectMapper mapper = new ObjectMapper();

    {
        mapper.registerSubtypes(Validate.class);
    }

    List<String>primaryKeyColumns;

    static final Validate MATCHED = new Validate(CompareEvent.TYPE.FOUND_MATCHED, null);

    TreeMap<ComparableLinkedList<Comparable>, Validate> expecteds = new TreeMap<>();

    @JsonProperty(value="validates")
    List<Validate> validates = new LinkedList<>();

    public CompareEventValidator() {}

    /**
     * Expects JSON file in format:
     * {
     *     "validates" : [
     *        {
     *            "pk" : ["4"],
     *            "break_type" : "FOUND_BREAK" ,
     *            "break_fields" : ["DOB"]
     *        },
     *        {
     *            "pk" : ["5"],
     *            "break_type" : "FOUND_ONLY_RIGHT"
     *        },
     *        etc...
     *     ]
     * }
     *
     * Only Breaks need to be defined. Assumes all other keys are meant to be matched.
     *
     * @param path
     * @return
     * @throws IOException
     */
    public static CompareEventValidator fromFile(String name, String path) throws IOException {
        CompareEventValidator cev = mapper.readValue(new File(path), CompareEventValidator.class);
        cev.name = name;
        return cev;
    }

    public static CompareEventValidator fromFile(String path) throws IOException {
        return fromFile("UNNAMED", path);
    }

    public void validateOnlyLeft(Comparable ... pks) {
        validates.add(new Validate(CompareEvent.TYPE.FOUND_ONLY_LEFT, null, pks));
    }

    public void validateOnlyRight(Comparable ... pks) {
        validates.add(new Validate(CompareEvent.TYPE.FOUND_ONLY_RIGHT, null, pks));
    }

    public void validateBreak(List<String> breakFields, Comparable ... pks) {
        validates.add(new Validate(CompareEvent.TYPE.FOUND_BREAK, breakFields, pks));
    }

    public void validate(Validate validate) {
        validates.add(validate);
    }

    @Override
    public void init(TDCompare comparison) {
        this.primaryKeyColumns = comparison.getPrimaryKey();
        String [] pkColNames = this.primaryKeyColumns.toArray(new String[primaryKeyColumns.size()]);
        // Just pick the left side. Left and Right meta data should have been normalized.
        TDStreamMetaData metaData = comparison.getNormalizedLeft().getMetaData();
        // Determine what classes each of the primary key values are supposed to be.
        Map<String, Class> streamClasses = new HashMap<>();
        this.primaryKeyColumns.stream().forEach(p -> streamClasses.put(p, metaData.getColumn(p).getColumnClass()));

        // Formatter map that would transform Validate.primaryKey
        Map<String, TypeFormatter> formatters = new HashMap<>();

        validates.stream().forEach( x -> {
            // Normalize the primary keys. The keys defined from JSON can be Integer or Double, etc... while
            // the Stream may have BigDecimals. This will cause compareTo failures. In order to prevent this, primitive
            // types are cast into a type that can be comparable (BigDecimal).
            ComparableLinkedList<Comparable> normalized = new ComparableLinkedList<>();
            for (int i=0; i<pkColNames.length; i++) {
                Comparable c = (Comparable) x.pk[i];
                if (c == null) {
                    //null requires no transformation, just add as is.
                    normalized.add(null);
                }
                else {
                    if (!formatters.containsKey(pkColNames[i])) {
                        // Class of the validator primary key column value
                        Class fromClass = c.getClass();
                        // Class of the left/right stream's primary key column.
                        Class toClass = streamClasses.get(pkColNames[i]);
                        // Derive the formatter object that will transform the validator primary key column value
                        // to be the same as the stream's class so compareTo method calls do not fail.
                        TypeFormatter formatter = TypeFormatterLibrary.getTypeFormatter(fromClass, toClass);
                        formatters.put(pkColNames[i], formatter);

                        LOG.info("Reformatting pk column {} from {} to {} using formatter {}", pkColNames[i], fromClass, toClass, formatter);
                    }
                    TypeFormatter formatter = formatters.get(pkColNames[i]);
                    if (formatter == null) {
                        // Classs is the same, do nothing but just add as is.
                        normalized.add(c);
                    }
                    else {
                        normalized.add((Comparable)formatter.apply(c));
                    }
                }
            }
            // Override the primaryKeys to be in the same Class as the actual dataset.
            x.setPrimaryKeys(normalized);
            expecteds.put(x.getPrimaryKeys(), x);
        });
        LOG.info("To validate: \n{} ", validates.stream().map(x -> x.toString()).collect(Collectors.joining("\n")));
    }

    static ComparableLinkedList<Comparable> normalizePrimaryKeys (Set<TDColumn> metaDataColumns, ComparableLinkedList<Comparable> pks ) {
        ComparableLinkedList<Comparable> result = new ComparableLinkedList<>();

        return result;
    }

    @Override
    public void accept(CompareEvent compareEvent) {
        Validate actual = null, expect=null;
        try {
            actual = new Validate (compareEvent, this.primaryKeyColumns);
            expect = expecteds.get(actual.getPrimaryKeys());
        }
        catch (Exception ex) {
            LOG.error(name + ": Error trying to determine expected and failed to compare ", ex);
            Assert.fail("There should not be an exception processing validation steps");
        }

        switch (compareEvent.getType()) {
            case FOUND_MATCHED:
                Assert.assertEquals(name + ": Expected FOUND_MATCHED for " + actual.getPrimaryKeys(), MATCHED, actual);
                break;
            case FOUND_BREAK:
            case FOUND_ONLY_LEFT:
            case FOUND_ONLY_RIGHT:
                Assert.assertEquals(name + String.format(":  Mismatched compare types for %s, Left: <%s>, Right<%s>",
                        actual.getPrimaryKeys(), compareEvent.getLeft(), compareEvent.getRight()),
                        expect, actual);
                break;
        }
    }

    @Override
    public void close() throws Exception {
        //noop
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public static class Validate {
        @JsonProperty(value="pk")
        Object[] pk;

        @JsonIgnore
        ComparableLinkedList<Comparable> primaryKeys;

        @JsonProperty(value="break_fields")
        Set<String> fieldBreaks;

        Set<String> normalizedFieldBreaks = null;

        @JsonIgnore
        CompareEvent.TYPE break_type;

        @JsonGetter(value="break_type")
        private String marshalheaders() {
            return break_type.toString();
        }

        @JsonSetter(value="break_type")
        private void unmarshalHeaders (String string) {
            break_type = CompareEvent.TYPE.valueOf(string);
        }

        protected Validate () {}

        protected Validate (CompareEvent e, List<String> primaryKeyColumns) {
            this (e.getType(), e.getFieldBreaks(), extract (e.getLeft(), e.getRight(), primaryKeyColumns));
        }

        public Validate(CompareEvent.TYPE break_type, Collection<String> breakFields, Comparable ... primaryKeyValues) {
            this.pk = primaryKeyValues;
            getPrimaryKeys();
            this.fieldBreaks = breakFields == null ? null : new HashSet<>(breakFields);
            this.break_type = break_type;
        }

        static Comparable [] extract (TDTuple left, TDTuple right, List<String>primaryKeyColumns) {
            String colNames [] = primaryKeyColumns.toArray(new String[primaryKeyColumns.size()]);
            Comparable pks [] = new Comparable[colNames.length];
            for (int i =0; i<primaryKeyColumns.size(); i++) {
                pks[i] = (Comparable) Optional.ofNullable(left).orElse(right).getValue(colNames[i]);
            }
            return pks;
        }

        public ComparableLinkedList<Comparable> getPrimaryKeys () {
            if (primaryKeys == null && pk != null) {
                this.primaryKeys = new ComparableLinkedList<>();
                for (Object x : pk) {
                    this.primaryKeys.add((Comparable)x);
                }
            }
            return primaryKeys;
        }

        public void setPrimaryKeys (ComparableLinkedList<Comparable> primaryKeys) {
            this.primaryKeys = primaryKeys;
            this.pk = primaryKeys.toArray();
        }

        @Override
        public String toString() {
            return "Validate{" +
                    "primaryKeys=" + getPrimaryKeys() +
                    ", fieldBreaks=" + fieldBreaks +
                    ", break_type=" + break_type +
                    '}';
        }

        private Set<String> getNormalizedFieldBreaks () {
            if (this.fieldBreaks != null && this.normalizedFieldBreaks == null) {
                this.normalizedFieldBreaks = this.fieldBreaks.stream().map(TDUtils::normalizeColumnName).collect(Collectors.toSet());
            }
            return this.normalizedFieldBreaks;
        }

        /**
         * Compares only the field breaks and break type *NOT* the primary key. It is expected that the primary
         * key has already been matched.
         * @param o
         * @return
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Validate)) return false;
            Validate validate = (Validate) o;
            return Objects.equals(getNormalizedFieldBreaks(), validate.getNormalizedFieldBreaks()) &&
                break_type == validate.break_type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldBreaks, break_type);
        }
    }
}
