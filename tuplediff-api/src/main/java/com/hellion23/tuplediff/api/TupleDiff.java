package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.compare.*;
import com.hellion23.tuplediff.api.config.*;
import com.hellion23.tuplediff.api.format.FieldTypeFormatter;
import com.hellion23.tuplediff.api.format.TypeFormatterLibrary;
import com.hellion23.tuplediff.api.handler.CascadingEventHandler;
import com.hellion23.tuplediff.api.handler.CompareEventHandler;
import com.hellion23.tuplediff.api.model.TDException;
import com.hellion23.tuplediff.api.model.TDSide;
import com.hellion23.tuplediff.api.model.TDStream;
import com.hellion23.tuplediff.api.stream.CSVTDStream;
import com.hellion23.tuplediff.api.stream.bean.BeanTDStream;
import com.hellion23.tuplediff.api.stream.json.JSONTDColumn;
import com.hellion23.tuplediff.api.stream.json.JSONTDStream;
import com.hellion23.tuplediff.api.stream.json.JSONType;
import com.hellion23.tuplediff.api.stream.source.*;
import com.hellion23.tuplediff.api.stream.sql.DataSourceProviders;
import com.hellion23.tuplediff.api.stream.sql.SortedSQLTDStream;
import com.hellion23.tuplediff.api.variable.VariableContext;
import com.hellion23.tuplediff.api.variable.VariableEngine;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.StringReader;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * Created by hleung on 7/1/2017.
 */
@Slf4j
public class TupleDiff {
    private final static Logger LOG = LoggerFactory.getLogger(TupleDiff.class);
    static ExecutorService executor = null;
    // Support conversion for enums.
    static BeanUtilsBean beanUtilsBean = new BeanUtilsBean(new ConvertUtilsBean(){
        @Override
        public Object convert(String value, Class clazz) {
            if (clazz.isEnum()){
                return Enum.valueOf(clazz, value);
            }else{
                return super.convert(value, clazz);
            }
        }
    });

    public static TDCompare configure (TDConfig config, CompareEventHandler ... eventHandlers) {
        CompareEventHandler eventHandler = eventHandlers.length > 1 ? new CascadingEventHandler(eventHandlers) : eventHandlers[0];
        return configure (config, eventHandler, null);
    }

    public static TDCompare configure (TDConfig config, CompareEventHandler eventHandler, ExecutorService executor) {
        return configure(config, new TupleDiffContext(eventHandler, executor, null));
    }

    public static TDCompare configure (TDConfig config, TupleDiffContext tupleDiffContext) {
        String name = Optional.ofNullable(config.getName()).orElse("TupleDiff-"+System.currentTimeMillis());
        List<String> primaryKey = Arrays.asList(config.getPrimarykey()); //TDUtils.normalizedColumnNames(Arrays.asList(config.getPrimarykey()));
        List<FieldTypeFormatter> formatters = constructFormatters (config.getFieldTypeFormatters());
        List<FieldComparator> comparators = constructComparators(config.getFieldComparators());
        List<String> excludeFields = config.getExcludefields() == null ? null : Arrays.asList(config.getExcludefields());
        TDStream left = constructStream(tupleDiffContext, config.getLeft(), TDSide.LEFT, formatters, primaryKey);
        TDStream right = constructStream(tupleDiffContext, config.getRight(), TDSide.RIGHT, formatters, primaryKey);
        TDCompare compare = new TDCompare(name, tupleDiffContext.getEventHandler(), tupleDiffContext.getExecutor(),
                primaryKey, left, right, excludeFields,
                true, comparators, formatters);
        return compare;
    }

    private static List<FieldComparator> constructComparators(ComparatorConfig[] comparatorConfigs) {
        if (comparatorConfigs == null) {
            return null;
        }
        List<FieldComparator> comparators = new LinkedList<>();
        for (ComparatorConfig cc : comparatorConfigs) {
            if (cc.getFieldClasses() == null && cc.getFieldNames() == null) {
                throw new TDException("Could not instantiate Comparator. Missing both field names or field classes. One or the other must be defined");
            }
            TypedComparator comp;
            try {
                if (cc.getThresholdDate() != null) {
                    comp = new ThresholdDateComparator(cc.getThresholdDate());
                }
                else if (cc.getThresholdNumber() != null) {
                    comp = new ThresholdNumberComparator(cc.getThresholdNumber());
                }
                else if (cc.getTruncateDate() != null) {
                    ChronoUnit unit = ChronoUnit.valueOf(cc.getTruncateDate());
                    comp = new TruncateDateComparator(unit);
                }
                else if (cc.getComparatorClass() != null) {
                    comp = (TypedComparator)cc.getComparatorClass().newInstance();
                    if (cc.getParams() != null) {
                        beanUtilsBean.populate(comp, asMap(cc.getParams()));
                    }
                }
                else {
                    throw new TDException("Could not instantiate Comparator. Comparator class must be defined," +
                            " or thresholddate, or thresholdnumber or thruncatedate.");
                }
            }
            catch (Exception e) {
                throw new TDException("Could not instantiate comparator using definition: " + cc.toString() + "; Exception: " + e.getMessage(), e);
            }
            ComparatorConfig.builder().thresholdNumber(500d).buildConfig();
            comparators.add( ComparatorLibrary.builder()
                    .classes(cc.getFieldClasses())
                    .names(cc.getFieldNames())
                    .comparator(comp).buildComparator());
        }
        return comparators;
    }

    private static List<FieldTypeFormatter> constructFormatters(FormatterConfig[] tfConfigs){
        return tfConfigs == null ? null : Arrays.asList(tfConfigs).stream().map(TypeFormatterLibrary.Builder::fromConfig).collect(Collectors.toList());
    }

    protected static TDStream constructStream(TupleDiffContext tupleDiffContext, StreamConfig config, TDSide side, List<FieldTypeFormatter> formatters, List<String> primaryKey) {
        String name = Optional.ofNullable(config.getName()).orElse (side.name());
        if (config.getClass() == CSVStreamConfig.class) {
            return constructCsvStream(name, (CSVStreamConfig)config, primaryKey, formatters, side, tupleDiffContext);
        }
        else if (config.getClass() == DBStreamConfig.class) {
            return constructDBStream(name, (DBStreamConfig)config, primaryKey, tupleDiffContext);
        }
        else if (config.getClass() == JSONStreamConfig.class) {
            return constructJSONStream(name, (JSONStreamConfig)config, primaryKey, tupleDiffContext);
        }
        else if (config.getClass() == BeanStreamConfig.class) {
            return constructBeanStream(name, (BeanStreamConfig)config, primaryKey);
        }
        else {
            throw new TDException("Do not know how to construct Stream config of type " + config.getClass());
        }
    }

    protected static TDStream constructBeanStream(String name, BeanStreamConfig config, List<String> primaryKey) {
        List<String> sortKey = config.isSortedByPrimaryKey() ? primaryKey : null;
        return new BeanTDStream(name, config.getBeanClass(), config.getIterator(), sortKey);
    }


    public static TDStream constructJSONStream(String name, JSONStreamConfig config, List<String> primaryKey, TupleDiffContext tupleDiffContext) {
        String [] pkPaths = config.getPrimarykeypaths();
        if (pkPaths != null && pkPaths.length > 0) {
            primaryKey = Arrays.asList(pkPaths);
        }
        LinkedHashMap<String, JSONType> comparecolumns = config.getComparecolumns();
        List<JSONTDColumn> interestedColumns = new LinkedList<>();
        if (comparecolumns != null && comparecolumns.size() >0) {
            comparecolumns.forEach( (a, b) -> {
                interestedColumns.add(new JSONTDColumn(b, a));
            });
        }

        JSONTDStream jsonStream = new JSONTDStream(name,
                constructStreamSource(config.getSource(), tupleDiffContext),
                primaryKey,
                false,
                config.getPathToTuples(),
                interestedColumns);
        return jsonStream;
    }

    protected static TDStream constructCsvStream (String name, CSVStreamConfig config, List<String> primaryKey, List<FieldTypeFormatter> formatters, TDSide side, TupleDiffContext tupleDiffContext) {
        CSVTDStream csvStream = new CSVTDStream(name, constructStreamSource(config.getSource(), tupleDiffContext));
        return csvStream;
    }


    public static StreamSource constructStreamSource (SourceConfig srcConfig, TupleDiffContext tupleDiffContext) {
        StreamSource streamSource;
        if (srcConfig.getClass() == FileSourceConfig.class) {
            FileSourceConfig cfg = (FileSourceConfig) srcConfig;
            streamSource =  new FileStreamSource(cfg.getPath());
        }
        else if (srcConfig.getClass() == HttpSourceConfig.class) {
            HttpSourceConfig cfg = (HttpSourceConfig) srcConfig;
            HttpAuth auth = constructHttpAuth (cfg.getAuth());
            if (tupleDiffContext == null) {
                tupleDiffContext = new TupleDiffContext();
            }
            VariableContext variableContext = tupleDiffContext.getVariableContext();
            List<String> urls = VariableEngine.doVariableSubstitutionsMulti(cfg.getUrl(), variableContext);
            List<String> requestPayloads = VariableEngine.doVariableSubstitutionsMulti(cfg.getRequestPayload(), variableContext);

            // Whoah! This Variable substitution created a forked bunch of URL's or Request Payloads!!
            if (urls.size() > 1 || requestPayloads.size() > 1) {
                CompositeHttpStreamSource chss = new CompositeHttpStreamSource(
                        cfg.getHttpClient(),
                        cfg.getMethod(), null, urls.iterator(),
                        null, requestPayloads.iterator(), auth, cfg.getHeaders());
                streamSource = chss;
            }
            else {
                HttpStreamSource hss = new HttpStreamSource(cfg.getHttpClient(), cfg.getUrl(), cfg.getMethod(), auth,
                        cfg.getRequestPayload(), cfg.getHeaders());
                streamSource = hss;
            }
        }
        else if (srcConfig.getClass() == StringSourceConfig.class) {
            StringSourceConfig cfg = (StringSourceConfig) srcConfig;
            streamSource = new StringStreamSource(cfg.getString()) ;
        }
        else {
            throw new TDException("Could not translate configuration to StreamSource based on class " + srcConfig.getClass());
        }

        return streamSource;
    }

    private static HttpAuth constructHttpAuth(HttpSourceConfig.Auth cfg) {
        if (cfg != null) {
            final HttpAuth.Type type = HttpAuth.Type.valueOf(cfg.getType());
            return new HttpAuth(type, cfg.getUsername(), cfg.getPassword());
        }
        else {
            return HttpAuth.NONE;
        }
    }

    protected static TDStream constructDBStream (String name, DBStreamConfig config, List<String> primaryKey, TupleDiffContext tupleDiffContext) {
        Supplier<DataSource> dsSupplier;
        DataSourceConfig ds = config.getDatasource();
        VariableContext variableContext = tupleDiffContext.getVariableContext();

        if (config.getDatasource().getActualDataSource() != null) {
            dsSupplier = () -> config.getDatasource().getActualDataSource();
        }
        else if (config.getDatasource().getHbdbid() != null) {
            dsSupplier = DataSourceProviders.getDb(ds.getHbdbid());
        }
        else {
            dsSupplier = DataSourceProviders.getDb(ds.getUrl(), ds.getUser(), ds.getPassword(), ds.getProperties());
        }

        // Extract the variable definitions embedded into SQL.
        // TODO Deprecate this stuff. There is already support for reading this from a separate file source.
        String sqlSplit [] = VariableEngine.extractEmbeddedVariableConfigsFromSQLComment(config.getSql());
        String sql = sqlSplit[0], variableDefinition = sqlSplit[1], contentType = sqlSplit[2];
        if (contentType != null) {
            try {
                VariableConfigs varConfigs = MarshalUtils.readValue(new StringReader(variableDefinition), VariableConfigs.class, contentType);
                VariableEngine.mergeVariables(variableContext, varConfigs);
            }
            catch(IOException ie) {
                throw new TDException("Could not read embedded variable configuration in SQL. Error: " + ie.getMessage()
                 + ". \n ContentType: " + contentType + ". VariableDefinition: " + variableDefinition, ie);
            }
        }
        // Replace variables in the sql with the values in the VariableContext
        sql = VariableEngine.doVariableSubstitutions(sql, variableContext);
        //
        List<String> orderBy = (config.getOrderBy() != null && config.getOrderBy().length > 0) ? Arrays.asList(config.getOrderBy()) : primaryKey;
        log.info(String.format ("Order By %s, for SQL: %s", orderBy, sql));
        return new SortedSQLTDStream(name, dsSupplier, sql, orderBy);
    }

    public static ExecutorService defaultExecutor() {
        if (executor == null) {
            executor = new ScheduledThreadPoolExecutor(10, r -> {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            });
        }
        return executor;
    }

    public static Map<String, String> asMap(Param [] params ) {
        if (params == null)
            return null;
        else {
            Map <String, String> m = new HashMap<>();
            for (Param p : params) {
                m.put(p.getName(), p.getValue());
            }
            return m;
        }
    }

}

