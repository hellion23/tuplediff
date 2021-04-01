package com.hellion23.tuplediff.api;

import com.hellion23.tuplediff.api.compare.ThresholdNumberComparator;
import com.hellion23.tuplediff.api.config.*;
import com.hellion23.tuplediff.api.president.President;
import com.hellion23.tuplediff.api.president.PresidentTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Run TDComparisons and validate.
 * .
 */
@Slf4j
public class ComparisonTest implements PresidentTest {

    String configTypes [] = { "CSV_FILE","CSV_CAMEL_CASE_FILE", "JSON_FILE", "JSON_HTTP", "JSON_HTTP_COMPOSITE", "DB", "BEAN" };

    StreamConfig leftConfigs [] = new StreamConfig[] {
        leftPresidentCSVStreamConfig,
        leftPresidentCamelCaseCSVStreamConfig,
        leftPresidentJSONFileStreamConfig,
        leftPresidentJSONHttpStreamConfig,
        leftPresidentCompositeJSONStreamConfig,
        leftPresidentDBStreamConfig,
        (StreamConfigSupplier) () -> presidentBeansConfig(true),
    };

    StreamConfig rightConfigs [] =  new StreamConfig[] {
        rightPresidentCSVStreamConfig,
        rightPresidentCamelCaseCSVStreamConfig,
        rightPresidentJSONFileStreamConfig,
        rightPresidentJSONHttpStreamConfig,
        rightPresidentCompositeJSONStreamConfig,
        rightPresidentDBStreamConfig,
        (StreamConfigSupplier) () -> presidentBeansConfig(false),
    };

    @Test
    public void testPermutationForCamelCase () throws Exception{
        int camelCaseIndex = 1;
        testPermutationsFor(camelCaseIndex);
    }

    /**
     * Test all permutations of different streams types against each other and validate that they return results as
     * expected. All leftConfigs are identical to each other and all RightConfigs are identical to one another, so
     * we should not expect to see the comparison itself result in any differences when the streams are compared
     * paired by permuting all
     * @throws Exception
     */
    @Test
    public void testAllPermutations() throws Exception {
        for (int i=0; i<leftConfigs.length; i++) {
            for (int j=0; j<rightConfigs.length; j++) {
                testPermutationFor(i, j);
            }
        }
    }

    protected void testPermutationFor (int leftIndex, int rightIndex) throws Exception {
        String name = configTypes[leftIndex] + " VS " + configTypes[rightIndex];
        log.info("RUNNING COMPARISON PERMUTATION " + name);
        StreamConfig left = resolveStreamConfig (leftConfigs[leftIndex]);
        StreamConfig right = resolveStreamConfig (rightConfigs[rightIndex]);
        TDConfig config = presidentsBuilder(left, right).name(name).build();
        compareAndValidate(config, presidentsValidator);
    }

    protected StreamConfig resolveStreamConfig (StreamConfig streamConfig) {
        if (streamConfig instanceof StreamConfigSupplier) {
            return ((StreamConfigSupplier)streamConfig).get();
        }
        else {
            return streamConfig;
        }
    }

    protected void testPermutationsFor (int i) throws Exception {
        for (int j=0; j<rightConfigs.length; j++) {
            testPermutationFor(i, j);
        }
    }


    interface StreamConfigSupplier extends StreamConfig, Supplier<StreamConfig> {}

    /**
     * Validate that FieldComparator & configurations work.
     *
     * @throws IOException
     */
    @Test
    public void testFieldComparators () throws IOException {
        //Basic field comparator. use Threshold comparator. This will implicitly test whether a csv string value will
        // be automatically converted to a Number.
        ComparatorConfig dobThresholdComp = ComparatorConfig.builder()
                .names("DOB").comparator(new ThresholdNumberComparator(500))
                .buildConfig();

        TDConfig config = presidentsBuilder(leftPresidentCSVStreamConfig, rightPresidentCamelCaseCSVStreamConfig)
                .fieldComparators(dobThresholdComp)
                .build();
        compareAndValidate(config, "presidents_field_comp_validator.json");

        // More realistic date comparator: use threshold of president_no = 2 (i.e. if comparison within a threshold of
        // 2, then this is not a break.
        ComparatorConfig presidentNoNumberThreshold = ComparatorConfig.builder()
                .names("PRESIDENT_NO").comparator(new ThresholdNumberComparator(2))
                .buildConfig();

        VariableConfig[] cfgs = new VariableConfig[] {VariableConfig.keyValue("RIGHT_IDS", Arrays.asList(1, 2, 3))};
        VariableConfigs varConfigs = new VariableConfigs(cfgs);

        List<President> leftPres = Arrays.asList(new President []{
            new President(1, "JIMMY", "CARTER", 1924, 40), // NO BREAK, off by 1
            new President(2, "BARRACK", "OBAMA",  1961, 44), // EQUALS
            new President(3, "GEORGE W.", "BUSH", 1946, 46) // BREAK, off by 3!
        });

        TDConfig config2 = presidentsBuilder(
                StreamConfig.bean().beanClass(President.class).iterator(leftPres.iterator()).build(),
                rightPresidentCompositeJSONStreamConfig)
                .fieldComparators(presidentNoNumberThreshold)
                .build();

        log.info("Configuration: " + MarshalUtils.toJSON(config));
        // Validate only one break (
        CompareEventValidator cev = new CompareEventValidator();
        cev.validateBreak(Arrays.asList("PRESIDENT_NO"), 3 );

        compareAndValidate(config2, varConfigs, cev);
    }

    protected TDConfig.Builder presidentsBuilder(StreamConfig leftCfg, StreamConfig rightCfg) {
        return TDConfig.builder()
                .primaryKey(presidentPrimaryKey)
                .left(leftCfg)
                .right(rightCfg)
                .excludeFields("comments");
    }

}
