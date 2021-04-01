package com.hellion23.tuplediff.api;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.hellion23.tuplediff.api.config.MarshalUtils;
import com.hellion23.tuplediff.api.config.TDConfig;
import com.hellion23.tuplediff.api.config.VariableConfigs;
import com.hellion23.tuplediff.api.handler.*;
import com.hellion23.tuplediff.api.variable.VariableContext;
import com.hellion23.tuplediff.api.variable.VariableEngine;

import java.io.File;
import java.util.LinkedList;

/**
 * Created by hleung on 7/31/2017.
 */
public class TDCompareCLI  {

    @Parameter(names={"--config", "-c"}, description = "TupleDiff Config File Path", required = true)
    String configPath;

    @Parameter(names={"--csv", "-f"}, description = "CSV File Path")
    String csvFilePath;

    @Parameter(names={"--console_rows", "-r"}, description = "Max # of break rows to print to console. Less than 0 = don't print anything. 0 = print everything. Default is 50 rows. ")
    int maxConsoleRows = 50;

    @Parameter(names={"--variables", "-v"}, description = "Variable config file path. This config file used to define embedded ${VARIABLES} inside the sql of the TupleDiff Config File")
    String variablePath;

    @Parameter(names = {"--help", "-?"}, help = true)
    private boolean help;

    public static void main (String args[]) throws Exception {
        TDCompareCLI cli = new TDCompareCLI();
        JCommander.newBuilder()
                .addObject(cli)
                .build()
                .parse(args);
        cli.run();
    }

    public void run() throws Exception {
        TDConfig config = MarshalUtils.readValue(new File(configPath), TDConfig.class);
        LinkedList<CompareEventHandler> handlers = new LinkedList<>();
        if (maxConsoleRows>0) {
            handlers.add(new ConsoleEventHandler(maxConsoleRows == 0 ? Long.MAX_VALUE : maxConsoleRows));
        }
        if (csvFilePath!=null) {
            handlers.add(new CSVEventHandler(csvFilePath));
        }
        handlers.add(new StatsEventHandler(true));
        TupleDiffContext context = new TupleDiffContext();
        context.setEventHandler(new CascadingEventHandler(handlers));

        if (variablePath != null) {
            VariableConfigs variableConfigs = MarshalUtils.readValue(new File(variablePath), VariableConfigs.class);
            VariableContext variableContext = VariableEngine.createVariableContext(variableConfigs);
            context.setVariableContext(variableContext);
        }
        TDCompare comparison = TupleDiff.configure(config, context);
        comparison.compare();
    }
}
