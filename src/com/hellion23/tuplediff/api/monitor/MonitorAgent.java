package com.hellion23.tuplediff.api.monitor;

import java.lang.instrument.Instrumentation;

/**
 * Created by Hermann on 10/3/2014.
 */
public class MonitorAgent {
    private static volatile Instrumentation globalInstr;
    public static void premain(String args, Instrumentation inst) {
        globalInstr = inst;
    }
    public static long getObjectSize(Object obj) {
        if (globalInstr == null)
            throw new IllegalStateException("Agent not initted");
        return globalInstr.getObjectSize(obj);
    }
}
