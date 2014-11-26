package com.hellion23.tuplediff.api.monitor;

/**
 * A Monitorable reports it's status to a Monitor (via the Monitor's handleEvent method) and allows itself to be
 * stopped by the monitor.
 *
 * Implementers of Monitorable should report it's STATE transition changes, and if reporting a TERMINATED state, should
 * also include a STOP_REASON. If the STOP_REASON is FAILED, the Exception should also be included. If a STOP_REASON
 * is not provided, it is assumed the TERMINATED reason is COMPLETED. The Monitor class would record when or if these
 * STATE transitions happened.
 *
 * A second raison d'etre for this Class is that implementers of Monitorable should have methods that throw
 * Monitorable.EXCEPTION in lieu of standard Exceptions, which in addition to having the base cause also includes the
 * Named source (this is particularly important when a Monitor is monitoring multiple Monitorable's w/ the same class,
 * such as TupleStream).
 *
 * Finally, implementations of Monitor can also shut down all Monitorable(s) if it detects that any of the supervised
 * Monitorable(s) is behaving badly by invoking its stop() method. A
 *
 * An example of the life cycle of a Monitorable object:
 *
 * 1) Class initializes. It's initial state is NEW. It reports to monitor to handleEvent the STARTING event.
 *         (monitor.handleEvent(this, STATE.STARTING);
 * 2) Class initialized. It should now report that it is in the STARTED status.
 * 3) doSomething() is invoked, so the class reports that it is in the RUNNING state. Additional details can be added
 *      to the reporting (the params for handleEvent are variable).
 * 4) doSomething() has completed and it's resources have cleaned up. It now reports the TERMINATED state along with
 *      COMPLETED as the STOP_REASON.
 * 5) Stats collected by the monitor (including start time/end time and other significant events can be
 *
 * In the event that the initialization() or doSomething() methods encounter an Exception, here is the suggested order
 *  of events:
 *
 *  1) These methods should have the entire method body wrapped in a try/catch block.
 *  2) After catching the error, the class should clean up it's own resources gracefully.
 *  3) The class should report to the monitor that it has failed (call handleEvent with the STATE.TERMINATED,
 *      STOP_REASON.FAILED and the Exception that caused it to fail.
 *  4) Re-throw the Exception as a wrapped MonitorableException that identifies which instance of the class has
 *      actually caused the problem.
 *  5) It is up to the the class that catches this re-thrown exception on how to proceed. It can ignore the Exception
 *      and proceed or it can inform the monitor to shut down all supervised components.
 *
 *
 * Created by Hermann on 9/28/2014.
 */
public interface Monitorable {
    public static enum STATE {
        NEW, STARTING, STARTED, RUNNING, STOPPING, TERMINATED
    }

    public static enum STOP_REASON {
        CANCEL, COMPLETED, FAILED, MONITOR
    }

    public static class EXCEPTION extends Exception {
        Throwable cause;
        Nameable source;
    }

    public void setMonitor(Monitor monitor);

    /**
     * Invocations to stop() should be idempotent and not throw any exceptions.
     */
    public void stop ();
}
