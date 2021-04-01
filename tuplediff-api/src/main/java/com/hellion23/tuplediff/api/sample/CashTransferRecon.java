package com.hellion23.tuplediff.api.sample;

import com.hellion23.tuplediff.api.TDCompare;
import com.hellion23.tuplediff.api.TupleDiff;
import com.hellion23.tuplediff.api.compare.CompareEvent;
import com.hellion23.tuplediff.api.config.StreamConfig;
import com.hellion23.tuplediff.api.config.TDConfig;
import com.hellion23.tuplediff.api.handler.CascadingEventHandler;
import com.hellion23.tuplediff.api.handler.CompareEventHandler;
import com.hellion23.tuplediff.api.handler.ConsoleEventHandler;
import com.hellion23.tuplediff.api.model.TDTuple;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a sample skeleton for database based reconciliation code.  The objective of the class is to synchronize
 * the CashTransfers in VPM 11 environment into the VPM 16 environment. The code to execute the insert/update/deletion
 * of these said objects are not in here as this is specific VPM webservice based code
 */
@Slf4j
public class CashTransferRecon {
    public static void main (String args []) {
        CashTransferRecon recon = new CashTransferRecon();
        recon.reconcile();
    }

    protected void reconcile() {
        TDConfig tableDiffConfig = TDConfig.builder()
                .left   (StreamConfig.sql("vpm-int-qa"          ,sql))
                .right  (StreamConfig.sql("vpm-full-production", sql))
                .primaryKey(
                    "TR_DATE", "FROM_ACCT", "FROM_FUND", "TO_ACCT", "TO_FUND", "QTY", "SYCODE", "SYSTRTPCODE")
                .excludeFields(
                    "REMARKS")
                .build();

        CascadingEventHandler handler = new CascadingEventHandler(
                new ConsoleEventHandler(),  // This is to print (some) of the results to log.
                new ReconcileHandler());
        TDCompare comparison = TupleDiff.configure(tableDiffConfig, handler);
        comparison.compare();
    }

    static class ReconcileHandler implements CompareEventHandler {
        @Override
        public void accept(CompareEvent compareEvent) {
            TDTuple vpm16 = compareEvent.getLeft();
            TDTuple vpm11 = compareEvent.getRight();

            switch (compareEvent.getType()) {
                case FOUND_ONLY_LEFT:
                    // TODO DELETE the extra item found in VPM 16.
                    break;
                case FOUND_ONLY_RIGHT:
                    // TODO INSERT the extra item found in VPM 11.
                    break;
                case FOUND_BREAK:
                    // TODO UPDATE the item found in VPM 11.
                    break;
                default:
                    // No-Op FOUND_MATCHED, DATA_LEFT, DATA_RIGHT, ignore
                    break;
            }
        }
    }


    String sql =
            "declare @asof datetime set @asof = '20190225'\n" + // Declare statements. Not part of the real SQL.
            "declare @end_date datetime set @end_date = '20190226'\n" +
            "--SQL_START\n" + // SQL_START comment needed to indicate this is where the real SQL starts
            "select \n" +
            "--t.TrfrId,\n" +
            "convert(varchar(8),cast(t.TrfrDate as date),112) tr_date,\n" +
            "sy.SyCode,\n" +
            "t.SysTrTpCode,\n" +
            "cbac.CBAcDesc from_acct,\n" +
            "fn.FnCode from_fund,\n" +
            "--tt.TrToId,\n" +
            "to_cbac.CBAcDesc to_acct,\n" +
            "to_fn.FnCode to_fund,\n" +
            "t.Qty,\n" +
            "t.XRate,\n" +
            "t.Remarks\n" +
            "from vpm..Transfer t \n" +
            "left join vpm..TransferTo tt on tt.TrfrId = t.TrfrId\n" +
            "left join vpm..sy on sy.syid = t.SyId\n" +
            "left join vpm..fn on fn.FnId = t.FnId\n" +
            "left join vpm..ClrBrkrAcct cbac on cbac.CBAcId = t.CBAcId\n" +
            "left join vpm..Cu on cu.CuId = t.cuid\n" +
            "left join vpm..ClrBrkrAcct to_cbac on to_cbac.CBAcId = tt.CBAcId\n" +
            "left join vpm..fn to_fn on to_fn.FnId= fn.FnId \n" +
            "where t.TrfrDate between @asof and @end_date\n" +
            "--SQL_END"; // SQL_END comment indicates where the sql block concludes.

}
