package com.hellion23.tuplediff.api.listener;

import com.hellion23.tuplediff.api.CompareEvent;
import com.hellion23.tuplediff.api.Config;

import java.util.Collection;

/**
  * Created by Hermann on 9/28/2014.
 */
public interface CompareEventListener <T extends Collection <CompareEvent> > {
    public void handleCompareEvent(CompareEvent compareEvent);
    public T getCompareEvents();
    public void init(Config config);
    public void close();

}
