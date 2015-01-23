package com.hellion23.tuplediff.api.listener;

import com.hellion23.tuplediff.api.CompareEvent;
import com.hellion23.tuplediff.api.Config;

import java.util.LinkedList;
import java.util.List;

/**
* @author: Hermann Leung
* Date: 1/23/2015
*/
public class ListCompareEventListener implements CompareEventListener <List<CompareEvent>> {
    List<CompareEvent> compareEvents = new LinkedList<CompareEvent>();

    public ListCompareEventListener () {
        this(null);
    }

    public ListCompareEventListener (List<CompareEvent> backingList) {
        if (backingList == null)
            this.compareEvents = new LinkedList<CompareEvent>();
        else
            this.compareEvents = backingList;
    }

    @Override
    public void handleCompareEvent(CompareEvent compareEvent) {
        compareEvents.add(compareEvent);
    }

    @Override
    public List<CompareEvent> getCompareEvents() {
        return compareEvents;
    }

    @Override
    public void init(Config config) {}

    @Override
    public void close() {}
}
