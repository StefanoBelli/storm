package org.apache.storm.windowing;

import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SutWindowManager {
    protected static class EventRecord {
        protected final Integer evt;
        protected final long atMs;

        private EventRecord(Integer evt, long atMs) {
            this.evt = evt;
            this.atMs = atMs;
        }
    }

    protected final WindowManager<Integer> sut;

    protected final List<EventRecord> eventRecords = new ArrayList<>();

    protected SutWindowManager(WindowLifecycleListener<Integer> wll) {
        sut = new WindowManager<>(wll);
    }

    protected SutWindowManager() {
        this(e -> {});
    }

    @Before
    public void setup() {
        long baseSimTimeMs = System.currentTimeMillis();
        for(int i = 0; i < 10; ++i) {
            EventRecord regEvt = new EventRecord(
                    i + rand(), baseSimTimeMs + (i * 200));

            eventRecords.add(regEvt);
            sut.add(regEvt.evt, regEvt.atMs);
        }
    }

    protected final long ms(int numEvt, int sumMs) {
        if(numEvt == 0) {
            return sumMs;
        }

        return eventRecords.get(numEvt - 1).atMs + sumMs;
    }

    protected final void setSutDefaultPolicies() {
        EvictionPolicy<Integer, ?> evp = new CountEvictionPolicy<>(Integer.MAX_VALUE);
        TriggerPolicy<Integer, ?> trp = new CountTriggerPolicy<>(Integer.MAX_VALUE, () -> true, evp);
        sut.setEvictionPolicy(evp);
        sut.setTriggerPolicy(trp);
    }

    private static int rand() {
        return new Random().nextInt();
    }
}
