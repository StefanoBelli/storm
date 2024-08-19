package org.apache.storm.windowing;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class WindowManagerAddTest extends SutWindowManager {

    private static final int EVICTION_COUNT = 5;
    private static final int TRIGGER_COUNT = 10;

    private final int[] expectedNewEvents;
    private final int[] expectedExpiredEvents;

    @Parameterized.Parameters
    public static Iterable<Object[]> params() {
        return Arrays.asList(new Object[][] {
                {0, arrayOf(), arrayOf()},
                {5, arrayOf(), arrayOf()},
                {9, range(5), arrayOf(6,7,8,9,10)},
                {10, arrayOf(), arrayOf()},
                {15, arrayOf(), arrayOf()},
                {19, range(6,15), arrayOf(16,17,18,19,20)},
                {20, arrayOf(), arrayOf()},
                {25, arrayOf(), arrayOf()},
                {29, range(16,25), arrayOf(26,27,28,29,30)}
        });
    }

    public WindowManagerAddTest(int numOfAdds, int[] expectedExpiredEvents, int[] expectedNewEvents) {
        super(Mockito.mock());
        this.numOfPreAdds = numOfAdds;
        this.expectedExpiredEvents = expectedExpiredEvents;
        this.expectedNewEvents = expectedNewEvents;
    }

    @Before
    @Override
    public void setup() {
        EvictionPolicy<Integer,?> evictionPolicy = new CountEvictionPolicy<>(EVICTION_COUNT);
        TriggerPolicy<Integer,?> triggerPolicy = new CountTriggerPolicy<>(TRIGGER_COUNT, this.sut, evictionPolicy);
        sut.setEvictionPolicy(evictionPolicy);
        sut.setTriggerPolicy(triggerPolicy);
        triggerPolicy.start();
        super.setup();
    }

    @Test
    public void test() {
        ArgumentCaptor<List<Integer>> expiredByExpiryCaptor = ArgumentCaptor.captor();
        ArgumentCaptor<List<Integer>> expiredByActivationCaptor = ArgumentCaptor.captor();
        ArgumentCaptor<List<Integer>> newEventsByActivationCaptor = ArgumentCaptor.captor();
        ArgumentCaptor<List<Integer>> eventsByActivationCaptor = ArgumentCaptor.captor();

        Integer evt = rand();
        long evtMs = System.currentTimeMillis();
        eventRecords.add(new EventRecord(evt, evtMs));
        sut.add(evt, evtMs);

        int wantedNumOfInvokes = Math.floorDiv(numOfPreAdds + 1, TRIGGER_COUNT);

        Mockito.verify(lifecycleListener, Mockito.times(wantedNumOfInvokes)).onExpiry(
                expiredByExpiryCaptor.capture());
        Mockito.verify(lifecycleListener, Mockito.times(wantedNumOfInvokes)).onActivation(
                eventsByActivationCaptor.capture(), newEventsByActivationCaptor.capture(),
                expiredByActivationCaptor.capture(), Mockito.anyLong());

        if((numOfPreAdds + 1) % TRIGGER_COUNT == 0) {
            List<Integer> expectedExpiredEvts = toListOfEvents(expectedExpiredEvents);
            List<Integer> expectedNewEvts = toListOfEvents(expectedNewEvents);

            assertEquals(expectedExpiredEvts, expiredByExpiryCaptor.getValue());
            assertEquals(expectedExpiredEvts, expiredByActivationCaptor.getValue());
            assertEquals(expectedNewEvts, newEventsByActivationCaptor.getValue());
            assertEquals(expectedNewEvts, newEventsByActivationCaptor.getValue());
        }
    }

    private List<Integer> toListOfEvents(int[] evtNums) {
        List<Integer> evts = new ArrayList<>();
        for(final int evtNum : evtNums) {
            evts.add(eventRecords.get(evtNum - 1).evt);
        }

        return evts;
    }

    private static int[] arrayOf(int ... a) {
        return a;
    }

    private static int[] range(int l, int r) {
        int[] arr = new int[r - l + 1];
        for(int i = 0; i < arr.length; ++i) {
            arr[i] = l + i;
        }

        return arr;
    }

    private static int[] range(int r) {
        return range(1, r);
    }
}
