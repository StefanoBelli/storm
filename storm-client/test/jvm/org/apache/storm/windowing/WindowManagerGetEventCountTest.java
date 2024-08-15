package org.apache.storm.windowing;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(Parameterized.class)
public final class WindowManagerGetEventCountTest extends SutWindowManager {

    private final int numOfEvt;
    private final int sumToEvtTs;
    private final boolean expectsIobExcp;
    private final int expectsRetVal;

    @Parameterized.Parameters
    public static Iterable<Object[]> params() {
        return Arrays.asList(new Object[][] {
                {0, -1, true, -1},
                {0, 0, true, -1},
                {1, -100, false, 0},
                {1, 0, false, 1},
                {5, 0, false, 5},
                {10, 0, false, 10},
                {10, +100, false, 10}
        });
    }

    public WindowManagerGetEventCountTest(
            int numOfEvt, int sumToEvtTs, boolean expectsIobExcp, int expectsRetVal) {
        this.numOfEvt = numOfEvt;
        this.sumToEvtTs = sumToEvtTs;
        this.expectsIobExcp = expectsIobExcp;
        this.expectsRetVal = expectsRetVal;
    }

    @Override
    @Before
    public void setup() {
        EvictionPolicy<Integer, ?> evp = new CountEvictionPolicy<>(Integer.MAX_VALUE);
        TriggerPolicy<Integer, ?> trp = new CountTriggerPolicy<>(Integer.MAX_VALUE, () -> true, evp);
        sut.setEvictionPolicy(evp);
        sut.setTriggerPolicy(trp);
        super.setup();
    }

    @Test
    public void test() {
        if(expectsIobExcp) {
            assertThrows(IndexOutOfBoundsException.class, () -> sut.getEventCount(ms(numOfEvt, sumToEvtTs)));
        } else {
            assertEquals(expectsRetVal, sut.getEventCount(ms(numOfEvt, sumToEvtTs)));
        }
    }
}
