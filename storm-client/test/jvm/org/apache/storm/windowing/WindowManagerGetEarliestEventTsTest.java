package org.apache.storm.windowing;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(Parameterized.class)
public final class WindowManagerGetEarliestEventTsTest extends SutWindowManager {
    private final int startEvtNum;
    private final int sumToStartEvtTimeMs;
    private final int endEvtNum;
    private final int sumToEndEvtTimeMs;
    private final boolean expectsIllArgExcp;
    private final int expectedEarliestEvtNum;

    private final static int IGNORE = 0;

    @Parameterized.Parameters
    public static Iterable<Object[]> params() {
        return Arrays.asList(new Object[][]{
                /*{0, -1, 0, -1, true, IGNORE},
                {0, -1, 0, 0, true, IGNORE},
                {0, -1, 0, -2, true, IGNORE},
                {0, 0, 0, 0, false, -1},
                {0, 0, 0, 1, false, -1},
                {0, 0, 0, -1, true, IGNORE},
                {1, 0, 1, 0, false, -1},
                {1, 0, 1, 1, false, -1},*/
                //{1, 0, 1, -1, false, -1},
                {1, -100, 2, 0, false, 1},
                //{1, 0, 1, +100, false, -1},
                {1, 0, 2, 0, false, 2},
                {1, 0, 3, 0, false, 2},
                {2, 0, 7, 0, false, 3},
                {10, -100, 10, +100, false, 10},
                {1, -100, 10, +100, false, 1},
                //{10, 0, 10, +100, false, -1},
                //{1, -200, 1, -100, false, -1},
                //{10, +100, 10, +200, false, -1},
        });
    }

    public WindowManagerGetEarliestEventTsTest(
            int startEvtNum, int sumToStartEvtTimeMs,
            int endEvtNum, int sumToEndEvtTimeMs,
            boolean expectsIllArgExcp, int expectedEarliestEvtNum) {
        this.startEvtNum = startEvtNum;
        this.sumToStartEvtTimeMs = sumToStartEvtTimeMs;
        this.endEvtNum = endEvtNum;
        this.sumToEndEvtTimeMs = sumToEndEvtTimeMs;
        this.expectsIllArgExcp = expectsIllArgExcp;
        this.expectedEarliestEvtNum = expectedEarliestEvtNum;
    }

    @Override
    @Before
    public void setup() {
        setSutDefaultPolicies();
        super.setup();
    }

    @Test
    public void test() {
        long startMs = ms(startEvtNum, sumToStartEvtTimeMs);
        long endMs = ms(endEvtNum, sumToEndEvtTimeMs);

        if(expectsIllArgExcp) {
            assertThrows(IllegalArgumentException.class, () -> sut.getEarliestEventTs(startMs, endMs));
        } else {
            assertEquals(getExpectedTs(), sut.getEarliestEventTs(startMs, endMs));
        }
    }

    private long getExpectedTs() {
        //avoid trying to access the list of evt records
        if(expectedEarliestEvtNum < 1) {
            return expectedEarliestEvtNum;
        }

        return eventRecords.get(expectedEarliestEvtNum - 1).atMs;
    }
}
