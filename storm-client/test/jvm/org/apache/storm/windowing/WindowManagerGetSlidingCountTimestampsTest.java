package org.apache.storm.windowing;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(Parameterized.class)
public final class WindowManagerGetSlidingCountTimestampsTest extends SutWindowManager {
    private final int startEvtNum;
    private final int sumToStartEvtTimeMs;
    private final int endEvtNum;
    private final int sumToEndEvtTimeMs;
    private final int slidingCount;
    private final boolean expectsIllArgExcp;
    private final int[] expectedNumEvtTss;

    @Parameterized.Parameters
    public static Iterable<Object[]> params() {
        return Arrays.asList(new Object[][]{
                /*{0, -1, 0, -1, 0, true, null},
                {0, -1, 0, 0, 0, true, null},
                {0, -1, 0, -2, 0, true, null},
                {0, -1, 0, -1, 1, true, null},
                {0, -1, 0, 0, 1, true, null},
                {0, -1, 0, -2, 1, true, null},
                {0, 0, 0, 0, 0, true, null},
                {0, 0, 0, 1, 0, true, null},
                {0, 0, 0, -1, 0, true, null},
                {0, 0, 0, 0, 1, false, null},
                {0, 0, 0, 1, 1, false, null},
                {0, 0, 0, -1, 1, true, null},
                {1, 0, 1, 0, 0, true, null},
                {1, 0, 1, 1, 0, true, null},
                {1, 0, 1, -1, 0, true, null},
                {1, 0, 1, 0, 1, false, new int[]{1}},*/
                {1, 0, 1, 1, 1, false, new int[]{}},
                {1, 0, 1, -1, 1, false, new int[]{}},
                {1, -100, 1, -50, 1, false, new int[]{}},
                {1, -100, 1, +100, 1, false, new int[]{1}},
                {1, 0, 3, 0, 1, false, new int[]{2,3}},
                {5, -100, 8, +50, 1, false, new int[]{5,6,7,8}},
                {8, 0, 10, +100, 1, false, new int[]{9,10}},
                {10, +100, 10, +200, 1, false, new int[]{}},
                {1, -100, 1, -50, 2, false, new int[]{}},
                {1, -100, 1, +100, 2, false, new int[]{}},
                {1, 0, 3, 0, 2, false, new int[]{3}},
                {5, -100, 8, +50, 2, false, new int[]{6,8}},
                {8, 0, 10, +100, 2, false, new int[]{10}},
                {10, +100, 10, +200, 2, false, new int[]{}}
        });
    }

    public WindowManagerGetSlidingCountTimestampsTest(
            int startEvtNum, int sumToStartEvtTimeMs,
            int endEvtNum, int sumToEndEvtTimeMs,
            int slidingCount, boolean expectsIllArgExcp,
            int[] expectedNumEvtTss) {
        this.slidingCount = slidingCount;
        this.endEvtNum = endEvtNum;
        this.startEvtNum = startEvtNum;
        this.sumToStartEvtTimeMs = sumToStartEvtTimeMs;
        this.sumToEndEvtTimeMs = sumToEndEvtTimeMs;
        this.expectsIllArgExcp = expectsIllArgExcp;
        this.expectedNumEvtTss = expectedNumEvtTss;
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
            assertThrows(IllegalArgumentException.class,
                    () -> sut.getSlidingCountTimestamps(startMs, endMs, slidingCount));
        } else {
            List<Long> tssThatMatchCriteria = sut.getSlidingCountTimestamps(startMs, endMs, slidingCount);
            List<Long> expectedTss = getExpectedTssToBePresent();

            assertEquals(expectedTss, tssThatMatchCriteria);
        }
    }

    private List<Long> getExpectedTssToBePresent() {
        List<Long> expectedTss = new ArrayList<>();
        for(final int evtNum : expectedNumEvtTss) {
            for(int i = 1; i <= eventRecords.size(); ++i) {
                if(evtNum == i) {
                    expectedTss.add(eventRecords.get(i - 1).atMs);
                    break;
                }
            }
        }

        return expectedTss;
    }
}
