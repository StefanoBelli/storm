package org.apache.storm.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(Enclosed.class)
public final class WindowedBoltExecutorTest {

    @RunWith(Parameterized.class)
    public static final class PrepareTest extends SutWindowedBoltExecutor {
        @Parameterized.Parameters
        public static Iterable<Object[]> params() {
            return Arrays.asList(new Object[][] {
                    { null, null, null, RuntimeException.class },
                    { null, null, validOutputCollector(), RuntimeException.class },
                    { null, null, invalidOutputCollector(), RuntimeException.class },
                    { null, makeTopologyContext(), null, RuntimeException.class },
                    { null, makeTopologyContext(), validOutputCollector(), RuntimeException.class },
                    { null, makeTopologyContext(), invalidOutputCollector(), RuntimeException.class },
                    //{ null, invalidTopologyContext(), null, RuntimeException.class },
                    //{ null, invalidTopologyContext(), validOutputCollector(), RuntimeException.class },
                    //{ null, invalidTopologyContext(), invalidOutputCollector(), RuntimeException.class },
                    { invalidConfig(), null, null, RuntimeException.class },
                    { invalidConfig(), null, validOutputCollector(), RuntimeException.class },
                    { invalidConfig(), null, invalidOutputCollector(), RuntimeException.class },
                    { invalidConfig(), makeTopologyContext(), null, RuntimeException.class },
                    { invalidConfig(), makeTopologyContext(), validOutputCollector(), IllegalArgumentException.class },
                    { invalidConfig(), makeTopologyContext(), invalidOutputCollector(), IllegalArgumentException.class },
                    //{ invalidConfig(), invalidTopologyContext(), null, RuntimeException.class },
                    //{ invalidConfig(), invalidTopologyContext(), validOutputCollector(), IllegalArgumentException.class },
                    //{ invalidConfig(), invalidTopologyContext(), invalidTopologyContext(), IllegalArgumentException.class },
                    { validConfig(), null, null, RuntimeException.class },
                    { validConfig(), null, validOutputCollector(), RuntimeException.class },
                    { validConfig(), null, invalidOutputCollector(), RuntimeException.class },
                    { validConfig(), makeTopologyContext(), null, RuntimeException.class },
                    { validConfig(), makeTopologyContext(), validOutputCollector(), null },
                    //{ validConfig(), makeTopologyContext(), invalidOutputCollector(), IllegalArgumentException.class },
                    //{ validConfig(), invalidTopologyContext(), null, RuntimeException.class },
                    //{ validConfig(), invalidTopologyContext(), validOutputCollector(), IllegalArgumentException.class },
                    //{ validConfig(), invalidTopologyContext(), invalidOutputCollector(), IllegalArgumentException.class},
                    { TopoConfs.validWinLenCount(), makeTopologyContext(), validOutputCollector(), null },
                    { TopoConfs.invalidWinLenCount(), makeTopologyContext(), validOutputCollector(), IllegalArgumentException.class },
                    { TopoConfs.validWinLenDuration(), makeTopologyContext(), validOutputCollector(), null },
                    { TopoConfs.invalidWinLenDuration(), makeTopologyContext(), validOutputCollector(), IllegalArgumentException.class },
                    { TopoConfs.validBothWinLenAndSlidIntvlDuration(), makeTopologyContext(), validOutputCollector(), null },
                    { TopoConfs.invalidBothWinLenAndSlidIntvlDuration(), makeTopologyContext(), validOutputCollector(), IllegalArgumentException.class },
                    { TopoConfs.invalidBothWinLenAndSlidIntvlCount(), makeTopologyContext(), validOutputCollector(), IllegalArgumentException.class },
            });
        }

        private final Class<Throwable> expectedException;
        private final Map<String, Object> config;
        private final TopologyContext topologyContext;
        private final OutputCollector outputCollector;

        public PrepareTest(
                Map<String, Object> config, TopologyContext topologyContext,
                OutputCollector outputCollector, Class<Throwable> expectedException) {
            this.config = config;
            this.topologyContext = topologyContext;
            this.outputCollector = outputCollector;
            this.expectedException = expectedException;
            buildWindowedBoltExecutor();
        }

        @Test
        public void test() throws Throwable {
            ThrowingRunnable runnable = () -> sut.prepare(config, topologyContext, outputCollector);
            VerificationMode expectedInvokeTimes;

            if(expectedException == null) {
                runnable.run();
                expectedInvokeTimes = Mockito.times(1);
            } else {
                assertThrows(expectedException, runnable);
                expectedInvokeTimes = Mockito.atMostOnce();
            }

            Mockito
                    .verify(bolt, expectedInvokeTimes)
                    .prepare(Mockito.eq(config), Mockito.eq(topologyContext), Mockito.any(OutputCollector.class));
        }
    }

    @RunWith(Parameterized.class)
    public static final class ExecuteTest extends SutWindowedBoltExecutor {
        @Parameterized.Parameters
        public static Iterable<Object[]> params() {
            return Arrays.asList(new Object[][] {
                    //{ null, null, arrayOfArray(), RuntimeException.class, false },
                    { null, makeTuple(123), arrayOfArray(), null, false },
                    { arrayOf(makeTuple(123)), makeTuple(456), arrayOfArray(), null, false },
                    { arrayOf(makeTuple(123), makeTuple(456)), makeTuple(789), arrayOfArray(), null, false },
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789)),
                            makeTuple(912), arrayOfArray(arrayOf(123, 456, 789, 912)), null, false },
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789), makeTuple(912)),
                            makeTuple(345), arrayOfArray(arrayOf(123, 456, 789, 912)), null, false },
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789), makeTuple(912),
                            makeTuple(345)),
                            makeTuple(678), arrayOfArray(arrayOf(123, 456, 789, 912)), null, false },
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789), makeTuple(912),
                            makeTuple(345), makeTuple(678)),
                            makeTuple(891), arrayOfArray(arrayOf(123, 456, 789, 912)), null, false },
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789), makeTuple(912),
                            makeTuple(345), makeTuple(678), makeTuple(891)),
                            makeTuple(234),
                            arrayOfArray(arrayOf(123, 456, 789, 912), arrayOf(345,678,891,234)), null, false },
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789), makeTuple(912),
                            makeTuple(345), makeTuple(678), makeTuple(891), makeTuple(234),
                            makeTuple(567), makeTuple(891), makeTuple(235)),
                            makeTuple(678),
                            arrayOfArray(arrayOf(123, 456, 789, 912),
                                    arrayOf(345,678,891,234), arrayOf(567,891,235,678)), null, false },
                    { null, makeTuple(1499), arrayOfArray(), null, true },
                    { arrayOf(makeTuple(10), makeTuple(20), makeTuple(500), makeTuple(700),
                            makeTuple(1400)), makeTuple(1499), arrayOfArray(), null, true },
                    { arrayOf(makeTuple(10), makeTuple(20), makeTuple(500), makeTuple(700),
                            makeTuple(999), makeTuple(1499)), makeTuple(1500),
                            arrayOfArray(arrayOf(10,20,500,700,999,1499,1500)), null, true },
                    { arrayOf(makeTuple(10), makeTuple(20), makeTuple(500), makeTuple(700),
                            makeTuple(999), makeTuple(1500), makeTuple(1560),
                            makeTuple(1700)), makeTuple(3100),
                            arrayOfArray(arrayOf(10,20,500,700,999,1500), arrayOf(1560, 1700)), null, true },
                    { arrayOf(makeTuple(10), makeTuple(20), makeTuple(500), makeTuple(700),
                            makeTuple(999), makeTuple(1500), makeTuple(1560),
                            makeTuple(1700)), makeTuple(3000),
                            arrayOfArray(arrayOf(10,20,500,700,999,1500), arrayOf(1560, 1700, 3000)), null, true },
            });
        }

        private final long[][] expectedOrderedWindowsOfTuplesUid;
        private final Class<Throwable> expectedException;
        private final Tuple[] preExecuteTuples;
        private final Tuple argTuple;
        private final boolean hasTimestamps;
        private final Map<String,Object> conf;

        public ExecuteTest(Tuple[] preExecuteTuples, Tuple argTuple,
                long[][] expectedOrderedWindowsOfTuplesUid,
                Class<Throwable> expectedException, boolean hasTimestamps) {
            this.preExecuteTuples = preExecuteTuples;
            this.argTuple = argTuple;
            this.expectedOrderedWindowsOfTuplesUid = expectedOrderedWindowsOfTuplesUid;
            this.expectedException = expectedException;
            this.hasTimestamps = hasTimestamps;

            if(hasTimestamps) {
                Mockito
                        .when(bolt.getTimestampExtractor())
                        .thenReturn(TupleFieldTimestampExtractor.of(TUPLE_FIELD));

                conf = TopoConfs.validBothWinLenAndSlidIntvlDuration();
            } else {
                conf = validConfig();
            }

            buildWindowedBoltExecutor();
        }

        @Before
        public void setup() {
            TopologyContext topologyContext = makeTopologyContext();
            OutputCollector outputCollector = makeOutputCollector();

            sut.prepare(conf, topologyContext, outputCollector);

            if(preExecuteTuples != null) {
                for(final Tuple tuple : preExecuteTuples) {
                    sut.execute(tuple);
                }
            }

            Mockito
                    .verify(bolt, Mockito.times(1))
                    .prepare(Mockito.eq(conf), Mockito.eq(topologyContext), Mockito.any(OutputCollector.class));
        }

        @Test
        public void test() throws Throwable {
            ThrowingRunnable runnable = () -> sut.execute(argTuple);

            if(expectedException == null) {
                runnable.run();
            } else {
                assertThrows(expectedException, runnable);
            }

            if(hasTimestamps) {
                sut.waterMarkEventGenerator.run();
            }

            assertEquals(expectedOrderedWindowsOfTuplesUid.length, boltReceivedTupleWindows.size());

            for(int i = 0; i < expectedOrderedWindowsOfTuplesUid.length; ++i) {
                List<Tuple> recvdTuples = boltReceivedTupleWindows.get(i).get();
                List<Long> expectedUids = toList(expectedOrderedWindowsOfTuplesUid[i]);

                assertEquals(expectedUids.size(), recvdTuples.size());

                for(int j = 0; j < expectedUids.size(); ++j) {
                    assertEquals(expectedUids.get(j), recvdTuples.get(j).getValue(0));
                }
            }

            Mockito
                    .verify(bolt, Mockito.times(expectedOrderedWindowsOfTuplesUid.length))
                    .execute(Mockito.any(TupleWindow.class));
        }

        private static long[][] arrayOfArray(long[] ... arr) { return arr; }

        private static Tuple[] arrayOf(Tuple ... t) { return t; }

        private static long[] arrayOf(long ... t) { return t; }

        private static List<Long> toList(long[] arr) {
            List<Long> l = new ArrayList<>();

            for(final long e : arr) {
                l.add(e);
            }

            return l;
        }
    }

    /*
    public static final class ExecuteWithTimestampTest extends SutWindowedBoltExecutor {

        public ExecuteWithTimestampTest() {
            super(true);
        }

        @Test
        public void testExecuteTuplesWithTimestamp() {
            sut.prepare(
                    TopoConfs.validBothWinLenAndSlidIntvlDuration(),
                    makeTopologyContext(),
                    makeOutputCollector());

            long[] tss = { 10, 20, 500, 700, 999, 1500, 1560, 1700 };
            for(final long ts : tss) {
                sut.execute(makeTuple(ts));
            }

            sut.execute(makeTuple(3000));

            sut.waterMarkEventGenerator.run();

            long[][] expectedWindows = { { 10, 20, 500, 700, 999, 1500 }, { 1560, 1700, 3000 } };

        }
    }
     */
}
