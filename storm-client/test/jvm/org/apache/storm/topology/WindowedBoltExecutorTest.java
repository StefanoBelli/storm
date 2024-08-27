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

import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

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
                    //{ validConfig(), invalidTopologyContext(), invalidOutputCollector(), IllegalArgumentException.class}
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
                    //{ null, null, arrayOfArray(), RuntimeException.class },
                    { null, makeTuple(123), arrayOfArray(), null },
                    { arrayOf(makeTuple(123)), makeTuple(456), arrayOfArray(), null },
                    { arrayOf(makeTuple(123), makeTuple(456)), makeTuple(789), arrayOfArray(), null },
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789)),
                            makeTuple(912), arrayOfArray(arrayOf(123, 456, 789, 912)), null},
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789), makeTuple(912)),
                            makeTuple(345), arrayOfArray(arrayOf(123, 456, 789, 912)), null},
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789), makeTuple(912),
                            makeTuple(345)),
                            makeTuple(678), arrayOfArray(arrayOf(123, 456, 789, 912)), null},
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789), makeTuple(912),
                            makeTuple(345), makeTuple(678)),
                            makeTuple(891), arrayOfArray(arrayOf(123, 456, 789, 912)), null},
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789), makeTuple(912),
                            makeTuple(345), makeTuple(678), makeTuple(891)),
                            makeTuple(234),
                            arrayOfArray(arrayOf(123, 456, 789, 912), arrayOf(345,678,891,234)), null},
                    { arrayOf(makeTuple(123), makeTuple(456), makeTuple(789), makeTuple(912),
                            makeTuple(345), makeTuple(678), makeTuple(891), makeTuple(234),
                            makeTuple(567), makeTuple(891), makeTuple(235)),
                            makeTuple(678),
                            arrayOfArray(arrayOf(123, 456, 789, 912), arrayOf(345,678,891,234), arrayOf(567,891,235,678)), null},
            });
        }

        private final long[][] expectedOrderedWindowsOfTuplesUid;
        private final Class<Throwable> expectedException;
        private final Tuple[] preExecuteTuples;
        private final Tuple argTuple;

        public ExecuteTest(Tuple[] preExecuteTuples, Tuple argTuple,
                long[][] expectedOrderedWindowsOfTuplesUid,
                Class<Throwable> expectedException) {

            this.preExecuteTuples = preExecuteTuples;
            this.argTuple = argTuple;
            this.expectedOrderedWindowsOfTuplesUid = expectedOrderedWindowsOfTuplesUid;
            this.expectedException = expectedException;
        }

        @Before
        public void setup() {
            Map<String, Object> config = validConfig();
            TopologyContext topologyContext = makeTopologyContext();
            OutputCollector outputCollector = makeOutputCollector();

            sut.prepare(config, topologyContext, outputCollector);

            if(preExecuteTuples != null) {
                for(final Tuple tuple : preExecuteTuples) {
                    sut.execute(tuple);
                }
            }

            Mockito
                    .verify(bolt, Mockito.times(1))
                    .prepare(Mockito.eq(config), Mockito.eq(topologyContext), Mockito.any(OutputCollector.class));
        }

        @Test
        public void test() throws Throwable {
            ThrowingRunnable runnable = () -> sut.execute(argTuple);

            if(expectedException == null) {
                runnable.run();
            } else {
                assertThrows(expectedException, runnable);
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
}
