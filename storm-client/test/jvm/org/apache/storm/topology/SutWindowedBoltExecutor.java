package org.apache.storm.topology;

import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.Config;
import org.mockito.Mockito;

import java.util.*;

public class SutWindowedBoltExecutor {
    protected final BaseWindowedBolt bolt;
    protected WindowedBoltExecutor sut;
    protected final List<TupleWindow> boltReceivedTupleWindows;

    protected static final String TUPLE_FIELD = "uniqueId";

    protected SutWindowedBoltExecutor() {
        boltReceivedTupleWindows = new ArrayList<>();
        bolt = Mockito.mock();
        Mockito
                .doAnswer(args ->
                        boltReceivedTupleWindows.add(args.getArgument(0)))
                .when(bolt).execute(Mockito.any(TupleWindow.class));
    }

    protected void buildWindowedBoltExecutor() {
        sut = new WindowedBoltExecutor(bolt);
    }

    /*
     * Config
     */
    protected static Map<String, Object> validConfig() {
        Map<String, Object> conf = new HashMap<>();

        conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 4);
        conf.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, 4);

        return conf;
    }

    protected static Map<String, Object> invalidConfig() {
        return new HashMap<>();
    }

    /*
     * TopologyContext
     */
    protected static TopologyContext invalidTopologyContext() {
        return new TopologyContext(null,null,null,
                null,null,null,
                null,null,null,null,null,null,
                null,null,null,null,
                null,null);
    }

    protected static TopologyContext makeTopologyContext() {
        return Mockito.mock();
    }

    /*
     * OutputCollector
     */
    protected static OutputCollector makeOutputCollector() {
        return Mockito.mock();
    }

    protected static OutputCollector validOutputCollector() {
        return new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                return new ArrayList<>();
            }

            @Override
            public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {

            }

            @Override
            public void ack(Tuple input) {

            }

            @Override
            public void fail(Tuple input) {

            }

            @Override
            public void resetTimeout(Tuple input) {

            }

            @Override
            public void flush() {

            }

            @Override
            public void reportError(Throwable error) {

            }
        });
    }

    protected static OutputCollector invalidOutputCollector() {
        return new OutputCollector(null);
    }

    /*
     * Tuple
     */
    protected static Tuple makeTuple(long value) {
        return makeTuple(new Fields(TUPLE_FIELD), new Values(value));
    }

    // internal

    private static Tuple makeTuple(Fields fields, Values values) {
        return new TupleImpl(makeContext(fields), values, null, 0, null);
    }
    
    private static GeneralTopologyContext makeContext(final Fields fields) {
        return new GeneralTopologyContext(null, new Config(), new HashMap<>(), null, null, null) {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return fields;
            }
        };
    }

    protected static class TopoConfs {
        public static Map<String,Object> validWinLenCount() {
            Map<String, Object> conf = new HashMap<>();

            conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 10);
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 11);

            return conf;
        }

        public static Map<String,Object> invalidWinLenCount() {
            Map<String, Object> conf = new HashMap<>();

            conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 10);
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 9);

            return conf;
        }

        public static Map<String,Object> validWinLenDuration() {
            Map<String, Object> conf = new HashMap<>();

            conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 900);
            conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 1);

            return conf;
        }

        public static Map<String,Object> invalidWinLenDuration() {
            Map<String, Object> conf = new HashMap<>();

            conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 1500);

            return conf;
        }

        public static Map<String,Object> validBothWinLenAndSlidIntvlDuration() {
            Map<String, Object> conf = new HashMap<>();

            conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 1500);
            conf.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, 1500);
            conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 4);

            return conf;
        }

        public static Map<String,Object> invalidBothWinLenAndSlidIntvlDuration() {
            Map<String, Object> conf = new HashMap<>();

            conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 1000);
            conf.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, 1500);
            conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 2);

            return conf;
        }

        public static Map<String,Object> invalidBothWinLenAndSlidIntvlCount() {
            Map<String, Object> conf = new HashMap<>();

            conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 10);
            conf.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, 15);
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 20);

            return conf;
        }
    }
}
