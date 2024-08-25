package org.apache.storm.topology;

import org.apache.storm.task.GeneralTopologyContext;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SutWindowedBoltExecutor {
    protected final BaseWindowedBolt bolt;
    protected final WindowedBoltExecutor sut;
    protected final List<TupleWindow> boltReceivedTupleWindows;

    private static final String TUPLE_FIELD = "uniqueId";

    protected SutWindowedBoltExecutor() {
        boltReceivedTupleWindows = new ArrayList<>();
        bolt = Mockito.mock();
        Mockito
                .doAnswer(args ->
                        boltReceivedTupleWindows.add(args.getArgument(0)))
                .when(bolt).execute(Mockito.any(TupleWindow.class));
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
    protected static TopologyContext makeTopologyContext() {
        return Mockito.mock();
    }

    /*
     * OutputCollector
     */
    protected static OutputCollector makeOutputCollector() {
        return Mockito.mock();
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
}
