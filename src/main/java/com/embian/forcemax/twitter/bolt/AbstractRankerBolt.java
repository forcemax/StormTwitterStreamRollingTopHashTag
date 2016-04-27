package com.embian.forcemax.twitter.bolt;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.embian.forcemax.twitter.util.Rankings;

/**
 * This abstract bolt provides the basic behavior of bolts that rank objects
 * according to their count.
 * <p/>
 * It uses a template method design pattern for
 * {@link AbstractRankerBolt#execute(Tuple, BasicOutputCollector)} to allow
 * actual bolt implementations to specify how incoming tuples are processed,
 * i.e. how the objects embedded within those tuples are retrieved and counted.
 */
public abstract class AbstractRankerBolt extends BaseRichBolt {

	private static final long serialVersionUID = 4931640198501530202L;
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
	private static final int DEFAULT_COUNT = 10;

	private final int emitFrequencyInSeconds;
	private final int count;
	private final Rankings rankings;
	private OutputCollector collector;

	public AbstractRankerBolt() {
		this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}

	public AbstractRankerBolt(int topN) {
		this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}

	public AbstractRankerBolt(int topN, int emitFrequencyInSeconds) {
		if (topN < 1) {
			throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
		}
		if (emitFrequencyInSeconds < 1) {
			throw new IllegalArgumentException(
					"The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
		}
		count = topN;
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		rankings = new Rankings(count);
	}

	protected Rankings getRankings() {
		return rankings;
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * This method functions as a template method (design pattern).
	 */
	public final void execute(Tuple input) {
		if (isTickTuple(input)) {
			getLogger().debug("Received tick tuple, triggering emit of current rankings");
			emitRankings(collector);
		} else {
			getLogger().debug("tuple : {}", input);
			updateRankingsWithTuple(input);
		}
		collector.ack(input);
	}

	abstract void updateRankingsWithTuple(Tuple tuple);

	private void emitRankings(OutputCollector collector) {
		collector.emit(new Values(rankings.copy()));
		getLogger().debug("Rankings: " + rankings);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rankings"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
		return conf;
	}

	protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

	abstract Logger getLogger();
}
