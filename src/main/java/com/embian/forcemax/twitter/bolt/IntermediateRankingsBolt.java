package com.embian.forcemax.twitter.bolt;

import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.embian.forcemax.twitter.util.Rankable;
import com.embian.forcemax.twitter.util.RankableObjectWithFields;

/**
 * This bolt ranks incoming objects by their count.
 * <p/>
 * It assumes the input tuples to adhere to the following format: (object,
 * object_count, additionalField1, additionalField2, ..., additionalFieldN).
 */
public final class IntermediateRankingsBolt extends AbstractRankerBolt {

	private static final long serialVersionUID = -1369800530256637409L;
	private static final Logger LOGGER = LoggerFactory.getLogger(IntermediateRankingsBolt.class);

	public IntermediateRankingsBolt() {
		super();
	}

	public IntermediateRankingsBolt(int topN) {
		super(topN);
	}

	public IntermediateRankingsBolt(int topN, int emitFrequencyInSeconds) {
		super(topN, emitFrequencyInSeconds);
	}

	void updateRankingsWithTuple(Tuple tuple) {
		Rankable rankable = RankableObjectWithFields.from(tuple);
		super.getRankings().updateWith(rankable);
	}

	@Override
	Logger getLogger() {
		return LOGGER;
	}
}
