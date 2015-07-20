package com.embian.forcemax.twitter.bolt;

import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.embian.forcemax.twitter.util.Rankings;

/**
 * This bolt merges incoming {@link Rankings}.
 * <p/>
 * It can be used to merge intermediate rankings generated by
 * {@link IntermediateRankingsBolt} into a final, consolidated ranking. To do
 * so, configure this bolt with a globalGrouping on
 * {@link IntermediateRankingsBolt}.
 */
public final class TotalRankingsBolt extends AbstractRankerBolt {

	private static final long serialVersionUID = -8447525895532302198L;
	private static final Logger LOGGER = LoggerFactory.getLogger(TotalRankingsBolt.class);

	public TotalRankingsBolt() {
		super();
	}

	public TotalRankingsBolt(int topN) {
		super(topN);
	}

	public TotalRankingsBolt(int topN, int emitFrequencyInSeconds) {
		super(topN, emitFrequencyInSeconds);
	}

	@Override
	void updateRankingsWithTuple(Tuple tuple) {
		Rankings rankingsToBeMerged = (Rankings) tuple.getValue(0);
		super.getRankings().updateWith(rankingsToBeMerged);
		super.getRankings().pruneZeroCounts();
	}

	@Override
	Logger getLogger() {
		return LOGGER;
	}

}