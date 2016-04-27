package com.embian.forcemax.twitter.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.embian.forcemax.twitter.util.Rankable;
import com.embian.forcemax.twitter.util.Rankings;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import twitter4j.Status;

public class PrinterBolt extends BaseRichBolt {
	private static final long serialVersionUID = -4545808801514519687L;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(PrinterBolt.class);
	private OutputCollector collector;

	public void execute(Tuple tuple, BasicOutputCollector collector) {
	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
//		LOGGER.info("input tuple : {}", input);
//		Object obj = input.getValueByField("tweet");
		Object obj = input.getValueByField("rankings");
		try {
			if (obj instanceof Rankings) {
				Rankings ranking = (Rankings) obj;
				LOGGER.debug("input ranking : {}", ranking);
				int count = 1;
				for(Rankable rank : ranking.getRankings()) {
					LOGGER.info("RANK : {}, HashTag : {}, Count : {}", count++, rank.getObject(), rank.getCount());
				}
			} else if (obj instanceof Status) {
				Status status = (Status) obj;
				LOGGER.info("input tweet's text : {}", status.getText());
			}
		} catch (Exception e) {
			LOGGER.error("Exception occurred. {}", e);
		}
		collector.ack(input);
	}
}