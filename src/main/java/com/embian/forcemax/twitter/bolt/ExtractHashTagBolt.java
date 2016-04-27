package com.embian.forcemax.twitter.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class ExtractHashTagBolt extends BaseRichBolt {
	private static final long serialVersionUID = -1647817314067529938L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ExtractHashTagBolt.class);
	
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void execute(Tuple input) {
//		LOGGER.info("input tuple : {}", tuple);
		Object obj = input.getValueByField("tweet");
		if (obj instanceof Status) {
			try {
				final Status status = (Status) obj;
				LOGGER.debug("input tweet : {}", status);
				
				final HashtagEntity[] hashtags = status.getHashtagEntities();
				if (hashtags != null) {
					for(HashtagEntity hashtag : hashtags) {
						if (hashtag != null) {
							final String strHashtag = hashtag.getText();
							collector.emit(new Values(strHashtag));
							LOGGER.debug("emit hashtag text : {}", strHashtag);
						}
					}
				}
			} catch (Exception e) {
				LOGGER.error("Exception occurred. {}", e);
			}
		}
		collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}