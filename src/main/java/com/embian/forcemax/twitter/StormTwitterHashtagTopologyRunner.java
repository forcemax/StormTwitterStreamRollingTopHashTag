package com.embian.forcemax.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.embian.forcemax.twitter.bolt.IntermediateRankingsBolt;
import com.embian.forcemax.twitter.bolt.PrinterBolt;
import com.embian.forcemax.twitter.bolt.RollingCountBolt;
import com.embian.forcemax.twitter.bolt.ExtractHashTagBolt;
import com.embian.forcemax.twitter.bolt.TotalRankingsBolt;
import com.embian.forcemax.twitter.spout.TwitterSpout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class StormTwitterHashtagTopologyRunner {
	private static final Logger LOGGER = LoggerFactory.getLogger(StormTwitterHashtagTopologyRunner.class);
	private static final int TOP_N = 10;
	
	public static void main(String[] args) {
		Config config = new Config();
		config.setDebug(false);

		TopologyBuilder builder = wireTopology();

		String runMode = "local";
		if(args.length < 1 || (!args[0].equals("local") && !args[0].equals("server"))) {
			LOGGER.warn("Run TwitterSampleRollingTopHashTag local mode. if you want to run it on server mode \n"
					+ "give first argument 'server'");
		}else{
			runMode = args[0];
		}

		final String topologyName = "twittersamplerollingtophashtag-topology";
		// Server
		if(runMode.equals("server")){
			try {
				StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error("{}", e);
			} catch (InvalidTopologyException e) {
				LOGGER.error("{}", e);
			} catch (AuthorizationException e) {
				LOGGER.error("{}", e);
			}
		}

		// Local
		if(runMode.equals("local")){
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("local_topology", config, builder.createTopology());
		}
		
		LOGGER.debug("Finsish submiiting {} in {} mode", topologyName, runMode);
	}
	
	private static TopologyBuilder wireTopology() {
		TopologyBuilder builder = new TopologyBuilder();

		// Twitter API Key
		String consumerKey = "consumerKey";
		String consumerSecret = "consumerSecret";
		String accessToken = "accessToken";
		String accessTokenSecret = "accessTokenSecret";
		String[] keyWords = new String[0];
		
		builder.setSpout("twitter", new TwitterSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keyWords));
		
		builder.setBolt("extractHashTag", new ExtractHashTagBolt(), 6).shuffleGrouping("twitter");
		builder.setBolt("rollingCount", new RollingCountBolt(60, 5), 12).fieldsGrouping("extractHashTag", new Fields("word"));
	    builder.setBolt("intermediateRanking", new IntermediateRankingsBolt(TOP_N, 5), 6).fieldsGrouping("rollingCount", new Fields("obj"));
	    builder.setBolt("totalRanker", new TotalRankingsBolt(TOP_N, 10)).globalGrouping("intermediateRanking");
	    builder.setBolt("print",  new PrinterBolt(), 2).shuffleGrouping("totalRanker");

	    return builder;
	}
}
