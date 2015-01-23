package storm.starter;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.*;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import java.util.Map;

public class WordCountTopology{
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("reader", new ReadDB(), 1);
		builder.setBolt("delect", new Detector(), 1).shuffleGrouping("reader");
		builder.setBolt("tran", new CorTrans(), 1).shuffleGrouping("delect");
		builder.setBolt("count", new Counter(), 1).fieldsGrouping("tran",new Fields("transformed_x"));
		Config conf = new Config();
		conf.setDebug(true);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(343);
			StormSubmitter.submitTopology(args[0],conf,builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wreader", conf, builder.createTopology());

			Thread.sleep(10000);

			cluster.shutdown();
		}
	}

}
