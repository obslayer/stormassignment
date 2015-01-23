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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.*;
import java.util.Map;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;
public class Counter extends BaseBasicBolt {
	JedisCluster jc;
	@Override
	public void prepare(Map StormConf, TopologyContext context) {
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		jedisClusterNodes.add(new HostAndPort("133.30.112.129", 6379));
		jedisClusterNodes.add(new HostAndPort("133.30.112.241", 6379));
		jc = new JedisCluster(jedisClusterNodes);
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String x = tuple.getString(0);
		String y = tuple.getString(1);
		String key = x + '_' + y;
		jc.setnx('z'+key, Integer.toString(0));
		jc.incr('z'+key);
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("cell", "count"));
	}
}
