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
import java.util.HashSet;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.utils.Utils;
import java.util.Set;
import java.util.Map;
public class ReadDB implements IRichSpout {
	
	JedisCluster jc;
	SpoutOutputCollector _collector;

	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
		this._collector=collector;
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		jedisClusterNodes.add(new HostAndPort("133.30.112.129", 6379));
		jedisClusterNodes.add(new HostAndPort("133.30.112.241", 6379));
		jc = new JedisCluster(jedisClusterNodes);
	}

	@Override
	public void nextTuple() {
		_collector.emit(new Values(jc.lpop("trajdata")));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("full_coordinates"));

	}

	@Override
	public void ack(Object arg0) {
	}

	@Override
	public void activate() {

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
