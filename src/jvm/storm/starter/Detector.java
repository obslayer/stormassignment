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
public class Detector extends BaseBasicBolt {
	JedisCluster jc;
	@Override
	public void prepare(Map stormconf, TopologyContext context) {
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		jedisClusterNodes.add(new HostAndPort("133.30.112.129", 6379));
		jedisClusterNodes.add(new HostAndPort("133.30.112.241", 6379));
		jc = new JedisCluster(jedisClusterNodes);
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String line = tuple.getString(0);
		if(line!=null){
			String[] terms = line.split(",");
			if(jc.llen("terms[0]")<2){
				jc.rpush(terms[0],terms[1]+','+terms[2]);
			}
			else{
				if(((jc.lindex("terms[0]",0)).compareTo(jc.lindex("terms[0]",1)))!=0 &&
					((jc.lindex("terms[0]",1)).compareTo(terms[1]+','+terms[2]))==0
					) collector.emit(new Values(terms[1], terms[2]));
				jc.lpop(terms[0]);
				jc.rpush(terms[0], terms[1]+','+terms[2]);
			}	
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("untransformed_x", "untransformed_y"));
	}
}


