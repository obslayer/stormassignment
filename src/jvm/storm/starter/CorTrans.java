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
public class CorTrans extends BaseBasicBolt {
	double x_min;
	double y_min;
	double scale;
	@Override
	public void prepare(Map stormconf, TopologyContext context) {
		x_min = 34.913994;
		y_min = 138.88663;
		scale = 0.00057;
	}
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		int x_id = (int) ((Float.parseFloat(tuple.getString(0)) - x_min) / scale);
		int y_id = (int) ((Float.parseFloat(tuple.getString(1)) - y_min) / scale);
		collector.emit(new Values(Integer.toString(x_id), Integer.toString(y_id)));
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transformed_x", "transformed_y"));
	}
}


