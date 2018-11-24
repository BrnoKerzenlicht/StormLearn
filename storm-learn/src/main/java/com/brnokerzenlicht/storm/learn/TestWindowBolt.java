package com.brnokerzenlicht.storm.learn;

import com.alibaba.fastjson.JSON;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.storm.shade.org.apache.commons.lang.time.DateFormatUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

/**
 * 滑动窗口Bolt
 */
public class TestWindowBolt extends BaseWindowedBolt {

    private static final String MODULE_NAME = "[TestWindowBolt]";
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " prepare");
        this.outputCollector = collector;
    }

    @Override
    public void execute(TupleWindow input) {
        try {
            List<Tuple> msg = input.get();
            System.err.println(
                    DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern()) + ":"
                            + Thread.currentThread().getName() + ":" + MODULE_NAME + " execute get() "
                            + JSON.toJSONString(msg));
            for (Tuple tuple : msg) {
                this.outputCollector.ack(tuple);
            }
            this.outputCollector.emit(new Values("get():" + msg.size()));

            msg = input.getExpired();
            System.err.println(
                    DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern()) + ":"
                            + Thread.currentThread().getName() + ":" + MODULE_NAME + " execute getExpired() "
                            + JSON.toJSONString(msg));
            for (Tuple tuple : msg) {
                this.outputCollector.ack(tuple);
            }
            this.outputCollector.emit(new Values("getExpired():" + msg.size()));

            msg = input.getNew();
            System.err.println(
                    DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern()) + ":"
                            + Thread.currentThread().getName() + ":" + MODULE_NAME + " execute getNew() "
                            + JSON.toJSONString(msg));
            for (Tuple tuple : msg) {
                this.outputCollector.ack(tuple);
            }
            this.outputCollector.emit(new Values("getNew():" + msg.size()));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " cleanup");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " getComponentConfiguration");
        return super.getComponentConfiguration();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " declareOutputFields"
                        + JSON.toJSONString(declarer));
        declarer.declare(new Fields("world count"));
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " declareOutputFields"
                        + JSON.toJSONString(declarer));
    }
}
