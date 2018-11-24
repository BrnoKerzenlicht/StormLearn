package com.brnokerzenlicht.storm.learn;

import com.alibaba.fastjson.JSON;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.storm.shade.org.apache.commons.lang.RandomStringUtils;
import org.apache.storm.shade.org.apache.commons.lang.time.DateFormatUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * 普通Spout
 * @See org.apache.storm.testing.TestWordSpout
 */
public class TestSpout implements IRichSpout {

    private static final String MODULE_NAME = "[TestSpout]";
    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " open");
        this.collector = collector;
    }

    @Override
    public void close() {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " close");
    }

    @Override
    public void activate() {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " activate");
    }

    @Override
    public void deactivate() {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " deactivate");
    }

    @Override
    public void nextTuple() {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " nextTuple");
        Utils.sleep(1000);
        // emit返回tuple被发送到的task的id
        List<Integer> result = collector.emit(new Values(RandomStringUtils.random(1)));
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " nextTuple" + result);
    }

    @Override
    public void ack(Object msgId) {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " ack" + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " fail" + msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " declareOutputFields"
                        + JSON.toJSONString(declarer));
        declarer.declare(new Fields("word"));
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " declareOutputFields"
                        + JSON.toJSONString(declarer));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " getComponentConfiguration");
        return null;
    }
}
