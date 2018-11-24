package com.brnokerzenlicht.storm.learn;

import com.alibaba.fastjson.JSON;
import java.util.Date;
import java.util.Map;
import org.apache.storm.shade.org.apache.commons.lang.time.DateFormatUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 * 普通Bolt
 */
public class TestBolt extends BaseRichBolt {

    private static final String MODULE_NAME = "[TestBolt]";
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " prepare");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            System.err.println(
                    DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern()) + ":"
                            + Thread.currentThread().getName() + ":" + MODULE_NAME + " execute "
                            + JSON.toJSONString(input));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " declareOutputFields");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " getComponentConfiguration");
        return super.getComponentConfiguration();
    }

    @Override
    public void cleanup() {
        System.err
                .println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern())
                        + ":" + Thread.currentThread().getName() + ":" + MODULE_NAME + " cleanup");
    }
}
