package com.nowcoder.async;

import com.alibaba.fastjson.JSON;
import com.nowcoder.model.Message;
import com.nowcoder.util.JedisAdapter;
import com.nowcoder.util.RedisKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class EventConsumer implements InitializingBean, ApplicationContextAware {
    private static final Logger logger = LoggerFactory.getLogger(EventConsumer.class);
    private Map<EventType, List<EventHandler>> config = new HashMap<EventType, List<EventHandler>>();
    private ApplicationContext applicationContext;

    @Autowired
    JedisAdapter jedisAdapter;

    @Override
    public void afterPropertiesSet() throws Exception {
    	// 获得所有的handler 代表消费者
        Map<String, EventHandler> beans = applicationContext.getBeansOfType(EventHandler.class);
        if (beans != null) {
        	// 遍历map, 遍历所有的消费者
            for (Map.Entry<String, EventHandler> entry : beans.entrySet()) {
            	// 每个消费者可以处理的事件的类型   一个消费者可以处理多个事件类型
                List<EventType> eventTypes = entry.getValue().getSupportEventTypes();
                // 将事件类型存储map中   key为处理事件的类型  value为一个list数组,里面存储的是消费者
	            // 一个事件可以被多个消费者处理
                for (EventType type : eventTypes) {
                    if (!config.containsKey(type)) {
                        config.put(type, new ArrayList<EventHandler>());
                    }
                    config.get(type).add(entry.getValue());
                }
            }
        }

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                	// 该系列事件的列表名字
                    String key = RedisKeyUtil.getEventQueueKey();
	                // 获得该系列的事件
	                // 参数0表示一直阻塞下去，直到List出现数据
                    List<String> events = jedisAdapter.brpop(0, key);
	                // 从中取出一个事件
                    for (String message : events) {
                        if (message.equals(key)) {
                            continue;
                        }

                        EventModel eventModel = JSON.parseObject(message, EventModel.class);
                        if (!config.containsKey(eventModel.getType())) {
                            logger.error("不能识别的事件");
                            continue;
                        }

                        for (EventHandler handler : config.get(eventModel.getType())) {
                            handler.doHandle(eventModel);
                        }
                    }
                }
            }
        });

        thread.start();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
