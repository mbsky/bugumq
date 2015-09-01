# BuguMQ的使用示例 #

## 建立连接 ##
```
Connection conn = Connection.getInstance();
conn.setHost("192.168.0.200");
conn.setPassword("foobared");
conn.setClientId("my_client_id");
conn.connect();
```

其中，clientId是能够唯一标识该客户端的字符串。

一个应用中，应该只有一个Connection，在应用开始的时候连接一次。在应用退出的时候，记得断开该连接：

```
conn.disconnect();
```

Connection内部使用了连接池，默认最多是8个连接，你可以自己定义这个数值，如下：
```
JedisPoolConfig config = new JedisPoolConfig();
config.setMaxActive(10);  //最多10个连接
conn.setPoolConfig(config);
        
conn.connect();
```
关于JedisPoolConfig的更多参数设置，请[查看其源代码](https://github.com/xetorthio/jedis/blob/master/src/main/java/redis/clients/jedis/JedisPoolConfig.java)。

## 获取Client ##
```
Client client = conn.getClient();
```

## 产生Queue消息 ##
```
client.produce("q1", "hello");
client.produce("q2", "world");

//产生紧急消息
client.produceUrgency("q1", "hurry up!");

//产生有效期为1分钟的消息
client.produce("q1", 60, "my message");

//产生2020年6月26日到期的消息
Date expireAt = parseDate("2020-06-26");
client.produce("q1", expireAt, "my message");
```

## 获取队列中消息数量 ##
```
long size = client.getQueueSize("q1");
```

## 删除队列中的消息 ##
```
//清除队列中的消息，只保留最新的5条。
client.retainQueue("q1", 5);

//清除队列中的全部消息
//client.clearQueue("q1");
```

## 消费Queue消息 ##
```
QueueListener listener = new QueueListener(){
    @Override
    public void onQueueMessage(String queue, String message) {
        System.out.println("queue: " + queue);
        System.out.println("message: " + message);
    }
};
client.consume(listener, "q1", "q2");
```

一个client，可以有多个QueueListener。一个QueueListener，可以监听一个或多个Queue的消息。

## 停止消费 ##
```
client.stopConsume("q1");

//停止所有的消费
//client.stopAllConsume();
```

## 发布Topic消息 ##
```
client.publish("topic/1", "hello");
client.publish("topic/2", "world");

//发布保留消息
client.publishRetain("topic/2", "welcome!");
```

## 清除Topic上的保留消息 ##
```
client.clearRetainMessage("topic/2");
```

## 订阅Topic消息 ##
```
TopicListener listener = new TopicListener(){
    @Override
    public void onTopicMessage(String topic, String message) {
        System.out.println("topic: " + topic);
        System.out.println("message: " + message);
    }
            
    @Override
    public void onPatternMessage(String pattern, String topic, String message) {
        System.out.println("pattern: " + pattern);
        System.out.println("topic: " + topic);
        System.out.println("message: " + message);
    }
};
        
client.setTopicListener(listener);
        
client.subscribe("topic/1", "topic/2");

//按模式订阅
//client.subscribePattern("topic/*");
```

**注意：**

（1）一个Client，只有一个TopicListener，用来监听所有的Topic消息。这一点与Queue消息不同。

（2）请尽量一次订阅多个Topic，而不要逐个订阅。因为在BuguMQ内部，每次订阅，都需要产生一个新的线程。

（3）按模式订阅时，无法获接收到Topic上的保留消息。

## 退订 ##
```
client.unsubscribe("topic/1", "topic/2");

//按模式退订
//client.unsubscribePattern("topic/*");
```

## 获取订阅者数量 ##
```
long count = client.getSubsribersCount("topic/1");
```

## 在线状态 ##
客户端可以报告自己的在线状态。要启用该功能，只需要设置心跳时间（KeepAlive），如下：
```
Connection conn = Connection.getInstance();
conn.setHost("192.168.0.200");
conn.setPassword("foobared");
conn.setClientId("my_client_id");
conn.setKeepAlive(60);   //60秒
conn.connect();
```
KeepAlive参数以秒为单位。设置好该参数后，client会自动每隔KeepAlive时间就向MQ服务器发送一条在线消息。服务器如果在1.5\*KeepAlive时间内没有收到来自client的在线消息，则表示该client已经不在线。

如果客户端没有设置KeepAlive参数，或者设置成小于等于0的数值，则不会发送在线消息，服务器会认为该客户端是不在线的。

当前client可以查询其它client的在线状态。如下：
```
//单个查询
boolean online = client.isOnline("client_x");

//批量查询
List<String> clientList = new ArrayList<String>();
clientList.add("client_a");
clientList.add("client_b");
clientList.add("client_c");
List<Boolean> onlineList = client.isOnline(clientList);
```