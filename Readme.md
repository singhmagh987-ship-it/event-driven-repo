# EDA (Event Driven Architecture) and Implementing Kafka in Spring Boot

Event-Driven Architecture basically has 2 types:
- **Pull-based**
- **Push-based**

## Advantages of EDA
- Async , loose coupling, handles large scale continuous stream of events.
---

## Kafka (Message Broker)

**Kafka** = Distributed event streaming platform (append log based)
High Availability - load is shared among different kafka brokers
No Single Point of Failure (Highly Resilient system) - multiple brokers / mutiple controllers (active+standby) / leader-follower pattern


### Kafka Architecture
Multi Broker Architecture and Leader-Follower Partition

### Kafka Cluster
Combination of Multiple Kafka servers (Brokers)

### Data Flow
Producer publishes/emit events → Kafka stores the Event (based on retention policy) → Consumer subscribes/read from the kafka

### Broker
Single kafka server instance which consists of some topic and some partitions

### Topic
A category where same kind of events are published by the producer

### Partition
Where the events are physically stored

### Segments
Multiple `.log` files inside partition where the events get stored

### Producer
It publishes the event along with the topic name and key

### Partition Assignment
```
Specific Partition = hash(key) % number_of_partitions
```
It is that specific partition to which publisher publishes the event

### Consumer
Subscribes to and reads from topics
It pulls/reads the event , process the event and do a manual batch offset commit. Kafka stores the committed offset for that consumer group + topic + partition in the "__consumer_offsets" topic (Kafka's internal topic).

### Consumer Groups
It is a logical unit of which many consumers can be a part of.

Rule is -
1) Multiple consumers within the same consumer group cannot read from the same partition.
2) Two consumer within different consumer group can read from the same partition.
The offset of each consumer group is stored in the _consumer_offsets topic.

### Replication Factor

How many partition replica should contain the event publishes by the producer. if the producer configures replication factor should be 3, the event will be stored in one leader partition along with two follower partition in different brokers.

### ISR (In-Sync Replicas)

The list which tells currently how many follower partition replica are in sync with the leader partition. This list includes the leader partition replica as well and this list cannot be empty as there will always be the leader partition/broker present to accept the request even if follower partition becomes out of sync.

### Controller
It is the one which takes care of the cluster metadata i.e updates the cluster metadata (_cluster_metadata.log) - cluster metadata involves Topics,Partitions,Segments and Brokers.
It is responsible for the creation and deleteion of the topic across the brokers.
It decides which broker consists of which topic's leader and follower parititon.
It decides which broker will be the leader or follower in case of broker failure or crash.
Shares all the updated cluster metadata information among the brokers.
Keeps check of the broker failure(s)

### Zookeeper and KRaft (Kafka Raft)
Zookeeper is a legacy external distributed system to manage the Controller.
KRaft (using Raft Consesnsus algorithms) is an internal layer or system within kafka to manage the Controller using Consesnsus/Quorum.
 - There would be mutilple Contollers - one Active and others as StandBy(ready to become leader or Active contoller in case of any failure(s))
 - Active Contoller sends hearbeats to the Standby controllers.

### Leader - Follower

All requests are served by the leader partition or broker.
All follower partition or broker continuous poll the leader partition or broker to be in sync.

 Questions to ask yourself?
 - ### what happens when the Active controller fails?
 - ### what happens when leader partition fails?
 - ### what happens when follower partition fails?
 - ## what happens when consumer fails?
---

## Key Points

- Both Consumer and Producer read and write only from/to the leader partition
- Kafka provides us the Replay event advantages

## Producer Flow
1. **Producer emits the event**

   * Producer emits the event using:

     ```java
     kafkaTemplate.send(topic_name, key, value)
     ```

2. **Serializer (Use the Built-in or Custom Serializer)**

   * Converts the key and value to `byte[]`.
   * Example:

     * For key, use the `StringSerializer`.
     * For value, use the `JsonSerializer`.

3. **Partitioner**

   * Returns the partition number where the event will be persisted.
   * Formula:

     ```
     Partition = hash(key) % number of partitions
     ```

4. **Record Accumulator**

   * It is a buffer where the events are batched based on per topic-partition.
   * For a particular topic and partition, the events are stored in a batch.
   * So events are sent to the Kafka broker in batch mode.
   * Each batch contains multiple event requests.

5. **Compression**

   * Used to compress the entire batch and send it to the Kafka broker.
   * Advantages:

     * Less network usage.
     * Saves disk storage when stored in Kafka.
   * Algorithms like `gzip`, `snappy`, etc.

6. **Sender Thread**

   * Processes the batch asynchronously and fetches the response from Kafka.
   * Clubs the related batches based on the leader broker and sends only a single `ProducerRequest` to avoid multiple network calls to the Kafka broker.

7. **Producer sends the event**

   * The event is sent to the Kafka broker.

The Producer configuration can be listed in the application.properties

1. mutilple broker URLs
2. maximum cluster metadata age
3. key-serializer
4. value-serializer
5. acks. if ack=1 means wait for the leader, if ack=0 means fire and forget, if ack=all means all in-sync replicas (ISR) should confirm the event log.
6. record accumulator - batch size (max) and buffer memory (max) and linger.ms.
7. compression-type


