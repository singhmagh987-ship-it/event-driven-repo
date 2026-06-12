# EDA (Event Driven Architecture) and Implementing Kafka in Spring Boot

Event-Driven Architecture basically has 2 types:
- **Pull-based**
- **Push-based**

## Advantages of EDA

---

## Kafka

**Kafka** = Distributed event streaming platform (append log based)

### Kafka Architecture
- Multi Broker Architecture and Leader-Follower Partition

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

### Controller

### Zookeeper and KRaft (Kafka Raft)

### Consumer Groups

---

## Key Points

- Both Consumer and Producer read and write only from/to the leader partition
- Kafka provides us the Replay event advantages
