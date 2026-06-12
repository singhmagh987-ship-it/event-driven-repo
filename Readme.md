EDA (Event Driven Architecture) and types - basically 2 - pull and push based.
Advantages of EDA ?

Kafka = distributed event streaming platform (append log based)
Kafka Archtitecture => Multi Broker Architecture and Leader-Follower Partition
Kafka Cluster = Combination of Multiple Kafka severs (Brokers)
Producer publishes/emit events --> Kafka stores the Event (based on retention policy) --> Consumer subscribes/read from the kafka
Broker = single kafka server instance which consists of some topic and some partitions
Topic = a category where same kind of event are published by the producer.
Partition = where the event are physically stored.
Segments = mutilples .log files inside partition where the event gets stored.
Producer = it publishes the event along with the topic name and key.
which specific Partition = hash (key) % number of partition. it is that specific partition to which publisher publishes the event.
Consumer = 
Both Consumer and Producer read and write only from/to the leader partition.
Kafka provides us the Replay event advantages.
