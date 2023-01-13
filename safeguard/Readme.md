# Conduktor Proxy Safeguard Demo

## What is Conduktor Proxy Safeguard?

Kafka is an extremely configurable system. This flexibility is great but can lead to situations where inefficient choices are made that can have unexpeccted impact. Conduktor Proxy's safeguard feature allows limits beyond the scope of regular Kafka to be imposed to ensure that the most efficient configuration is applied in Kafka. 

* [Create topic Safeguard](createTopic/Readme.md) - Limits on topic creation to ensure that any topics created in the cluster adhere to a minimum specification for Replication Factor and Partition count.
* [Alter configs Safeguard](alterConfigs/Readme.md) - Protect Kafka from inefficient configurations.
* [On produce Safeguard](onProduce/Readme.md) - Protect Kafka from inefficient messages.