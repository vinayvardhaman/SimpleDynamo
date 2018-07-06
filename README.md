# SimpleDynamo
Implement a simplified version of the Amazon Dynamo key-value storage system. A distributed key-value storage system that provides both
availability and linearizability and performs successful read and write operations even in the presence of failures.

Linearizability is implemented using Chain replication. A write is always made to the first partition,and then propagates to the next two
partitions in order. The last partition returns theresult of the write. Read operations are always made to the last of the chain of partitions. 

Messages between nodes are used to detect a failure. using a reasonable timeout period and considering a node failed if it does not respond
before the timeout. When the coordinator for a request fails, its successor is contacted next to fulfill the request.

When a node recovers from failure, it copies all of the object writes that it missed during the failure. This is done by making requests to
the appropriate nodes(successor and predecessors) and copying their data.

Supports concurrent read and write operations also  handles a failure occurring during read and write operations.

Replication in content provider is handled just as it is in Dynamo. Each key-value pair is replicated over three consecutive partitions, starting from the
partition that the key belongs to (based on its consistent hashing).
