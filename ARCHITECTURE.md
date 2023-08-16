# Client

- get(key) -> (String, Integer)
- put (key, value) -> Integer

# Node

- onPutRequest
    - go into waiting metadata
    - contact the next W nodes by sending the key
    - wait for all the replies with the version number
    - if while waiting receives a nack with the id of another coordinator send a contact request to that coordinator
    - if W acks are received go into commit metadata end exit waiting metadata
    - send write message to the first W nodes that responded
    - send putResponse to the client
    - go to idle metadata

- onWrite
    - write (key,value,version) to file / memory


- onContactRequest
    - if not ready on that key reply ack and go into ready metadata
    - if ready on that key by a coordinator with higher id wait for the write to terminate and then reply with ack
    - if ready on that key by a coordinator with lower id reply nack with the coordinator id

    - if waiting and id is lower and on the same key,
        - send abort message to all nodes contacted
        - push the putRequest on a stack
        - reply with the version number to the higher id node and go into ready metadata
    - if waiting and id is higher and on the same key:
        - wait to finish the current write and then reply with the version number and go into ready metadata
- onAbort
    - go into idle metadata, without pushing anything on the queue


- onGetRequest
    - read from R nodes
    - return latest version, GetResponse message to the client

- onRead
    - reply with (value, version)


# Inter Server messages
- onWrite (key, value, version)
- contactRequest (key)
- contactResponse (key , version)
- abort(key)
- nack(key, nodeid)
- read(key)
- readResponse (key, value, version)


# Client Server messages
- get(key)
- getResponse(key, value, version)
- put(key, value)
- putResponse(key, version)