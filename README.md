POC broker and consumer queue processor.  This repo also has a modified PostgresOrderedMessageQueue, which supports streaming consumer queues by "correllation_key" whilst still maintaining the stream id in the original message.  I also added attempt counting so resiliency features can be added in the future.  A DLQ can be attained by setting LOCKED_BY to "DLQ" with a date very far into the future if attempt count is over a certain threshold.

# To run
- Edit postgres config at top of [./src/index.ts](https://github.com/TomKaltz/ConsumerQueueExp/blob/main/src/index.ts)
- Run ```npm run dev```
