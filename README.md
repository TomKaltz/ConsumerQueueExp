Completely rewritten POC broker and consumer queue processor according to per consumer stream progress tracking.  

#### Features
- The broker supports policies and projectors.
- Polling and processing happens at the same time, no need to dispatch events to a separate queue.
- Progress of each stream(correlation_key) is tracked in a separate table per consumer.
- Streams are processed in parallel with very high concurrency.
- Stream processing errors are retried after a delay.
- A stream's processing is halted (locked_until: infinity, locked_by: halted) after a configurable number of consecutive errors.

#### Limitations
- Process managers are not supported.
- Calculated correllationKeys for policies and projectors are not supported.

#### To run
- Edit postgres config at top of [./src/index.ts](https://github.com/TomKaltz/ConsumerQueueExp/blob/main/src/index.ts)
- Run ```npm install```
- Run ```npm run dev```
- To see the consumer halting in action, uncomment the throw new Error("chaos") in [./src/domain/NeverEnding.saga.ts](https://github.com/TomKaltz/ConsumerQueueExp/blob/main/src/domain/NeverEnding.saga.ts)

