Completely rewritten POC broker and consumer queue processor according to indivudual stream progress tracking.  

#### Features
- The broker supports policies and projectors.
- Progress of each stream(correlation_key) is tracked in a separate table per consumer.
- Streams are processed in parallel with high concurrency. (default 5000)
- Stream processing errors are retried after a delay.
- A stream's processing is halted after a configurable number of consecutive errors.

#### Limitations
- Process managers are not supported.

#### To run
- Edit postgres config at top of [./src/index.ts](https://github.com/TomKaltz/ConsumerQueueExp/blob/main/src/index.ts)
- ```npm install```
- Run ```npm run dev```

