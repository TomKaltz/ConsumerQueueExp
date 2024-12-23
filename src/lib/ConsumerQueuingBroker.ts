import { 
    Broker, 
    STATE_EVENT,
    app, 
    log, 
    store,
    drain,
    ArtifactType,
    EventHandlerFactory,
    subscriptions,
    Messages
  } from "@rotorsoft/eventually";
  import { PostgresOrderedMessageQueue } from './PostgresOrderedMessageQueue';
  import { MessageQueue } from './interface';
  
  const event_handler_types: Array<ArtifactType> = [
    "policy",
    "process-manager",
    "projector",
  ];
  
  const debugLog = (message: string, ...context: any[]): void => {
    // log().info(message, ...context);
  };
  
  const queues = new Map<string, MessageQueue<Messages>>();
  
  export const ConsumerQueueingBroker = (options?: {
    timeout?: number;
    limit?: number;
    delay?: number;
    subscribed?: boolean;
  }): Broker => {
    debugLog('ConsumerQueueingBroker started with options:', options);
    const {
      timeout = 5000,
      limit = 10,
      delay = 500,
      subscribed = true
    } = options ?? {};
  
    // Initialize policies and create queues for each
    const policies = [...app().artifacts.values()].filter(artifact => artifact.type === 'policy');
    
    policies.forEach(async artifact => {
      try {
        const queueName = artifact.factory.name;
        const queue = PostgresOrderedMessageQueue(queueName);
        await queue.seed();
        queues.set(queueName, queue);
        debugLog(`Created queue for policy ${queueName}`);
      } catch (error) {
        log().error(error);
      }
    });
  
    // Connect private event handlers
    const consumers = [...app().artifacts.values()]
      .filter(
        (v) =>
          event_handler_types.includes(v.type) &&
          v.inputs.length &&
          v.inputs.at(0)?.scope === "private"
      )
      .map((md) => ({
        factory: md.factory as EventHandlerFactory,
        names: md.inputs.map((input) => input.name),
      }));
  
    debugLog('Consumers initialized:', consumers);
  
    const drainAll = async (): Promise<void> => {
      debugLog("drainAll called");
      const pRes = await Promise.allSettled(
        consumers.map(async (c) => {
          debugLog("drainAll consumer start", {
            name: c.factory.name,
            events: c.names,
          });
  
          const queue = queues.get(c.factory.name);
          if (queue) {
            const lease = await subscriptions().poll(c.factory.name, {
              names: c.names,
              timeout,
              limit,
            });

            if (lease) {
              try {
                await queue.enqueue(lease.events.map(event=>({
                  name: event.name,
                  correlationKey: event.stream,
                  data: event,
                })));

                const watermark = lease.events.at(-1)?.id ?? lease.watermark;
                const ok = await subscriptions().ack(lease, watermark);
                
                if (!ok) {
                  throw new Error(`Failed to acknowledge lease ${lease.lease} for ${c.factory.name}`);
                }
              } catch (error) {
                log().error(error);
                return;
              }
            }
          } else {
            // Handle regular drain for non-policy consumers
            const { total, error } = await drain(c.factory, {
              names: c.names,
              timeout,
              limit,
              times: 3,
            });
            total && debugLog(`~~~ ${c.factory.name} drained ${total} events...`);
            if (error) {
              log().error(error);
              await throttledDrain();
            }
          }
          
          debugLog("drainAll consumer end", { name: c.factory.name });
        })
      );
  
      pRes.forEach((result, index) => {
        if (result.status === 'rejected') {
          log().error(`Consumer ${consumers[index].factory.name} failed: ${result.reason}`);
        }
      });
      debugLog("drainAll function end");
    };
  
    const throttledDrain = singleFlightDebounce(drainAll);
    
    const __drain = async (times: number | undefined = undefined) => {
      debugLog('__drain called');
      throttledDrain();
    };
  
    let periodicDrainInterval: NodeJS.Timeout;
  
    // Set up periodic drain
    periodicDrainInterval = setInterval(() => {
      throttledDrain().catch((error) => {
        log().error(error);
      });
    }, 2000);
  
    // Subscribe broker to commit events
    subscribed &&
      app().on("commit", async ({ factory, snapshot }) => {
        debugLog('Commit event received', { factory, snapshot });
        // Handle STATE_EVENT commits
        if (snapshot) {
          const commit = app().commits.get(factory.name);
          if (commit && commit(snapshot)) {
            try {
              const { id, stream, name, metadata, version } = snapshot.event!;
              await store().commit(
                stream,
                [
                  {
                    name: STATE_EVENT,
                    data: snapshot.state,
                  },
                ],
                {
                  correlation: metadata.correlation,
                  causation: { event: { id, name, stream } },
                },
                version
              );
            } catch (error) {
              log().error(error);
            }
          }
        }
  
        await throttledDrain();
      });
  
    return {
      name: 'ConsumerQueueingBroker',
      drain: __drain,
      dispose: async (): Promise<void> => {
        debugLog('Disposing broker');
        clearInterval(periodicDrainInterval);
        await Promise.all([...queues.values()].map(queue => queue.dispose?.()));
        queues.clear();
      }
    };
  };
  
/**
 * Creates a debounced version of an async function that ensures only one execution runs at a time.
 * If called while already running, it will queue up one additional execution to run after the current one completes.
 * Any further calls while running will be ignored - only one pending execution is remembered.
 * @param func The async function to debounce
 * @returns A debounced version of the input function that manages single-flight execution
 */
  function singleFlightDebounce(func: () => Promise<void>) {
    let running = false;
    let pendingExecution = false;
  
    return async () => {
      if (running) {
        pendingExecution = true;
        return;
      }
  
      do {
        running = true;
        pendingExecution = false;
        try {
          await func();
        } catch (error) {
          log().error(error);
        } finally {
          running = false;
        }
      } while (pendingExecution);
    };
  } 