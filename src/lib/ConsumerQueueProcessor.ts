import { 
  Policy,
  PolicyFactory,
  app,
  client,
  log,
  Messages,
  Message,
  CommittedEvent,
  command,
  EventHandlerFactory,
  InferPolicy,
  State
} from "@rotorsoft/eventually";
import { PostgresOrderedMessageQueue } from './PostgresOrderedMessageQueue';

const debugLog = (message: string, ...context: any[]): void => {
  //log().info(message, ...context);
};

export const ConsumerQueueProcessor = async <S extends State, C extends Messages, E extends Messages>(
  policyFactory: EventHandlerFactory<S, C, E>,
  options?: {
    concurrency?: number;
    pollInterval?: number;
  }
) => {
  const {
    concurrency = 3,
    pollInterval = 1000,
  } = options ?? {};

  const policyName = policyFactory.name;
  // Get policy instance
  const policy = policyFactory() as Policy<any, any>;
  const queue = PostgresOrderedMessageQueue(policyName);
  let isProcessing = false;
  let shouldStop = false;
  let activeDequeues = 0;
  
  const processMessage = async (event: CommittedEvent): Promise<void> => {
    try {
      debugLog(`Processing event for policy ${policyName}`, { event });
      
      
      // Check if policy can handle this event
      if (!policy.on[event.name]) {
        throw new Error(`Policy ${policyName} cannot handle event ${event.name}`);
      }

      // Process the event through the policy
      const cmd = await policy.on[event.name](event);
      
      // If policy returns a command, send it with proper metadata
      if (cmd) {
        debugLog(`Policy ${policyName} generated command`, { cmd });
        
        if(!cmd.stream) {
          throw new Error(`Command ${cmd.name} has no stream specified`);
        }

        await command({
          ...cmd,
          actor: {
            id: policyName,
            name: policyName,
          }
        }, {
          correlation: event.metadata?.correlation || crypto.randomUUID(),
          causation: {
            event: {
              id: event.id,
              name: event.name,
              stream: event.stream
            }
          }
        });
      }
    } catch (error) {
      log().error(`Error processing message in policy ${policyName}: ${error}`);
      throw error; // Let the caller handle retries
    }
  };

  const startDequeue = async (): Promise<void> => {
    if (activeDequeues >= concurrency || shouldStop) {
      return;
    }

    activeDequeues++;
    debugLog(`Starting dequeue operation (active: ${activeDequeues}/${concurrency})`);

    try {
      const messageHandler = async (message: Message<Messages>): Promise<void> => {
        try {
          if (message.data && 'name' in message.data) {
            await processMessage(message.data as CommittedEvent);
          }
        } catch (error) {
          log().error(`Failed to process message in ${policyName} ${error}`);
          throw error;
        }
      };

      const result = await queue.dequeue(messageHandler);
      
      // If no messages were available, wait before trying again
      if (result === false) {
        debugLog(`No messages available for ${policyName}, backing off for ${pollInterval}ms`);
        await new Promise(resolve => setTimeout(resolve, pollInterval));
      }
    } catch (error) {
      log().error(`Error in dequeue operation: ${error}`);
    } finally {
      activeDequeues--;
      debugLog(`Completed dequeue operation (active: ${activeDequeues}/${concurrency})`);
      
      // If we're still processing and have capacity, start another dequeue
      if (isProcessing && !shouldStop) {
        void startDequeue();
      }
    }
  };

  const maintainConcurrency = async (): Promise<void> => {
    while (isProcessing && !shouldStop) {
      const dequeuesNeeded = concurrency - activeDequeues;
      
      // Start new dequeue operations until we reach concurrency limit
      if (dequeuesNeeded > 0) {
        debugLog(`Starting ${dequeuesNeeded} new dequeue operations`);
        for (let i = 0; i < dequeuesNeeded && !shouldStop; i++) {
          void startDequeue();
        }
      }

      // Wait before checking again
      await new Promise(resolve => setTimeout(resolve, pollInterval));
    }
  };

  const start = async (): Promise<void> => {
    if (isProcessing) {
      debugLog(`${policyName} processor already running`);
      return;
    }

    debugLog(`Starting processor for ${policyName} with concurrency ${concurrency}`);
    isProcessing = true;
    shouldStop = false;

    // Start the concurrency maintenance loop
    void maintainConcurrency();
  };

  const stop = async (): Promise<void> => {
    debugLog(`Stopping processor for ${policyName}`);
    shouldStop = true;
    
    // Wait for all dequeue operations to complete
    while (activeDequeues > 0) {
      debugLog(`Waiting for ${activeDequeues} dequeue operations to complete`);
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    isProcessing = false;
    debugLog(`Stopped processor for ${policyName}`);
  };

  const processor = {
    start,
    stop,
    name: policyName
  };

  // Auto-start the processor
  await processor.start();

  return processor;
};
