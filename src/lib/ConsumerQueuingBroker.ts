import {
  Broker,
  STATE_EVENT,
  app,
  log,
  store,
  ArtifactType,
  EventHandlerFactory,
  Messages,
} from "@rotorsoft/eventually";
import { MessageQueue } from "./interface";
import { StreamConsumerProcessor } from "./StreamConsumerProcessor";
import { Pool } from "pg";

const event_handler_types: Array<ArtifactType> = [
  "policy",
  "process-manager",
  "projector",
];

const debugLog = (message: string, ...context: any[]): void => {
  // log().info(message, ...context);
};

const queues = new Map<string, MessageQueue<Messages>>();
const processors = new Map<string, Awaited<ReturnType<typeof StreamConsumerProcessor>>>();

export interface ConsumerQueueingBrokerOptions {
  /** PostgreSQL connection pool */
  pool: Pool;
  /** Custom events table name. Defaults to "events" */
  eventsTable?: string;
  /** Whether to subscribe to commit events. Defaults to true */
  subscribed?: boolean;
  /** Maximum concurrent events processed per handler. Defaults to 50 */
  concurrency?: number;
  /** Number of events to process per stream in each poll. Defaults to 10 */
  eventsPerStream?: number;
  /** Whether to initialize processors on broker creation. Defaults to true */
  initializeOnStart?: boolean;
  /** Whether to start processors after initialization. Defaults to true */
  startOnInit?: boolean;
  /** Whether to automatically seed processors (and their progress tables) on initialization. Defaults to false */
  autoSeed?: boolean;
  /** Maximum number of consecutive errors before halting a processor. Defaults to 3 */
  maxConsecutiveErrors?: number;
}

/**
 * Creates a broker that manages event stream processors for policies, process managers, and projectors
 * using a consumer queuing pattern for reliable event processing.
 * 
 * @param options Configuration options for the broker
 * @returns A broker instance that manages stream processors
 */
export const ConsumerQueueingBroker = async (
  options: ConsumerQueueingBrokerOptions
): Promise<Broker & {
  initializeProcessors: () => Promise<void>;
  startProcessors: () => Promise<void>;
}> => {
  debugLog("ConsumerQueueingBroker started with options:", options);
  const {
    eventsTable = "events",
    subscribed = true,
    pool,
    concurrency = 50,
    eventsPerStream = 10,
    initializeOnStart = true,
    startOnInit = true,
    autoSeed = false,
    maxConsecutiveErrors = 3
  } = options ?? {};

  // Initialize stream processors for policies, process managers, and projectors
  const streamHandlers = [...app().artifacts.values()].filter(
    (artifact) => 
      event_handler_types.includes(artifact.type) &&
      artifact.inputs.length &&
      artifact.inputs.at(0)?.scope === "private"
  );

  debugLog(`Found ${streamHandlers.length} stream handlers to initialize`);
  streamHandlers.forEach((artifact) => {
    debugLog(`Initializing stream handler for ${artifact.factory.name}`, {
      type: artifact.type,
      inputs: artifact.inputs
    });
  });

  // Initialize processors
  const initializeProcessors = async () => {
    await Promise.all(
      streamHandlers.map(async (artifact) => {
        try {
          debugLog(`Creating processor for ${artifact.factory.name}`);
          const processor = await StreamConsumerProcessor(artifact.factory as EventHandlerFactory, {
            name: artifact.factory.name,
            interestedEvents: artifact.inputs.map((input) => input.name),
            eventsTable,
            concurrency,
            eventsPerStream,
            pool,
            autoSeed,
            maxConsecutiveErrors
          });
          processors.set(artifact.factory.name, processor);
          
          if (autoSeed) {
            debugLog(`Auto-seeding processor for ${artifact.factory.name}`);
            await processor.seed();
          }
          
          debugLog(`Initialized processor for ${artifact.factory.name}`);
        } catch (error) {
          log().error(error);
        }
      })
    );
  };

  // Start all processors
  const startProcessors = async () => {
    await Promise.all(
      [...processors.values()].map(async (processor) => {
        try {
          await processor.start();
          debugLog(`Started processor for ${processor.name}`);
        } catch (error) {
          log().error(error);
        }
      })
    );
  };

  // Initialize and start if configured
  if (initializeOnStart) {
    await initializeProcessors();
    if (startOnInit) {
      await startProcessors();
    }
  }

  // Subscribe broker to commit events and notify processors
  subscribed &&
    app().on("commit", async ({ factory, snapshot }) => {
      debugLog("Commit event received", { factory, snapshot });
      
      if (snapshot?.event) {
        // Notify processors about the event
        [...processors.values()].forEach(processor => {
          processor.notifyEvent(snapshot.event!.name);
        });
      }

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
    });

  return {
    name: "ConsumerQueueingBroker",
    initializeProcessors,
    startProcessors,
    drain: async (): Promise<void> => {
      debugLog("Draining broker - starting all processors");
      await Promise.all([...processors.values()].map(processor => processor.start()));
    },
    dispose: async (): Promise<void> => {
      debugLog("Disposing broker");
      await Promise.all([...processors.values()].map((processor) => processor.stop()));
      processors.clear();
      queues.clear();
    },
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
};
