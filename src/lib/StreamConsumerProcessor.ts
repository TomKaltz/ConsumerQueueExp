import {
  CommittedEvent,
  EventHandlerFactory,
  Messages,
  State,
  log,
  app,
  command,
  store,
  ArtifactType,
  ProjectionMap,
  ProjectorFactory,
  client,
  drain,
} from "@rotorsoft/eventually";
import { Pool } from "pg";
import { hostname } from "os";
import { pid } from "process";

const event_handler_types: Array<ArtifactType> = [
  "policy",
  "process-manager",
  "projector",
];

type ProcessingSlot = {
  stream: string | null;
  isBusy: boolean;
  lastProcessedId: number;
};

export type ConsumerConfig = {
  name: string;
  interestedEvents: string[];
  eventsTable: string;
  concurrency?: number;
  eventsPerStream?: number;
  pool: Pool;
  autostart?: boolean;
  autoSeed?: boolean;
  retryDelayMs?: number;
  maxConsecutiveErrors?: number;
};

const getProgressTableName = (consumerName: string) =>
  `consumer_progress_${consumerName.toLowerCase().replace(/[^a-z0-9_]/g, "_")}`;

const createProgressTableSQL = (tableName: string) => `
CREATE TABLE IF NOT EXISTS "${tableName}" (
    correlation_key text NOT NULL,
    last_processed_id bigint NOT NULL,
    last_processed_at timestamptz NOT NULL DEFAULT NOW(),
    locked_by text,
    locked_until timestamptz,
    PRIMARY KEY (correlation_key)
);

-- Add error tracking columns
ALTER TABLE "${tableName}"
ADD COLUMN IF NOT EXISTS consecutive_errors integer NOT NULL DEFAULT 0;

ALTER TABLE "${tableName}"
ADD COLUMN IF NOT EXISTS last_error text;

ALTER TABLE "${tableName}"
ADD COLUMN IF NOT EXISTS last_error_at timestamptz;

-- Index for progress table locking
CREATE INDEX IF NOT EXISTS "${tableName}_locked_until_idx" 
ON "${tableName}" (locked_until) 
WHERE locked_until IS NOT NULL;

-- Index for locked_by to help with cleanup
CREATE INDEX IF NOT EXISTS "${tableName}_locked_by_idx" 
ON "${tableName}" (locked_by) 
WHERE locked_by IS NOT NULL;

-- Index for last_processed_id to help with finding unprocessed events
CREATE INDEX IF NOT EXISTS "${tableName}_last_processed_id_idx" 
ON "${tableName}" (last_processed_id);

-- Composite index for common query pattern
CREATE INDEX IF NOT EXISTS "${tableName}_locked_composite_idx" 
ON "${tableName}" (locked_by, locked_until) 
WHERE locked_by IS NOT NULL;

-- Index for error tracking
CREATE INDEX IF NOT EXISTS "${tableName}_consecutive_errors_idx" 
ON "${tableName}" (consecutive_errors DESC) 
WHERE consecutive_errors > 0;
`;

/**
 * Creates a stream consumer processor that handles events for policies, process managers, or projectors
 * 
 * @param handlerFactory - Factory function that creates the event handler (policy, process manager, or projector)
 * @param consumerConfig - Configuration object for the consumer
 * @param consumerConfig.name - Unique name for this consumer instance
 * @param consumerConfig.interestedEvents - Array of event names this consumer should process
 * @param consumerConfig.eventsTable - Name of the table containing the events
 * @param consumerConfig.pool - Postgres connection pool
 * @param consumerConfig.pollInterval - Milliseconds to wait between polling for new events (default: 1000)
 * @param consumerConfig.concurrency - Number of parallel processors to run (default: 50)
 * @param consumerConfig.eventsPerStream - Number of events per stream to process per poll (default: 10)
 * @param consumerConfig.autostart - Whether to start processing immediately (default: true)
 * @param consumerConfig.autoSeed - Whether to automatically create tables on start (default: false)
 * @param consumerConfig.retryDelayMs - Milliseconds to wait before retrying a stream after an error (default: 200)
 * @param consumerConfig.maxConsecutiveErrors - Maximum number of consecutive errors before halting a stream (default: 10)
 * @returns A processor object with start, stop, drop, seed, and dispose methods
 */
export const StreamConsumerProcessor = async <S extends State>(
  handlerFactory: EventHandlerFactory<S> | ProjectorFactory<S>,
  consumerConfig: ConsumerConfig
) => {
  const {
    name,
    interestedEvents,
    eventsTable,
    concurrency = 50,
    eventsPerStream = 10,
    pool,
    autostart = true,
    autoSeed = false,
    retryDelayMs = 200,
    maxConsecutiveErrors = 10
  } = consumerConfig;

  const lockId = `${hostname()}:${pid}:${name}`;
  const processingSlots: ProcessingSlot[] = Array(concurrency).fill(null).map(() => ({
    stream: null,
    isBusy: false,
    lastProcessedId: -1
  }));

  let isProcessing = false;
  let shouldStop = false;
  let currentBackoff = 100; // Start with 100ms backoff
  const maxBackoff = 5000; // Max backoff of 5 seconds
  const minBackoff = 100; // Min backoff of 100ms
  let nextPollTime: number | null = null;
  let isInitialized = false;
  let pollPromise: Promise<void> | null = null;
  let backoffTimer: NodeJS.Timeout | null = null;

  // Get the artifact metadata to determine the type
  const artifact = [...app().artifacts.values()].find(
    (a) => a.factory.name === handlerFactory.name
  );

  if (!artifact) {
    throw new Error(`Handler ${name} not found in app artifacts`);
  }

  // Allow policies, process managers, and projectors
  if (!event_handler_types.includes(artifact.type)) {
    throw new Error(`Handler ${name} must be a policy, process manager, or projector`);
  }

  const progressTable = getProgressTableName(name);

  const seed = async (): Promise<void> => {
    await pool.query(createProgressTableSQL(progressTable));

    // If this is a projector, seed its store
    if (artifact.type === "projector" && artifact.projector) {
      await artifact.projector.store.seed(
        artifact.projector.schema,
        artifact.projector.indexes
      );
    }
  };

  // Move seeding to happen only once during instantiation if enabled
  if (autoSeed) {
    await seed();
  }

  const processEvent = async (
    event: Pick<CommittedEvent, keyof CommittedEvent>
  ): Promise<void> => {
    try {
      if (artifact.type === "policy") {
        const handler = handlerFactory as EventHandlerFactory<S>;
        const policy = handler();
        if (!policy.on[event.name]) return;
        const result = await policy.on[event.name](event as any, {} as S);
        if (result) {
          await command(
            {
              ...result,
              actor: { id: name, name },
            },
            {
              correlation: event.metadata?.correlation || event.stream,
              causation: {
                event: {
                  id: event.id,
                  name: event.name,
                  stream: event.stream,
                },
              },
            }
          );
        }
      } else if (artifact.type === "projector") {
        // Get the projector instance and its store
        const projector = (handlerFactory as ProjectorFactory<S>)();
        if (!projector.on[event.name]) return;

        // Create projection map for this event
        const projectionMap: ProjectionMap<S> = {
          records: new Map(),
          updates: [],
          deletes: []
        };

        // Get patches from projector handler
        const patches = await projector.on[event.name](event as any, projectionMap);
        if (patches) {
          // Add patches to the projection map
          for (const patch of patches) {
            if ('id' in patch) {
              // Remove id from patch data since it's handled separately in SQL
              const { id, ...rest } = patch;
              projectionMap.records.set(id, rest as any);
            } else {
              projectionMap.updates.push(patch);
            }
          }
        }

        // Get the projector's store and commit the map with the event's id as watermark
        const projectorStore = app().artifacts.get(handlerFactory.name)?.projector?.store;
        if (projectorStore) {
          await projectorStore.commit(projectionMap, event.id);
        }
      } else {
        // TODO: Load process manager state
        throw new Error("Process manager state loading not implemented yet");
      }
    } catch (error) {
      // Only log if it's a process manager since those errors are more critical
      if (artifact.type === "process-manager") {
        log().error(
          `Error processing event in handler ${name}: ${
            error instanceof Error ? error.message : String(error)
          }`
        );
      }
      throw error;
    }
  };

  const getAvailableSlots = () => 
    processingSlots.filter(slot => !slot.isBusy).length;

  const resetBackoff = (reason?: string) => {
    const wasBackedOff = currentBackoff > minBackoff;
    currentBackoff = minBackoff;
    nextPollTime = Date.now() + currentBackoff;
    
    // Clear any existing backoff timer
    if (backoffTimer) {
      clearTimeout(backoffTimer);
      backoffTimer = null;
    }

    // Trigger immediate poll only if we're not already polling
    if (!pollPromise) {
      pollPromise = pollForEvents().finally(() => {
        pollPromise = null;
      });
    }
  };

  const pollForEvents = async () => {
    if (!isProcessing || shouldStop) return;

    const availableSlots = getAvailableSlots();
    if (availableSlots === 0) return;

    try {
      const { rows: events } = await pool.query<Pick<CommittedEvent, keyof CommittedEvent>>(
        `WITH candidate_streams AS (
          -- Find unlocked streams with unprocessed events
          SELECT DISTINCT ON (e.stream) 
              e.stream,
              COALESCE(p.last_processed_id, -1) as last_processed_id
          FROM "${eventsTable}" e
          LEFT JOIN "${progressTable}" p ON p.correlation_key = e.stream
          WHERE e.name = ANY($1)
          AND (
              p.correlation_key IS NULL 
              OR e.id > p.last_processed_id
          )
          AND (
              p.locked_until IS NULL
              OR p.locked_until < NOW()
          )
          GROUP BY e.stream, p.last_processed_id
          ORDER BY e.stream, MIN(e.id) ASC
          LIMIT $2
        ),
        locked_streams AS (
          -- Lock streams atomically
          INSERT INTO "${progressTable}" AS p
          (correlation_key, last_processed_id, last_processed_at, locked_by, locked_until)
          SELECT 
              stream,
              last_processed_id,
              NOW(),
              $3,
              NOW() + interval '5 minutes'
          FROM candidate_streams
          ON CONFLICT (correlation_key) DO UPDATE
          SET locked_by = $3,
              locked_until = NOW() + interval '5 minutes',
              last_processed_at = NOW()
          WHERE p.locked_until < NOW()
          RETURNING correlation_key, last_processed_id
        )
        -- Get events for locked streams
        SELECT e.*
        FROM "${eventsTable}" e
        INNER JOIN locked_streams ls ON ls.correlation_key = e.stream
        WHERE e.name = ANY($1)
        AND e.id > ls.last_processed_id
        ORDER BY e.stream, e.id ASC
        LIMIT $4`,
        [interestedEvents, availableSlots, lockId, availableSlots * eventsPerStream]
      );

      if (events.length === 0) {
        // No events found, increase backoff
        currentBackoff = Math.min(currentBackoff * 2, maxBackoff);
        nextPollTime = Date.now() + currentBackoff;
        
        // Set backoff timer
        if (!backoffTimer) {
          backoffTimer = setTimeout(() => {
            backoffTimer = null;
            if (!pollPromise) {
              pollPromise = pollForEvents().finally(() => {
                pollPromise = null;
              });
            }
          }, currentBackoff);
        }
        return;
      }

      // Reset backoff since we found events
      currentBackoff = minBackoff;
      if (backoffTimer) {
        clearTimeout(backoffTimer);
        backoffTimer = null;
      }

      // Group events by stream
      const eventsByStream = events.reduce((acc, event) => {
        (acc[event.stream] = acc[event.stream] || []).push(event);
        return acc;
      }, {} as Record<string, typeof events>);

      // Assign streams to available slots
      for (const [stream, streamEvents] of Object.entries(eventsByStream)) {
        const slot = processingSlots.find(s => !s.isBusy);
        if (!slot) break;

        slot.stream = stream;
        slot.isBusy = true;
        slot.lastProcessedId = -1;

        void processStreamInSlot(slot, streamEvents);
      }
    } catch (error) {
      log().error(`Error polling for events: ${error instanceof Error ? error.message : String(error)}`);
      
      // On error, wait before retrying
      currentBackoff = Math.min(currentBackoff * 2, maxBackoff);
      nextPollTime = Date.now() + currentBackoff;
      if (!backoffTimer) {
        backoffTimer = setTimeout(() => {
          backoffTimer = null;
          if (!pollPromise) {
            pollPromise = pollForEvents().finally(() => {
              pollPromise = null;
            });
          }
        }, currentBackoff);
      }
    }
  };

  const processStreamInSlot = async (
    slot: ProcessingSlot,
    events: Pick<CommittedEvent, keyof CommittedEvent>[]
  ) => {
    try {
      // Process events in sequence for this stream
      for (const event of events) {
        if (shouldStop) break;
        try {
          await processEvent(event);
          slot.lastProcessedId = event.id;
        } catch (error) {
          // Update error tracking and set retry delay or halt the stream
          if (slot.stream) {
            const errorCount = await pool.query(
              `UPDATE "${progressTable}"
              SET consecutive_errors = COALESCE(consecutive_errors, 0) + 1,
                  last_error = $2,
                  last_error_at = NOW(),
                  last_processed_at = NOW(),
                  last_processed_id = CASE 
                    WHEN $3 > last_processed_id THEN $3 
                    ELSE last_processed_id 
                  END,
                  locked_by = CASE 
                    WHEN COALESCE(consecutive_errors, 0) + 1 >= $4 THEN 'halted'
                    ELSE 'retry_delay'
                  END,
                  locked_until = CASE 
                    WHEN COALESCE(consecutive_errors, 0) + 1 >= $4 THEN 'infinity'::timestamptz
                    ELSE NOW() + (($5 || ' milliseconds')::interval)
                  END
              WHERE correlation_key = $1
              RETURNING consecutive_errors`,
              [
                slot.stream, 
                error instanceof Error ? error.message : String(error), 
                slot.lastProcessedId > -1 ? slot.lastProcessedId : event.id - 1,
                maxConsecutiveErrors,
                retryDelayMs
              ]
            );

            // Only log when stream is halted
            if (errorCount.rows[0]?.consecutive_errors >= maxConsecutiveErrors) {
              log().red().error(
                `Stream ${slot.stream} halted after ${maxConsecutiveErrors} consecutive errors. Last error: ${
                  error instanceof Error ? error.message : String(error)
                }`
              );
            }
          }
          break;
        }
      }

      // If we processed any events successfully, update progress
      if (slot.lastProcessedId > -1) {
        await pool.query(
          `UPDATE "${progressTable}" 
          SET last_processed_id = CASE 
              WHEN $2 > last_processed_id THEN $2 
              ELSE last_processed_id 
            END,
            last_processed_at = NOW(),
            consecutive_errors = 0,
            last_error = NULL,
            last_error_at = NULL,
            locked_by = NULL,
            locked_until = NULL
          WHERE correlation_key = $1`,
          [slot.stream, slot.lastProcessedId]
        );
      }
    } finally {
      // Reset slot
      slot.stream = null;
      slot.isBusy = false;
      slot.lastProcessedId = -1;

      // Try to fill available slots, but only if we're not already polling
      if (isProcessing && !shouldStop && !pollPromise) {
        pollPromise = pollForEvents().finally(() => {
          pollPromise = null;
        });
      }
    }
  };

  const notifyEvent = (eventName: string) => {
    if (interestedEvents.includes(eventName)) {
      resetBackoff('Notification received,');
    }
  };

  const start = async (): Promise<void> => {
    if (isProcessing) {
      resetBackoff('Explicit start called,');
      return;
    }

    isProcessing = true;
    shouldStop = false;
    currentBackoff = minBackoff;

    if (!isInitialized) {
      isInitialized = true;
    }

    void pollForEvents();
  };

  const stop = async (): Promise<void> => {
    shouldStop = true;
    isProcessing = false;

    if (backoffTimer) {
      clearTimeout(backoffTimer);
      backoffTimer = null;
    }

    // Release all locks
    const streams = processingSlots
      .filter(slot => slot.stream)
      .map(slot => slot.stream);

    if (streams.length > 0) {
      await pool.query(
        `UPDATE "${progressTable}"
         SET locked_by = NULL,
         locked_until = NULL
         WHERE locked_by = $1
         AND correlation_key = ANY($2)`,
        [lockId, streams]
      );
    }

    // Reset all slots
    processingSlots.forEach(slot => {
      slot.stream = null;
      slot.isBusy = false;
      slot.lastProcessedId = -1;
    });
  };

  const drop = async (): Promise<void> => {
    await stop();
    // Drop the progress table
    await pool.query(`DROP TABLE IF EXISTS "${progressTable}"`);
    
    // If this is a projector, drop its store
    if (artifact.type === "projector" && artifact.projector) {
      await artifact.projector.store.drop();
    }
  };

  const dispose = async (): Promise<void> => {
    await stop();
  };

  const processor = {
    start,
    stop,
    drop,
    seed,
    dispose,
    name,
    notifyEvent
  };

  // Auto-start the processor unless disabled
  if (autostart) {
    await processor.start();
  }

  return processor;
};
