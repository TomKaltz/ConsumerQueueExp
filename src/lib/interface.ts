import { Message, Messages, Disposable } from "@rotorsoft/eventually";

export interface MessageQueue<M extends Messages> extends Disposable {
    /**
     * Enqueues messages
     * @param messages messages with correlation keys
     */
    enqueue: (messages: (Message<M> & { correlationKey?: string })[]) => Promise<void>;
    /**
     * Dequeues message on top of the queue after being processed by consumer callback
     * @param callback consumer callback that receives the message with storage attributes {id, created} and returns a promise or error
     * @param opts optional options for dequeuing
     * @param opts.stream optional stream name to support independent concurrent consumers
     * @param opts.leaseMillis optional lease duration in milliseconds before lock expires (default dictated by adapter)
     * @returns promise that resolves true when message is successfully processed, false when stream is empty or lock cannot be acquired,
     *          rejects when message has failed to be processed
     */
    dequeue: (callback: (message: Message<M> & {
        id: number | string;
        created: Date;
    }) => Promise<void>, opts?: {
        correlationKey?: string;
        leaseMillis?: number;
    }) => Promise<boolean>;
    /**
     * Seeds the store
     */
    seed: () => Promise<void>;
    /**
     * Drops the store
     */
    drop: () => Promise<void>;
}