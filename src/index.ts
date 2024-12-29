process.env.PG_HOST = "localhost";
process.env.PG_USER = "postgres";
process.env.PG_PASSWORD = "postgres";
process.env.PG_DATABASE = "postgres";
process.env.PG_PORT = "5432";
process.env.LOG_LEVEL = "info";
process.env.OAS_UI = "Rapidoc";

import { ExpressApp } from "@rotorsoft/eventually-express";
import {
  app,
  bootstrap,
  broker,
  store,
  subscriptions,
  client,
  sleep,
} from "@rotorsoft/eventually";
import {
  PostgresProjectorStore,
  PostgresSubscriptionStore,
} from "@rotorsoft/eventually-pg";
import { PostgresStore } from "./lib/PostgresStore";
import { ConsumerQueueingBroker } from "./lib/ConsumerQueuingBroker";
import { NeverEndingSaga } from "./domain/NeverEnding.saga";
import { OtherAggregate } from "./domain/Other.aggregate";
import { FirstAggregate } from "./domain/First.aggregate";
import { SagaProjector } from "./domain/Saga.projector";
import { config } from "./lib/config";
import { Pool } from "pg";

const EVENTS_TABLE = "cqe_events";

bootstrap(async () => {
  const eApp = new ExpressApp();
  app(eApp)
    .with(FirstAggregate)
    .with(OtherAggregate)
    .with(NeverEndingSaga)
    .with(SagaProjector, {
      projector: {
        store: PostgresProjectorStore("cqe_saga_projector"),
        indexes: [
          {
            id: "asc",
          },
        ],
      },
    })
    .build();

  const pool = new Pool({ ...config.pg, application_name: "cqe", max: 10 });
  
  const eventStore = PostgresStore(EVENTS_TABLE, pool);
  // await eventStore.drop();
  await eventStore.seed();
  store(eventStore);

  const queueBroker = await ConsumerQueueingBroker({
    eventsTable: EVENTS_TABLE,
    pool, // using a shared pool for performance reasons (separate pools seem to perform badly)
    consumerConfig: {
      autoSeed: true,
      concurrency: 5000, // Per consumer processing pool. Higher concurrency for large backlogs.
      eventsPerStream: 10, // This is the batch of events per stream before updating progress.
      retryDelayMs: 50, // The delay before a stream is eligible for retry
      maxConsecutiveErrors: 3, // The number of consecutive errors before permanently halting the stream (locked_until set to infinity)
    }
  });
  broker(queueBroker);

  await eApp.listen(3043);
  console.log("eventually listening on 3043");

  app().once("commit", async (event) => {
    console.timeEnd("first-commit");
  });

  const theClient = client();
  console.time("first-commit");
  console.time("submit-sagas");
  console.time("first-command-to-sags-all-finished");
  console.log("starting 10,000 sagas!");

  for (let i = 0; i < 10000; i++) {
    theClient
      .command(
        FirstAggregate,
        "StartSaga",
        {},
        { stream: crypto.randomUUID().replace(/-/gi, "") }
      )
      .then(() => {
        if (i === 9999) {
          console.timeEnd("submit-sagas");
        }
      })
      .catch(console.error);
  }
});
