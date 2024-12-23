process.env.PG_HOST="localhost"
process.env.PG_USER="postgres"
process.env.PG_PASSWORD="postgres"
process.env.PG_DATABASE="postgres"
process.env.PG_PORT="5432"
process.env.LOG_LEVEL="data"
process.env.OAS_UI="Rapidoc"

import { ExpressApp } from "@rotorsoft/eventually-express";
import {
  app,
  bootstrap,
  broker,
  store,
  subscriptions,
  client,
} from "@rotorsoft/eventually";
import {
  PostgresStore,
  PostgresSubscriptionStore,
} from "@rotorsoft/eventually-pg";
import { ConsumerQueueingBroker } from "./lib/ConsumerQueuingBroker";
import { ConsumerQueueProcessor } from "./lib/ConsumerQueueProcessor";
import { NeverEndingSaga } from "./domain/NeverEnding.saga";
import { OtherAggregate } from "./domain/Other.aggregate";
import { FirstAggregate } from "./domain/First.aggregate";

bootstrap(async () => {
  const eApp = new ExpressApp();
  app(eApp)
    .with(FirstAggregate)
    .with(OtherAggregate)
    .with(NeverEndingSaga)
    .build();

  const eventStore = PostgresStore("cqe_events");
  await eventStore.seed();
  store(eventStore);
  
  const subcriptionStore = PostgresSubscriptionStore("qce_subscriptions");
  await subcriptionStore.seed();
  subscriptions(subcriptionStore);
  
  broker(ConsumerQueueingBroker());

  eApp
    .listen(3043)
    .then(() => {
      console.log("eventually listening on 3043");
    })
    .catch(console.error);

  // this will automatically start processing events
  await ConsumerQueueProcessor(NeverEndingSaga,{
    concurrency: 100, // 100 concurrent processing "slots"
    pollInterval: 1000,
  });

  // start a randomly Identified Saga
  await client().command(FirstAggregate,'StartSaga',{},{stream:crypto.randomUUID()});
});
