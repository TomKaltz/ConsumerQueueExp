process.env.PG_HOST = "localhost";
process.env.PG_USER = "postgres";
process.env.PG_PASSWORD = "postgres";
process.env.PG_DATABASE = "postgres";
process.env.PG_PORT = "5432";
process.env.LOG_LEVEL = "info";
process.env.OAS_UI = "Rapidoc";

import {
  app,
  bootstrap,
  broker,
  store,
} from "@rotorsoft/eventually";
import { PostgresProjectorStore } from "@rotorsoft/eventually-pg";
import { PostgresStore } from "./lib/PostgresStore";
import { ConsumerQueueingBroker } from "./lib/ConsumerQueuingBroker";
import { SagaProjector } from "./domain/Saga.projector";
import { config } from "./lib/config";
import { Pool } from "pg";
import { FirstAggregate } from "./domain/First.aggregate";
import { OtherAggregate } from "./domain/Other.aggregate";

const EVENTS_TABLE = "cqe_events";

bootstrap(async () => {
  app()
    .with(FirstAggregate)
    .with(OtherAggregate)
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
    store(eventStore);
    
    const queueBroker = await ConsumerQueueingBroker({
      eventsTable: EVENTS_TABLE,
      pool,
    });
    broker(queueBroker);
    app().listen();
});
