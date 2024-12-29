import { emit, InferAggregate } from "@rotorsoft/eventually";
import { z } from "zod";



let sagaEndedCounter = 0;

const incrementSagaEndedCounter = () => {
  sagaEndedCounter++;
  if (sagaEndedCounter === 1) {
    console.log("sagaEndedCounter", sagaEndedCounter);
    console.time("all-sagas-processed");
  }
  if (sagaEndedCounter === 10000) {
    console.log("sagaEndedCounter", sagaEndedCounter);
    console.timeEnd("all-sagas-processed");

  console.timeEnd("first-command-to-sags-all-finished")
  }
};

export const OtherAggregateSchemas = {
  commands: {
    DoSecondThing: z.object({}),
  },
  events: {
    SecondThingDone: z.object({
      callCount: z.number(),
    }),
  },
  state: z.object({
    callCount: z.number(),
  }),
};

export const OtherAggregate = (
  stream: string
): InferAggregate<typeof OtherAggregateSchemas> => ({
  description: "Another aggregate to test the consumer queue",
  stream,
  init: () => ({
    callCount: 0,
  }),
  schemas: OtherAggregateSchemas,
  on: {
    DoSecondThing: async (_, state) => {
      return emit("SecondThingDone", {
        callCount: state.callCount + 1,
      });
    },
  },
  reduce: {
    SecondThingDone: (_, event) => {
      incrementSagaEndedCounter();
      return {
        callCount: event.data.callCount,
      };
    },
  },
});
