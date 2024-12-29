import { emit, InferAggregate } from "@rotorsoft/eventually";
import { z } from "zod";

export const FirstAggregateSchemas = {
  commands: {
    DoFirstThing: z.object({}),
    StartSaga: z.object({}),
  },
  events: {
    FirstThingDone: z.object({
      callCount: z.number(),
    }),
    SagaEnded: z.object({}),
  },
  state: z.object({
    callCount: z.number(),
  }),
};

export const FirstAggregate = (
  stream: string
): InferAggregate<typeof FirstAggregateSchemas> => ({
  description: "An aggregate to test the consumer queue",
  stream,
  init: () => ({
    callCount: 0,
  }),
  schemas: FirstAggregateSchemas,
  on: {
    DoFirstThing: async (_, state) => {
      if (state.callCount < 30) {
        return emit("FirstThingDone", {
          callCount: state.callCount + 1,
        });
      } else if (state.callCount === 30) {
        return emit("SagaEnded", {});
      }
      throw new Error('should not have been called again')
      // return []
    },
    StartSaga: async (_, state) => {
      return emit("FirstThingDone", {
        callCount: state.callCount + 1,
      });
    },
  },
  reduce: {
    FirstThingDone: (_, event) => ({
      callCount: event.data.callCount,
    }),
    SagaEnded: (_, event) => ({}),
  },
});
