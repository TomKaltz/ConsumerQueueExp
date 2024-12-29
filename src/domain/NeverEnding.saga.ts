import { InferPolicy, cmd, sleep, log } from "@rotorsoft/eventually";
import { FirstAggregateSchemas } from "./First.aggregate";
import { OtherAggregateSchemas } from "./Other.aggregate";

export const NeverEndingSagaSchemas = {
  events: {
    FirstThingDone: FirstAggregateSchemas.events.FirstThingDone,
    // SecondThingDone: OtherAggregateSchemas.events.SecondThingDone,
    // SagaEnded: FirstAggregateSchemas.events.SagaEnded,
  },
  commands: {
    // DoFirstThing: FirstAggregateSchemas.commands.DoFirstThing,
    DoSecondThing: OtherAggregateSchemas.commands.DoSecondThing,
  },
};

export const NeverEndingSaga = (): InferPolicy<
  typeof NeverEndingSagaSchemas
> => ({
  description: "this is the saga that never ends",
  schemas: NeverEndingSagaSchemas,
  on: {
    FirstThingDone: async (event) => {
      // Woohoo! we can do async stuff here without blocking other streams!!!
      // await sleep(10)
      throw new Error("chaos");
      return cmd("DoSecondThing", {}, event.stream.split("-")[0] + "-Other");
    },
    // SecondThingDone: async (event) => {
    //   return cmd("DoFirstThing", {}, event.stream.split("-")[0]);
    // },
    // SagaEnded: async (event) => {
    //   log().info(`Saga ended for stream ${event.stream}`);
    //   return undefined;
    // }
  },
});
