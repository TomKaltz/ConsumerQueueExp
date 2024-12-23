import { InferPolicy, cmd } from "@rotorsoft/eventually";
import { FirstAggregateSchemas } from "./First.aggregate";
import { OtherAggregateSchemas } from "./Other.aggregate";

export const NeverEndingSagaSchemas = {
  events: {
    FirstThingDone: FirstAggregateSchemas.events.FirstThingDone,
    SecondThingDone: OtherAggregateSchemas.events.SecondThingDone,
  },
  commands: {
    DoFirstThing: FirstAggregateSchemas.commands.DoFirstThing,
    DoSecondThing: OtherAggregateSchemas.commands.DoSecondThing,
  },
};
export const NeverEndingSaga = (): InferPolicy<
  typeof NeverEndingSagaSchemas
> => ({
  description: "a saga that never ends",
  schemas: NeverEndingSagaSchemas,
  on: {
    FirstThingDone: async (event) => {
      return cmd("DoSecondThing", {}, event.stream.split("-")[0] + "-Other");
    },
    SecondThingDone: async (event) => {
      return cmd("DoFirstThing", {}, event.stream.split("-")[0]);
    },
  },
});
