import { InferProjector, ProjectorSchemas } from "@rotorsoft/eventually";
import { z } from "zod";
import { OtherAggregateSchemas } from "./Other.aggregate";
import { FirstAggregateSchemas } from "./First.aggregate";

export const SagaProjectorSchemas = {
    state: z.object({
        id: z.string(),
        startedAt: z.date({coerce: true}).optional(),
        completed: z.boolean().optional(),
        completedAt: z.date({coerce: true}).optional(),
    }),
    events: {
        FirstThingDone: FirstAggregateSchemas.events.FirstThingDone,
        SecondThingDone: OtherAggregateSchemas.events.SecondThingDone
    },
};

export const SagaProjector = (): InferProjector<typeof SagaProjectorSchemas> => {
  return {
    description: "Saga projector",
    schemas: SagaProjectorSchemas,
    on: {
        FirstThingDone: async (event, map) => {
            return [
                {
                    id: event.stream,
                    startedAt: event.created,
                },
            ];
        },
        SecondThingDone: async (event, map) => {
            return [
                {
                    id: event.stream.split("-")[0],
                    completed: true,
                    completedAt: event.created,
                },
            ];
        },
    },
  };
};
