import { Console, Effect, Stream } from "effect"
import * as CFKV from "./cfkv"
import { KeyValueStore } from "@effect/platform"
import { Typeson } from "typeson"
import { builtin as typesonBuiltin } from "typeson-registry"
import {
	NodeFileSystem,
	NodeKeyValueStore,
	NodeRuntime,
} from "@effect/platform-node"

const OUT_PATH = __dirname + "/out"

const typeson = new Typeson().register([typesonBuiltin])

const persistKeyValuePair = (
	key: string,
	value: any,
	metadata?: Record<string, any>,
) =>
	Effect.gen(function* () {
		const kv = yield* KeyValueStore.KeyValueStore

		yield* Effect.all([
			kv.set(key, typeson.stringifySync(value)),

			metadata
				? kv.set(key + ":__metadata", JSON.stringify(metadata))
				: Effect.succeedNone,
		])
	})

const program = CFKV.streamKeys().pipe(
	Stream.filter((key) => /^flow:(\w+):action_tracks:(\d+)$/.test(key)),
	Stream.mapEffect((key) => CFKV.getKeyValuePair(key, false), {
		concurrency: 1,
	}),
	Stream.tap(({ key, value }) => persistKeyValuePair(key, value)),
	Stream.runForEach(Console.log),
)

NodeRuntime.runMain(
	program.pipe(
		Effect.provide(NodeKeyValueStore.layerFileSystem(OUT_PATH)),
		Effect.provide(NodeFileSystem.layer),
	),
)
