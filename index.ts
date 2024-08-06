import { Chunk, Console, Effect, Metric, pipe, Schedule, Stream } from "effect"
import * as CFKV from "./cfkv"
import { FileSystem, KeyValueStore, Path } from "@effect/platform"
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

const keysCounter = Metric.counter("keys").pipe(Metric.withConstantInput(1))

const processedKeysCounter = Metric.counter("processedKeys").pipe(
	Metric.withConstantInput(1),
)

const scheduledLogging = pipe(
	Effect.all({
		keysCounter: Metric.value(keysCounter),
		processedKeysCounter: Metric.value(processedKeysCounter),
	}),
	Effect.flatMap(v =>
		Console.log(
			`Keys: count=${v.keysCounter.count} processed=${v.processedKeysCounter.count}`,
		),
	),
	Effect.repeat(Schedule.spaced("2000 millis")),
)

const dumpKVData = pipe(
	CFKV.streamKeys(),
	Stream.filter(key => /^flow:(\w+):action_tracks:(\d+)$/.test(key)),
	Stream.tap(v => keysCounter(Effect.succeedNone)),
	Stream.mapEffect(
		key =>
			pipe(
				CFKV.getKeyValuePair(key, false),
				Effect.map(v => persistKeyValuePair(v.key, v.value)),
			),
		{ concurrency: 100 },
	),
	Stream.tap(v => processedKeysCounter(v)),
	// drain the stream and ignore the output and convert to an Effect
	Stream.runDrain,
)

const listFileSystemKeyValueStore = pipe(
	Effect.gen(function* () {
		const fs = yield* FileSystem.FileSystem

		const keys = yield* fs.readDirectory(OUT_PATH, { recursive: true })
		const resolvedKeys = keys.map(f => decodeURIComponent(f))

		resolvedKeys.sort()

		return Chunk.unsafeFromArray(resolvedKeys)
	}),
)

const loadKvData = pipe(
	listFileSystemKeyValueStore, //
)

const program = Effect.gen(function* () {
	yield* Effect.fork(scheduledLogging)

	yield* dumpKVData
})

NodeRuntime.runMain(
	program.pipe(
		Effect.provide(Path.layer),
		Effect.provide(NodeKeyValueStore.layerFileSystem(OUT_PATH)),
		Effect.provide(NodeFileSystem.layer),
	),
)
