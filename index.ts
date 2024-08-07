import { FileSystem, KeyValueStore, Path } from "@effect/platform"
import {
	NodeContext,
	NodeFileSystem,
	NodeKeyValueStore,
	NodeRuntime,
} from "@effect/platform-node"
import {
	Chunk,
	Console,
	Effect,
	Metric,
	Option,
	pipe,
	Schedule,
	Stream,
} from "effect"
import * as CFKV from "./cfkv"
import { typeson } from "./utils"
import { Command, Options } from "@effect/cli"

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

/**
 * Dumps KV data using helper worker instead of using heavy rate-limited Cloudflare API.
 */
const dumpKVDataWithWorker = (args: { workerUrl: string }) =>
	pipe(
		CFKV.streamKeys(),
		Stream.filter(key => /^flow:(\w+):action_tracks:(\d+)$/.test(key)),
		Stream.rechunk(64),
		Stream.throttle({
			cost: Chunk.size,
			duration: "1 second",
			units: 1000,
		}),
		Stream.tap(_ => keysCounter(Effect.succeedNone)),
		Stream.rechunk(128),
		Stream.mapChunksEffect(chunk =>
			CFKV.getKeyValuePairsWithWorker(
				Chunk.toReadonlyArray(chunk),
				args.workerUrl,
			).pipe(Effect.andThen(Chunk.unsafeFromArray)),
		),
		Stream.mapEffect(([k, v]) => persistKeyValuePair(k, v), {
			concurrency: "unbounded",
		}),
		Stream.tap(_ => processedKeysCounter(Effect.succeedNone)),
		Stream.runDrain,
	)

/**
 * Dumps KV using Cloudflare API.
 * Global rate limit is 1200 requests per 5 minutes
 * We limit it to 1000 requests per 5 minutes (or 4 rps) to give some
 * wiggling room for other services to run.
 * We also make chunks smaller for Stream.throttle
 * See: https://developers.cloudflare.com/fundamentals/api/reference/limits/
 */
const dumpKVData = pipe(
	CFKV.streamKeys(),
	Stream.filter(key => /^flow:(\w+):action_tracks:(\d+)$/.test(key)),
	Stream.rechunk(10),
	Stream.throttle({
		cost: Chunk.size,
		duration: "1 second",
		units: 4,
	}),
	Stream.tap(_ => keysCounter(Effect.succeedNone)),
	Stream.mapEffect(
		key =>
			pipe(
				CFKV.getKeyValuePair(key, false),
				Effect.map(v => persistKeyValuePair(v.key, v.value)),
			),
		{ concurrency: "unbounded" },
	),
	Stream.tap(v => processedKeysCounter(v)),
	// drain the stream and ignore the output and convert to an Effect
	Stream.runDrain,
)

const listFileSystemKeyValueStore = (path: string) =>
	Effect.gen(function* () {
		const fs = yield* FileSystem.FileSystem

		const keys = yield* fs.readDirectory(path, { recursive: true })
		const resolvedKeys = keys.map(f => decodeURIComponent(f))

		resolvedKeys.sort()

		return Chunk.unsafeFromArray(resolvedKeys)
	})

const dumpCommand = Command.make(
	"dump",
	{
		outdir: Options.directory("outdir").pipe(Options.withDefault("dump/")),
		worker: Options.text("worker").pipe(
			Options.optional,
			Options.withDescription("URL to a auxilary worker for faster exporting"),
		),
	},
	args => {
		return Effect.gen(function* () {
			const path = yield* Path.Path

			yield* Effect.fork(scheduledLogging)

			const workerUrl = Option.getOrNull(args.worker)
			const dumpEffect = workerUrl
				? dumpKVDataWithWorker({ workerUrl })
				: dumpKVData

			if (!workerUrl) {
				yield* Console.log(
					"Using Cloudflare API to dump data. This will be slow. Consider passing --worker",
				)
			} else {
				yield* Console.log("Using worker to fetch data:")
				yield* Console.log("Worker URL:\t" + workerUrl)
			}

			yield* Console.log("Dump directory\t" + path.resolve(args.outdir) + "\n")

			yield* pipe(
				dumpEffect,

				Effect.provide(NodeKeyValueStore.layerFileSystem(args.outdir)),
			)

			yield* Console.log("Done!")
		})
	},
)

const cli = Command.run(dumpCommand, {
	name: "Cloudflare KV export",
	version: "v0.1.0",
})

pipe(
	cli(process.argv),
	Effect.provide(Path.layer),
	Effect.provide(NodeContext.layer),
	Effect.provide(NodeFileSystem.layer),
	NodeRuntime.runMain,
)
