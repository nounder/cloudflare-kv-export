import { Command, Options } from "@effect/cli"
import { FileSystem, Path } from "@effect/platform"
import { NodeContext, NodeFileSystem, NodeRuntime } from "@effect/platform-node"
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

const persistKeyValuePair = (
	key: string,
	value: ArrayBuffer,
	basePath: string,
) =>
	Effect.gen(function* () {
		const path = yield* Path.Path
		const fs = yield* FileSystem.FileSystem

		yield* Effect.all([
			fs.writeFile(
				path.resolve(basePath, encodeURIComponent(key)),
				new Uint8Array(value),
			),
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
const dumpKVDataWithWorker = (args: {
	keyPattern?: RegExp
	workerUrl: string
	rps: number
	chunkSize: number
	outdir: string
}) =>
	pipe(
		CFKV.streamKeys(),
		args.keyPattern ? Stream.filter(key => args.keyPattern!.test(key)) : v => v,
		Stream.rechunk(64),
		Stream.throttle({
			cost: Chunk.size,
			duration: "1 second",
			units: args.rps,
		}),
		Stream.tap(_ => keysCounter(Effect.succeedNone)),
		Stream.rechunk(args.chunkSize),
		Stream.mapChunksEffect(chunk =>
			CFKV.getKeyValuePairsWithWorker(
				Chunk.toReadonlyArray(chunk),
				args.workerUrl,
			).pipe(Effect.andThen(Chunk.unsafeFromArray)),
		),
		Stream.mapEffect(([k, v]) => persistKeyValuePair(k, v, args.outdir), {
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
const dumpKVData = (args: { keyPattern?: RegExp; outdir: string }) =>
	pipe(
		CFKV.streamKeys(),
		args.keyPattern ? Stream.filter(key => args.keyPattern!.test(key)) : v => v,
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
					Effect.map(v => persistKeyValuePair(v.key, v.value, args.outdir)),
				),
			{ concurrency: "unbounded" },
		),
		Stream.tap(v => processedKeysCounter(v)),
		// drain the stream and ignore the output and convert to an Effect
		Stream.runDrain,
	)

const dumpCommand = Command.make(
	"dump",
	{
		outdir: Options.directory("outdir").pipe(Options.withDefault("dump/")),
		workerUrl: Options.text("worker-url").pipe(
			Options.optional,
			Options.withDescription("URL to a auxilary worker for faster exporting"),
		),
		workerRps: Options.integer("worker-rps").pipe(
			Options.withDescription("Maximum number of requests per second"),
			Options.withDefault(100),
		),
		workerChunkSize: Options.integer("worker-chunk").pipe(
			Options.withDescription("Number of keys to fetch in a single request"),
			Options.withDefault(32),
		),
		keyPattern: Options.text("key-pattern").pipe(
			Options.optional,
			Options.withDescription("Pattern to filter keys"),
		),
	},
	args => {
		return Effect.gen(function* () {
			const path = yield* Path.Path

			const keyRegexp = Option.map(args.keyPattern, v => new RegExp(v))

			const workerUrl = Option.getOrNull(args.workerUrl)
			const dumpEffect = workerUrl
				? dumpKVDataWithWorker({
						workerUrl,
						keyPattern: Option.getOrUndefined(keyRegexp),
						rps: args.workerRps,
						chunkSize: args.workerChunkSize,
						outdir: args.outdir,
				  })
				: dumpKVData({
						keyPattern: Option.getOrUndefined(keyRegexp),
						outdir: args.outdir,
				  })

			if (!workerUrl) {
				yield* Console.log(
					"Using Cloudflare API to dump data. This will be slow. Consider passing --worker",
				)
			} else {
				yield* Console.log("Using worker to fetch data:")
				yield* Console.log("Worker URL:\t" + workerUrl)
			}

			yield* Console.log("Dump directory\t" + path.resolve(args.outdir) + "\n")

			yield* Effect.fork(scheduledLogging)

			const fs = yield* FileSystem.FileSystem

			yield* fs.makeDirectory(args.outdir, { recursive: true })

			yield* dumpEffect

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
