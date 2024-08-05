import { Typeson } from "typeson"
import { builtin as typesonBuiltin } from "typeson-registry"
import {
	HttpClient,
	HttpClientRequest,
	HttpClientResponse,
	KeyValueStore,
} from "@effect/platform"
import {
	NodeFileSystem,
	NodeKeyValueStore,
	NodeRuntime,
} from "@effect/platform-node"
import { Chunk, Console, Effect, Option, pipe, Stream } from "effect"

const typeson = new Typeson().register([typesonBuiltin])

const { CF_ACCOUNT_ID, CF_KV_NAMESPACE_ID } = Bun.env

const OUT_PATH = __dirname + "/out"

const CF_KV_URL = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/storage/kv/namespaces/${CF_KV_NAMESPACE_ID}`

const requestCfKvApi = (url: string, { params = {} } = {}) =>
	pipe(
		HttpClientRequest.get(new URL(url, CF_KV_URL + "/")),
		HttpClientRequest.bearerToken(Bun.env.CF_API_KEY as string),
		HttpClientRequest.setUrlParams(params),
		HttpClient.fetchOk,
	)

const listKeys = ({ cursor = "", limit = 1000 } = {}) =>
	pipe(
		requestCfKvApi(`keys`, { params: { cursor, limit } }),
		HttpClientResponse.json,
		Effect.map((r: any) => ({
			keys: Chunk.fromIterable(r.result.map((v: any) => v.name) as string[]),
			cursor: (r.result_info.cursor as string) || null,
		})),
	)

const streamKeys = ({ cursor = "" } = {}) =>
	Stream.paginateChunkEffect(cursor, (curCursor) =>
		listKeys({ cursor: curCursor }).pipe(
			Effect.andThen((page) => [
				page.keys, //
				Option.fromNullable(page.cursor),
			]),
		),
	)

const getKeyValue = (key: string) =>
	pipe(
		requestCfKvApi(`values/${encodeURIComponent(key)}`), //
	)

const getKeyMetadata = (key: string) =>
	pipe(
		requestCfKvApi(`metadata/${encodeURIComponent(key)}`),
		HttpClientResponse.json,
		Effect.map((r: any) => r.result as Record<string, any>),
	)

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

const program = streamKeys().pipe(
	Stream.filter((key) => /^flow:(\w+):action_tracks:(\d+)$/.test(key)),
	Stream.mapEffect(
		(key) =>
			Effect.all(
				{
					key: Effect.succeed(key),
					value: getKeyValue(key).pipe(
						HttpClientResponse.json, //
					),
				},
				{ concurrency: "unbounded" },
			),

		{ concurrency: 1 },
	),
	Stream.tap(({ key, value }) => persistKeyValuePair(key, value)),
	Stream.runForEach(Console.log),
)

NodeRuntime.runMain(
	program.pipe(
		Effect.provide(NodeKeyValueStore.layerFileSystem(OUT_PATH)),
		Effect.provide(NodeFileSystem.layer),
	),
)
