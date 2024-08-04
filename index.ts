import { DevTools } from "@effect/experimental"
import {
	HttpClient,
	HttpClientRequest,
	HttpClientResponse,
} from "@effect/platform"
import { NodeRuntime, NodeSocket } from "@effect/platform-node"
import { Chunk, Console, Effect, Layer, Option, pipe, Stream } from "effect"

const { CF_ACCOUNT_ID, CF_KV_NAMESPACE_ID } = Bun.env

const requestKVData = ({ cursor = "", limit = 10 } = {}) =>
	pipe(
		HttpClientRequest.get(
			`https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/storage/kv/namespaces/${CF_KV_NAMESPACE_ID}/keys`,
			{
				urlParams: {
					limit,
					cursor: cursor || undefined,
				},
			},
		),
		HttpClientRequest.bearerToken(Bun.env.CF_API_KEY as string),
		HttpClient.fetchOk,
		HttpClientResponse.json,
		Effect.map((r: any) => ({
			keys: Chunk.fromIterable(r.result.map((v: any) => v.name as string)),
			cursor: (r.result_info.cursor as string) || null,
		})),
	)

//const streamKVData = () => Stream.unfoldEffect("", requestKVData);

const streamKVData = () =>
	Stream.paginateChunkEffect("", (cursor) =>
		requestKVData({ cursor }).pipe(
			Effect.andThen((page) => [page.keys, Option.fromNullable(page.cursor)]),
		),
	)

const program = Stream.runCollect(
	//
	streamKVData().pipe(Stream.take(5)),
).pipe(Effect.tap(Console.log))

if (false) {
	const DevToolsLive = DevTools.layerWebSocket().pipe(
		Layer.provide(NodeSocket.layerWebSocketConstructor),
	)

	program.pipe(Effect.provide(DevToolsLive), NodeRuntime.runMain)
} else {
	Effect.runFork(
		Effect.tapErrorTag(program, "ResponseError", (e) =>
			e.response.text.pipe(Effect.tap((text) => Console.error(text))),
		),
	)
}
