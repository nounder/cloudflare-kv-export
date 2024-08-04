import { DevTools } from "@effect/experimental"
import {
	HttpClient,
	HttpClientRequest,
	HttpClientResponse,
} from "@effect/platform"
import { NodeRuntime, NodeSocket } from "@effect/platform-node"
import { Console, Effect, Layer, Option, pipe, Stream } from "effect"

const { CF_ACCOUNT_ID, CF_KV_NAMESPACE_ID } = Bun.env

const requestKVData = (cursor?: string) =>
	pipe(
		HttpClientRequest.get(
			`https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/storage/kv/namespaces/${CF_KV_NAMESPACE_ID}/keys`,
			{
				urlParams: {
					limit: 10,
					cursor: cursor || undefined,
				},
			},
		),
		HttpClientRequest.bearerToken(Bun.env.CF_API_KEY as string),
		HttpClient.fetchOk,
		Effect.tap((res) => Console.log(res)),
		HttpClientResponse.json,
		Effect.tap((res) => Console.log(res)),
	)

//const streamKVData = () => Stream.unfoldEffect("", requestKVData);

const streamKVData = () =>
	Stream.unfoldEffect("", (cursor) =>
		requestKVData(cursor).pipe(
			Effect.map((res: any) => {
				if (res.result_info.cursor) {
					return Option.some([res.result, res.result_info.cursor])
				} else {
					return Option.none()
				}
			}),
		),
	)

const program = Stream.runCollect(
	//
	streamKVData().pipe(Stream.take(5)),
)

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
