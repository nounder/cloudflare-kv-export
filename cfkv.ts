import {
	HttpClient,
	HttpClientRequest,
	HttpClientResponse,
} from "@effect/platform"
import { Chunk, Effect, Option, pipe, Stream } from "effect"

const { CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_KV_NAMESPACE_ID } = process.env

const CF_KV_URL = `https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/storage/kv/namespaces/${CLOUDFLARE_KV_NAMESPACE_ID}`

const requestCfKvApi = (url: string, { params = {} } = {}) =>
	pipe(
		HttpClientRequest.get(new URL(url, CF_KV_URL + "/")),
		HttpClientRequest.bearerToken(Bun.env.CF_API_KEY as string),
		HttpClientRequest.setUrlParams(params),
		HttpClient.fetchOk,
	)

export const listKeys = ({ cursor = "", limit = 1000 } = {}) =>
	pipe(
		requestCfKvApi(`keys`, { params: { cursor, limit } }),
		HttpClientResponse.json,
		Effect.map((r: any) => ({
			keys: Chunk.fromIterable(r.result.map((v: any) => v.name) as string[]),
			cursor: (r.result_info.cursor as string) || null,
		})),
	)

export const streamKeys = ({ cursor = "", limit = 1000 } = {}) =>
	Stream.paginateChunkEffect(cursor, curCursor =>
		pipe(
			listKeys({ cursor: curCursor, limit }),
			Effect.andThen(page => [
				page.keys, //
				Option.fromNullable(page.cursor),
			]),
		),
	)

export const getKeyValue = (key: string) =>
	pipe(
		requestCfKvApi(`values/${encodeURIComponent(key)}`), //
	)

export const getKeyMetadata = (key: string) =>
	pipe(
		requestCfKvApi(`metadata/${encodeURIComponent(key)}`),
		HttpClientResponse.json,
		Effect.map((r: any) => r.result as Record<string, any>),
	)

export const getKeyValuePair = (key: string, withMetadata = false) =>
	Effect.all(
		{
			key: Effect.succeed(key),
			value: getKeyValue(key).pipe(
				HttpClientResponse.json, //
			),
			metadata: withMetadata ? getKeyMetadata(key) : Effect.succeed(null),
		},
		{ concurrency: "unbounded" },
	)
