import { Typeson } from "typeson"
import { builtin as typesonBuiltin } from "typeson-registry"

const typeson = new Typeson().register([typesonBuiltin])

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const { keys, type = "json"} = await request.json() as any

		const kvPairs = {}

		for (const key of keys) {
			const value = await env.KV.get(key, type)

			kvPairs[key] = value
		}

		return new Response(JSON.stringify(kvPairs), {
			headers: { 'content-type': 'application/json' },
		})

	},
} satisfies ExportedHandler<Env>;
