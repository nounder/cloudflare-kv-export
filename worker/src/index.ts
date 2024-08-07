export default {
	/**
	 * Returns key-value pairs for the given keys decoded for specific type.
	 */
	async fetch(request, env, ctx): Promise<Response> {
		const { keys, type = "json"} = await request.json() as any


		const values = await Promise.all(keys.map(key => env.KV.get(key, type)))
		const pairs = Array.from(values, (value, i) => [keys[i], value])

		return new Response(JSON.stringify(pairs), {
			headers: { 'content-type': 'application/json' },
		})

	},
} satisfies ExportedHandler<Env>;
