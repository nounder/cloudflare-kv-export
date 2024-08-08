# Cloudflare KV Export (Express)

Super fast Cloudflare KV exporter. Dump your KV data in minutes instead of hours.

Normally, Cloudflare API rate-limits to 4 requests per second. Meaning that dumping 100k kv pairs will take hours.

This tool allows you to download 1000s of kv pairs per second thanks to auxiliary Cloudflare worker (found in `worker`) that helps in downloading the data.


## Setup

Use node, bun, or deno. For node you have to use `--strip-types` flag or `ts-node`

1. `bun install`
2. `bun index.ts`


## Usage

There are two ways to use this tool: fast & slow.

### Fast (with worker)

First, you have to deploy auxiliary worker: `cd worker && bunx wrangler deploy`

After it's deployed pass worker URL to `--worker-url` option like so:

```sh
bun index.ts --worker-url $WORKER_URL
```

When you're done remember to delete the worker.


### Slow (without worker)

Run the script without any options, like so:

```sh
bun index.ts
```

This will use Cloudflare API with stellar speed of ~4 keys per second.


## Customizations

You can use pass custom `--outdir` to specify directory where to dump KV data.

You can also adjust performance parameters, like number of concurrent connections with `--worker-rps` and `--worker-chunk`. Pass `--help` for more details.


## Output

You will find KV data in directory specified by `--outdir` (default `dump/`.) Each file is individual key-value pair. Keys are url-encoded in file name.


