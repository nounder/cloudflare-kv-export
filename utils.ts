import { FileSystem } from "@effect/platform"
import { Chunk, Effect } from "effect"
import { Typeson } from "typeson"
import { builtin as typesonBuiltin } from "typeson-registry"

export const typeson = new Typeson().register([typesonBuiltin])

/**
 * Lists all keys in the file system key-value store
 * stored with KeyValueStore service.
 *
 * Keys order is undefined.
 */
export const listFileSystemKeyValueStore = (path: string) =>
	Effect.gen(function* () {
		const fs = yield* FileSystem.FileSystem

		const keys = yield* fs.readDirectory(path, { recursive: true })
		const resolvedKeys = keys.map(f => decodeURIComponent(f))

		return resolvedKeys as string[]
	})
