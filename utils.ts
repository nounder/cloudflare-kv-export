import { Typeson } from "typeson"
import { builtin as typesonBuiltin } from "typeson-registry"

export const typeson = new Typeson().register([typesonBuiltin])
