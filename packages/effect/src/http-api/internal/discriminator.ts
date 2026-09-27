import * as InternalRecord from "../../internal/record.ts"
import type * as JsonSchema from "../../JsonSchema.ts"
import * as Predicate from "../../Predicate.ts"

const COMPONENTS_SCHEMAS = "#/components/schemas/"

interface Discriminator {
  readonly propertyName: string
  readonly mapping: Record<string, string>
}

interface ObjectComponent {
  readonly properties: Record<string, unknown>
  readonly required: ReadonlyArray<unknown>
}

/**
 * Adds an OpenAPI `discriminator` to each `oneOf` or `anyOf` whose members are
 * all references to object components that share a required string literal
 * property, and whose literal values each select exactly one member.
 *
 * When several properties qualify, `_tag` is used if it is one of them;
 * otherwise no discriminator is added.
 *
 * A union with one `{ "type": "null" }` member is rewritten to
 * `[{ <objects>, discriminator }, { "type": "null" }]`, the shape that
 * `Schema.NullOr` of a union already produces.
 *
 * @internal
 */
export function addDiscriminators(
  document: JsonSchema.MultiDocument<"openapi-3.1">
): JsonSchema.MultiDocument<"openapi-3.1"> {
  const definitions = document.definitions

  const out: JsonSchema.Definitions = {}
  for (const key of Object.keys(definitions)) {
    InternalRecord.assignProperty(out, key, walk(definitions[key]))
  }
  const [head, ...tail] = document.schemas
  return {
    dialect: document.dialect,
    schemas: [walk(head), ...tail.map(walk)],
    definitions: out
  }

  function walk<A>(node: A): A {
    if (!Predicate.isObject(node)) return node
    const out: Record<string, unknown> = {}
    for (const key of Object.keys(node)) {
      const value = node[key]
      let walked = value
      switch (key) {
        case "$defs":
        case "properties":
        case "patternProperties":
        case "dependentSchemas":
          walked = Predicate.isObject(value) ? mapValues(value, walk) : value
          break
        case "allOf":
        case "anyOf":
        case "oneOf":
        case "prefixItems":
          walked = Array.isArray(value) ? value.map(walk) : value
          break
        case "not":
        case "additionalProperties":
        case "propertyNames":
        case "unevaluatedProperties":
        case "items":
        case "contains":
        case "unevaluatedItems":
        case "if":
        case "then":
        case "else":
        case "contentSchema":
          walked = walk(value)
      }
      InternalRecord.assignProperty(out, key, walked)
    }
    const union = getUnion(out)
    if (union !== undefined && !Object.hasOwn(out, "discriminator")) {
      const discriminator = getDiscriminator(union.members)
      if (discriminator !== undefined) {
        out.discriminator = discriminator
      } else {
        // `Schema.Union([A, B, Schema.Null])` puts `null` next to the objects. Nest the
        // objects in their own union, as `Schema.NullOr(Schema.Union([A, B]))` does, so
        // the discriminator describes only object members.
        const nullable = splitNull(union.members)
        const inner = nullable === undefined ? undefined : getDiscriminator(nullable.objects)
        if (nullable !== undefined && inner !== undefined) {
          InternalRecord.assignProperty(out, union.keyword, [
            { [union.keyword]: nullable.objects, discriminator: inner },
            nullable.nullMember
          ])
        }
      }
    }
    return out as A
  }

  function getDiscriminator(members: ReadonlyArray<unknown>): Discriminator | undefined {
    if (members.length < 2) return undefined
    const refs: Array<string> = []
    const objects: Array<ObjectComponent> = []
    for (const member of members) {
      if (!Predicate.isObject(member) || typeof member.$ref !== "string") return undefined
      const resolved = resolve(member)
      if (
        resolved === undefined || !Predicate.isObject(resolved.properties) || !Array.isArray(resolved.required)
      ) {
        return undefined
      }
      refs.push(member.$ref)
      objects.push({ properties: resolved.properties, required: resolved.required })
    }

    const candidates: Array<Discriminator> = []
    for (const propertyName of objects[0].required) {
      if (typeof propertyName !== "string") continue
      const mapping = getMapping(propertyName, refs, objects)
      if (mapping !== undefined) candidates.push({ propertyName, mapping })
    }
    return candidates.find((candidate) => candidate.propertyName === "_tag") ??
      (candidates.length === 1 ? candidates[0] : undefined)
  }

  function getMapping(
    propertyName: string,
    refs: ReadonlyArray<string>,
    objects: ReadonlyArray<ObjectComponent>
  ): Record<string, string> | undefined {
    const mapping: Record<string, string> = {}
    for (let index = 0; index < objects.length; index++) {
      const { properties, required } = objects[index]
      if (!required.includes(propertyName) || !Object.hasOwn(properties, propertyName)) return undefined
      const values = getStringLiterals(properties[propertyName])
      if (values === undefined) return undefined
      for (const value of values) {
        if (Object.hasOwn(mapping, value) && mapping[value] !== refs[index]) return undefined
        InternalRecord.assignProperty(mapping, value, refs[index])
      }
    }
    return mapping
  }

  function getStringLiterals(schema: unknown): ReadonlyArray<string> | undefined {
    const resolved = resolve(schema)
    if (resolved === undefined) return undefined
    if (typeof resolved.const === "string") return [resolved.const]
    const values = resolved.enum
    return Array.isArray(values) && values.length > 0 && values.every(Predicate.isString) ? values : undefined
  }

  function resolve(schema: unknown): Record<string, unknown> | undefined {
    const seen = new Set<string>()
    let current = schema
    while (Predicate.isObject(current) && typeof current.$ref === "string") {
      const key = getComponentKey(current.$ref)
      if (key === undefined || seen.has(key) || !Object.hasOwn(definitions, key)) return undefined
      seen.add(key)
      current = definitions[key]
    }
    return Predicate.isObject(current) ? current : undefined
  }
}

function getUnion(
  schema: Record<string, unknown>
): { readonly keyword: "oneOf" | "anyOf"; readonly members: ReadonlyArray<unknown> } | undefined {
  const oneOf = Array.isArray(schema.oneOf) ? schema.oneOf : undefined
  const anyOf = Array.isArray(schema.anyOf) ? schema.anyOf : undefined
  // A discriminator cannot say which keyword it applies to when both are present.
  if (oneOf !== undefined && anyOf !== undefined) return undefined
  if (oneOf !== undefined) return { keyword: "oneOf", members: oneOf }
  if (anyOf !== undefined) return { keyword: "anyOf", members: anyOf }
  return undefined
}

function splitNull(
  members: ReadonlyArray<unknown>
): { readonly objects: ReadonlyArray<unknown>; readonly nullMember: unknown } | undefined {
  const objects = members.filter((member) => !isNullSchema(member))
  return members.length - objects.length === 1
    ? { objects, nullMember: members.find(isNullSchema) }
    : undefined
}

function isNullSchema(schema: unknown): boolean {
  return Predicate.isObject(schema) && schema.type === "null" && Object.keys(schema).length === 1
}

function getComponentKey(ref: string): string | undefined {
  if (!ref.startsWith(COMPONENTS_SCHEMAS)) return undefined
  const key = ref.slice(COMPONENTS_SCHEMAS.length)
  return key.length > 0 && !key.includes("/") ? key : undefined
}

function mapValues(
  record: Record<string, unknown>,
  f: (value: unknown) => unknown
): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  for (const key of Object.keys(record)) {
    InternalRecord.assignProperty(out, key, f(record[key]))
  }
  return out
}
