// 职责: 锁定渠道 schema 中 fakeStream 的可空性分流——per-model 覆盖必须 nullish，
//         渠道级策略必须 optional，二者同时成立才既不误拒全量读取也不掩盖配置缺失。
// 关联文件:
//   frontend/src/features/channels/data/schema.ts
//   internal/server/gql/ent.resolvers.go
// 注意事项:
//   后端把未配置的 per-model 覆盖封送为 GraphQL null，optional() 会拒绝整条 modelProtocol。
//   渠道级 fakeStream 由 resolver 归一化为 off，永不返回 null，因此不能被改成 nullish。
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const schemaPath = resolve(here, 'schema.ts');
const source = readFileSync(schemaPath, 'utf8');

function between(startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  assert.notEqual(start, -1, `missing start marker ${startMarker}`);
  const end = source.indexOf(endMarker, start);
  assert.notEqual(end, -1, `missing end marker ${endMarker}`);
  return source.slice(start, end);
}

test('per-model fakeStream override accepts null from the backend', () => {
  const block = between('export const modelProtocolSchema', 'export type ModelProtocol');
  const field = /fakeStream:\s*z\.enum\(\[[^\]]*\]\)(\.\w+\(\))*/.exec(block);
  assert.ok(field, 'modelProtocolSchema must declare a fakeStream enum field');
  assert.match(field[0], /\.nullish\(\)/, 'per-model fakeStream must be nullish so a GraphQL null does not fail the parse');
});

test('channel-level fakeStream stays optional because the resolver normalizes it', () => {
  const block = between('export const channelPoliciesSchema', 'export type ChannelPolicies');
  assert.match(block, /fakeStream:\s*fakeStreamModeSchema\.optional\(\)/,
    'channel-level fakeStream must not be nullish: the resolver always yields a concrete mode');
});
