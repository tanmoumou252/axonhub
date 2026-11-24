// 职责: 锁定端点弹窗里新建模型协议表单的 fakeStream 复位与编辑预填行为，
//         防止关闭弹窗或删除正在编辑项时把上一次的覆盖值带进下一次新建。
// 关联文件:
//   frontend/src/features/channels/components/channels-endpoints-dialog.tsx
//   frontend/src/features/channels/data/schema.ts
// 注意事项:
//   复位值必须是 'inherit'（表单哨兵），不是 undefined，否则 Select 会失去受控值。
//   编辑预填不能被改成复位，否则打开编辑会清空已有覆盖。
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const dialogPath = resolve(here, 'channels-endpoints-dialog.tsx');
const source = readFileSync(dialogPath, 'utf8');

function between(startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  assert.notEqual(start, -1, `missing start marker ${startMarker}`);
  const end = source.indexOf(endMarker, start);
  assert.notEqual(end, -1, `missing end marker ${endMarker}`);
  return source.slice(start, end);
}

const RESET = "setNewProtocolFakeStream('inherit')";

test('opening the dialog resets the fake stream form field', () => {
  const block = between('const justOpened = open && !wasOpenRef.current', '}, [open, channel,');
  assert.equal(block.split(RESET).length - 1, 1, 'the open-reset effect must reset fakeStream exactly once');
});

test('saving a protocol resets the field so the next add starts clean', () => {
  const block = between('const handleSaveModelProtocol', ', [editingProtocolModel, newProtocolModel');
  assert.equal(block.split(RESET).length - 1, 1, 'handleSaveModelProtocol must reset fakeStream once');
});

test('cancelling an edit resets the field', () => {
  const block = between('const handleCancelEditModelProtocol', 'const handleRemoveModelProtocol');
  assert.equal(block.split(RESET).length - 1, 1, 'handleCancelEditModelProtocol must reset fakeStream once');
});

test('removing the protocol being edited resets the field', () => {
  const block = between('const handleRemoveModelProtocol', 'const handleToggleModelProtocol');
  assert.equal(block.split(RESET).length - 1, 1, 'the editing-branch of handleRemoveModelProtocol must reset fakeStream once');
});

test('starting an edit prefills instead of resetting', () => {
  const block = between('const handleStartEditModelProtocol', 'const handleCancelEditModelProtocol');
  assert.match(block, /setNewProtocolFakeStream\(protocol\.fakeStream \?\? 'inherit'\)/,
    'edit must prefill the stored override, falling back to inherit');
  assert.equal(block.split(RESET).length - 1, 0, 'the edit handler must not blind-reset the field');
});
