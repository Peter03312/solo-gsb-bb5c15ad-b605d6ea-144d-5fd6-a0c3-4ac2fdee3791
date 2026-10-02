'use strict';
// 断网整包补录端到端 HTTP 测试。
// 启动使用临时 SQLite 文件的真实服务实例（空白库，自建数据），验证：
//   正常补录：整包有序记录（PROCESS 含产出批次/原料输入（批号可未知）/包内引用；CLEAN 全清/部分清洗）
//             -> 返回外部记录号到事件号映射、逐条状态与影响摘要；重算沿现有时间线生效；
//   幂等：相同补录批号 + 相同内容重试返回原结果（idempotent_replay，映射一致，不重复写入事件/批次）；
//   冲突：补录批号相同内容不同 -> 409 BACKFILL_NO_CONFLICT；
//         外部记录号与已入库内容冲突 -> 409 EXT_RECORD_CONFLICT；同号同内容不重复写入（REUSED）；
//   整包回滚：未知设备/原料/批次、空清洗集合、包内时间冲突、与既有事件时间冲突、未来时间、
//             未来/循环/非更早包内引用、空包 -> 全部拒绝，库里不留任何部分事件；
//   影响：补录改变早期时间线后放行失效、标签卷停用、出货单待评估、处置结论需重评；
//   对账：GET /api/backfills 列表、GET /api/backfills/:no 明细（映射、INSERTED/REUSED、影响快照）；
//   兼容：既有单条事件接口请求/响应形状不变；/api/state 事件补录留痕字段为附加项。
//   node scripts/test-backfill.js
// 退出码 0 表示全部通过。

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');

const PORT = '39153';
const BASE = `http://127.0.0.1:${PORT}`;
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'backfill-test-'));
const dbPath = path.join(tmpDir, 'test.db');

let failures = 0;
function check(name, cond, extra) {
  if (cond) console.log(`  ✓ ${name}`);
  else {
    failures += 1;
    console.error(`  ✗ ${name}`, extra !== undefined ? JSON.stringify(extra) : '');
  }
}

async function api(method, urlPath, body) {
  const res = await fetch(BASE + urlPath, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined
  });
  let json = null;
  try { json = await res.json(); } catch (_) { /* 非 JSON */ }
  return { status: res.status, json };
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitReady(proc) {
  for (let i = 0; i < 60; i++) {
    try {
      const res = await fetch(BASE + '/api/state');
      if (res.ok) return;
    } catch (_) { /* 未就绪 */ }
    if (proc.exitCode !== null) throw new Error('服务提前退出');
    await sleep(250);
  }
  throw new Error('服务启动超时');
}

async function main() {
  const proc = spawn(process.execPath, [path.join(__dirname, '..', 'server', 'server.js')], {
    env: { ...process.env, PORT, HOST: '127.0.0.1', DB_PATH: dbPath, SEED_DEMO: 'false' },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  proc.stdout.on('data', () => {});
  proc.stderr.on('data', (d) => process.stderr.write('[server] ' + d));

  try {
    await waitReady(proc);
    console.log('服务已就绪，开始测试');

    const newId = async (url, body) => (await api('POST', url, body)).json.id;
    const MILK = await newId('/api/materials', { code: 'MILK', name: '奶粉', allergens: ['乳'] });
    const PNT = await newId('/api/materials', { code: 'PNT', name: '花生碎', allergens: ['花生'] });
    const SALT = await newId('/api/materials', { code: 'SALT', name: '食用盐', allergens: [] });
    const LINE = await newId('/api/equipment', { code: 'LINE-A', name: '一线' });
    const LINE2 = await newId('/api/equipment', { code: 'LINE-B', name: '二线' });
    const LOT = await newId('/api/material-lots', { material_id: MILK, lot_code: 'L-100' });
    const state = async () => (await api('GET', '/api/state')).json;
    const batchOf = (s, code) => s.batches.find((b) => b.code === code);

    // ---------- 1. 正常整包补录（设备/原料可用编码，批号可未知，包内引用用外部记录号） ----------
    console.log('1) 整包补录 BF-1：P1(乳+花生) -> 部分清洗(只去乳) -> P2(盐，引用 P1)');
    const bf1 = {
      backfill_no: 'BF-001',
      note: '断网 10/01 上午补录',
      records: [
        { ext_record_no: 'EXT-P1', type: 'PROCESS', event_time: '2026-10-01T08:00',
          equipment_code: 'LINE-A', batch_code: 'BF-B1', note: '断网加工 B1',
          material_inputs: [
            { material_id: MILK, lot_id: LOT },
            { material_code: 'PNT' } // 批号未知
          ] },
        { ext_record_no: 'EXT-C1', type: 'CLEAN', event_time: '2026-10-01T09:00',
          equipment_id: LINE, note: '断网期间部分清洗', clean_allergens: ['乳', '乳'] },
        { ext_record_no: 'EXT-P2', type: 'PROCESS', event_time: '2026-10-01T10:00',
          equipment_id: LINE, batch_code: 'BF-B2', note: '断网加工 B2，引用 B1',
          material_inputs: [{ material_id: SALT }],
          ref_record_nos: ['EXT-P1'] }
      ]
    };
    let r = await api('POST', '/api/backfills', bf1);
    check('补录成功 200', r.status === 200, r.json);
    check('record_count=3 / inserted=3 / reused=0',
      r.json.record_count === 3 && r.json.inserted_count === 3 && r.json.reused_count === 0, r.json);
    check('返回外部记录号 -> 事件号映射（3 条）',
      Object.keys(r.json.event_mapping).length === 3 &&
      r.json.event_mapping['EXT-P1'] === r.json.events[0].event_id, r.json);
    const eidP1 = r.json.event_mapping['EXT-P1'];

    let s = await state();
    check('B1 推导含乳+花生',
      JSON.stringify(batchOf(s, 'BF-B1').allergens.slice().sort()) === JSON.stringify(['乳', '花生'].sort()),
      batchOf(s, 'BF-B1') && batchOf(s, 'BF-B1').allergens);
    // B2 经设备残留继承：乳已被部分清洗去除、花生仍残留；引用 B1 又直接带回乳+花生（清洗不截断批次引用）
    check('B2：部分清洗后乳不走设备残留（经引用仍直接带入），含乳+花生',
      JSON.stringify(batchOf(s, 'BF-B2').allergens.slice().sort()) === JSON.stringify(['乳', '花生'].sort()),
      batchOf(s, 'BF-B2') && batchOf(s, 'BF-B2').allergens);
    check('B2 的乳来自批次引用、花生另有设备残留路径',
      batchOf(s, 'BF-B2').paths['乳'].some((p) => p.includes('批次引用:BF-B1')) &&
      batchOf(s, 'BF-B2').paths['花生'].some((p) => p.includes('设备残留:LINE-A')) &&
      !batchOf(s, 'BF-B2').paths['乳'].some((p) => p.includes('设备残留')),
      batchOf(s, 'BF-B2').paths);
    const p2ev = s.events.find((e) => e.ext_record_no === 'EXT-P2');
    check('state 事件附补录留痕字段（ext_record_no/backfill_no/backfilled）',
      p2ev.backfilled === true && p2ev.backfill_no === 'BF-001', p2ev);
    check('state 普通字段保持兼容（clean_scope_full 等仍在）',
      s.events.find((e) => e.ext_record_no === 'EXT-C1').clean_scope_full === false);

    // ---------- 2. 幂等重放：同号同内容重试 ----------
    console.log('2) 相同补录批号与内容重试 -> 原结果（不重复写入）');
    const beforeCount = (await state()).events.length;
    r = await api('POST', '/api/backfills', bf1);
    check('重试 200 且 idempotent_replay=true',
      r.status === 200 && r.json.idempotent_replay === true, r.json);
    check('映射与首次一致',
      r.json.event_mapping['EXT-P1'] === eidP1 &&
      r.json.event_mapping['EXT-C1'] != null && r.json.event_mapping['EXT-P2'] != null, r.json);
    s = await state();
    check('事件数不变（不重复写入）', s.events.length === beforeCount,
      { now: s.events.length, before: beforeCount });
    check('批次未重复', s.batches.filter((b) => b.code === 'BF-B1').length === 1);

    // 同号但内容不同（保持引用合法：仅改动清洗记录备注）-> 内容哈希不同
    const bf1Changed2 = {
      backfill_no: 'BF-001',
      note: '断网 10/01 上午补录',
      records: bf1.records.map((rec) =>
        rec.ext_record_no === 'EXT-C1' ? { ...rec, note: '备注被修改' } : rec)
    };
    r = await api('POST', '/api/backfills', bf1Changed2);
    check('同号但内容不同 -> 409 BACKFILL_NO_CONFLICT',
      r.status === 409 && r.json.code === 'BACKFILL_NO_CONFLICT', r.json);

    // ---------- 3. 批号相同内容不同 ----------
    console.log('3) 补录批号相同、内容不同 -> 409 整包拒绝');
    const bf1Changed = JSON.parse(JSON.stringify(bf1));
    bf1Changed.records[0].note = '改了备注';
    r = await api('POST', '/api/backfills', bf1Changed);
    check('内容差异（备注） -> 409 BACKFILL_NO_CONFLICT',
      r.status === 409 && r.json.code === 'BACKFILL_NO_CONFLICT', r.json);

    // ---------- 4. 外部记录号冲突 / 同号同内容复用 ----------
    console.log('4) 外部记录号：同号内容不同整包拒绝；同号同内容复用（REUSED）');
    const conflictPkg = {
      backfill_no: 'BF-002',
      records: [
        { ext_record_no: 'EXT-P1', type: 'PROCESS', event_time: '2026-10-01T08:00',
          equipment_id: LINE, batch_code: 'BF-OTHER',
          material_inputs: [{ material_id: PNT }] } // 内容不同
      ]
    };
    r = await api('POST', '/api/backfills', conflictPkg);
    check('外部记录号内容冲突 -> 409 EXT_RECORD_CONFLICT',
      r.status === 409 && r.json.code === 'EXT_RECORD_CONFLICT', r.json);

    // 同号同内容 + 一条新记录：复用既有事件，不重复写入
    const reusePkg = {
      backfill_no: 'BF-003',
      note: '跨包重传：P1 已入库，本包仅新增 P3',
      records: [
        { ext_record_no: 'EXT-P1', type: 'PROCESS', event_time: '2026-10-01T08:00',
          equipment_id: LINE, batch_code: 'BF-B1', note: '断网加工 B1',
          material_inputs: [
            { material_id: MILK, lot_id: LOT },
            { material_code: 'PNT' }
          ] },
        { ext_record_no: 'EXT-P3', type: 'PROCESS', event_time: '2026-10-01T11:00',
          equipment_id: LINE, batch_code: 'BF-B3', note: '盐批，引用既有 B2（按编码）',
          material_inputs: [{ material_code: 'SALT' }],
          ref_batch_codes: ['BF-B2'] }
      ]
    };
    r = await api('POST', '/api/backfills', reusePkg);
    check('同号同内容复用 200（reused=1, inserted=1）',
      r.status === 200 && r.json.reused_count === 1 && r.json.inserted_count === 1, r.json);
    check('复用映射指向原事件号',
      r.json.event_mapping['EXT-P1'] === eidP1 &&
      r.json.events.find((e) => e.ext_record_no === 'EXT-P1').status === 'REUSED' &&
      r.json.events.find((e) => e.ext_record_no === 'EXT-P3').status === 'INSERTED', r.json);
    s = await state();
    check('B1 仍只有一个批次/事件（未重复写入）',
      s.batches.filter((x) => x.code === 'BF-B1').length === 1 &&
      s.events.filter((x) => x.ext_record_no === 'EXT-P1').length === 1);
    check('B3 引用 B2：乳+花生经批次引用带入（设备残留花生亦在）',
      JSON.stringify(batchOf(s, 'BF-B3').allergens.slice().sort()) === JSON.stringify(['乳', '花生'].sort()),
      batchOf(s, 'BF-B3') && batchOf(s, 'BF-B3').allergens);

    // 跨包按外部记录号引用更早产出（EXT-P3 已由 BF-003 入库于 11:00）
    const bf4b = {
      backfill_no: 'BF-004B',
      records: [
        { ext_record_no: 'EXT-P4', type: 'PROCESS', event_time: '2026-10-01T11:30',
          equipment_id: LINE, batch_code: 'BF-B4',
          material_inputs: [{ material_code: 'SALT' }],
          ref_record_nos: ['EXT-P3'] }
      ]
    };
    r = await api('POST', '/api/backfills', bf4b);
    check('跨包 ref_record_nos 引用更早产出成功', r.status === 200, r.json);

    // 跨包引用未来时间的已入库记录 -> 拒绝
    const bf4c = {
      backfill_no: 'BF-004C',
      records: [
        { ext_record_no: 'EXT-P5', type: 'PROCESS', event_time: '2026-10-01T07:30',
          equipment_id: LINE, batch_code: 'BF-B5',
          material_inputs: [{ material_code: 'SALT' }],
          ref_record_nos: ['EXT-P3'] } // 11:00 晚于 07:30
      ]
    };
    r = await api('POST', '/api/backfills', bf4c);
    check('跨包引用未来产出 -> 400 INVALID_RECORD_REF 且整包回滚',
      r.status === 400 && r.json.code === 'INVALID_RECORD_REF', r.json);
    s = await state();
    check('回滚后无 BF-B5', !s.batches.some((b) => b.code === 'BF-B5'));

    // ---------- 5. 整包回滚：各种非法包不留部分事件 ----------
    console.log('5) 非法补录整包回滚（统计事件数前后一致）');
    const rejectAndRollback = async (name, pkg, expectCode) => {
      const before = (await state()).events.length;
      const batchesBefore = (await state()).batches.length;
      const rr = await api('POST', '/api/backfills', pkg);
      const after = (await state()).events.length;
      const batchesAfter = (await state()).batches.length;
      check(name,
        rr.status === 400 && (!expectCode || rr.json.code === expectCode) &&
        after === before && batchesAfter === batchesBefore,
        { status: rr.status, code: rr.json.code, detail: rr.json.details });
    };

    await rejectAndRollback('空包 -> EMPTY_PACKAGE', { backfill_no: 'BF-E1', records: [] }, 'EMPTY_PACKAGE');
    await rejectAndRollback('空清洗集合 -> EMPTY_CLEAN_SCOPE', {
      backfill_no: 'BF-E2',
      records: [{ ext_record_no: 'X1', type: 'CLEAN', event_time: '2026-10-01T07:00',
        equipment_id: LINE, clean_allergens: [] }]
    }, 'EMPTY_CLEAN_SCOPE');
    await rejectAndRollback('未知设备 -> UNKNOWN_EQUIPMENT', {
      backfill_no: 'BF-E3',
      records: [{ ext_record_no: 'X2', type: 'CLEAN', event_time: '2026-10-01T07:00',
        equipment_id: 9999 }]
    }, 'UNKNOWN_EQUIPMENT');
    await rejectAndRollback('未知原料 -> UNKNOWN_MATERIAL', {
      backfill_no: 'BF-E4',
      records: [{ ext_record_no: 'X3', type: 'PROCESS', event_time: '2026-10-01T07:00',
        equipment_id: LINE, batch_code: 'BX1',
        material_inputs: [{ material_id: 8888 }] }]
    }, 'UNKNOWN_MATERIAL');
    await rejectAndRollback('未知供应批号 -> LOT_NOT_FOUND（沿用批号错误约定）', {
      backfill_no: 'BF-E5',
      records: [{ ext_record_no: 'X4', type: 'PROCESS', event_time: '2026-10-01T07:00',
        equipment_id: LINE, batch_code: 'BX2',
        material_inputs: [{ material_id: MILK, lot_id: 7777 }] }]
    }, 'LOT_NOT_FOUND');
    await rejectAndRollback('引用不存在的批次 -> UNKNOWN_BATCH', {
      backfill_no: 'BF-E6',
      records: [{ ext_record_no: 'X5', type: 'PROCESS', event_time: '2026-10-01T07:00',
        equipment_id: LINE, batch_code: 'BX3',
        material_inputs: [{ material_id: SALT }],
        ref_batch_codes: ['NO-SUCH-BATCH'] }]
    }, 'UNKNOWN_BATCH');
    await rejectAndRollback('产出批次编码与既有冲突 -> BATCH_CODE_CONFLICT', {
      backfill_no: 'BF-E7',
      records: [{ ext_record_no: 'X6', type: 'PROCESS', event_time: '2026-10-01T07:00',
        equipment_id: LINE, batch_code: 'BF-B1',
        material_inputs: [{ material_id: SALT }] }]
    }, 'BATCH_CODE_CONFLICT');
    await rejectAndRollback('包内时间冲突（同设备同时刻）-> TIME_CONFLICT', {
      backfill_no: 'BF-E8',
      records: [
        { ext_record_no: 'Y1', type: 'CLEAN', event_time: '2026-10-01T07:00', equipment_id: LINE },
        { ext_record_no: 'Y2', type: 'CLEAN', event_time: '2026-10-01T07:00', equipment_id: LINE }
      ]
    }, 'TIME_CONFLICT');
    await rejectAndRollback('与既有事件时间冲突 -> TIME_CONFLICT', {
      backfill_no: 'BF-E9',
      records: [{ ext_record_no: 'Y3', type: 'CLEAN', event_time: '2026-10-01T08:00',
        equipment_id: LINE }]
    }, 'TIME_CONFLICT');
    await rejectAndRollback('包内外部记录号重复 -> DUPLICATE_EXT_RECORD', {
      backfill_no: 'BF-E10',
      records: [
        { ext_record_no: 'DUP', type: 'CLEAN', event_time: '2026-10-01T06:00', equipment_id: LINE },
        { ext_record_no: 'DUP', type: 'CLEAN', event_time: '2026-10-01T07:00', equipment_id: LINE }
      ]
    }, 'DUPLICATE_EXT_RECORD');

    // 未来时间
    let rr = await api('POST', '/api/backfills', {
      backfill_no: 'BF-E11',
      records: [{ ext_record_no: 'F1', type: 'CLEAN',
        event_time: '2099-01-01T00:00', equipment_id: LINE }]
    });
    check('未来时间 -> 400 FUTURE_TIME', rr.status === 400 && rr.json.code === 'FUTURE_TIME', rr.json);

    // 包内引用指向更晚产出
    rr = await api('POST', '/api/backfills', {
      backfill_no: 'BF-E12',
      records: [
        { ext_record_no: 'N1', type: 'PROCESS', event_time: '2026-10-02T08:00',
          equipment_id: LINE, batch_code: 'BN1', material_inputs: [{ material_id: SALT }],
          ref_record_nos: ['N2'] }, // 指向后面的记录
        { ext_record_no: 'N2', type: 'PROCESS', event_time: '2026-10-02T09:00',
          equipment_id: LINE, batch_code: 'BN2', material_inputs: [{ material_id: SALT }] }
      ]
    });
    check('包内引用非更早产出 -> 400 INVALID_RECORD_REF',
      rr.status === 400 && rr.json.code === 'INVALID_RECORD_REF', rr.json);

    // 循环引用（按编码互相引用，包内更晚产出被显式拦截；时间线兜底为整包校验）
    rr = await api('POST', '/api/backfills', {
      backfill_no: 'BF-E13',
      records: [
        { ext_record_no: 'C-A', type: 'PROCESS', event_time: '2026-10-02T08:00',
          equipment_id: LINE2, batch_code: 'CYC-A', material_inputs: [{ material_id: SALT }],
          ref_batch_codes: ['CYC-B'] },
        { ext_record_no: 'C-B', type: 'PROCESS', event_time: '2026-10-02T09:00',
          equipment_id: LINE2, batch_code: 'CYC-B', material_inputs: [{ material_id: SALT }],
          ref_batch_codes: ['CYC-A'] }
      ]
    });
    check('引用包内更晚产出 -> 400 INVALID_RECORD_REF（整包回滚）',
      rr.status === 400 && rr.json.code === 'INVALID_RECORD_REF', rr.json);
    s = await state();
    check('回滚后无 CYC-A/CYC-B 批次',
      !s.batches.some((b) => b.code === 'CYC-A' || b.code === 'CYC-B'));

    // ---------- 6. 补录影响：早期时间线变化联动放行/卷/出货/处置 ----------
    console.log('6) 补录到放行/标签卷/出货之前的时间点 -> 重算影响摘要');
    // 新设备 LINE-C：先补一条“盐批”并放行、登记卷、出货
    const LINEC = await newId('/api/equipment', { code: 'LINE-C', name: '三线' });
    const bf6 = {
      backfill_no: 'BF-006',
      records: [
        { ext_record_no: 'Q1', type: 'PROCESS', event_time: '2026-10-01T12:00',
          equipment_id: LINEC, batch_code: 'BQ1', material_inputs: [{ material_id: SALT }] },
        { ext_record_no: 'Q2', type: 'CLEAN', event_time: '2026-10-01T13:00',
          equipment_id: LINEC, note: '全清' }
      ]
    };
    r = await api('POST', '/api/backfills', bf6);
    check('LINE-C 补录成功', r.status === 200, r.json);
    s = await state();
    const bq1 = batchOf(s, 'BQ1');
    check('BQ1 无过敏原', bq1.allergens.length === 0, bq1.allergens);

    r = await api('POST', '/api/releases', {
      batch_id: bq1.id, released_at: '2026-10-01T14:00', released_by: '质检'
    });
    const releaseId = r.json.id;
    r = await api('POST', '/api/label-rolls', {
      batch_id: bq1.id, release_id: releaseId, allergens: [],
      print_qty: 100, registered_by: '包装'
    });
    const rollId = r.json.id;
    await api('POST', `/api/label-rolls/${rollId}/consume`, {
      expected_revision: 0, applied: 10, voided: 0
    });
    r = await api('POST', '/api/shipments', {
      batch_id: bq1.id, ship_qty: 10, shipped_by: '仓管',
      allocations: [{ roll_id: rollId, qty: 10 }]
    });
    check('出货成功', r.status === 200 && r.json.status === 'SHIPPED', r.json);
    const shipCode = r.json.shipment_code;

    // 补录一条更早的花生加工（在 Q1 之前，且清洗改为在花生加工之前不存在）
    // 11:00 花生加工 -> 12:00 BQ1(盐) 继承花生残留 -> 13:00 全清
    const bf7 = {
      backfill_no: 'BF-007',
      records: [
        { ext_record_no: 'Q0', type: 'PROCESS', event_time: '2026-10-01T11:00',
          equipment_id: LINEC, batch_code: 'BQ0',
          material_inputs: [{ material_code: 'PNT' }] }
      ]
    };
    r = await api('POST', '/api/backfills', bf7);
    check('更早花生批补录成功', r.status === 200, r.json);
    check('影响摘要：放行失效 / 卷停用 / 出货待评估',
      (r.json.invalidated || []).some((x) => x.batch_code === 'BQ1') &&
      (r.json.retired_rolls || []).some((x) => x.id === rollId) &&
      (r.json.pending_shipments || []).some((x) => x.shipment_code === shipCode),
      { invalidated: r.json.invalidated, retired: r.json.retired_rolls, pending: r.json.pending_shipments });
    s = await state();
    check('重算后 BQ1 新增花生', batchOf(s, 'BQ1').allergens.includes('花生'));

    // 处置登记后再补录（无变化则不触发；这里验证 stale_dispositions 字段存在且为数组）
    let ev = await api('GET', `/api/shipments/${shipCode}/evaluation`);
    const ver = ev.json.version;
    r = await api('POST', `/api/shipments/${shipCode}/dispositions`, {
      decision: 'RECALL', decided_by: '质检', basis: '花生未声明', version: ver
    });
    check('处置登记成功', r.status === 200, r.json);
    // 再补一个全清到 11:30（花生之前残留被截断？11:30 在 11:00 之后，不改变 BQ1；改为补乳原料批）
    const bf8 = {
      backfill_no: 'BF-008',
      records: [
        { ext_record_no: 'QM', type: 'PROCESS', event_time: '2026-10-01T10:30',
          equipment_id: LINEC, batch_code: 'BQM',
          material_inputs: [{ material_code: 'MILK' }] } // 乳残留也进入 BQ1
      ]
    };
    r = await api('POST', '/api/backfills', bf8);
    check('再次补录影响摘要含 stale_dispositions 数组',
      r.status === 200 && Array.isArray(r.json.stale_dispositions), r.json);

    // ---------- 7. 对账查询 ----------
    console.log('7) 按补录批号对账');
    r = await api('GET', '/api/backfills/BF-003');
    check('BF-003 明细 200', r.status === 200, r.json);
    check('明细含映射与逐条状态（REUSED/INSERTED）',
      r.json.event_mapping['EXT-P1'] === eidP1 &&
      r.json.records.find((x) => x.ext_record_no === 'EXT-P1').status === 'REUSED' &&
      r.json.records.find((x) => x.ext_record_no === 'EXT-P1').origin_backfill_no === 'BF-001' &&
      r.json.records.find((x) => x.ext_record_no === 'EXT-P3').status === 'INSERTED',
      r.json);
    check('明细含影响快照',
      r.json.impact && Array.isArray(r.json.impact.invalidated), r.json.impact);

    r = await api('GET', '/api/backfills/NO-SUCH');
    check('未知补录批号 -> 404 BACKFILL_NOT_FOUND',
      r.status === 404 && r.json.code === 'BACKFILL_NOT_FOUND', r.json);

    r = await api('GET', '/api/backfills');
    check('补录列表含全部成功批号、不含被拒绝批号',
      r.status === 200 &&
      r.json.map((x) => x.backfill_no).sort().join(',') ===
        ['BF-001', 'BF-003', 'BF-004B', 'BF-006', 'BF-007', 'BF-008'].sort().join(','),
      r.json.map((x) => x.backfill_no));

    // ---------- 8. 既有单条事件接口兼容 ----------
    console.log('8) 单条事件接口请求/响应/错误约定保持兼容');
    r = await api('POST', '/api/events', {
      type: 'CLEAN', event_time: '2026-10-01T15:00', equipment_id: LINE, note: '单条接口'
    });
    check('单条 CLEAN 响应形状不变（id + 影响字段）',
      r.status === 200 && typeof r.json.id === 'number' &&
      Array.isArray(r.json.invalidated) && Array.isArray(r.json.retired_rolls), r.json);
    r = await api('POST', '/api/events', {
      type: 'CLEAN', event_time: 'bad-time', equipment_id: LINE
    });
    check('单条接口时间格式错误仍是 400 + error',
      r.status === 400 && typeof r.json.error === 'string' && !r.json.code, r.json);
    s = await state();
    const normal = s.events.find((e) => e.note === '单条接口');
    check('单条接口事件无补录留痕（ext_record_no=null）',
      normal.ext_record_no === null && normal.backfill_no === '' && normal.backfilled === false, normal);

    // 经既有 DELETE 接口删除补录事件：接口约定不变；对账映射保留并标 event_exists=false。
    // 先删引用者 EXT-P4（BF-B4），再删被引用的 EXT-P3（BF-B3）。
    const p4Id = (await api('GET', '/api/backfills/BF-004B')).json
      .event_mapping['EXT-P4'];
    r = await api('DELETE', `/api/events/${p4Id}`);
    check('DELETE 引用者补录事件（200 ok）', r.status === 200 && r.json.ok === true, r.json);
    const p3Id = (await api('GET', '/api/backfills/BF-003')).json
      .event_mapping['EXT-P3'];
    r = await api('DELETE', `/api/events/${p3Id}`);
    check('DELETE 单条接口可删除补录事件（200 ok）', r.status === 200 && r.json.ok === true, r.json);
    r = await api('GET', '/api/backfills/BF-003');
    check('对账映射保留且该条 event_exists=false（审计不删）',
      r.json.event_mapping['EXT-P3'] === p3Id &&
      r.json.records.find((x) => x.ext_record_no === 'EXT-P3').event_exists === false, r.json);
    r = await api('GET', '/api/backfills/BF-001');
    check('其它补录包对账不受影响（EXT-P1 仍 event_exists=true）',
      r.json.records.find((x) => x.ext_record_no === 'EXT-P1').event_exists === true, r.json);

  } finally {
    proc.kill();
  }

  if (failures) {
    console.error(`\n${failures} 项未通过`);
    process.exit(1);
  }
  console.log('\n全部通过');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
