'use strict';
// 断网期间加工/已核实清洗日志的整包补录。
//
// 提交：POST /api/backfills  { backfill_no, note?, records:[ 有序记录 ] }
//   每条记录含 ext_record_no（外部记录号）、event_time、设备（equipment_id 或 equipment_code）：
//     - PROCESS：batch_code（产出批次）、material_inputs（原料输入；供应批号 lot_id/lot_code 可省略=未知）
//                和/或 ref_batch_codes（按批次编码引用）、ref_record_nos（按外部记录号引用更早的加工产出：
//                本包内须为更前序号，也可指向此前已补录入库的外部记录号；均要求产出时间严格更早）；
//     - CLEAN  ：clean_allergens 省略/null=全清；非空数组=部分清洗；显式空集拒绝。
//
// 幂等与冲突：
//   - 相同补录批号 + 相同内容重试：返回首次结果（不重复写入、不再次产生影响）；
//   - 补录批号相同但内容不同：整包拒绝（409 BACKFILL_NO_CONFLICT）；
//   - 外部记录号已入库：同号同内容复用原事件（不重复写入）；同号内容不同整包拒绝（409 EXT_RECORD_CONFLICT）。
//
// 整包事务：未知对象、空清洗集合、时间冲突、未来时间、未来/循环引用等任一不满足均整包回滚，不留部分事件。
// 成功后按现有时间线规则重算设备残留与放行、标签卷、出货、处置影响，返回外部记录号 -> 事件号映射与影响摘要。
//
// 对账：GET /api/backfills（列表）、GET /api/backfills/:backfill_no（明细：映射、逐条状态、影响快照）。

const crypto = require('crypto');
const { validateAll } = require('./validation');
const { recompute } = require('./recompute');
const { resolveMaterialInputs, LotError } = require('./lots');

const TIME_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$/;

class BackfillError extends Error {
  constructor(code, message, details) {
    super(message);
    this.name = 'BackfillError';
    this.code = code;
    this.details = details || [];
  }
}

// 与 replan 相同的稳定序列化（对象键排序），用于规范化内容哈希。
function canonicalString(v) {
  if (Array.isArray(v)) return '[' + v.map(canonicalString).join(',') + ']';
  if (v && typeof v === 'object') {
    return '{' + Object.keys(v).sort()
      .map((k) => JSON.stringify(k) + ':' + canonicalString(v[k])).join(',') + '}';
  }
  return JSON.stringify(v);
}
function sha256(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

// 当前本地时间字符串（秒可选），与事件时间同一格式做字典序比较即得先后。
function nowTime(withSeconds) {
  const d = new Date();
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T` +
    `${p(d.getHours())}:${p(d.getMinutes())}${withSeconds ? ':' + p(d.getSeconds()) : ''}`;
}

function trim(v) {
  return String(v == null ? '' : v).trim();
}

// ---------- 1. 形状规范化（不碰数据库；仅类型/必填/清洗范围等形态校验） ----------
function normalizePackage(raw) {
  raw = raw || {};
  const backfill_no = trim(raw.backfill_no);
  if (!backfill_no) {
    throw new BackfillError('INVALID_REQUEST', '补录批号 backfill_no 不能为空');
  }
  const note = trim(raw.note);
  if (!Array.isArray(raw.records)) {
    throw new BackfillError('INVALID_REQUEST', 'records 必须是有序记录数组');
  }
  if (raw.records.length === 0) {
    throw new BackfillError('EMPTY_PACKAGE', '补录包不能为空：至少提交一条加工或清洗记录');
  }

  const seenExt = new Set();
  const records = raw.records.map((r0, seq) => {
    const where = `records[${seq}]`;
    const r = r0 && typeof r0 === 'object' ? r0 : null;
    if (!r) throw new BackfillError('INVALID_REQUEST', `${where} 必须是对象`);

    const ext_record_no = trim(r.ext_record_no);
    if (!ext_record_no) {
      throw new BackfillError('INVALID_REQUEST', `${where} 缺少外部记录号 ext_record_no`);
    }
    if (seenExt.has(ext_record_no)) {
      throw new BackfillError(
        'DUPLICATE_EXT_RECORD',
        `${where}：外部记录号 "${ext_record_no}" 在同一补录包内重复`,
        [`seq=${seq}, ext_record_no=${ext_record_no}`]
      );
    }
    seenExt.add(ext_record_no);

    const type = trim(r.type).toUpperCase();
    if (type !== 'PROCESS' && type !== 'CLEAN') {
      throw new BackfillError('INVALID_REQUEST', `${where}：记录类型必须为 PROCESS 或 CLEAN`);
    }
    const event_time = trim(r.event_time);
    if (!TIME_RE.test(event_time)) {
      throw new BackfillError(
        'INVALID_REQUEST',
        `${where}：事件时间格式应为 YYYY-MM-DDTHH:MM，例如 2026-10-01T08:00`
      );
    }
    if (event_time > nowTime(event_time.length === 19)) {
      throw new BackfillError(
        'FUTURE_TIME',
        `${where}：记录时间 ${event_time} 晚于当前时间，补录只接受已发生的记录`,
        [`seq=${seq}, ext_record_no=${ext_record_no}, event_time=${event_time}`]
      );
    }
    const recNote = trim(r.note);

    // 设备：id 或 code 至少给一个（存在性在解析阶段核对）。
    const hasEqId = r.equipment_id !== undefined && r.equipment_id !== null && r.equipment_id !== '';
    const equipment_code = trim(r.equipment_code);
    const equipment_id = hasEqId ? parseInt(r.equipment_id, 10) : null;
    if (hasEqId && !Number.isInteger(equipment_id)) {
      throw new BackfillError('INVALID_REQUEST', `${where}：equipment_id 不合法`);
    }
    if (!hasEqId && !equipment_code) {
      throw new BackfillError('INVALID_REQUEST', `${where}：缺少设备 equipment_id 或 equipment_code`);
    }

    const rec = {
      seq, ext_record_no, type, event_time, rec_note: recNote,
      equipment_id, equipment_code: hasEqId ? null : equipment_code
    };

    if (type === 'CLEAN') {
      if (r.clean_allergens !== undefined && r.clean_allergens !== null) {
        if (!Array.isArray(r.clean_allergens)) {
          throw new BackfillError(
            'INVALID_REQUEST',
            `${where}：清洗范围 clean_allergens 必须是过敏原名称数组（省略该字段表示全清）`
          );
        }
        if (r.clean_allergens.length === 0) {
          throw new BackfillError(
            'EMPTY_CLEAN_SCOPE',
            `${where}：清洗范围不能为空数组（显式空集表示未去除任何过敏原，应省略 clean_allergens 或按实际去除项填写）`
          );
        }
        const bad = r.clean_allergens.filter((a) => typeof a !== 'string' || !a.trim());
        if (bad.length) {
          throw new BackfillError(
            'INVALID_REQUEST',
            `${where}：清洗范围中的过敏原名称必须是非空白字符串，不允许非字符串或空白名称`
          );
        }
        rec.clean_allergens = [...new Set(r.clean_allergens.map((a) => a.trim()))];
      } else {
        rec.clean_allergens = null; // 省略 = 全清
      }
      // 清洗记录不得携带加工专属字段
      if (r.batch_code !== undefined || r.material_inputs !== undefined ||
          r.ref_batch_codes !== undefined || r.ref_record_nos !== undefined) {
        throw new BackfillError('INVALID_REQUEST', `${where}：清洗记录只接受 clean_allergens，不携带产出批次/原料/引用字段`);
      }
      return rec;
    }

    // ---- PROCESS ----
    if (r.clean_allergens !== undefined && r.clean_allergens !== null) {
      throw new BackfillError('INVALID_REQUEST', `${where}：clean_allergens 仅适用于清洗记录`);
    }
    const batch_code = trim(r.batch_code);
    if (!batch_code) {
      throw new BackfillError('INVALID_REQUEST', `${where}：加工记录缺少产出批次 batch_code`);
    }
    rec.batch_code = batch_code;

    // 原料输入：material_id 或 material_code 标识原料；lot_id/lot_code 可省略（批号未知）。
    rec.material_inputs_raw = [];
    if (r.material_inputs !== undefined && r.material_inputs !== null) {
      if (!Array.isArray(r.material_inputs)) {
        throw new BackfillError('INVALID_REQUEST', `${where}：material_inputs 必须是数组`);
      }
      r.material_inputs.forEach((inp, j) => {
        const w = `${where}.material_inputs[${j}]`;
        if (!inp || typeof inp !== 'object') {
          throw new BackfillError('INVALID_REQUEST', `${w} 必须是对象`);
        }
        const midRaw = inp.material_id !== undefined && inp.material_id !== null && inp.material_id !== ''
          ? parseInt(inp.material_id, 10) : null;
        if (midRaw !== null && !Number.isInteger(midRaw)) {
          throw new BackfillError('INVALID_REQUEST', `${w}：material_id 不合法`);
        }
        const mcode = trim(inp.material_code);
        if (midRaw === null && !mcode) {
          throw new BackfillError('INVALID_REQUEST', `${w}：缺少原料 material_id 或 material_code`);
        }
        const hasLotId = inp.lot_id !== undefined && inp.lot_id !== null && inp.lot_id !== '';
        const hasLotCode = inp.lot_code !== undefined && inp.lot_code !== null && inp.lot_code !== '';
        let lotIdRaw = null;
        let lotCode = null;
        if (hasLotId && hasLotCode) {
          throw new BackfillError('INVALID_REQUEST', `${w}：lot_id 与 lot_code 只能提供其一`);
        }
        if (hasLotId) {
          lotIdRaw = parseInt(inp.lot_id, 10);
          if (!Number.isInteger(lotIdRaw)) {
            throw new BackfillError('INVALID_REQUEST', `${w}：lot_id 不合法`);
          }
        } else if (hasLotCode) {
          lotCode = trim(inp.lot_code);
          if (!lotCode) throw new BackfillError('INVALID_REQUEST', `${w}：lot_code 不能为空`);
        }
        rec.material_inputs_raw.push({ material_id: midRaw, material_code: midRaw === null ? mcode : null, lot_id: lotIdRaw, lot_code: lotCode });
      });
    }

    rec.ref_batch_codes = stringList(r.ref_batch_codes, `${where}.ref_batch_codes`);
    rec.ref_record_nos = stringList(r.ref_record_nos, `${where}.ref_record_nos`);
    return rec;
  });

  return { backfill_no, note, records };
}

function stringList(v, where) {
  if (v === undefined || v === null) return [];
  if (!Array.isArray(v)) throw new BackfillError('INVALID_REQUEST', `${where} 必须是字符串数组`);
  const out = [];
  v.forEach((x) => {
    if (typeof x !== 'string' || !x.trim()) {
      throw new BackfillError('INVALID_REQUEST', `${where} 中每项必须是非空白字符串`);
    }
    out.push(x.trim());
  });
  return [...new Set(out)];
}

// ---------- 2. 引用解析：设备/原料/批号/引用目标 ----------
// 解析后每条记录补齐：equipment_id、material_inputs:{material_id,material_lot_id}、
// ref_batch_codes（包内记录号引用先换算成对应产出批次编码），以及规范化哈希。
function resolvePackage(db, pkg) {
  const equipRows = db.prepare('SELECT id, code FROM equipment').all();
  const equipById = new Map(equipRows.map((e) => [e.id, e]));
  const equipByCode = new Map(equipRows.map((e) => [e.code, e]));
  const matRows = db.prepare('SELECT id, code FROM materials').all();
  const matById = new Map(matRows.map((m) => [m.id, m]));
  const matByCode = new Map(matRows.map((m) => [m.code, m]));

  const extToSeq = new Map(pkg.records.map((r) => [r.ext_record_no, r.seq]));

  const resolved = pkg.records.map((rec) => ({ ...rec, material_inputs_raw: rec.material_inputs_raw || [] }));

  for (const rec of resolved) {
    // 设备
    if (rec.equipment_id !== null) {
      if (!equipById.has(rec.equipment_id)) {
        throw new BackfillError(
          'UNKNOWN_EQUIPMENT',
          `records[${rec.seq}]：设备 #${rec.equipment_id} 不存在，请先登记设备`,
          [`seq=${rec.seq}, equipment_id=${rec.equipment_id}`]
        );
      }
    } else {
      const eq = equipByCode.get(rec.equipment_code);
      if (!eq) {
        throw new BackfillError(
          'UNKNOWN_EQUIPMENT',
          `records[${rec.seq}]：设备编码 "${rec.equipment_code}" 不存在，请先登记设备`,
          [`seq=${rec.seq}, equipment_code=${rec.equipment_code}`]
        );
      }
      rec.equipment_id = eq.id;
      rec.equipment_code = null;
    }

    if (rec.type === 'CLEAN') continue; // 清洗无引用解析

    // 原料编码 -> 原料 id（随后交给 lots.resolveMaterialInputs 统一做批号存在性/归属校验）
    const lotInputs = rec.material_inputs_raw.map((inp) => {
      let material_id = inp.material_id;
      if (material_id === null) {
        const m = matByCode.get(inp.material_code);
        if (!m) {
          throw new BackfillError(
            'UNKNOWN_MATERIAL',
            `records[${rec.seq}]：原料编码 "${inp.material_code}" 不存在`,
            [`seq=${rec.seq}, material_code=${inp.material_code}`]
          );
        }
        material_id = m.id;
      } else if (!matById.has(material_id)) {
        throw new BackfillError(
          'UNKNOWN_MATERIAL',
          `records[${rec.seq}]：原料 #${material_id} 不存在`,
          [`seq=${rec.seq}, material_id=${material_id}`]
        );
      }
      return { material_id, lot_id: inp.lot_id, lot_code: inp.lot_code };
    });
    // 批号不存在 / 批号与原料不匹配在此抛 LotError（LOT_NOT_FOUND/LOT_MATERIAL_MISMATCH），整包拒绝。
    rec.material_inputs = resolveMaterialInputs(db, lotInputs);

    // ref_record_nos：指向更早的加工产出——
    //   - 在本补录包内：必须是更前序号的 PROCESS 且时间严格更早；
    //   - 不在包内：可指向已入库补录事件的外部记录号（必须是 PROCESS 且产出时间严格更早）。
    const refCodes = new Set(rec.ref_batch_codes);
    for (const refNo of rec.ref_record_nos) {
      const targetSeq = extToSeq.get(refNo);
      if (targetSeq !== undefined) {
        if (targetSeq >= rec.seq) {
          throw new BackfillError(
            'INVALID_RECORD_REF',
            `records[${rec.seq}]：外部记录号 "${refNo}" 位于本包第 ${targetSeq} 条，不是更早的记录；包内引用只可指向更早产出`,
            [`seq=${rec.seq}, ref_record_no=${refNo}, target_seq=${targetSeq}`]
          );
        }
        const target = resolved[targetSeq];
        if (target.type !== 'PROCESS') {
          throw new BackfillError(
            'INVALID_RECORD_REF',
            `records[${rec.seq}]：外部记录号 "${refNo}" 是清洗记录，不能作为批次引用`,
            [`seq=${rec.seq}, ref_record_no=${refNo}`]
          );
        }
        if (target.event_time >= rec.event_time) {
          throw new BackfillError(
            'INVALID_RECORD_REF',
            `records[${rec.seq}]：引用的包内产出 "${refNo}"（${target.event_time}）不是更早时间，不允许引用未来或同时刻批次`,
            [`seq=${rec.seq}, ref_record_no=${refNo}, target_time=${target.event_time}`]
          );
        }
        refCodes.add(target.batch_code);
        continue;
      }
      // 跨包引用：解析已入库的外部记录号
      const prior = db.prepare(
        `SELECT ev.type, ev.event_time, b.code AS batch_code
           FROM events ev LEFT JOIN batches b ON b.id = ev.batch_id
          WHERE ev.ext_record_no = ?`
      ).get(refNo);
      if (!prior) {
        throw new BackfillError(
          'INVALID_RECORD_REF',
          `records[${rec.seq}]：引用的外部记录号 "${refNo}" 既不在本补录包内，也未在此前补录入库`,
          [`seq=${rec.seq}, ref_record_no=${refNo}`]
        );
      }
      if (prior.type !== 'PROCESS' || !prior.batch_code) {
        throw new BackfillError(
          'INVALID_RECORD_REF',
          `records[${rec.seq}]：外部记录号 "${refNo}" 是清洗记录，不能作为批次引用`,
          [`seq=${rec.seq}, ref_record_no=${refNo}`]
        );
      }
      if (prior.event_time >= rec.event_time) {
        throw new BackfillError(
          'INVALID_RECORD_REF',
          `records[${rec.seq}]：外部记录号 "${refNo}" 的产出时间 ${prior.event_time} 不是更早时间，不允许引用未来或同时刻批次`,
          [`seq=${rec.seq}, ref_record_no=${refNo}, target_time=${prior.event_time}`]
        );
      }
      refCodes.add(prior.batch_code);
    }
    rec.ref_codes = [...refCodes];
  }

  // 规范化内容哈希（引用、原料均已解析为 id；顺序不敏感的集合先排序，包内有序数组保持顺序）。
  for (const rec of resolved) {
    const c = rec.type === 'PROCESS'
      ? {
          type: 'PROCESS',
          event_time: rec.event_time,
          equipment_id: rec.equipment_id,
          batch_code: rec.batch_code,
          note: rec.rec_note,
          material_inputs: rec.material_inputs
            .map((x) => ({ m: x.material_id, l: x.material_lot_id }))
            .sort((a, b) => a.m - b.m || (a.l || 0) - (b.l || 0)),
          ref_batch_codes: rec.ref_codes.slice().sort()
        }
      : {
          type: 'CLEAN',
          event_time: rec.event_time,
          equipment_id: rec.equipment_id,
          note: rec.rec_note,
          clean_allergens: rec.clean_allergens ? rec.clean_allergens.slice().sort() : null
        };
    rec.content_hash = sha256(canonicalString(c));
  }

  return {
    backfill_no: pkg.backfill_no,
    note: pkg.note,
    records: resolved,
    content_hash: sha256(canonicalString(resolved.map((r) => r.content_hash)))
  };
}

// ---------- 3. 提交（整包事务：幂等/冲突 -> 写入 -> 校验 -> 重算 -> 留痕） ----------
function submitBackfill(db, rawBody) {
  const norm = normalizePackage(rawBody);
  const resolved = resolvePackage(db, norm);

  // 同补录批号：内容相同=幂等重放原结果；内容不同=整包拒绝。
  const existing = db.prepare('SELECT * FROM backfill_batches WHERE backfill_no = ?')
    .get(resolved.backfill_no);
  if (existing) {
    if (existing.content_hash !== resolved.content_hash) {
      throw new BackfillError(
        'BACKFILL_NO_CONFLICT',
        `补录批号 "${resolved.backfill_no}" 已入库但内容与本次提交不同：相同批号必须提交完全相同的内容`,
        [`stored_content_hash=${existing.content_hash}`, `request_content_hash=${resolved.content_hash}`]
      );
    }
    return replayStoredResponse(db, existing);
  }

  // 外部记录号全局冲突预检（事务前读当前正式库）：
  //   同号同内容 -> REUSE 既有事件；同号内容不同 -> 整包拒绝。
  const evByExt = db.prepare(
    `SELECT e.id AS event_id, e.ext_record_no,
            (SELECT br.content_hash FROM backfill_records br
              WHERE br.event_id = e.id ORDER BY br.id LIMIT 1) AS stored_hash,
            (SELECT br.backfilled_at FROM backfill_records br
              WHERE br.event_id = e.id ORDER BY br.id LIMIT 1) AS origin_backfill_no
       FROM events e WHERE e.ext_record_no IS NOT NULL`
  ).all();
  const extMap = new Map(evByExt.map((e) => [e.ext_record_no, e]));
  for (const rec of resolved.records) {
    const hit = extMap.get(rec.ext_record_no);
    if (!hit) { rec.reuse = null; continue; }
    if (!hit.stored_hash) {
      throw new BackfillError(
        'EXT_RECORD_CONFLICT',
        `records[${rec.seq}]：外部记录号 "${rec.ext_record_no}" 已绑定事件 #${hit.event_id} 但缺少补录内容留痕，无法核对，整包拒绝`,
        [`seq=${rec.seq}, ext_record_no=${rec.ext_record_no}, event_id=${hit.event_id}`]
      );
    }
    if (hit.stored_hash !== rec.content_hash) {
      throw new BackfillError(
        'EXT_RECORD_CONFLICT',
        `records[${rec.seq}]：外部记录号 "${rec.ext_record_no}" 与已入库事件 #${hit.event_id} 内容冲突，整包拒绝`,
        [`seq=${rec.seq}, ext_record_no=${rec.ext_record_no}, event_id=${hit.event_id}`,
         `stored_content_hash=${hit.stored_hash}`, `request_content_hash=${rec.content_hash}`]
      );
    }
    rec.reuse = { event_id: hit.event_id, origin_backfill_no: hit.origin_backfill_no || '' };
  }

  let response;
  runBackfillTransaction(() => db.transaction(() => {
    const d = db;

    // ---- 事务内显式预检（错误信息明确；validateAll 仍为最终兜底）----
    const newRecords = resolved.records.filter((r) => !r.reuse);

    // 产出批次编码：不得与既有批次冲突；包内新产出编码不得重复。
    const existingBatch = d.prepare('SELECT id, code FROM batches').all();
    const codeToBatchId = new Map(existingBatch.map((b) => [b.code, b.id]));
    const pkgNewCodes = new Map(); // 包内新产出批次编码 -> seq
    for (const rec of newRecords) {
      if (rec.type !== 'PROCESS') continue;
      if (codeToBatchId.has(rec.batch_code)) {
        throw new BackfillError(
          'BATCH_CODE_CONFLICT',
          `records[${rec.seq}]：产出批次编码 "${rec.batch_code}" 已存在，批次编码必须唯一`,
          [`seq=${rec.seq}, batch_code=${rec.batch_code}`]
        );
      }
      if (pkgNewCodes.has(rec.batch_code)) {
        throw new BackfillError(
          'BATCH_CODE_CONFLICT',
          `records[${rec.seq}]：产出批次编码 "${rec.batch_code}" 在本补录包内重复产出`,
          [`seq=${rec.seq}, batch_code=${rec.batch_code}`]
        );
      }
      pkgNewCodes.set(rec.batch_code, rec.seq);
    }

    // 引用批次（含包内更早产出换算来的编码）：必须能解析到批次 id；
    // 引用包内批次时只可指向更早产出（无论用 ref_batch_codes 还是 ref_record_nos 换算）。
    for (const rec of newRecords) {
      if (rec.type !== 'PROCESS') continue;
      for (const code of rec.ref_codes) {
        const targetSeq = pkgNewCodes.get(code);
        if (targetSeq !== undefined) {
          if (targetSeq >= rec.seq) {
            throw new BackfillError(
              'INVALID_RECORD_REF',
              `records[${rec.seq}]：引用批次 "${code}" 是本补录包第 ${targetSeq} 条的产出，包内引用只可指向更早产出`,
              [`seq=${rec.seq}, ref_batch_code=${code}, target_seq=${targetSeq}`]
            );
          }
          continue; // 包内更早产出，插入时已先建批次
        }
        if (!codeToBatchId.has(code)) {
          throw new BackfillError(
            'UNKNOWN_BATCH',
            `records[${rec.seq}]：引用批次编码 "${code}" 不存在，也不在本补录包的更早产出中`,
            [`seq=${rec.seq}, ref_batch_code=${code}`]
          );
        }
      }
    }

    // 时间冲突：同一设备同一时刻不得多事件（对既有事件 + 包内全部记录）。
    const slotInPkg = new Set();
    for (const rec of resolved.records) {
      const key = `${rec.equipment_id}|${rec.event_time}`;
      if (slotInPkg.has(key)) {
        throw new BackfillError(
          'TIME_CONFLICT',
          `records[${rec.seq}]：设备 #${rec.equipment_id} 在 ${rec.event_time} 与包内另一条记录时间冲突`,
          [`seq=${rec.seq}, equipment_id=${rec.equipment_id}, event_time=${rec.event_time}`]
        );
      }
      slotInPkg.add(key);
    }
    const conflictStmt = d.prepare(
      'SELECT id FROM events WHERE equipment_id = ? AND event_time = ? LIMIT 1'
    );
    for (const rec of newRecords) {
      const hit = conflictStmt.get(rec.equipment_id, rec.event_time);
      if (hit) {
        throw new BackfillError(
          'TIME_CONFLICT',
          `records[${rec.seq}]：设备 #${rec.equipment_id} 在 ${rec.event_time} 已存在事件 #${hit.id}，时间冲突`,
          [`seq=${rec.seq}, equipment_id=${rec.equipment_id}, event_time=${rec.event_time}, existing_event_id=${hit.id}`]
        );
      }
    }

    // ---- 按有序记录写入（复用记录跳过）----
    const insBatch = d.prepare('INSERT INTO batches (code) VALUES (?)');
    const insEvent = d.prepare(
      `INSERT INTO events (event_time, type, equipment_id, batch_id, clean_allergens, note,
                           ext_record_no, backfill_no)
       VALUES (?,?,?,?,?,?,?,?)`
    );
    const insMatInput = d.prepare(
      'INSERT INTO process_inputs (event_id, material_id, material_lot_id) VALUES (?,?,?)'
    );
    const insBatchRef = d.prepare('INSERT INTO process_inputs (event_id, batch_id) VALUES (?,?)');

    const written = []; // {rec, event_id}
    for (const rec of resolved.records) {
      if (rec.reuse) { written.push({ rec, event_id: rec.reuse.event_id }); continue; }

      let batchId = null;
      if (rec.type === 'PROCESS') {
        batchId = insBatch.run(rec.batch_code).lastInsertRowid;
        codeToBatchId.set(rec.batch_code, batchId);
      }
      const cleanJson = rec.type === 'CLEAN' && rec.clean_allergens
        ? JSON.stringify(rec.clean_allergens) : null;
      const eventId = insEvent.run(
        rec.event_time, rec.type, rec.equipment_id, batchId,
        cleanJson, rec.rec_note, rec.ext_record_no, resolved.backfill_no
      ).lastInsertRowid;

      if (rec.type === 'PROCESS') {
        for (const inp of rec.material_inputs) {
          insMatInput.run(eventId, inp.material_id, inp.material_lot_id);
        }
        for (const code of rec.ref_codes) {
          const refId = codeToBatchId.get(code);
          if (refId == null) {
            throw new BackfillError(
              'UNKNOWN_BATCH',
              `records[${rec.seq}]：引用批次编码 "${code}" 无法解析（包内引用只可指向更早产出）`,
              [`seq=${rec.seq}, ref_batch_code=${code}`]
            );
          }
          insBatchRef.run(eventId, refId);
        }
      }
      written.push({ rec, event_id: eventId });
    }

    // ---- 全量时间线校验（未来/循环引用、重复产出等最终兜底；失败整包回滚）----
    const errors = validateAll(d);
    if (errors.length) {
      throw new BackfillError('PACKAGE_REJECTED', '补录包应用后的时间线校验未通过，整包回滚', errors);
    }

    // ---- 按现有时间线规则重算：设备残留/放行/标签卷/出货/处置 ----
    const impact = recompute(d);

    // ---- 补录留痕（与事件同一事务；失败随整包回滚）----
    const bfInfo = d.prepare(
      `INSERT INTO backfill_batches
         (backfill_no, note, status, record_count, inserted_count, reused_count,
          content_hash, impact_json, applied_at)
       VALUES (?,?, 'APPLIED', ?, ?, ?, ?, ?, datetime('now'))`
    ).run(
      resolved.backfill_no, resolved.note,
      resolved.records.length, newRecords.length, resolved.records.length - newRecords.length,
      resolved.content_hash, JSON.stringify(impact)
    );
    const backfillId = bfInfo.lastInsertRowid;
    const insRec = d.prepare(
      `INSERT INTO backfill_records
         (backfill_id, seq, ext_record_no, event_id, content_hash, status, backfilled_at)
       VALUES (?,?,?,?,?,?,?)`
    );
    for (const { rec, event_id } of written) {
      insRec.run(
        backfillId, rec.seq, rec.ext_record_no, event_id, rec.content_hash,
        rec.reuse ? 'REUSED' : 'INSERTED',
        rec.reuse ? (rec.reuse.origin_backfill_no || '') : resolved.backfill_no
      );
    }

    response = buildResponse(resolved, written, impact, false);
  })());

  return response;
}

// 并发兜底：两个请求同刻提交相同补录批号/外部记录号时，显式预检可能都通过，
// 最终由数据库唯一约束拦截。此处翻译成与预检一致的业务错误（事务已整体回滚，不留部分事件）。
function runBackfillTransaction(fn) {
  try {
    return fn();
  } catch (err) {
    if (err && err.name === 'SqliteError' && /UNIQUE/i.test(String(err.message || ''))) {
      const msg = String(err.message);
      if (msg.includes('backfill_batches.backfill_no') || msg.includes('backfill_no')) {
        throw new BackfillError(
          'BACKFILL_NO_CONFLICT',
          '补录批号已存在：相同批号必须提交完全相同的内容（并发提交时另一请求已入库）'
        );
      }
      if (msg.includes('idx_events_ext_record')) {
        throw new BackfillError(
          'EXT_RECORD_CONFLICT',
          '存在与已入库事件冲突的外部记录号（并发提交时另一请求已使用该号），整包回滚'
        );
      }
      throw new BackfillError('PACKAGE_REJECTED', `补录落库触发唯一约束，整包回滚：${msg}`);
    }
    throw err;
  }
}

function buildResponse(resolved, written, impact, idempotentReplay) {
  const bySeq = new Map(written.map((w) => [w.rec.seq, w]));
  const event_mapping = {};
  const events = resolved.records.map((rec) => {
    const w = bySeq.get(rec.seq);
    const event_id = w ? w.event_id : (rec.reuse && rec.reuse.event_id) || null;
    event_mapping[rec.ext_record_no] = event_id;
    return {
      seq: rec.seq,
      ext_record_no: rec.ext_record_no,
      event_id,
      type: rec.type,
      event_time: rec.event_time,
      equipment_id: rec.equipment_id,
      ...(rec.type === 'PROCESS' ? { batch_code: rec.batch_code } : {}),
      status: rec.reuse ? 'REUSED' : 'INSERTED'
    };
  });
  return {
    ok: true,
    idempotent_replay: idempotentReplay,
    backfill_no: resolved.backfill_no,
    record_count: resolved.records.length,
    inserted_count: written.filter((w) => !w.rec.reuse).length,
    reused_count: written.filter((w) => w.rec.reuse).length,
    event_mapping,
    events,
    invalidated: impact.invalidated,
    retired_rolls: impact.retiredRolls,
    pending_shipments: impact.pendingShipments,
    stale_dispositions: impact.staleDispositions
  };
}

// 幂等重放：补录批号相同且内容相同，返回首次落库的映射与影响快照（不再次写入、不再次产生影响）。
function replayStoredResponse(db, row) {
  let impact = {};
  try { impact = JSON.parse(row.impact_json || '{}'); } catch (_) { impact = {}; }
  const recRows = db.prepare(
    `SELECT seq, ext_record_no, event_id, status
       FROM backfill_records WHERE backfill_id = ? ORDER BY seq`
  ).all(row.id);
  const event_mapping = {};
  const events = recRows.map((r) => {
    event_mapping[r.ext_record_no] = r.event_id;
    return {
      seq: r.seq, ext_record_no: r.ext_record_no, event_id: r.event_id,
      status: r.status // INSERTED / REUSED
    };
  });
  return {
    ok: true,
    idempotent_replay: true,
    backfill_no: row.backfill_no,
    record_count: row.record_count,
    inserted_count: row.inserted_count,
    reused_count: row.reused_count,
    event_mapping,
    events,
    invalidated: impact.invalidated || [],
    retired_rolls: impact.retiredRolls || [],
    pending_shipments: impact.pendingShipments || [],
    stale_dispositions: impact.staleDispositions || []
  };
}

// ---------- 4. 对账查询 ----------
function getBackfill(db, backfillNo) {
  const row = db.prepare('SELECT * FROM backfill_batches WHERE backfill_no = ?').get(backfillNo);
  if (!row) {
    throw new BackfillError('BACKFILL_NOT_FOUND', `补录批号 "${backfillNo}" 不存在`);
  }
  let impact = {};
  try { impact = JSON.parse(row.impact_json || '{}'); } catch (_) { impact = {}; }

  const recRows = db.prepare(
    `SELECT br.seq, br.ext_record_no, br.event_id, br.content_hash, br.status,
            br.backfilled_at AS origin_backfill_no,
            ev.id AS existing_event_id, ev.event_time, ev.type, ev.equipment_id,
            b.code AS batch_code
       FROM backfill_records br
       LEFT JOIN events ev ON ev.id = br.event_id
       LEFT JOIN batches b ON b.id = ev.batch_id
      WHERE br.backfill_id = ? ORDER BY br.seq`
  ).all(row.id);

  const event_mapping = {};
  const records = recRows.map((r) => {
    event_mapping[r.ext_record_no] = r.event_id;
    return {
      seq: r.seq,
      ext_record_no: r.ext_record_no,
      event_id: r.event_id,
      event_exists: r.existing_event_id != null, // 事件若事后经单条接口删除，此处为 false（映射留痕）
      status: r.status,                           // INSERTED=本包写入；REUSED=复用更早补录包的事件
      origin_backfill_no: r.origin_backfill_no || null,
      type: r.type || null,
      event_time: r.event_time || null,
      equipment_id: r.equipment_id != null ? r.equipment_id : null,
      ...(r.batch_code ? { batch_code: r.batch_code } : {})
    };
  });

  return {
    backfill_no: row.backfill_no,
    note: row.note,
    status: row.status,
    record_count: row.record_count,
    inserted_count: row.inserted_count,
    reused_count: row.reused_count,
    content_hash: row.content_hash,
    created_at: row.created_at,
    applied_at: row.applied_at,
    event_mapping,
    records,
    impact: {
      invalidated: impact.invalidated || [],
      retired_rolls: impact.retiredRolls || [],
      pending_shipments: impact.pendingShipments || [],
      stale_dispositions: impact.staleDispositions || []
    }
  };
}

function listBackfills(db) {
  return db.prepare(
    `SELECT backfill_no, note, status, record_count, inserted_count, reused_count,
            content_hash, created_at, applied_at
       FROM backfill_batches ORDER BY id DESC`
  ).all();
}

module.exports = {
  BackfillError,
  LotError,
  submitBackfill,
  getBackfill,
  listBackfills,
  // 导出供测试：
  normalizePackage,
  resolvePackage
};
