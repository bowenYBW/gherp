#!/usr/bin/env node
/* ==================================================================
   server.js —— 广宏鞋业ERP系统 · 局域网协同数据中枢（兼容增强版）
   ------------------------------------------------------------------
   本文件与 ERP 前端「数据同步」模块的 syncLanPing / syncLanPull /
   syncLanPush 完全对齐，并针对旧版（v2.0.14 / v3.0）做了如下修复：

   1) 局域网真正可用
      旧版只监听 127.0.0.1，局域网内其他电脑根本连不上，多机协同名存实亡。
      现默认监听全网（双栈），任何一台同网段电脑都能连接。

   2) 「启动服务器」按钮不再必然失败
      旧版在同一端口再拉一个子进程，必然 EADDRINUSE（实测直接 START_FAILED）。
      现改为切换监听范围：启动=对局域网开放，停止=仅本机可访问，面板始终在线。

   3) 修复时间字段混用导致的「旧数据覆盖新数据」
      ERP 的 updatedAt 同时存在 ISO（toISOString）、本地格式化（toLocaleString）、
      纯日期（toLocaleDateString）和时间戳等写法。旧版直接做字符串比较，
      由于 '/' 的字符码大于 '-'，本地格式恒被判定为「更新」，实测会把
      10-27 的新记录判成旧于 10-25 的旧记录。现统一解析为毫秒时间戳再比较。

   4) 修复多台电脑「自增 id 撞车」导致的数据丢失
      ERP 的库存/BOM/生产等表由 IndexedDB autoIncrement 分配 id，每台电脑
      都从 1 开始，旧版按 id 去重合并，实测 A 电脑的 id=1 记录被 B 电脑的
      id=1 记录直接覆盖丢失。现改为以服务器注入的全局唯一键 _uid 为主键合并，
      并在下发快照时保证同一张表内 id 不重复，前端可安全落库。

   5) 修复「已删除的记录被拉回来」
      删除同步可识别「本设备创建/最后修改、但本次推送中已不存在」的记录并
      安全清除；也可携带 _deleted 墓碑。可用启动参数调整策略。

   6) 修复「连上空服务器会把本机数据清空」的重大隐患
      旧版拉取恒返回全量（含空数组），前端 putAll 会 clear 后写入，
      新电脑首次连接即可能丢掉本机全部数据。现对「尚未推送过的设备」
      启用首次连接保护，必须先推送完成合并，之后拉取才下发数据。

   7) 健壮性增强
      表名别名兼容（服务器键名 / IndexedDB 键名互通）、请求体上限、
      自动备份轮转、键名兼容迁移、统计接口、错误兜底。

   ------------------------------------------------------------------
   用法：
     node server.js [port]                默认端口 7001
     node server.js 7001
     node server.js --port=7001
     node server.js 7001 --no-open        启动但不自动打开监控面板
     node server.js 7001 --local          仅本机监听（不对局域网开放）
     node server.js 7001 --no-delete-sync 关闭删除同步（最保守，只增不删）
     node server.js 7001 --delete-all     删除同步改为全量覆盖语义（慎用）
     node server.js 7001 --pull-no-guard  关闭「首次连接保护」（不推荐）

   ERP 前端使用的接口（返回结构与之完全兼容）：
     GET  /api/ping   心跳   -> { ok, clients, serverTime, ver }
     GET  /api/pull   拉取   -> { ok, __ver, data, serverTime, stats }
     POST /api/push   推送   -> { ok, conflict, ver, merged, added, deleted }
     GET  /api/stats  统计   （面板/运维用）
     GET  /api/health GET /api/version

   内置管理接口（仅 127.0.0.1 可访问，安全）：
     GET  /admin/dashboard  可视化监控面板
     GET  /admin/status     运行状态
     POST /admin/start      开放局域网访问
     POST /admin/stop       停止对外（仅本机，面板不受影响）
     GET  /admin/output     增量读取运行日志
   ================================================================== */

'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const cp = require('child_process');
const crypto = require('crypto');

/* ---------- 启动参数 ---------- */
let PORT = 7001;
let AUTO_OPEN = true;
let LOCAL_ONLY = false;
let PULL_GUARD = true;              // 首次连接保护
let DELETE_MODE_DEFAULT = 'owner';  // none | owner | all

(function parseArgs() {
  process.argv.slice(2).forEach(function (a) {
    if (a === '--no-open') AUTO_OPEN = false;
    else if (a === '--local' || a === '--local-only') LOCAL_ONLY = true;
    else if (a === '--no-delete-sync') DELETE_MODE_DEFAULT = 'none';
    else if (a === '--delete-all') DELETE_MODE_DEFAULT = 'all';
    else if (a === '--pull-no-guard') PULL_GUARD = false;
    else if (/^--port=\d+$/.test(a)) PORT = parseInt(a.slice(7), 10);
    else if (/^\d+$/.test(a)) PORT = parseInt(a, 10);
  });
  if (!(PORT >= 1 && PORT <= 65535)) PORT = 7001;
})();

const STARTED_AT = Date.now();
const DATA_FILE = path.join(__dirname, 'shoe_erp.server.db.json');
const BACKUP_DIR = path.join(__dirname, 'erp_server_backup');
const MAX_BODY = 256 * 1024 * 1024;   // 单次推送最大 256MB

/* ---------- 数据表定义（服务器标准键名，与 ERP 前端约定一致） ---------- */
const SERVER_KEYS = ['orders', 'bom', 'sole', 'uppersole', 'finished', 'last', 'prodGroups', 'prodRecords'];
const KEY_LABELS = {
  orders: '订单', bom: 'BOM物料', sole: '大底', uppersole: '鞋面',
  finished: '成品', last: '楦头', prodGroups: '生产组', prodRecords: '生产记录'
};

/* ERP 前端 IndexedDB 键名等历史写法 -> 服务器标准键名（双向兼容） */
const KEY_ALIAS_LOWER = {
  'orders': 'orders',
  'bom': 'bom', 'bomitems': 'bom', 'bomitem': 'bom',
  'sole': 'sole', 'outsole': 'sole', 'outsolestock': 'sole',
  'uppersole': 'uppersole', 'upper': 'uppersole', 'upperstock': 'uppersole',
  'finished': 'finished', 'finishedstock': 'finished',
  'last': 'last', 'laststock': 'last',
  'prodgroups': 'prodGroups', 'productiongroups': 'prodGroups', 'prodgroup': 'prodGroups',
  'prodrecords': 'prodRecords', 'productionrecords': 'prodRecords', 'prodrecord': 'prodRecords'
};

function normKey(k) {
  if (!k) return null;
  if (SERVER_KEYS.indexOf(k) !== -1) return k;
  return KEY_ALIAS_LOWER[String(k).toLowerCase()] || null;
}

function emptyData() {
  const d = {};
  SERVER_KEYS.forEach(function (k) { d[k] = []; });
  return d;
}

/* ---------- 内存快照与运行状态 ---------- */
let store = {
  __ver: 0,
  __devices: {},   // ip -> { firstSeen, lastSeen, pushes, clientId }
  __merged: 0,
  __lastMerge: 0,
  data: emptyData()
};

let CURRENT_HOST = null;      // null=全网（双栈），'127.0.0.1'=仅本机
const clients = new Map();    // clientId -> 最近心跳时间
const logState = { buf: [], pushCount: 0, pullCount: 0 };

/* ---------- 工具 ---------- */
function genUid() {
  return 'u' + Date.now().toString(36) + crypto.randomBytes(4).toString('hex');
}

function pushLog(tag, text) {
  if (text !== '') console.log(text);
  String(text).split('\n').forEach(function (line) {
    line = line.replace(/\s+$/, '');
    if (!line) return;
    logState.buf.push({ t: Date.now(), tag: tag, text: line });
  });
  if (logState.buf.length > 800) logState.buf.splice(0, logState.buf.length - 800);
}

function touchClient(id) {
  if (!id || id === 'unknown') return;
  clients.set(id, Date.now());
  if (clients.size > 500) {
    const arr = Array.from(clients.entries()).sort(function (a, b) { return a[1] - b[1]; });
    for (let i = 0; i < arr.length - 300; i++) clients.delete(arr[i][0]);
  }
}

function onlineClients() {
  const now = Date.now();
  let n = 0;
  clients.forEach(function (t) { if (now - t < 60000) n++; });
  return n;
}

function clientIp(req) {
  let ip = (req.socket && req.socket.remoteAddress) || '';
  if (ip.indexOf('::ffff:') === 0) ip = ip.slice(7);
  if (ip === '::1') ip = '127.0.0.1';
  return ip || 'unknown';
}

function isLocalIp(ip) { return ip === '127.0.0.1' || ip === '::1'; }

function parseQuery(url) {
  const q = {};
  const i = String(url).indexOf('?');
  if (i === -1) return q;
  String(url).slice(i + 1).split('&').forEach(function (pair) {
    if (!pair) return;
    const j = pair.indexOf('=');
    const k = j === -1 ? pair : pair.slice(0, j);
    const v = j === -1 ? '' : pair.slice(j + 1);
    try { q[decodeURIComponent(k)] = decodeURIComponent(v); } catch (e) { q[k] = v; }
  });
  return q;
}

function lanIps() {
  const out = [];
  try {
    const ifs = os.networkInterfaces();
    Object.keys(ifs).forEach(function (name) {
      (ifs[name] || []).forEach(function (a) {
        if (a && a.family === 'IPv4' && !a.internal) out.push(a.address);
      });
    });
  } catch (e) { /* ignore */ }
  return out;
}

function send(res, code, obj, extraHeaders) {
  const headers = Object.assign({ 'Content-Type': 'application/json; charset=utf-8' }, extraHeaders || {});
  try {
    res.writeHead(code, headers);
    res.end(JSON.stringify(obj));
  } catch (e) { /* 连接已断开，忽略 */ }
}

/* ---------- 时间解析（关键：ERP 的 updatedAt 格式混用） ---------- */
function parseTime(v) {
  if (v === null || v === undefined || v === '') return 0;
  if (typeof v === 'number') {
    if (!isFinite(v) || v <= 0) return 0;
    return v < 1e11 ? Math.round(v * 1000) : Math.round(v);   // 秒 -> 毫秒
  }
  if (v instanceof Date) return v.getTime();
  const s = String(v).trim();
  if (!s) return 0;
  if (/^\d{10}$/.test(s)) return parseInt(s, 10) * 1000;
  if (/^\d{13}$/.test(s)) return parseInt(s, 10);
  const t = Date.parse(s);
  if (!isNaN(t)) return t;
  // 兜底：中文「上午/下午」等形式
  const m = s.match(/^(\d{4})\D+(\d{1,2})\D+(\d{1,2})\D*\s*(上午|下午|AM|PM)?\s*(\d{1,2})?[:：]?(\d{1,2})?[:：]?(\d{1,2})?/i);
  if (m) {
    let h = parseInt(m[5] || '0', 10);
    const ap = m[4] || '';
    if ((ap === '下午' || /^pm$/i.test(ap)) && h < 12) h += 12;
    if ((ap === '上午' || /^am$/i.test(ap)) && h === 12) h = 0;
    const d = new Date(+m[1], +m[2] - 1, +m[3], h, parseInt(m[6] || '0', 10), parseInt(m[7] || '0', 10));
    const tt = d.getTime();
    return isNaN(tt) ? 0 : tt;
  }
  return 0;
}

function recTime(r) {
  if (!r || typeof r !== 'object') return 0;
  const cands = [r.updatedAt, r.updated_at, r.updateTime, r.modifyTime, r.modifiedAt, r.mtime, r.clearedAt, r.createdAt];
  for (let i = 0; i < cands.length; i++) {
    const t = parseTime(cands[i]);
    if (t) return t;
  }
  return 0;
}

/* 内容指纹忽略：同步元字段、时间字段，以及会被服务器重映射的 id。
   id 必须忽略——下发的快照可能对撞车的 id 做过重映射，
   若把 id 计入指纹，同一条记录会被误判成「内容不同」而反复触发更新。 */
const FP_SKIP = {
  id: 1, _uid: 1, _origin: 1, _lastSrc: 1, _srvAt: 1, __vn: 1,
  updatedAt: 1, updated_at: 1, updateTime: 1, modifyTime: 1, modifiedAt: 1, mtime: 1
};

function fp(rec) {
  const keys = Object.keys(rec).filter(function (k) { return !FP_SKIP[k]; }).sort();
  const o = {};
  keys.forEach(function (k) { o[k] = rec[k]; });
  let s;
  try { s = JSON.stringify(o); } catch (e) { s = String(o); }
  return crypto.createHash('sha1').update(s).digest('hex');
}

function isTomb(r) {
  return !!(r && (r._deleted === true || r.deleted === true || r.isDeleted === true));
}

function oiKey(rec, ip) {
  if (!rec || typeof rec !== 'object') return null;
  const id = rec.id;
  if (id === undefined || id === null || id === '') return null;
  return String(rec._origin || ip || 'unknown') + '#' + String(id);
}

/* ---------- 持久化 ---------- */
function rotateBackup() {
  try {
    if (!fs.existsSync(DATA_FILE)) return;
    const list = fs.existsSync(BACKUP_DIR)
      ? fs.readdirSync(BACKUP_DIR).filter(function (f) { return /^db_\d+\.json$/.test(f); }).sort()
      : [];
    const last = list[list.length - 1];
    if (last && Date.now() - parseInt(last.slice(3, -5), 10) < 5 * 60 * 1000) return;
    if (!fs.existsSync(BACKUP_DIR)) fs.mkdirSync(BACKUP_DIR, { recursive: true });
    fs.copyFileSync(DATA_FILE, path.join(BACKUP_DIR, 'db_' + Date.now() + '.json'));
    const now = fs.readdirSync(BACKUP_DIR).filter(function (f) { return /^db_\d+\.json$/.test(f); }).sort();
    while (now.length > 20) {
      try { fs.unlinkSync(path.join(BACKUP_DIR, now.shift())); } catch (e) { /* ignore */ }
    }
  } catch (e) { /* 备份失败不影响主流程 */ }
}

function load() {
  try {
    if (!fs.existsSync(DATA_FILE)) {
      console.log('[LOAD] ' + DATA_FILE + ' 不存在，创建空数据库');
      save();
      return;
    }
    const parsed = JSON.parse(fs.readFileSync(DATA_FILE, 'utf-8'));
    if (!parsed || typeof parsed !== 'object' || !parsed.data) {
      console.warn('[LOAD] 数据文件结构异常，按空库启动（原文件不会被覆盖）');
      return;
    }
    const data = emptyData();
    Object.keys(parsed.data).forEach(function (k) {
      const nk = normKey(k);
      if (nk && Array.isArray(parsed.data[k])) data[nk] = data[nk].concat(parsed.data[k]);
    });
    store = {
      __ver: parsed.__ver || 0,
      __devices: (parsed.__devices && typeof parsed.__devices === 'object') ? parsed.__devices : {},
      __merged: parsed.__merged || 0,
      __lastMerge: parsed.__lastMerge || 0,
      data: data
    };
    let fixed = 0;
    SERVER_KEYS.forEach(function (k) {
      store.data[k] = store.data[k].filter(function (r) { return r && typeof r === 'object'; });
      store.data[k].forEach(function (r) {
        if (!r._uid) { r._uid = genUid(); fixed++; }
        if (!r._origin) r._origin = 'legacy';
        if (!r._lastSrc) r._lastSrc = r._origin;
      });
    });
    console.log('[LOAD] 已加载 ' + DATA_FILE + ' (v' + store.__ver + ')');
    console.log('       ' + SERVER_KEYS.map(function (k) {
      return KEY_LABELS[k] + ' ' + store.data[k].length;
    }).join(' | '));
    if (fixed) console.log('       [MIGRATE] 已为 ' + fixed + ' 条历史记录补全全局唯一键 _uid');
  } catch (e) {
    console.error('[LOAD] 读取数据文件失败：' + e.message);
    console.error('       将使用空数据库启动，原数据文件不会被覆盖');
  }
}

function save() {
  try {
    rotateBackup();
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(store), 'utf-8');
    fs.renameSync(tmp, DATA_FILE);
  } catch (e) {
    console.error('[SAVE] 写入失败：' + e.message);
  }
}

function readBody(req) {
  return new Promise(function (resolve, reject) {
    const chunks = [];
    let size = 0;
    req.on('data', function (c) {
      size += c.length;
      if (size > MAX_BODY) {
        reject(new Error('PAYLOAD_TOO_LARGE'));
        try { req.destroy(); } catch (e) { /* ignore */ }
        return;
      }
      chunks.push(c);
    });
    req.on('end', function () {
      const buf = Buffer.concat(chunks).toString('utf-8');
      if (!buf) { resolve({}); return; }
      try { resolve(JSON.parse(buf)); } catch (e) { reject(new Error('INVALID_JSON')); }
    });
    req.on('error', reject);
  });
}

/* ---------- 记录规范化：补全全局唯一键与来源标记 ---------- */
function normalizeRec(rec, sr, ctx, contentFromClient) {
  const base = rec && typeof rec === 'object' ? rec : {};
  const origin = base._origin || (sr && sr._origin) || ctx.ip || 'unknown';
  /* _lastSrc 表示「内容最后一次真正发生变更的设备」：
     客户端只是把服务器下发的快照原样推回来时（最终采用服务器版本），
     不能算作该设备修改，否则会让后续删除检测误判成「别人创建的记录是我删的」。 */
  const lastSrc = contentFromClient
    ? (ctx.ip || origin)
    : ((sr && sr._lastSrc) || (sr && sr._origin) || origin);
  const out = {
    _uid: base._uid || (sr && sr._uid) || genUid(),
    _origin: origin,
    _lastSrc: lastSrc,
    _srvAt: ctx.now
  };
  Object.keys(base).forEach(function (k) {
    if (k === '_uid' || k === '_origin' || k === '_lastSrc' || k === '_srvAt') return;
    out[k] = base[k];
  });
  if (!out.updatedAt) out.updatedAt = new Date(recTime(base) || ctx.now).toISOString();
  return out;
}

/* ---------- 单表合并 ----------
   以服务器现有顺序为基准遍历：已存在的记录保持原位置，客户端新增的追加到末尾。
   这样前端每次拉取看到的记录顺序稳定，不会因合并而抖动。 */
function mergeTable(key, clientRows, srvRows, ctx) {
  const srvByUid = new Map();
  const srvByOI = new Map();
  srvRows.forEach(function (r) {
    if (!r || typeof r !== 'object') return;
    if (r._uid) srvByUid.set(String(r._uid), r);
    const oi = oiKey(r);
    if (oi && !srvByOI.has(oi)) srvByOI.set(oi, r);
  });

  const cliByUid = new Map();
  const cliByOI = new Map();
  clientRows.forEach(function (cr) {
    if (!cr || typeof cr !== 'object') return;
    if (cr._uid) cliByUid.set(String(cr._uid), cr);
    const oi = oiKey(cr, ctx.ip);
    if (oi && !cliByOI.has(oi)) cliByOI.set(oi, cr);
  });

  const out = [];
  const matched = new Set();
  let added = 0, updated = 0, kept = 0, srvWin = 0, deleted = 0;

  /* 1) 服务器已有记录：保持原有顺序 */
  srvRows.forEach(function (sr) {
    if (!sr || typeof sr !== 'object') return;
    let cr = null;
    if (sr._uid && cliByUid.has(String(sr._uid))) cr = cliByUid.get(String(sr._uid));
    if (!cr) {
      const oi = oiKey(sr);
      if (oi && cliByOI.has(oi)) cr = cliByOI.get(oi);
    }

    if (cr) {
      matched.add(cr);
      const ct = recTime(cr);
      const st = recTime(sr);
      let pickClient = false, same = false;
      if (ct > st) pickClient = true;
      else if (ct < st) pickClient = false;
      else {
        const cf = fp(cr);
        const sf = fp(sr);
        if (cf === sf) { pickClient = false; same = true; }
        else pickClient = cf > sf;   // 同时间戳时按指纹定序，保证两端收敛到同一结果
      }
      if (pickClient) updated++;
      else if (same) kept++;
      else srvWin++;
      out.push(normalizeRec(pickClient ? cr : sr, sr, ctx, pickClient));
    } else {
      /* 客户端本次未上报这条：判断是否属于本设备的删除 */
      if (isTomb(sr)) { out.push(sr); return; }
      const owner = sr._origin || 'unknown';
      const lastSrc = sr._lastSrc || owner;
      const mine = !!ctx.ip && (owner === ctx.ip || lastSrc === ctx.ip);
      if (ctx.deleteMode === 'all' && ctx.rowsProvided) { deleted++; return; }
      if (ctx.deleteMode === 'owner' && ctx.clientHasRows && mine) { deleted++; return; }
      out.push(sr);
    }
  });

  /* 2) 客户端新增记录（服务器尚未持有）追加到末尾 */
  clientRows.forEach(function (cr) {
    if (!cr || typeof cr !== 'object') return;
    if (matched.has(cr)) return;
    out.push(normalizeRec(cr, null, ctx, true));
    added++;
  });

  return { rows: out, added: added, updated: updated, kept: kept, srvWin: srvWin, deleted: deleted };
}

/* ---------- 全量合并 ---------- */
function mergeAll(clientData, ip, opts) {
  const now = Date.now();
  const result = emptyData();
  const stats = {};
  let added = 0, updated = 0, deleted = 0, srvWin = 0, provided = 0;

  SERVER_KEYS.forEach(function (key) {
    const srvRows = store.data[key] || [];
    if (!opts.providedKeys.has(key)) {
      result[key] = srvRows;
      stats[key] = { count: srvRows.length, skipped: true };
      return;
    }
    provided++;
    const clientRows = clientData[key] || [];
    const r = mergeTable(key, clientRows, srvRows, {
      ip: ip,
      now: now,
      deleteMode: opts.deleteMode,
      rowsProvided: true,
      clientHasRows: clientRows.some(function (x) { return !isTomb(x); })
    });
    result[key] = r.rows;
    added += r.added; updated += r.updated; deleted += r.deleted; srvWin += r.srvWin;
    stats[key] = {
      added: r.added, updated: r.updated, kept: r.kept,
      deleted: r.deleted, serverWin: r.srvWin, count: r.rows.length
    };
  });

  return {
    data: result, stats: stats, added: added, updated: updated,
    deleted: deleted, serverWin: srvWin, provided: provided
  };
}

/* ---------- 下发快照：保证同表内 id 唯一 ---------- */
function ensureUniqueIds(rows) {
  const used = new Set();
  let maxNum = 0;
  rows.forEach(function (r) {
    if (r && typeof r === 'object' && typeof r.id === 'number' && isFinite(r.id) && r.id > maxNum) maxNum = r.id;
  });
  const out = [];
  let remapped = 0;
  rows.forEach(function (r) {
    if (!r || typeof r !== 'object') return;
    let id = r.id;
    if (id === undefined || id === null || id === '') {
      id = ++maxNum;
      r = Object.assign({}, r, { id: id });
      remapped++;
    }
    if (used.has(typeof id + ':' + String(id))) {
      id = ++maxNum;
      r = Object.assign({}, r, { id: id });
      remapped++;
    }
    used.add(typeof id + ':' + String(id));
    out.push(r);
  });
  return { rows: out, remapped: remapped };
}

function buildSnapshot(ip, opts) {
  const out = emptyData();
  const stats = {};
  const registered = !!store.__devices[ip];
  let hasData = false;
  SERVER_KEYS.forEach(function (k) { if ((store.data[k] || []).length) hasData = true; });

  const isErpClient = !!opts.clientId && opts.clientId !== 'unknown';
  if (opts.guard && isErpClient && !registered && hasData) {
    return {
      data: out, stats: stats, guarded: true, registered: false, remapped: 0,
      message: '本机设备尚未向服务器推送过数据。为保护该设备本机已有数据不被覆盖，'
        + '服务器暂不下发数据。请先点击「推送本机数据」完成首次合并，之后拉取即可获得全量数据。'
    };
  }

  let remapped = 0;
  SERVER_KEYS.forEach(function (key) {
    const live = (store.data[key] || []).filter(function (r) { return !isTomb(r); });
    if (live.length === 0 && !opts.full) {
      stats[key] = { count: 0, omitted: true };
      return;
    }
    const u = ensureUniqueIds(live);
    out[key] = u.rows;
    remapped += u.remapped;
    stats[key] = { count: u.rows.length, omitted: false };
  });

  return { data: out, stats: stats, guarded: false, registered: registered, remapped: remapped };
}

/* ---------- 监听管理（启动/停止 = 切换监听范围） ---------- */
function listenOn(host) {
  return new Promise(function (resolve, reject) {
    function start(h) {
      const onErr = function (e) {
        if (!h && (e.code === 'EAFNOSUPPORT' || e.code === 'EADDRNOTAVAIL' || e.code === 'EINVAL')) {
          server.removeListener('error', onErr);
          start('0.0.0.0');
          return;
        }
        reject(e);
      };
      server.once('error', onErr);
      const cb = function () {
        server.removeListener('error', onErr);
        CURRENT_HOST = h || null;
        resolve(true);
      };
      if (h) server.listen(PORT, h, cb);
      else server.listen(PORT, cb);
    }

    if (server.listening) {
      try { if (server.closeIdleConnections) server.closeIdleConnections(); } catch (e) { /* ignore */ }
      const t = setTimeout(function () {
        try { if (server.closeAllConnections) server.closeAllConnections(); } catch (e) { /* ignore */ }
      }, 400);
      server.close(function () {
        clearTimeout(t);
        start(host);
      });
    } else {
      start(host);
    }
  });
}

function serverStatus() {
  return {
    running: true,
    lan: CURRENT_HOST !== '127.0.0.1',
    host: CURRENT_HOST || '0.0.0.0',
    pid: process.pid,
    port: PORT,
    uptime: Date.now() - STARTED_AT,
    outputLines: logState.buf.length,
    pushCount: logState.pushCount,
    pullCount: logState.pullCount,
    clients: onlineClients(),
    ver: store.__ver
  };
}

/* ---------- 打开浏览器 ---------- */
function openBrowser(url) {
  const platform = process.platform;
  let cmd;
  if (platform === 'darwin') cmd = ['open', [url]];
  else if (platform === 'win32') cmd = ['cmd', ['/c', 'start', '""', url]];
  else cmd = ['xdg-open', [url]];

  try {
    const child = cp.spawn(cmd[0], cmd[1], { stdio: 'ignore', detached: true });
    child.on('error', function (err) {
      console.log('[OPEN] 无法自动打开浏览器：' + err.message + '，请手动访问 ' + url);
    });
    child.unref();
  } catch (e) {
    console.log('[OPEN] 自动打开失败，请手动访问 ' + url);
  }
}

/* ---------- 可视化监控面板（内置，单文件零依赖） ---------- */
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>广宏鞋业ERP · 协同服务器监控</title>
<style>
  :root{
    --bg:#0b1020; --panel:#121933; --panel2:#0f1529; --line:#1e2945;
    --txt:#e6ecff; --sub:#8a98bd; --acc:#4f8cff; --acc2:#38d6c4;
    --ok:#2ecc71; --warn:#f5a623; --err:#ff5a5f; --purple:#a06bff;
  }
  *{box-sizing:border-box}
  body{margin:0;font-family:"Segoe UI","Microsoft YaHei",system-ui,sans-serif;
    background:radial-gradient(1200px 600px at 80% -10%,#1a2547 0,transparent 60%),
    radial-gradient(900px 500px at -10% 10%,#15233f 0,transparent 55%),var(--bg);
    color:var(--txt);min-height:100vh;padding:20px 24px 40px}
  h1,h2,h3{margin:0}
  .top{display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:18px}
  .logo{width:42px;height:42px;border-radius:11px;
    background:linear-gradient(135deg,var(--acc),var(--purple));
    display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:800;
    box-shadow:0 6px 20px rgba(79,140,255,.35)}
  .title{font-size:19px;font-weight:700;letter-spacing:.5px}
  .title small{display:block;color:var(--sub);font-size:12px;font-weight:400;margin-top:2px;letter-spacing:0}
  .statusbar{flex:1;min-width:260px;display:flex;align-items:center;gap:10px;
    background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:10px 14px}
  .dot{width:10px;height:10px;border-radius:50%;background:var(--sub);flex-shrink:0;
    box-shadow:0 0 0 0 currentColor;transition:.3s}
  .dot.ok{background:var(--ok);animation:pulse 2s infinite}
  .dot.warn{background:var(--warn)}
  .dot.err{background:var(--err);animation:pulse 1s infinite}
  @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(46,204,113,.5)}70%{box-shadow:0 0 0 9px rgba(46,204,113,0)}100%{box-shadow:0 0 0 0 rgba(46,204,113,0)}}
  .statmsg{font-size:13px;color:var(--sub)}
  .statmsg b{color:var(--txt)}
  .addr{display:flex;align-items:center;gap:8px;background:var(--panel);
    border:1px solid var(--line);border-radius:12px;padding:6px 10px;flex-wrap:wrap}
  .addr label{font-size:12px;color:var(--sub);white-space:nowrap}
  .addr input{background:var(--panel2);border:1px solid var(--line);color:var(--txt);
    border-radius:8px;padding:7px 10px;font-size:13px;font-family:Consolas,monospace;
    width:250px;outline:none}
  .addr input:focus{border-color:var(--acc)}
  .btn{border:1px solid var(--line);background:var(--panel2);color:var(--txt);
    border-radius:9px;padding:8px 14px;font-size:13px;cursor:pointer;transition:.15s;white-space:nowrap}
  .btn:hover{border-color:var(--acc);color:var(--acc)}
  .btn.primary{background:linear-gradient(135deg,var(--acc),var(--purple));border-color:transparent;color:#fff;font-weight:600}
  .btn.primary:hover{color:#fff;filter:brightness(1.1);transform:translateY(-1px)}
  .btn.danger{color:var(--err);border-color:rgba(255,90,95,.35)}
  .btn:disabled{opacity:.4;cursor:not-allowed;transform:none!important}
  .grid{display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));margin-bottom:18px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:16px 18px;position:relative;overflow:hidden}
  .card::after{content:"";position:absolute;right:-20px;top:-20px;width:80px;height:80px;
    background:radial-gradient(circle,rgba(79,140,255,.15),transparent 70%)}
  .card .k{font-size:12px;color:var(--sub);margin-bottom:8px;letter-spacing:1px}
  .card .v{font-size:28px;font-weight:700;font-variant-numeric:tabular-nums}
  .card .u{font-size:12px;color:var(--sub);margin-left:4px}
  .card.acc .v{color:var(--acc)} .card.ok .v{color:var(--ok)}
  .card.warn .v{color:var(--warn)} .card.purple .v{color:var(--purple)}
  .layout{display:grid;gap:16px;grid-template-columns:1.15fr 1fr}
  @media(max-width:1080px){.layout{grid-template-columns:1fr}}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:16px 18px}
  .panel h2{font-size:14px;color:var(--sub);letter-spacing:1px;text-transform:uppercase;
    margin-bottom:14px;display:flex;align-items:center;gap:8px}
  .panel h2::before{content:"";width:3px;height:14px;border-radius:2px;background:var(--acc)}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th{text-align:left;color:var(--sub);font-weight:500;font-size:11px;letter-spacing:1px;
    text-transform:uppercase;padding:8px 10px;border-bottom:1px solid var(--line)}
  td{padding:10px;border-bottom:1px solid rgba(30,41,69,.6);font-variant-numeric:tabular-nums}
  .pill{display:inline-block;padding:3px 9px;border-radius:20px;font-size:11px;font-weight:600}
  .pill.on{background:rgba(46,204,113,.15);color:var(--ok)}
  .pill.idle{background:rgba(245,166,35,.15);color:var(--warn)}
  .pill.off{background:rgba(138,152,189,.15);color:var(--sub)}
  .bar{height:6px;border-radius:4px;background:var(--panel2);overflow:hidden;min-width:60px}
  .bar i{display:block;height:100%;border-radius:4px;transition:width .4s}
  .chart{width:100%;height:150px}
  .logpanel{margin-top:18px}
  .loghead{display:flex;align-items:center;gap:10px;margin-bottom:12px;flex-wrap:wrap}
  .log{background:#080c1a;border:1px solid var(--line);border-radius:12px;padding:12px 14px;
    height:300px;overflow-y:auto;font-family:Consolas,"Cascadia Code",monospace;font-size:12px;
    line-height:1.7}
  .log .l{white-space:pre-wrap;word-break:break-all}
  .log .t{color:var(--sub)}
  .log .tag{display:inline-block;padding:0 5px;border-radius:4px;font-size:11px;margin-right:6px}
  .log .tag.out{background:rgba(79,140,255,.15);color:var(--acc)}
  .log .tag.err{background:rgba(255,90,95,.15);color:var(--err)}
  .log .tag.info{background:rgba(160,107,255,.15);color:var(--purple)}
  .log .tag.get{background:rgba(56,214,196,.15);color:var(--acc2)}
  .log .tag.post{background:rgba(79,140,255,.15);color:var(--acc)}
  .filters input{background:var(--panel2);border:1px solid var(--line);color:var(--txt);
    border-radius:8px;padding:6px 10px;font-size:12px;outline:none;width:160px}
  .toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:14px}
  .hint{font-size:12px;color:var(--sub);background:rgba(245,166,35,.08);
    border:1px solid rgba(245,166,35,.25);border-radius:10px;padding:10px 14px;margin-top:16px;line-height:1.7}
  .hint code{background:var(--panel2);padding:1px 6px;border-radius:5px;color:var(--acc2);font-family:monospace}
  .diag{margin-top:14px;font-size:13px}
  .diag .row{display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px dashed var(--line)}
  .diag .ck{width:20px;height:20px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;flex-shrink:0}
  .diag .ck.y{background:rgba(46,204,113,.15);color:var(--ok)}
  .diag .ck.n{background:rgba(255,90,95,.15);color:var(--err)}
  .diag .ck.p{background:rgba(245,166,35,.15);color:var(--warn)}
  .footer{margin-top:24px;text-align:center;color:var(--sub);font-size:12px}
  .hidden{display:none}
  ::-webkit-scrollbar{width:8px;height:8px}
  ::-webkit-scrollbar-thumb{background:var(--line);border-radius:4px}
</style>
</head>
<body>

<div class="top">
  <div class="logo">鞋</div>
  <div class="title">广宏鞋业ERP · 协同服务器监控
    <small>局域网数据中枢 · 可视化面板 v3.0</small>
	<small>提示：新电脑首次连接后，请先点「推送本机数据」完成首次合并，再拉取全量</small>
  </div>
  <div class="statusbar">
    <span class="dot" id="dot"></span>
    <span class="statmsg" id="statmsg">正在连接…</span>
  </div>
  <div class="addr">
    <label>服务器地址</label>
    <input id="addr" type="text" spellcheck="false" placeholder="http://localhost:7001">
    <button class="btn" id="btnTest">测试连接</button>
    <button class="btn primary" id="btnStart">▶ 启动服务器</button>
    <button class="btn danger hidden" id="btnStop">⏹ 停止</button>
  </div>
</div>

<div class="grid">
  <div class="card acc"><div class="k">数据版本 v3.0</div><div class="v" id="cVer">–</div></div>
  <div class="card ok"><div class="k">在线客户端</div><div class="v" id="cClients">0</div></div>
  <div class="card warn"><div class="k">累计合并次数</div><div class="v" id="cMerged">0</div></div>
  <div class="card purple"><div class="k">服务器运行时长</div><div class="v" id="cUptime">–</div></div>
  <div class="card"><div class="k">总记录数</div><div class="v" id="cTotal">0</div></div>
  <div class="card acc"><div class="k">上次合并时间</div><div class="v" id="cLastMerge" style="font-size:16px">–</div></div>
</div>

<div class="layout">
  <div class="panel">
    <h2>客户端连接</h2>
    <table>
      <thead><tr><th>客户端 ID</th><th>最近心跳</th><th>距上次</th><th>状态</th><th>活跃度</th></tr></thead>
      <tbody id="clientBody"><tr><td colspan="5" style="color:var(--sub);text-align:center;padding:24px">暂无数据</td></tr></tbody>
    </table>
  </div>
  <div class="panel">
    <h2>数据构成</h2>
    <table>
      <thead><tr><th>数据集</th><th>条数</th><th>占比</th></tr></thead>
      <tbody id="dataBody"></tbody>
    </table>
    <div style="margin-top:16px">
      <h2 style="margin-bottom:8px">合并趋势</h2>
      <svg class="chart" id="chart" viewBox="0 0 400 150" preserveAspectRatio="none"></svg>
    </div>
  </div>
</div>

<div class="panel logpanel">
  <h2>服务器启动输出</h2>
  <div class="loghead">
    <button class="btn" id="btnRefresh">⟳ 刷新输出</button>
    <button class="btn" id="btnClear">清空日志</button>
    <button class="btn" id="btnExport">导出 CSV</button>
    <span style="flex:1"></span>
    <label style="font-size:12px;color:var(--sub)"><input type="checkbox" id="autoScroll" checked> 自动滚动</label>
  </div>
  <div class="log" id="log"></div>
</div>

<div class="panel logpanel">
  <h2>连接诊断</h2>
  <div class="diag" id="diag">
    <div class="row"><span class="ck p">?</span><span>点击「开始诊断」检测连接链路</span></div>
  </div>
  <div class="toolbar">
    <button class="btn primary" id="btnDiag">开始诊断</button>
  </div>
</div>

<div class="hint">
  <b>使用提示</b><br>
  • 本提示由袁博文小朋友提供，如有不解的地方，请自行摸索。<br>
  • 地址栏可直接修改，回车或失焦自动保存（存于本机浏览器）。<br>
  • 「▶ 启动服务器」仅在<b>与服务器同一台机器</b>上可用，远程模式只做监控。<br>
  • 端口占用：在 CMD 执行 <code>netstat -ano | findstr :7001</code> 找到占用进程并结束。<br>
  • 本机首次启动如提示 Node 未安装，请先到 <code>https://nodejs.org</code> 安装 LTS 版。
</div>

<div class="footer">广宏鞋业ERP协同服务器 · 数据合并采用「按 updatedAt 取新 + 版本号兜底」策略 · 所有操作均在内存中安全合并后落盘</div>

<script>
(function(){
  "use strict";
  var STORE_KEY = 'gh_erp_addr';
  var addrEl = document.getElementById('addr');
  var defaultAddr = 'http://' + (location.hostname || 'localhost') + ':7001';
  addrEl.value = localStorage.getItem(STORE_KEY) || defaultAddr;

  function persist(){ localStorage.setItem(STORE_KEY, addrEl.value.trim()); }

  var lastVer = null;
  var mergePoints = [];
  var mergeCount = 0;
  var lastMergeTime = '–';
  var clientMap = {};
  var logEntries = [];
  var autoTimer = null;

  function normalizeUrl(input){
    var v = (input || '').trim();
    if(!v) return '';
    if(!/^https?:\\/\\//i.test(v)) v = 'http://' + v;
    try{ var u = new URL(v); return u.origin; }catch(e){ return v.replace(/\\/+$/,''); }
  }
  function origin(){ return normalizeUrl(addrEl.value); }

  function setStatus(state, text){
    var dot = document.getElementById('dot');
    dot.className = 'dot ' + state;
    document.getElementById('statmsg').innerHTML = text;
  }

  function fmtAgo(ts){
    if(!ts) return '–';
    var s = Math.floor((Date.now() - ts) / 1000);
    if(s < 5) return '刚刚';
    if(s < 60) return s + ' 秒前';
    if(s < 3600) return Math.floor(s/60) + ' 分钟前';
    return Math.floor(s/3600) + ' 小时前';
  }
  function fmtUptime(ms){
    if(!ms || ms < 0) return '–';
    var s = Math.floor(ms/1000);
    var h = Math.floor(s/3600), m = Math.floor(s%3600/60), sec = s%60;
    if(h > 0) return h + '小时' + m + '分';
    if(m > 0) return m + '分' + sec + '秒';
    return sec + ' 秒';
  }

  function classifyClient(id, ts){
    var age = ts ? (Date.now() - ts)/1000 : 99999;
    if(age < 30) return ['on','在线'];
    if(age < 120) return ['idle','空闲'];
    return ['off','离线'];
  }

  function renderClients(){
    var ids = Object.keys(clientMap);
    var body = document.getElementById('clientBody');
    if(ids.length === 0){
      body.innerHTML = '<tr><td colspan="5" style="color:var(--sub);text-align:center;padding:24px">暂无客户端活动记录</td></tr>';
      return;
    }
    ids.sort(function(a,b){ return (clientMap[b].last||0) - (clientMap[a].last||0); });
    var html = '';
    ids.forEach(function(id){
      var c = clientMap[id];
      var st = classifyClient(id, c.last);
      var pct = Math.max(2, Math.min(100, 100 - Math.min(99, ((Date.now()-(c.last||0))/1000)/40)));
      html += '<tr>' +
        '<td style="font-family:monospace;font-size:12px">' + esc(id) + '</td>' +
        '<td style="color:var(--sub)">' + new Date(c.last||0).toLocaleTimeString('zh-CN',{hour12:false}) + '</td>' +
        '<td>' + fmtAgo(c.last) + '</td>' +
        '<td><span class="pill ' + st[0] + '">' + st[1] + '</span></td>' +
        '<td><div class="bar"><i style="width:' + pct.toFixed(0) + '%;background:' +
          (st[0]==='on'?'var(--ok)':st[0]==='idle'?'var(--warn)':'var(--sub)') + '"></i></div></td>' +
        '</tr>';
    });
    body.innerHTML = html;
  }

  function esc(s){
    return String(s).replace(/[&<>"]/g, function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]; });
  }

  function renderData(stats){
    var body = document.getElementById('dataBody');
    var total = 0;
    stats.forEach(function(s){ total += s.count; });
    var html = '';
    stats.forEach(function(s){
      var pct = total ? (s.count/total*100).toFixed(1) : 0;
      html += '<tr><td>' + s.name + '</td><td style="font-weight:600">' + s.count + '</td>' +
        '<td><div class="bar"><i style="width:' + Math.max(2,pct*2.4).toFixed(0) +
        '%;background:linear-gradient(90deg,var(--acc),var(--purple))"></i></div></td></tr>';
    });
    body.innerHTML = html;
    document.getElementById('cTotal').textContent = total;
  }

  function renderChart(){
    var svg = document.getElementById('chart');
    var max = Math.max(5, Math.max.apply(null, mergePoints.map(function(p){return p.v;})));
    var w = 400, h = 150, pl = 28, pr = 8, pt = 10, pb = 20;
    var iw = w - pl - pr, ih = h - pt - pb;
    var step = mergePoints.length > 1 ? iw / (mergePoints.length - 1) : 0;
    var pts = mergePoints.map(function(p,i){
      return [pl + step*i, pt + ih - (p.v/max)*ih];
    });
    var grid = '';
    for(var g=0; g<=3; g++){
      var gy = pt + ih*g/3;
      grid += '<line x1="'+pl+'" y1="'+gy+'" x2="'+(w-pr)+'" y2="'+gy+'" stroke="#1e2945"/>' +
        '<text x="'+(pl-6)+'" y="'+(gy+4)+'" fill="#8a98bd" font-size="9" text-anchor="end">' +
        Math.round(max*(1-g/3)) + '</text>';
    }
    var path = pts.map(function(p,i){ return (i?'L':'M') + p[0].toFixed(1) + ',' + p[1].toFixed(1); }).join(' ');
    var area = path + ' L' + pts[pts.length-1][0].toFixed(1) + ',' + (pt+ih) + ' L' + pl + ',' + (pt+ih) + ' Z';
    var line = pts.map(function(p,i){ return (i?' L':'M') + p[0].toFixed(1) + ' ' + p[1].toFixed(1); }).join('');
    svg.innerHTML = grid +
      '<path d="' + area + '" fill="url(#grad)"/>' +
      '<path d="' + line + '" fill="none" stroke="#4f8cff" stroke-width="2"/>' +
      pts.map(function(p){ return '<circle cx="'+p[0]+'" cy="'+p[1]+'" r="3" fill="#4f8cff"/>'; }).join('') +
      '<defs><linearGradient id="grad" x1="0" y1="0" x2="0" y2="1">' +
      '<stop offset="0%" stop-color="#4f8cff" stop-opacity="0.35"/>' +
      '<stop offset="100%" stop-color="#4f8cff" stop-opacity="0"/></linearGradient></defs>';
  }

  function addLog(tag, text){
    var lines = String(text).split('\\n');
    lines.forEach(function(line){
      line = line.replace(/\\s+$/,'');
      if(!line) return;
      var e = { t: Date.now(), tag: tag, text: line };
      logEntries.push(e);
      if(logEntries.length > 500) logEntries.shift();
      var cls = (line.match(/^\\s*(GET|POST|PUT|DELETE|OPTIONS)\\b/) || [])[1];
      var displayTag = cls ? cls.toLowerCase() : tag;
      var div = document.createElement('div');
      div.className = 'l';
      div.innerHTML = '<span class="t">' + new Date(e.t).toLocaleTimeString('zh-CN',{hour12:false}) +
        '</span> <span class="tag ' + displayTag + '">' + displayTag.toUpperCase() + '</span>' + esc(line);
      var log = document.getElementById('log');
      log.appendChild(div);
      if(document.getElementById('autoScroll').checked){
        log.scrollTop = log.scrollHeight;
      }
    });
  }

  function renderLog(){
    var log = document.getElementById('log');
    log.innerHTML = '';
    logEntries.slice(-300).forEach(function(e){
      var div = document.createElement('div');
      div.className = 'l';
      div.innerHTML = '<span class="t">' + new Date(e.t).toLocaleTimeString('zh-CN',{hour12:false}) +
        '</span> <span class="tag ' + e.tag + '">' + e.tag.toUpperCase() + '</span>' + esc(e.text);
      log.appendChild(div);
    });
    if(document.getElementById('autoScroll').checked) log.scrollTop = log.scrollHeight;
  }

  function fetchJSON(url, opts){
    opts = opts || {};
    return fetch(origin() + url, {
      method: opts.method || 'GET',
      headers: opts.body ? {'Content-Type':'application/json'} : {},
      body: opts.body || null
    }).then(function(r){
      if(!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    });
  }

  function pullSnapshot(){
    return fetchJSON('/api/pull').then(function(d){
      if(d && d.data){
        document.getElementById('cVer').textContent = d.__ver;
        if(lastVer !== null && d.__ver > lastVer){
          mergeCount++;
          lastMergeTime = new Date().toLocaleTimeString('zh-CN',{hour12:false});
          mergePoints.push({ t: Date.now(), v: mergeCount });
          if(mergePoints.length > 40) mergePoints.shift();
          renderChart();
          addLog('info', '↑ 检测到版本变化 v' + lastVer + ' → v' + d.__ver + '，已合并');
        }
        lastVer = d.__ver;
        document.getElementById('cMerged').textContent = mergeCount;
        document.getElementById('cLastMerge').textContent = lastMergeTime;

        var stats = ['orders','bom','sole','uppersole','finished','last','prodGroups','prodRecords'].map(function(k){
          return { name: ({
            orders:'订单', bom:'BOM物料', sole:'大底', uppersole:'鞋面',
            finished:'成品', last:'楦头', prodGroups:'生产组', prodRecords:'生产记录'
          })[k], count: (d.data[k] || []).length };
        });
        renderData(stats);
      }
      return d;
    });
  }

  function tick(){
    fetchJSON('/api/ping').then(function(d){
      setStatus('ok', '<b>已连接</b> · 服务端时间 ' + new Date(d.serverTime).toLocaleTimeString('zh-CN',{hour12:false}) +
        ' · 数据版本 v' + d.ver);
      document.getElementById('cClients').textContent = d.clients;
      document.getElementById('cVer').textContent = d.ver;
      return pullSnapshot();
    }).then(function(){
      renderClients();
    }).catch(function(e){
      setStatus('err', '<b>未连接</b> · ' + esc(e.message) + ' · 检查地址或确认服务器已启动');
      document.getElementById('cClients').textContent = '0';
    });
  }

  /* ---- 启动 / 停止 ---- */
  function isLocal(){
    var o = origin();
    return /(localhost|127\\.0\\.0\\.1|0\\.0\\.0\\.0)(:|$)/.test(o);
  }

  function getPort(){
    var m = origin().match(/:(\\d+)$/);
    return m ? parseInt(m[1],10) : 7001;
  }

  function tryStart(){
    if(!isLocal()){
      alert('启动功能仅支持本机（localhost）。远程地址只能监控，无法代为拉起进程。');
      return;
    }
    var port = getPort();
    setStatus('warn', '正在启动服务器（端口 ' + port + '）…');
    addLog('info', '[admin] 请求启动业务服务器，端口 ' + port);
    fetchJSON('/admin/start', { method:'POST', body: JSON.stringify({ port: port }) })
      .then(function(d){
        addLog('info', '[admin] 启动结果：' + JSON.stringify(d));
        if(d.ok){ setStatus('ok','服务器已启动，PID=' + (d.pid||'?') + '，端口 ' + port); }
        else { setStatus('err','启动失败：' + esc(d.error||d.reason||'未知')); }
        toggleButtons(d.ok);
        setTimeout(tick, 600);
      })
      .catch(function(e){ setStatus('err','启动请求失败：' + esc(e.message)); });
  }

  function tryStop(){
    fetchJSON('/admin/stop', { method:'POST' })
      .then(function(d){
        addLog('info', '[admin] 停止结果：' + JSON.stringify(d));
        setStatus('warn','服务器已停止');
        toggleButtons(false);
        setTimeout(tick, 400);
      })
      .catch(function(e){ addLog('err','停止请求失败：' + esc(e.message)); });
  }

  function toggleButtons(running){
    document.getElementById('btnStart').classList.toggle('hidden', running);
    document.getElementById('btnStop').classList.toggle('hidden', !running);
  }

  function refreshOutput(){
    fetchJSON('/admin/output').then(function(d){
      if(d && Array.isArray(d.lines)){
        d.lines.forEach(function(l){ addLog(l.tag || 'out', l.text); });
        if(d.status){
          document.getElementById('cUptime').textContent = fmtUptime(d.status.uptime);
          toggleButtons(d.status.running);
          if(d.status.running) setStatus('ok','服务器运行中 PID=' + d.status.pid);
        }
      }
    }).catch(function(){});
  }

  function runDiag(){
    var box = document.getElementById('diag');
    box.innerHTML = '';
    var steps = [
      { name:'解析地址：' + origin(), fn: function(){ return Promise.resolve({ok:true}); } },
      { name:'GET /api/ping 心跳', fn: function(){ return fetchJSON('/api/ping'); } },
      { name:'GET /api/pull 拉取快照', fn: function(){ return fetchJSON('/api/pull'); } },
      { name:'GET /admin/status 进程状态', fn: function(){ return fetchJSON('/admin/status'); } }
    ];
    var i = 0;
    function next(){
      if(i >= steps.length){
        box.insertAdjacentHTML('beforeend',
          '<div class="row"><span class="ck y">✓</span><span>全部链路正常，服务器可访问</span></div>');
        return;
      }
      var s = steps[i++];
      box.insertAdjacentHTML('beforeend',
        '<div class="row"><span class="ck p">…</span><span>' + esc(s.name) + '</span></div>');
      var row = box.lastChild;
      Promise.resolve(s.fn()).then(function(){
        row.firstChild.className = 'ck y';
        row.firstChild.textContent = '✓';
        next();
      }).catch(function(e){
        row.firstChild.className = 'ck n';
        row.firstChild.textContent = '✕';
        row.lastChild.textContent = row.lastChild.textContent + ' → ' + e.message;
        var tip = document.createElement('div');
        tip.className = 'row';
        tip.style.color = 'var(--sub)';
        tip.innerHTML = '<span></span><span>排查建议：确认地址正确 / 服务器已 node server.js 启动 / 防火墙放行该端口</span>';
        box.appendChild(tip);
      });
    }
    next();
  }

  /* ---- 事件 ---- */
  addrEl.addEventListener('change', function(){ persist(); tick(); });
  addrEl.addEventListener('keydown', function(e){ if(e.key === 'Enter') addrEl.blur(); });
  document.getElementById('btnTest').addEventListener('click', tick);
  document.getElementById('btnStart').addEventListener('click', tryStart);
  document.getElementById('btnStop').addEventListener('click', tryStop);
  document.getElementById('btnRefresh').addEventListener('click', refreshOutput);
  document.getElementById('btnClear').addEventListener('click', function(){
    logEntries = []; renderLog(); addLog('info','日志已清空');
  });
  document.getElementById('btnExport').addEventListener('click', function(){
    var csv = '\\u65f6\\u95f4,\\u7c7b\\u578b,\\u5185\\u5bb9\\n' +
      logEntries.map(function(e){
        return [new Date(e.t).toISOString(), e.tag, '"'+e.text.replace(/"/g,'""')+'"'].join(',');
      }).join('\\n');
    var a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob(['\\ufeff'+csv],{type:'text/csv'}));
    a.download = 'erp-server-log-' + Date.now() + '.csv';
    a.click();
  });
  document.getElementById('btnDiag').addEventListener('click', runDiag);

  /* ---- 初始化 ---- */
  mergePoints.push({ t: Date.now(), v: 0 });
  renderChart();
  tick();
  refreshOutput();
  autoTimer = setInterval(function(){
    tick();
    if(document.getElementById('btnStop').classList.contains('hidden') === false){
      refreshOutput();
    }
  }, 3000);
})();
</script>
</body>
</html>`;
/* ---------- HTTP 路由 ---------- */
const server = http.createServer(async function (req, res) {
  const ip = clientIp(req);
  const clientId = req.headers['x-client-id'] || 'unknown';
  const method = req.method || 'GET';
  const rawUrl = req.url || '/';
  const pathOnly = rawUrl.split('?')[0];
  const query = parseQuery(rawUrl);

  touchClient(clientId);

  /* 跨域：ERP 页面多为 file:// 打开，属跨源请求 */
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Client-Id, X-Requested-With, Accept, Origin');
  res.setHeader('Access-Control-Max-Age', '86400');

  if (method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  try {
    /* ---------- 图标 ---------- */
    if (pathOnly === '/favicon.ico') { res.writeHead(204); res.end(); return; }

    /* ---------- 首页 ---------- */
    if (method === 'GET' && pathOnly === '/') {
      const ips = lanIps();
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end('<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">'
        + '<title>广宏鞋业ERP · 协同服务器</title>'
        + '<style>body{font-family:"Microsoft YaHei",system-ui,sans-serif;background:#0b1020;color:#e6ecff;padding:40px;line-height:1.9}'
        + 'h1{font-size:20px}code{background:#121933;padding:2px 8px;border-radius:6px;color:#38d6c4}'
        + 'a{color:#4f8cff}</style></head><body>'
        + '<h1>广宏鞋业ERP · 协同服务器运行中</h1>'
        + '<p>数据版本：<code>v' + store.__ver + '</code>　监听端口：<code>' + PORT + '</code></p>'
        + '<p>本机监控面板：<a href="/admin/dashboard">/admin/dashboard</a></p>'
        + '<p>ERP 客户端连接地址：<code>http://' + (ips[0] || '本机IP') + ':' + PORT + '</code></p>'
        + '</body></html>');
    }

    /* ---------- 心跳（ERP syncLanPing） ---------- */
    if (method === 'GET' && pathOnly === '/api/ping') {
      return send(res, 200, {
        ok: true,
        clients: onlineClients(),
        serverTime: Date.now(),
        ver: store.__ver,
        port: PORT,
        mode: CURRENT_HOST === '127.0.0.1' ? 'local' : 'lan'
      });
    }

    /* ---------- 拉取（ERP syncLanPull） ---------- */
    if (method === 'GET' && pathOnly === '/api/pull') {
      logState.pullCount++;
      const full = query.full === '1' || query.full === 'true';
      const snap = buildSnapshot(ip, { full: full, guard: PULL_GUARD, clientId: clientId });

      if (snap.guarded) {
        pushLog('get', 'PULL ← ' + ip + ' 【首次连接保护】未下发数据，需先推送本机数据');
      } else {
        const brief = SERVER_KEYS.filter(function (k) {
          return snap.stats[k] && !snap.stats[k].omitted;
        }).map(function (k) {
          return KEY_LABELS[k] + ':' + snap.stats[k].count;
        }).join(' ');
        pushLog('get', 'PULL ← ' + ip + ' 下发 v' + store.__ver
          + (brief ? ' [' + brief + ']' : ' [服务器暂无数据]')
          + (snap.remapped ? ' · 重映射id ' + snap.remapped : ''));
      }

      return send(res, 200, {
        ok: true,
        __ver: store.__ver,
        data: snap.data,
        serverTime: Date.now(),
        stats: snap.stats,
        guarded: !!snap.guarded,
        registered: !!snap.registered,
        message: snap.message || ''
      });
    }

    /* ---------- 推送（ERP syncLanPush） ---------- */
    if (method === 'POST' && pathOnly === '/api/push') {
      let body;
      try {
        body = await readBody(req);
      } catch (e) {
        const code = e.message === 'PAYLOAD_TOO_LARGE' ? 413 : 400;
        pushLog('err', 'PUSH ← ' + ip + ' 请求体异常：' + e.message);
        return send(res, code, { ok: false, error: e.message });
      }

      const raw = (body && body.data && typeof body.data === 'object' && !Array.isArray(body.data))
        ? body.data : null;
      if (!raw) return send(res, 400, { ok: false, error: 'invalid payload: "data" object required' });

      const providedKeys = new Set();
      const clientData = emptyData();
      Object.keys(raw).forEach(function (k) {
        const nk = normKey(k);
        if (!nk) return;
        providedKeys.add(nk);
        const v = raw[k];
        if (Array.isArray(v)) clientData[nk] = clientData[nk].concat(v);
        else if (v && typeof v === 'object') clientData[nk].push(v);
      });
      if (!providedKeys.size) {
        return send(res, 400, { ok: false, error: 'invalid payload: 无可用数据表（键名无法识别）' });
      }

      let deleteMode = DELETE_MODE_DEFAULT;
      if (body.options && ['none', 'owner', 'all'].indexOf(body.options.deleteMode) !== -1) {
        deleteMode = body.options.deleteMode;
      }

      const merged = mergeAll(clientData, ip, { providedKeys: providedKeys, deleteMode: deleteMode });

      store.data = merged.data;
      store.__ver++;
      store.__merged += merged.added + merged.updated;
      store.__lastMerge = Date.now();

      if (!store.__devices[ip]) store.__devices[ip] = { firstSeen: Date.now(), pushes: 0 };
      const dev = store.__devices[ip];
      dev.lastSeen = Date.now();
      dev.pushes++;
      dev.clientId = clientId;

      const devKeys = Object.keys(store.__devices);
      if (devKeys.length > 300) {
        devKeys.sort(function (a, b) {
          return (store.__devices[a].lastSeen || 0) - (store.__devices[b].lastSeen || 0);
        }).slice(0, devKeys.length - 200).forEach(function (k) { delete store.__devices[k]; });
      }

      logState.pushCount++;
      save();

      pushLog('post', 'PUSH ← ' + ip + ' 新增' + merged.added + ' 更新' + merged.updated
        + ' 删除' + merged.deleted + ' 服务器优先' + merged.serverWin + ' → v' + store.__ver);

      return send(res, 200, {
        ok: true,
        conflict: merged.serverWin > 0,
        ver: store.__ver,
        merged: merged.added + merged.updated,
        added: merged.added,
        updated: merged.updated,
        deleted: merged.deleted,
        serverWin: merged.serverWin,
        registered: true,
        stats: merged.stats
      });
    }

    /* ---------- 统计 / 健康检查 ---------- */
    if (method === 'GET' && pathOnly === '/api/stats') {
      return send(res, 200, {
        ok: true,
        ver: store.__ver,
        serverTime: Date.now(),
        port: PORT,
        mode: CURRENT_HOST === '127.0.0.1' ? 'local' : 'lan',
        uptime: Date.now() - STARTED_AT,
        clients: onlineClients(),
        pushCount: logState.pushCount,
        pullCount: logState.pullCount,
        mergedTotal: store.__merged,
        lastMerge: store.__lastMerge,
        tables: SERVER_KEYS.map(function (k) {
          return { key: k, label: KEY_LABELS[k], count: (store.data[k] || []).length };
        }),
        devices: Object.keys(store.__devices).map(function (k) {
          return Object.assign({ ip: k }, store.__devices[k]);
        }),
        lanIps: lanIps()
      });
    }

    if (method === 'GET' && (pathOnly === '/api/health' || pathOnly === '/api/version')) {
      return send(res, 200, {
        ok: true, service: 'guanghong-erp-sync', version: 'v4.0',
        port: PORT, ver: store.__ver, serverTime: Date.now()
      });
    }

    /* ---------- 管理接口（仅本机） ---------- */
    if (pathOnly.indexOf('/admin/') === 0) {
      if (!isLocalIp(ip)) {
        return send(res, 403, {
          ok: false, error: 'ADMIN_LOCAL_ONLY',
          message: '管理接口仅允许 127.0.0.1 访问'
        });
      }

      if (method === 'GET' && pathOnly === '/admin/dashboard') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        return res.end(DASHBOARD_HTML);
      }

      if (method === 'GET' && pathOnly === '/admin/status') {
        return send(res, 200, Object.assign({ ok: true }, serverStatus()));
      }

      if (method === 'POST' && pathOnly === '/admin/start') {
        if (CURRENT_HOST !== '127.0.0.1') {
          return send(res, 200, { ok: true, already: true, pid: process.pid, port: PORT, mode: 'lan' });
        }
        // 先回响应，再切换监听，避免 close 掉正在处理的本次连接
        send(res, 200, { ok: true, pid: process.pid, port: PORT, mode: 'lan', switching: true });
        setTimeout(function () {
          listenOn(null).then(function () {
            pushLog('info', '[admin] 已开放局域网访问（端口 ' + PORT + '）');
          }).catch(function (e) {
            pushLog('err', '[admin] 开放局域网失败：' + e.message);
          });
        }, 300);
        return;
      }

      if (method === 'POST' && pathOnly === '/admin/stop') {
        if (CURRENT_HOST === '127.0.0.1') {
          return send(res, 200, { ok: true, killed: false, reason: 'ALREADY_LOCAL' });
        }
        send(res, 200, { ok: true, killed: true, reason: 'LOCAL_ONLY', switching: true });
        setTimeout(function () {
          listenOn('127.0.0.1').then(function () {
            pushLog('info', '[admin] 已停止对外服务（仅本机可访问，监控面板不受影响）');
          }).catch(function (e) {
            pushLog('err', '[admin] 切换仅本机监听失败：' + e.message);
          });
        }, 300);
        return;
      }

      if (method === 'GET' && pathOnly === '/admin/output') {
        const lines = logState.buf.splice(0, logState.buf.length);
        return send(res, 200, { ok: true, lines: lines, status: serverStatus() });
      }

      return send(res, 404, { ok: false, error: 'admin endpoint not found: ' + pathOnly });
    }

    /* ---------- 404 ---------- */
    return send(res, 404, { ok: false, error: 'not found: ' + rawUrl });
  } catch (e) {
    pushLog('err', '[ERROR] ' + (e && e.message ? e.message : String(e)));
    try { send(res, 500, { ok: false, error: (e && e.message) || String(e) }); } catch (e2) { /* ignore */ }
  }
});

server.on('error', function (e) {
  if (server.listening) console.error('❌ 服务器错误：' + (e && e.message));
});

process.on('uncaughtException', function (e) {
  console.error('[UNCAUGHT] ' + ((e && e.stack) || e));
});
process.on('unhandledRejection', function (e) {
  console.error('[UNHANDLED] ' + ((e && e.message) || e));
});

/* ---------- 启动 ---------- */
load();

const bootHost = LOCAL_ONLY ? '127.0.0.1' : null;
const dashboardUrl = 'http://127.0.0.1:' + PORT + '/admin/dashboard';

listenOn(bootHost).then(function () {
  const ips = lanIps();
  pushLog('info', '═══════════════════════════════════════════════════════════');
  pushLog('info', '  广宏鞋业ERP系统 · 局域网协同服务器 v4.0');
  pushLog('info', '═══════════════════════════════════════════════════════════');
  pushLog('info', '  监听端口：' + PORT + '（' + (LOCAL_ONLY ? '仅本机' : '局域网开放') + '）');
  pushLog('info', '  数据文件：' + DATA_FILE);
  pushLog('info', '  本机监控面板：' + dashboardUrl);
  if (ips.length) {
    pushLog('info', '  ERP 客户端连接地址：' + ips.map(function (i) { return 'http://' + i + ':' + PORT; }).join(' 或 '));
  }
  pushLog('info', '  删除同步：' + DELETE_MODE_DEFAULT + '　首次连接保护：' + (PULL_GUARD ? '开' : '关'));
  pushLog('info', '  提示：新电脑首次连接后，请先点「推送本机数据」完成首次合并，再拉取全量');
  pushLog('info', '═══════════════════════════════════════════════════════════');

  if (AUTO_OPEN) {
    // 等服务端口真正可访问后再打开，避免浏览器抢跑
    const probe = function () {
      http.get('http://127.0.0.1:' + PORT + '/api/ping', function (r) {
        r.resume();
        if (r.statusCode === 200) {
          console.log('[OPEN] 正在打开监控面板：' + dashboardUrl);
          openBrowser(dashboardUrl);
        } else {
          setTimeout(probe, 300);
        }
      }).on('error', function () { setTimeout(probe, 300); });
    };
    setTimeout(probe, 300);
  } else {
    console.log('[OPEN] 已按 --no-open 跳过自动打开浏览器，请手动访问：');
    console.log('       ' + dashboardUrl);
  }
}).catch(function (e) {
  if (e && e.code === 'EADDRINUSE') {
    console.error('❌ 端口 ' + PORT + ' 已被占用，请换一个端口或关闭占用程序');
    console.error('   排查：netstat -ano | findstr :' + PORT);
  } else {
    console.error('❌ 服务器启动失败：' + ((e && e.message) || e));
  }
  process.exit(1);
});
