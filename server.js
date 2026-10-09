/* ==================================================================
   server.js —— 广宏鞋业ERP系统 局域网协同数据中枢
   ------------------------------------------------------------------
   用法：
     node server.js [port]       默认端口 7001
     node server.js 7001
   数据文件：shoe_erp.server.db.json（与 .html 同目录）
   ================================================================== */

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = parseInt(process.argv[2] || '7001', 10);
const DATA_FILE = path.join(__dirname, 'shoe_erp.server.db.json');

/* ---------- 内存快照 ---------- */
let store = {
  __ver: 0,
  data: {
    orders: [],
    bom: [],
    sole: [],
    uppersole: [],
    finished: [],
    last: [],
    prodGroups: [],
    prodRecords: []
  }
};

/* ---------- 持久化：shoe_erp.server.db.json ---------- */
function load() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const raw = fs.readFileSync(DATA_FILE, 'utf-8');
      const parsed = JSON.parse(raw);
      if (parsed && parsed.data) {
        store = parsed;
        console.log('[LOAD] 已从 ' + DATA_FILE + ' 加载数据 v' + (store.__ver || 0));
        console.log('       订单 ' + (store.data.orders || []).length +
          ' 条, BOM ' + (store.data.bom || []).length +
          ' 条, 大底 ' + (store.data.sole || []).length +
          ' 条, 鞋面 ' + (store.data.uppersole || []).length +
          ' 条, 成品 ' + (store.data.finished || []).length +
          ' 条, 楦头 ' + (store.data.last || []).length +
          ' 条, 生产组 ' + (store.data.prodGroups || []).length +
          ' 条, 生产记录 ' + (store.data.prodRecords || []).length + ' 条');
      }
    } else {
      console.log('[LOAD] ' + DATA_FILE + ' 不存在，创建空数据库');
      save();
    }
  } catch (e) {
    console.error('[LOAD] 读取数据文件失败：' + e.message);
    console.error('       将使用空数据库启动，原有数据不受影响（文件未修改）');
  }
}

function save() {
  try {
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(store, null, 2), 'utf-8');
    fs.renameSync(tmp, DATA_FILE);
    console.log('[SAVE] 已写入 ' + DATA_FILE + ' (v' + store.__ver + ')');
  } catch (e) {
    console.error('[SAVE] 写入失败：' + e.message);
  }
}

/* ---------- 工具 ---------- */
function readBody(req) {
  return new Promise((resolve, reject) => {
    let buf = '';
    req.setEncoding('utf-8');
    req.on('data', chunk => { buf += chunk; });
    req.on('end', () => {
      try { resolve(buf ? JSON.parse(buf) : {}); }
      catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

function send(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}

/* ---------- 三路合并 ---------- */
function merge(localData, serverData) {
  const sets = ['orders', 'bom', 'sole', 'uppersole', 'finished', 'last', 'prodGroups', 'prodRecords'];
  const out = {};
  let mergedCount = 0;

  sets.forEach(key => {
    const local = Array.isArray(localData[key]) ? localData[key] : [];
    const server = Array.isArray(serverData[key]) ? serverData[key] : [];
    const localMap = {}, serverMap = {};
    local.forEach(r => { if (r && r.id) localMap[r.id] = r; });
    server.forEach(r => { if (r && r.id) serverMap[r.id] = r; });

    const ids = new Set();
    local.forEach(r => { if (r && r.id) ids.add(r.id); });
    server.forEach(r => { if (r && r.id) ids.add(r.id); });

    const result = [];
    ids.forEach(id => {
      const l = localMap[id], s = serverMap[id];
      let pick;
      if (l && !s) pick = l;
      else if (!l && s) pick = s;
      else {
        const lu = l.updatedAt || '', su = s.updatedAt || '';
        if (su > lu) pick = s;
        else if (lu > su) pick = l;
        else pick = ((s.__vn || 0) >= (l.__vn || 0)) ? s : l;
      }
      if (pick) {
        result.push(pick);
        if (s && l && (s.updatedAt || '') > (l.updatedAt || '')) mergedCount++;
      }
    });
    out[key] = result;
  });

  return { data: out, mergedCount };
}

/* ---------- HTTP 路由 ---------- */
const clients = new Set();

const server = http.createServer(async (req, res) => {
  const url = req.url;
  const clientId = req.headers['x-client-id'] || 'unknown';
  const now = new Date().toLocaleTimeString('zh-CN', { hour12: false });

  console.log('[' + now + '] ' + req.method + ' ' + url + ' ← ' + clientId);

  /* ---- 跨域 ---- */
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Client-Id');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  try {
    /* ---- 心跳 ---- */
    if (req.method === 'GET' && url === '/api/ping') {
      clients.add(clientId);
      return send(res, 200, {
        ok: true,
        clients: clients.size,
        serverTime: Date.now(),
        ver: store.__ver
      });
    }

    /* ---- 拉取全量快照 ---- */
    if (req.method === 'GET' && url.startsWith('/api/pull')) {
      return send(res, 200, {
        ok: true,
        __ver: store.__ver,
        data: store.data,
        serverTime: Date.now()
      });
    }

    /* ---- 推送本地快照 ---- */
    if (req.method === 'POST' && url === '/api/push') {
      const body = await readBody(req);
      if (!body || !body.data) return send(res, 400, { ok: false, error: 'invalid payload' });

      const result = merge(body.data, store.data);

      // 检查是否有冲突（服务器有客户端没有的新数据）
      let hasNewer = false;
      const sets = ['orders', 'bom', 'sole', 'uppersole', 'finished', 'last', 'prodGroups', 'prodRecords'];
      sets.forEach(key => {
        const clientArr = body.data[key] || [];
        const serverArr = store.data[key] || [];
        serverArr.forEach(sr => {
          if (!sr.updatedAt) return;
          const cr = clientArr.find(c => c.id === sr.id);
          if (cr && (sr.updatedAt > (cr.updatedAt || ''))) hasNewer = true;
        });
      });

      store.data = result.data;
      store.__ver++;
      save();

      console.log('       ↑ 合并完成，新增/更新 ' + result.mergedCount + ' 条, 版本 → v' + store.__ver);

      return send(res, 200, {
        ok: true,
        conflict: hasNewer,
        ver: store.__ver,
        merged: result.mergedCount
      });
    }

    /* ---- 404 ---- */
    send(res, 404, { ok: false, error: 'not found: ' + url });
  } catch (e) {
    console.error('[ERROR] ' + e.message);
    send(res, 500, { ok: false, error: e.message });
  }
});

/* ---------- 启动 ---------- */
load();
server.listen(PORT, '0.0.0.0', () => {
  console.log('');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  广宏鞋业ERP系统 · 局域网协同服务器 v2.0.14');
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  监听端口：' + PORT);
  console.log('  数据文件：' + DATA_FILE);
  console.log('  客户端连接：http://<本机IP>:' + PORT);
  console.log('═══════════════════════════════════════════════════════════');
  console.log('');
});

server.on('error', e => {
  if (e.code === 'EADDRINUSE') {
    console.error('❌ 端口 ' + PORT + ' 已被占用，请换一个端口或关闭占用程序');
  } else {
    console.error('❌ 服务器错误：' + e.message);
  }
  process.exit(1);
});
