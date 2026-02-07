const http = require('http')
const url = require('url')
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const os = require('os')

const state = {
  sessions: new Map(),
  users: new Map(),
  invites: new Map(),
  meetings: new Map(),
  orders: new Map(),
  waiterCalls: new Map(),
  blocks: new Set(),
  reports: [],
  sseUsers: new Map(),
  sseStaff: new Map(),
  // Meta para SSE: trackear actividad para cerrar conexiones inactivas
  sseUserMeta: new Map(),   // key: res, value: { startedAt, lastWrite }
  sseStaffMeta: new Map(),  // key: res, value: { startedAt, lastWrite }
  rate: {
    invitesByUserHour: new Map(),
    lastInvitePair: new Map(),
    restrictedUsers: new Map(),
    consumptionByUserHour: new Map(),
    reactionsByUserHour: new Map(),
    tableChangesByUserHour: new Map(),
  },
}

const dataDir = path.join(__dirname, 'data')
if (!fs.existsSync(dataDir)) { try { fs.mkdirSync(dataDir) } catch {} }
const GLOBAL_STAFF_PIN = String(process.env.STAFF_PIN || '')
const ALLOW_GLOBAL_STAFF_PIN = String(process.env.ALLOW_GLOBAL_STAFF_PIN || 'false') === 'true'
const ADMIN_SECRET = String(process.env.ADMIN_SECRET || '')
const RESEND_API_KEY = String(process.env.RESEND_API_KEY || '')
const SENDGRID_API_KEY = String(process.env.SENDGRID_API_KEY || '')
const EMAIL_FROM = String(process.env.EMAIL_FROM || '')

// Créditos por venue: persistidos en /data/venues.json
const venuesPath = path.join(dataDir, 'venues.json')
let db = null
let dbReady = false
try {
  const { Pool } = require('pg')
  const candidates = [
    String(process.env.DATABASE_URL || ''),
      String(process.env.RAILWAY_DATABASE_URL || ''),
    String(process.env.POSTGRES_URL || ''),
    String(process.env.POSTGRESQL_URL || ''),
    String(process.env.PGURL || ''),
    String(process.env.PG_URL || ''),
    String(process.env.URL_DE_BASE_DE_DATOS || ''),
    String(process.env.POSTGRES_URL_DE_BASE_DE_DATOS || '')
  ].filter(v => !!v)
  const conn = candidates[0] || ''
  if (conn) {
      db = new Pool({ connectionString: conn, ssl: { require: true, rejectUnauthorized: false } })
    ;(async () => { try { await initDB() } catch {} })()
  }
} catch {}
async function initDB() {
  if (!db || dbReady) return !!db
  await db.query('CREATE TABLE IF NOT EXISTS venues (venue_id TEXT PRIMARY KEY, name TEXT NOT NULL, credits INTEGER NOT NULL, active BOOLEAN NOT NULL, pin TEXT, email TEXT)')
  await db.query('ALTER TABLE IF EXISTS venues ADD COLUMN IF NOT EXISTS pin TEXT')
  await db.query('ALTER TABLE IF EXISTS venues ADD COLUMN IF NOT EXISTS email TEXT')
  await db.query('CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, role TEXT NOT NULL, alias TEXT, selfie TEXT, selfie_approved BOOLEAN, available BOOLEAN, prefs_json TEXT, zone TEXT, muted BOOLEAN, receive_mode TEXT, table_id TEXT, visibility TEXT, paused_until BIGINT, silenced BOOLEAN)')
  await db.query('CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, emitter_id TEXT NOT NULL, receiver_id TEXT NOT NULL, product TEXT NOT NULL, quantity INTEGER NOT NULL, price INTEGER NOT NULL, total INTEGER NOT NULL, status TEXT NOT NULL, created_at BIGINT NOT NULL, expires_at BIGINT NOT NULL, emitter_table TEXT, receiver_table TEXT, mesa_entrega TEXT, is_invitation BOOLEAN)')
  await db.query('CREATE TABLE IF NOT EXISTS table_closures (session_id TEXT NOT NULL, table_id TEXT NOT NULL, closed BOOLEAN NOT NULL, PRIMARY KEY (session_id, table_id))')
  await db.query('CREATE TABLE IF NOT EXISTS waiter_calls (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, user_id TEXT NOT NULL, table_id TEXT, reason TEXT, status TEXT, ts BIGINT NOT NULL)')
  await db.query('CREATE TABLE IF NOT EXISTS catalog_items (session_id TEXT NOT NULL, name TEXT NOT NULL, price INTEGER NOT NULL, PRIMARY KEY (session_id, name))')
  dbReady = true
  return true
}
async function sendEmail(to, subject, text) {
  try {
    if (RESEND_API_KEY) {
      const r = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + RESEND_API_KEY },
        body: JSON.stringify({ from: EMAIL_FROM || 'no-reply@discos.app', to, subject, text })
      })
      return r.ok
    } else if (SENDGRID_API_KEY) {
      const body = {
        personalizations: [{ to: [{ email: to }] }],
        from: { email: EMAIL_FROM || 'no-reply@discos.app' },
        subject,
        content: [{ type: 'text/plain', value: text }]
      }
      const r = await fetch('https://api.sendgrid.com/v3/mail/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + SENDGRID_API_KEY },
        body: JSON.stringify(body)
      })
      return r.status === 202
    } else {
      console.log('[email] no provider configured', { to, subject })
      return false
    }
  } catch (e) {
    console.log('[email] error', String(e && e.message || e))
    return false
  }
}
async function isDBConnected() {
  if (!db) return false
  try { await db.query('SELECT 1'); return true } catch { return false }
}
async function listPublicTables() {
  if (!db) return []
  await initDB()
  const r = await db.query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
  return r.rows.map(w => w.table_name)
}
async function readVenues() {
  if (db) {
    try {
      await initDB()
      const r = await db.query('SELECT venue_id, name, credits, active, pin, email FROM venues')
      const obj = {}
      for (const row of r.rows) obj[row.venue_id] = { name: row.name, credits: Number(row.credits || 0), active: !!row.active, pin: row.pin || '', email: row.email || '' }
      return obj
    } catch {}
  }
  try {
    if (!fs.existsSync(venuesPath)) return {}
    const raw = fs.readFileSync(venuesPath, 'utf-8')
    const obj = JSON.parse(raw || '{}')
    return typeof obj === 'object' && obj ? obj : {}
  } catch { return {} }
}
async function writeVenues(obj) {
  if (db) {
    try {
      await initDB()
      const entries = Object.entries(obj)
      for (const [id, v] of entries) {
        await db.query(
          'INSERT INTO venues (venue_id, name, credits, active, pin, email) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (venue_id) DO UPDATE SET name=EXCLUDED.name, credits=EXCLUDED.credits, active=EXCLUDED.active, pin=EXCLUDED.pin, email=EXCLUDED.email',
          [String(id), String(v.name || id), Number(v.credits || 0), v.active !== false, String(v.pin || ''), String(v.email || '')]
        )
      }
      return
    } catch {}
  }
  try { fs.writeFileSync(venuesPath, JSON.stringify(obj)) } catch {}
}
async function dbReadGlobalCatalog() {
  if (!db) return []
  await initDB()
  const r = await db.query('SELECT name, price FROM catalog_items WHERE session_id=$1 ORDER BY name', ['global'])
  return r.rows.map(w => ({ name: w.name, price: Number(w.price || 0) }))
}
async function dbWriteGlobalCatalog(items) {
  if (!db) return
  await initDB()
  await db.query('DELETE FROM catalog_items WHERE session_id=$1', ['global'])
  for (const it of Array.isArray(items) ? items : []) {
    await db.query('INSERT INTO catalog_items (session_id, name, price) VALUES ($1,$2,$3)', ['global', String(it.name || ''), Number(it.price || 0)])
  }
}
async function dbReadSessionCatalog(sessionId) {
  if (!db) return []
  await initDB()
  const r = await db.query('SELECT name, price FROM catalog_items WHERE session_id=$1 ORDER BY name', [String(sessionId)])
  return r.rows.map(w => ({ name: w.name, price: Number(w.price || 0) }))
}
async function dbWriteSessionCatalog(sessionId, items) {
  if (!db) return
  await initDB()
  await db.query('DELETE FROM catalog_items WHERE session_id=$1', [String(sessionId)])
  for (const it of Array.isArray(items) ? items : []) {
    await db.query('INSERT INTO catalog_items (session_id, name, price) VALUES ($1,$2,$3)', [String(sessionId), String(it.name || ''), Number(it.price || 0)])
  }
}
async function dbUpsertUser(u) {
  if (!db) return
  await initDB()
  const vals = [
    String(u.id),
    String(u.sessionId || ''),
    String(u.role || 'user'),
    String(u.alias || ''),
    String(u.selfie || ''),
    !!u.selfieApproved,
    !!u.available,
    JSON.stringify(u.prefs || {}),
    String(u.zone || ''),
    !!u.muted,
    String(u.receiveMode || 'all'),
    String(u.tableId || ''),
    String(u.visibility || 'visible'),
    Number(u.pausedUntil || 0),
    !!u.silenced
  ]
  await db.query('INSERT INTO users (id, session_id, role, alias, selfie, selfie_approved, available, prefs_json, zone, muted, receive_mode, table_id, visibility, paused_until, silenced) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT (id) DO UPDATE SET session_id=EXCLUDED.session_id, role=EXCLUDED.role, alias=EXCLUDED.alias, selfie=EXCLUDED.selfie, selfie_approved=EXCLUDED.selfie_approved, available=EXCLUDED.available, prefs_json=EXCLUDED.prefs_json, zone=EXCLUDED.zone, muted=EXCLUDED.muted, receive_mode=EXCLUDED.receive_mode, table_id=EXCLUDED.table_id, visibility=EXCLUDED.visibility, paused_until=EXCLUDED.paused_until, silenced=EXCLUDED.silenced', vals)
}
async function dbGetUser(id) {
  if (!db) return null
  await initDB()
  const r = await db.query('SELECT id, session_id, role, alias, selfie, selfie_approved, available, prefs_json, zone, muted, receive_mode, table_id, visibility, paused_until, silenced FROM users WHERE id=$1', [String(id)])
  if (!r.rows.length) return null
  const w = r.rows[0]
  return {
    id: w.id, sessionId: w.session_id, role: w.role, alias: w.alias || '',
    selfie: w.selfie || '', selfieApproved: !!w.selfie_approved, available: !!w.available,
    prefs: (typeof w.prefs_json === 'string' ? JSON.parse(w.prefs_json || '{}') : {}),
    zone: w.zone || '', muted: !!w.muted, receiveMode: w.receive_mode || 'all',
    tableId: w.table_id || '', visibility: w.visibility || 'visible', pausedUntil: Number(w.paused_until || 0), silenced: !!w.silenced
  }
}
async function dbGetUsersBySession(sessionId) {
  if (!db) return []
  await initDB()
  const r = await db.query('SELECT id, alias, selfie_approved, muted FROM users WHERE session_id=$1 AND role=$2', [String(sessionId), 'user'])
  return r.rows.map(w => ({ id: w.id, alias: w.alias || '', selfieApproved: !!w.selfie_approved, muted: !!w.muted }))
}
async function dbInsertOrder(o) {
  if (!db) return
  await initDB()
  const vals = [
    String(o.id), String(o.sessionId), String(o.emitterId), String(o.receiverId),
    String(o.product), Number(o.quantity || 1), Number(o.price || 0), Number(o.total || 0),
    String(o.status), Number(o.createdAt || 0), Number(o.expiresAt || 0),
    String(o.emitterTable || ''), String(o.receiverTable || ''), String(o.mesaEntrega || ''), !!o.isInvitation
  ]
  await db.query('INSERT INTO orders (id, session_id, emitter_id, receiver_id, product, quantity, price, total, status, created_at, expires_at, emitter_table, receiver_table, mesa_entrega, is_invitation) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status, quantity=EXCLUDED.quantity, price=EXCLUDED.price, total=EXCLUDED.total, emitter_table=EXCLUDED.emitter_table, receiver_table=EXCLUDED.receiver_table, mesa_entrega=EXCLUDED.mesa_entrega, expires_at=EXCLUDED.expires_at', vals)
}
async function dbUpdateOrderStatus(id, status) {
  if (!db) return
  await initDB()
  await db.query('UPDATE orders SET status=$2 WHERE id=$1', [String(id), String(status)])
}
async function dbGetOrdersBySession(sessionId, stateFilter) {
  if (!db) return []
  await initDB()
  if (stateFilter) {
    const r = await db.query('SELECT id, product, quantity, price, total, status, created_at, expires_at, emitter_id, receiver_id, emitter_table, receiver_table, mesa_entrega FROM orders WHERE session_id=$1 AND status=$2', [String(sessionId), String(stateFilter)])
    return r.rows
  }
  const r = await db.query('SELECT id, product, quantity, price, total, status, created_at, expires_at, emitter_id, receiver_id, emitter_table, receiver_table, mesa_entrega FROM orders WHERE session_id=$1', [String(sessionId)])
  return r.rows
}
async function dbGetOrdersByTable(sessionId, tableId) {
  if (!db) return []
  await initDB()
  const r = await db.query('SELECT id, product, quantity, price, total, status, created_at, expires_at, emitter_id, receiver_id, emitter_table, receiver_table, mesa_entrega FROM orders WHERE session_id=$1 AND (emitter_table=$2 OR receiver_table=$2 OR mesa_entrega=$2)', [String(sessionId), String(tableId)])
  return r.rows
}
async function dbGetOrdersByUser(userId) {
  if (!db) return []
  await initDB()
  const r = await db.query('SELECT id, product, quantity, price, total, status, created_at, expires_at, emitter_id, receiver_id, emitter_table, receiver_table, mesa_entrega FROM orders WHERE emitter_id=$1 OR receiver_id=$1', [String(userId)])
  return r.rows
}
async function dbSetTableClosed(sessionId, tableId, closed) {
  if (!db) return
  await initDB()
  await db.query('INSERT INTO table_closures (session_id, table_id, closed) VALUES ($1,$2,$3) ON CONFLICT (session_id, table_id) DO UPDATE SET closed=EXCLUDED.closed', [String(sessionId), String(tableId), !!closed])
}
async function dbIsTableClosed(sessionId, tableId) {
  if (!db) return false
  await initDB()
  const r = await db.query('SELECT closed FROM table_closures WHERE session_id=$1 AND table_id=$2', [String(sessionId), String(tableId)])
  if (!r.rows.length) return false
  return !!r.rows[0].closed
}
async function dbInsertWaiterCall(c) {
  if (!db) return
  await initDB()
  const vals = [String(c.id), String(c.sessionId), String(c.userId), String(c.tableId || ''), String(c.reason || ''), String(c.status || ''), Number(c.ts || 0)]
  await db.query('INSERT INTO waiter_calls (id, session_id, user_id, table_id, reason, status, ts) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status', vals)
}
async function dbUpdateWaiterCallStatus(id, status) {
  if (!db) return
  await initDB()
  await db.query('UPDATE waiter_calls SET status=$2 WHERE id=$1', [String(id), String(status)])
}
async function dbGetWaiterCalls(sessionId) {
  if (!db) return []
  await initDB()
  const r = await db.query('SELECT id, session_id, user_id, table_id, reason, status, ts FROM waiter_calls WHERE session_id=$1', [String(sessionId)])
  return r.rows
}

// Autorización admin: requiere ADMIN_SECRET por header o query
function isAdminAuthorized(req, query) {
  const headerSecret = String(req.headers['x-admin-secret'] || '')
  const querySecret = String(query.admin_secret || '')
  if (!ADMIN_SECRET) return false
  return headerSecret === ADMIN_SECRET || querySecret === ADMIN_SECRET
}

const defaultCatalog = [
  { name: 'Cerveza', price: 10000 },
  { name: 'Mojito', price: 20000 },
  { name: 'Gin Tonic', price: 18000 },
  { name: 'Agua', price: 5000 },
  { name: 'Tequila Shot', price: 15000 },
  { name: 'Vodka Shot', price: 12000 },
]
function sanitizeItem(it) {
  return { name: String(it.name || '').slice(0, 60), price: Number(it.price || 0) }
}
function readGlobalCatalog() {
  try {
    const p = path.join(dataDir, 'catalog.json')
    if (fs.existsSync(p)) {
      const raw = fs.readFileSync(p, 'utf-8')
      const arr = JSON.parse(raw || '[]')
      if (Array.isArray(arr) && arr.length) return arr.map(sanitizeItem)
    }
  } catch {}
  return defaultCatalog
}
function writeGlobalCatalog(items) {
  try {
    const clean = Array.isArray(items) ? items.map(sanitizeItem) : []
    fs.writeFileSync(path.join(dataDir, 'catalog.json'), JSON.stringify(clean))
  } catch {}
}

function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' })
  res.end(JSON.stringify(obj))
}

function parseBody(req) {
  return new Promise(resolve => {
    const chunks = []
    req.on('data', c => chunks.push(c))
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString()
      try { resolve(JSON.parse(raw || '{}')) } catch { resolve({}) }
    })
  })
}

function genId(prefix) {
  return `${prefix}_${crypto.randomBytes(8).toString('hex')}`
}

function now() { return Date.now() }
function csvEscape(v) {
  const s = String(v == null ? '' : v)
  return `"${s.replace(/"/g, '""')}"`
}

function ensureSession(sessionId) { return state.sessions.get(sessionId) }

function sendToUser(userId, event, data) {
  const clients = state.sseUsers.get(userId)
  if (!clients) return
  const payload = `event: ${event}\n` + `data: ${JSON.stringify(data)}\n\n`
  for (const res of clients) {
    try { res.write(payload) } catch {}
    const meta = state.sseUserMeta.get(res) || { startedAt: now(), lastWrite: now() }
    meta.lastWrite = now()
    state.sseUserMeta.set(res, meta)
  }
}

function sendToStaff(sessionId, event, data) {
  const clients = state.sseStaff.get(sessionId)
  if (!clients) return
  const payload = `event: ${event}\n` + `data: ${JSON.stringify(data)}\n\n`
  for (const res of clients) {
    try { res.write(payload) } catch {}
    const meta = state.sseStaffMeta.get(res) || { startedAt: now(), lastWrite: now() }
    meta.lastWrite = now()
    state.sseStaffMeta.set(res, meta)
  }
}

function within(ms, ts) { return now() - ts < ms }

function rateCanInvite(fromId, toId) {
  const hourKey = fromId
  const hourBucket = state.rate.invitesByUserHour.get(hourKey) || []
  const fresh = hourBucket.filter(ts => within(60 * 60 * 1000, ts))
  if (fresh.length >= 5) return false
  const pairKey = `${fromId}:${toId}`
  const last = state.rate.lastInvitePair.get(pairKey)
  if (last && within(30 * 60 * 1000, last.ts) && last.blocked) return false
  state.rate.invitesByUserHour.set(hourKey, [...fresh, now()])
  return true
}

function applyRestrictedIfNeeded(userId) {
  const count = state.rate.restrictedUsers.get(userId) || 0
  if (count >= 2) return true
  return false
}

function markBlockedEvent(targetId) {
  const prev = state.rate.restrictedUsers.get(targetId) || 0
  state.rate.restrictedUsers.set(targetId, prev + 1)
}

function isBlockedPair(a, b) {
  return state.blocks.has(`${a}:${b}`) || state.blocks.has(`${b}:${a}`)
}

function persistOrders(sessionId) {
  const list = []
  for (const o of state.orders.values()) if (o.sessionId === sessionId) list.push(o)
  try { fs.writeFileSync(path.join(dataDir, `orders_${sessionId}.json`), JSON.stringify(list)) } catch {}
  if (db) {
    for (const o of list) { try { dbUpdateOrderStatus(o.id, o.status) } catch {} }
  }
}
function persistReports(sessionId) {
  const list = state.reports.filter(r => r.sessionId === sessionId)
  try { fs.writeFileSync(path.join(dataDir, `reports_${sessionId}.json`), JSON.stringify(list)) } catch {}
}
function wipeSessionData(sessionId) {
  try { fs.unlinkSync(path.join(dataDir, `orders_${sessionId}.json`)) } catch {}
  try { fs.unlinkSync(path.join(dataDir, `reports_${sessionId}.json`)) } catch {}
}
function archiveSession(sessionId) {
  const s = ensureSession(sessionId)
  if (!s) return
  let usersCount = 0
  const mesasSet = new Set()
  let invitesSent = 0, invitesAccepted = 0
  const ordersStats = { pendiente_cobro:0, cobrado:0, en_preparacion:0, entregado:0, cancelado:0, expirado:0 }
  const topItems = {}
  const orders = []
  const reports = []
  for (const u of state.users.values()) if (u.sessionId === sessionId && u.role === 'user') { usersCount++; if (u.tableId) mesasSet.add(u.tableId) }
  for (const inv of state.invites.values()) if (inv.sessionId === sessionId) { invitesSent++; if (inv.status === 'aceptado') invitesAccepted++ }
  for (const o of state.orders.values()) if (o.sessionId === sessionId) { ordersStats[o.status] = (ordersStats[o.status]||0)+1; orders.push(o); topItems[o.product] = (topItems[o.product]||0)+1 }
  for (const r of state.reports) if (r.sessionId === sessionId) reports.push(r)
  const data = {
    sessionId,
    venueId: s.venueId || 'default',
    venue: s.venue,
    startedAt: s.startedAt,
    endedAt: now(),
    analytics: { usersCount, mesasActivas: mesasSet.size, invitesSent, invitesAccepted, orders: ordersStats, topItems },
    orders,
    reports
  }
  try { fs.writeFileSync(path.join(dataDir, `archive_${sessionId}.json`), JSON.stringify(data)) } catch {}
  try {
    const ordersHeader = ['id','sessionId','emitterId','emitterAlias','receiverId','receiverAlias','product','quantity','price','total','status','createdAt','expiresAt','emitterTable','receiverTable','mesaEntrega'].join(',')
    const ordersLines = [ordersHeader]
    for (const o of orders) {
      const emAlias = (state.users.get(o.emitterId)?.alias || o.emitterId)
      const reAlias = (state.users.get(o.receiverId)?.alias || o.receiverId)
      ordersLines.push([
        csvEscape(o.id),
        csvEscape(o.sessionId),
        csvEscape(o.emitterId),
        csvEscape(emAlias),
        csvEscape(o.receiverId),
        csvEscape(reAlias),
        csvEscape(o.product),
        csvEscape(o.quantity),
        csvEscape(o.price),
        csvEscape(o.total),
        csvEscape(o.status),
        csvEscape(o.createdAt),
        csvEscape(o.expiresAt),
        csvEscape(o.emitterTable || ''),
        csvEscape(o.receiverTable || ''),
        csvEscape(o.mesaEntrega || '')
      ].join(','))
    }
    fs.writeFileSync(path.join(dataDir, `archive_${sessionId}_orders.csv`), ordersLines.join('\n'))
  } catch {}
  try {
    const reportsHeader = ['sessionId','fromId','fromAlias','targetId','targetAlias','category','note','ts'].join(',')
    const reportsLines = [reportsHeader]
    for (const r of reports) {
      const fromAlias = (state.users.get(r.fromId)?.alias || r.fromId)
      const targetAlias = (state.users.get(r.targetId)?.alias || r.targetId)
      reportsLines.push([
        csvEscape(r.sessionId),
        csvEscape(r.fromId),
        csvEscape(fromAlias),
        csvEscape(r.targetId),
        csvEscape(targetAlias),
        csvEscape(r.category),
        csvEscape(r.note),
        csvEscape(r.ts)
      ].join(','))
    }
    fs.writeFileSync(path.join(dataDir, `archive_${sessionId}_reports.csv`), reportsLines.join('\n'))
  } catch {}
}
function endAndArchive(sessionId) {
  archiveSession(sessionId)
  state.sessions.delete(sessionId)
  // Cerrar SSE staff de la sesión
  const staffConns = state.sseStaff.get(sessionId) || []
  for (const res of staffConns) { try { res.end() } catch {} state.sseStaffMeta.delete(res) }
  state.sseStaff.delete(sessionId)
  // Cerrar SSE de usuarios pertenecientes a la sesión y limpiar memoria
  for (const [uid, u] of state.users) if (u.sessionId === sessionId) state.users.delete(uid)
  for (const [uid, list] of state.sseUsers) {
    const user = state.users.get(uid) // ya removido arriba si era de la sesión
    if (user && user.sessionId === sessionId) {
      for (const res of list) { try { res.end() } catch {} state.sseUserMeta.delete(res) }
      state.sseUsers.delete(uid)
    }
  }
  for (const [k, v] of state.invites) if (v.sessionId === sessionId) state.invites.delete(k)
  for (const [k, v] of state.meetings) if (v.sessionId === sessionId) state.meetings.delete(k)
  for (const [k, v] of state.orders) if (v.sessionId === sessionId) state.orders.delete(k)
}
function expireOldSessions() {
  for (const s of state.sessions.values()) {
    if (s.active && (now() - s.startedAt) >= 12 * 60 * 60 * 1000) {
      endAndArchive(s.id)
    }
  }
}

function serveStatic(req, res, pathname) {
  const filePath = path.join(__dirname, 'public', pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, ''))
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return }
    const ext = path.extname(filePath)
    const type = ext === '.html' ? 'text/html' : ext === '.js' ? 'application/javascript' : ext === '.css' ? 'text/css' : ext === '.json' ? 'application/json' : 'application/octet-stream'
    res.writeHead(200, { 'Content-Type': type })
    res.end(data)
  })
}

const server = http.createServer(async (req, res) => {
  const { pathname, query } = url.parse(req.url, true)
  if (req.method === 'OPTIONS') {
    res.writeHead(200, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Admin-Secret',
    })
    res.end()
    return
  }

  if (pathname.startsWith('/api/')) {
    expireOldSessions()
    if (pathname === '/api/session/start' && req.method === 'POST') {
      try {
        const body = await parseBody(req)
        const venueId = String(body.venueId || 'default')
        for (const s of state.sessions.values()) {
          if (s.active && (now() - s.startedAt) < 12 * 60 * 60 * 1000 && s.venueId === venueId) {
            json(res, 200, { sessionId: s.id, pin: s.pin, venueId, reused: true })
            return
          }
        }
        const venues = await readVenues()
        const entry = venues[venueId] || { name: venueId, credits: 0, active: true }
        const current = Number(entry.credits || 0)
        if (entry.active === false) { json(res, 403, { error: 'inactive' }); return }
        if (current <= 0) { json(res, 403, { error: 'no_credit' }); return }
        venues[venueId] = { name: String(entry.name || venueId), credits: current - 1, active: entry.active !== false, pin: String(entry.pin || ''), email: String(entry.email || '') }
        await writeVenues(venues)
        const sessionId = genId('sess')
        const pin = String(Math.floor(1000 + Math.random() * 9000))
        state.sessions.set(sessionId, { id: sessionId, venueId, venue: body.venue || 'Venue', startedAt: now(), active: true, pin, publicBaseUrl: '', closedTables: new Set() })
        json(res, 200, { sessionId, pin, venueId })
        return
      } catch (e) {
        json(res, 500, { error: 'session_start_failed', message: String(e && e.message ? e.message : e) })
        return
      }
    }
    if (pathname === '/api/session/active' && req.method === 'GET') {
      const venueId = String(query.venueId || '')
      for (const s of state.sessions.values()) {
        if (s.active && (now() - s.startedAt) < 12 * 60 * 60 * 1000) {
          if (!venueId || s.venueId === venueId) {
            json(res, 200, { active: true, sessionId: s.id, pin: s.pin, venueId: s.venueId })
            return
          }
        }
      }
      json(res, 200, { active: false, error: 'no_active' })
      return
    }
    if (pathname === '/api/session/end' && req.method === 'POST') {
      const body = await parseBody(req)
      const s = ensureSession(body.sessionId)
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      endAndArchive(body.sessionId)
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/session/public-base' && req.method === 'POST') {
      const body = await parseBody(req)
      const s = ensureSession(body.sessionId)
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      const urlStr = String(body.publicBaseUrl || '').trim()
      s.publicBaseUrl = urlStr
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/session/public-base' && req.method === 'GET') {
      const sessionId = query.sessionId
      const s = ensureSession(sessionId)
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      json(res, 200, { publicBaseUrl: s.publicBaseUrl || '' })
      return
    }
    if (pathname === '/api/join' && req.method === 'POST') {
      const body = await parseBody(req)
      const s = ensureSession(body.sessionId)
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      const role = body.role === 'staff' ? 'staff' : 'user'
      if (role === 'staff') {
        const pinStr = String(body.pin || '')
        const okSessionPin = pinStr === String(s.pin)
        const okGlobalPin = (ALLOW_GLOBAL_STAFF_PIN && GLOBAL_STAFF_PIN && pinStr === GLOBAL_STAFF_PIN)
        let okVenuePin = false
        try {
          const venues = await readVenues()
          const v = venues[s.venueId]
          if (v && String(v.pin || '') && pinStr === String(v.pin)) okVenuePin = true
        } catch {}
        if (!pinStr || (!okSessionPin && !okGlobalPin && !okVenuePin)) { json(res, 403, { error: 'bad_pin' }); return }
      } else {
        const alias = String(body.alias || '').trim().slice(0, 32)
        if (!alias) { json(res, 400, { error: 'alias_required' }); return }
      }
      const id = genId(role === 'staff' ? 'staff' : 'user')
      const user = { id, sessionId: body.sessionId, role, alias: role === 'user' ? String(body.alias || '').trim().slice(0, 32) : '', selfie: '', selfieApproved: false, available: false, prefs: { tags: [] }, zone: '', muted: false, receiveMode: 'all', allowedSenders: new Set(), tableId: '', visibility: 'visible', pausedUntil: 0, silenced: false, danceState: 'idle', dancePartnerId: '', meetingId: '' }
      state.users.set(id, user)
      try { await dbUpsertUser(user) } catch {}
      json(res, 200, { user })
      return
    }
    if (pathname === '/api/user/get' && req.method === 'GET') {
      const userId = query.userId
      let u = state.users.get(userId)
      if (!u && db) { try { u = await dbGetUser(userId) } catch {} }
      if (!u) { json(res, 200, { found: false }); return }
      json(res, 200, { found: true, user: { id: u.id, sessionId: u.sessionId, role: u.role, alias: u.alias, selfie: u.selfieApproved ? u.selfie : '', selfieApproved: u.selfieApproved, available: u.available, prefs: u.prefs, zone: u.zone, tableId: u.tableId, visibility: u.visibility } })
      return
    }
    if (pathname === '/api/user/profile' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      u.alias = String(body.alias || '').slice(0, 32)
      const selfieStr = String(body.selfie || '')
      // Validaciones de imagen: tamaño ≤ 500KB, MIME permitido (jpeg/webp)
      let okImage = false, err = ''
      if (selfieStr.startsWith('data:')) {
        const m = selfieStr.match(/^data:(.*?);base64,(.+)$/)
        if (m) {
          const mime = m[1]
          const b64 = m[2]
          const buf = Buffer.from(b64, 'base64')
          const size = buf.length
          const allowed = mime === 'image/jpeg' || mime === 'image/webp'
          if (!allowed) { err = 'bad_mime' }
          else if (size > 500 * 1024) { err = 'too_big' }
          else okImage = true
        } else { err = 'bad_format' }
      } else if (selfieStr) {
        err = 'bad_format'
      }
      if (!okImage) { json(res, 400, { error: 'bad_image', reason: err }); return }
      u.selfie = selfieStr
      u.selfieApproved = false
      try { await dbUpsertUser(u) } catch {}
      json(res, 200, { ok: true })
      return
    }
    // Admin: listar venues
    if (pathname === '/api/admin/venues' && req.method === 'GET') {
      if (!ADMIN_SECRET) { json(res, 403, { error: 'no_admin_secret' }); return }
      if (!isAdminAuthorized(req, query)) { json(res, 403, { error: 'forbidden' }); return }
      const venues = await readVenues()
      const list = []
      for (const id of Object.keys(venues)) {
        const v = venues[id] || {}
        list.push({ venueId: id, name: String(v.name || id), credits: Number(v.credits || 0), active: v.active !== false, pin: String(v.pin || ''), email: String(v.email || '') })
      }
      json(res, 200, { venues: list })
      return
    }
    // Admin: sumar créditos a un venue
    if (pathname === '/api/admin/venues/credit' && req.method === 'POST') {
      if (!ADMIN_SECRET) { json(res, 403, { error: 'no_admin_secret' }); return }
      if (!isAdminAuthorized(req, query)) { json(res, 403, { error: 'forbidden' }); return }
      const body = await parseBody(req)
      const venueId = String(body.venueId || '').trim()
      const amount = Number(body.amount || 0)
      if (!venueId || !Number.isFinite(amount)) { json(res, 400, { error: 'bad_input' }); return }
      const venues = await readVenues()
      const existed = !!venues[venueId]
      const prev = venues[venueId] || { name: venueId, credits: 0, active: true, pin: '', email: '' }
      const nextCredits = Number(prev.credits || 0) + amount
      let pin = String(prev.pin || '')
      if (!existed && !pin) {
        pin = String(Math.floor(1000 + Math.random() * 9000))
      }
      venues[venueId] = { name: String(prev.name || venueId), credits: Math.max(0, nextCredits), active: prev.active !== false, pin, email: String(prev.email || '') }
      await writeVenues(venues)
      if (!existed && String(venues[venueId].email || '')) {
        const to = String(venues[venueId].email)
        const subject = `PIN del local ${venues[venueId].name}`
        const link = `${process.env.PUBLIC_BASE_URL || ''}/?venueId=${encodeURIComponent(venueId)}`
        const text = `Hola,\n\nSe creó el local "${venues[venueId].name}" (ID: ${venueId}).\nPIN del venue: ${pin}\nAcceso: ${link}\n\nSi lo deseas, puedes cambiar el PIN desde el Panel Admin.\n`
        try { await sendEmail(to, subject, text) } catch {}
      }
      json(res, 200, { ok: true, venueId, credits: venues[venueId].credits })
      return
    }
    if (pathname === '/api/admin/venues/pin' && req.method === 'POST') {
      if (!ADMIN_SECRET) { json(res, 403, { error: 'no_admin_secret' }); return }
      if (!isAdminAuthorized(req, query)) { json(res, 403, { error: 'forbidden' }); return }
      const body = await parseBody(req)
      const venueId = String(body.venueId || '').trim()
      const pin = String(body.pin || '').trim()
      if (!venueId || !pin) { json(res, 400, { error: 'bad_input' }); return }
      const venues = await readVenues()
      const prev = venues[venueId] || { name: venueId, credits: 0, active: true, email: '' }
      venues[venueId] = { name: String(prev.name || venueId), credits: Number(prev.credits || 0), active: prev.active !== false, pin, email: String(prev.email || '') }
      await writeVenues(venues)
      if (String(venues[venueId].email || '')) {
        const to = String(venues[venueId].email)
        const subject = `PIN actualizado para ${venues[venueId].name}`
        const text = `Hola,\n\nEl PIN del local "${venues[venueId].name}" (ID: ${venueId}) fue actualizado.\nNuevo PIN: ${pin}\n\nSi no solicitaste este cambio, por favor contáctanos.\n`
        try { await sendEmail(to, subject, text) } catch {}
      }
      json(res, 200, { ok: true, venueId, pin })
      return
    }
    if (pathname === '/api/admin/venues/email' && req.method === 'POST') {
      if (!ADMIN_SECRET) { json(res, 403, { error: 'no_admin_secret' }); return }
      if (!isAdminAuthorized(req, query)) { json(res, 403, { error: 'forbidden' }); return }
      const body = await parseBody(req)
      const venueId = String(body.venueId || '').trim()
      const email = String(body.email || '').trim()
      if (!venueId || !email || !email.includes('@')) { json(res, 400, { error: 'bad_input' }); return }
      const venues = await readVenues()
      const prev = venues[venueId] || { name: venueId, credits: 0, active: true, pin: '' }
      venues[venueId] = { name: String(prev.name || venueId), credits: Number(prev.credits || 0), active: prev.active !== false, pin: String(prev.pin || ''), email }
      await writeVenues(venues)
      json(res, 200, { ok: true, venueId, email })
      return
    }
    if (pathname === '/api/admin/venues/delete' && req.method === 'POST') {
      if (!ADMIN_SECRET) { json(res, 403, { error: 'no_admin_secret' }); return }
      if (!isAdminAuthorized(req, query)) { json(res, 403, { error: 'forbidden' }); return }
      const body = await parseBody(req)
      const venueId = String(body.venueId || '').trim()
      if (!venueId) { json(res, 400, { error: 'bad_input' }); return }
      for (const s of state.sessions.values()) {
        if (s.venueId === venueId) endAndArchive(s.id)
      }
      if (db) {
        try {
          await initDB()
          await db.query('DELETE FROM venues WHERE venue_id=$1', [String(venueId)])
        } catch {}
      } else {
        const venues = await readVenues()
        if (venues[venueId]) {
          delete venues[venueId]
          await writeVenues(venues)
        }
      }
      json(res, 200, { ok: true, venueId })
      return
    }
    if (pathname === '/api/admin/venues/active' && req.method === 'POST') {
      if (!ADMIN_SECRET) { json(res, 403, { error: 'no_admin_secret' }); return }
      if (!isAdminAuthorized(req, query)) { json(res, 403, { error: 'forbidden' }); return }
      const body = await parseBody(req)
      const venueId = String(body.venueId || '').trim()
      const active = !!body.active
      if (!venueId) { json(res, 400, { error: 'bad_input' }); return }
      const venues = await readVenues()
      const prev = venues[venueId] || { name: venueId, credits: 0, active: true, pin: '', email: '' }
      venues[venueId] = { name: String(prev.name || venueId), credits: Number(prev.credits || 0), active, pin: String(prev.pin || ''), email: String(prev.email || '') }
      await writeVenues(venues)
      json(res, 200, { ok: true, venueId, active })
      return
    }
    if (pathname === '/api/admin/venues/name' && req.method === 'POST') {
      if (!ADMIN_SECRET) { json(res, 403, { error: 'no_admin_secret' }); return }
      if (!isAdminAuthorized(req, query)) { json(res, 403, { error: 'forbidden' }); return }
      const body = await parseBody(req)
      const venueId = String(body.venueId || '').trim()
      const name = String(body.name || '').trim()
      if (!venueId || !name) { json(res, 400, { error: 'bad_input' }); return }
      const venues = await readVenues()
      const prev = venues[venueId] || { credits: 0, active: true, pin: '', email: '' }
      venues[venueId] = { name, credits: Number(prev.credits || 0), active: prev.active !== false, pin: String(prev.pin || ''), email: String(prev.email || '') }
      await writeVenues(venues)
      json(res, 200, { ok: true, venueId, name })
      return
    }
    if (pathname === '/api/admin/db-status' && req.method === 'GET') {
      if (!ADMIN_SECRET) { json(res, 403, { error: 'no_admin_secret' }); return }
      if (!isAdminAuthorized(req, query)) { json(res, 403, { error: 'forbidden' }); return }
      const connected = await isDBConnected()
      json(res, 200, { connected })
      return
    }
    if (pathname === '/api/admin/db-tables' && req.method === 'GET') {
      if (!ADMIN_SECRET) { json(res, 403, { error: 'no_admin_secret' }); return }
      if (!isAdminAuthorized(req, query)) { json(res, 403, { error: 'forbidden' }); return }
      const tables = await listPublicTables()
      json(res, 200, { tables })
      return
    }
    if (pathname === '/api/admin/db-init' && req.method === 'POST') {
      if (!ADMIN_SECRET) { json(res, 403, { error: 'no_admin_secret' }); return }
      if (!isAdminAuthorized(req, query)) { json(res, 403, { error: 'forbidden' }); return }
      await initDB()
      const tables = await listPublicTables()
      json(res, 200, { ok: true, tables })
      return
    }
    if (pathname === '/api/user/update' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      if (typeof body.alias === 'string') u.alias = body.alias.slice(0,32)
      if (Array.isArray(body.tags)) u.prefs.tags = body.tags.slice(0,5)
      if (typeof body.visibility === 'string') u.visibility = body.visibility
      if (typeof body.tableId === 'string') u.tableId = body.tableId.slice(0,32)
      if (typeof body.available === 'boolean') u.available = body.available
      try { await dbUpsertUser(u) } catch {}
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/user/pause' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      u.pausedUntil = now() + 30*60*1000
      try { await dbUpsertUser(u) } catch {}
      json(res, 200, { ok: true, pausedUntil: u.pausedUntil })
      return
    }
    if (pathname === '/api/user/change-table' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      const hourKey = u.id
      const bucket = state.rate.tableChangesByUserHour.get(hourKey) || []
      const fresh = bucket.filter(ts => within(60*60*1000, ts))
      if (fresh.length >= 2) { json(res, 429, { error: 'table_changes_limit' }); return }
      u.tableId = String(body.newTable || '').slice(0,32)
      state.rate.tableChangesByUserHour.set(hourKey, [...fresh, now()])
      try { await dbUpsertUser(u) } catch {}
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/moderation/approve-selfie' && req.method === 'POST') {
      const body = await parseBody(req)
      const staff = state.users.get(body.staffId)
      const target = state.users.get(body.userId)
      if (!staff || staff.role !== 'staff') { json(res, 403, { error: 'no_staff' }); return }
      if (!target) { json(res, 404, { error: 'no_user' }); return }
      target.selfieApproved = true
      try { await dbUpsertUser(target) } catch {}
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/user/available' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      u.available = !!body.available
      u.receiveMode = body.receiveMode || 'all'
      u.prefs = body.prefs || {}
      u.zone = body.zone || ''
      try { await dbUpsertUser(u) } catch {}
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/users/available' && req.method === 'GET') {
      const sessionId = query.sessionId
      const only = query.onlyAvailable === 'true'
      const tagsQ = (query.tags || '').split(',').filter(Boolean)
      const zoneQ = query.zone || ''
      const min = Number(query.ageMin || 0)
      const max = Number(query.ageMax || 200)
      const excludeId = String(query.excludeUserId || '')
      const arr = []
      for (const u of state.users.values()) {
        if (u.sessionId !== sessionId || u.role !== 'user') continue
        if (only && !u.available) continue
        if (excludeId && u.id === excludeId) continue
        if (u.danceState && u.danceState !== 'idle') continue
        const ageOk = u.prefs && u.prefs.age ? (u.prefs.age >= min && u.prefs.age <= max) : true
        const tagsOk = tagsQ.length ? (Array.isArray(u.prefs.tags) && tagsQ.every(t => u.prefs.tags.includes(t))) : true
        const zoneOk = zoneQ ? (u.zone === zoneQ) : true
        const partner = (u.dancePartnerId && state.users.get(u.dancePartnerId)) || null
        const partnerAlias = partner ? (partner.alias || partner.id) : ''
        arr.push({ id: u.id, alias: u.alias, selfie: u.selfieApproved ? u.selfie : '', tags: u.prefs.tags || [], zone: u.zone, available: u.available, tableId: u.tableId, danceState: u.danceState || 'idle', partnerAlias })
      }
      json(res, 200, { users: arr })
      return
    }
    if (pathname === '/api/mesas/active' && req.method === 'GET') {
      const sessionId = query.sessionId
      const map = new Map()
      for (const u of state.users.values()) {
        if (u.sessionId !== sessionId || u.role !== 'user') continue
        const t = u.tableId || ''
        if (!t) continue
        const entry = map.get(t) || { tableId: t, people: 0, disponibles: 0, incognitos: 0, tags: [] }
        entry.people += (u.visibility !== 'incognito') ? 1 : 0
        entry.incognitos += (u.visibility === 'incognito') ? 1 : 0
        entry.disponibles += (u.available ? 1 : 0)
        if (Array.isArray(u.prefs.tags)) {
          for (const tag of u.prefs.tags) if (!entry.tags.includes(tag)) entry.tags.push(tag)
        }
        map.set(t, entry)
      }
      json(res, 200, { mesas: Array.from(map.values()) })
      return
    }
    if (pathname === '/api/invite/dance' && req.method === 'POST') {
      const body = await parseBody(req)
      const from = state.users.get(body.fromId)
      const to = state.users.get(body.toId)
      if (!from || !to) { json(res, 404, { error: 'no_user' }); return }
      if (from.danceState && from.danceState !== 'idle') { json(res, 403, { error: 'busy_self' }); return }
      if (to.danceState && to.danceState !== 'idle') { json(res, 403, { error: 'busy_target' }); return }
      if (from.muted || from.silenced) { json(res, 429, { error: 'silenced' }); return }
      if (to.pausedUntil && now() < to.pausedUntil) { json(res, 403, { error: 'paused' }); return }
      if (applyRestrictedIfNeeded(from.id)) { json(res, 429, { error: 'restricted' }); return }
      if (isBlockedPair(from.id, to.id)) { json(res, 403, { error: 'blocked' }); return }
      if (to.receiveMode === 'mesas') {
        if (!from.zone || !to.zone || from.zone !== to.zone) { json(res, 403, { error: 'mode_mesas' }); return }
      }
      if (to.receiveMode === 'invitedOnly') {
        const allowed = to.allowedSenders && to.allowedSenders.has(from.id)
        if (!allowed) { json(res, 403, { error: 'mode_invited_only' }); return }
      }
      const msg = body.messageType === 'invitoCancion' ? 'invitoCancion' : 'bailamos'
      if (!rateCanInvite(from.id, to.id)) { json(res, 429, { error: 'rate' }); return }
      const invId = genId('inv')
      const inv = { id: invId, sessionId: from.sessionId, fromId: from.id, toId: to.id, msg, status: 'pendiente', createdAt: now() }
      state.invites.set(invId, inv)
      const fromSelfie = from.selfieApproved ? from.selfie : ''
      sendToUser(to.id, 'dance_invite', { invite: { id: invId, from: { id: from.id, alias: from.alias, selfie: fromSelfie, tableId: from.tableId || '', zone: from.zone || '' } , msg } })
      for (const other of state.invites.values()) {
        if (other.sessionId === inv.sessionId && other.fromId === to.id && other.toId === from.id) {
          if (within(10*60*1000, other.createdAt)) {
            sendToUser(from.id, 'match', { with: { id: to.id, alias: to.alias } })
            sendToUser(to.id, 'match', { with: { id: from.id, alias: from.alias } })
          }
        }
      }
      json(res, 200, { inviteId: invId })
      return
    }
    if (pathname === '/api/invite/respond' && req.method === 'POST') {
      const body = await parseBody(req)
      const inv = state.invites.get(body.inviteId)
      if (!inv) { json(res, 404, { error: 'no_invite' }); return }
      const action = body.action === 'accept' ? 'accept' : 'pass'
      const note = String(body.note || '').slice(0, 140)
      if (action === 'pass') {
        inv.status = 'pasado'
        state.rate.lastInvitePair.set(`${inv.fromId}:${inv.toId}`, { ts: now(), blocked: true })
        sendToUser(inv.fromId, 'invite_result', { inviteId: inv.id, status: 'pasado', note })
        json(res, 200, { ok: true })
        return
      }
      inv.status = 'aceptado'
      const toUser = state.users.get(inv.toId)
      if (toUser && toUser.allowedSenders) toUser.allowedSenders.add(inv.fromId)
      const meetingId = genId('meet')
      const points = ['Pista', 'Barra', 'Punto X']
      const point = points[Math.floor(Math.random() * points.length)]
      const minutes = Math.floor(Math.random() * 6) + 5
      const expiresAt = now() + minutes * 60 * 1000
      const meeting = { id: meetingId, sessionId: inv.sessionId, inviteId: inv.id, point, expiresAt, cancelled: false }
      state.meetings.set(meetingId, meeting)
      // Marcar estado "esperando" para ambos
      const uFrom = state.users.get(inv.fromId); const uTo = state.users.get(inv.toId)
      if (uFrom) { uFrom.danceState = 'waiting'; uFrom.dancePartnerId = inv.toId; uFrom.meetingId = meetingId }
      if (uTo) { uTo.danceState = 'waiting'; uTo.dancePartnerId = inv.fromId; uTo.meetingId = meetingId }
      // Notificar estado a ambos
      sendToUser(inv.fromId, 'dance_status', { state: 'waiting', partner: { id: inv.toId, alias: uTo ? (uTo.alias || uTo.id) : '' }, meeting })
      sendToUser(inv.toId, 'dance_status', { state: 'waiting', partner: { id: inv.fromId, alias: uFrom ? (uFrom.alias || uFrom.id) : '' }, meeting })
      sendToUser(inv.fromId, 'invite_result', { inviteId: inv.id, status: 'aceptado', meeting, note })
      sendToUser(inv.toId, 'invite_result', { inviteId: inv.id, status: 'aceptado', meeting, note })
      json(res, 200, { meeting })
      return
    }
    if (pathname === '/api/meeting/cancel' && req.method === 'POST') {
      const body = await parseBody(req)
      const m = state.meetings.get(body.meetingId)
      if (!m) { json(res, 404, { error: 'no_meeting' }); return }
      m.cancelled = true
      try {
        const inv = state.invites.get(m.inviteId)
        const uFrom = inv ? state.users.get(inv.fromId) : null
        const uTo = inv ? state.users.get(inv.toId) : null
        if (uFrom) { uFrom.danceState = 'idle'; uFrom.dancePartnerId = ''; uFrom.meetingId = '' }
        if (uTo) { uTo.danceState = 'idle'; uTo.dancePartnerId = ''; uTo.meetingId = '' }
        if (inv) {
          sendToUser(inv.fromId, 'dance_status', { state: 'idle' })
          sendToUser(inv.toId, 'dance_status', { state: 'idle' })
        }
      } catch {}
      sendToStaff(m.sessionId, 'meeting_cancel', { meetingId: m.id })
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/meeting/confirm' && req.method === 'POST') {
      const body = await parseBody(req)
      const m = state.meetings.get(body.meetingId)
      if (!m) { json(res, 404, { error: 'no_meeting' }); return }
      // Cambiar estado a "bailando" para ambos
      const inv = state.invites.get(m.inviteId)
      if (inv) {
        const uFrom = state.users.get(inv.fromId); const uTo = state.users.get(inv.toId)
        if (uFrom) { uFrom.danceState = 'dancing'; uFrom.meetingId = m.id }
        if (uTo) { uTo.danceState = 'dancing'; uTo.meetingId = m.id }
        sendToUser(inv.fromId, 'dance_status', { state: 'dancing', partner: { id: inv.toId, alias: uTo ? (uTo.alias || uTo.id) : '' }, meeting: m })
        sendToUser(inv.toId, 'dance_status', { state: 'dancing', partner: { id: inv.fromId, alias: uFrom ? (uFrom.alias || uFrom.id) : '' }, meeting: m })
        const planTxt = String(body.plan || '')
        sendToUser(inv.fromId, 'meeting_plan', { meetingId: m.id, plan: planTxt })
        sendToUser(inv.toId, 'meeting_plan', { meetingId: m.id, plan: planTxt })
      }
      sendToStaff(m.sessionId, 'meeting_confirm', { meetingId: m.id, plan: body.plan || '' })
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/block' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      const t = state.users.get(body.targetId)
      if (!u || !t) { json(res, 404, { error: 'no_user' }); return }
      state.blocks.add(`${u.id}:${t.id}`)
      markBlockedEvent(body.targetId)
      persistReports(u.sessionId)
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/report' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      const t = state.users.get(body.targetId)
      if (!u || !t) { json(res, 404, { error: 'no_user' }); return }
      state.reports.push({ sessionId: u.sessionId, fromId: u.id, targetId: t.id, category: body.category || '', note: String(body.note || '').slice(0, 140), ts: now() })
      sendToStaff(u.sessionId, 'report', { targetId: t.id })
      persistReports(u.sessionId)
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/consumption/invite' && req.method === 'POST') {
      const body = await parseBody(req)
      const from = state.users.get(body.fromId)
      const to = state.users.get(body.toId)
      if (!from || !to) { json(res, 404, { error: 'no_user' }); return }
      if (isBlockedPair(from.id, to.id)) { json(res, 403, { error: 'blocked' }); return }
      const hourKey = from.id
      const bucket = state.rate.consumptionByUserHour.get(hourKey) || []
      const fresh = bucket.filter(ts => within(60 * 60 * 1000, ts))
      if (fresh.length >= 5) { json(res, 429, { error: 'rate_consumo' }); return }
      state.rate.consumptionByUserHour.set(hourKey, [...fresh, now()])
      const reqId = genId('cinv')
      const note = String(body.note || '').slice(0, 140)
      sendToUser(to.id, 'consumption_invite', { requestId: reqId, from: { id: from.id, alias: from.alias, tableId: from.tableId || '' }, product: body.product, note })
      json(res, 200, { requestId: reqId })
      return
    }
    if (pathname === '/api/consumption/invite/bulk' && req.method === 'POST') {
      const body = await parseBody(req)
      const from = state.users.get(body.fromId)
      const to = state.users.get(body.toId)
      if (!from || !to) { json(res, 404, { error: 'no_user' }); return }
      if (isBlockedPair(from.id, to.id)) { json(res, 403, { error: 'blocked' }); return }
      const hourKey = from.id
      const bucket = state.rate.consumptionByUserHour.get(hourKey) || []
      const fresh = bucket.filter(ts => within(60 * 60 * 1000, ts))
      if (fresh.length >= 5) { json(res, 429, { error: 'rate_consumo' }); return }
      state.rate.consumptionByUserHour.set(hourKey, [...fresh, now()])
      const items = Array.isArray(body.items) ? body.items.map(it => ({ product: String(it.product || ''), quantity: Math.max(1, Number(it.quantity || 1)) })) : []
      const filtered = items.filter(it => it.product)
      if (!filtered.length) { json(res, 400, { error: 'no_items' }); return }
      const reqId = genId('cinv')
      const note = String(body.note || '').slice(0, 140)
      sendToUser(to.id, 'consumption_invite_bulk', { requestId: reqId, from: { id: from.id, alias: from.alias, tableId: from.tableId || '' }, items: filtered, note })
      json(res, 200, { requestId: reqId })
      return
    }
    if (pathname === '/api/consumption/respond' && req.method === 'POST') {
      const body = await parseBody(req)
      if (body.action !== 'accept') { json(res, 200, { ok: true }); return }
      const from = state.users.get(body.fromId)
      const to = state.users.get(body.toId)
      if (!from || !to) { json(res, 404, { error: 'no_user' }); return }
      const orderId = genId('ord')
      const expiresAt = now() + 10 * 60 * 1000
      const order = { id: orderId, sessionId: from.sessionId, emitterId: from.id, receiverId: to.id, product: body.product, quantity: 1, price: 0, total: 0, status: 'pendiente_cobro', createdAt: now(), expiresAt, emitterTable: from.tableId || '', receiverTable: to.tableId || '', mesaEntrega: to.tableId || '', isInvitation: true }
      state.orders.set(orderId, order)
      sendToStaff(order.sessionId, 'order_new', { order })
      try { await dbInsertOrder(order) } catch {}
      persistOrders(order.sessionId)
      json(res, 200, { orderId })
      return
    }
    if (pathname === '/api/consumption/respond/bulk' && req.method === 'POST') {
      const body = await parseBody(req)
      if (body.action !== 'accept') { json(res, 200, { ok: true }); return }
      const from = state.users.get(body.fromId)
      const to = state.users.get(body.toId)
      if (!from || !to) { json(res, 404, { error: 'no_user' }); return }
      const items = Array.isArray(body.items) ? body.items.map(it => ({ product: String(it.product || ''), quantity: Math.max(1, Number(it.quantity || 1)) })) : []
      const filtered = items.filter(it => it.product)
      const ids = []
      for (const it of filtered) {
        const orderId = genId('ord')
        const expiresAt = now() + 10 * 60 * 1000
        const order = { id: orderId, sessionId: from.sessionId, emitterId: from.id, receiverId: to.id, product: it.product, quantity: it.quantity, price: 0, total: 0, status: 'pendiente_cobro', createdAt: now(), expiresAt, emitterTable: from.tableId || '', receiverTable: to.tableId || '', mesaEntrega: to.tableId || '', isInvitation: true }
        state.orders.set(orderId, order)
        ids.push(orderId)
        sendToStaff(order.sessionId, 'order_new', { order })
        try { await dbInsertOrder(order) } catch {}
      }
      if (ids.length) persistOrders(from.sessionId)
      json(res, 200, { orderIds: ids })
      return
    }
    if (pathname === '/api/order/new' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      const orderId = genId('ord')
      const expiresAt = now() + 10 * 60 * 1000
      const itemName = String(body.product || '')
      const qty = Math.max(1, Number(body.quantity || 1))
      const s = ensureSession(u.sessionId)
      const base = db ? await dbReadGlobalCatalog() : readGlobalCatalog()
      const itemsBase = (s && Array.isArray(s.catalog) && s.catalog.length) ? s.catalog : base
      const found = itemsBase.find(i => i.name === itemName)
      const price = found ? found.price : 0
      const total = price * qty
      const mesaEntrega = u.tableId || ''
      const order = { id: orderId, sessionId: u.sessionId, emitterId: u.id, receiverId: u.id, product: itemName, quantity: qty, price, total, status: 'pendiente_cobro', createdAt: now(), expiresAt, emitterTable: u.tableId || '', receiverTable: u.tableId || '', mesaEntrega }
      state.orders.set(orderId, order)
      sendToStaff(order.sessionId, 'order_new', { order })
      sendToUser(u.id, 'order_update', { order })
      try { await dbInsertOrder(order) } catch {}
      persistOrders(order.sessionId)
      json(res, 200, { orderId })
      return
    }
    if (pathname === '/api/order/bulk' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      const s = ensureSession(u.sessionId)
      const base = db ? await dbReadGlobalCatalog() : readGlobalCatalog()
      const itemsBase = (s && Array.isArray(s.catalog) && s.catalog.length) ? s.catalog : base
      const items = Array.isArray(body.items) ? body.items : []
      const orderIds = []
      for (const it of items) {
        const name = String(it.product || '')
        const qty = Math.max(1, Number(it.quantity || 1))
        if (!name || qty <= 0) continue
        const found = itemsBase.find(i => i.name === name)
        const price = found ? Number(found.price || 0) : 0
        const total = price * qty
        const orderId = genId('ord')
        const expiresAt = now() + 10 * 60 * 1000
        const mesaEntrega = u.tableId || ''
        const order = { id: orderId, sessionId: u.sessionId, emitterId: u.id, receiverId: u.id, product: name, quantity: qty, price, total, status: 'pendiente_cobro', createdAt: now(), expiresAt, emitterTable: u.tableId || '', receiverTable: u.tableId || '', mesaEntrega }
        state.orders.set(orderId, order)
        orderIds.push(orderId)
        sendToStaff(order.sessionId, 'order_new', { order })
        sendToUser(u.id, 'order_update', { order })
        try { await dbInsertOrder(order) } catch {}
      }
      if (orderIds.length) persistOrders(u.sessionId)
      json(res, 200, { orderIds })
      return
    }
    if (pathname.startsWith('/api/staff/orders/') && req.method === 'POST') {
      const orderId = pathname.split('/').pop()
      const body = await parseBody(req)
      const order = state.orders.get(orderId)
      if (!order) { json(res, 404, { error: 'no_order' }); return }
      const status = body.status
      if (!['cobrado', 'entregado', 'cancelado', 'en_preparacion', 'expirado'].includes(status)) { json(res, 400, { error: 'bad_status' }); return }
      order.status = status
      sendToUser(order.emitterId, 'order_update', { order })
      sendToUser(order.receiverId, 'order_update', { order })
      sendToStaff(order.sessionId, 'order_update', { order })
      try { await dbUpdateOrderStatus(order.id, order.status) } catch {}
      persistOrders(order.sessionId)
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/staff/orders' && req.method === 'GET') {
      const sessionId = query.sessionId
      const stateFilter = query.state || ''
      if (db) {
        const rows = await dbGetOrdersBySession(sessionId, stateFilter || null)
        const out = []
        for (const r of rows) {
          out.push({
            id: r.id,
            product: r.product,
            quantity: Number(r.quantity || 1),
            price: Number(r.price || 0),
            total: Number(r.total || 0),
            status: r.status,
            createdAt: Number(r.created_at || 0),
            expiresAt: Number(r.expires_at || 0),
            emitterId: r.emitter_id,
            receiverId: r.receiver_id,
            emitterTable: r.emitter_table || '',
            receiverTable: r.receiver_table || '',
            mesaEntrega: r.mesa_entrega || '',
            isInvitation: !!r.is_invitation
          })
        }
        json(res, 200, { orders: out })
      } else {
        const list = []
        for (const o of state.orders.values()) {
          if (o.sessionId !== sessionId) continue
          if (stateFilter && o.status !== stateFilter) continue
          list.push({
            id: o.id,
            product: o.product,
            quantity: Number(o.quantity || 1),
            price: Number(o.price || 0),
            total: Number(o.total || 0),
            status: o.status,
            createdAt: Number(o.createdAt || 0),
            expiresAt: Number(o.expiresAt || 0),
            emitterId: o.emitterId,
            receiverId: o.receiverId,
            emitterTable: o.emitterTable || '',
            receiverTable: o.receiverTable || '',
            mesaEntrega: o.mesaEntrega || '',
            isInvitation: !!o.isInvitation
          })
        }
        json(res, 200, { orders: list })
      }
      return
    }
    if (pathname === '/api/table/orders' && req.method === 'GET') {
      const sessionId = query.sessionId
      const tableId = String(query.tableId || '')
      const userId = query.userId
      const u = state.users.get(userId)
      if (!u || u.sessionId !== sessionId || u.role !== 'user') { json(res, 403, { error: 'no_user' }); return }
      if (u.tableId !== tableId) { json(res, 403, { error: 'not_in_table' }); return }
      const s = ensureSession(sessionId)
      const closed = db ? await dbIsTableClosed(sessionId, tableId) : (s && s.closedTables && s.closedTables.has(tableId))
      if (closed) { json(res, 200, { orders: [] }); return }
      if (db) {
        const rows = await dbGetOrdersByTable(sessionId, tableId)
        const out = []
        for (const o of rows) {
          let emitterAlias = ''
          let receiverAlias = ''
          const em = state.users.get(o.emitter_id) || (db ? await dbGetUser(o.emitter_id) : null)
          const re = state.users.get(o.receiver_id) || (db ? await dbGetUser(o.receiver_id) : null)
          emitterAlias = em ? em.alias : ''
          receiverAlias = re ? re.alias : ''
          out.push({
            id: o.id,
            product: o.product,
            quantity: o.quantity || 1,
            price: o.price || 0,
            total: o.total || 0,
            status: o.status,
            createdAt: o.created_at,
            expiresAt: o.expires_at,
            emitterId: o.emitter_id,
            receiverId: o.receiver_id,
            emitterAlias,
            receiverAlias,
            mesaEntrega: o.mesa_entrega || '',
          })
        }
        json(res, 200, { orders: out })
      } else {
        const list = []
        for (const o of state.orders.values()) {
          if (o.sessionId !== sessionId) continue
          if (o.emitterTable === tableId || o.receiverTable === tableId || o.mesaEntrega === tableId) {
            const emitter = state.users.get(o.emitterId)
            const receiver = state.users.get(o.receiverId)
            list.push({
              id: o.id,
              product: o.product,
              quantity: o.quantity || 1,
              price: o.price || 0,
              total: o.total || 0,
              status: o.status,
              createdAt: o.createdAt,
              expiresAt: o.expiresAt,
              emitterId: o.emitterId,
              receiverId: o.receiverId,
              emitterAlias: emitter ? emitter.alias : '',
              receiverAlias: receiver ? receiver.alias : '',
              mesaEntrega: o.mesaEntrega || '',
            })
          }
        }
        json(res, 200, { orders: list })
      }
      return
    }
    if (pathname === '/api/staff/table/orders' && req.method === 'GET') {
      const sessionId = query.sessionId
      const tableId = String(query.tableId || '')
      const s = ensureSession(sessionId)
      const closed = db ? await dbIsTableClosed(sessionId, tableId) : (s && s.closedTables && s.closedTables.has(tableId))
      if (closed) { json(res, 200, { orders: [] }); return }
      if (db) {
        const rows = await dbGetOrdersByTable(sessionId, tableId)
        const out = []
        for (const o of rows) {
          let emitterAlias = ''
          let receiverAlias = ''
          const em = state.users.get(o.emitter_id) || (db ? await dbGetUser(o.emitter_id) : null)
          const re = state.users.get(o.receiver_id) || (db ? await dbGetUser(o.receiver_id) : null)
          emitterAlias = em ? em.alias : ''
          receiverAlias = re ? re.alias : ''
          out.push({
            id: o.id,
            product: o.product,
            quantity: o.quantity || 1,
            price: o.price || 0,
            total: o.total || 0,
            status: o.status,
            createdAt: o.created_at,
            expiresAt: o.expires_at,
            emitterId: o.emitter_id,
            receiverId: o.receiver_id,
            emitterAlias,
            receiverAlias,
            mesaEntrega: o.mesa_entrega || '',
          })
        }
        json(res, 200, { orders: out })
      } else {
        const list = []
        for (const o of state.orders.values()) {
          if (o.sessionId !== sessionId) continue
          if (o.emitterTable === tableId || o.receiverTable === tableId || o.mesaEntrega === tableId) {
            const emitter = state.users.get(o.emitterId)
            const receiver = state.users.get(o.receiverId)
            list.push({
              id: o.id,
              product: o.product,
              quantity: o.quantity || 1,
              price: o.price || 0,
              total: o.total || 0,
              status: o.status,
              createdAt: o.createdAt,
              expiresAt: o.expiresAt,
              emitterId: o.emitterId,
              receiverId: o.receiverId,
              emitterAlias: emitter ? emitter.alias : '',
              receiverAlias: receiver ? receiver.alias : '',
              mesaEntrega: o.mesaEntrega || '',
            })
          }
        }
        json(res, 200, { orders: list })
      }
      return
    }
    if (pathname === '/api/staff/table/close' && req.method === 'POST') {
      const body = await parseBody(req)
      const s = ensureSession(body.sessionId)
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      const t = String(body.tableId || '')
      s.closedTables = s.closedTables || new Set()
      if (body.closed) s.closedTables.add(t)
      else s.closedTables.delete(t)
      try { await dbSetTableClosed(s.id, t, !!body.closed) } catch {}
      sendToStaff(s.id, 'table_closed', { tableId: t, closed: !!body.closed })
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/user/orders' && req.method === 'GET') {
      const userId = query.userId
      if (db) {
        const rows = await dbGetOrdersByUser(userId)
        const out = []
        for (const r of rows) {
          let emitterAlias = ''
          let receiverAlias = ''
          const em = state.users.get(r.emitter_id) || (db ? await dbGetUser(r.emitter_id) : null)
          const re = state.users.get(r.receiver_id) || (db ? await dbGetUser(r.receiver_id) : null)
          emitterAlias = em ? (em.alias || '') : ''
          receiverAlias = re ? (re.alias || '') : ''
          out.push({
            id: r.id,
            product: r.product,
            quantity: Number(r.quantity || 1),
            price: Number(r.price || 0),
            total: Number(r.total || 0),
            status: r.status,
            createdAt: Number(r.created_at || 0),
            expiresAt: Number(r.expires_at || 0),
            emitterId: r.emitter_id,
            receiverId: r.receiver_id,
            emitterTable: r.emitter_table || '',
            receiverTable: r.receiver_table || '',
            mesaEntrega: r.mesa_entrega || '',
            emitterAlias,
            receiverAlias,
            isInvitation: !!r.is_invitation
          })
        }
        json(res, 200, { orders: out })
      } else {
        const list = []
        for (const o of state.orders.values()) {
          if (o.emitterId === userId || o.receiverId === userId) {
            const em = state.users.get(o.emitterId)
            const re = state.users.get(o.receiverId)
            list.push({
              id: o.id,
              product: o.product,
              quantity: Number(o.quantity || 1),
              price: Number(o.price || 0),
              total: Number(o.total || 0),
              status: o.status,
              createdAt: Number(o.createdAt || 0),
              expiresAt: Number(o.expiresAt || 0),
              emitterId: o.emitterId,
              receiverId: o.receiverId,
              emitterTable: o.emitterTable || '',
              receiverTable: o.receiverTable || '',
              mesaEntrega: o.mesaEntrega || '',
              emitterAlias: em ? (em.alias || '') : '',
              receiverAlias: re ? (re.alias || '') : '',
              isInvitation: !!o.isInvitation
            })
          }
        }
        json(res, 200, { orders: list })
      }
      return
    }
    if (pathname === '/api/dance/finish' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      const partnerId = u.dancePartnerId || ''
      const p = partnerId ? state.users.get(partnerId) : null
      u.danceState = 'idle'; u.dancePartnerId = ''; u.meetingId = ''
      if (p) { p.danceState = 'idle'; p.dancePartnerId = ''; p.meetingId = '' }
      try {
        sendToUser(u.id, 'dance_status', { state: 'idle' })
        if (p) sendToUser(p.id, 'dance_status', { state: 'idle' })
        if (u.meetingId) {
          const m = state.meetings.get(u.meetingId)
          if (m) {
            m.cancelled = true
            sendToStaff(m.sessionId, 'meeting_cancel', { meetingId: m.id })
          }
        }
      } catch {}
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/waiter/call' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      const callId = genId('call')
      const call = { id: callId, sessionId: u.sessionId, userId: u.id, tableId: u.tableId || '', reason: String(body.reason || ''), status: 'pendiente', ts: now() }
      state.waiterCalls.set(callId, call)
      sendToStaff(call.sessionId, 'waiter_call', { call })
      sendToUser(call.userId, 'waiter_update', { call })
      try { await dbInsertWaiterCall(call) } catch {}
      json(res, 200, { callId })
      return
    }
    if (pathname === '/api/staff/waiter' && req.method === 'GET') {
      const sessionId = query.sessionId
      if (db) {
        const rows = await dbGetWaiterCalls(sessionId)
        const out = []
        for (const c of rows) {
          const u = state.users.get(c.user_id) || (db ? await dbGetUser(c.user_id) : null)
          out.push({
            id: c.id,
            sessionId: c.session_id,
            userId: c.user_id,
            tableId: c.table_id || '',
            reason: c.reason,
            status: c.status,
            ts: c.ts,
            userAlias: u ? u.alias : '',
            zone: u ? u.zone : '',
          })
        }
        json(res, 200, { calls: out })
      } else {
        const list = []
        for (const c of state.waiterCalls.values()) {
          if (c.sessionId !== sessionId) continue
          const u = state.users.get(c.userId)
          list.push({
            id: c.id,
            sessionId: c.sessionId,
            userId: c.userId,
            tableId: c.tableId || '',
            reason: c.reason,
            status: c.status,
            ts: c.ts,
            userAlias: u ? u.alias : '',
            zone: u ? u.zone : '',
          })
        }
        json(res, 200, { calls: list })
      }
      return
    }
    if (pathname.startsWith('/api/staff/waiter/') && req.method === 'POST') {
      const callId = pathname.split('/').pop()
      const body = await parseBody(req)
      const c = state.waiterCalls.get(callId)
      if (!c) { json(res, 404, { error: 'no_call' }); return }
      c.status = body.status || 'atendido'
      sendToStaff(c.sessionId, 'waiter_update', { call: c })
      sendToUser(c.userId, 'waiter_update', { call: c })
      try { await dbUpdateWaiterCallStatus(c.id, c.status) } catch {}
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/reaction/send' && req.method === 'POST') {
      const body = await parseBody(req)
      const from = state.users.get(body.fromId)
      const to = state.users.get(body.toId)
      if (!from || !to) { json(res, 404, { error: 'no_user' }); return }
      if (isBlockedPair(from.id, to.id)) { json(res, 403, { error: 'blocked' }); return }
      const hourKey = from.id
      const bucket = state.rate.reactionsByUserHour.get(hourKey) || []
      const fresh = bucket.filter(ts => within(60*60*1000, ts))
      if (fresh.length >= 10) { json(res, 429, { error: 'rate_reaction' }); return }
      state.rate.reactionsByUserHour.set(hourKey, [...fresh, now()])
      const type = body.type === 'brindis' ? 'brindis' : 'saludo'
      sendToUser(to.id, 'reaction', { from: { id: from.id, alias: from.alias }, type })
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/staff/promos' && req.method === 'POST') {
      const body = await parseBody(req)
      const s = ensureSession(body.sessionId)
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      s.promos = Array.isArray(body.promos) ? body.promos : []
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/promos' && req.method === 'GET') {
      const sessionId = query.sessionId
      const s = ensureSession(sessionId)
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      json(res, 200, { promos: s.promos || [] })
      return
    }
    if (pathname === '/api/staff/analytics' && req.method === 'GET') {
      const sessionId = query.sessionId
      let usersCount = 0
      const mesasSet = new Set()
      let invitesSent = 0, invitesAccepted = 0
      const orders = { pendiente_cobro:0, cobrado:0, en_preparacion:0, entregado:0, cancelado:0, expirado:0 }
      for (const u of state.users.values()) if (u.sessionId === sessionId && u.role === 'user') { usersCount++; if (u.tableId) mesasSet.add(u.tableId) }
      for (const inv of state.invites.values()) if (inv.sessionId === sessionId) { invitesSent++; if (inv.status === 'aceptado') invitesAccepted++ }
      for (const o of state.orders.values()) if (o.sessionId === sessionId) orders[o.status] = (orders[o.status]||0)+1
      const topItems = {}
      for (const o of state.orders.values()) if (o.sessionId === sessionId) topItems[o.product] = (topItems[o.product]||0)+1
      json(res, 200, { usersCount, mesasActivas: mesasSet.size, invitesSent, invitesAccepted, orders, topItems })
      return
    }
    if (pathname === '/api/staff/catalog' && req.method === 'POST') {
      const body = await parseBody(req)
      const s = ensureSession(body.sessionId)
      const items = Array.isArray(body.items) ? body.items : []
      const clean = []
      for (const it of items) {
        const name = String(it.name || '').slice(0, 60)
        const price = Number(it.price || 0)
        if (!name) continue
        clean.push({ name, price })
      }
      if (s) s.catalog = clean
      if (db) {
        try { await dbWriteSessionCatalog(s ? s.id : 'global', clean) } catch {}
        try { await dbWriteGlobalCatalog(clean) } catch {}
      } else {
        writeGlobalCatalog(clean)
      }
      if (s) sendToStaff(s.id, 'catalog_update', { items: s.catalog })
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/catalog' && req.method === 'GET') {
      const sessionId = query.sessionId
      if (db) {
        let items = []
        if (sessionId) {
          try { items = await dbReadSessionCatalog(sessionId) } catch {}
        }
        if (!items || !items.length) {
          try { items = await dbReadGlobalCatalog() } catch {}
        }
        json(res, 200, { items })
        return
      }
      const s = sessionId ? ensureSession(sessionId) : null
      const base = readGlobalCatalog()
      const items = (s && Array.isArray(s.catalog) && s.catalog.length) ? s.catalog : base
      json(res, 200, { items })
      return
    }
    if (pathname === '/api/survey/submit' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      state.surveys = state.surveys || []
      state.surveys.push({ sessionId: u.sessionId, userId: u.id, score: Number(body.score||0), safe: !!body.safe, note: String(body.note||'').slice(0,140), ts: now() })
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/staff/users' && req.method === 'GET') {
      const sessionId = query.sessionId
      if (db) {
        const list = await dbGetUsersBySession(sessionId)
        json(res, 200, { users: list })
      } else {
        const list = []
        for (const u of state.users.values()) if (u.sessionId === sessionId && u.role === 'user') list.push({ id: u.id, alias: u.alias, selfieApproved: u.selfieApproved, muted: u.muted })
        json(res, 200, { users: list })
      }
      return
    }
    if (pathname === '/api/staff/moderate' && req.method === 'POST') {
      const body = await parseBody(req)
      const staff = state.users.get(body.staffId)
      const target = state.users.get(body.userId)
      if (!staff || staff.role !== 'staff') { json(res, 403, { error: 'no_staff' }); return }
      if (!target) { json(res, 404, { error: 'no_user' }); return }
      target.muted = !!body.muted
      try { await dbUpsertUser(target) } catch {}
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/staff/reports' && req.method === 'GET') {
      const sessionId = query.sessionId
      const list = state.reports.filter(r => r.sessionId === sessionId)
      json(res, 200, { reports: list })
      return
    }
    if (pathname === '/api/events/user' && req.method === 'GET') {
      const userId = query.userId
      const u = state.users.get(userId)
      if (!u) { res.writeHead(404); res.end(); return }
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'Access-Control-Allow-Origin': '*' })
      const arr = state.sseUsers.get(userId) || []
      arr.push(res)
      state.sseUsers.set(userId, arr)
      state.sseUserMeta.set(res, { startedAt: now(), lastWrite: now() })
      req.on('close', () => {
        const list = state.sseUsers.get(userId) || []
        state.sseUsers.set(userId, list.filter(r => r !== res))
        state.sseUserMeta.delete(res)
      })
      return
    }
    if (pathname === '/api/events/staff' && req.method === 'GET') {
      const sessionId = query.sessionId
      const s = ensureSession(sessionId)
      if (!s) { res.writeHead(404); res.end(); return }
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'Access-Control-Allow-Origin': '*' })
      const arr = state.sseStaff.get(sessionId) || []
      arr.push(res)
      state.sseStaff.set(sessionId, arr)
      state.sseStaffMeta.set(res, { startedAt: now(), lastWrite: now() })
      req.on('close', () => {
        const list = state.sseStaff.get(sessionId) || []
        state.sseStaff.set(sessionId, list.filter(r => r !== res))
        state.sseStaffMeta.delete(res)
      })
      return
    }
    if (pathname === '/api/health' && (req.method === 'GET' || req.method === 'HEAD')) {
      if (req.method === 'HEAD') { res.writeHead(200); res.end() } else { json(res, 200, { ok: true }) }
      return
    }
    if (pathname === '/api/network' && req.method === 'GET') {
      const nets = os.networkInterfaces()
      const ips = []
      for (const name of Object.keys(nets)) {
        for (const n of nets[name] || []) {
          if (n.family === 'IPv4' && !n.internal) ips.push(n.address)
        }
      }
      const port = String(process.env.PORT || 3000)
      const urls = ips.map(ip => `http://${ip}:${port}`)
      json(res, 200, { ips, port, urls })
      return
    }
    res.writeHead(404); res.end('Not found')
    return
  }
  serveStatic(req, res, pathname)
})

server.on('request', (req, res) => {
  const { pathname } = url.parse(req.url, true)
  if (pathname === '/favicon.ico') {
    res.writeHead(200, { 'Content-Type': 'image/x-icon', 'Cache-Control': 'max-age=86400', 'Access-Control-Allow-Origin': '*' })
    res.end('')
  }
})

setInterval(() => {
  const nowTs = now()
  for (const o of state.orders.values()) {
    if (o.status === 'pendiente_cobro' && nowTs > o.expiresAt) {
      o.status = 'expirado'
      sendToUser(o.emitterId, 'order_update', { order: o })
      sendToUser(o.receiverId, 'order_update', { order: o })
      sendToStaff(o.sessionId, 'order_update', { order: o })
      persistOrders(o.sessionId)
    }
  }
  for (const m of state.meetings.values()) {
    if (!m.cancelled && nowTs > m.expiresAt) {
      m.cancelled = true
      sendToStaff(m.sessionId, 'meeting_expired', { meetingId: m.id })
      const inv = state.invites.get(m.inviteId)
      if (inv) {
        const uFrom = state.users.get(inv.fromId)
        const uTo = state.users.get(inv.toId)
        if (uFrom) { uFrom.danceState = 'idle'; uFrom.dancePartnerId = ''; uFrom.meetingId = '' }
        if (uTo) { uTo.danceState = 'idle'; uTo.dancePartnerId = ''; uTo.meetingId = '' }
        try {
          sendToUser(inv.fromId, 'meeting_expired', { meetingId: m.id })
          sendToUser(inv.toId, 'meeting_expired', { meetingId: m.id })
          sendToUser(inv.fromId, 'dance_status', { state: 'idle' })
          sendToUser(inv.toId, 'dance_status', { state: 'idle' })
        } catch {}
      }
    }
  }
  // Cierre automático de SSE inactivas (>15 min sin eventos enviados)
  const INACT_MS = 15 * 60 * 1000
  for (const arr of state.sseUsers.values()) {
    for (const res of arr.slice()) {
      const meta = state.sseUserMeta.get(res)
      const last = meta ? meta.lastWrite : nowTs
      if ((nowTs - last) > INACT_MS) {
        try { res.end() } catch {}
        state.sseUserMeta.delete(res)
        const list = arr.filter(r => r !== res)
        // actualizar referencia en el mapa
        const uidEntry = [...state.sseUsers.entries()].find(([, vals]) => vals === arr)
        if (uidEntry) state.sseUsers.set(uidEntry[0], list)
      }
    }
  }
  for (const [sessId, arr] of state.sseStaff.entries()) {
    for (const res of arr.slice()) {
      const meta = state.sseStaffMeta.get(res)
      const last = meta ? meta.lastWrite : nowTs
      if ((nowTs - last) > INACT_MS) {
        try { res.end() } catch {}
        state.sseStaffMeta.delete(res)
        state.sseStaff.set(sessId, arr.filter(r => r !== res))
      }
    }
  }
}, 10000)

const PORT = process.env.PORT || 3000
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on http://localhost:${PORT}/`)
})
