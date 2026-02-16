const http = require('http')
const url = require('url')
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const os = require('os')

const SERVICE_NAME = String(process.env.SERVICE_NAME || 'discos')
const LOG_LEVEL = String(process.env.LOG_LEVEL || 'info')
const LOG_LEVELS = { debug: 10, info: 20, warn: 30, error: 40 }
function log(level, msg, extra = {}) {
  const lvl = LOG_LEVELS[level] || 20
  if (lvl < (LOG_LEVELS[LOG_LEVEL] || 20)) return
  const err = extra && extra.error ? String(extra.error) : ''
  const msgOut = err ? `${msg} | ${err}` : msg
  const payload = { ts: Date.now(), level, service: SERVICE_NAME, msg: msgOut, ...extra }
  try { process.stdout.write(JSON.stringify(payload) + '\n') } catch {}
}

process.on('uncaughtException', e => { try { log('error', 'uncaught', { error: String(e && (e.stack || e.message) || e) }) } catch {} })
process.on('unhandledRejection', e => { try { log('error', 'unhandled', { error: String(e && (e.stack || e.message) || e) }) } catch {} })

const state = {
  sessions: new Map(),
  users: new Map(),
  invites: new Map(),
  meetings: new Map(),
  orders: new Map(),
  consumptionInvites: new Map(),
  waiterCalls: new Map(),
  djRequests: new Map(),
  blocks: new Set(),
  reports: [],
  sseUsers: new Map(),
  sseStaff: new Map(),
  // Meta para SSE: trackear actividad para cerrar conexiones inactivas
  sseUserMeta: new Map(),   // key: res, value: { startedAt, lastWrite }
  sseStaffMeta: new Map(),  // key: res, value: { startedAt, lastWrite }
  behaviorScore: new Map(),
  scoreLastDecay: 0,
  rate: {
    invitesByUserHour: new Map(),
    lastInvitePair: new Map(),
    passesByPair: new Map(),
    restrictedUsers: new Map(),
    consumptionByUserHour: new Map(),
    thanksByUserHour: new Map(),
    tableChangesByUserHour: new Map(),
  },
}

let lastSSEPing = 0

const dataDir = path.join(__dirname, 'data')
if (!fs.existsSync(dataDir)) { try { fs.mkdirSync(dataDir) } catch {} }
const GLOBAL_STAFF_PIN = String(process.env.STAFF_PIN || '')
const ALLOW_GLOBAL_STAFF_PIN = String(process.env.ALLOW_GLOBAL_STAFF_PIN || 'false') === 'true'
const ADMIN_SECRET = String(process.env.ADMIN_SECRET || '')
const ADMIN_STAFF_SECRET = String(process.env.ADMIN_STAFF_SECRET || '2207')
const RESEND_API_KEY = String(process.env.RESEND_API_KEY || '')
const SENDGRID_API_KEY = String(process.env.SENDGRID_API_KEY || '')
const EMAIL_FROM = String(process.env.EMAIL_FROM || '')
const GMAIL_SMTP_USER = String(process.env.GMAIL_SMTP_USER || '')
const GMAIL_SMTP_PASS = String(process.env.GMAIL_SMTP_PASS || '')
const GMAIL_SMTP_HOST = String(process.env.GMAIL_SMTP_HOST || 'smtp.gmail.com')
const GMAIL_SMTP_PORT = Number(process.env.GMAIL_SMTP_PORT || 465)
const GMAIL_SMTP_SECURE = String(process.env.GMAIL_SMTP_SECURE || 'true') === 'true'
const SESSION_TTL_MS = 12 * 60 * 60 * 1000
const CLOUDINARY_CLOUD_NAME = String(process.env.CLOUDINARY_CLOUD_NAME || '')
const CLOUDINARY_API_KEY = String(process.env.CLOUDINARY_API_KEY || '')
const CLOUDINARY_API_SECRET = String(process.env.CLOUDINARY_API_SECRET || '')
const CLOUDINARY_FOLDER = String(process.env.CLOUDINARY_FOLDER || 'discos')
const REDIS_URL = String(process.env.REDIS_URL || process.env.RAILWAY_REDIS_URL || '')

const DB_REQUIRED = String(process.env.DB_REQUIRED || 'false') === 'true'
const DB_FAILFAST = String(process.env.DB_FAILFAST || 'false') === 'true'
let db = null
let dbReady = false
let dbConn = ''
let lastDbError = ''
let dbTriedNoSsl = false
let dbSslMode = 'auto'
let cloudinary = null
try {
  if (CLOUDINARY_CLOUD_NAME && CLOUDINARY_API_KEY && CLOUDINARY_API_SECRET) {
    const { v2 } = require('cloudinary')
    cloudinary = v2
    cloudinary.config({
      cloud_name: CLOUDINARY_CLOUD_NAME,
      api_key: CLOUDINARY_API_KEY,
      api_secret: CLOUDINARY_API_SECRET
    })
  }
} catch (e) {
  log('error', 'cloudinary_init_failed', { error: String(e && (e.stack || e.message) || e) })
}
let redisClient = null
let redisReady = false
let redisInitError = ''
async function initRedis() {
  if (!REDIS_URL) return false
  if (redisReady && redisClient) return true
  try {
    const { createClient } = require('redis')
    redisClient = createClient({ url: REDIS_URL })
    redisClient.on('error', (err) => {
      redisInitError = String(err && (err.stack || err.message) || err)
      log('error', 'redis_error', { error: redisInitError })
    })
    await redisClient.connect()
    redisReady = true
    redisInitError = ''
    return true
  } catch (e) {
    redisInitError = String(e && (e.stack || e.message) || e)
    log('error', 'redis_init_failed', { error: redisInitError })
    return false
  }
}
async function bootstrapDB() {
  try {
    await initDB()
    await ensureGlobalCatalogSeed()
  } catch (e) {
    const msg = String(e && (e.stack || e.message) || e)
    const isSslRelated = /ECONNRESET|SSL|ssl|self signed|no pg_hba/i.test(msg)
    if (!dbTriedNoSsl && dbSslMode !== 'off' && isSslRelated) {
      dbTriedNoSsl = true
      try { await db.end() } catch {}
      db = new Pool({ connectionString: dbConn, ssl: false })
      try {
        await initDB()
        await ensureGlobalCatalogSeed()
        lastDbError = ''
        log('warn', 'db_bootstrap_retry_no_ssl')
        return
      } catch (e2) {
        lastDbError = String(e2 && (e2.stack || e2.message) || e2)
        log('error', 'db_bootstrap_failed', { error: lastDbError })
        if (DB_FAILFAST) process.exit(1)
        return
      }
    }
    lastDbError = msg
    log('error', 'db_bootstrap_failed', { error: lastDbError })
    if (DB_FAILFAST) process.exit(1)
  }
}
try {
  const { Pool } = require('pg')
  const candidates = [
    String(process.env.DATABASE_URL || ''),
    String(process.env.DATABASE_PUBLIC_URL || ''),
    String(process.env.RAILWAY_DATABASE_URL || ''),
    String(process.env.POSTGRES_URL || ''),
    String(process.env.POSTGRESQL_URL || ''),
    String(process.env.PGURL || ''),
    String(process.env.PG_URL || ''),
    String(process.env.URL_DE_BASE_DE_DATOS || ''),
    String(process.env.POSTGRES_URL_DE_BASE_DE_DATOS || '')
  ].filter(v => !!v)
  dbConn = candidates[0] || ''
  if (!dbConn && DB_REQUIRED) {
    lastDbError = 'db_missing'
    log('error', 'db_missing')
    if (DB_FAILFAST) process.exit(1)
  }
  if (dbConn) {
    const sslEnv = String(process.env.DB_SSL || '').toLowerCase()
    const wantsNoSsl = sslEnv === 'false' || /sslmode=disable/i.test(dbConn) || /localhost|127\.0\.0\.1/i.test(dbConn)
    const ssl = wantsNoSsl ? false : { require: true, rejectUnauthorized: false }
    dbSslMode = wantsNoSsl ? 'off' : (sslEnv === 'true' ? 'on' : 'auto')
    dbTriedNoSsl = wantsNoSsl
    db = new Pool({ connectionString: dbConn, ssl })
    ;(async () => {
      await bootstrapDB()
    })()
  }
} catch (e) {
  lastDbError = String(e && (e.stack || e.message) || e)
  log('error', 'db_init_failed', { error: lastDbError })
  if (DB_FAILFAST) process.exit(1)
}
async function initDB() {
  if (!db || dbReady) return !!db
  await db.query('CREATE TABLE IF NOT EXISTS venues (venue_id TEXT PRIMARY KEY, name TEXT NOT NULL, credits INTEGER NOT NULL, active BOOLEAN NOT NULL, pin TEXT, email TEXT)')
  await db.query('ALTER TABLE IF EXISTS venues ADD COLUMN IF NOT EXISTS pin TEXT')
  await db.query('ALTER TABLE IF EXISTS venues ADD COLUMN IF NOT EXISTS email TEXT')
  await db.query('CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, role TEXT NOT NULL, alias TEXT, selfie TEXT, selfie_approved BOOLEAN, available BOOLEAN, prefs_json TEXT, zone TEXT, muted BOOLEAN, receive_mode TEXT, table_id TEXT, visibility TEXT, paused_until BIGINT, silenced BOOLEAN)')
  await db.query('CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, emitter_id TEXT NOT NULL, receiver_id TEXT NOT NULL, product TEXT NOT NULL, quantity INTEGER NOT NULL, price INTEGER NOT NULL, total INTEGER NOT NULL, status TEXT NOT NULL, created_at BIGINT NOT NULL, expires_at BIGINT NOT NULL, emitter_table TEXT, receiver_table TEXT, mesa_entrega TEXT, is_invitation BOOLEAN)')
  await db.query('CREATE TABLE IF NOT EXISTS table_closures (session_id TEXT NOT NULL, table_id TEXT NOT NULL, closed BOOLEAN NOT NULL, PRIMARY KEY (session_id, table_id))')
  await db.query('CREATE TABLE IF NOT EXISTS waiter_calls (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, user_id TEXT NOT NULL, table_id TEXT, reason TEXT, status TEXT, ts BIGINT NOT NULL)')
  await db.query('CREATE TABLE IF NOT EXISTS catalog_items (session_id TEXT NOT NULL, name TEXT NOT NULL, price INTEGER NOT NULL, category TEXT, subcategory TEXT, description TEXT, PRIMARY KEY (session_id, name))')
  await db.query('ALTER TABLE IF EXISTS catalog_items ADD COLUMN IF NOT EXISTS category TEXT')
  await db.query('ALTER TABLE IF EXISTS catalog_items ADD COLUMN IF NOT EXISTS subcategory TEXT')
  await db.query('ALTER TABLE IF EXISTS catalog_items ADD COLUMN IF NOT EXISTS description TEXT')
  await db.query('CREATE TABLE IF NOT EXISTS venue_catalog_items (venue_id TEXT NOT NULL, mode TEXT NOT NULL, name TEXT NOT NULL, price INTEGER NOT NULL, category TEXT, subcategory TEXT, description TEXT, PRIMARY KEY (venue_id, mode, name))')
  await db.query('ALTER TABLE IF EXISTS venue_catalog_items ADD COLUMN IF NOT EXISTS description TEXT')
  await db.query('CREATE TABLE IF NOT EXISTS venue_catalog_meta (venue_id TEXT NOT NULL, mode TEXT NOT NULL, initialized BOOLEAN NOT NULL, PRIMARY KEY (venue_id, mode))')
  await db.query('CREATE TABLE IF NOT EXISTS idempotency_keys (key TEXT PRIMARY KEY, route TEXT NOT NULL, status INTEGER NOT NULL, response_json TEXT NOT NULL, created_at BIGINT NOT NULL)')
  await db.query('CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, session_id TEXT, entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT, ts BIGINT NOT NULL)')
  lastDbError = ''
  dbReady = true
  return true
}
async function sendEmail(to, subject, text) {
  try {
    if (GMAIL_SMTP_USER && GMAIL_SMTP_PASS) {
      const nodemailer = require('nodemailer')
      const transporter = nodemailer.createTransport({
        host: GMAIL_SMTP_HOST,
        port: GMAIL_SMTP_PORT,
        secure: GMAIL_SMTP_SECURE,
        auth: { user: GMAIL_SMTP_USER, pass: GMAIL_SMTP_PASS }
      })
      const from = EMAIL_FROM || GMAIL_SMTP_USER
      const info = await transporter.sendMail({ from, to, subject, text })
      const resp = String(info && info.response || '')
      const status = /(\d{3})/.test(resp) ? Number(RegExp.$1) : 250
      return { ok: true, provider: 'smtp', status }
    }
    return { ok: false, provider: 'none', status: 0, error: 'no_provider_smtp' }
  } catch (e) {
    const msg = String(e && e.message || e)
    let status = 0
    if (/535/i.test(msg)) status = 535
    else if (/530|534|535|550|553|554/i.test(msg)) {
      const m = msg.match(/(\d{3})/)
      status = m ? Number(m[1]) : 0
    }
    return { ok: false, provider: 'smtp', status, error: msg }
  }
}
async function isDBConnected() {
  if (!db) return false
  try { await db.query('SELECT 1'); lastDbError = ''; return true } catch (e) { lastDbError = String(e && (e.stack || e.message) || e); return false }
}
function requireDB() {
  if (!db) throw new Error('db_required')
}
async function listPublicTables() {
  requireDB()
  await initDB()
  const r = await db.query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
  return r.rows.map(w => w.table_name)
}
async function readVenues() {
  const filePath = path.join(dataDir, 'venues.json')
  const readFile = () => {
    try {
      const raw = fs.readFileSync(filePath, 'utf8')
      const data = JSON.parse(raw || '{}')
      return (data && typeof data === 'object') ? data : {}
    } catch {}
    return {}
  }
  if (!db) return readFile()
  try {
    await initDB()
    const r = await db.query('SELECT venue_id, name, credits, active, pin, email FROM venues')
    const obj = {}
    for (const row of r.rows) obj[row.venue_id] = { name: row.name, credits: Number(row.credits || 0), active: !!row.active, pin: row.pin || '', email: row.email || '' }
    return obj
  } catch {
    return readFile()
  }
}
async function writeVenues(obj) {
  const filePath = path.join(dataDir, 'venues.json')
  const writeFile = () => {
    try { fs.writeFileSync(filePath, JSON.stringify(obj || {})) } catch {}
  }
  if (!db) { writeFile(); return }
  try {
    await initDB()
    const entries = Object.entries(obj)
    for (const [id, v] of entries) {
      await db.query(
        'INSERT INTO venues (venue_id, name, credits, active, pin, email) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (venue_id) DO UPDATE SET name=EXCLUDED.name, credits=EXCLUDED.credits, active=EXCLUDED.active, pin=EXCLUDED.pin, email=EXCLUDED.email',
        [String(id), String(v.name || id), Number(v.credits || 0), v.active !== false, String(v.pin || ''), String(v.email || '')]
      )
    }
  } catch {
    writeFile()
  }
}
async function dbReadGlobalCatalog(mode) {
  requireDB()
  await initDB()
  const sessionId = catalogSessionIdForMode(mode)
  let rows = []
  try {
    const r = await db.query('SELECT name, price, category, subcategory, description FROM catalog_items WHERE session_id=$1 ORDER BY name', [sessionId])
    rows = r.rows || []
  } catch {}
  if (!rows.length && normalizeMode(mode) === 'restaurant') {
    try {
      const legacy = await db.query('SELECT name, price, category, subcategory, description FROM catalog_items WHERE session_id=$1 ORDER BY name', ['global'])
      rows = legacy.rows || []
    } catch {}
  }
  return rows.map(w => ({ name: w.name, price: Number(w.price || 0), category: String(w.category || 'otros'), subcategory: String(w.subcategory || ''), description: String(w.description || '') }))
}
async function dbWriteGlobalCatalog(mode, items) {
  requireDB()
  await initDB()
  const sessionId = catalogSessionIdForMode(mode)
  await db.query('DELETE FROM catalog_items WHERE session_id=$1', [sessionId])
  for (const it of Array.isArray(items) ? items : []) {
    await db.query('INSERT INTO catalog_items (session_id, name, price, category, subcategory, description) VALUES ($1,$2,$3,$4,$5,$6)', [sessionId, String(it.name || ''), Number(it.price || 0), String(it.category || 'otros'), String(it.subcategory || ''), String(it.description || '')])
  }
}
async function dbFillGlobalRestaurantDescriptions() {
  requireDB()
  await initDB()
  const fileItems = readGlobalCatalog('restaurant')
  if (!Array.isArray(fileItems) || !fileItems.length) return
  const map = new Map()
  for (const it of fileItems) {
    const name = String(it.name || '').trim()
    const desc = String(it.description || '').trim()
    if (name && desc) map.set(name, desc)
  }
  if (!map.size) return
  const sessionIds = [catalogSessionIdForMode('restaurant'), 'global']
  for (const sessionId of sessionIds) {
    for (const [name, desc] of map.entries()) {
      await db.query(
        "UPDATE catalog_items SET description=$1 WHERE session_id=$2 AND name=$3 AND (description IS NULL OR description='')",
        [desc, sessionId, name]
      )
    }
  }
}
async function ensureGlobalCatalogSeed() {
  requireDB()
  await initDB()
  try {
    const disco = await dbReadGlobalCatalog('disco')
    if (!disco.length) await dbWriteGlobalCatalog('disco', readGlobalCatalog('disco'))
    const rest = await dbReadGlobalCatalog('restaurant')
    if (!rest.length) await dbWriteGlobalCatalog('restaurant', readGlobalCatalog('restaurant'))
    else await dbFillGlobalRestaurantDescriptions()
  } catch {}
}
async function dbReadSessionCatalog(sessionId) {
  requireDB()
  await initDB()
  const r = await db.query('SELECT name, price, category, subcategory, description FROM catalog_items WHERE session_id=$1 ORDER BY name', [String(sessionId)])
  return r.rows.map(w => ({ name: w.name, price: Number(w.price || 0), category: String(w.category || 'otros'), subcategory: String(w.subcategory || ''), description: String(w.description || '') }))
}
async function dbWriteSessionCatalog(sessionId, items) {
  requireDB()
  await initDB()
  await db.query('DELETE FROM catalog_items WHERE session_id=$1', [String(sessionId)])
  for (const it of Array.isArray(items) ? items : []) {
    await db.query('INSERT INTO catalog_items (session_id, name, price, category, subcategory, description) VALUES ($1,$2,$3,$4,$5,$6)', [String(sessionId), String(it.name || ''), Number(it.price || 0), String(it.category || 'otros'), String(it.subcategory || ''), String(it.description || '')])
  }
}
async function dbReadVenueCatalog(venueId, mode) {
  requireDB()
  await initDB()
  const r = await db.query('SELECT name, price, category, subcategory, description FROM venue_catalog_items WHERE venue_id=$1 AND mode=$2 ORDER BY name', [String(venueId || 'default'), normalizeMode(mode)])
  return r.rows.map(w => ({ name: w.name, price: Number(w.price || 0), category: String(w.category || 'otros'), subcategory: String(w.subcategory || ''), description: String(w.description || '') }))
}
async function dbWriteVenueCatalog(venueId, mode, items) {
  requireDB()
  await initDB()
  const v = String(venueId || 'default')
  const m = normalizeMode(mode)
  await db.query('DELETE FROM venue_catalog_items WHERE venue_id=$1 AND mode=$2', [v, m])
  for (const it of Array.isArray(items) ? items : []) {
    await db.query('INSERT INTO venue_catalog_items (venue_id, mode, name, price, category, subcategory, description) VALUES ($1,$2,$3,$4,$5,$6,$7)', [v, m, String(it.name || ''), Number(it.price || 0), String(it.category || 'otros'), String(it.subcategory || ''), String(it.description || '')])
  }
}
async function dbDeleteVenueCatalog(venueId, mode) {
  requireDB()
  await initDB()
  await db.query('DELETE FROM venue_catalog_items WHERE venue_id=$1 AND mode=$2', [String(venueId || 'default'), normalizeMode(mode)])
}
async function dbIsVenueCatalogInitialized(venueId, mode) {
  requireDB()
  await initDB()
  const r = await db.query('SELECT initialized FROM venue_catalog_meta WHERE venue_id=$1 AND mode=$2', [String(venueId || 'default'), normalizeMode(mode)])
  if (!r.rows.length) return false
  return !!r.rows[0].initialized
}
async function dbSetVenueCatalogInitialized(venueId, mode, initialized) {
  requireDB()
  await initDB()
  const v = String(venueId || 'default')
  const m = normalizeMode(mode)
  if (!initialized) {
    await db.query('DELETE FROM venue_catalog_meta WHERE venue_id=$1 AND mode=$2', [v, m])
    return
  }
  await db.query('INSERT INTO venue_catalog_meta (venue_id, mode, initialized) VALUES ($1,$2,$3) ON CONFLICT (venue_id, mode) DO UPDATE SET initialized=EXCLUDED.initialized', [v, m, true])
}
async function dbUpsertUser(u) {
  requireDB()
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
  requireDB()
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
  requireDB()
  await initDB()
  const r = await db.query('SELECT id, alias, selfie_approved, muted FROM users WHERE session_id=$1 AND role=$2', [String(sessionId), 'user'])
  return r.rows.map(w => ({ id: w.id, alias: w.alias || '', selfieApproved: !!w.selfie_approved, muted: !!w.muted }))
}
async function dbInsertOrder(o, client = null) {
  requireDB()
  if (!client) await initDB()
  const vals = [
    String(o.id), String(o.sessionId), String(o.emitterId), String(o.receiverId),
    String(o.product), Number(o.quantity || 1), Number(o.price || 0), Number(o.total || 0),
    String(o.status), Number(o.createdAt || 0), Number(o.expiresAt || 0),
    String(o.emitterTable || ''), String(o.receiverTable || ''), String(o.mesaEntrega || ''), !!o.isInvitation
  ]
  const runner = client || db
  await runner.query('INSERT INTO orders (id, session_id, emitter_id, receiver_id, product, quantity, price, total, status, created_at, expires_at, emitter_table, receiver_table, mesa_entrega, is_invitation) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status, quantity=EXCLUDED.quantity, price=EXCLUDED.price, total=EXCLUDED.total, emitter_table=EXCLUDED.emitter_table, receiver_table=EXCLUDED.receiver_table, mesa_entrega=EXCLUDED.mesa_entrega, expires_at=EXCLUDED.expires_at', vals)
}
async function dbUpdateOrderStatus(id, status) {
  requireDB()
  await initDB()
  await db.query('UPDATE orders SET status=$2 WHERE id=$1', [String(id), String(status)])
}
async function dbGetOrdersBySession(sessionId, stateFilter) {
  requireDB()
  await initDB()
  if (stateFilter) {
    const r = await db.query('SELECT id, product, quantity, price, total, status, created_at, expires_at, emitter_id, receiver_id, emitter_table, receiver_table, mesa_entrega FROM orders WHERE session_id=$1 AND status=$2', [String(sessionId), String(stateFilter)])
    return r.rows
  }
  const r = await db.query('SELECT id, product, quantity, price, total, status, created_at, expires_at, emitter_id, receiver_id, emitter_table, receiver_table, mesa_entrega FROM orders WHERE session_id=$1', [String(sessionId)])
  return r.rows
}
async function dbGetOrdersByTable(sessionId, tableId) {
  requireDB()
  await initDB()
  const r = await db.query('SELECT id, product, quantity, price, total, status, created_at, expires_at, emitter_id, receiver_id, emitter_table, receiver_table, mesa_entrega FROM orders WHERE session_id=$1 AND (emitter_table=$2 OR receiver_table=$2 OR mesa_entrega=$2)', [String(sessionId), String(tableId)])
  return r.rows
}
async function dbGetOrdersByUser(userId) {
  requireDB()
  await initDB()
  const r = await db.query('SELECT id, product, quantity, price, total, status, created_at, expires_at, emitter_id, receiver_id, emitter_table, receiver_table, mesa_entrega FROM orders WHERE emitter_id=$1 OR receiver_id=$1', [String(userId)])
  return r.rows
}
async function dbSetTableClosed(sessionId, tableId, closed) {
  requireDB()
  await initDB()
  await db.query('INSERT INTO table_closures (session_id, table_id, closed) VALUES ($1,$2,$3) ON CONFLICT (session_id, table_id) DO UPDATE SET closed=EXCLUDED.closed', [String(sessionId), String(tableId), !!closed])
}
async function dbIsTableClosed(sessionId, tableId) {
  requireDB()
  await initDB()
  const r = await db.query('SELECT closed FROM table_closures WHERE session_id=$1 AND table_id=$2', [String(sessionId), String(tableId)])
  if (!r.rows.length) return false
  return !!r.rows[0].closed
}
async function dbInsertWaiterCall(c, client = null) {
  requireDB()
  if (!client) await initDB()
  const vals = [String(c.id), String(c.sessionId), String(c.userId), String(c.tableId || ''), String(c.reason || ''), String(c.status || ''), Number(c.ts || 0)]
  const runner = client || db
  await runner.query('INSERT INTO waiter_calls (id, session_id, user_id, table_id, reason, status, ts) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status', vals)
}
async function dbUpdateWaiterCallStatus(id, status) {
  requireDB()
  await initDB()
  await db.query('UPDATE waiter_calls SET status=$2 WHERE id=$1', [String(id), String(status)])
}
async function dbGetWaiterCalls(sessionId) {
  requireDB()
  await initDB()
  const r = await db.query('SELECT id, session_id, user_id, table_id, reason, status, ts FROM waiter_calls WHERE session_id=$1', [String(sessionId)])
  return r.rows
}
async function withDbTx(fn) {
  requireDB()
  await initDB()
  const client = await db.connect()
  try {
    await client.query('BEGIN')
    const out = await fn(client)
    await client.query('COMMIT')
    return out
  } catch (e) {
    try { await client.query('ROLLBACK') } catch {}
    throw e
  } finally {
    try { client.release() } catch {}
  }
}
async function dbGetIdempotent(key, route) {
  requireDB()
  await initDB()
  const r = await db.query('SELECT status, response_json FROM idempotency_keys WHERE key=$1 AND route=$2', [String(key), String(route)])
  if (!r.rows.length) return null
  const row = r.rows[0]
  let payload = null
  try { payload = JSON.parse(row.response_json || '{}') } catch { payload = {} }
  return { status: Number(row.status || 200), body: payload }
}
async function dbSetIdempotent(key, route, status, body) {
  if (!key) return
  requireDB()
  await initDB()
  const payload = JSON.stringify(body || {})
  await db.query('INSERT INTO idempotency_keys (key, route, status, response_json, created_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (key) DO NOTHING', [String(key), String(route), Number(status || 200), payload, now()])
}
async function dbInsertEvent(evt, client = null) {
  if (!evt) return
  const payload = evt.payload ? JSON.stringify(evt.payload) : ''
  const vals = [String(evt.id || genId('evt')), String(evt.sessionId || ''), String(evt.entityType || ''), String(evt.entityId || ''), String(evt.eventType || ''), payload, Number(evt.ts || now())]
  if (client) {
    await client.query('INSERT INTO events (id, session_id, entity_type, entity_id, event_type, payload_json, ts) VALUES ($1,$2,$3,$4,$5,$6,$7)', vals)
    return
  }
  requireDB()
  await initDB()
  await db.query('INSERT INTO events (id, session_id, entity_type, entity_id, event_type, payload_json, ts) VALUES ($1,$2,$3,$4,$5,$6,$7)', vals)
}

// Autorización admin: requiere ADMIN_SECRET por header o query
function isAdminAuthorized(req, query) {
  const headerSecret = String(req.headers['x-admin-secret'] || '')
  const querySecret = String(query.admin_secret || '')
  if (!ADMIN_SECRET) return false
  return headerSecret === ADMIN_SECRET || querySecret === ADMIN_SECRET
}
function isStaffAuthorized(req, query) {
  const headerSecret = String(req.headers['x-staff-secret'] || '')
  const querySecret = String(query.staff_secret || '')
  if (!ADMIN_STAFF_SECRET) return false
  return headerSecret === ADMIN_STAFF_SECRET || querySecret === ADMIN_STAFF_SECRET
}
function normalizeMode(mode) {
  const v = String(mode || '').toLowerCase()
  if (v === 'restaurant' || v === '1') return 'restaurant'
  return 'disco'
}
function getCategoryLabelsForMode(mode) {
  if (normalizeMode(mode) === 'restaurant') {
    return { cervezas: 'Hamburguesas', botellas: 'Perros calientes', cocteles: 'Pizzas', sodas: 'Bebidas', otros: 'Otros' }
  }
  return { cervezas: 'Cerveza', botellas: 'Botella', cocteles: 'Coctel', sodas: 'Soda', otros: 'Otro' }
}
function formatCatalogItemLabel(mode, item) {
  if (!item) return ''
  const catKey = String(item.category || '').toLowerCase()
  const labels = getCategoryLabelsForMode(mode)
  const cat = labels[catKey] || item.category || ''
  const sub = String(item.subcategory || '').trim()
  if (cat && sub) return `${cat} • ${sub} • ${item.name}`
  if (cat) return `${cat} • ${item.name}`
  return item.name
}
function catalogSessionIdForMode(mode) {
  return normalizeMode(mode) === 'restaurant' ? 'global_restaurant' : 'global_disco'
}
function venueCatalogPath(venueId, mode) {
  const safe = String(venueId || 'default').replace(/[^a-zA-Z0-9_-]/g, '_')
  return path.join(dataDir, `catalog_${safe}_${normalizeMode(mode)}.json`)
}

const defaultCatalog = [
  // Cervezas
  { name: 'Cerveza Lager', price: 10000, category: 'cervezas', subcategory: '' },
  { name: 'Cerveza IPA', price: 12000, category: 'cervezas', subcategory: '' },
  { name: 'Cerveza Stout', price: 12000, category: 'cervezas', subcategory: '' },
  { name: 'Cerveza Pilsner', price: 10000, category: 'cervezas', subcategory: '' },
  { name: 'Cerveza Wheat', price: 11000, category: 'cervezas', subcategory: '' },
  { name: 'Dorada', price: 11000, category: 'cervezas', subcategory: 'Club Colombia' },
  { name: 'Roja', price: 11000, category: 'cervezas', subcategory: 'Club Colombia' },
  { name: 'Negra', price: 11000, category: 'cervezas', subcategory: 'Club Colombia' },
  // Botellas
  { name: 'Botella de Ron', price: 120000, category: 'botellas', subcategory: '' },
  { name: 'Botella de Aguardiente', price: 100000, category: 'botellas', subcategory: '' },
  { name: 'Botella de Whisky', price: 220000, category: 'botellas', subcategory: '' },
  { name: 'Botella de Tequila', price: 180000, category: 'botellas', subcategory: '' },
  { name: 'Botella de Vodka', price: 160000, category: 'botellas', subcategory: '' },
  { name: 'Botella de Gin', price: 160000, category: 'botellas', subcategory: '' },
  // Cocteles
  { name: 'Mojito', price: 20000, category: 'cocteles', subcategory: '' },
  { name: 'Gin Tonic', price: 18000, category: 'cocteles', subcategory: '' },
  { name: 'Margarita', price: 22000, category: 'cocteles', subcategory: '' },
  { name: 'Piña Colada', price: 22000, category: 'cocteles', subcategory: '' },
  { name: 'Cuba Libre', price: 18000, category: 'cocteles', subcategory: '' },
  { name: 'Negroni', price: 24000, category: 'cocteles', subcategory: '' },
  // Sodas y sin alcohol
  { name: 'Agua', price: 5000, category: 'sodas', subcategory: '' },
  { name: 'Agua con gas', price: 6000, category: 'sodas', subcategory: '' },
  { name: 'Soda', price: 6000, category: 'sodas', subcategory: '' },
  { name: 'Tónica', price: 7000, category: 'sodas', subcategory: '' },
  { name: 'Coca Cola', price: 7000, category: 'sodas', subcategory: '' },
  { name: 'Sprite', price: 7000, category: 'sodas', subcategory: '' },
  { name: 'Jugo natural', price: 12000, category: 'sodas', subcategory: '' },
]
const allowedCategories = ['cervezas','botellas','cocteles','sodas','otros']
const allowedGenders = ['m','f','o','na']
function sanitizeItem(it) {
  const name = String(it.name || '').slice(0, 60)
  const price = Number(it.price || 0)
  const rawCat = String(it.category || '').toLowerCase().slice(0, 24)
  const category = allowedCategories.includes(rawCat) ? rawCat : 'otros'
  const subcategory = String(it.subcategory || '').slice(0, 60)
  const description = String(it.description || '').slice(0, 240)
  return { name, price, category, subcategory, description }
}
function readGlobalCatalog(mode) {
  const m = normalizeMode(mode)
  try {
    const p = path.join(dataDir, 'catalog.json')
    if (fs.existsSync(p)) {
      const raw = fs.readFileSync(p, 'utf-8')
      const parsed = JSON.parse(raw || '[]')
      if (Array.isArray(parsed)) {
        if (m === 'restaurant' && parsed.length) return parsed.map(sanitizeItem)
      } else if (parsed && typeof parsed === 'object') {
        const arr = m === 'restaurant' ? parsed.restaurant : parsed.disco
        if (Array.isArray(arr) && arr.length) return arr.map(sanitizeItem)
      }
    }
  } catch {}
  return defaultCatalog
}
function writeGlobalCatalog(mode, items) {
  try {
    const clean = Array.isArray(items) ? items.map(sanitizeItem) : []
    const p = path.join(dataDir, 'catalog.json')
    if (fs.existsSync(p)) {
      const raw = fs.readFileSync(p, 'utf-8')
      const parsed = JSON.parse(raw || '[]')
      if (Array.isArray(parsed)) {
        if (normalizeMode(mode) === 'restaurant') {
          fs.writeFileSync(p, JSON.stringify(clean))
          return
        }
        fs.writeFileSync(p, JSON.stringify({ restaurant: parsed, disco: clean }))
        return
      }
      if (parsed && typeof parsed === 'object') {
        const out = { restaurant: Array.isArray(parsed.restaurant) ? parsed.restaurant : [], disco: Array.isArray(parsed.disco) ? parsed.disco : [] }
        if (normalizeMode(mode) === 'restaurant') out.restaurant = clean
        else out.disco = clean
        fs.writeFileSync(p, JSON.stringify(out))
        return
      }
    }
    if (normalizeMode(mode) === 'restaurant') fs.writeFileSync(p, JSON.stringify(clean))
    else fs.writeFileSync(p, JSON.stringify({ restaurant: [], disco: clean }))
  } catch {}
}
function readVenueCatalogFile(venueId, mode) {
  try {
    const p = venueCatalogPath(venueId, mode)
    if (!fs.existsSync(p)) return []
    const raw = fs.readFileSync(p, 'utf-8')
    const parsed = JSON.parse(raw || '[]')
    if (Array.isArray(parsed) && parsed.length) return parsed.map(sanitizeItem)
  } catch {}
  return []
}
function writeVenueCatalogFile(venueId, mode, items) {
  try {
    const clean = Array.isArray(items) ? items.map(sanitizeItem) : []
    const p = venueCatalogPath(venueId, mode)
    fs.writeFileSync(p, JSON.stringify(clean))
  } catch {}
}
function deleteVenueCatalogFile(venueId, mode) {
  try {
    const p = venueCatalogPath(venueId, mode)
    if (fs.existsSync(p)) fs.unlinkSync(p)
  } catch {}
}
async function getGlobalCatalogForMode(mode) {
  if (db) {
    const items = await dbReadGlobalCatalog(mode)
    if (items && items.length) return items
  }
  return readGlobalCatalog(mode)
}
async function getVenueCatalogState(venueId, mode) {
  if (!venueId) return { items: [], initialized: false }
  if (db) {
    try {
      const initialized = await dbIsVenueCatalogInitialized(venueId, mode)
      if (!initialized) return { items: [], initialized: false }
      const items = await dbReadVenueCatalog(venueId, mode)
      return { items: Array.isArray(items) ? items : [], initialized: true }
    } catch {}
  }
  try {
    const p = venueCatalogPath(venueId, mode)
    if (!fs.existsSync(p)) return { items: [], initialized: false }
    const items = readVenueCatalogFile(venueId, mode)
    return { items: Array.isArray(items) ? items : [], initialized: true }
  } catch {}
  return { items: [], initialized: false }
}
function getSessionMode(s) {
  return s && s.mode ? normalizeMode(s.mode) : 'disco'
}
async function getCatalogBaseForSession(s) {
  const mode = getSessionMode(s)
  const venueId = s && s.venueId ? s.venueId : ''
  const venueState = await getVenueCatalogState(venueId, mode)
  if (venueState.initialized) return venueState.items
  if (s && Array.isArray(s.catalog) && s.catalog.length) return s.catalog
  return await getGlobalCatalogForMode(mode)
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
function getIdempotencyKey(req) {
  return String(req.headers['x-idempotency-key'] || '').trim()
}
function readRequestId(req) {
  return String(req.headers['x-request-id'] || '').trim()
}
function asString(v) {
  return String(v == null ? '' : v).trim()
}
function reqString(v, min = 1, max = 200) {
  const s = asString(v)
  if (!s || s.length < min) return ''
  if (s.length > max) return s.slice(0, max)
  return s
}
function reqInt(v, min = 0, max = 1000000000) {
  const n = Number(v)
  if (!Number.isFinite(n)) return min
  if (n < min) return min
  if (n > max) return max
  return Math.floor(n)
}
function reqBool(v) {
  return !!v
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
function ensureSessionExpiry(s) {
  if (!s) return
  if (!s.startedAt) s.startedAt = now()
  if (!s.expiresAt) s.expiresAt = Number(s.startedAt) + SESSION_TTL_MS
}
function isSessionExpired(s) {
  if (!s) return true
  ensureSessionExpiry(s)
  return Number(s.expiresAt || 0) <= now()
}
function sessionRedisKey(sessionId) { return `discos:session:${sessionId}` }
function sessionIndexRedisKey(venueId, mode) { return `discos:session_index:${venueId}:${mode}` }
function clearSessionData(sessionId) {
  for (const [uid, u] of state.users.entries()) if (u.sessionId === sessionId) state.users.delete(uid)
  for (const [k, v] of state.invites.entries()) if (v.sessionId === sessionId) state.invites.delete(k)
  for (const [k, v] of state.meetings.entries()) if (v.sessionId === sessionId) state.meetings.delete(k)
  for (const [k, v] of state.orders.entries()) if (v.sessionId === sessionId) state.orders.delete(k)
  for (const [k, v] of state.consumptionInvites.entries()) if (v.sessionId === sessionId) state.consumptionInvites.delete(k)
  for (const [k, v] of state.waiterCalls.entries()) if (v.sessionId === sessionId) state.waiterCalls.delete(k)
  for (const [k, v] of state.djRequests.entries()) if (v.sessionId === sessionId) state.djRequests.delete(k)
  state.reports = state.reports.filter(r => String(r.sessionId || '') !== String(sessionId))
}
function serializeUser(u) {
  return {
    ...u,
    allowedSenders: Array.from(u.allowedSenders || []),
  }
}
function serializeSessionState(sessionId) {
  const s = state.sessions.get(sessionId)
  if (!s) return null
  ensureSessionExpiry(s)
  const users = []
  for (const u of state.users.values()) if (u.sessionId === sessionId) users.push(serializeUser(u))
  const invites = []
  for (const v of state.invites.values()) if (v.sessionId === sessionId) invites.push(v)
  const meetings = []
  for (const v of state.meetings.values()) if (v.sessionId === sessionId) meetings.push(v)
  const orders = []
  for (const v of state.orders.values()) if (v.sessionId === sessionId) orders.push(v)
  const consumptionInvites = []
  for (const v of state.consumptionInvites.values()) if (v.sessionId === sessionId) consumptionInvites.push(v)
  const waiterCalls = []
  for (const v of state.waiterCalls.values()) if (v.sessionId === sessionId) waiterCalls.push(v)
  const djRequests = []
  for (const v of state.djRequests.values()) if (v.sessionId === sessionId) djRequests.push(v)
  const reports = state.reports.filter(r => String(r.sessionId || '') === String(sessionId))
  return {
    session: { ...s, closedTables: Array.from(s.closedTables || []) },
    users,
    invites,
    meetings,
    orders,
    consumptionInvites,
    waiterCalls,
    djRequests,
    reports,
    blocks: Array.from(state.blocks || [])
  }
}
function hydrateSessionState(payload) {
  if (!payload || !payload.session || !payload.session.id) return false
  const s = payload.session
  s.closedTables = new Set(s.closedTables || [])
  state.sessions.set(s.id, s)
  clearSessionData(s.id)
  if (Array.isArray(payload.users)) {
    for (const u of payload.users) {
      const next = { ...u, allowedSenders: new Set(u.allowedSenders || []) }
      state.users.set(next.id, next)
    }
  }
  if (Array.isArray(payload.invites)) {
    for (const inv of payload.invites) state.invites.set(inv.id, inv)
  }
  if (Array.isArray(payload.meetings)) {
    for (const m of payload.meetings) state.meetings.set(m.id, m)
  }
  if (Array.isArray(payload.orders)) {
    for (const o of payload.orders) state.orders.set(o.id, o)
  }
  if (Array.isArray(payload.consumptionInvites)) {
    for (const c of payload.consumptionInvites) state.consumptionInvites.set(c.id, c)
  }
  if (Array.isArray(payload.waiterCalls)) {
    for (const w of payload.waiterCalls) state.waiterCalls.set(w.id, w)
  }
  if (Array.isArray(payload.djRequests)) {
    for (const d of payload.djRequests) state.djRequests.set(d.id, d)
  }
  if (Array.isArray(payload.reports)) {
    state.reports = state.reports.concat(payload.reports)
  }
  if (Array.isArray(payload.blocks)) {
    state.blocks = new Set(payload.blocks)
  }
  return true
}
async function saveSessionToRedis(sessionId) {
  const ok = await initRedis()
  if (!ok || !redisClient) return false
  const payload = serializeSessionState(sessionId)
  if (!payload) return false
  const ttlMs = Math.max(1000, Number(payload.session.expiresAt || 0) - now())
  const key = sessionRedisKey(sessionId)
  const idxKey = sessionIndexRedisKey(payload.session.venueId || 'default', getSessionMode(payload.session) || 'disco')
  await redisClient.set(key, JSON.stringify(payload), { PX: ttlMs })
  await redisClient.set(idxKey, sessionId, { PX: ttlMs })
  return true
}
async function loadSessionFromRedis(sessionId) {
  const ok = await initRedis()
  if (!ok || !redisClient) return false
  const key = sessionRedisKey(sessionId)
  const raw = await redisClient.get(key)
  if (!raw) return false
  try {
    const payload = JSON.parse(raw)
    return hydrateSessionState(payload)
  } catch {
    return false
  }
}
async function loadSessionFromRedisByIndex(venueId, mode) {
  const ok = await initRedis()
  if (!ok || !redisClient) return false
  const idxKey = sessionIndexRedisKey(venueId || 'default', normalizeMode(mode || ''))
  const sessionId = await redisClient.get(idxKey)
  if (!sessionId) return false
  if (state.sessions.has(sessionId)) return true
  return await loadSessionFromRedis(sessionId)
}
async function purgeSessionFromRedis(sessionId) {
  const ok = await initRedis()
  if (!ok || !redisClient) return false
  const s = state.sessions.get(sessionId)
  const key = sessionRedisKey(sessionId)
  if (s) {
    const idxKey = sessionIndexRedisKey(s.venueId || 'default', getSessionMode(s) || 'disco')
    await redisClient.del(idxKey)
  }
  await redisClient.del(key)
  return true
}
async function uploadSelfieToCloudinary(selfieStr, user) {
  if (!cloudinary) throw new Error('cloudinary_not_configured')
  const folder = CLOUDINARY_FOLDER || 'discos'
  const publicId = `${String(user.sessionId || 'sess')}_${String(user.id || 'user')}_${now()}`
  const res = await cloudinary.uploader.upload(selfieStr, { folder, public_id: publicId, overwrite: true, resource_type: 'image' })
  return { url: res.secure_url || res.url || '', publicId: res.public_id || publicId }
}
async function cleanupSessionSelfies(sessionId) {
  if (!cloudinary) return
  const targets = []
  for (const u of state.users.values()) if (u.sessionId === sessionId && u.selfiePublicId) targets.push(u.selfiePublicId)
  if (!targets.length) return
  for (const pid of targets) {
    try { await cloudinary.uploader.destroy(pid) } catch {}
  }
}

function sendToUser(userId, event, data) {
  const clients = state.sseUsers.get(userId)
  if (!clients) return
  const eid = String(now())
  const payload = `id: ${eid}\n` + `event: ${event}\n` + `data: ${JSON.stringify(data)}\n\n`
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
  const eid = String(now())
  const payload = `id: ${eid}\n` + `event: ${event}\n` + `data: ${JSON.stringify(data)}\n\n`
  for (const res of clients) {
    try { res.write(payload) } catch {}
    const meta = state.sseStaffMeta.get(res) || { startedAt: now(), lastWrite: now() }
    meta.lastWrite = now()
    state.sseStaffMeta.set(res, meta)
  }
}
function sendToAllUsersInSession(sessionId, event, data) {
  const eid = String(now())
  const payload = `id: ${eid}\n` + `event: ${event}\n` + `data: ${JSON.stringify(data)}\n\n`
  for (const [uid, clients] of state.sseUsers.entries()) {
    const u = state.users.get(uid)
    if (!u || u.sessionId !== sessionId) continue
    if (!clients) continue
    for (const res of clients) {
      try { res.write(payload) } catch {}
      const meta = state.sseUserMeta.get(res) || { startedAt: now(), lastWrite: now() }
      meta.lastWrite = now()
      state.sseUserMeta.set(res, meta)
    }
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
  if (last && Number(last.untilTs || 0) > now()) return false
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

function updateBehaviorScore(userId, delta) {
  const cur = Number(state.behaviorScore.get(userId) || 0)
  state.behaviorScore.set(userId, cur + Number(delta || 0))
}

function computeInviteTTL(toUser) {
  try {
    const nowTs = now()
    const recentMeet = Number(toUser && toUser.lastMeetingEndedAt || 0)
    if (recentMeet && (nowTs - recentMeet) < (10 * 60 * 1000)) return 45 * 1000
    const lastActive = Number(toUser && toUser.lastActiveAt || 0)
    if (!lastActive || (nowTs - lastActive) > (2 * 60 * 1000)) return 90 * 1000
    return 60 * 1000
  } catch { return 60 * 1000 }
}
function isBlockedPair(a, b) {
  return state.blocks.has(`${a}:${b}`) || state.blocks.has(`${b}:${a}`)
}

function persistOrders(sessionId) {
  const list = []
  for (const o of state.orders.values()) if (o.sessionId === sessionId) list.push(o)
  for (const o of list) { try { dbUpdateOrderStatus(o.id, o.status) } catch {} }
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
async function endAndArchive(sessionId) {
  archiveSession(sessionId)
  try { await cleanupSessionSelfies(sessionId) } catch {}
  try { await purgeSessionFromRedis(sessionId) } catch {}
  state.sessions.delete(sessionId)
  // Cerrar SSE staff de la sesión
  const staffConns = state.sseStaff.get(sessionId) || []
  for (const res of staffConns) { try { res.end() } catch {} state.sseStaffMeta.delete(res) }
  state.sseStaff.delete(sessionId)
  const affectedUsers = []
  for (const [uid, u] of state.users) {
    if (u.sessionId === sessionId) affectedUsers.push(uid)
  }
  for (const uid of affectedUsers) {
    try { sendToUser(uid, 'session_end', { sessionId }) } catch {}
    const list = state.sseUsers.get(uid) || []
    for (const res of list) { try { res.end() } catch {} state.sseUserMeta.delete(res) }
    state.sseUsers.delete(uid)
    state.users.delete(uid)
  }
  for (const [k, v] of state.invites) if (v.sessionId === sessionId) state.invites.delete(k)
  for (const [k, v] of state.meetings) if (v.sessionId === sessionId) state.meetings.delete(k)
  for (const [k, v] of state.orders) if (v.sessionId === sessionId) state.orders.delete(k)
}
async function closeAllTablesForSession(sessionId) {
  const s = ensureSession(sessionId)
  if (!s) return
  s.closedTables = s.closedTables || new Set()
  const tables = new Set()
  const affected = []
  for (const u of state.users.values()) {
    if (u.sessionId !== sessionId || u.role !== 'user') continue
    const t = String(u.tableId || '').trim()
    if (t) {
      tables.add(t)
      affected.push({ user: u, tableId: t })
      u.tableId = ''
    }
  }
  if (db && tables.size) {
    await withDbTx(async (client) => {
      for (const t of tables) {
        await client.query('SELECT closed FROM table_closures WHERE session_id=$1 AND table_id=$2 FOR UPDATE', [String(s.id), String(t)])
        await client.query('INSERT INTO table_closures (session_id, table_id, closed) VALUES ($1,$2,$3) ON CONFLICT (session_id, table_id) DO UPDATE SET closed=EXCLUDED.closed', [String(s.id), String(t), true])
        await dbInsertEvent({ sessionId: s.id, entityType: 'table', entityId: t, eventType: 'closed', payload: { closed: true }, ts: now() }, client)
      }
    })
  }
  for (const t of tables) {
    s.closedTables.add(t)
    sendToStaff(s.id, 'table_closed', { tableId: t, closed: true })
  }
  for (const entry of affected) {
    try { await dbUpsertUser(entry.user) } catch {}
    sendToUser(entry.user.id, 'table_closed', { tableId: entry.tableId })
  }
}
async function expireOldSessions() {
  for (const s of state.sessions.values()) {
    if (isSessionExpired(s)) {
      sendToStaff(s.id, 'session_expired', { sessionId: s.id, venueId: s.venueId || '', expiredAt: now() })
      await closeAllTablesForSession(s.id)
      await endAndArchive(s.id)
    }
  }
}
function deactivateSession(sessionId) {
  const s = ensureSession(sessionId)
  if (!s) return
  ensureSessionExpiry(s)
  s.active = false
  s.endedAt = now()
  try { cleanupSessionSelfies(sessionId) } catch {}
  try { purgeSessionFromRedis(sessionId) } catch {}
  const staffConns = state.sseStaff.get(sessionId) || []
  for (const res of staffConns) { try { res.end() } catch {} state.sseStaffMeta.delete(res) }
  state.sseStaff.delete(sessionId)
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
  const startedAt = now()
  const rid = readRequestId(req) || genId('req')
  req.requestId = rid
  res.setHeader('X-Request-Id', rid)
  res.on('finish', () => {
    const ms = now() - startedAt
    log('info', 'http', { requestId: rid, method: req.method, path: req.url, status: res.statusCode, ms })
  })
  try {
    const { pathname, query } = url.parse(req.url, true)
    if (req.method === 'OPTIONS') {
      res.writeHead(200, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Admin-Secret,X-Request-Id,X-Idempotency-Key',
      })
      res.end()
      return
    }

    if (pathname.startsWith('/api/')) {
      await expireOldSessions()
      if (pathname === '/api/session/start' && req.method === 'POST') {
      try {
        const body = await parseBody(req)
        const venueId = String(body.venueId || 'default')
        const mode = normalizeMode(body.mode || '')
        try { await loadSessionFromRedisByIndex(venueId, mode) } catch {}
        for (const s of state.sessions.values()) {
          ensureSessionExpiry(s)
          if (!isSessionExpired(s) && s.venueId === venueId && getSessionMode(s) === mode) {
            s.active = true
            try { await saveSessionToRedis(s.id) } catch {}
            json(res, 200, { sessionId: s.id, pin: s.pin, venueId, reused: true, mode: s.mode || mode })
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
        const startedAt = now()
        state.sessions.set(sessionId, { id: sessionId, venueId, venue: body.venue || 'Venue', startedAt, expiresAt: startedAt + SESSION_TTL_MS, active: true, pin, publicBaseUrl: '', closedTables: new Set(), mode })
        try { await saveSessionToRedis(sessionId) } catch {}
        json(res, 200, { sessionId, pin, venueId, mode })
        return
      } catch (e) {
        json(res, 500, { error: 'session_start_failed', message: String(e && e.message ? e.message : e) })
        return
      }
    }
    if (pathname === '/api/session/active' && req.method === 'GET') {
      const venueId = String(query.venueId || '')
      const mode = normalizeMode(query.mode || '')
      try { await loadSessionFromRedisByIndex(venueId, mode) } catch {}
      for (const s of state.sessions.values()) {
        ensureSessionExpiry(s)
        if (isSessionExpired(s)) continue
        if ((!venueId || s.venueId === venueId) && getSessionMode(s) === mode) {
          let venueName = s.venue || ''
          try {
            const venues = await readVenues()
            const v = venues[s.venueId]
            venueName = (v && v.name) ? v.name : (venueName || s.venueId)
          } catch {}
          json(res, 200, { active: true, sessionId: s.id, pin: s.pin, venueId: s.venueId, venueName, mode: s.mode || 'disco' })
          return
        }
      }
      json(res, 200, { active: false, error: 'no_active' })
      return
    }
    if (pathname === '/api/session/info' && req.method === 'GET') {
      const sessionId = String(query.sessionId || '')
      let s = ensureSession(sessionId)
      if (!s && sessionId) { try { await loadSessionFromRedis(sessionId) } catch {} s = ensureSession(sessionId) }
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      json(res, 200, { sessionId: s.id, venueId: s.venueId, mode: s.mode || 'disco' })
      return
    }
    if (pathname === '/api/session/end' && req.method === 'POST') {
      const body = await parseBody(req)
      let s = ensureSession(body.sessionId)
      if (!s && body.sessionId) { try { await loadSessionFromRedis(body.sessionId) } catch {} s = ensureSession(body.sessionId) }
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      const pinStr = String(body.pin || '')
      const adminSecret = String(req.headers['x-admin-secret'] || query.admin_secret || '')
      const okAdmin = ADMIN_SECRET && adminSecret === ADMIN_SECRET
      const okSessionPin = pinStr === String(s.pin)
      const okGlobalPin = (ALLOW_GLOBAL_STAFF_PIN && GLOBAL_STAFF_PIN && pinStr === GLOBAL_STAFF_PIN)
      let okVenuePin = false
      try {
        const venues = await readVenues()
        const v = venues[s.venueId]
        if (v && String(v.pin || '') && pinStr === String(v.pin)) okVenuePin = true
      } catch {}
      if (!okAdmin && !okSessionPin && !okVenuePin && !okGlobalPin) { json(res, 403, { error: 'forbidden' }); return }
      deactivateSession(body.sessionId)
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/session/public-base' && req.method === 'POST') {
      const body = await parseBody(req)
      let s = ensureSession(body.sessionId)
      if (!s && body.sessionId) { try { await loadSessionFromRedis(body.sessionId) } catch {} s = ensureSession(body.sessionId) }
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      const pinStr = String(body.pin || '')
      const adminSecret = String(req.headers['x-admin-secret'] || query.admin_secret || '')
      const okAdmin = ADMIN_SECRET && adminSecret === ADMIN_SECRET
      const okSessionPin = pinStr === String(s.pin)
      let okVenuePin = false
      try {
        const venues = await readVenues()
        const v = venues[s.venueId]
        if (v && String(v.pin || '') && pinStr === String(v.pin)) okVenuePin = true
      } catch {}
      if (!okAdmin && !okSessionPin && !okVenuePin) { json(res, 403, { error: 'forbidden' }); return }
      const urlStr = String(body.publicBaseUrl || '').trim()
      s.publicBaseUrl = urlStr
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/session/mode' && req.method === 'POST') {
      const body = await parseBody(req)
      let s = ensureSession(body.sessionId)
      if (!s && body.sessionId) { try { await loadSessionFromRedis(body.sessionId) } catch {} s = ensureSession(body.sessionId) }
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      const pinStr = String(body.pin || '')
      const adminSecret = String(req.headers['x-admin-secret'] || query.admin_secret || '')
      const okAdmin = ADMIN_SECRET && adminSecret === ADMIN_SECRET
      const okSessionPin = pinStr === String(s.pin)
      let okVenuePin = false
      try {
        const venues = await readVenues()
        const v = venues[s.venueId]
        if (v && String(v.pin || '') && pinStr === String(v.pin)) okVenuePin = true
      } catch {}
      if (!okAdmin && !okSessionPin && !okVenuePin) { json(res, 403, { error: 'forbidden' }); return }
      s.mode = normalizeMode(body.mode || s.mode || '')
      json(res, 200, { ok: true, mode: s.mode })
      return
    }
    if (pathname === '/api/session/public-base' && req.method === 'GET') {
      const sessionId = query.sessionId
      let s = ensureSession(sessionId)
      if (!s && sessionId) { try { await loadSessionFromRedis(sessionId) } catch {} s = ensureSession(sessionId) }
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      json(res, 200, { publicBaseUrl: s.publicBaseUrl || '' })
      return
    }
    if (pathname === '/api/join' && req.method === 'POST') {
      const body = await parseBody(req)
      let s = ensureSession(body.sessionId)
      if (!s && body.sessionId) { try { await loadSessionFromRedis(body.sessionId) } catch {} s = ensureSession(body.sessionId) }
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
      const user = { id, sessionId: body.sessionId, role, alias: role === 'user' ? String(body.alias || '').trim().slice(0, 32) : '', selfie: '', selfieApproved: false, available: false, prefs: { tags: [], gender: '' }, zone: '', muted: false, receiveMode: 'all', allowedSenders: new Set(), tableId: '', visibility: 'visible', pausedUntil: 0, silenced: false, danceState: 'idle', dancePartnerId: '', meetingId: '' }
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
      const sActive = ensureSession(u.sessionId)
      if (!sActive) { json(res, 200, { found: false }); return }
      const partner = (u.dancePartnerId && state.users.get(u.dancePartnerId)) || null
      const partnerAlias = partner ? (partner.alias || partner.id) : ''
      json(res, 200, { found: true, user: { id: u.id, sessionId: u.sessionId, role: u.role, alias: u.alias, selfie: u.selfie || '', selfieApproved: u.selfieApproved, available: u.available, prefs: u.prefs, zone: u.zone, tableId: u.tableId, visibility: u.visibility, danceState: u.danceState || 'idle', partnerAlias } })
      return
    }
    if (pathname === '/api/user/profile' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      u.alias = String(body.alias || '').slice(0, 32)
      const genderRaw = String(body.gender || '').toLowerCase()
      if (!allowedGenders.includes(genderRaw)) { json(res, 400, { error: 'gender_required' }); return }
      u.prefs = u.prefs || {}
      u.prefs.gender = genderRaw
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
      let upload = null
      try {
        upload = await uploadSelfieToCloudinary(selfieStr, u)
      } catch (e) {
        json(res, 500, { error: 'selfie_upload_failed' })
        return
      }
      if (!upload || !upload.url) { json(res, 500, { error: 'selfie_upload_failed' }); return }
      u.selfie = upload.url
      u.selfiePublicId = upload.publicId || ''
      u.selfieApproved = true
      try { await dbUpsertUser(u) } catch {}
      json(res, 200, { ok: true, selfie: u.selfie })
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
    if (pathname === '/api/admin/venues/pin/send' && req.method === 'POST') {
      if (!ADMIN_STAFF_SECRET && !ADMIN_SECRET) { json(res, 403, { error: 'no_staff_secret' }); return }
      if (!isStaffAuthorized(req, query) && !isAdminAuthorized(req, query)) { json(res, 403, { error: 'forbidden' }); return }
      const body = await parseBody(req)
      const venueId = String(body.venueId || '').trim()
      if (!venueId) { json(res, 400, { error: 'bad_input' }); return }
      const venues = await readVenues()
      const v = venues[venueId]
      if (!v) { json(res, 404, { error: 'venue_not_found' }); return }
      const pin = String(v.pin || '')
      const email = String(v.email || '')
      if (!pin) { json(res, 400, { error: 'no_pin' }); return }
      if (!email) { json(res, 400, { error: 'no_email' }); return }
      const to = email
      const subject = `PIN del local ${String(v.name || venueId)}`
      const link = `${process.env.PUBLIC_BASE_URL || ''}/?venueId=${encodeURIComponent(venueId)}`
      const text = `Hola,\n\nEste es el PIN del local "${String(v.name || venueId)}" (ID: ${venueId}).\nPIN del venue: ${pin}\nAcceso: ${link}\n\nSi no solicitaste este envío, por favor contáctanos.\n`
      const r = await sendEmail(to, subject, text)
      if (!r.ok) { json(res, 502, { error: 'email_failed', provider: r.provider, status: r.status, message: r.error }); return }
      json(res, 200, { ok: true, venueId })
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
        if (s.venueId === venueId) await endAndArchive(s.id)
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
      json(res, 200, { connected, error: lastDbError || '' })
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
    if (pathname === '/api/staff/table-change-pin' && req.method === 'POST') {
      const body = await parseBody(req)
      const staff = state.users.get(body.staffId)
      if (!staff || staff.role !== 'staff') { json(res, 403, { error: 'no_staff' }); return }
      const s = ensureSession(body.sessionId)
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      if (staff.sessionId !== s.id) { json(res, 403, { error: 'forbidden' }); return }
      const pin = String(crypto.randomInt(0, 1000000)).padStart(6, '0')
      s.tableChangePin = pin
      s.tableChangePinExpiresAt = now() + (10 * 60 * 1000)
      try { await saveSessionToRedis(s.id) } catch {}
      json(res, 200, { ok: true, pin })
      return
    }
    if (pathname === '/api/user/change-table' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      const s = ensureSession(u.sessionId)
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      const sessionPin = String(s.tableChangePin || '').trim()
      const pin = String(body.pin || '').trim()
      const newTable = String(body.newTable || '').slice(0,32)
      const hasCurrent = String(u.tableId || '').trim()
      const isChange = hasCurrent && newTable && newTable !== hasCurrent
      if (isChange) {
        if (!sessionPin) { json(res, 403, { error: 'pin_required' }); return }
        if (s.tableChangePinExpiresAt && Number(s.tableChangePinExpiresAt) <= now()) { json(res, 403, { error: 'pin_expired' }); return }
        if (!pin || pin !== sessionPin) { json(res, 403, { error: 'bad_pin' }); return }
        const hourKey = u.id
        const bucket = state.rate.tableChangesByUserHour.get(hourKey) || []
        const fresh = bucket.filter(ts => within(60*60*1000, ts))
        if (fresh.length >= 2) { json(res, 429, { error: 'table_changes_limit' }); return }
        state.rate.tableChangesByUserHour.set(hourKey, [...fresh, now()])
      }
      u.tableId = newTable
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
      u.prefs = Object.assign({}, u.prefs || {}, (body.prefs || {}))
      u.zone = body.zone || ''
      try { u.lastActiveAt = now() } catch {}
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
      const activeWindowMs = 20 * 60 * 1000
      for (const u of state.users.values()) {
        if (u.sessionId !== sessionId || u.role !== 'user') continue
        if (only && !u.available) continue
        if (excludeId && u.id === excludeId) continue
        if (u.danceState && u.danceState !== 'idle') continue
        const lastActiveAt = Number(u.lastActiveAt || 0)
        if (lastActiveAt && (now() - lastActiveAt) > activeWindowMs) continue
        if (excludeId) {
          const key = `${excludeId}:${u.id}`
          const passes = Number(state.rate.passesByPair.get(key) || 0)
          if (passes >= 2) continue
        }
        const ageOk = u.prefs && u.prefs.age ? (u.prefs.age >= min && u.prefs.age <= max) : true
        const tagsOk = tagsQ.length ? (Array.isArray(u.prefs.tags) && tagsQ.every(t => u.prefs.tags.includes(t))) : true
        const zoneOk = zoneQ ? (u.zone === zoneQ) : true
        const partner = (u.dancePartnerId && state.users.get(u.dancePartnerId)) || null
        const partnerAlias = partner ? (partner.alias || partner.id) : ''
        arr.push({ id: u.id, alias: u.alias, selfie: u.selfie || '', tags: u.prefs.tags || [], zone: u.zone, available: u.available, tableId: u.tableId, danceState: u.danceState || 'idle', partnerAlias, gender: (u.prefs && u.prefs.gender) ? u.prefs.gender : '' })
      }
      arr.sort((a, b) => {
        const sa = Number(state.behaviorScore.get(a.id) || 0)
        const sb = Number(state.behaviorScore.get(b.id) || 0)
        if (sb !== sa) return sb - sa
        const aa = String(a.alias || a.id), bb = String(b.alias || b.id)
        return aa.localeCompare(bb)
      })
      json(res, 200, { users: arr })
      return
    }
    if (pathname === '/api/users/dance' && req.method === 'GET') {
      const sessionId = query.sessionId
      const waiting = []
      const dancing = []
      for (const u of state.users.values()) {
        if (u.sessionId !== sessionId || u.role !== 'user') continue
        const partner = (u.dancePartnerId && state.users.get(u.dancePartnerId)) || null
        const partnerAlias = partner ? (partner.alias || partner.id) : ''
        const obj = { id: u.id, alias: u.alias, selfie: u.selfie || '', tableId: u.tableId, zone: u.zone, danceState: u.danceState || 'idle', partnerAlias }
        if (u.danceState === 'waiting') waiting.push(obj)
        else if (u.danceState === 'dancing') dancing.push(obj)
      }
      json(res, 200, { waiting, dancing })
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
      // Pausa social eliminada
      if (applyRestrictedIfNeeded(from.id)) { json(res, 429, { error: 'restricted' }); return }
      if (isBlockedPair(from.id, to.id)) { json(res, 403, { error: 'blocked' }); return }
      // Si el receptor está activo para bailar, entregar todas las invitaciones sin restricciones de modo
      if (!to.available) {
        if (to.receiveMode === 'mesas') {
          if (!from.zone || !to.zone || from.zone !== to.zone) { json(res, 403, { error: 'mode_mesas' }); return }
        }
        if (to.receiveMode === 'invitedOnly') {
          const allowed = to.allowedSenders && to.allowedSenders.has(from.id)
          if (!allowed) { json(res, 403, { error: 'mode_invited_only' }); return }
        }
      }
      const msg = body.messageType === 'invitoCancion' ? 'invitoCancion' : 'bailamos'
      if (!rateCanInvite(from.id, to.id)) { json(res, 429, { error: 'rate' }); return }
      const invId = genId('inv')
      const ttlMs = computeInviteTTL(to)
      const inv = { id: invId, sessionId: from.sessionId, fromId: from.id, toId: to.id, msg, status: 'pendiente', createdAt: now(), expiresAt: now() + ttlMs }
      state.invites.set(invId, inv)
      const fromSelfie = from.selfie || ''
      sendToUser(to.id, 'dance_invite', { invite: { id: invId, from: { id: from.id, alias: from.alias, selfie: fromSelfie, tableId: from.tableId || '', zone: from.zone || '', gender: (from.prefs && from.prefs.gender) ? from.prefs.gender : '' } , msg, expiresAt: inv.expiresAt } })
      json(res, 200, { inviteId: invId, expiresAt: inv.expiresAt })
      return
    }
    if (pathname === '/api/invite/ack' && req.method === 'POST') {
      const body = await parseBody(req)
      const inv = state.invites.get(body.inviteId)
      if (!inv) { json(res, 404, { error: 'no_invite' }); return }
      if (String(inv.toId) !== String(body.toId)) { json(res, 403, { error: 'forbidden' }); return }
      inv.seenAt = now()
      const uTo = state.users.get(inv.toId)
      try { if (uTo) uTo.lastActiveAt = now() } catch {}
      try { sendToUser(inv.fromId, 'invite_seen', { inviteId: inv.id, to: { id: inv.toId, alias: uTo ? (uTo.alias || uTo.id) : inv.toId } }) } catch {}
      json(res, 200, { ok: true })
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
        {
          const pairKey = `${inv.fromId}:${inv.toId}`
          const prev = Number(state.rate.passesByPair.get(pairKey) || 0)
          const next = prev + 1
          state.rate.passesByPair.set(pairKey, next)
          let cooldownMs = 0
          if (next === 1) cooldownMs = 5 * 60 * 1000
          else if (next === 2) cooldownMs = 30 * 60 * 1000
          else cooldownMs = 24 * 60 * 60 * 1000
          const emitterScore = Number(state.behaviorScore.get(inv.fromId) || 0)
          const mult = emitterScore < 0 ? Math.min(2, 1 + ((-emitterScore) / 10)) : 1
          state.rate.lastInvitePair.set(pairKey, { ts: now(), blocked: true, untilTs: now() + Math.floor(cooldownMs * mult) })
        }
        updateBehaviorScore(inv.toId, -1)
        sendToUser(inv.fromId, 'invite_result', { inviteId: inv.id, status: 'pasado', note })
        json(res, 200, { ok: true })
        return
      }
      inv.status = 'aceptado'
      updateBehaviorScore(inv.toId, +1)
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
      const fromId = reqString(body.fromId, 1, 80)
      const toId = reqString(body.toId, 1, 80)
      const from = state.users.get(fromId)
      const to = state.users.get(toId)
      if (!from || !to) { json(res, 404, { error: 'no_user' }); return }
      if (isBlockedPair(from.id, to.id)) { json(res, 403, { error: 'blocked' }); return }
      const idemKey = getIdempotencyKey(req)
      if (idemKey) {
        const prev = await dbGetIdempotent(idemKey, pathname)
        if (prev) { json(res, prev.status, prev.body); return }
      }
      const hourKey = from.id
      const bucket = state.rate.consumptionByUserHour.get(hourKey) || []
      const fresh = bucket.filter(ts => within(60 * 60 * 1000, ts))
      if (fresh.length >= 5) { json(res, 429, { error: 'rate_consumo' }); return }
      state.rate.consumptionByUserHour.set(hourKey, [...fresh, now()])
      const reqId = genId('cinv')
      const note = reqString(body.note, 0, 140)
      const qty = reqInt(body.quantity || 1, 1, 999)
      const product = reqString(body.product, 1, 140)
      if (!product) { json(res, 400, { error: 'no_product' }); return }
      const ttlMs = computeInviteTTL(to)
      state.consumptionInvites.set(reqId, { id: reqId, sessionId: from.sessionId, fromId: from.id, toId: to.id, createdAt: now(), expiresAt: now() + ttlMs, seenAt: 0, notSeenNotified: false })
      sendToUser(to.id, 'consumption_invite', { requestId: reqId, from: { id: from.id, alias: from.alias, tableId: from.tableId || '', gender: (from.prefs && from.prefs.gender) ? from.prefs.gender : '' }, product, quantity: qty, note, expiresAt: now() + ttlMs })
      if (idemKey) await dbSetIdempotent(idemKey, pathname, 200, { requestId: reqId })
      json(res, 200, { requestId: reqId })
      return
    }
    if (pathname === '/api/consumption/invite/bulk' && req.method === 'POST') {
      const body = await parseBody(req)
      const fromId = reqString(body.fromId, 1, 80)
      const toId = reqString(body.toId, 1, 80)
      const from = state.users.get(fromId)
      const to = state.users.get(toId)
      if (!from || !to) { json(res, 404, { error: 'no_user' }); return }
      if (isBlockedPair(from.id, to.id)) { json(res, 403, { error: 'blocked' }); return }
      const idemKey = getIdempotencyKey(req)
      if (idemKey) {
        const prev = await dbGetIdempotent(idemKey, pathname)
        if (prev) { json(res, prev.status, prev.body); return }
      }
      const hourKey = from.id
      const bucket = state.rate.consumptionByUserHour.get(hourKey) || []
      const fresh = bucket.filter(ts => within(60 * 60 * 1000, ts))
      if (fresh.length >= 5) { json(res, 429, { error: 'rate_consumo' }); return }
      state.rate.consumptionByUserHour.set(hourKey, [...fresh, now()])
      const items = Array.isArray(body.items) ? body.items.map(it => ({ product: reqString(it.product, 1, 140), quantity: reqInt(it.quantity || 1, 1, 999) })) : []
      const filtered = items.filter(it => it.product)
      if (!filtered.length) { json(res, 400, { error: 'no_items' }); return }
      const reqId = genId('cinv')
      const note = reqString(body.note, 0, 140)
      const ttlMsBulk = computeInviteTTL(to)
      state.consumptionInvites.set(reqId, { id: reqId, sessionId: from.sessionId, fromId: from.id, toId: to.id, createdAt: now(), expiresAt: now() + ttlMsBulk, seenAt: 0, notSeenNotified: false })
      sendToUser(to.id, 'consumption_invite_bulk', { requestId: reqId, from: { id: from.id, alias: from.alias, tableId: from.tableId || '', gender: (from.prefs && from.prefs.gender) ? from.prefs.gender : '' }, items: filtered, note, expiresAt: now() + ttlMsBulk })
      if (idemKey) await dbSetIdempotent(idemKey, pathname, 200, { requestId: reqId })
      json(res, 200, { requestId: reqId })
      return
    }
    if (pathname === '/api/consumption/ack' && req.method === 'POST') {
      const body = await parseBody(req)
      const ci = state.consumptionInvites.get(body.requestId)
      if (!ci) { json(res, 404, { error: 'no_request' }); return }
      if (String(ci.toId) !== String(body.toId)) { json(res, 403, { error: 'forbidden' }); return }
      ci.seenAt = now()
      const uTo = state.users.get(ci.toId)
      try { sendToUser(ci.fromId, 'consumption_seen', { requestId: ci.id, to: { id: ci.toId, alias: uTo ? (uTo.alias || uTo.id) : ci.toId } }) } catch {}
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/consumption/respond' && req.method === 'POST') {
      const body = await parseBody(req)
      const fromId = reqString(body.fromId, 1, 80)
      const toId = reqString(body.toId, 1, 80)
      const from = state.users.get(fromId)
      const to = state.users.get(toId)
      if (body.action !== 'accept') {
        if (from && to) {
          const itemName = reqString(body.product, 1, 140)
          try { sendToUser(from.id, 'consumption_passed', { to: { id: to.id, alias: to.alias }, product: itemName }) } catch {}
        }
        json(res, 200, { ok: true })
        return
      }
      if (!from || !to) { json(res, 404, { error: 'no_user' }); return }
      const s = ensureSession(from.sessionId)
      const itemsBase = await getCatalogBaseForSession(s)
      const itemName = reqString(body.product, 1, 140)
      if (!itemName) { json(res, 400, { error: 'no_product' }); return }
      const found = itemsBase.find(i => i.name === itemName)
      const price = found ? Number(found.price || 0) : 0
      const qty = reqInt(body.quantity || 1, 1, 999)
      const idemKey = getIdempotencyKey(req)
      if (idemKey) {
        const prev = await dbGetIdempotent(idemKey, pathname)
        if (prev) { json(res, prev.status, prev.body); return }
      }
      const orderId = genId('ord')
      const expiresAt = now() + 10 * 60 * 1000
      const createdAt = now()
      const order = { id: orderId, sessionId: from.sessionId, emitterId: from.id, receiverId: to.id, product: itemName, quantity: qty, price, total: price * qty, status: 'pendiente_cobro', createdAt, expiresAt, emitterTable: from.tableId || '', receiverTable: to.tableId || '', mesaEntrega: to.tableId || '', isInvitation: true }
      await withDbTx(async (client) => {
        await dbInsertOrder(order, client)
        await dbInsertEvent({ sessionId: order.sessionId, entityType: 'order', entityId: order.id, eventType: 'created', payload: { product: order.product, quantity: order.quantity, price: order.price, total: order.total, emitterId: order.emitterId, receiverId: order.receiverId }, ts: createdAt }, client)
      })
      state.orders.set(orderId, order)
      sendToStaff(order.sessionId, 'order_new', { order })
      sendToUser(order.emitterId, 'order_update', { order })
      sendToUser(order.receiverId, 'order_update', { order })
      sendToUser(order.emitterId, 'consumption_accepted', { from: { id: to.id, alias: to.alias }, product: itemName, quantity: qty })
      if (idemKey) await dbSetIdempotent(idemKey, pathname, 200, { orderId })
      json(res, 200, { orderId })
      return
    }
    if (pathname === '/api/consumption/respond/bulk' && req.method === 'POST') {
      const body = await parseBody(req)
      const fromId = reqString(body.fromId, 1, 80)
      const toId = reqString(body.toId, 1, 80)
      const from = state.users.get(fromId)
      const to = state.users.get(toId)
      if (body.action !== 'accept') {
        if (from && to) {
          const items = Array.isArray(body.items) ? body.items.map(it => ({ product: reqString(it.product, 1, 140), quantity: reqInt(it.quantity || 1, 1, 999) })) : []
          const filtered = items.filter(it => it.product)
          try { sendToUser(from.id, 'consumption_passed', { to: { id: to.id, alias: to.alias }, items: filtered }) } catch {}
        }
        json(res, 200, { ok: true })
        return
      }
      if (!from || !to) { json(res, 404, { error: 'no_user' }); return }
      const s = ensureSession(from.sessionId)
      const itemsBase = await getCatalogBaseForSession(s)
      const items = Array.isArray(body.items) ? body.items.map(it => ({ product: reqString(it.product, 1, 140), quantity: reqInt(it.quantity || 1, 1, 999) })) : []
      const filtered = items.filter(it => it.product)
      if (!filtered.length) { json(res, 400, { error: 'no_items' }); return }
      const idemKey = getIdempotencyKey(req)
      if (idemKey) {
        const prev = await dbGetIdempotent(idemKey, pathname)
        if (prev) { json(res, prev.status, prev.body); return }
      }
      const createdAt = now()
      const orders = []
      await withDbTx(async (client) => {
        for (const it of filtered) {
          const found = itemsBase.find(i => i.name === it.product)
          const price = found ? Number(found.price || 0) : 0
          const total = price * Number(it.quantity || 1)
          const orderId = genId('ord')
          const expiresAt = now() + 10 * 60 * 1000
          const order = { id: orderId, sessionId: from.sessionId, emitterId: from.id, receiverId: to.id, product: it.product, quantity: it.quantity, price, total, status: 'pendiente_cobro', createdAt, expiresAt, emitterTable: from.tableId || '', receiverTable: to.tableId || '', mesaEntrega: to.tableId || '', isInvitation: true }
          await dbInsertOrder(order, client)
          await dbInsertEvent({ sessionId: order.sessionId, entityType: 'order', entityId: order.id, eventType: 'created', payload: { product: order.product, quantity: order.quantity, price: order.price, total: order.total, emitterId: order.emitterId, receiverId: order.receiverId }, ts: createdAt }, client)
          orders.push(order)
        }
      })
      for (const order of orders) {
        state.orders.set(order.id, order)
        sendToStaff(order.sessionId, 'order_new', { order })
        sendToUser(order.emitterId, 'order_update', { order })
        sendToUser(order.receiverId, 'order_update', { order })
        sendToUser(order.emitterId, 'consumption_accepted', { from: { id: to.id, alias: to.alias }, product: order.product, quantity: order.quantity })
      }
      const ids = orders.map(o => o.id)
      if (idemKey) await dbSetIdempotent(idemKey, pathname, 200, { orderIds: ids })
      json(res, 200, { orderIds: ids })
      return
    }
    if (pathname === '/api/thanks/send' && req.method === 'POST') {
      const body = await parseBody(req)
      const from = state.users.get(body.fromId)
      const to = state.users.get(body.toId)
      if (!from || !to) { json(res, 404, { error: 'no_user' }); return }
      const hourKey = from.id
      const bucket = state.rate.thanksByUserHour.get(hourKey) || []
      const fresh = bucket.filter(ts => within(60 * 60 * 1000, ts))
      if (fresh.length >= 10) { json(res, 429, { error: 'rate_thanks' }); return }
      state.rate.thanksByUserHour.set(hourKey, [...fresh, now()])
      const message = String(body.message || '').slice(0, 140)
      const context = String(body.context || '')
      sendToUser(to.id, 'thanks', { from: { id: from.id, alias: from.alias }, context, message })
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/order/new' && req.method === 'POST') {
      const body = await parseBody(req)
      const userId = reqString(body.userId, 1, 80)
      const u = state.users.get(userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      const itemName = reqString(body.product, 1, 140)
      if (!itemName) { json(res, 400, { error: 'no_product' }); return }
      const qty = reqInt(body.quantity || 1, 1, 999)
      const s = ensureSession(u.sessionId)
      const itemsBase = await getCatalogBaseForSession(s)
      const found = itemsBase.find(i => i.name === itemName)
      const price = found ? found.price : 0
      const total = price * qty
      const idemKey = getIdempotencyKey(req)
      if (idemKey) {
        const prev = await dbGetIdempotent(idemKey, pathname)
        if (prev) { json(res, prev.status, prev.body); return }
      }
      const orderId = genId('ord')
      const expiresAt = now() + 10 * 60 * 1000
      const createdAt = now()
      const mesaEntrega = u.tableId || ''
      const order = { id: orderId, sessionId: u.sessionId, emitterId: u.id, receiverId: u.id, product: itemName, quantity: qty, price, total, status: 'pendiente_cobro', createdAt, expiresAt, emitterTable: u.tableId || '', receiverTable: u.tableId || '', mesaEntrega }
      await withDbTx(async (client) => {
        await dbInsertOrder(order, client)
        await dbInsertEvent({ sessionId: order.sessionId, entityType: 'order', entityId: order.id, eventType: 'created', payload: { product: order.product, quantity: order.quantity, price: order.price, total: order.total, emitterId: order.emitterId, receiverId: order.receiverId }, ts: createdAt }, client)
      })
      state.orders.set(orderId, order)
      sendToStaff(order.sessionId, 'order_new', { order })
      sendToUser(u.id, 'order_update', { order })
      if (idemKey) await dbSetIdempotent(idemKey, pathname, 200, { orderId })
      json(res, 200, { orderId })
      return
    }
    if (pathname === '/api/order/bulk' && req.method === 'POST') {
      const body = await parseBody(req)
      const userId = reqString(body.userId, 1, 80)
      const u = state.users.get(userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      const s = ensureSession(u.sessionId)
      const itemsBase = await getCatalogBaseForSession(s)
      const items = Array.isArray(body.items) ? body.items.map(it => ({ product: reqString(it.product, 1, 140), quantity: reqInt(it.quantity || 1, 1, 999) })) : []
      const filtered = items.filter(it => it.product)
      if (!filtered.length) { json(res, 400, { error: 'no_items' }); return }
      const idemKey = getIdempotencyKey(req)
      if (idemKey) {
        const prev = await dbGetIdempotent(idemKey, pathname)
        if (prev) { json(res, prev.status, prev.body); return }
      }
      const createdAt = now()
      const orders = []
      await withDbTx(async (client) => {
        for (const it of filtered) {
          const found = itemsBase.find(i => i.name === it.product)
          const price = found ? Number(found.price || 0) : 0
          const total = price * it.quantity
          const orderId = genId('ord')
          const expiresAt = now() + 10 * 60 * 1000
          const mesaEntrega = u.tableId || ''
          const order = { id: orderId, sessionId: u.sessionId, emitterId: u.id, receiverId: u.id, product: it.product, quantity: it.quantity, price, total, status: 'pendiente_cobro', createdAt, expiresAt, emitterTable: u.tableId || '', receiverTable: u.tableId || '', mesaEntrega }
          await dbInsertOrder(order, client)
          await dbInsertEvent({ sessionId: order.sessionId, entityType: 'order', entityId: order.id, eventType: 'created', payload: { product: order.product, quantity: order.quantity, price: order.price, total: order.total, emitterId: order.emitterId, receiverId: order.receiverId }, ts: createdAt }, client)
          orders.push(order)
        }
      })
      for (const order of orders) {
        state.orders.set(order.id, order)
        sendToStaff(order.sessionId, 'order_new', { order })
        sendToUser(u.id, 'order_update', { order })
      }
      const orderIds = orders.map(o => o.id)
      if (idemKey) await dbSetIdempotent(idemKey, pathname, 200, { orderIds })
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
      await withDbTx(async (client) => {
        const r = await client.query('SELECT status FROM orders WHERE id=$1 FOR UPDATE', [String(order.id)])
        if (!r.rows.length) throw new Error('no_order')
        await client.query('UPDATE orders SET status=$2 WHERE id=$1', [String(order.id), String(status)])
        await dbInsertEvent({ sessionId: order.sessionId, entityType: 'order', entityId: order.id, eventType: 'status', payload: { status }, ts: now() }, client)
      })
      order.status = status
      sendToUser(order.emitterId, 'order_update', { order })
      sendToUser(order.receiverId, 'order_update', { order })
      sendToStaff(order.sessionId, 'order_update', { order })
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/staff/orders' && req.method === 'GET') {
      const sessionId = query.sessionId
      const stateFilter = query.state || ''
      const s = ensureSession(sessionId)
      const mode = getSessionMode(s)
      let catalogIndex = null
      try {
        const itemsBase = s ? await getCatalogBaseForSession(s) : await getGlobalCatalogForMode(mode)
        const idx = {}
        for (const it of Array.isArray(itemsBase) ? itemsBase : []) {
          const key = String(it.name || '').toLowerCase()
          if (key) idx[key] = it
        }
        catalogIndex = idx
      } catch {}
      const labelFor = (name) => {
        if (!catalogIndex) return ''
        const key = String(name || '').toLowerCase()
        const it = catalogIndex[key]
        return formatCatalogItemLabel(mode, it)
      }
      if (db) {
        const rows = await dbGetOrdersBySession(sessionId, stateFilter || null)
        const out = []
        for (const r of rows) {
          out.push({
            id: r.id,
            product: r.product,
            productLabel: labelFor(r.product),
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
            productLabel: labelFor(o.product),
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
      const sessionId = reqString(body.sessionId, 1, 80)
      const s = ensureSession(sessionId)
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      const t = reqString(body.tableId, 1, 40)
      if (!t) { json(res, 400, { error: 'no_table' }); return }
      const closed = reqBool(body.closed)
      const idemKey = getIdempotencyKey(req)
      if (idemKey) {
        const prev = await dbGetIdempotent(idemKey, pathname)
        if (prev) { json(res, prev.status, prev.body); return }
      }
      s.closedTables = s.closedTables || new Set()
      if (closed) s.closedTables.add(t)
      else s.closedTables.delete(t)
      await withDbTx(async (client) => {
        await client.query('SELECT closed FROM table_closures WHERE session_id=$1 AND table_id=$2 FOR UPDATE', [String(s.id), String(t)])
        await client.query('INSERT INTO table_closures (session_id, table_id, closed) VALUES ($1,$2,$3) ON CONFLICT (session_id, table_id) DO UPDATE SET closed=EXCLUDED.closed', [String(s.id), String(t), closed])
        await dbInsertEvent({ sessionId: s.id, entityType: 'table', entityId: t, eventType: closed ? 'closed' : 'opened', payload: { closed }, ts: now() }, client)
      })
      if (closed) {
        const affected = []
        for (const u of state.users.values()) {
          if (u.sessionId !== s.id || u.role !== 'user') continue
          if (String(u.tableId || '') !== String(t)) continue
          affected.push(u)
        }
        for (const u of affected) {
          u.tableId = ''
          u.sessionId = ''
          try { await dbUpsertUser(u) } catch {}
          sendToUser(u.id, 'table_closed', { tableId: t })
          sendToUser(u.id, 'session_end', { sessionId: s.id, tableId: t })
          const list = state.sseUsers.get(u.id) || []
          for (const res of list) { try { res.end() } catch {} state.sseUserMeta.delete(res) }
          state.sseUsers.delete(u.id)
          state.users.delete(u.id)
        }
      }
      sendToStaff(s.id, 'table_closed', { tableId: t, closed })
      if (idemKey) await dbSetIdempotent(idemKey, pathname, 200, { ok: true })
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
    if (pathname === '/api/user/invites' && req.method === 'GET') {
      const userId = query.userId
      const TTL = 60 * 1000
      const invites = []
      for (const inv of state.invites.values()) {
        if (inv.toId === userId && inv.status === 'pendiente' && within(TTL, inv.createdAt)) {
          const from = state.users.get(inv.fromId)
          invites.push({
            id: inv.id,
            msg: inv.msg,
            createdAt: inv.createdAt,
            expiresAt: inv.expiresAt,
            from: { id: inv.fromId, alias: from ? (from.alias || '') : '', selfie: from ? (from.selfie || '') : '', tableId: from ? (from.tableId || '') : '', zone: from ? (from.zone || '') : '', gender: (from && from.prefs && from.prefs.gender) ? from.prefs.gender : '' }
          })
        }
      }
      json(res, 200, { invites })
      return
    }
    if (pathname === '/api/user/invites/history' && req.method === 'GET') {
      const userId = query.userId
      const out = []
      for (const inv of state.invites.values()) {
        if ((inv.fromId === userId || inv.toId === userId)) {
          const from = state.users.get(inv.fromId)
          const to = state.users.get(inv.toId)
          out.push({
            id: inv.id,
            msg: inv.msg,
            status: inv.status,
            createdAt: inv.createdAt,
            expiresAt: inv.expiresAt || 0,
            from: { id: inv.fromId, alias: from ? (from.alias || '') : '' },
            to: { id: inv.toId, alias: to ? (to.alias || '') : '' },
          })
        }
      }
      // ordenar por fecha ascendente para consistencia
      out.sort((a, b) => Number(a.createdAt || 0) - Number(b.createdAt || 0))
      json(res, 200, { invites: out })
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
      const userId = reqString(body.userId, 1, 80)
      const u = state.users.get(userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      const idemKey = getIdempotencyKey(req)
      if (idemKey) {
        const prev = await dbGetIdempotent(idemKey, pathname)
        if (prev) { json(res, prev.status, prev.body); return }
      }
      const callId = genId('call')
      const reason = reqString(body.reason, 0, 140)
      const ts = now()
      const call = { id: callId, sessionId: u.sessionId, userId: u.id, tableId: u.tableId || '', reason, status: 'pendiente', ts }
      await withDbTx(async (client) => {
        await dbInsertWaiterCall(call, client)
        await dbInsertEvent({ sessionId: call.sessionId, entityType: 'waiter_call', entityId: call.id, eventType: 'created', payload: { userId: call.userId, tableId: call.tableId, reason: call.reason }, ts }, client)
      })
      state.waiterCalls.set(callId, call)
      sendToStaff(call.sessionId, 'waiter_call', { call })
      sendToUser(call.userId, 'waiter_update', { call })
      if (idemKey) await dbSetIdempotent(idemKey, pathname, 200, { callId })
      json(res, 200, { callId })
      return
    }
    if (pathname === '/api/dj/request' && req.method === 'POST') {
      const body = await parseBody(req)
      const u = state.users.get(body.userId)
      if (!u) { json(res, 404, { error: 'no_user' }); return }
      const table = String(u.tableId || '')
      if (!table) { json(res, 400, { error: 'no_table' }); return }
      {
        const s = ensureSession(u.sessionId)
        const enabled = !!(s && s.djEnabled)
        const until = Number(s && s.djEnabledUntil || 0)
        if (!enabled || (until && now() > until)) { json(res, 403, { error: 'dj_disabled' }); return }
      }
      const song = String(body.song || '').slice(0, 120)
      if (!song) { json(res, 400, { error: 'no_song' }); return }
      const reqId = genId('dj')
      const item = { id: reqId, sessionId: u.sessionId, userId: u.id, tableId: table, song, status: 'pendiente', ts: now() }
      state.djRequests.set(reqId, item)
      sendToStaff(item.sessionId, 'dj_request', { request: item })
      try { /* optional db insert */ } catch {}
      json(res, 200, { requestId: reqId })
      return
    }
    if (pathname === '/api/dj/status' && req.method === 'GET') {
      const sessionId = query.sessionId
      const s = ensureSession(sessionId)
      const enabled = !!(s && s.djEnabled)
      const until = Number(s && s.djEnabledUntil || 0)
      const current = s && s.djCurrent ? s.djCurrent : null
      json(res, 200, { enabled, until, current })
      return
    }
    if (pathname === '/api/staff/dj/status' && req.method === 'GET') {
      const sessionId = query.sessionId
      const s = ensureSession(sessionId)
      const enabled = !!(s && s.djEnabled)
      const until = Number(s && s.djEnabledUntil || 0)
      json(res, 200, { enabled, until })
      return
    }
    if (pathname === '/api/staff/dj/toggle' && req.method === 'POST') {
      const body = await parseBody(req)
      const s = ensureSession(body.sessionId)
      if (!s) { json(res, 404, { error: 'no_session' }); return }
      const enabled = !!body.enabled
      const ttlMin = Math.max(0, Number(body.ttlMinutes || 0))
      s.djEnabled = enabled
      s.djEnabledUntil = enabled ? (ttlMin ? (now() + ttlMin * 60 * 1000) : 0) : 0
      sendToStaff(s.sessionId, 'dj_toggle', { enabled: s.djEnabled, until: s.djEnabledUntil })
      sendToAllUsersInSession(s.sessionId, 'dj_toggle', { enabled: s.djEnabled, until: s.djEnabledUntil })
      json(res, 200, { ok: true, enabled: s.djEnabled, until: s.djEnabledUntil })
      return
    }
    if (pathname === '/api/staff/dj' && req.method === 'GET') {
      const sessionId = query.sessionId
      const tableId = String(query.tableId || '')
      const list = []
      for (const r of state.djRequests.values()) {
        if (r.sessionId !== sessionId) continue
        if (tableId && r.tableId !== tableId) continue
        const u = state.users.get(r.userId)
        list.push({ id: r.id, tableId: r.tableId, song: r.song, status: r.status, ts: r.ts, userAlias: u ? u.alias : r.userId })
      }
      json(res, 200, { requests: list })
      return
    }
    if (pathname.startsWith('/api/staff/dj/') && req.method === 'POST') {
      const reqId = pathname.split('/').pop()
      const body = await parseBody(req)
      const r = state.djRequests.get(reqId)
      if (!r) { json(res, 404, { error: 'no_request' }); return }
      const status = String(body.status || 'atendido')
      r.status = status
      sendToStaff(r.sessionId, 'dj_update', { request: r })
      if (status === 'programado' || status === 'atendido') {
        const queue = []
        for (const it of state.djRequests.values()) {
          if (it.sessionId !== r.sessionId) continue
          if (it.id === r.id) continue
          if (it.status === 'pendiente' || it.status === 'atendido' || it.status === 'programado') queue.push(it)
        }
        queue.sort((a, b) => Number(a.ts || 0) - Number(b.ts || 0))
        sendToUser(r.userId, 'dj_update', { request: r, queue: queue.map(q => ({ tableId: q.tableId, song: q.song })) })
        if (status === 'programado') {
          const u = state.users.get(r.userId)
          const alias = u ? u.alias : ''
          const s = ensureSession(r.sessionId)
          if (s) s.djCurrent = { song: r.song, tableId: r.tableId, alias, state: 'programado', ts: now() }
          sendToAllUsersInSession(r.sessionId, 'dj_now_programmed', { song: r.song, tableId: r.tableId, alias })
        }
      } else if (status === 'sonando' || status === 'terminado' || status === 'descartado') {
        sendToUser(r.userId, 'dj_update', { request: r })
        if (status === 'sonando') {
          const u = state.users.get(r.userId)
          const alias = u ? u.alias : ''
          const s = ensureSession(r.sessionId)
          if (s) s.djCurrent = { song: r.song, tableId: r.tableId, alias, state: 'sonando', ts: now() }
          sendToAllUsersInSession(r.sessionId, 'dj_now_playing', { song: r.song, tableId: r.tableId, alias })
        } else if (status === 'terminado') {
          const s = ensureSession(r.sessionId)
          if (s) s.djCurrent = null
          sendToAllUsersInSession(r.sessionId, 'dj_now_stopped', { song: r.song, tableId: r.tableId })
        }
      } else {
        sendToUser(r.userId, 'dj_update', { request: r })
      }
      json(res, 200, { ok: true })
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
    if (pathname === '/api/staff/venue_health' && req.method === 'GET') {
      const sessionId = query.sessionId
      const minOld = Math.max(1, Number(query.min || 5))
      const nowTs = now()
      let invitesActive = 0
      let invitesTotal = 0
      let invitesSeen = 0
      let invitesAccepted = 0
      const tablesActivity = {}
      for (const inv of state.invites.values()) {
        if (inv.sessionId !== sessionId) continue
        invitesTotal++
        if (inv.status === 'pendiente') invitesActive++
        if (inv.seenAt) invitesSeen++
        if (inv.status === 'aceptado') invitesAccepted++
        const toUser = state.users.get(inv.toId)
        const t = toUser && toUser.tableId ? toUser.tableId : ''
        if (t) {
          const ent = tablesActivity[t] || { received: 0, accepted: 0 }
          ent.received++
          if (inv.status === 'aceptado') ent.accepted++
          tablesActivity[t] = ent
        }
      }
      for (const ci of state.consumptionInvites.values()) {
        if (ci.sessionId !== sessionId) continue
        if (ci.expiresAt && nowTs < ci.expiresAt) invitesActive++
        const toUser = state.users.get(ci.toId)
        const t = toUser && toUser.tableId ? toUser.tableId : ''
        if (t) {
          const ent = tablesActivity[t] || { received: 0, accepted: 0 }
          ent.received++
          tablesActivity[t] = ent
        }
      }
      const ordersPendingOverX = []
      for (const o of state.orders.values()) {
        if (o.sessionId !== sessionId) continue
        if (o.status === 'pendiente_cobro' && (nowTs - Number(o.createdAt || nowTs)) > (minOld * 60 * 1000)) ordersPendingOverX.push(o)
      }
      const alerts = []
      const windowMs = 10 * 60 * 1000
      const tablesWindow = {}
      for (const inv of state.invites.values()) {
        if (inv.sessionId !== sessionId) continue
        if (!inv.createdAt || (nowTs - inv.createdAt) > windowMs) continue
        const toUser = state.users.get(inv.toId)
        const t = toUser && toUser.tableId ? toUser.tableId : ''
        if (!t) continue
        const entry = tablesWindow[t] || { received: 0, accepted: 0 }
        entry.received++
        if (inv.status === 'aceptado') entry.accepted++
        tablesWindow[t] = entry
      }
      for (const t of Object.keys(tablesWindow)) {
        const entry = tablesWindow[t]
        if (entry.received >= 12 && entry.accepted === 0) alerts.push(`Mesa ${t} recibió ${entry.received} invitaciones en 10 min, 0 aceptadas`)
      }
      const seenRate = invitesTotal ? Math.round((invitesSeen / invitesTotal) * 100) : 0
      const acceptedRate = invitesTotal ? Math.round((invitesAccepted / invitesTotal) * 100) : 0
      const tables = Object.keys(tablesActivity).map(t => ({ tableId: t, received: tablesActivity[t].received, accepted: tablesActivity[t].accepted }))
      tables.sort((a, b) => {
        if (b.received !== a.received) return b.received - a.received
        return a.tableId.localeCompare(b.tableId)
      })
      // Pico de invitaciones por hora (últimas 24h)
      const buckets = new Map()
      for (const inv of state.invites.values()) {
        if (inv.sessionId !== sessionId) continue
        const ts = Number(inv.createdAt || 0)
        if (!ts || (nowTs - ts) > (24 * 60 * 60 * 1000)) continue
        const d = new Date(ts)
        const hr = d.getHours()
        buckets.set(hr, (buckets.get(hr) || 0) + 1)
      }
      let peakHour = -1, peakCount = 0
      for (const [hr, count] of buckets.entries()) { if (count > peakCount) { peakCount = count; peakHour = hr } }
      const pad = (n) => String(n).padStart(2, '0')
      const peakHourLabel = peakHour >= 0 ? `${pad(peakHour)}:00` : ''
      const mostActiveTable = tables.length ? tables[0].tableId : ''
      json(res, 200, { invitesActive, seenRate, acceptedRate, tablesActivity: tables, ordersPendingOverMin: ordersPendingOverX.length, alerts, peakHour, peakHourLabel, mostActiveTable })
      return
    }
    if (pathname === '/api/staff/catalog' && req.method === 'POST') {
      const body = await parseBody(req)
      const s = ensureSession(body.sessionId)
      const mode = normalizeMode(body.mode || (s && s.mode) || '')
      const initVenueCatalog = !!body.initVenueCatalog
      const resetVenueCatalog = !!body.resetVenueCatalog
      const items = Array.isArray(body.items) ? body.items : []
      const clean = []
      for (const it of items) {
        const name = String(it.name || '').slice(0, 60)
        const price = Number(it.price || 0)
        const rawCat = String(it.category || '').toLowerCase().slice(0, 24)
        const category = allowedCategories.includes(rawCat) ? rawCat : 'otros'
        const subcategory = String(it.subcategory || '').slice(0, 60)
        const description = String(it.description || '').slice(0, 240)
        const combo = !!it.combo
        const includes = Array.isArray(it.includes) ? it.includes.map(x => String(x || '').slice(0, 60)).filter(x => x) : []
        const discount = Math.max(0, Math.min(100, Number(it.discount || 0)))
        if (!name) continue
        clean.push({ name, price, category, subcategory, description, combo, includes, discount })
      }
      if (s) s.catalog = clean
      const venueId = s && s.venueId ? s.venueId : String(body.venueId || '')
      if (db) {
        if (venueId) {
          try {
            if (resetVenueCatalog) {
              await dbDeleteVenueCatalog(venueId, mode)
              await dbSetVenueCatalogInitialized(venueId, mode, false)
            } else {
              if (clean.length) await dbWriteVenueCatalog(venueId, mode, clean)
              else await dbDeleteVenueCatalog(venueId, mode)
              await dbSetVenueCatalogInitialized(venueId, mode, true)
            }
          } catch {}
        } else if (s) {
          try { await dbWriteSessionCatalog(s.id, clean) } catch {}
        }
        else { try { await dbWriteGlobalCatalog(mode, clean) } catch {} }
      } else {
        if (venueId) {
          if (resetVenueCatalog) deleteVenueCatalogFile(venueId, mode)
          else writeVenueCatalogFile(venueId, mode, clean)
        } else if (!s) {
          writeGlobalCatalog(mode, clean)
        }
      }
      if (s) sendToStaff(s.id, 'catalog_update', { items: s.catalog })
      json(res, 200, { ok: true })
      return
    }
    if (pathname === '/api/catalog' && req.method === 'GET') {
      const sessionId = query.sessionId
      let s = sessionId ? ensureSession(sessionId) : null
      if (!s && sessionId) { try { await loadSessionFromRedis(sessionId) } catch {} s = ensureSession(sessionId) }
      const mode = s && s.mode ? s.mode : query.mode
      const venueId = (s && s.venueId) ? s.venueId : String(query.venueId || '')
      if (db) {
        let items = []
        let source = ''
        let venueInitialized = false
        if (venueId) {
          const st = await getVenueCatalogState(venueId, mode)
          venueInitialized = st.initialized
          if (venueInitialized) {
            items = st.items || []
            source = 'venue'
          }
        }
        if ((!items || !items.length) && !venueInitialized && sessionId) {
          try { items = await dbReadSessionCatalog(sessionId) } catch {}
          if (items && items.length && !source) source = 'session'
        }
        if (!venueInitialized && (!items || !items.length)) {
          try { items = await dbReadGlobalCatalog(mode) } catch {}
          if (items && items.length && !source) source = 'global'
        }
        if (!venueInitialized && (!items || !items.length)) {
          try { items = readGlobalCatalog(mode) } catch {}
          if (!source) source = 'file'
        }
        json(res, 200, { items, source, venueInitialized })
        return
      }
      let items = []
      let source = ''
      let venueInitialized = false
      if (venueId) {
        try {
          const p = venueCatalogPath(venueId, mode)
          if (fs.existsSync(p)) {
            venueInitialized = true
            items = readVenueCatalogFile(venueId, mode)
            source = 'venue'
          }
        } catch {}
      }
      const useSession = !!(s && Array.isArray(s.catalog) && s.catalog.length)
      if ((!items || !items.length) && !venueInitialized && useSession) {
        items = s.catalog
        if (!source) source = 'session'
      }
      if (!venueInitialized && (!items || !items.length)) {
        items = readGlobalCatalog(mode)
        if (!source) source = 'file'
      }
      json(res, 200, { items, source, venueInitialized })
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
      try { u.lastActiveAt = now() } catch {}
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
  } catch (e) {
    log('error', 'http_error', { requestId: rid, method: req.method, path: req.url, error: String(e && (e.stack || e.message) || e) })
    try { json(res, 500, { error: 'server_error', requestId: rid }) } catch {}
  }
})

server.on('request', (req, res) => {
  const { pathname } = url.parse(req.url, true)
  if (pathname === '/favicon.ico') {
    res.writeHead(200, { 'Content-Type': 'image/x-icon', 'Cache-Control': 'max-age=86400', 'Access-Control-Allow-Origin': '*' })
    res.end('')
  }
})

setInterval(async () => {
  if (!REDIS_URL) return
  const sessions = Array.from(state.sessions.keys())
  for (const sid of sessions) { try { await saveSessionToRedis(sid) } catch {} }
}, 10000)

setInterval(async () => {
  const nowTs = now()
  for (const o of state.orders.values()) {
    if (o.status === 'pendiente_cobro' && nowTs > o.expiresAt) {
      o.status = 'expirado'
      sendToUser(o.emitterId, 'order_update', { order: o })
      sendToUser(o.receiverId, 'order_update', { order: o })
      sendToStaff(o.sessionId, 'order_update', { order: o })
      try {
        await withDbTx(async (client) => {
          const r = await client.query('SELECT status FROM orders WHERE id=$1 FOR UPDATE', [String(o.id)])
          if (!r.rows.length) return
          await client.query('UPDATE orders SET status=$2 WHERE id=$1', [String(o.id), 'expirado'])
          await dbInsertEvent({ sessionId: o.sessionId, entityType: 'order', entityId: o.id, eventType: 'status', payload: { status: 'expirado' }, ts: nowTs }, client)
        })
      } catch {}
    }
  }
  // Expirar invitaciones pendientes (>30s sin respuesta)
  for (const inv of state.invites.values()) {
    if (inv.status === 'pendiente' && inv.expiresAt && nowTs > inv.expiresAt) {
      inv.status = 'expirado'
      try {
        const reason = inv.seenAt ? '' : 'unseen'
        if (reason === 'unseen') { try { updateBehaviorScore(inv.fromId, -2) } catch {} }
        sendToUser(inv.fromId, 'invite_result', { inviteId: inv.id, status: 'expirado', reason })
        sendToUser(inv.toId, 'invite_result', { inviteId: inv.id, status: 'expirado', reason })
      } catch {}
    }
  }
  for (const inv of state.invites.values()) {
    if (inv.status === 'pendiente' && !inv.seenAt && !inv.notSeenNotified && inv.expiresAt && nowTs > inv.expiresAt) {
      inv.notSeenNotified = true
      const uTo = state.users.get(inv.toId)
      try {
        sendToUser(inv.toId, 'invite_suppress', { inviteId: inv.id })
      } catch {}
    }
  }
  for (const ci of state.consumptionInvites.values()) {
    if (!ci.seenAt && !ci.notSeenNotified && ci.expiresAt && nowTs > ci.expiresAt) {
      ci.notSeenNotified = true
      const uTo = state.users.get(ci.toId)
      try {
        sendToUser(ci.fromId, 'consumption_not_seen', { requestId: ci.id, to: { id: ci.toId, alias: uTo ? (uTo.alias || uTo.id) : ci.toId } })
        sendToUser(ci.toId, 'consumption_suppress', { requestId: ci.id })
      } catch {}
    }
    if (ci.expiresAt && nowTs > ci.expiresAt) {
      state.consumptionInvites.delete(ci.id)
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
        if (uFrom) { uFrom.danceState = 'idle'; uFrom.dancePartnerId = ''; uFrom.meetingId = ''; uFrom.lastMeetingEndedAt = now() }
        if (uTo) { uTo.danceState = 'idle'; uTo.dancePartnerId = ''; uTo.meetingId = ''; uTo.lastMeetingEndedAt = now() }
        try {
          sendToUser(inv.fromId, 'meeting_expired', { meetingId: m.id })
          sendToUser(inv.toId, 'meeting_expired', { meetingId: m.id })
          sendToUser(inv.fromId, 'dance_status', { state: 'idle' })
          sendToUser(inv.toId, 'dance_status', { state: 'idle' })
        } catch {}
      }
    }
  }
  if ((nowTs - lastSSEPing) > 60 * 1000) {
    lastSSEPing = nowTs
    for (const uid of state.sseUsers.keys()) {
      try { sendToUser(uid, 'ping', { ts: nowTs }) } catch {}
    }
    for (const sessId of state.sseStaff.keys()) {
      try { sendToStaff(sessId, 'ping', { ts: nowTs }) } catch {}
    }
  }
  // Decaimiento de puntaje de comportamiento cada 10 minutos
  if (!state.scoreLastDecay || (nowTs - state.scoreLastDecay) > (10 * 60 * 1000)) {
    state.scoreLastDecay = nowTs
    for (const [uid, score] of state.behaviorScore.entries()) {
      if (score > 0) state.behaviorScore.set(uid, score - 1)
      else if (score < 0) state.behaviorScore.set(uid, score + 1)
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
