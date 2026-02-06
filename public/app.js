// Añadimos venueId para operar en modo SaaS multi-venue
let S = { sessionId: '', venueId: '', user: null, staff: null, role: '', sse: null, staffSSE: null, currentInvite: null, meeting: null, consumptionReq: null, nav: { history: [], current: '' }, notifications: { invites: 0 }, timers: { userPoll: 0, staffPoll: 0, userReconnect: 0, staffReconnect: 0, catalogSave: 0 }, staffTab: '', cart: [] }

function q(id) { return document.getElementById(id) }
function show(id) {
  for (const el of document.querySelectorAll('.screen')) el.classList.remove('active')
  q(id).classList.add('active')
  if (S.nav && S.nav.current && S.nav.current !== id) S.nav.history.push(S.nav.current)
  S.nav.current = id
  const tb = q('topbar')
  if (tb) tb.classList.add('show')
  setActiveNavByScreen(id)
  if (String(id).startsWith('screen-staff')) {
    const map = {
      'screen-staff': 'tab-staff-panel',
      'screen-staff-orders': 'tab-staff-orders',
      'screen-staff-mesas': 'tab-staff-mesas',
      'screen-staff-users': 'tab-staff-users',
      'screen-staff-waiter': 'tab-staff-waiter',
      'screen-staff-reportes': 'tab-staff-reportes',
      'screen-staff-promos': 'tab-staff-promos',
      'screen-staff-catalog': 'tab-staff-catalog',
    }
    for (const el of document.querySelectorAll('#staff-tabs .tab-item')) el.classList.remove('active')
    const tabId = map[id]
    if (tabId) { const el = q(tabId); if (el) el.classList.add('active') }
  }
  const isStaffView = String(id).startsWith('screen-staff')
  const nav = document.getElementById('navbar')
  const fab = document.getElementById('fab-call')
  const fabLabel = document.getElementById('fab-call-label')
  if (nav) nav.style.display = isStaffView ? 'none' : ''
  if (fab) fab.style.display = isStaffView ? 'none' : ''
  if (fabLabel) fabLabel.style.display = isStaffView ? 'none' : ''
}
function goBack() {
  if (!S.nav || !S.nav.history.length) return
  const prev = S.nav.history.pop()
  for (const el of document.querySelectorAll('.screen')) el.classList.remove('active')
  q(prev).classList.add('active')
  S.nav.current = prev
  const tb = q('topbar')
  const roots = ['screen-welcome', 'screen-user-home', 'screen-staff']
  if (tb) tb.classList.toggle('show', !!(S.nav.history.length && !roots.includes(S.nav.current)))
  setActiveNavByScreen(prev)
}
function goHome() {
  if (S.role === 'user') { show('screen-user-home'); return }
  if (S.role === 'staff') { show('screen-staff'); return }
  show('screen-welcome')
}
function setActiveNav(tab) {
  for (const el of document.querySelectorAll('.nav-item')) el.classList.remove('active')
  const map = { carta: 'nav-carta', disponibles: 'nav-disponibles', mesas: 'nav-mesas', orders: 'nav-orders', perfil: 'nav-perfil' }
  const id = map[tab]
  if (id) { const el = q(id); if (el) el.classList.add('active') }
}
function setActiveNavByScreen(screenId) {
  const reverse = { 'screen-consumption': 'carta', 'screen-disponibles-select': 'disponibles', 'screen-disponibles': 'disponibles', 'screen-mesas': 'mesas', 'screen-orders-user': 'orders', 'screen-edit-profile': 'perfil' }
  const tab = reverse[screenId]
  if (tab) setActiveNav(tab)
}
function setBadgeNav(tab, count) {
  const map = { disponibles: 'badge-disponibles' }
  const id = map[tab]
  const el = id ? q(id) : null
  if (!el) return
  if (count > 0) { el.classList.add('show'); el.textContent = count > 9 ? '9+' : String(count) }
  else { el.classList.remove('show'); el.textContent = '' }
}

function normalizeTableId(v) {
  const m = String(v || '').match(/\d+/)
  return m ? m[0] : ''
}

function setTheme(t) {
  try { localStorage.setItem('discos_theme', t) } catch {}
  if (document && document.body) document.body.dataset.theme = t
}
function toggleTheme() {
  const cur = document && document.body ? document.body.dataset.theme : ''
  setTheme(cur === 'light' ? 'dark' : 'light')
}

function showStaffTab(tab) {
  const contentMap = {
    panel: 'staff-panel-content',
    orders: 'staff-orders-content',
    mesas: 'staff-mesas-content',
    users: 'staff-users-content',
    waiter: 'staff-waiter-content',
    reportes: 'staff-reportes-content',
    promos: 'staff-promos-content',
    catalog: 'staff-catalog-content',
  }
  for (const el of document.querySelectorAll('#staff-content .section')) el.classList.remove('active')
  const id = contentMap[tab]
  if (id) { const el = q(id); if (el) el.classList.add('active') }
  for (const el of document.querySelectorAll('#staff-tabs .tab-item')) el.classList.remove('active')
  const tabMap = {
    panel: 'tab-staff-panel', orders: 'tab-staff-orders', mesas: 'tab-staff-mesas', users: 'tab-staff-users',
    waiter: 'tab-staff-waiter', reportes: 'tab-staff-reportes', promos: 'tab-staff-promos', catalog: 'tab-staff-catalog'
  }
  const tId = tabMap[tab]; if (tId) { const el = q(tId); if (el) el.classList.add('active') }
  if (tab === 'orders') loadOrders(q('staff-orders-filter')?.value || '')
  else if (tab === 'users') loadUsers()
  else if (tab === 'waiter') loadWaiterCalls()
  else if (tab === 'reportes') loadReports()
  else if (tab === 'promos') loadStaffPromos()
  else if (tab === 'catalog') loadStaffCatalogEditor()
  else if (tab === 'panel') loadSessionInfo()
  S.staffTab = tab
}

async function loadSessionInfo() {
  try {
    let pin = ''
    try { const r = await api('/api/session/active'); pin = r.pin || '' } catch {}
    let baseCandidate = ''
    try {
      const pb = await api(`/api/session/public-base?sessionId=${encodeURIComponent(S.sessionId)}`)
      baseCandidate = (pb.publicBaseUrl || '').trim()
      const inp = q('public-base'); if (inp && baseCandidate) inp.value = baseCandidate
    } catch {}
    let base = baseCandidate || location.origin
    // QR y link de sesión incluyen venueId + sessionId
    const url = `${base}/?venueId=${encodeURIComponent(S.venueId || 'default')}&sessionId=${encodeURIComponent(S.sessionId)}&aj=1`
    const pd = q('pin-display'); if (pd) pd.textContent = pin
    const qrImg = q('qr-session'); if (qrImg) qrImg.src = `https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=${encodeURIComponent(url)}`
    const share = q('share-url'); if (share) { share.href = url; share.textContent = url }
  } catch {}
}
async function api(path, opts = {}) {
  const res = await fetch(path, { method: 'GET', ...opts, headers: { 'Content-Type': 'application/json' } })
  let data = null
  try { data = await res.json() } catch {}
  if (!res.ok) {
    const msg = data && data.error ? data.error : 'api'
    throw new Error(msg)
  }
  return data
}

function showModal(title, msg, type = 'info') {
  const m = q('modal')
  const t = q('modal-text')
  const tt = q('modal-title')
  if (!m || !t || !tt) return
  m.classList.remove('type-error', 'type-success', 'type-info')
  m.classList.add('type-' + type)
  tt.textContent = title || ''
  t.textContent = msg || ''
  m.classList.add('show')
}
function showError(msg) { showModal('Error', msg || '', 'error') }
function showInfo(msg) { showModal('Info', msg || '', 'info') }
function showSuccess(msg) { showModal('Listo', msg || '', 'success') }
function confirmAction(paraphrase) {
  return typeof window !== 'undefined' ? window.confirm(paraphrase) : true
}
function formatPriceShort(p) {
  const n = Number(p || 0)
  if (n >= 1000) {
    const k = n / 1000
    const txt = k % 1 === 0 ? String(k) : k.toFixed(1)
    return txt + 'k'
  }
  return String(n)
}
function scheduleUserSSEReconnect() {
  if (S.timers.userReconnect) return
  S.timers.userReconnect = setTimeout(() => { S.timers.userReconnect = 0; startEvents() }, 3000)
}
function scheduleStaffSSEReconnect() {
  if (S.timers.staffReconnect) return
  S.timers.staffReconnect = setTimeout(() => { S.timers.staffReconnect = 0; startStaffEvents() }, 3000)
}
function scheduleCatalogSave() {
  if (S.timers.catalogSave) { try { clearTimeout(S.timers.catalogSave) } catch {} }
  S.timers.catalogSave = setTimeout(async () => { S.timers.catalogSave = 0; try { await saveStaffCatalog() } catch {} }, 800)
}
function startUserPolls() {
  if (S.timers.userPoll) { try { clearInterval(S.timers.userPoll) } catch {} }
  S.timers.userPoll = setInterval(() => {
    if (S.user && S.user.available && S.nav.current === 'screen-disponibles') refreshAvailableList()
    if (S.nav.current === 'screen-orders-user') loadUserOrders()
  }, 8000)
}
function startStaffPolls() {
  if (S.timers.staffPoll) { try { clearInterval(S.timers.staffPoll) } catch {} }
  S.timers.staffPoll = setInterval(() => {
    if (S.nav.current === 'screen-staff') { loadUsers(); loadOrders(); loadWaiterCalls(); loadReports(); loadAnalytics() }
  }, 8000)
}

async function join(role, codeOverride = '', pinOverride = '') {
  try {
    showError('')
    const code = codeOverride || (q('join-code') ? q('join-code').value.trim() : '')
    const pin = pinOverride || (q('join-pin') ? q('join-pin').value.trim() : '')
    if (!code) { showError('Ingresa el código de sesión'); return }
    if (role === 'staff' && !pin) { showError('Ingresa el PIN de sesión'); return }
    S.sessionId = code
    const r = await api('/api/join', { method: 'POST', body: JSON.stringify({ sessionId: code, role, pin }) })
    S.user = r.user
    S.role = role
    try { saveLocalUser() } catch {}
    if (role === 'user') {
      show('screen-profile')
    } else {
      show('screen-staff')
      showStaffTab('session')
      await loadSessionInfo()
      startStaffEvents()
      loadOrders()
      loadUsers()
      loadReports()
      loadStaffCatalogEditor()
    }
    startEvents()
  } catch (e) {
    showError(String(e.message))
  }
}

async function saveProfile() {
  const alias = q('alias').value.trim()
  const tableId = (q('profile-table') ? q('profile-table').value.trim() : '')
  const file = q('selfie').files[0]
  let selfie = ''
  if (file) {
    selfie = await processSelfie(file).catch(() => '')
    if (!selfie) { showError('Selfie inválida o muy grande'); setTimeout(() => showError(''), 1400); return }
  }
  if (!alias) { showError('Ingresa tu alias'); setTimeout(() => showError(''), 1200); return }
  if (!tableId) { showError('Ingresa tu mesa'); setTimeout(() => showError(''), 1200); return }
  if (!file) { showError('Debes subir tu selfie'); setTimeout(() => showError(''), 1400); return }
  await api('/api/user/profile', { method: 'POST', body: JSON.stringify({ userId: S.user.id, alias, selfie }) })
  await api('/api/user/change-table', { method: 'POST', body: JSON.stringify({ userId: S.user.id, newTable: tableId }) })
  S.user.alias = alias
  S.user.selfie = selfie
  S.user.tableId = tableId
  try { saveLocalUser() } catch {}
  q('selfie-note').textContent = 'Selfie requiere aprobación antes de mostrarse'
  const ua = q('user-alias'), us = q('user-selfie'); if (ua) ua.textContent = S.user.alias || S.user.id; if (us) us.src = S.user.selfie || ''
  const ut = q('user-table'); if (ut) ut.textContent = S.user.tableId || '-'
  show('screen-user-home')
}

function dataUrlBytes(d) {
  const m = String(d || '').match(/^data:.*;base64,(.+)$/)
  return m ? Math.floor(m[1].length * 3 / 4) : 0
}
function loadImageFromFile(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => {
      const img = new Image()
      img.onload = () => resolve(img)
      img.onerror = reject
      img.src = reader.result
    }
    reader.onerror = reject
    reader.readAsDataURL(file)
  })
}
async function processSelfie(file) {
  const img = await loadImageFromFile(file)
  const max = 640
  const ratio = Math.min(1, max / Math.max(img.width || max, img.height || max))
  const w = Math.max(1, Math.floor((img.width || max) * ratio))
  const h = Math.max(1, Math.floor((img.height || max) * ratio))
  const canvas = document.createElement('canvas')
  canvas.width = w
  canvas.height = h
  const ctx = canvas.getContext('2d')
  ctx.drawImage(img, 0, 0, w, h)
  let q = 0.8
  let out = canvas.toDataURL('image/jpeg', q)
  let tries = 0
  while (dataUrlBytes(out) > 500 * 1024 && tries < 5) {
    q = Math.max(0.4, q - 0.1)
    out = canvas.toDataURL('image/jpeg', q)
    tries++
  }
  if (dataUrlBytes(out) > 500 * 1024) {
    q = 0.8
    out = canvas.toDataURL('image/webp', q)
    tries = 0
    while (dataUrlBytes(out) > 500 * 1024 && tries < 5) {
      q = Math.max(0.4, q - 0.1)
      out = canvas.toDataURL('image/webp', q)
      tries++
    }
  }
  return dataUrlBytes(out) <= 500 * 1024 ? out : ''
}

async function setAvailable() {
  const next = q('switch-available') ? q('switch-available').checked : false
  const receiveMode = q('receive-mode') ? q('receive-mode').value : 'all'
  const zone = q('zone') ? q('zone').value.trim() : ''
  const prev = !!(S.user && S.user.available)
  const modeTxt = receiveMode === 'all' ? 'de todos' : (receiveMode === 'mesas' ? 'de tu zona' : 'solo de invitados')
  const zoneTxt = zone ? `en zona ${zone}` : 'en todas las zonas'
  const phr = next ? `Vas a activar modo disponible para bailar y recibir ${modeTxt} ${zoneTxt}. ¿Confirmas?`
                    : `Vas a desactivar el modo disponible para bailar. ¿Confirmas?`
  const ok = confirmAction(phr)
  if (!ok) { if (q('switch-available')) q('switch-available').checked = prev; return }
  await api('/api/user/available', { method: 'POST', body: JSON.stringify({ userId: S.user.id, available: next, receiveMode, zone }) })
  if (!S.user) S.user = {}
  S.user.available = next
  S.user.receiveMode = receiveMode
  S.user.zone = zone
  if (next) { setActiveNav('disponibles'); await viewAvailable(); show('screen-user-home') }
}

async function viewAvailable() {
  if (!S.user || !S.user.available) {
    showError('Oye: debes poner modo activo antes de ver las personas disponibles')
    setTimeout(() => showError(''), 1500)
    return
  }
  const r = await api(`/api/users/available?sessionId=${encodeURIComponent(S.sessionId)}&onlyAvailable=true`)
  const container = q('available-list')
  container.innerHTML = ''
  for (const u of r.users) {
    const div = document.createElement('div')
    div.className = 'item'
    const img = document.createElement('img')
    img.width = 48; img.height = 48
    img.style.borderRadius = '50%'
    img.src = u.selfie || ''
    const alias = document.createElement('div')
    alias.className = 'alias'
    alias.textContent = u.alias || u.id
    const tbl = document.createElement('div')
    tbl.className = 'zone'
    tbl.textContent = u.tableId ? `Mesa: ${u.tableId}` : ''
    const zone = document.createElement('div')
    zone.className = 'zone'
    zone.textContent = u.zone ? `Zona: ${u.zone}` : ''
    const row = document.createElement('div')
    row.className = 'row'
    const bDance = document.createElement('button'); bDance.textContent = 'Bailar'; bDance.onclick = () => sendInviteQuick(u)
    const bSaludo = document.createElement('button'); bSaludo.textContent = 'Saludo'; bSaludo.onclick = () => { setReceiver(u); sendReaction(u.id, 'saludo') }
    const bBrindis = document.createElement('button'); bBrindis.textContent = 'Brindis'; bBrindis.onclick = () => { setReceiver(u); sendReaction(u.id, 'brindis') }
    const bConsumo = document.createElement('button'); bConsumo.textContent = 'Invitar'; bConsumo.onclick = () => { setReceiver(u); q('consumption-target').value = u.id; openConsumption() }
    row.append(bDance, bSaludo, bBrindis, bConsumo)
    div.append(img, alias, tbl, zone, row)
    container.append(div)
  }
  show('screen-disponibles')
}
async function refreshAvailableList() {
  if (!S.user || !S.user.available) return
  const r = await api(`/api/users/available?sessionId=${encodeURIComponent(S.sessionId)}&onlyAvailable=true`)
  const container = q('available-list')
  if (!container) return
  container.innerHTML = ''
  for (const u of r.users) {
    const div = document.createElement('div')
    div.className = 'item'
    const img = document.createElement('img')
    img.width = 48; img.height = 48
    img.style.borderRadius = '50%'
    img.src = u.selfie || ''
    const alias = document.createElement('div')
    alias.className = 'alias'
    alias.textContent = u.alias || u.id
    const tbl = document.createElement('div')
    tbl.className = 'zone'
    tbl.textContent = u.tableId ? `Mesa: ${u.tableId}` : ''
    const zone = document.createElement('div')
    zone.className = 'zone'
    zone.textContent = u.zone ? `Zona: ${u.zone}` : ''
    const row = document.createElement('div')
    row.className = 'row'
    const bDance = document.createElement('button'); bDance.textContent = 'Bailar'; bDance.onclick = () => sendInviteQuick(u)
    const bSaludo = document.createElement('button'); bSaludo.textContent = 'Saludo'; bSaludo.onclick = () => { setReceiver(u); sendReaction(u.id, 'saludo') }
    const bBrindis = document.createElement('button'); bBrindis.textContent = 'Brindis'; bBrindis.onclick = () => { setReceiver(u); sendReaction(u.id, 'brindis') }
    const bConsumo = document.createElement('button'); bConsumo.textContent = 'Invitar'; bConsumo.onclick = () => { setReceiver(u); q('consumption-target').value = u.id; openConsumption() }
    row.append(bDance, bSaludo, bBrindis, bConsumo)
    div.append(img, alias, tbl, zone, row)
    container.append(div)
  }
}
async function viewAvailableByTable() {
  if (!S.user || !S.user.available) {
    showError('Oye: debes poner modo activo antes de ver las personas disponibles')
    setTimeout(() => showError(''), 1500)
    return
  }
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  const r = await api(`/api/users/available?sessionId=${encodeURIComponent(S.sessionId)}&onlyAvailable=true`)
  const list = (r.users || []).filter(u => u.tableId === S.user.tableId)
  const container = q('available-list')
  container.innerHTML = ''
  for (const u of list) {
    const div = document.createElement('div')
    div.className = 'item'
    const img = document.createElement('img')
    img.width = 48; img.height = 48
    img.style.borderRadius = '50%'
    img.src = u.selfie || ''
    const alias = document.createElement('div')
    alias.className = 'alias'
    alias.textContent = u.alias || u.id
    const zone = document.createElement('div')
    zone.className = 'zone'
    zone.textContent = u.zone ? `Zona: ${u.zone}` : ''
    const row = document.createElement('div')
    row.className = 'row'
    const bDance = document.createElement('button'); bDance.textContent = 'Bailar'; bDance.onclick = () => sendInviteQuick(u)
    const bSaludo = document.createElement('button'); bSaludo.textContent = 'Saludo'; bSaludo.onclick = () => { setReceiver(u); sendReaction(u.id, 'saludo') }
    const bBrindis = document.createElement('button'); bBrindis.textContent = 'Brindis'; bBrindis.onclick = () => { setReceiver(u); sendReaction(u.id, 'brindis') }
    const bConsumo = document.createElement('button'); bConsumo.textContent = 'Invitar'; bConsumo.onclick = () => { setReceiver(u); q('consumption-target').value = u.id; openConsumption() }
    row.append(bDance, bSaludo, bBrindis, bConsumo)
    div.append(img, alias, zone, row)
    container.append(div)
  }
  show('screen-disponibles')
}
function showAvailableChoice() {
  show('screen-disponibles-select')
}

let inviteMsgType = 'bailamos'
function openInvite(u) {
  S.currentInvite = u
  q('invite-person').textContent = `A ${u.alias || u.id}`
  inviteMsgType = 'bailamos'
  show('screen-user-invite')
}

function chooseMsg(e) {
  inviteMsgType = e.target.dataset.msg
}

async function sendInvite() {
  if (!S.currentInvite) return
  const name = S.currentInvite.alias || S.currentInvite.id
  const ok = confirmAction(`Vas a invitar a ${name} con el mensaje "${inviteMsgType}". ¿Confirmas?`)
  if (!ok) return
  await api('/api/invite/dance', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId: S.currentInvite.id, messageType: inviteMsgType }) })
  show('screen-user-home')
}
function setReceiver(u) {
  const el = q('avail-receiver-id')
  if (el) el.textContent = u.id
}
async function sendInviteQuick(u) {
  setReceiver(u)
  S.currentInvite = u
  inviteMsgType = 'bailamos'
  const name = u.alias || u.id
  const ok = confirmAction(`Vas a invitar a ${name} con el mensaje "bailamos". ¿Confirmas?`)
  if (!ok) return
  await api('/api/invite/dance', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId: u.id, messageType: 'bailamos' }) })
  show('screen-disponibles')
}

function startEvents() {
  if (!S.user) return
  if (S.sse) S.sse.close()
  S.sse = new EventSource(`/api/events/user?userId=${encodeURIComponent(S.user.id)}`)
  S.sse.onopen = () => { if (S.timers.userReconnect) { try { clearTimeout(S.timers.userReconnect) } catch {}; S.timers.userReconnect = 0 } }
  S.sse.onerror = () => { scheduleUserSSEReconnect() }
  S.sse.addEventListener('dance_invite', e => {
    const data = JSON.parse(e.data)
    S.currentInvite = { id: data.invite.id, from: data.invite.from }
    const mesaTxt = data.invite.from.tableId ? ` • Mesa ${data.invite.from.tableId}` : ''
    const zoneTxt = data.invite.from.zone ? ` • Zona ${data.invite.from.zone}` : ''
    q('invite-received-info').textContent = `${data.invite.from.alias} te invita${mesaTxt}${zoneTxt}`
    const img = q('invite-from-selfie')
    if (img) img.src = data.invite.from.selfie || ''
    S.notifications.invites = (S.notifications.invites || 0) + 1
    setBadgeNav('disponibles', S.notifications.invites)
    show('screen-invite-received')
  })
  S.sse.addEventListener('match', e => {
    const data = JSON.parse(e.data)
    showError(`Match con ${data.with.alias}`)
    setTimeout(() => showError(''), 1500)
  })
  S.sse.addEventListener('invite_result', e => {
    const data = JSON.parse(e.data)
    if (data.status === 'aceptado') {
      S.meeting = data.meeting
      if (data.note) { const noteEl = q('meeting-note'); if (noteEl) noteEl.textContent = `Respuesta: ${data.note}`; showError(`Respuesta: ${data.note}`); setTimeout(() => showError(''), 1500) }
      renderMeeting()
    } else {
      if (data.note) { showError(`Respuesta: ${data.note}`); setTimeout(() => showError(''), 1500) }
      show('screen-user-home')
    }
  })
  S.sse.addEventListener('consumption_invite', e => {
    const data = JSON.parse(e.data)
    S.consumptionReq = data
    const msg = data.note ? ` • Mensaje: ${data.note}` : ''
    const mesaTxt = data.from.tableId ? ` • Mesa ${data.from.tableId}` : ''
    q('invite-received-info').textContent = `${data.from.alias} te invita ${data.product}${mesaTxt}${msg}`
    show('screen-invite-received')
  })
  S.sse.addEventListener('consumption_invite_bulk', e => {
    const data = JSON.parse(e.data)
    S.consumptionReq = data
    const msg = data.note ? ` • Mensaje: ${data.note}` : ''
    const mesaTxt = data.from.tableId ? ` • Mesa ${data.from.tableId}` : ''
    const listTxt = (Array.isArray(data.items) ? data.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : '')
    q('invite-received-info').textContent = `${data.from.alias} te invita ${listTxt}${mesaTxt}${msg}`
    show('screen-invite-received')
  })
  S.sse.addEventListener('order_update', e => {
    const data = JSON.parse(e.data)
    loadUserOrders()
  })
  S.sse.addEventListener('waiter_update', e => {
    const data = JSON.parse(e.data)
    showError(`Estado de tu llamado: ${data.call.status}`)
    setTimeout(() => showError(''), 1200)
  })
  S.sse.addEventListener('meeting_expired', e => {
    S.meeting = null
    show('screen-user-home')
  })
  S.sse.addEventListener('reaction', e => {
    const data = JSON.parse(e.data)
    showError(`${data.from.alias} te envió ${data.type}`)
    setTimeout(() => showError(''), 1500)
  })
  startUserPolls()
}

function renderMeeting() {
  const m = S.meeting
  const left = Math.max(0, Math.floor((m.expiresAt - Date.now()) / 1000))
  q('meeting-info').textContent = `Punto: ${m.point} • Tiempo: ${left}s`
  const noteEl = q('meeting-note')
  if (noteEl) noteEl.textContent = ''
  show('screen-meeting')
}

async function respondInvite(accept) {
  if (S.consumptionReq) {
    const hasItems = Array.isArray(S.consumptionReq.items) && S.consumptionReq.items.length > 0
    const listTxt = hasItems ? S.consumptionReq.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : S.consumptionReq.product
    const phr = accept ? `Vas a aceptar invitación de consumo de ${S.consumptionReq.from.alias} (${listTxt}). ¿Confirmas?`
                       : `Vas a ignorar invitación de consumo. ¿Confirmas?`
    const ok = confirmAction(phr)
    if (!ok) return
    if (accept) {
      if (hasItems) {
        await api('/api/consumption/respond/bulk', { method: 'POST', body: JSON.stringify({ fromId: S.consumptionReq.from.id, toId: S.user.id, items: S.consumptionReq.items, action: 'accept', requestId: S.consumptionReq.requestId || '' }) })
      } else {
        await api('/api/consumption/respond', { method: 'POST', body: JSON.stringify({ fromId: S.consumptionReq.from.id, toId: S.user.id, product: S.consumptionReq.product, action: 'accept', requestId: S.consumptionReq.requestId || '' }) })
      }
    }
    S.consumptionReq = null
    show('screen-user-home')
    return
  }
  if (!S.currentInvite) return
  const id = S.currentInvite.id
  const action = accept ? 'accept' : 'pass'
  const phr2 = accept ? `Vas a aceptar invitación de baile. ¿Confirmas?` : `Vas a pasar la invitación de baile. ¿Confirmas?`
  const ok2 = confirmAction(phr2)
  if (!ok2) return
  const note = (q('invite-response') ? q('invite-response').value.trim().slice(0, 120) : '')
  await api('/api/invite/respond', { method: 'POST', body: JSON.stringify({ inviteId: id, action, note }) })
  S.notifications.invites = Math.max(0, (S.notifications.invites || 0) - 1)
  setBadgeNav('disponibles', S.notifications.invites)
  if (!accept) show('screen-user-home')
}

async function cancelMeeting() {
  if (!S.meeting) return
  const ok = confirmAction('Vas a cancelar el encuentro. ¿Confirmas?')
  if (!ok) return
  await api('/api/meeting/cancel', { method: 'POST', body: JSON.stringify({ meetingId: S.meeting.id }) })
  S.meeting = null
  show('screen-user-home')
}

async function confirmMeeting() {
  if (!S.meeting) return
  const plan = S.meetingPlan || ''
  const planTxt = plan === 'come' ? 'Ven por mí' : plan === 'go' ? 'Ya voy por ti' : plan === 'pista' ? 'Nos vemos en la pista — no me quites la mirada que me pierdo' : ''
  const phr = planTxt ? `Vas a confirmar el encuentro: ${planTxt}. ¿Confirmas?` : 'Vas a confirmar el encuentro. ¿Confirmas?'
  const ok = confirmAction(phr)
  if (!ok) return
  showConfetti()
  await api('/api/meeting/confirm', { method: 'POST', body: JSON.stringify({ meetingId: S.meeting.id, plan }) })
  show('screen-user-home')
}
function setMeetingPlan(plan) {
  S.meetingPlan = plan
  const txt = plan === 'come' ? 'Elegiste: Ven por mí'
            : plan === 'go' ? 'Elegiste: Ya voy por ti'
            : plan === 'pista' ? 'Elegiste: Nos vemos en la pista — no me quites la mirada que me pierdo'
            : ''
  const el = q('meeting-plan-display'); if (el) el.textContent = txt
}
function showConfetti() {
  try {
    const container = document.createElement('div')
    container.className = 'confetti'
    const colors = ['#7d88ff','#5868ff','#ff6b6b','#ffb84b','#38d49c','#d43b5a','#e0a33d','#2cab83']
    const pieces = 80
    for (let i = 0; i < pieces; i++) {
      const e = document.createElement('i')
      const c = colors[Math.floor(Math.random()*colors.length)]
      e.style.background = c
      e.style.left = Math.floor(Math.random()*100) + 'vw'
      e.style.animationDelay = (Math.random()*0.8) + 's'
      e.style.transform = `translateY(-20px) rotate(${Math.floor(Math.random()*180)}deg)`
      container.appendChild(e)
    }
    document.body.appendChild(container)
    setTimeout(() => { try { document.body.removeChild(container) } catch {} }, 2400)
  } catch {}
}

async function sendConsumption() {
  const product = q('product').value
  const toId = q('consumption-target').value.trim()
  const qty = Math.max(1, Number(q('quantity').value || 1))
  const note = (q('consumption-note') ? q('consumption-note').value.trim() : '')
  if (!toId) return
  const displayTo = (S.usersIndex && S.usersIndex[toId] ? S.usersIndex[toId].alias : toId)
  const items = (S.cart && S.cart.length) ? S.cart.slice() : (product ? [{ product, quantity: qty }] : [])
  if (!items.length) { showError('Selecciona producto(s)'); return }
  const listTxt = items.map(it => `${it.quantity} x ${it.product}`).join(', ')
  const phr = note ? `Vas a invitar consumo: ${listTxt} para ${displayTo}. Mensaje: "${note}". ¿Confirmas?`
                    : `Vas a invitar consumo: ${listTxt} para ${displayTo}. ¿Confirmas?`
  const ok = confirmAction(phr)
  if (!ok) return
  if (items.length > 1) {
    await api('/api/consumption/invite/bulk', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId, items, note }) })
  } else {
    await api('/api/consumption/invite', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId, product: items[0].product, quantity: items[0].quantity, note }) })
  }
  S.cart = []
  renderCart()
  const inp = q('product'); if (inp) inp.value = ''
  const qn = q('quantity'); if (qn) qn.value = '1'
  show('screen-user-home')
}

async function orderSelf() {
  const product = q('product').value
  const qty = Math.max(1, Number(q('quantity').value || 1))
  const items = (S.cart && S.cart.length) ? S.cart.slice() : (product ? [{ product, quantity: qty }] : [])
  if (!items.length) { showError('Selecciona producto(s)'); return }
  const listTxt = items.map(it => `${it.quantity} x ${it.product}`).join(', ')
  const ok = confirmAction(`Vas a ordenar: ${listTxt} para ti. ¿Confirmas?`)
  if (!ok) return
  if (items.length > 1) {
    await api('/api/order/bulk', { method: 'POST', body: JSON.stringify({ userId: S.user.id, items, for: 'self' }) })
  } else {
    await api('/api/order/new', { method: 'POST', body: JSON.stringify({ userId: S.user.id, product: items[0].product, quantity: items[0].quantity, for: 'self' }) })
  }
  S.cart = []
  renderCart()
  const inp = q('product'); if (inp) inp.value = ''
  const qn = q('quantity'); if (qn) qn.value = '1'
  showError('Pedido creado')
  setTimeout(() => showError(''), 1000)
}
async function orderTable() {
  const product = q('product').value
  const qty = Math.max(1, Number(q('quantity').value || 1))
  const items = (S.cart && S.cart.length) ? S.cart.slice() : (product ? [{ product, quantity: qty }] : [])
  if (!items.length) { showError('Selecciona producto(s)'); return }
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); return }
  const listTxt = items.map(it => `${it.quantity} x ${it.product}`).join(', ')
  const ok = confirmAction(`Vas a ordenar: ${listTxt} para la mesa ${S.user.tableId}. ¿Confirmas?`)
  if (!ok) return
  if (items.length > 1) {
    await api('/api/order/bulk', { method: 'POST', body: JSON.stringify({ userId: S.user.id, items, for: 'mesa' }) })
  } else {
    await api('/api/order/new', { method: 'POST', body: JSON.stringify({ userId: S.user.id, product: items[0].product, quantity: items[0].quantity, for: 'mesa' }) })
  }
  S.cart = []
  renderCart()
  const inp = q('product'); if (inp) inp.value = ''
  const qn = q('quantity'); if (qn) qn.value = '1'
  showError('Pedido para mesa creado')
  setTimeout(() => showError(''), 1000)
}

async function blockUser() {
  const targetId = q('block-id').value.trim()
  if (!targetId) return
  const ok = confirmAction(`Vas a bloquear a ${targetId}. ¿Confirmas?`)
  if (!ok) return
  await api('/api/block', { method: 'POST', body: JSON.stringify({ userId: S.user.id, targetId }) })
}

async function reportUser() {
  const targetId = q('report-id').value.trim()
  const category = q('report-cat').value.trim()
  const note = q('report-note').value.trim()
  if (!targetId) return
  const ok = confirmAction(`Vas a reportar a ${targetId} por "${category}". Nota: "${note}". ¿Confirmas?`)
  if (!ok) return
  await api('/api/report', { method: 'POST', body: JSON.stringify({ userId: S.user.id, targetId, category, note }) })
}

async function startStaffSession() {
  try {
    showError('')
    const pinInput = (q('join-pin') ? q('join-pin').value.trim() : '')
    if (!pinInput) { showError('Ingresa el PIN'); return }
    // venueId opcional desde la URL para multi-tenant (sin cambiar UI)
    try {
      const u = new URL(location.href)
      const v = u.searchParams.get('venueId') || ''
      if (v) S.venueId = v
    } catch {}
    let r = null
    try { r = await api(`/api/session/active${S.venueId ? ('?venueId=' + encodeURIComponent(S.venueId)) : ''}`) } catch {}
    if (!r || !r.sessionId) r = await api('/api/session/start', { method: 'POST', body: JSON.stringify({ venueId: S.venueId || 'default' }) })
    const joinCodeEl = q('join-code'); if (joinCodeEl) joinCodeEl.value = r.sessionId
    S.sessionId = r.sessionId
    S.venueId = r.venueId || (S.venueId || 'default')
    const joinRes = await api('/api/join', { method: 'POST', body: JSON.stringify({ sessionId: r.sessionId, role: 'staff', pin: pinInput }) })
    S.user = joinRes.user
    S.role = 'staff'
    try { saveLocalUser() } catch {}
    show('screen-staff')
    showStaffTab('panel')
    await loadSessionInfo()
    startStaffEvents()
    loadOrders()
    loadUsers()
    loadReports()
    loadAnalytics()
    loadStaffPromos()
    loadStaffCatalogEditor()
  } catch (e) {
    showError(String(e.message))
  }
}

async function endStaffSession() {
  if (!S.sessionId) return
  const ok = typeof window !== 'undefined' ? window.confirm('¿Cerrar sesión de la noche y borrar datos?') : true
  if (!ok) return
  try { await saveStaffCatalog() } catch {}
  await api('/api/session/end', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId }) })
  try { if (S.sse) S.sse.close() } catch {}
  try { if (S.staffSSE) S.staffSSE.close() } catch {}
  S.sessionId = ''; S.user = null; S.role = ''; S.sse = null; S.staffSSE = null
  try { localStorage.removeItem('discos_user') } catch {}
  show('screen-welcome')
  showError('Sesión destruida')
  setTimeout(() => showError(''), 1200)
}
async function restartStaffSession() {
  if (S.sessionId) {
    const ok = typeof window !== 'undefined' ? window.confirm('¿Destruir sesión actual y crear una nueva?') : true
    if (!ok) return
    try { await saveStaffCatalog() } catch {}
    try { await api('/api/session/end', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId }) }) } catch {}
    try { if (S.sse) S.sse.close() } catch {}
    try { if (S.staffSSE) S.staffSSE.close() } catch {}
    S.sessionId = ''; S.user = null; S.role = ''; S.sse = null; S.staffSSE = null
    try { localStorage.removeItem('discos_user') } catch {}
  }
  await startStaffSession()
}

function startStaffEvents() {
  try { if (S.staffSSE) S.staffSSE.close() } catch {}
  S.staffSSE = new EventSource(`/api/events/staff?sessionId=${encodeURIComponent(S.sessionId)}`)
  S.staffSSE.onopen = () => { if (S.timers.staffReconnect) { try { clearTimeout(S.timers.staffReconnect) } catch {}; S.timers.staffReconnect = 0 } }
  S.staffSSE.onerror = () => { scheduleStaffSSEReconnect() }
  S.staffSSE.addEventListener('order_new', e => {
    loadOrders()
    loadAnalytics()
  })
  S.staffSSE.addEventListener('order_update', e => {
    loadOrders()
    loadAnalytics()
  })
  S.staffSSE.addEventListener('report', e => {
    loadReports()
    loadAnalytics()
  })
  S.staffSSE.addEventListener('waiter_call', e => {
    const data = JSON.parse(e.data)
    loadWaiterCalls()
    loadAnalytics()
    if (S.staffTab !== 'waiter') { showError(`Llamado de mesero: Mesa ${data.call.tableId || '-'}`); setTimeout(() => showError(''), 1500) }
  })
  S.staffSSE.addEventListener('waiter_update', e => {
    const data = JSON.parse(e.data)
    loadWaiterCalls()
    loadAnalytics()
    if (S.staffTab !== 'waiter') { showError(`Llamado actualizado: ${data.call.status}`); setTimeout(() => showError(''), 1200) }
  })
  S.staffSSE.addEventListener('table_closed', e => {
    loadAnalytics()
    viewStaffTableHistory()
  })
  S.staffSSE.addEventListener('catalog_update', e => {
    loadStaffCatalogEditor()
  })
  startStaffPolls()
}

async function loadOrders(state = '') {
  const qs = state ? `&state=${encodeURIComponent(state)}` : ''
  const r = await api(`/api/staff/orders?sessionId=${encodeURIComponent(S.sessionId)}${qs}`)
  const container = q('staff-orders-list') || q('orders')
  container.innerHTML = ''
  for (const o of r.orders) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    const chip = document.createElement('span')
    chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
    chip.textContent = o.status.replace('_', ' ')
    const mesaInfo = (o.mesaEntrega || o.receiverTable || o.emitterTable) ? ` • Mesa entrega ${o.mesaEntrega || o.receiverTable}` : ''
    const emAlias = (S.usersIndex && S.usersIndex[o.emitterId] ? S.usersIndex[o.emitterId].alias : o.emitterId)
    const reAlias = (S.usersIndex && S.usersIndex[o.receiverId] ? S.usersIndex[o.receiverId].alias : o.receiverId)
    info.textContent = `${o.product} x${o.quantity || 1} • $${o.total || 0} • Emisor ${emAlias} → Receptor ${reAlias}${mesaInfo} `
    info.append(chip)
    const row = document.createElement('div')
    row.className = 'row'
    const b1 = document.createElement('button'); b1.textContent = 'Cobrado'; b1.onclick = () => updateOrder(o.id, 'cobrado')
    const b0 = document.createElement('button'); b0.textContent = 'En preparación'; b0.onclick = () => updateOrder(o.id, 'en_preparacion')
    const b2 = document.createElement('button'); b2.textContent = 'Entregado'; b2.onclick = () => updateOrder(o.id, 'entregado')
    const b3 = document.createElement('button'); b3.textContent = 'Cancelar'; b3.onclick = () => updateOrder(o.id, 'cancelado')
    row.append(b0, b1, b2, b3)
    div.append(info, row)
    container.append(div)
  }
}

async function updateOrder(id, status) {
  await api(`/api/staff/orders/${id}`, { method: 'POST', body: JSON.stringify({ status }) })
  loadOrders()
}

async function loadUsers() {
  const r = await api(`/api/staff/users?sessionId=${encodeURIComponent(S.sessionId)}`)
  S.usersIndex = {}
  const container = q('staff-users') || q('users')
  container.innerHTML = ''
  for (const u of r.users) {
    S.usersIndex[u.id] = { alias: u.alias || u.id }
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    info.textContent = `${u.alias || u.id} • Selfie ${u.selfieApproved ? 'aprobada' : 'pendiente'} • ${u.muted ? 'silenciado' : 'activo'}`
    const row = document.createElement('div')
    row.className = 'row'
    const approve = document.createElement('button'); approve.textContent = 'Aprobar selfie'; approve.onclick = () => approveSelfie(u.id)
    const mute = document.createElement('button'); mute.textContent = u.muted ? 'Activar' : 'Silenciar'; mute.onclick = () => moderateUser(u.id, !u.muted)
    row.append(approve, mute)
    div.append(info, row)
    container.append(div)
  }
}

async function loadWaiterCalls() {
  const r = await api(`/api/staff/waiter?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('waiter-calls')
  container.innerHTML = ''
  for (const c of r.calls) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    info.textContent = `Mesa ${c.tableId || '-'} • ${c.userAlias ? c.userAlias : c.userId} • ${c.reason} • ${c.status}`
    const row = document.createElement('div')
    row.className = 'row'
    const btn = document.createElement('button'); btn.textContent = 'Atendido'; btn.onclick = async () => { await api(`/api/staff/waiter/${c.id}`, { method: 'POST', body: JSON.stringify({ status: 'atendido' }) }); loadWaiterCalls() }
    row.append(btn)
    div.append(info, row)
    container.append(div)
  }
  const bWaiter = q('badge-tab-waiter')
  if (bWaiter) { const v = (r.calls || []).filter(x => x.status === 'pendiente').length; bWaiter.classList.toggle('show', v > 0); bWaiter.textContent = v > 9 ? '9+' : String(v) }
}
async function approveSelfie(userId) {
  await api('/api/moderation/approve-selfie', { method: 'POST', body: JSON.stringify({ staffId: S.user.id, userId }) })
  loadUsers()
}

async function moderateUser(userId, muted) {
  await api('/api/staff/moderate', { method: 'POST', body: JSON.stringify({ staffId: S.user.id, userId, muted }) })
  loadUsers()
}

async function loadReports() {
  const r = await api(`/api/staff/reports?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('reports')
  container.innerHTML = ''
  for (const rep of r.reports) {
    const div = document.createElement('div')
    div.className = 'card'
    div.textContent = `De ${rep.fromId} sobre ${rep.targetId} • ${rep.category} • ${rep.note}`
    container.append(div)
  }
}
function bind() {
  const btnJoinUser = q('btn-join-user'); if (btnJoinUser) btnJoinUser.onclick = () => join('user')
  const btnJoinStaff = q('btn-join-staff'); if (btnJoinStaff) btnJoinStaff.onclick = startStaffSession
  const btnSaveProfile = q('btn-save-profile'); if (btnSaveProfile) btnSaveProfile.onclick = saveProfile
  const swAvail = q('switch-available'); if (swAvail) swAvail.onchange = setAvailable
  const receiveModeEl = q('receive-mode'); if (receiveModeEl) receiveModeEl.onchange = setAvailable
  const zoneEl = q('zone'); if (zoneEl) zoneEl.oninput = setAvailable
  const btnViewAvail = q('btn-view-available'); if (btnViewAvail) btnViewAvail.onclick = showAvailableChoice
  const btnViewMenu = q('btn-view-menu'); if (btnViewMenu) btnViewMenu.onclick = openMenu
  for (const b of document.querySelectorAll('.btn-invite-msg')) b.onclick = chooseMsg
  const btnInviteSend = q('btn-invite-send'); if (btnInviteSend) btnInviteSend.onclick = sendInvite
  const btnInviteAccept = q('btn-invite-accept'); if (btnInviteAccept) btnInviteAccept.onclick = () => respondInvite(true)
  const btnInvitePass = q('btn-invite-pass'); if (btnInvitePass) btnInvitePass.onclick = () => respondInvite(false)
  q('btn-meeting-cancel').onclick = cancelMeeting
  const btnInviteConsumption = q('btn-invite-consumption'); if (btnInviteConsumption) btnInviteConsumption.onclick = openConsumption
  q('btn-consumption-send').onclick = sendConsumption
  const btnAddCart = q('btn-add-to-cart'); if (btnAddCart) btnAddCart.onclick = addToCart
  const btnWaiterOrder = q('btn-waiter-order'); if (btnWaiterOrder) btnWaiterOrder.onclick = callWaiterOrder
  const btnBack = q('btn-back'); if (btnBack) btnBack.onclick = goBack
  const nc = q('nav-carta'), nd = q('nav-disponibles'), nm = q('nav-mesas'), no = q('nav-orders'), nf = q('nav-perfil')
  if (nc) nc.onclick = () => { setActiveNav('carta'); openMenu() }
  if (nd) nd.onclick = () => { setActiveNav('disponibles'); showAvailableChoice() }
  if (nm) nm.onclick = () => { setActiveNav('mesas'); exploreMesas() }
  if (no) no.onclick = () => { setActiveNav('orders'); loadUserOrders(); show('screen-orders-user') }
  if (nf) nf.onclick = () => { setActiveNav('perfil'); openEditProfile() }
  const linkStaff = q('link-staff'); if (linkStaff) linkStaff.onclick = (e) => { e.preventDefault(); show('screen-staff-welcome') }
  const fab = q('fab-call'); if (fab) fab.onclick = openCallWaiter
  const bAT = q('btn-avail-by-table'); if (bAT) bAT.onclick = exploreMesas
  const bAA = q('btn-avail-all'); if (bAA) bAA.onclick = viewAvailable
  const btnOrderSelf = q('btn-order-self'); if (btnOrderSelf) btnOrderSelf.onclick = orderSelf
  const btnOrderTable = q('btn-order-table'); if (btnOrderTable) btnOrderTable.onclick = orderTable
  q('btn-block').onclick = blockUser
  q('btn-report').onclick = reportUser
  const btnTheme = q('btn-theme-toggle'); if (btnTheme) btnTheme.onclick = toggleTheme
  const btnThemeTop = q('btn-theme-toggle-top'); if (btnThemeTop) btnThemeTop.onclick = toggleTheme
  const btnThemeWelcome = q('btn-theme-toggle-welcome'); if (btnThemeWelcome) btnThemeWelcome.onclick = toggleTheme
  const modalClose = q('modal-close'); if (modalClose) modalClose.onclick = () => { const m = q('modal'); if (m) m.classList.remove('show') }
  const modalEl = q('modal'); if (modalEl) modalEl.onclick = (e) => { if (e.target && e.target.id === 'modal') modalEl.classList.remove('show') }
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') { const m = q('modal'); if (m) m.classList.remove('show') } })
  const homeBtn = q('btn-home'); if (homeBtn) homeBtn.onclick = goHome
  const tabPanel = q('tab-staff-panel'); if (tabPanel) tabPanel.onclick = () => showStaffTab('panel')
  const tabSession = q('tab-staff-session'); if (tabSession) tabSession.onclick = () => showStaffTab('session')
  const tabOrders = q('tab-staff-orders'); if (tabOrders) tabOrders.onclick = () => showStaffTab('orders')
  const tabMesas = q('tab-staff-mesas'); if (tabMesas) tabMesas.onclick = () => showStaffTab('mesas')
  const tabUsers = q('tab-staff-users'); if (tabUsers) tabUsers.onclick = () => showStaffTab('users')
  const tabWaiter = q('tab-staff-waiter'); if (tabWaiter) tabWaiter.onclick = () => showStaffTab('waiter')
  const tabReportes = q('tab-staff-reportes'); if (tabReportes) tabReportes.onclick = () => showStaffTab('reportes')
  const tabPromos = q('tab-staff-promos'); if (tabPromos) tabPromos.onclick = () => showStaffTab('promos')
  const tabCatalog = q('tab-staff-catalog'); if (tabCatalog) tabCatalog.onclick = () => showStaffTab('catalog')
  q('btn-start-session-welcome').onclick = startStaffSession
  const btnScan = q('btn-scan-qr'); if (btnScan) btnScan.onclick = startScanQR
  q('btn-end-session').onclick = endStaffSession
  const btnExploreMesas = q('btn-explore-mesas'); if (btnExploreMesas) btnExploreMesas.onclick = exploreMesas
  q('btn-edit-profile').onclick = openEditProfile
  q('btn-edit-save').onclick = saveEditProfile
  q('btn-pause-social').onclick = pauseSocial
  const btnViewPromos = q('btn-view-promos'); if (btnViewPromos) btnViewPromos.onclick = viewPromos
  const btnCallWaiter = q('btn-call-waiter'); if (btnCallWaiter) btnCallWaiter.onclick = openCallWaiter
  q('btn-waiter-send').onclick = sendWaiterCall
  q('mesa-only-available').onchange = () => loadMesaPeople(S.currentTableId)
  q('btn-select-table').onclick = openSelectTable
  q('btn-select-table-save').onclick = saveSelectTable
  q('btn-view-orders').onclick = () => { loadUserOrders(); show('screen-orders-user') }
  q('btn-invite-block').onclick = blockFromInvite
  q('btn-invite-report').onclick = reportFromInvite
  q('btn-meeting-confirm').onclick = confirmMeeting
  const btnMeetCome = q('btn-meet-come'); if (btnMeetCome) btnMeetCome.onclick = () => setMeetingPlan('come')
  const btnMeetGo = q('btn-meet-go'); if (btnMeetGo) btnMeetGo.onclick = () => setMeetingPlan('go')
  const btnMeetPista = q('btn-meet-pista'); if (btnMeetPista) btnMeetPista.onclick = () => setMeetingPlan('pista')
  const btnStaffTableView = q('btn-staff-table-view'); if (btnStaffTableView) btnStaffTableView.onclick = viewStaffTableHistory
  const btnStaffTableClose = q('btn-staff-table-close'); if (btnStaffTableClose) btnStaffTableClose.onclick = closeStaffTable
  const btnStaff2TableView = q('btn-staff2-table-view'); if (btnStaff2TableView) btnStaff2TableView.onclick = viewStaffTableHistory2
  const btnStaff2TableClose = q('btn-staff2-table-close'); if (btnStaff2TableClose) btnStaff2TableClose.onclick = closeStaffTable2
  const btnStaffCatalogSave = q('btn-staff-catalog-save'); if (btnStaffCatalogSave) btnStaffCatalogSave.onclick = saveStaffCatalog
  const btnStaffCatalogAdd = q('btn-staff-catalog-add'); if (btnStaffCatalogAdd) btnStaffCatalogAdd.onclick = () => {
    const name = q('staff-catalog-add-name')?.value.trim()
    const price = Number(q('staff-catalog-add-price')?.value || 0)
    const container = q('staff-catalog-list')
    if (!container || !name) return
    const row = document.createElement('div')
    row.className = 'row'
    const nameInput = document.createElement('input'); nameInput.type = 'text'; nameInput.placeholder = 'Nombre'; nameInput.value = name
    const priceInput = document.createElement('input'); priceInput.type = 'number'; priceInput.min = '0'; priceInput.value = price
    nameInput.oninput = scheduleCatalogSave
    priceInput.oninput = scheduleCatalogSave
    const del = document.createElement('button'); del.textContent = 'Eliminar'; del.onclick = () => { try { row.remove(); scheduleCatalogSave() } catch {} }
    row.append(nameInput, priceInput, del)
    container.append(row)
    const inpName = q('staff-catalog-add-name'); if (inpName) inpName.value = ''
    const inpPrice = q('staff-catalog-add-price'); if (inpPrice) inpPrice.value = ''
    saveStaffCatalog()
  }
  const anUsers = q('an-users'); if (anUsers) anUsers.onclick = () => { showStaffTab('users'); loadUsers() }
  const anMesas = q('an-mesas'); if (anMesas) anMesas.onclick = () => { showStaffTab('mesas') }
  const anInv = q('an-invites'); if (anInv) anInv.onclick = () => { showStaffTab('orders'); loadOrders() }
  const anAcc = q('an-accepted'); if (anAcc) anAcc.onclick = () => { showStaffTab('orders'); loadOrders('pendiente_cobro') }
  const filt = q('staff-orders-filter'); if (filt) filt.onchange = () => loadOrders(filt.value)
  const copyBtn = q('btn-copy-link')
  if (copyBtn) copyBtn.onclick = async () => {
    try {
      const href = q('share-url')?.href || ''
      await navigator.clipboard.writeText(href)
      showError('Link copiado')
      setTimeout(() => showError(''), 1000)
    } catch (e) { showError('No se pudo copiar') }
  }
  const savePB = q('btn-save-public-base')
  if (savePB) savePB.onclick = async () => {
    try {
      const val = q('public-base').value.trim()
      await api('/api/session/public-base', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, publicBaseUrl: val }) })
      showError('URL pública guardada')
      setTimeout(() => showError(''), 1000)
      await startStaffSession()
    } catch (e) { showError(String(e.message)) }
  }
}

async function exploreMesas() {
  await loadMesasActive()
  show('screen-mesas')
}

async function loadMesasActive() {
  const r = await api(`/api/mesas/active?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('mesas-list')
  container.innerHTML = ''
  for (const m of r.mesas) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    info.textContent = `${m.tableId} • Personas ${m.people} • Disponibles ${m.disponibles} • +${m.incognitos} incógnitos`
    const tags = document.createElement('div')
    tags.textContent = (m.tags && m.tags.length) ? `Tags: ${m.tags.join(', ')}` : ''
    const btn = document.createElement('button')
    btn.textContent = 'Ver mesa'
    btn.onclick = () => openMesaView(m.tableId)
    div.append(info, tags, btn)
    container.append(div)
  }
}

function openMesaView(tableId) {
  S.currentTableId = tableId
  q('mesa-title').textContent = `Mesa ${tableId}`
  loadMesaPeople(tableId)
  loadMesaOrders(tableId)
  try { if (S.mesaOrdersInterval) clearInterval(S.mesaOrdersInterval) } catch {}
  S.mesaOrdersInterval = setInterval(() => {
    if (S.currentTableId) loadMesaOrders(S.currentTableId)
  }, 5000)
  show('screen-mesa-view')
}

async function loadMesaPeople(tableId) {
  const only = q('mesa-only-available').checked
  const r = await api(`/api/users/available?sessionId=${encodeURIComponent(S.sessionId)}&onlyAvailable=${only ? 'true':'false'}`)
  const list = r.users.filter(u => u.tableId === tableId)
  const container = q('mesa-people')
  container.innerHTML = ''
  for (const u of list) {
    const div = document.createElement('div')
    div.className = 'item'
    const img = document.createElement('img')
    img.width = 48; img.height = 48
    img.style.borderRadius = '50%'
    img.src = u.selfie || ''
    const alias = document.createElement('div')
    alias.className = 'alias'
    alias.textContent = u.alias || u.id
    const row = document.createElement('div')
    row.className = 'row'
    const bInvite = document.createElement('button'); bInvite.textContent = 'Bailar'; bInvite.onclick = () => openInvite(u)
    const bSaludo = document.createElement('button'); bSaludo.textContent = 'Saludo'; bSaludo.onclick = () => sendReaction(u.id, 'saludo')
    const bBrindis = document.createElement('button'); bBrindis.textContent = 'Brindis'; bBrindis.onclick = () => sendReaction(u.id, 'brindis')
    const bConsumo = document.createElement('button'); bConsumo.textContent = 'Invitar'; bConsumo.onclick = () => { q('consumption-target').value = u.id; openConsumption() }
    row.append(bInvite, bSaludo, bBrindis, bConsumo)
    div.append(img, alias, row)
    container.append(div)
  }
}

async function loadMesaOrders(tableId) {
  const r = await api(`/api/table/orders?sessionId=${encodeURIComponent(S.sessionId)}&tableId=${encodeURIComponent(tableId)}&userId=${encodeURIComponent(S.user.id)}`)
  const container = q('mesa-orders')
  container.innerHTML = ''
  for (const o of r.orders) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    const chip = document.createElement('span')
    chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
    chip.textContent = o.status.replace('_', ' ')
    info.textContent = `${o.product} • ${o.emitterAlias || o.emitterId} → ${o.receiverAlias || o.receiverId}`
    const row = document.createElement('div')
    row.className = 'row'
    info.append(chip)
    if (o.isInvitation) {
      const invChip = document.createElement('span')
      invChip.className = 'chip'
      invChip.textContent = 'Invitación'
      info.append(invChip)
    }
    div.append(info, row)
    container.append(div)
  }
}

async function sendReaction(toId, type) {
  await api('/api/reaction/send', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId, type }) })
}

function openEditProfile() {
  q('edit-alias').value = S.user.alias || ''
  q('edit-tags').value = (Array.isArray(S.user.prefs?.tags) ? S.user.prefs.tags.join(',') : '')
  q('edit-table').value = S.user.tableId || ''
  show('screen-edit-profile')
}

async function saveEditProfile() {
  const alias = q('edit-alias').value.trim()
  const tags = q('edit-tags').value.split(',').map(s => s.trim()).filter(Boolean)
  const tableRaw = q('edit-table').value.trim()
  const tableId = normalizeTableId(tableRaw)
  await api('/api/user/update', { method: 'POST', body: JSON.stringify({ userId: S.user.id, alias, tags, tableId }) })
  showError('Perfil actualizado')
  setTimeout(() => showError(''), 1000)
  show('screen-user-home')
}

async function pauseSocial() {
  const ok = confirmAction('Vas a activar la pausa social. ¿Confirmas?')
  if (!ok) return
  await api('/api/user/pause', { method: 'POST', body: JSON.stringify({ userId: S.user.id }) })
  showError('Pausa social activada')
  setTimeout(() => showError(''), 1000)
}

function openCallWaiter() {
  q('waiter-reason').value = ''
  show('screen-call-waiter')
}

async function sendWaiterCall() {
  const reason = q('waiter-reason').value.trim()
  const ok = confirmAction(`Vas a llamar al mesero por: ${reason || 'atención'}. ¿Confirmas?`)
  if (!ok) return
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  await api('/api/waiter/call', { method: 'POST', body: JSON.stringify({ userId: S.user.id, reason }) })
  showError('Mesero llamado')
  setTimeout(() => showError(''), 1000)
  show('screen-user-home')
}
async function callWaiterOrder() {
  const ok = confirmAction(`Vas a pedir tomar orden en tu mesa ${S.user.tableId || '-'}. ¿Confirmas?`)
  if (!ok) return
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  await api('/api/waiter/call', { method: 'POST', body: JSON.stringify({ userId: S.user.id, reason: 'Tomar orden en mesa' }) })
  showError('Mesero pedido para tomar orden')
  setTimeout(() => showError(''), 1200)
}

async function viewPromos() {
  const r = await api(`/api/promos?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('promos-list')
  container.innerHTML = ''
  for (const p of r.promos || []) {
    const div = document.createElement('div')
    div.className = 'card'
    div.textContent = p.title || ''
    container.append(div)
  }
  show('screen-promos')
}

function openSelectTable() {
  loadSelectTableList()
  q('select-table-manual').value = ''
  show('screen-select-table')
}

async function loadSelectTableList() {
  const r = await api(`/api/mesas/active?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('select-table-list')
  container.innerHTML = ''
  for (const m of r.mesas) {
    const div = document.createElement('div')
    div.className = 'item'
    div.textContent = `${m.tableId} • Personas ${m.people}`
    div.onclick = async () => {
      await api('/api/user/change-table', { method: 'POST', body: JSON.stringify({ userId: S.user.id, newTable: m.tableId }) })
      show('screen-user-home')
    }
    container.append(div)
  }
}

async function saveSelectTable() {
  const manualRaw = q('select-table-manual').value.trim()
  const manual = normalizeTableId(manualRaw)
  if (!manual) { show('screen-user-home'); return }
  await api('/api/user/change-table', { method: 'POST', body: JSON.stringify({ userId: S.user.id, newTable: manual }) })
  S.user.tableId = manual
  const ut = q('user-table'); if (ut) ut.textContent = S.user.tableId || '-'
  show('screen-user-home')
}

async function loadUserOrders() {
  const r = await api(`/api/user/orders?userId=${encodeURIComponent(S.user.id)}`)
  const container = q('user-orders')
  if (!container) return
  container.innerHTML = ''
  for (const o of r.orders) {
    const div = document.createElement('div')
    div.className = 'card'
    const chip = document.createElement('span')
    chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
    chip.textContent = o.status.replace('_',' ')
    const invChip = document.createElement('span')
    if (o.isInvitation) { invChip.className = 'chip'; invChip.textContent = 'Invitación' }
    div.textContent = `${o.product} x${o.quantity || 1} • $${o.total || 0} • ${o.emitterId === S.user.id ? 'Enviado a' : 'Recibido de'} ${o.emitterId === S.user.id ? o.receiverId : o.emitterId}`
    div.append(chip)
    if (o.isInvitation) div.append(invChip)
    container.append(div)
  }
}

async function blockFromInvite() {
  if (!S.currentInvite || !S.currentInvite.from) return
  await api('/api/block', { method: 'POST', body: JSON.stringify({ userId: S.user.id, targetId: S.currentInvite.from.id }) })
  show('screen-user-home')
}

async function reportFromInvite() {
  if (!S.currentInvite || !S.currentInvite.from) return
  await api('/api/report', { method: 'POST', body: JSON.stringify({ userId: S.user.id, targetId: S.currentInvite.from.id, category: 'invite', note: '' }) })
  show('screen-user-home')
}
async function startScanQR() {
  try {
    showError('')
    const video = q('qr-video')
    if (!('BarcodeDetector' in window)) { showError('Tu navegador no soporta escaneo QR'); return }
    const detector = new BarcodeDetector({ formats: ['qr_code'] })
    const stream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' } })
    video.srcObject = stream
    await video.play()
    video.style.display = 'block'
    const tick = async () => {
      try {
        const codes = await detector.detect(video)
        if (codes && codes.length) {
          const data = codes[0].rawValue || codes[0].data || ''
          try {
            const u = new URL(data)
            const sid = u.searchParams.get('sessionId') || u.searchParams.get('s')
            if (sid) {
              const tracks = stream.getTracks(); tracks.forEach(t => t.stop())
              location.href = data
              return
            }
          } catch {
            if (data.startsWith('sess_')) {
              const base = location.origin
              const url = `${base}/?venueId=${encodeURIComponent(S.venueId || 'default')}&sessionId=${encodeURIComponent(data)}&aj=1`
              const tracks = stream.getTracks(); tracks.forEach(t => t.stop())
              location.href = url
              return
            }
          }
        }
      } catch {}
      requestAnimationFrame(tick)
    }
    tick()
  } catch (e) {
    showError(String(e.message))
  }
}

function init() {
  bind()
  try {
    const u = new URL(location.href)
    const vid = u.searchParams.get('venueId') || ''
    if (vid) S.venueId = vid
    const sid = u.searchParams.get('sessionId') || u.searchParams.get('s')
    if (sid && q('join-code')) q('join-code').value = sid
    const aj = u.searchParams.get('aj')
    const staffParam = u.searchParams.get('staff')
    restoreLocalUser().then(ok => {
      if (ok) return
      if (sid && aj === '1') { setTimeout(() => join('user', sid), 50) }
      else if (staffParam === '1') { show('screen-staff-welcome') }
      else { show('screen-welcome') }
    })
  } catch {}
  if ('serviceWorker' in navigator) navigator.serviceWorker.register('/sw.js')
  try {
    setTheme('dark')
    try { localStorage.setItem('discos_theme', 'dark') } catch {}
  } catch { setTheme('dark') }
}

init()
function saveLocalUser() {
  const payload = { sessionId: S.sessionId, venueId: S.venueId || 'default', role: S.role || (S.user ? S.user.role : ''), userId: S.user ? S.user.id : '' }
  try { localStorage.setItem('discos_user', JSON.stringify(payload)) } catch {}
}
async function restoreLocalUser() {
  try {
    const raw = localStorage.getItem('discos_user')
    if (!raw) return false
    const d = JSON.parse(raw)
    if (!d.sessionId || !d.userId || !d.role) return false
    if (d.venueId) S.venueId = d.venueId
    const r = await api(`/api/user/get?userId=${encodeURIComponent(d.userId)}`).catch(() => null)
    if (!r || !r.user) return false
    S.user = r.user
    S.sessionId = r.user.sessionId
    S.role = r.user.role
    if (S.role === 'staff') {
      show('screen-staff')
      showStaffTab('session')
      await loadSessionInfo()
      startStaffEvents()
      loadOrders(); loadUsers(); loadReports(); loadAnalytics(); loadStaffPromos()
      return true
    } else if (S.role === 'user') {
      startEvents()
      show('screen-user-home')
      const rc = q('restore-chip')
      if (rc) {
        rc.textContent = 'Sesión restaurada'
        rc.style.display = 'inline-block'
        setTimeout(() => { rc.style.display = 'none' }, 1500)
      }
      const ua = q('user-alias'), us = q('user-selfie'), ut = q('user-table')
      if (ua) ua.textContent = S.user.alias || S.user.id
      if (ut) ut.textContent = S.user.tableId || '-'
      if (us) us.src = S.user.selfie || ''
      return true
    }
    return false
  } catch { return false }
}
async function loadAnalytics() {
  const r = await api(`/api/staff/analytics?sessionId=${encodeURIComponent(S.sessionId)}`)
  const users = q('an-users'), mesas = q('an-mesas'), inv = q('an-invites'), acc = q('an-accepted'), ratio = q('an-ratio')
  if (users) users.textContent = `Usuarios: ${r.usersCount}`
  if (mesas) mesas.textContent = `Mesas activas: ${r.mesasActivas}`
  if (inv) inv.textContent = `Invitaciones: ${r.invitesSent}`
  if (acc) acc.textContent = `Aceptadas: ${r.invitesAccepted}`
  const pct = r.invitesSent ? Math.round((r.invitesAccepted / r.invitesSent) * 100) : 0
  if (ratio) ratio.textContent = `Ratio: ${pct}%`
  const orders = q('an-orders')
  if (orders) {
    orders.innerHTML = ''
    for (const key of Object.keys(r.orders || {})) {
      const chip = document.createElement('span')
      chip.className = 'chip ' + (key === 'pendiente_cobro' ? 'pending' : key)
      chip.textContent = `${key}: ${r.orders[key]}`
      chip.onclick = () => { showStaffTab('orders'); loadOrders(key) }
      orders.append(chip)
    }
  }
  const bOrders = q('badge-tab-orders')
  if (bOrders) { const v = r.orders?.pendiente_cobro || 0; bOrders.classList.toggle('show', v > 0); bOrders.textContent = v > 9 ? '9+' : String(v) }
  const bMesas = q('badge-tab-mesas')
  if (bMesas) { const v = r.mesasActivas || 0; bMesas.classList.toggle('show', v > 0); bMesas.textContent = v > 9 ? '9+' : String(v) }
  const bUsers = q('badge-tab-usuarios')
  if (bUsers) { const v = r.usersCount || 0; bUsers.classList.toggle('show', v > 0); bUsers.textContent = v > 9 ? '9+' : String(v) }
  const top = q('an-top')
  if (top) {
    top.innerHTML = ''
    for (const name of Object.keys(r.topItems || {})) {
      const chip = document.createElement('span')
      chip.className = 'chip'
      chip.textContent = `${name}: ${r.topItems[name]}`
      top.append(chip)
    }
  }
}
function addStaffPromo() {
  const inp = q('staff-promos-input')
  const list = q('staff-promos-list')
  if (!inp || !list) return
  const title = inp.value.trim()
  if (!title) return
  const div = document.createElement('div')
  div.className = 'card'
  div.textContent = title
  list.append(div)
  inp.value = ''
}
async function saveStaffPromos() {
  const list = q('staff-promos-list')
  if (!list) return
  const promos = []
  for (const el of list.children) promos.push({ title: el.textContent })
  await api('/api/staff/promos', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, promos }) })
  showError('Promos guardadas')
  setTimeout(() => showError(''), 1000)
}
async function openConsumption() {
  await loadCatalog()
  const target = q('consumption-target'), sendBtn = q('btn-consumption-send')
  if (target) target.style.display = ''
  if (sendBtn) sendBtn.style.display = ''
  const targetLabel = q('consumption-target-label')
  if (targetLabel) targetLabel.style.display = ''
  const noteRow = q('consumption-note-row')
  if (noteRow) noteRow.style.display = ''
  const btnSelf = q('btn-order-self'), btnTable = q('btn-order-table')
  if (btnSelf) btnSelf.style.display = 'none'
  if (btnTable) btnTable.style.display = 'none'
  const cartSection = q('cart-section'); if (cartSection) { cartSection.style.display = ''; renderCart() }
  const addCart = q('btn-add-to-cart'); if (addCart) addCart.style.display = ''
  const grid = q('catalog-list'); if (grid) grid.style.display = ''
  show('screen-consumption')
}
async function openMenu() {
  await loadCatalog()
  const target = q('consumption-target'), sendBtn = q('btn-consumption-send')
  if (target) target.style.display = 'none'
  if (sendBtn) sendBtn.style.display = 'none'
  const targetLabel = q('consumption-target-label')
  if (targetLabel) targetLabel.style.display = 'none'
  const noteRow = q('consumption-note-row')
  if (noteRow) noteRow.style.display = 'none'
  const btnSelf = q('btn-order-self'), btnTable = q('btn-order-table')
  if (btnSelf) btnSelf.style.display = ''
  if (btnTable) btnTable.style.display = ''
  const cartSection = q('cart-section'); if (cartSection) { cartSection.style.display = ''; renderCart() }
  const addCart = q('btn-add-to-cart'); if (addCart) addCart.style.display = ''
  const grid = q('catalog-list'); if (grid) grid.style.display = ''
  show('screen-consumption')
}
async function loadCatalog() {
  try {
    const r = await api(`/api/catalog${S.sessionId ? ('?sessionId=' + encodeURIComponent(S.sessionId)) : ''}`)
    const container = q('catalog-list')
    if (!container) return
    container.innerHTML = ''
    for (const it of r.items || []) {
      const div = document.createElement('div')
      div.className = 'card'
      const name = document.createElement('div')
      name.textContent = it.name
      const price = document.createElement('span')
      price.className = 'chip'
      price.textContent = formatPriceShort(it.price)
      div.onclick = () => { q('product').value = it.name }
      div.append(name, price)
      container.append(div)
    }
  } catch {}
}
function renderCart() {
  const list = q('cart-list')
  if (!list) return
  list.innerHTML = ''
  for (let i = 0; i < S.cart.length; i++) {
    const it = S.cart[i]
    const row = document.createElement('div')
    row.className = 'row'
    const label = document.createElement('div')
    label.textContent = `${it.quantity} x ${it.product}`
    const del = document.createElement('button')
    del.textContent = 'Eliminar'
    del.onclick = () => { try { S.cart.splice(i, 1); renderCart() } catch {} }
    row.append(label, del)
    list.append(row)
  }
}
function addToCart() {
  const product = q('product') ? q('product').value : ''
  const qty = Math.max(1, Number(q('quantity') ? q('quantity').value : 1))
  if (!product) { showError('Selecciona un producto'); return }
  S.cart.push({ product, quantity: qty })
  const inp = q('product'); if (inp) inp.value = ''
  const qn = q('quantity'); if (qn) qn.value = '1'
  renderCart()
}
async function loadStaffCatalogEditor() {
  try {
    const r = await api(`/api/catalog${S.sessionId ? ('?sessionId=' + encodeURIComponent(S.sessionId)) : ''}`)
    const container = q('staff-catalog-list')
    if (!container) return
    container.innerHTML = ''
    for (const it of r.items || []) {
      const row = document.createElement('div')
      row.className = 'row'
      const name = document.createElement('input')
      name.type = 'text'
      name.placeholder = 'Nombre'
      name.value = it.name
      const price = document.createElement('input')
      price.type = 'number'
      price.min = '0'
      price.value = Number(it.price || 0)
      name.oninput = scheduleCatalogSave
      price.oninput = scheduleCatalogSave
      const del = document.createElement('button'); del.textContent = 'Eliminar'; del.onclick = () => { try { row.remove(); scheduleCatalogSave() } catch {} }
      row.append(name, price, del)
      container.append(row)
    }
  } catch {}
}
async function saveStaffCatalog() {
  try {
    const list = q('staff-catalog-list')
    if (!list) return
    const items = []
    for (const row of list.children) {
      const nameInput = row.querySelector('input[type="text"]')
      const priceInput = row.querySelector('input[type="number"]')
      if (!nameInput || !priceInput) continue
      const name = nameInput.value.trim()
      const price = Number(priceInput.value || 0)
      items.push({ name, price })
    }
    await api('/api/staff/catalog', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, items }) })
    showError('Carta guardada')
    setTimeout(() => showError(''), 1000)
    await loadStaffCatalogEditor()
  } catch (e) { showError(String(e.message)) }
}
async function loadStaffPromos() {
  const r = await api(`/api/promos?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('staff-promos-list')
  if (!container) return
  container.innerHTML = ''
  for (const p of r.promos || []) {
    const div = document.createElement('div')
    div.className = 'card'
    div.textContent = p.title || ''
    container.append(div)
  }
}
async function viewStaffTableHistory() {
  const raw = q('staff-table-id')?.value.trim()
  const t = normalizeTableId(raw)
  if (!t) return
  const r = await api(`/api/staff/table/orders?sessionId=${encodeURIComponent(S.sessionId)}&tableId=${encodeURIComponent(t)}`)
  const container = q('staff-table-orders')
  if (!container) return
  container.innerHTML = ''
  for (const o of (r.orders || [])) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    info.textContent = `${o.product} x${o.quantity || 1} • $${o.total || 0} • ${o.emitterAlias || o.emitterId}→${o.receiverAlias || o.receiverId}`
    const chip = document.createElement('span')
    chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
    chip.textContent = o.status.replace('_', ' ')
    info.append(chip)
    div.append(info)
    container.append(div)
  }
}
async function viewStaffTableHistory2() {
  const raw = q('staff2-table-id')?.value.trim()
  const t = normalizeTableId(raw)
  if (!t) return
  const r = await api(`/api/staff/table/orders?sessionId=${encodeURIComponent(S.sessionId)}&tableId=${encodeURIComponent(t)}`)
  const container = q('staff2-table-orders')
  if (!container) return
  container.innerHTML = ''
  for (const o of (r.orders || [])) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    info.textContent = `${o.product} x${o.quantity || 1} • $${o.total || 0} • ${o.emitterAlias || o.emitterId}→${o.receiverAlias || o.receiverId}`
    const chip = document.createElement('span')
    chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
    chip.textContent = o.status.replace('_', ' ')
    info.append(chip)
    div.append(info)
    container.append(div)
  }
}
async function closeStaffTable() {
  const raw = q('staff-table-id')?.value.trim()
  const t = normalizeTableId(raw)
  if (!t) return
  await api('/api/staff/table/close', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, tableId: t, closed: true }) })
  showError('Mesa cerrada (historial oculto)')
  setTimeout(() => showError(''), 1000)
  viewStaffTableHistory()
}
async function closeStaffTable2() {
  const raw = q('staff2-table-id')?.value.trim()
  const t = normalizeTableId(raw)
  if (!t) return
  await api('/api/staff/table/close', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, tableId: t, closed: true }) })
  showError('Mesa cerrada (historial oculto)')
  setTimeout(() => showError(''), 1000)
  viewStaffTableHistory2()
}
