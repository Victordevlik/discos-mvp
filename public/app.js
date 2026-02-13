// Añadimos venueId para operar en modo SaaS multi-venue
let S = { sessionId: '', venueId: '', user: null, staff: null, role: '', sse: null, staffSSE: null, currentInvite: null, meeting: null, consumptionReq: null, nav: { history: [], current: '' }, notifications: { invites: 0 }, timers: { userPoll: 0, staffPoll: 0, userReconnect: 0, staffReconnect: 0, catalogSave: 0, modalHide: 0 }, staffTab: '', cart: [], messageTTL: 4000, modalShownAt: 0, isMeetingReceiver: false, meetingPlan: '', sched: {}, loading: {}, catalogGroups: {}, catalogCat: '', catalogSubcat: '', waiterReason: '', invitesQueue: [], inInviteFlow: false, missed: [], skipConfirmInvite: false, audioCtx: null, modalKind: '', appMode: '' }

function q(id) { return document.getElementById(id) }
function isRestaurantMode() { return S.appMode === 'restaurant' }
function normalizeModeParam(mode) {
  const m = String(mode || '').toLowerCase()
  return (m === 'restaurant' || m === '1') ? 'restaurant' : ''
}
function applyMode(mode) {
  const m = normalizeModeParam(mode)
  if (m === 'restaurant') {
    S.appMode = 'restaurant'
    applyRestaurantMode()
  } else {
    S.appMode = ''
    applyDiscoMode()
  }
  setModeInUrl(m)
  return m
}
function getModeFromUrl() {
  try {
    const u = new URL(location.href)
    return normalizeModeParam(u.searchParams.get('mode') || u.searchParams.get('restaurant') || '')
  } catch { return '' }
}
async function syncSessionMode(sessionId) {
  if (!sessionId) return ''
  try {
    const r = await api(`/api/session/info?sessionId=${encodeURIComponent(sessionId)}`)
    return applyMode(r.mode || '')
  } catch {}
  return ''
}
function setModeInUrl(mode) {
  const u = new URL(location.href)
  if (mode === 'restaurant') u.searchParams.set('mode', 'restaurant')
  else u.searchParams.delete('mode')
  u.searchParams.delete('restaurant')
  history.replaceState({}, '', u.pathname + u.search + u.hash)
}
function applyRestaurantMode() {
  const setTxt = (sel, txt) => { const el = document.querySelector(sel); if (el) el.textContent = txt }
  if (document && document.body) document.body.dataset.venue = 'restaurant'
  document.title = 'Restaurante'
  setTxt('#welcome-title', 'Restaurante')
  setTxt('#welcome-subtitle', 'Ordena, llama al mesero y revisa tu cuenta en segundos.')
  setTxt('#venue-type-title', 'Selecciona el tipo de venue')
  setTxt('#staff-welcome-title', 'Ingreso Restaurante')
  setTxt('#staff-title', 'Restaurante — Panel de órdenes')
  setTxt('#staff-panel-title', 'Escanéame para unirte al restaurante')
  setTxt('#tab-staff-catalog', 'Menú')
  setTxt('#menu-staff-catalog', 'Menú')
  const search = q('catalog-search'); if (search) search.placeholder = 'Buscar en menú'
  const lblGender = q('label-gender'); if (lblGender) lblGender.style.display = 'none'
  const gender = q('profile-gender'); if (gender) { gender.style.display = 'none'; gender.required = false }
  const lblSelfie = q('label-selfie'); if (lblSelfie) lblSelfie.style.display = 'none'
  const selfie = q('selfie'); if (selfie) { selfie.style.display = 'none'; selfie.required = false }
  const selfieNote = q('selfie-note'); if (selfieNote) selfieNote.style.display = 'none'
  const heroSelfie = q('user-selfie-hero'); if (heroSelfie) heroSelfie.style.display = 'none'
  const waiterDisco = q('waiter-reasons-disco'); if (waiterDisco) waiterDisco.style.display = 'none'
  const waiterRest = q('waiter-reasons-restaurant'); if (waiterRest) waiterRest.style.display = ''
  const invitesTitle = q('user-invites-title'); if (invitesTitle) invitesTitle.style.display = 'none'
  const invitesList = q('user-invites'); if (invitesList) invitesList.style.display = 'none'
  const invitesInbox = q('screen-invites-inbox'); if (invitesInbox) invitesInbox.style.display = 'none'
  setTxt('#user-venue-footer-label', 'Disfruta de la mejor comida con')
  setTxt('#nav-carta span', 'Menú')
  setTxt('#nav-disponibles span', 'Promos')
  setTxt('#nav-orders span', 'Órdenes')
  setTxt('#home-hero-main', 'Disfruta tu experiencia')
  setTxt('#home-hero-sub', 'Pide, consulta tu cuenta y llama al mesero')
  const hideIds = ['home-availability-title', 'home-availability-row', 'home-end-dance-row', 'tip-refresh-bailar', 'btn-end-dance', 'screen-disponibles-select', 'screen-disponibles', 'screen-meeting', 'screen-user-invite', 'screen-invite-received', 'screen-mesas', 'screen-dj-request', 'fab-call', 'fab-call-label', 'tab-staff-dj', 'staff-dj-content', 'btn-copy-link-dj', 'catalog-tip', 'catalog-product-row', 'catalog-qty-row', 'catalog-add-row']
  for (const id of hideIds) { const el = q(id); if (el) el.style.display = 'none' }
}
function applyDiscoMode() {
  const setTxt = (sel, txt) => { const el = document.querySelector(sel); if (el) el.textContent = txt }
  if (document && document.body) document.body.dataset.venue = ''
  document.title = 'Discos'
  setTxt('#welcome-title', 'Discos')
  setTxt('#welcome-subtitle', 'Conecta, baila y comparte consumos — seguro y sin fricción.')
  setTxt('#venue-type-title', 'Selecciona el tipo de venue')
  setTxt('#staff-welcome-title', 'Ingreso Staff')
  setTxt('#staff-title', 'Discoteca — Panel de órdenes')
  setTxt('#staff-panel-title', 'Escanéame para unirte a la fiesta')
  setTxt('#tab-staff-catalog', 'Carta')
  setTxt('#menu-staff-catalog', 'Carta')
  const search = q('catalog-search'); if (search) search.placeholder = 'Buscar en carta'
  const lblGender = q('label-gender'); if (lblGender) lblGender.style.display = ''
  const gender = q('profile-gender'); if (gender) { gender.style.display = ''; gender.required = true }
  const lblSelfie = q('label-selfie'); if (lblSelfie) lblSelfie.style.display = ''
  const selfie = q('selfie'); if (selfie) { selfie.style.display = ''; selfie.required = true }
  const selfieNote = q('selfie-note'); if (selfieNote) selfieNote.style.display = ''
  const heroSelfie = q('user-selfie-hero'); if (heroSelfie) heroSelfie.style.display = ''
  const waiterDisco = q('waiter-reasons-disco'); if (waiterDisco) waiterDisco.style.display = ''
  const waiterRest = q('waiter-reasons-restaurant'); if (waiterRest) waiterRest.style.display = 'none'
  const invitesTitle = q('user-invites-title'); if (invitesTitle) invitesTitle.style.display = ''
  const invitesList = q('user-invites'); if (invitesList) invitesList.style.display = ''
  const invitesInbox = q('screen-invites-inbox'); if (invitesInbox) invitesInbox.style.display = ''
  setTxt('#user-venue-footer-label', 'Disfruta esta noche con')
  setTxt('#nav-carta span', 'Carta')
  setTxt('#nav-disponibles span', 'Bailar')
  setTxt('#nav-orders span', 'Órdenes')
  setTxt('#home-hero-main', 'Activa tu modo fiesta')
  setTxt('#home-hero-sub', 'Hazte visible y súmate al baile ahora')
  const showIds = ['home-availability-title', 'home-availability-row', 'home-end-dance-row', 'screen-disponibles-select', 'screen-disponibles', 'screen-meeting', 'screen-user-invite', 'screen-invite-received', 'screen-mesas', 'screen-dj-request', 'fab-call', 'fab-call-label', 'tab-staff-dj', 'staff-dj-content', 'btn-copy-link-dj', 'catalog-tip', 'catalog-product-row', 'catalog-qty-row', 'catalog-add-row']
  for (const id of showIds) { const el = q(id); if (el) el.style.display = '' }
}
function chooseVenueMode(mode) {
  if (mode === 'restaurant') {
    S.appMode = 'restaurant'
    applyRestaurantMode()
  } else {
    S.appMode = ''
    applyDiscoMode()
  }
  setModeInUrl(mode)
  show('screen-staff-welcome')
}
function show(id) {
  maybeAutoCancelMeetingOnLeave(id)
  for (const el of document.querySelectorAll('.screen')) el.classList.remove('active')
  q(id).classList.add('active')
  if (S.nav && S.nav.current && S.nav.current !== id) S.nav.history.push(S.nav.current)
  S.nav.current = id
  try { localStorage.setItem('discos_last_view', id) } catch {}
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
  if (fab) fab.style.display = (isStaffView || isRestaurantMode()) ? 'none' : ''
  if (fabLabel) fabLabel.style.display = (isStaffView || isRestaurantMode()) ? 'none' : ''
  if (isStaffView) { renderVenueTitle() }
  if (id !== 'screen-dj-request') {
    try { if (S.timers.djUserCountdown) { clearInterval(S.timers.djUserCountdown); S.timers.djUserCountdown = 0 } } catch {}
  }
}
function maybeAutoCancelMeetingOnLeave(nextId) {
  try {
    if (S.nav && S.nav.current === 'screen-meeting' && nextId !== 'screen-meeting' && S.meeting && S.isMeetingReceiver && String(S.user?.danceState || '') === 'waiting') {
      const meetingId = S.meeting.id
      S.meeting = null
      S.meetingPlan = ''
      S.isMeetingReceiver = false
      S.inInviteFlow = false
      if (S.user) { S.user.danceState = 'idle'; S.user.dancePartnerId = ''; S.user.meetingId = '' }
      showError('Encuentro cancelado')
      setTimeout(() => showError(''), 1200)
      api('/api/meeting/cancel', { method: 'POST', body: JSON.stringify({ meetingId }) }).catch(() => {})
      scheduleRefreshDanceList()
    }
  } catch {}
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
  try { localStorage.setItem('discos_last_user_tab', tab) } catch {}
}
function setActiveNavByScreen(screenId) {
  const reverse = { 'screen-consumption': 'carta', 'screen-disponibles-select': 'disponibles', 'screen-disponibles': 'disponibles', 'screen-mesas': 'mesas', 'screen-call-waiter': 'mesas', 'screen-orders-user': 'orders', 'screen-edit-profile': 'perfil' }
  if (isRestaurantMode()) {
    if (screenId === 'screen-promos') { setActiveNav('disponibles'); return }
    if (screenId === 'screen-disponibles' || screenId === 'screen-disponibles-select' || screenId === 'screen-mesas') return
  }
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

function restoreLastView() {
  try {
    const lastView = (() => { try { return localStorage.getItem('discos_last_view') || '' } catch { return '' } })()
    const lastUserTab = (() => { try { return localStorage.getItem('discos_last_user_tab') || '' } catch { return '' } })()
    const lastStaffTab = (() => { try { return localStorage.getItem('discos_last_staff_tab') || '' } catch { return '' } })()
    if (S.role === 'user') {
      if (isRestaurantMode()) {
        const blocked = ['screen-disponibles', 'screen-disponibles-select', 'screen-mesas', 'screen-meeting', 'screen-user-invite', 'screen-invite-received', 'screen-dj-request']
        if (blocked.includes(lastView)) {
          show('screen-user-home')
          return
        }
      }
      if (lastView && lastView !== 'screen-welcome') {
        show(lastView)
        if (lastView === 'screen-dj-request') startDJUserCountdown()
        else if (lastView === 'screen-orders-user') { loadUserOrders(); loadUserInvitesHistory() }
        else if (lastView === 'screen-disponibles' || lastView === 'screen-disponibles-select') scheduleRefreshAvailableList()
        else if (lastView === 'screen-mesas') exploreMesas()
      } else if (lastUserTab) {
        setActiveNav(lastUserTab)
      }
    } else if (S.role === 'staff') {
      show('screen-staff')
      const tab = lastStaffTab || 'panel'
      showStaffTab(tab)
    }
  } catch {}
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
  if (S.djOnly && tab !== 'dj') tab = 'dj'
  const contentMap = {
    panel: 'staff-panel-content',
    orders: 'staff-orders-content',
    mesas: 'staff-mesas-content',
    users: 'staff-users-content',
    waiter: 'staff-waiter-content',
    reportes: 'staff-reportes-content',
    promos: 'staff-promos-content',
    catalog: 'staff-catalog-content',
    analytics: 'staff-analytics-content',
    dj: 'staff-dj-content',
  }
  for (const el of document.querySelectorAll('#staff-content .section')) el.classList.remove('active')
  const id = contentMap[tab]
  if (id) { const el = q(id); if (el) el.classList.add('active') }
  for (const el of document.querySelectorAll('#staff-tabs .tab-item')) el.classList.remove('active')
  const tabMap = {
    panel: 'tab-staff-panel', orders: 'tab-staff-orders', mesas: 'tab-staff-mesas', users: 'tab-staff-users',
    waiter: 'tab-staff-waiter', reportes: 'tab-staff-reportes', promos: 'tab-staff-promos', catalog: 'tab-staff-catalog', dj: 'tab-staff-dj'
  }
  const tId = tabMap[tab]; if (tId) { const el = q(tId); if (el) el.classList.add('active') }
  applyDjOnlyUI()
  for (const el of document.querySelectorAll('#staff-menu .menu-item')) el.classList.remove('active')
  const menuMap = {
    panel: 'menu-staff-panel', orders: 'menu-staff-orders', mesas: 'menu-staff-mesas', users: 'menu-staff-users',
    waiter: 'menu-staff-waiter', reportes: 'menu-staff-reportes', promos: 'menu-staff-promos', catalog: 'menu-staff-catalog'
  }
  const mId = menuMap[tab]; if (mId) { const el = q(mId); if (el) el.classList.add('active') }
  if (tab === 'orders') loadOrders(q('staff-orders-filter')?.value || '')
  else if (tab === 'users') loadUsers()
  else if (tab === 'waiter') loadWaiterCalls()
  else if (tab === 'reportes') loadReports()
  else if (tab === 'promos') loadStaffPromos()
  else if (tab === 'catalog') loadStaffCatalogEditor()
  else if (tab === 'panel') loadSessionInfo()
  else if (tab === 'analytics') loadAnalytics()
  else if (tab === 'dj') loadDJRequests()
  S.staffTab = tab
  try { localStorage.setItem('discos_last_staff_tab', tab) } catch {}
}
function applyDjOnlyUI() {
  const djOnly = !!S.djOnly
  const menu = q('staff-menu'); if (menu) menu.style.display = djOnly ? 'none' : ''
  const btnAnalytics = q('btn-staff-analytics'); if (btnAnalytics) btnAnalytics.style.display = djOnly ? 'none' : ''
  for (const el of document.querySelectorAll('#staff-tabs .tab-item')) {
    if (!djOnly) el.style.display = ''
    else el.style.display = (el.id === 'tab-staff-dj') ? '' : 'none'
  }
}

async function loadSessionInfo() {
  try {
    let pin = ''
    try {
      const mode = isRestaurantMode() ? 'restaurant' : 'disco'
      const qv = S.venueId ? ('venueId=' + encodeURIComponent(S.venueId) + '&') : ''
      const r = await api(`/api/session/active?${qv}mode=${encodeURIComponent(mode)}`)
      pin = r.pin || ''
      if (r.mode) applyMode(r.mode)
    } catch {}
    let baseCandidate = ''
    try {
      const pb = await api(`/api/session/public-base?sessionId=${encodeURIComponent(S.sessionId)}`)
      baseCandidate = (pb.publicBaseUrl || '').trim()
      const inp = q('public-base'); if (inp && baseCandidate) inp.value = baseCandidate
    } catch {}
    let base = baseCandidate || location.origin
    const modeQuery = isRestaurantMode() ? '&mode=restaurant' : ''
    const url = `${base}/?venueId=${encodeURIComponent(S.venueId || 'default')}&aj=1${modeQuery}`
    const pd = q('pin-display'); if (pd) pd.textContent = pin
    const qrImg = q('qr-session'); if (qrImg) qrImg.src = `https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=${encodeURIComponent(url)}`
    const share = q('share-url'); if (share) { share.href = url; share.title = url }
    renderVenueTitle()
  } catch {}
}

async function renderVenueTitle() {
  try {
    const mode = isRestaurantMode() ? 'restaurant' : 'disco'
    const qv = S.venueId ? ('venueId=' + encodeURIComponent(S.venueId) + '&') : ''
    const sess = await api(`/api/session/active?${qv}mode=${encodeURIComponent(mode)}`)
    const el = q('staff-title')
    if (el) {
      const modeLabel = mode === 'restaurant' ? 'Restaurante' : 'Discoteca'
      const venueLabel = sess.venueName || S.venueId || ''
      if (S.djOnly) el.textContent = `${modeLabel} — Panel DJ ${venueLabel}`
      else el.textContent = `${modeLabel} — Panel de órdenes ${venueLabel}`
    }
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
async function apiAdmin(path, secret, opts = {}) {
  const headers = { 'Content-Type': 'application/json', 'X-Admin-Secret': String(secret || '') }
  const res = await fetch(path, { method: 'GET', ...opts, headers })
  let data = null
  try { data = await res.json() } catch {}
  if (!res.ok) {
    const msg = data && data.error ? data.error : 'api_admin'
    throw new Error(msg)
  }
  return data
}
async function apiStaff(path, secret, opts = {}) {
  const headers = { 'Content-Type': 'application/json', 'X-Staff-Secret': String(secret || '') }
  const res = await fetch(path, { method: 'GET', ...opts, headers })
  let data = null
  try { data = await res.json() } catch {}
  if (!res.ok) {
    const msg = data && data.error ? [
      data.error,
      data.provider ? String(data.provider) : '',
      data.status != null ? String(data.status) : '',
      data.message ? String(data.message) : ''
    ].filter(Boolean).join(':') : 'api_staff'
    throw new Error(msg)
  }
  return data
}
async function sendVenuePinAdminWelcome() {
  try {
    const secret = q('admin-secret-welcome')?.value.trim() || '2207'
    let venueId = 'default'
    try {
      const u = new URL(location.href)
      const v = u.searchParams.get('venueId') || ''
      if (v) venueId = v
    } catch {}
    await apiStaff('/api/admin/venues/pin/send', secret, { method: 'POST', body: JSON.stringify({ venueId }) })
    showError('PIN enviado al email')
    setTimeout(() => showError(''), 1000)
  } catch (e) { showError(String(e.message)) }
}

function showModal(title, msg, type = 'info') {
  const m = q('modal')
  const t = q('modal-text')
  const tt = q('modal-title')
  if (!m || !t || !tt) return
  if (document.hidden || S.appHidden) {
    const txt = [title, msg].filter(Boolean).join(': ')
    if (txt) {
      S.missed = Array.isArray(S.missed) ? S.missed : []
      S.missed.push(txt)
    }
    return
  }
  try { const btn = q('modal-action'); if (btn) btn.remove() } catch {}
  try { const inp = q('modal-input'); if (inp) inp.remove() } catch {}
  const row = document.querySelector('#modal .row')
  if (row) {
    try {
      for (const child of Array.from(row.children)) {
        if (child.id !== 'modal-close') child.remove()
      }
    } catch {}
    let closeBtn = q('modal-close')
    if (!closeBtn) {
      closeBtn = document.createElement('button')
      closeBtn.id = 'modal-close'
      closeBtn.className = 'secondary'
      closeBtn.textContent = 'Cerrar'
      closeBtn.onclick = () => {
        const mm = q('modal'); if (mm) mm.classList.remove('show')
        if (S.modalKind === 'invite') {
          if (S.consumptionReq) {
            S.invitesQueue.push({ type: 'consumption', data: S.consumptionReq })
          } else if (S.currentInvite && S.currentInvite.id) {
            const cur = S.currentInvite
            S.invitesQueue.push({ type: 'dance', id: cur.id, invite: { id: cur.id, from: cur.from, expiresAt: Number(cur.expiresAt || 0) } })
          }
          S.notifications.invites = (S.notifications.invites || 0) + 1
          setBadgeNav('disponibles', S.notifications.invites)
          S.inInviteFlow = false
          S.modalKind = ''
          showNextInvite()
        } else {
          S.modalKind = ''
        }
      }
      row.append(closeBtn)
    }
  }
  m.classList.remove('type-error', 'type-success', 'type-info')
  m.classList.add('type-' + type)
  tt.textContent = title || ''
  tt.style.display = title ? '' : 'none'
  t.textContent = msg || ''
  m.classList.add('show')
  S.modalShownAt = Date.now()
  if (S.timers.modalHide) { try { clearTimeout(S.timers.modalHide) } catch {}; S.timers.modalHide = 0 }
}
function showError(msg) {
  if (!msg) {
    const ttl = Number(S.messageTTL || 0)
    const since = Date.now() - Number(S.modalShownAt || 0)
    if (ttl > 0 && since < ttl) {
      const rem = ttl - since
      if (S.timers.modalHide) { try { clearTimeout(S.timers.modalHide) } catch {} }
      S.timers.modalHide = setTimeout(() => { const m = q('modal'); if (m) m.classList.remove('show'); S.timers.modalHide = 0 }, rem)
      return
    }
    const m = q('modal'); if (m) m.classList.remove('show'); return
  }
  showModal('', msg || '', 'error')
}
function showInfo(msg) { showModal('Info', msg || '', 'info') }
function showSuccess(msg) { showModal('Listo', msg || '', 'success') }
function showImageModal(url) {
  showModal('', '', 'info')
  const t = q('modal-text')
  if (!t) return
  try { t.innerHTML = '' } catch {}
  const img = document.createElement('img')
  img.src = url || ''
  img.style.display = 'block'
  img.style.width = '50%'
  img.style.height = 'auto'
  img.style.maxHeight = '40vh'
  img.style.margin = '0 auto'
  img.style.borderRadius = '12px'
  img.style.border = '1px solid #333'
  t.append(img)
}
function openInviteModal(expiresAt) {
  S.modalKind = 'invite'
  const isConsumption = !!S.consumptionReq
  showModal(isConsumption ? 'Invitación de consumo' : '', '', 'info')
  const t = q('modal-text')
  const row = document.querySelector('#modal .row')
  if (!t || !row) return
  try { t.innerHTML = '' } catch {}
  try { row.innerHTML = '' } catch {}
  if (isConsumption) {
    const data = S.consumptionReq || {}
    const box = document.createElement('div')
    box.className = 'ticket'
    const head = document.createElement('div')
    head.className = 'ticket-head'
    const alias = (data.from && (data.from.alias || data.from.id)) ? (data.from.alias || data.from.id) : ''
    const mesaTxt = (data.from && data.from.tableId) ? ` • Mesa ${data.from.tableId}` : ''
    head.textContent = alias ? `De ${alias}${mesaTxt}` : ''
    const exp = document.createElement('div')
    exp.id = 'consume-exp-text'
    const tgt = Number(expiresAt || data.expiresAt || 0)
    exp.textContent = tgt ? '' : ''
    box.append(head)
    box.append(exp)
    const list = document.createElement('div')
    list.className = 'ticket-items'
    if (Array.isArray(data.items) && data.items.length) {
      for (const it of data.items) {
        const li = document.createElement('div')
        li.className = 'ticket-item'
        li.textContent = `${it.quantity} x ${it.product}`
        list.append(li)
      }
    } else {
      const qty = Math.max(1, Number(data.quantity || 1))
      const li = document.createElement('div')
      li.className = 'ticket-item'
      li.textContent = `${qty} x ${data.product || ''}`
      list.append(li)
    }
    t.append(box)
    t.append(list)
    const bA = document.createElement('button')
    bA.id = 'modal-btn-invite-accept'
    bA.className = 'success'
    bA.textContent = 'Aceptar'
    bA.onclick = () => { try { const m = q('modal'); if (m) m.classList.remove('show') } catch {}; respondInvite(true) }
    const bP = document.createElement('button')
    bP.id = 'modal-btn-invite-pass'
    bP.className = 'warning'
    bP.textContent = 'Pasar'
    bP.onclick = () => { try { const m = q('modal'); if (m) m.classList.remove('show') } catch {}; respondInvite(false) }
    row.append(bA, bP)
    startInviteCountdown(tgt)
  } else {
    const wrap = document.createElement('div')
    wrap.className = 'ring-wrap'
    const selfie = (S.currentInvite && S.currentInvite.from && S.currentInvite.from.selfie) ? S.currentInvite.from.selfie
                 : (S.consumptionReq && S.consumptionReq.from && S.consumptionReq.from.selfie) ? S.consumptionReq.from.selfie
                 : ''
    if (selfie) {
      const pic = document.createElement('img')
      pic.id = 'invite-ring-selfie'
      pic.src = selfie
      wrap.append(pic)
    }
    const ring = document.createElement('div')
    ring.id = 'invite-ring-modal'
    ring.className = 'ring-overlay'
    const txt = document.createElement('span')
    txt.id = 'invite-ring-modal-txt'
    wrap.append(ring)
    wrap.append(txt)
    t.append(wrap)
    const bA = document.createElement('button')
    bA.id = 'modal-btn-invite-accept'
    bA.className = 'success'
    bA.textContent = 'Aceptar'
    bA.onclick = () => { try { const m = q('modal'); if (m) m.classList.remove('show') } catch {}; respondInvite(true) }
    const bP = document.createElement('button')
    bP.id = 'modal-btn-invite-pass'
    bP.className = 'warning'
    bP.textContent = 'Pasar'
    bP.onclick = () => { try { const m = q('modal'); if (m) m.classList.remove('show') } catch {}; respondInvite(false) }
    row.append(bA, bP)
    startInviteCountdown(expiresAt)
  }
}
function openInviteWaitModal(expiresAt, target) {
  S.modalKind = 'wait_invite'
  showModal('Invitación enviada', '', 'info')
  const t = q('modal-text')
  const row = document.querySelector('#modal .row')
  if (!t || !row) return
  try { t.innerHTML = '' } catch {}
  try { row.innerHTML = '' } catch {}
  const wrap = document.createElement('div')
  wrap.className = 'ring-wrap'
  const selfie = (target && target.selfie) ? target.selfie : ''
  if (selfie) {
    const pic = document.createElement('img')
    pic.id = 'invite-ring-selfie'
    pic.src = selfie
    wrap.append(pic)
  }
  const ring = document.createElement('div')
  ring.id = 'invite-ring-modal'
  ring.className = 'ring-overlay'
  const txt = document.createElement('span')
  txt.id = 'invite-ring-modal-txt'
  wrap.append(ring)
  wrap.append(txt)
  t.append(wrap)
  startInviteCountdown(expiresAt)
}
function ensureAudio() {
  try {
    if (!S.audioCtx) {
      const Ctx = window.AudioContext || window.webkitAudioContext
      if (Ctx) S.audioCtx = new Ctx()
    }
    if (S.audioCtx && S.audioCtx.state === 'suspended') { S.audioCtx.resume().catch(()=>{}) }
  } catch {}
}
function playNotify(kind = 'short') {
  if (document.hidden || S.appHidden) return
  try { if (navigator && navigator.vibrate) {
    if (kind === 'dance') navigator.vibrate([140,40,140])
    else if (kind === 'consumption') navigator.vibrate([80,30,80,30,80])
    else navigator.vibrate(120)
  } } catch {}
  try {
    ensureAudio()
    if (!S.audioCtx) return
    const osc = S.audioCtx.createOscillator()
    const gain = S.audioCtx.createGain()
    osc.type = 'sine'
    osc.frequency.setValueAtTime(kind === 'dance' ? 880 : kind === 'consumption' ? 660 : 740, S.audioCtx.currentTime)
    gain.gain.setValueAtTime(0.0001, S.audioCtx.currentTime)
    gain.gain.exponentialRampToValueAtTime(0.2, S.audioCtx.currentTime + 0.02)
    gain.gain.exponentialRampToValueAtTime(0.0001, S.audioCtx.currentTime + (kind === 'consumption' ? 0.24 : 0.16))
    osc.connect(gain); gain.connect(S.audioCtx.destination)
    osc.start()
    osc.stop(S.audioCtx.currentTime + (kind === 'consumption' ? 0.26 : 0.18))
  } catch {}
}
function toggleAnalytics() {
  const cur = S.staffTab || 'panel'
  if (cur === 'analytics') {
    const back = S.lastStaffTab && S.lastStaffTab !== 'analytics' ? S.lastStaffTab : 'panel'
    showStaffTab(back)
  } else {
    S.lastStaffTab = cur
    showStaffTab('analytics')
  }
}
function openMenuMoreModal() {
  showModal('Más', '', 'info')
  const t = q('modal-text')
  if (!t) return
  try { t.innerHTML = '' } catch {}
  const list = document.createElement('div')
  list.className = 'menu modal-grid'
  const mk = (tab, label) => {
    const b = document.createElement('button')
    b.className = 'menu-item'
    b.textContent = label
    b.onclick = () => {
      showStaffTab(tab)
      const m = q('modal'); if (m) m.classList.remove('show')
    }
    return b
  }
  list.append(
    mk('mesas', 'Mesas'),
    mk('users', 'Usuarios'),
    mk('reportes', 'Reportes'),
    mk('promos', 'Promos'),
    mk('catalog', 'Carta'),
  )
  t.append(list)
}
function showModalAction(title, msg, btnText, handler, type = 'info') {
  showModal(title, msg, type)
  const row = document.querySelector('#modal .row')
  if (!row) return
  let btn = q('modal-action')
  if (btn) { try { btn.remove() } catch {} }
  btn = document.createElement('button')
  btn.id = 'modal-action'
  btn.className = 'success'
  btn.textContent = btnText || 'Aceptar'
  btn.onclick = () => { try { const m = q('modal'); if (m) m.classList.remove('show') } catch {}; if (typeof handler === 'function') handler() }
  const closeBtn = q('modal-close')
  if (closeBtn && closeBtn.parentElement === row) row.insertBefore(btn, closeBtn)
  else row.append(btn)
}
async function confirmAction(paraphrase) {
  return await new Promise(resolve => {
    const m = q('modal')
    const closeBtn = q('modal-close')
    const onCancel = () => { try { if (m) m.classList.remove('show') } catch {}; cleanup(); resolve(false) }
    const onEsc = (e) => { if (e.key === 'Escape') { onCancel() } }
    const onOverlay = (e) => { if (e.target && e.target.id === 'modal') { onCancel() } }
    const cleanup = () => {
      try { document.removeEventListener('keydown', onEsc) } catch {}
      try { if (m) m.removeEventListener('click', onOverlay) } catch {}
      try { if (closeBtn) closeBtn.onclick = () => { const mm = q('modal'); if (mm) mm.classList.remove('show') } } catch {}
    }
    showModalAction('Confirmar', paraphrase || '', 'Aceptar', () => { cleanup(); resolve(true) }, 'info')
    try { document.addEventListener('keydown', onEsc) } catch {}
    try { if (m) m.addEventListener('click', onOverlay) } catch {}
    try { if (closeBtn) closeBtn.onclick = onCancel } catch {}
  })
}
async function promptInput(title, placeholder = '') {
  return await new Promise(resolve => {
    showModal(title || '', '', 'info')
    const t = q('modal-text')
    const row = document.querySelector('#modal .row')
    const closeBtn = q('modal-close')
    if (!t || !row || !closeBtn) { resolve(''); return }
    const inp = document.createElement('input')
    inp.id = 'modal-input'
    inp.type = 'text'
    inp.placeholder = placeholder || ''
    inp.style.width = '100%'
    t.innerHTML = ''
    t.appendChild(inp)
    let btn = q('modal-action')
    if (btn) { try { btn.remove() } catch {} }
    btn = document.createElement('button')
    btn.id = 'modal-action'
    btn.className = 'success'
    btn.textContent = 'Aceptar'
    btn.onclick = () => { try { const m = q('modal'); if (m) m.classList.remove('show') } catch {}; resolve(String(inp.value || '').trim()) }
    if (closeBtn && closeBtn.parentElement === row) row.insertBefore(btn, closeBtn)
    else row.append(btn)
    inp.focus()
    const onEnter = (e) => { if (e.key === 'Enter') { btn.click() } }
    inp.addEventListener('keydown', onEnter)
    closeBtn.onclick = () => { try { const m = q('modal'); if (m) m.classList.remove('show') } catch {}; resolve('') }
  })
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
  if (!S.sessionId) return
  if (S.timers.staffReconnect) return
  S.timers.staffReconnect = setTimeout(() => { S.timers.staffReconnect = 0; startStaffEvents() }, 3000)
}
function scheduleCatalogSave() {
  if (S.timers.catalogSave) { try { clearTimeout(S.timers.catalogSave) } catch {} }
  S.timers.catalogSave = setTimeout(async () => { S.timers.catalogSave = 0; try { await saveStaffCatalog() } catch {} }, 800)
}
function scheduleLater(key, fn, wait = 500) {
  S.sched = S.sched || {}
  if (S.sched[key]) { try { clearTimeout(S.sched[key]) } catch {} }
  S.sched[key] = setTimeout(async () => {
    S.sched[key] = 0
    const lk = `_${key}_loading`
    S.loading = S.loading || {}
    if (S.loading[lk]) { scheduleLater(key, fn, wait); return }
    S.loading[lk] = true
    try { await fn() } catch {}
    S.loading[lk] = false
  }, wait)
}
function buildSig(list, mapFn) {
  try { return (list || []).map(mapFn).join('||') } catch { return '' }
}
function scheduleStaffOrdersUpdate() {
  scheduleLater('staff_orders', async () => {
    S.ui = S.ui || {}
    if (S.ui.freezeStaffOrders) { scheduleStaffOrdersUpdate(); return }
    const cur = q('staff-orders-filter')?.value || ''
    await loadOrders(cur)
    await loadAnalytics()
  }, 500)
}
function scheduleStaffUsersUpdate() { scheduleLater('staff_users', async () => { await loadUsers() }, 500) }
function scheduleStaffWaiterUpdate() { scheduleLater('staff_waiter', async () => { await loadWaiterCalls(); await loadAnalytics() }, 500) }
function scheduleStaffReportsUpdate() { scheduleLater('staff_reports', async () => { await loadReports(); await loadAnalytics() }, 500) }
function scheduleStaffAnalyticsUpdate() { scheduleLater('staff_analytics', async () => { await loadAnalytics() }, 500) }
function scheduleUserOrdersUpdate() { scheduleLater('user_orders', async () => { await loadUserOrders() }, 400) }
function scheduleRenderUserHeader() { scheduleLater('user_header', async () => { renderUserHeader() }, 300) }
function scheduleRefreshAvailableList() { scheduleLater('user_avail', async () => { await refreshAvailableList() }, 500) }
function scheduleStaffDJUpdate() { scheduleLater('staff_dj', async () => { await loadDJRequests() }, 400) }
async function loadI18n() {
  try {
    const r = await fetch(`/i18n/es.json`)
    if (r.ok) { S.i18n = await r.json(); return }
  } catch {}
  S.i18n = {}
}
function t(key) {
  const m = S.i18n || {}
  return m[key] || key
}
function genderLabel(code) {
  const v = String(code || '').toLowerCase()
  if (v === 'm') return t('gender_male')
  if (v === 'f') return t('gender_female')
  if (v === 'o') return t('gender_other')
  if (v === 'na') return t('gender_na')
  return ''
}
function renderGenderSelect() {
  const sel = q('profile-gender')
  if (!sel) return
  sel.innerHTML = ''
  const opt0 = document.createElement('option')
  opt0.value = ''
  opt0.textContent = t('gender_placeholder')
  opt0.disabled = true
  opt0.selected = true
  const mk = (val, label) => {
    const o = document.createElement('option')
    o.value = val
    o.textContent = label
    return o
  }
  sel.append(opt0, mk('m', t('gender_male')), mk('f', t('gender_female')), mk('o', t('gender_other')), mk('na', t('gender_na')))
  const cur = (S.user && S.user.prefs && S.user.prefs.gender) ? S.user.prefs.gender : ''
  if (cur) sel.value = cur
  const lbl = q('label-gender')
  if (lbl) lbl.textContent = t('gender_label')
}
function startUserPolls() {
  if (S.timers.userPoll) { try { clearInterval(S.timers.userPoll) } catch {} }
  const wait = S.sseReady ? 15000 : 8000
  S.timers.userPoll = setInterval(() => {
    if (document.hidden) return
    if (S.user && S.user.available && S.nav.current === 'screen-disponibles') scheduleRefreshAvailableList()
    if (S.nav.current === 'screen-orders-user') { scheduleUserOrdersUpdate(); scheduleLater('user_invites', async () => { await loadUserInvitesHistory() }, 600) }
    if (S.nav.current === 'screen-user-home') scheduleRenderUserHeader()
  }, wait)
}
function startStaffPolls() {
  if (S.timers.staffPoll) { try { clearInterval(S.timers.staffPoll) } catch {} }
  const wait = S.staffSseReady ? 15000 : 8000
  S.timers.staffPoll = setInterval(() => {
    if (document.hidden) return
    if (S.nav.current === 'screen-staff') { scheduleStaffUsersUpdate(); scheduleStaffOrdersUpdate(); scheduleStaffWaiterUpdate(); scheduleStaffReportsUpdate(); scheduleStaffAnalyticsUpdate() }
  }, wait)
}

async function join(role, codeOverride = '', pinOverride = '') {
  try {
    showError('')
    const code = codeOverride || (q('join-code') ? q('join-code').value.trim() : '')
    const pin = pinOverride || (q('join-pin') ? q('join-pin').value.trim() : '')
    if (!code) { showError('Ingresa el código de sesión'); return }
    if (role === 'staff' && !pin) { showError('Ingresa el PIN de sesión'); return }
    let alias = ''
    if (role === 'user') {
      try {
        const u = new URL(location.href)
        if (u.searchParams.get('dj') === '1') { showError('Acceso DJ solo staff'); return }
      } catch {}
      const venueKey = makeLocalKey(S.venueId || 'default', getCurrentModeKey())
      let local = null
      try {
        const m = getLocalUsers()
        if (m && m[venueKey]) local = m[venueKey]
      } catch {}
      if (local && local.role === 'user' && local.userId && local.sessionId && local.sessionId === code) {
        const existing = await api(`/api/user/get?userId=${encodeURIComponent(local.userId)}`).catch(() => null)
        if (existing && existing.user) {
          S.user = existing.user
          S.role = 'user'
          S.sessionId = existing.user.sessionId
          await syncSessionMode(S.sessionId)
          try { saveLocalUser() } catch {}
          startEvents()
          show('screen-user-home')
          showError('Ya estás registrado en esta sesión')
          setTimeout(() => showError(''), 1200)
          return
        }
      }
      alias = (q('alias') ? q('alias').value.trim() : '')
      if (!alias) {
        alias = await promptInput('Ingresa tu alias', 'Tu alias')
      }
      if (!alias) { showError('Ingresa tu alias'); return }
    }
    S.sessionId = code
    let r = null
    try {
      r = await api('/api/join', { method: 'POST', body: JSON.stringify({ sessionId: code, role, pin, alias }) })
    } catch (e) {
      if (role === 'user' && String(e.message) === 'no_session') {
        let active = null
        const activeMode = getModeFromUrl() || (isRestaurantMode() ? 'restaurant' : 'disco')
        const qv = S.venueId ? ('venueId=' + encodeURIComponent(S.venueId) + '&') : ''
        try { active = await api(`/api/session/active?${qv}mode=${encodeURIComponent(activeMode === 'restaurant' ? 'restaurant' : 'disco')}`) } catch {}
        if (active && active.sessionId) {
          S.sessionId = active.sessionId
          r = await api('/api/join', { method: 'POST', body: JSON.stringify({ sessionId: active.sessionId, role, pin: '', alias }) })
        } else {
          showError('Sin sesión activa para este local'); return
        }
      } else {
        throw e
      }
    }
    S.user = r.user
    S.role = role
    await syncSessionMode(S.sessionId)
    try { saveLocalUser() } catch {}
    if (role === 'user') {
      const aliasInput = q('alias'); if (aliasInput) aliasInput.value = S.user.alias || alias
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
      if (S.autoStaffTab) { showStaffTab(S.autoStaffTab); S.autoStaffTab = '' }
    }
    startEvents()
  } catch (e) {
    showError(String(e.message))
  }
}

async function saveProfile() {
  const alias = q('alias').value.trim()
  const tableId = (q('profile-table') ? q('profile-table').value.trim() : '')
  const gender = (q('profile-gender') ? q('profile-gender').value : '')
  const file = q('selfie').files[0]
  const isRestaurant = isRestaurantMode()
  let selfie = ''
  if (file) {
    selfie = await processSelfie(file).catch(() => '')
    if (!selfie) { showError('Selfie inválida o muy grande'); setTimeout(() => showError(''), 1400); return }
  }
  if (!alias) { showError('Ingresa tu alias'); setTimeout(() => showError(''), 1200); return }
  if (!isRestaurant && !gender) { showError(t('gender_required')); setTimeout(() => showError(''), 1200); return }
  if (!tableId) { showError('Ingresa tu mesa'); setTimeout(() => showError(''), 1200); return }
  if (!isRestaurant && !file) { showError('Debes subir tu selfie'); setTimeout(() => showError(''), 1400); return }
  if (isRestaurant) {
    await api('/api/user/update', { method: 'POST', body: JSON.stringify({ userId: S.user.id, alias, tableId }) })
  } else {
    await api('/api/user/profile', { method: 'POST', body: JSON.stringify({ userId: S.user.id, alias, selfie, gender }) })
    await api('/api/user/change-table', { method: 'POST', body: JSON.stringify({ userId: S.user.id, newTable: tableId }) })
  }
  S.user.alias = alias
  if (selfie) S.user.selfie = selfie
  S.user.tableId = tableId
  if (!S.user.prefs) S.user.prefs = {}
  if (!isRestaurant) S.user.prefs.gender = gender
  try { saveLocalUser() } catch {}
  const selfieNote = q('selfie-note'); if (selfieNote) selfieNote.textContent = selfie ? 'Selfie cargada' : ''
  const ua = q('user-alias'), us = q('user-selfie'); if (ua) ua.textContent = S.user.alias || S.user.id; if (us) us.src = S.user.selfie || ''
  const ut = q('user-table'); if (ut) ut.textContent = S.user.tableId || '-'
  show('screen-user-home')
}

function dataUrlBytes(d) {
  const m = String(d || '').match(/^data:.*;base64,(.+)$/)
  return m ? Math.floor(m[1].length * 3 / 4) : 0
}
async function loadImageFromFile(file) {
  if (window && window.createImageBitmap) {
    try { return await createImageBitmap(file) } catch {}
  }
  try {
    return await new Promise((resolve, reject) => {
      const img = new Image()
      const url = URL.createObjectURL(file)
      img.onload = () => { try { URL.revokeObjectURL(url) } catch {}; resolve(img) }
      img.onerror = () => { try { URL.revokeObjectURL(url) } catch {}; reject(new Error('img_load_fail')) }
      img.src = url
    })
  } catch {}
  return await new Promise((resolve, reject) => {
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
  let bytes = dataUrlBytes(out)
  let tries = 0
  while (bytes > 500 * 1024 && tries < 5) {
    q = Math.max(0.4, q - 0.1)
    out = canvas.toDataURL('image/jpeg', q)
    bytes = dataUrlBytes(out)
    tries++
  }
  if (bytes > 500 * 1024) {
    q = 0.8
    out = canvas.toDataURL('image/webp', q)
    bytes = dataUrlBytes(out)
    tries = 0
    while (bytes > 500 * 1024 && tries < 5) {
      q = Math.max(0.4, q - 0.1)
      out = canvas.toDataURL('image/webp', q)
      bytes = dataUrlBytes(out)
      tries++
    }
  }
  try { if (img && img.close) img.close() } catch {}
  return bytes <= 500 * 1024 ? out : ''
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
  const ok = await confirmAction(phr)
  if (!ok) { if (q('switch-available')) q('switch-available').checked = prev; return }
  await api('/api/user/available', { method: 'POST', body: JSON.stringify({ userId: S.user.id, available: next, receiveMode, zone }) })
  if (!S.user) S.user = {}
  S.user.available = next
  S.user.receiveMode = receiveMode
  S.user.zone = zone
  if (next) { setActiveNav('disponibles'); await viewAvailable(); show('screen-user-home') }
}

function buildAvailableItem(u) {
  const div = document.createElement('div')
  div.className = 'item'
  const top = document.createElement('div')
  top.className = 'avail-top'
  const img = document.createElement('img')
  img.className = 'avail-avatar'
  img.width = 44; img.height = 44
  img.src = u.selfie || ''
  if (u.selfie) img.onclick = () => showImageModal(u.selfie)
  const info = document.createElement('div')
  info.className = 'avail-info'
  const alias = document.createElement('div')
  alias.className = 'alias'
  alias.textContent = u.alias || u.id
  const sub = document.createElement('div')
  sub.className = 'avail-sub'
  const parts = []
  if (u.tableId) parts.push(`Mesa ${u.tableId}`)
  if (u.gender) parts.push(genderLabel(u.gender))
  sub.textContent = parts.join(' • ')
  info.append(alias, sub)
  top.append(img, info)
  const row = document.createElement('div')
  row.className = 'row compact'
  const bDance = document.createElement('button')
  bDance.className = 'info'
  const busy = (u.danceState && u.danceState !== 'idle')
  bDance.textContent = busy ? 'Ocupado' : 'Invitar a bailar 💃'
  bDance.disabled = !!busy
  bDance.onclick = () => sendInviteQuick(u)
  const bConsumo = document.createElement('button'); bConsumo.className = 'success'; bConsumo.textContent = 'Invitar una copa 🥂'; bConsumo.onclick = () => { setReceiver(u); q('consumption-target').value = u.id; openConsumption() }
  row.append(bDance, bConsumo)
  div.append(top, row)
  return div
}
async function viewAvailable() {
  if (!S.user || !S.user.available) {
    showError('Oye: debes poner modo activo antes de ver las personas disponibles')
    setTimeout(() => showError(''), 1500)
    return
  }
  const r = await api(`/api/users/available?sessionId=${encodeURIComponent(S.sessionId)}&onlyAvailable=true&excludeUserId=${encodeURIComponent(S.user.id)}`)
  const container = q('available-list')
  container.innerHTML = ''
  for (const u of r.users) {
    if (u.id === S.user.id) continue
    if (u.danceState && u.danceState !== 'idle') continue
    if (u.id === S.user.id) continue
    container.append(buildAvailableItem(u))
  }
  show('screen-disponibles')
  await loadDanceSessionList()
}
async function refreshAvailableList() {
  if (!S.user || !S.user.available) return
  const r = await api(`/api/users/available?sessionId=${encodeURIComponent(S.sessionId)}&onlyAvailable=true&excludeUserId=${encodeURIComponent(S.user.id)}`)
  const container = q('available-list')
  if (!container) return
  const list = (r.users || []).filter(u => u.id !== S.user.id && (!u.danceState || u.danceState === 'idle'))
  const sig = buildSig(list, u => [u.id, u.alias, u.selfie, u.gender, u.tableId, u.zone, (u.tags || []).join(','), u.danceState, u.partnerAlias].join('~'))
  S.ui = S.ui || {}
  if (S.ui.availableSig === sig) { scheduleRefreshDanceList(); return }
  S.ui.availableSig = sig
  container.innerHTML = ''
  for (const u of list) {
    container.append(buildAvailableItem(u))
  }
  scheduleRefreshDanceList()
}

async function loadDanceSessionList() {
  const cont = q('dance-session-list'); if (!cont) return
  const r = await api(`/api/users/dance?sessionId=${encodeURIComponent(S.sessionId)}`)
  const waitingSig = buildSig(r.waiting || [], u => [u.id, u.alias, u.selfie, u.danceState, u.partnerAlias].join('~'))
  const dancingSig = buildSig(r.dancing || [], u => [u.id, u.alias, u.selfie, u.danceState, u.partnerAlias].join('~'))
  const sig = `${waitingSig}##${dancingSig}`
  S.ui = S.ui || {}
  if (S.ui.danceListSig === sig) return
  S.ui.danceListSig = sig
  cont.innerHTML = ''
  const mk = (list, title) => {
    if (!Array.isArray(list) || !list.length) return
    const section = document.createElement('div')
    const t = document.createElement('div'); t.className = 'section-title'; t.textContent = title
    section.append(t)
    for (const u of list) {
      if (S.user && u.id === S.user.id) continue
      const div = document.createElement('div')
      div.className = 'item'
      const img = document.createElement('img'); img.width = 48; img.height = 48; img.src = u.selfie || ''
      const alias = document.createElement('div'); alias.className = 'alias'; alias.textContent = u.alias || u.id
      const statusChip = document.createElement('span'); statusChip.className = 'chip ' + (u.danceState === 'dancing' ? 'success' : 'pending')
      statusChip.textContent = u.danceState === 'dancing' ? `Bailando con ${u.partnerAlias || ''}` : `Esperando con ${u.partnerAlias || ''}`
      div.append(img, alias, statusChip)
      section.append(div)
    }
    cont.append(section)
  }
  mk(r.waiting, 'Esperando')
  mk(r.dancing, 'Bailando')
}
function scheduleRefreshDanceList() { scheduleLater('user_dance_list', async () => { await loadDanceSessionList() }, 1000) }
async function viewAvailableByTable() {
  if (!S.user || !S.user.available) {
    showError('Oye: debes poner modo activo antes de ver las personas disponibles')
    setTimeout(() => showError(''), 1500)
    return
  }
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  const r = await api(`/api/users/available?sessionId=${encodeURIComponent(S.sessionId)}&onlyAvailable=true&excludeUserId=${encodeURIComponent(S.user.id)}`)
  const list = (r.users || []).filter(u => u.tableId === S.user.tableId && (u.danceState === 'idle' || !u.danceState))
  const container = q('available-list')
  container.innerHTML = ''
  for (const u of list) {
    container.append(buildAvailableItem(u))
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
  const ok = await confirmAction(`¿Te gustaría invitar a ${S.currentInvite.alias || S.currentInvite.id}?`)
  if (!ok) return
  try {
    const r = await api('/api/invite/dance', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId: S.currentInvite.id, messageType: inviteMsgType }) })
    const exp = Number(r.expiresAt || (Date.now() + 60 * 1000))
    openInviteWaitModal(exp, S.currentInvite)
  } catch (e) {
    const msg = String(e && e.message || '')
    if (msg === 'blocked') showError('No puedes invitar a esta persona por ahora'); else if (msg === 'rate') showError('Demasiadas invitaciones recientes'); else if (msg === 'busy_target') showError('Está bailando ahora, te avisamos si vuelve'); else showError('No se pudo enviar la invitación')
    setTimeout(() => showError(''), 1500)
  }
}
function setReceiver(u) {
  const el = q('avail-receiver-id')
  if (el) el.textContent = u.id
}
async function sendInviteQuick(u) {
  setReceiver(u)
  S.currentInvite = u
  inviteMsgType = 'bailamos'
  const ok = await confirmAction(`¿Te gustaría invitar a ${u.alias || u.id}?`)
  if (!ok) return
  try {
    const r = await api('/api/invite/dance', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId: u.id, messageType: 'bailamos' }) })
    const exp = Number(r.expiresAt || (Date.now() + 60 * 1000))
    openInviteWaitModal(exp, u)
  } catch (e) {
    const msg = String(e && e.message || '')
    if (msg === 'blocked') showError('No puedes invitar a esta persona por ahora'); else if (msg === 'rate') showError('Demasiadas invitaciones recientes'); else if (msg === 'busy_target') showError('Está bailando ahora, te avisamos si vuelve'); else showError('No se pudo enviar la invitación')
    setTimeout(() => showError(''), 1500)
  }
}

function startEvents() {
  if (!S.user) return
  if (S.sse) S.sse.close()
  S.sse = new EventSource(`/api/events/user?userId=${encodeURIComponent(S.user.id)}`)
  S.sse.onopen = () => { if (S.timers.userReconnect) { try { clearTimeout(S.timers.userReconnect) } catch {}; S.timers.userReconnect = 0 }; S.sseReady = true; startUserPolls() }
  S.sse.onerror = () => { S.sseReady = false; startUserPolls(); scheduleUserSSEReconnect() }
  S.sse.addEventListener('dance_invite', e => {
    const data = JSON.parse(e.data)
    playNotify('dance')
    S.invitesQueue.push({ type: 'dance', id: data.invite.id, invite: data.invite })
    S.notifications.invites = (S.notifications.invites || 0) + 1
    setBadgeNav('disponibles', S.notifications.invites)
    updateTipSticker()
    ;(async () => { try { await api('/api/invite/ack', { method: 'POST', body: JSON.stringify({ inviteId: data.invite.id, toId: S.user.id }) }) } catch {} })()
    if (document.hidden) {
      const msg = `Invitación de baile de ${data.invite.from.alias}`
      S.missed.push(msg)
    }
    if (!S.inInviteFlow) { showNextInvite() }
  })
  S.sse.addEventListener('dance_status', e => {
    const data = JSON.parse(e.data)
    S.user = S.user || {}
    S.user.danceState = data.state || 'idle'
    S.user.partnerAlias = data.partner ? (data.partner.alias || data.partner.id || '') : S.user.partnerAlias
    if (data.meeting && data.meeting.id) { S.meeting = data.meeting }
    scheduleRenderUserHeader()
    updateTipSticker()
    scheduleRefreshDanceList()
  })
  S.sse.addEventListener('invite_result', e => {
    const data = JSON.parse(e.data)
    stopInviteCountdown()
    const m = q('modal'); if (m) m.classList.remove('show')
    const invId = String(data.inviteId || '')
    if (invId) {
      const before = S.invitesQueue.length
      S.invitesQueue = S.invitesQueue.filter(x => !(x.type === 'dance' && String(x.id || '') === invId))
      const removed = before - S.invitesQueue.length
      if (removed > 0) {
        S.notifications.invites = Math.max(0, (S.notifications.invites || 0) - removed)
        setBadgeNav('disponibles', S.notifications.invites)
        updateTipSticker()
      }
    }
    if (data.status === 'aceptado') {
      S.meeting = data.meeting
      S.isMeetingReceiver = !!(S.currentInvite && data.inviteId && S.currentInvite.id === data.inviteId)
      S.currentInvite = null
      if (data.note) { const noteEl = q('meeting-note'); if (noteEl) noteEl.textContent = `Respuesta: ${data.note}`; showError(`Respuesta: ${data.note}`); setTimeout(() => showError(''), 1500) }
      renderMeeting()
    } else if (data.status === 'pasado') {
      const msg = data.note ? `Respuesta: ${data.note}` : 'Vio tu invitación pero decidió pasar'
      if (document.hidden) { S.missed.push(msg) } else { showError(msg); setTimeout(() => showError(''), 1500) }
      show('screen-user-home')
      S.inInviteFlow = false
      showNextInvite()
    } else if (data.status === 'expirado') {
      const msg = String(data.reason || '') === 'unseen' ? 'No estaba mirando el teléfono' : 'Invitación expirada'
      if (document.hidden) { S.missed.push(msg) } else { showError(msg); setTimeout(() => showError(''), 1500) }
      show('screen-user-home')
      S.inInviteFlow = false
      showNextInvite()
    }
    scheduleRefreshDanceList()
  })
  S.sse.addEventListener('meeting_plan', e => {
    const data = JSON.parse(e.data)
    if (data.meetingId && S.meeting && data.meetingId === S.meeting.id) {
      S.meetingPlan = data.plan || ''
      const el = q('meeting-plan-display')
      if (el) {
        const planTxt = S.meetingPlan === 'come' ? 'Ven por mí'
                      : S.meetingPlan === 'go' ? 'Ya voy por ti'
                      : S.meetingPlan === 'pista' ? 'Nos vemos en la pista — no me quites la mirada que me pierdo'
                      : ''
        el.textContent = planTxt ? `Plan: ${planTxt}` : ''
      }
    }
  })
  S.sse.addEventListener('meeting_expired', e => {
    const data = JSON.parse(e.data)
    if (S.meeting && data.meetingId && data.meetingId === S.meeting.id) {
      S.meeting = null
      S.meetingPlan = ''
      S.isMeetingReceiver = false
      S.inInviteFlow = false
      if (S.user) { S.user.danceState = 'idle'; S.user.dancePartnerId = ''; S.user.meetingId = '' }
      showError('El encuentro expiró')
      setTimeout(() => showError(''), 1500)
      show('screen-user-home')
      showNextInvite()
    }
  })
  S.sse.addEventListener('consumption_invite', e => {
    const data = JSON.parse(e.data)
    playNotify('consumption')
    S.invitesQueue.push({ type: 'consumption', data })
    S.notifications.invites = (S.notifications.invites || 0) + 1
    setBadgeNav('disponibles', S.notifications.invites)
    updateTipSticker()
    ;(async () => { try { await api('/api/consumption/ack', { method: 'POST', body: JSON.stringify({ requestId: data.requestId, toId: S.user.id }) }) } catch {} })()
    if (document.hidden) {
      const g = genderLabel(data.from.gender)
      const gTxt = g ? ` • ${g}` : ''
      const msg = `Invitación de consumo de ${data.from.alias}${gTxt}: ${data.product}`
      S.missed.push(msg)
    }
    if (!S.inInviteFlow) { showNextInvite() }
  })
  S.sse.addEventListener('consumption_invite_bulk', e => {
    const data = JSON.parse(e.data)
    playNotify('consumption')
    S.invitesQueue.push({ type: 'consumption', data })
    S.notifications.invites = (S.notifications.invites || 0) + 1
    setBadgeNav('disponibles', S.notifications.invites)
    updateTipSticker()
    ;(async () => { try { await api('/api/consumption/ack', { method: 'POST', body: JSON.stringify({ requestId: data.requestId, toId: S.user.id }) }) } catch {} })()
    if (document.hidden) {
      const listTxt = (Array.isArray(data.items) ? data.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : '')
      const g = genderLabel(data.from.gender)
      const gTxt = g ? ` • ${g}` : ''
      const msg = `Invitación de consumo de ${data.from.alias}${gTxt}: ${listTxt}`
      S.missed.push(msg)
    }
    if (!S.inInviteFlow) { showNextInvite() }
  })
function updateTipSticker() {
  const el = q('tip-refresh-bailar'); if (!el) return
  const show = Number(S.notifications.invites || 0) > 0 && String(S.user && S.user.danceState || 'idle') !== 'dancing'
  el.style.display = show ? 'inline-block' : 'none'
  el.classList.toggle('pulse', show)
}
  S.sse.addEventListener('invite_seen', e => {
    const data = JSON.parse(e.data)
    const u = data.to && data.to.alias ? data.to.alias : (data.to && data.to.id ? data.to.id : '')
    const msg = `Tu invitación le apareció a ${u}`
    if (document.hidden) { S.missed.push(msg) } else { showSuccess(msg) }
  })
  S.sse.addEventListener('invite_not_seen', e => {
    const data = JSON.parse(e.data)
    const u = data.to && data.to.alias ? data.to.alias : (data.to && data.to.id ? data.to.id : '')
    const msg = `No estaba mirando el teléfono`
    if (document.hidden) { S.missed.push(msg) } else { showError(msg); setTimeout(() => showError(''), 1500) }
  })
  S.sse.addEventListener('invite_suppress', e => {
    const data = JSON.parse(e.data)
    const invId = String(data.inviteId || '')
    if (invId) {
      const before = S.invitesQueue.length
      S.invitesQueue = S.invitesQueue.filter(x => !(x.type === 'dance' && String(x.id || '') === invId))
      const removed = before - S.invitesQueue.length
      if (removed > 0) {
        S.notifications.invites = Math.max(0, (S.notifications.invites || 0) - removed)
        setBadgeNav('disponibles', S.notifications.invites)
        updateTipSticker()
        const m = q('modal'); if (m) m.classList.remove('show')
        show('screen-user-home')
        S.inInviteFlow = false
        showNextInvite()
        showError('Invitación no disponible'); setTimeout(() => showError(''), 1500)
      }
    }
  })
  S.sse.addEventListener('consumption_seen', e => {
    const data = JSON.parse(e.data)
    const u = data.to && data.to.alias ? data.to.alias : (data.to && data.to.id ? data.to.id : '')
    const msg = `Tu invitación de consumo le apareció a ${u}`
    if (document.hidden) { S.missed.push(msg) } else { showSuccess(msg) }
  })
  S.sse.addEventListener('consumption_not_seen', e => {
    const data = JSON.parse(e.data)
    const msg = 'No estaba mirando el teléfono'
    if (document.hidden) { S.missed.push(msg) } else { showError(msg); setTimeout(() => showError(''), 1500) }
  })
  S.sse.addEventListener('consumption_suppress', e => {
    const data = JSON.parse(e.data)
    const reqId = String(data.requestId || '')
    if (reqId) {
      const before = S.invitesQueue.length
      S.invitesQueue = S.invitesQueue.filter(x => !(x.type === 'consumption' && String(x.data && x.data.requestId || '') === reqId))
      const removed = before - S.invitesQueue.length
      if (removed > 0) {
        S.notifications.invites = Math.max(0, (S.notifications.invites || 0) - removed)
        setBadgeNav('disponibles', S.notifications.invites)
        updateTipSticker()
        const m = q('modal'); if (m) m.classList.remove('show')
        show('screen-user-home')
        S.inInviteFlow = false
        showNextInvite()
        showError('Invitación de consumo no disponible'); setTimeout(() => showError(''), 1500)
      }
    }
  })
  S.sse.addEventListener('consumption_accepted', e => {
    const data = JSON.parse(e.data)
    const msg = `${data.from.alias} aceptó tu invitación: ${data.quantity} x ${data.product}`
    stopInviteCountdown()
    const m = q('modal'); if (m) m.classList.remove('show')
    if (document.hidden) { S.missed.push(msg) } else { showSuccess(msg) }
    ;(async () => {
      try {
        const r = await api(`/api/staff/analytics?sessionId=${encodeURIComponent(S.sessionId)}`)
        const items = r.topItems || {}
        const names = Object.keys(items).filter(name => name !== data.product)
        const top = names.sort((a, b) => Number(items[b] || 0) - Number(items[a] || 0)).slice(0, 2)
        if (top.length) {
          const txt = top.length === 1 ? `Popular esta noche: ${top[0]}` : `Popular esta noche: ${top[0]} • ${top[1]}`
          showSuccess(txt)
        }
      } catch {}
    })()
  })
  S.sse.addEventListener('consumption_passed', e => {
    const data = JSON.parse(e.data)
    const listTxt = (Array.isArray(data.items) ? data.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : data.product)
    const msg = `${data.to.alias} pasó tu invitación: ${listTxt}`
    stopInviteCountdown()
    const m = q('modal'); if (m) m.classList.remove('show')
    if (document.hidden) { S.missed.push(msg) } else { showError(msg); setTimeout(() => showError(''), 1500) }
  })
  S.sse.addEventListener('dj_update', e => {
    const data = JSON.parse(e.data)
    const r = data.request || {}
    if (r.status === 'atendido') { showSuccess('Tu canción está en cola') }
    else if (r.status === 'programado') {
      const queue = Array.isArray(data.queue) ? data.queue.slice(0, 3) : []
      const txt = queue.length ? `Pronto sonará tu canción • Siguiente: ${queue.map(q => q.song).join(' • ')}` : 'Pronto sonará tu canción'
      showSuccess(txt)
    }
    else if (r.status === 'descartado') { showError('Tu solicitud fue descartada'); setTimeout(() => showError(''), 1500) }
    else if (r.status === 'sonando') { showSuccess('¡Tu canción está sonando!') }
    else if (r.status === 'terminado') { /* opcional: sin mensaje */ }
  })
  S.sse.addEventListener('order_update', e => {
    const data = JSON.parse(e.data)
    if (document.hidden) { S.missed.push('Se actualizó tu orden') } else { scheduleUserOrdersUpdate() }
  })
  S.sse.addEventListener('waiter_update', e => {
    const data = JSON.parse(e.data)
    const msg = `Estado de tu llamado: ${data.call.status}`
    if (document.hidden) { S.missed.push(msg) } else { showError(msg); setTimeout(() => showError(''), 1200) }
  })
  S.sse.addEventListener('table_closed', e => {
    const data = JSON.parse(e.data)
    const closedId = String(data.tableId || '').trim()
    if (S.user && closedId && String(S.user.tableId || '') === closedId) {
      S.user.tableId = ''
      const ut = q('user-table'); if (ut) ut.textContent = '-'
      const tc = q('user-table-chip'); if (tc) tc.textContent = 'Mesa -'
      showError(`Mesa ${closedId} cerrada, selecciona una nueva`)
      setTimeout(() => showError(''), 1500)
      openSelectTable()
    }
  })
  S.sse.addEventListener('meeting_expired', e => {
    S.meeting = null
    show('screen-user-home')
    S.inInviteFlow = false
    showNextInvite()
    scheduleRefreshDanceList()
  })
  S.sse.addEventListener('thanks', e => {
    const data = JSON.parse(e.data)
    showSuccess(`Mensaje de ${data.from.alias}: ${data.message}`)
  })
  S.sse.addEventListener('dj_toggle', e => {
    try {
      const data = JSON.parse(e.data)
      const enabled = !!data.enabled
      const until = Number(data.until || 0)
      S.djUserEnabled = enabled
      S.djUserUntil = until
      if (S.nav.current === 'screen-dj-request') renderUserDJStatus(enabled, until)
    } catch {}
  })
  S.sse.addEventListener('dj_now_playing', e => {
    try {
      const data = JSON.parse(e.data)
      const chip = q('dj-now-playing')
      if (chip) {
        const who = data.alias ? ` • ${data.alias}` : ''
        const mesa = data.tableId ? ` • Mesa ${data.tableId}` : ''
        chip.textContent = data.song ? `Está sonando: ${data.song}${who}${mesa}` : 'Está sonando'
        chip.classList.remove('dj-programado')
        chip.style.display = 'inline-block'
      }
    } catch {}
  })
  S.sse.addEventListener('dj_now_programmed', e => {
    try {
      const data = JSON.parse(e.data)
      const chip = q('dj-now-playing')
      if (chip) {
        const who = data.alias ? ` • ${data.alias}` : ''
        const mesa = data.tableId ? ` • Mesa ${data.tableId}` : ''
        chip.textContent = data.song ? `Programado: ${data.song}${who}${mesa}` : 'Programado'
        chip.classList.add('dj-programado')
        chip.style.display = 'inline-block'
      }
    } catch {}
  })
  S.sse.addEventListener('dj_now_stopped', e => {
    try {
      const chip = q('dj-now-playing')
      if (chip) {
        chip.style.display = 'none'
        chip.classList.remove('dj-programado')
      }
    } catch {}
  })
  S.sse.addEventListener('session_end', e => {
    try { if (S.sse) S.sse.close() } catch {}
    showModalAction('Sesión finalizada', 'La sesión de esta noche ha terminado', 'Aceptar', () => {
      try { removeLocalUser(S.venueId) } catch {}
      S.sessionId = ''; S.user = null; S.role = ''; S.sse = null
      show('screen-welcome')
    }, 'info')
  })
  startUserPolls()
  loadPendingInvites()
}

function renderMeeting() {
  const m = S.meeting
  const left = Math.max(0, Math.floor((m.expiresAt - Date.now()) / 1000))
  q('meeting-info').textContent = `Punto: ${m.point} • Tiempo: ${left}s`
  const noteEl = q('meeting-note')
  const showChoices = !!S.isMeetingReceiver
  const bCome = q('btn-meet-come'), bGo = q('btn-meet-go'), bPista = q('btn-meet-pista')
  const bConfirm = q('btn-meeting-confirm')
  if (bCome) bCome.style.display = showChoices ? '' : 'none'
  if (bGo) bGo.style.display = showChoices ? '' : 'none'
  if (bPista) bPista.style.display = showChoices ? '' : 'none'
  if (bConfirm) bConfirm.style.display = showChoices ? '' : 'none'
  const mpd = q('meeting-plan-display')
  if (mpd) {
    const planTxt = S.meetingPlan === 'come' ? 'Ven por mí'
                  : S.meetingPlan === 'go' ? 'Ya voy por ti'
                  : S.meetingPlan === 'pista' ? 'Nos vemos en la pista — no me quites la mirada que me pierdo'
                  : ''
    mpd.textContent = planTxt ? `Plan: ${planTxt}` : ''
  }
  show('screen-meeting')
}

async function respondInvite(accept) {
  if (S.consumptionReq) {
    const hasItems = Array.isArray(S.consumptionReq.items) && S.consumptionReq.items.length > 0
    const listTxt = hasItems ? S.consumptionReq.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : S.consumptionReq.product
    if (accept) {
      try {
        if (hasItems) {
          await api('/api/consumption/respond/bulk', { method: 'POST', body: JSON.stringify({ fromId: S.consumptionReq.from.id, toId: S.user.id, items: S.consumptionReq.items, action: 'accept', requestId: S.consumptionReq.requestId || '' }) })
          openThanks(S.consumptionReq.from.id, 'consumption')
        } else {
          const qty = Math.max(1, Number(S.consumptionReq.quantity || 1))
          await api('/api/consumption/respond', { method: 'POST', body: JSON.stringify({ fromId: S.consumptionReq.from.id, toId: S.user.id, product: S.consumptionReq.product, quantity: qty, action: 'accept', requestId: S.consumptionReq.requestId || '' }) })
          openThanks(S.consumptionReq.from.id, 'consumption')
        }
      } catch (e) {
        const msg = String(e.message || 'error')
        if (msg === 'no_user') { showError('Usuario no disponible'); setTimeout(()=>showError(''),1000) }
        else { showError('No se pudo aceptar la invitación'); setTimeout(()=>showError(''),1200) }
      }
    }
    S.consumptionReq = null
    show('screen-user-home')
    S.inInviteFlow = false
    showNextInvite()
    return
  }
  if (!S.currentInvite) return
  const id = S.currentInvite.id
  const action = accept ? 'accept' : 'pass'
  stopInviteCountdown()
  const note = (q('invite-response') ? q('invite-response').value.trim().slice(0, 120) : '')
  await api('/api/invite/respond', { method: 'POST', body: JSON.stringify({ inviteId: id, action, note }) })
  { const cur = (S.notifications.invites || 0) - 1; S.notifications.invites = cur < 0 ? 0 : cur }
  setBadgeNav('disponibles', S.notifications.invites)
  if (!accept) show('screen-user-home')
  if (!accept) { S.inInviteFlow = false; showNextInvite() }
}

function openThanks(toId, context) {
  showModal('Agradecer', 'Elige un mensaje para agradecer', 'success')
  const row = document.querySelector('#modal .row')
  if (!row) return
  const options = ['¡Gracias!', 'Mil gracias, cuando quieras', 'Gracias por la invitación']
  for (const txt of options) {
    const b = document.createElement('button')
    b.className = 'info'
    b.textContent = txt
    b.onclick = async () => {
      try {
        const m = q('modal'); if (m) m.classList.remove('show')
      } catch {}
      await api('/api/thanks/send', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId, context, message: txt }) })
      showSuccess('Se envió tu agradecimiento')
    }
    row.append(b)
  }
}

async function cancelMeeting() {
  if (!S.meeting) return
  const ok = await confirmAction('Vas a cancelar el encuentro. ¿Confirmas?')
  if (!ok) return
  await api('/api/meeting/cancel', { method: 'POST', body: JSON.stringify({ meetingId: S.meeting.id }) })
  S.meeting = null
  show('screen-user-home')
  S.inInviteFlow = false
  showNextInvite()
}

async function confirmMeeting() {
  if (!S.meeting) return
  const plan = S.meetingPlan || ''
  const planTxt = plan === 'come' ? 'Ven por mí' : plan === 'go' ? 'Ya voy por ti' : plan === 'pista' ? 'Nos vemos en la pista — no me quites la mirada que me pierdo' : ''
  const phr = planTxt ? `Vas a confirmar el encuentro: ${planTxt}. ¿Confirmas?` : 'Vas a confirmar el encuentro. ¿Confirmas?'
  const ok = await confirmAction(phr)
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
    if (window && window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) return
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
  const ok = await confirmAction(phr)
  if (!ok) return
  try {
    if (items.length > 1) {
      await api('/api/consumption/invite/bulk', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId, items, note }) })
    } else {
      await api('/api/consumption/invite', { method: 'POST', body: JSON.stringify({ fromId: S.user.id, toId, product: items[0].product, quantity: items[0].quantity, note }) })
    }
    S.cart = []
    renderCart()
    const inp = q('product'); if (inp) inp.value = ''
    const qn = q('quantity'); if (qn) qn.value = '1'
    const exp = Date.now() + 60 * 1000
    openInviteWaitModal(exp, { id: toId, alias: displayTo, selfie: '' })
    showSuccess('Invitación enviada')
  } catch (e) {
    const msg = String(e.message || 'error')
    if (msg === 'blocked') { showError('No puedes invitar a esta persona (bloqueado)'); setTimeout(()=>showError(''),1000); return }
    if (msg === 'rate_consumo') { showError('Límite alcanzado: 5 invitaciones/hora'); setTimeout(()=>showError(''),1000); return }
    if (msg === 'no_user') { showError('Persona no encontrada'); setTimeout(()=>showError(''),1000); return }
    showError('No se pudo enviar la invitación'); setTimeout(()=>showError(''),1200)
  }
}

async function orderTable() {
  const product = q('product').value
  const qty = Math.max(1, Number(q('quantity').value || 1))
  const items = (S.cart && S.cart.length) ? S.cart.slice() : (product ? [{ product, quantity: qty }] : [])
  if (!items.length) { showError('Selecciona producto(s)'); return }
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); return }
  const listTxt = items.map(it => `${it.quantity} x ${it.product}`).join(', ')
  const ok = await confirmAction(`Vas a ordenar: ${listTxt} para la mesa ${S.user.tableId}. ¿Confirmas?`)
  if (!ok) return
  for (const it of items) {
    await api('/api/order/new', { method: 'POST', body: JSON.stringify({ userId: S.user.id, product: it.product, quantity: it.quantity, for: 'mesa' }) })
  }
  S.cart = []
  renderCart()
  const inp = q('product'); if (inp) inp.value = ''
  const qn = q('quantity'); if (qn) qn.value = '1'
  showConfetti()
  showError('Pedido para mesa creado')
  setTimeout(() => showError(''), 1000)
}

async function blockUser() {
  const targetId = q('block-id').value.trim()
  if (!targetId) return
  const ok = await confirmAction(`Vas a bloquear a ${targetId}. ¿Confirmas?`)
  if (!ok) return
  await api('/api/block', { method: 'POST', body: JSON.stringify({ userId: S.user.id, targetId }) })
}

async function reportUser() {
  const targetId = q('report-id').value.trim()
  const category = q('report-cat').value.trim()
  const note = q('report-note').value.trim()
  if (!targetId) return
  const ok = await confirmAction(`Vas a reportar a ${targetId} por "${category}". Nota: "${note}". ¿Confirmas?`)
  if (!ok) return
  await api('/api/report', { method: 'POST', body: JSON.stringify({ userId: S.user.id, targetId, category, note }) })
}

async function startStaffSession() {
  try {
    showError('')
    const pinInput = (q('join-pin') ? q('join-pin').value.trim() : '')
    try {
      const u = new URL(location.href)
      const v = u.searchParams.get('venueId') || ''
      if (v) S.venueId = v
    } catch {}
    let r = null
    const mode = getModeFromUrl() || (isRestaurantMode() ? 'restaurant' : '')
    const activeMode = mode === 'restaurant' ? 'restaurant' : 'disco'
    const qv = S.venueId ? ('venueId=' + encodeURIComponent(S.venueId) + '&') : ''
    try { r = await api(`/api/session/active?${qv}mode=${encodeURIComponent(activeMode)}`) } catch {}
    if (!r || !r.sessionId) r = await api('/api/session/start', { method: 'POST', body: JSON.stringify({ venueId: S.venueId || 'default', mode }) })
    const joinCodeEl = q('join-code'); if (joinCodeEl) joinCodeEl.value = r.sessionId
    S.sessionId = r.sessionId
    S.venueId = r.venueId || (S.venueId || 'default')
    if (r.mode || mode) applyMode(r.mode || mode)
    const pinToUse = pinInput || r.pin || ''
    if (!pinToUse) { showError('Ingresa el PIN'); return }
    const joinRes = await api('/api/join', { method: 'POST', body: JSON.stringify({ sessionId: r.sessionId, role: 'staff', pin: pinToUse }) })
    S.user = joinRes.user
    S.role = 'staff'
    S.sessionPin = pinToUse
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
  const ok = await confirmAction('¿Cerrar sesión de la noche y borrar datos?')
  if (!ok) return
  const pin = await promptInput('Confirma con PIN', 'Ingresa el PIN de sesión')
  if (!pin) { showError('Ingresa el PIN'); setTimeout(() => showError(''), 1200); return }
  try { await saveStaffCatalog() } catch {}
  await api('/api/session/end', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, pin }) })
  try { if (S.sse) S.sse.close() } catch {}
  try { if (S.staffSSE) S.staffSSE.close() } catch {}
  S.sessionId = ''; S.user = null; S.role = ''; S.sse = null; S.staffSSE = null
  try { removeLocalUser(S.venueId) } catch {}
  S.appMode = ''
  applyDiscoMode()
  setModeInUrl('')
  show('screen-venue-type')
  showError('Sesión destruida')
  setTimeout(() => showError(''), 1200)
}
async function restartStaffSession() {
  if (S.sessionId) {
    const ok = await confirmAction('¿Destruir sesión actual y crear una nueva?')
    if (!ok) return
    try { await saveStaffCatalog() } catch {}
    const pin = await promptInput('Confirma con PIN', 'Ingresa el PIN de sesión')
    if (!pin) return
    try { await api('/api/session/end', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, pin }) }) } catch {}
    try { if (S.sse) S.sse.close() } catch {}
    try { if (S.staffSSE) S.staffSSE.close() } catch {}
    S.sessionId = ''; S.user = null; S.role = ''; S.sse = null; S.staffSSE = null
    try { removeLocalUser(S.venueId) } catch {}
  }
  await startStaffSession()
}

function startStaffEvents() {
  if (!S.sessionId) return
  try { if (S.staffSSE) S.staffSSE.close() } catch {}
  S.staffSSE = new EventSource(`/api/events/staff?sessionId=${encodeURIComponent(S.sessionId)}`)
  S.staffSSE.onopen = () => {
    S.staffSseReady = true
    startStaffPolls()
    if (S.timers.staffReconnect) { try { clearTimeout(S.timers.staffReconnect) } catch {}; S.timers.staffReconnect = 0 }
    scheduleStaffOrdersUpdate()
    scheduleStaffAnalyticsUpdate()
    scheduleStaffUsersUpdate()
    scheduleStaffWaiterUpdate()
    scheduleStaffDJUpdate()
  }
  S.staffSSE.onerror = () => { S.staffSseReady = false; startStaffPolls(); scheduleStaffSSEReconnect() }
  S.staffSSE.addEventListener('order_new', e => {
    scheduleStaffOrdersUpdate()
  })
  S.staffSSE.addEventListener('order_update', e => {
    scheduleStaffOrdersUpdate()
  })
  S.staffSSE.addEventListener('report', e => {
    scheduleStaffReportsUpdate()
  })
  S.staffSSE.addEventListener('waiter_call', e => {
    const data = JSON.parse(e.data)
    scheduleStaffWaiterUpdate()
    if (S.staffTab === 'orders') scheduleStaffOrdersUpdate()
    if (S.staffTab !== 'waiter') { showError(`Llamado de mesero: Mesa ${data.call.tableId || '-'}`); setTimeout(() => showError(''), 1500) }
  })
  S.staffSSE.addEventListener('waiter_update', e => {
    const data = JSON.parse(e.data)
    scheduleStaffWaiterUpdate()
    if (S.staffTab === 'orders') scheduleStaffOrdersUpdate()
    if (S.staffTab !== 'waiter') { showError(`Llamado actualizado: ${data.call.status}`); setTimeout(() => showError(''), 1200) }
  })
  S.staffSSE.addEventListener('table_closed', e => {
    scheduleStaffAnalyticsUpdate()
    viewStaffTableHistory()
  })
  S.staffSSE.addEventListener('session_expired', e => {
    showModal('Sesión expirada', 'Mesas cerradas y sesión finalizada. Inicia nueva sesión para continuar.', 'error')
    setTimeout(() => showError(''), 2500)
  })
  S.staffSSE.addEventListener('dj_request', e => { scheduleStaffDJUpdate() })
  S.staffSSE.addEventListener('dj_update', e => { scheduleStaffDJUpdate() })
  S.staffSSE.addEventListener('dj_toggle', e => {
    try {
      const data = JSON.parse(e.data)
      renderDJStatus(!!data.enabled, Number(data.until || 0))
    } catch {}
  })
  S.staffSSE.addEventListener('catalog_update', e => {
    scheduleLater('staff_catalog', async () => { await loadStaffCatalogEditor() }, 500)
  })
  startStaffPolls()
}

async function ensureSessionActiveOffer() {
  try {
    let v = S.venueId
    if (!v) {
      try { const u = new URL(location.href); v = u.searchParams.get('venueId') || '' } catch {}
    }
    if (!v) v = 'default'
    const mode = isRestaurantMode() ? 'restaurant' : 'disco'
    const r = await api(`/api/session/active?venueId=${encodeURIComponent(v)}&mode=${encodeURIComponent(mode)}`).catch(() => null)
    if (r && r.sessionId && r.venueId === v) return true
    show('screen-staff-welcome')
    return false
  } catch { return false }
}
async function promptCatalogBootstrap() {
  return await new Promise(resolve => {
    const title = t('catalog_bootstrap_title') || 'Carta del local'
    const msg = t('catalog_bootstrap_text') || '¿Quieres copiar la carta global para ahorrar tiempo o prefieres crear la tuya desde cero?'
    showModal(title, msg, 'info')
    const row = document.querySelector('#modal .row')
    const closeBtn = q('modal-close')
    if (!row || !closeBtn) { resolve(''); return }
    const btnCopy = document.createElement('button')
    btnCopy.className = 'success'
    btnCopy.textContent = t('catalog_bootstrap_copy') || 'Copiar carta global'
    const btnNew = document.createElement('button')
    btnNew.className = 'secondary'
    btnNew.textContent = t('catalog_bootstrap_new') || 'Crear desde 0'
    const cleanup = () => {
      try { btnCopy.remove() } catch {}
      try { btnNew.remove() } catch {}
      try { closeBtn.onclick = () => { const m = q('modal'); if (m) m.classList.remove('show') } } catch {}
    }
    const finish = (val) => { const m = q('modal'); if (m) m.classList.remove('show'); cleanup(); resolve(val) }
    btnCopy.onclick = () => finish('copy')
    btnNew.onclick = () => finish('new')
    closeBtn.onclick = () => finish('')
    row.insertBefore(btnCopy, closeBtn)
    row.insertBefore(btnNew, closeBtn)
  })
}
async function getCatalogData(force = false) {
  const ttl = 60000
  S.cache = S.cache || {}
  if (!force && S.cache.catalog && (Date.now() - (S.cache.catalogTs || 0) < ttl)) return S.cache.catalog
  const qs = S.sessionId ? `?sessionId=${encodeURIComponent(S.sessionId)}` : (isRestaurantMode() ? '?mode=restaurant' : '')
  const r = await api(`/api/catalog${qs}`)
  S.cache.catalog = r
  S.cache.catalogTs = Date.now()
  return r
}
async function ensureCatalogIndex() {
  if (S.catalogIndex && S.cache && S.cache.catalogIndexTs && (Date.now() - S.cache.catalogIndexTs < 60000)) return
  try {
    const r = await getCatalogData()
    const idx = {}
    for (const it of r.items || []) {
      const key = String(it.name || '').toLowerCase()
      if (key) idx[key] = it
    }
    S.catalogIndex = idx
    S.cache = S.cache || {}
    S.cache.catalogIndexTs = Date.now()
  } catch {}
}
function formatOrderProductFull(name) {
  const key = String(name || '').toLowerCase()
  const it = S.catalogIndex ? S.catalogIndex[key] : null
  if (!it) return name
  const catKey = (it.category || '').toLowerCase()
  const cats = { cervezas: 'Cerveza', botellas: 'Botella', cocteles: 'Coctel', sodas: 'Soda', otros: 'Otro' }
  const meta = getCatalogMeta()
  const cat = isRestaurantMode() ? (meta.labels[catKey] || it.category || '') : (cats[catKey] || it.category || '')
  const sub = String(it.subcategory || '').trim()
  if (cat && sub) return `${cat} • ${sub} • ${it.name}`
  if (cat) return `${cat} • ${it.name}`
  return it.name
}
function formatTimeShort(ms) {
  const t = Number(ms || 0)
  if (!t || Number.isNaN(t)) return ''
  const d = new Date(t)
  const pad = (n) => String(n).padStart(2, '0')
  const hh = pad(d.getHours()), mm = pad(d.getMinutes())
  const dd = pad(d.getDate()), mo = pad(d.getMonth() + 1)
  return `${hh}:${mm} · ${dd}/${mo}`
}
async function loadOrders(state = '') {
  const qs = state ? `&state=${encodeURIComponent(state)}` : ''
  const r = await api(`/api/staff/orders?sessionId=${encodeURIComponent(S.sessionId)}${qs}`)
  const container = q('staff-orders-list') || q('orders')
  const listAsc = (r.orders || []).slice().sort((a, b) => {
    const ta = Number(a.createdAt || 0), tb = Number(b.createdAt || 0)
    if (ta !== tb) return ta - tb
    const ia = String(a.id || ''), ib = String(b.id || '')
    return ia.localeCompare(ib)
  })
  const filtered = state ? listAsc : listAsc.filter(o => o.status !== 'cobrado')
  const sigOrders = buildSig(filtered, o => [o.id, o.status, o.quantity, o.total, o.product, o.emitterId, o.receiverId, o.receiverTable, o.emitterTable, o.mesaEntrega, o.createdAt, o.isInvitation].join('~'))
  const sig = sigOrders
  S.ui = S.ui || {}
  S.ui.staffOrdersSigMap = S.ui.staffOrdersSigMap || {}
  const sigKey = state || '__open__'
  if (S.ui.staffOrdersSigMap[sigKey] === sig) return
  S.ui.staffOrdersSigMap[sigKey] = sig
  container.innerHTML = ''
  await ensureCatalogIndex()
  const groups = new Map()
  for (const o of filtered) {
    const mesa = String(o.mesaEntrega || o.receiverTable || o.emitterTable || '').trim()
    const key = mesa || 'sin_mesa'
    if (!groups.has(key)) groups.set(key, [])
    groups.get(key).push(o)
  }
  const keys = Array.from(groups.keys()).sort((a, b) => {
    if (a === 'sin_mesa') return 1
    if (b === 'sin_mesa') return -1
    const na = Number(a), nb = Number(b)
    if (Number.isFinite(na) && Number.isFinite(nb)) return na - nb
    return String(a).localeCompare(String(b))
  })
  for (const key of keys) {
    const card = document.createElement('div')
    card.className = 'card'
    const head = document.createElement('div')
    head.className = 'row'
    const title = document.createElement('div')
    title.textContent = key === 'sin_mesa' ? 'Sin mesa' : `Mesa ${key}`
    const countChip = document.createElement('span')
    countChip.className = 'chip'
    countChip.textContent = `${groups.get(key).length} órdenes`
    const total = groups.get(key).reduce((acc, o) => acc + Number(o.total || 0), 0)
    const totalChip = document.createElement('span')
    totalChip.className = 'chip'
    totalChip.textContent = `$${total}`
    const btn = document.createElement('button')
    btn.className = 'info'
    btn.textContent = 'Ver pedidos'
    const chips = document.createElement('div')
    chips.className = 'row'
    chips.append(countChip, totalChip)
    head.append(title, chips, btn)
    const details = document.createElement('div')
    details.style.display = 'none'
    btn.onclick = () => { details.style.display = details.style.display === 'none' ? '' : 'none' }
    for (const o of groups.get(key)) {
      const div = document.createElement('div')
      div.className = 'card'
      const info = document.createElement('div')
      const chip = document.createElement('span')
      chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
      chip.textContent = o.status.replace('_', ' ')
      const mesaInfo = (o.mesaEntrega || o.receiverTable || o.emitterTable) ? ` • Mesa entrega ${o.mesaEntrega || o.receiverTable}` : ''
      const emAlias = (S.usersIndex && S.usersIndex[o.emitterId] ? S.usersIndex[o.emitterId].alias : o.emitterId)
      const reAlias = (S.usersIndex && S.usersIndex[o.receiverId] ? S.usersIndex[o.receiverId].alias : o.receiverId)
      const amountTxt = ` • $${o.total || 0}`
      const label = o.productLabel || formatOrderProductFull(o.product)
      const timeTxt = o.createdAt ? ` • ${formatTimeShort(o.createdAt)}` : ''
      info.textContent = `${label} x${o.quantity || 1}${amountTxt} • Emisor ${emAlias} → Receptor ${reAlias}${mesaInfo}${timeTxt}`
      info.append(chip)
      if (o.isInvitation) {
        const invChip = document.createElement('span')
        invChip.className = 'chip'
        invChip.textContent = 'Invitación'
        info.append(invChip)
      }
      const row = document.createElement('div')
      row.className = 'row'
      const b1 = document.createElement('button'); b1.className = 'success'; b1.textContent = 'Cobrado'; b1.onclick = () => updateOrder(o.id, 'cobrado')
      const b0 = document.createElement('button'); b0.className = 'warning'; b0.textContent = 'En preparación'; b0.onclick = () => updateOrder(o.id, 'en_preparacion')
      const b2 = document.createElement('button'); b2.className = 'info'; b2.textContent = 'Entregado'; b2.onclick = () => updateOrder(o.id, 'entregado')
      const b3 = document.createElement('button'); b3.className = 'danger'; b3.textContent = 'Cancelar'; b3.onclick = () => updateOrder(o.id, 'cancelado')
      row.append(b0, b1, b2, b3)
      div.append(info, row)
      details.append(div)
    }
    card.append(head, details)
    container.append(card)
  }
  const list = q('staff-orders-list')
  if (list) {
    list.onpointerdown = () => {
      S.ui = S.ui || {}; S.ui.freezeStaffOrders = true
      S.timers = S.timers || {}
      if (S.timers.ordersFreeze) { try { clearTimeout(S.timers.ordersFreeze) } catch {} }
      S.timers.ordersFreeze = setTimeout(() => { S.ui.freezeStaffOrders = false; S.timers.ordersFreeze = 0 }, 1500)
    }
    list.onpointerup = () => { S.ui = S.ui || {}; S.ui.freezeStaffOrders = false; if (S.timers.ordersFreeze) { try { clearTimeout(S.timers.ordersFreeze) } catch {}; S.timers.ordersFreeze = 0 } }
    list.onmouseleave = () => { S.ui = S.ui || {}; S.ui.freezeStaffOrders = false; if (S.timers.ordersFreeze) { try { clearTimeout(S.timers.ordersFreeze) } catch {}; S.timers.ordersFreeze = 0 } }
  }
}

async function updateOrder(id, status) {
  await api(`/api/staff/orders/${id}`, { method: 'POST', body: JSON.stringify({ status }) })
  const cur = q('staff-orders-filter')?.value || ''
  loadOrders(cur)
}

async function loadUsers() {
  const r = await api(`/api/staff/users?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('staff-users') || q('users')
  const sig = buildSig(r.users || [], u => [u.id, u.alias, u.muted].join('~'))
  S.ui = S.ui || {}
  if (S.ui.usersSig === sig) return
  S.ui.usersSig = sig
  S.ui.staffOrdersSigMap = {}
  S.usersIndex = {}
  container.innerHTML = ''
  for (const u of r.users) {
    S.usersIndex[u.id] = { alias: u.alias || u.id }
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    info.textContent = `${u.alias || u.id} • Selfie cargada • ${u.muted ? 'silenciado' : 'activo'}`
    const row = document.createElement('div')
    row.className = 'row'
    const mute = document.createElement('button'); mute.className = u.muted ? 'success' : 'warning'; mute.textContent = u.muted ? 'Activar' : 'Silenciar'; mute.onclick = () => moderateUser(u.id, !u.muted)
    row.append(mute)
    div.append(info, row)
    container.append(div)
  }
}

async function loadWaiterCalls() {
  const r = await api(`/api/staff/waiter?sessionId=${encodeURIComponent(S.sessionId)}`)
  const container = q('waiter-calls')
  const list = (r.calls || []).slice().filter(c => c.status !== 'atendido' && c.status !== 'cancelado').sort((a, b) => Number(a.ts || 0) - Number(b.ts || 0))
  const sig = buildSig(list, c => [c.id, c.status, c.tableId, c.userAlias, c.userId, c.reason, c.ts].join('~'))
  S.ui = S.ui || {}
  if (S.ui.waiterSig === sig) return
  S.ui.waiterSig = sig
  container.innerHTML = ''
  for (const c of list) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    info.textContent = `Mesa ${c.tableId || '-'} • ${c.userAlias ? c.userAlias : c.userId} • ${c.reason} • ${c.status} • ${formatTimeShort(c.ts)}`
    const row = document.createElement('div')
    row.className = 'row'
    const b1 = document.createElement('button'); b1.className = 'info'; b1.textContent = 'En camino'; b1.onclick = async () => { await api(`/api/staff/waiter/${c.id}`, { method: 'POST', body: JSON.stringify({ status: 'en_camino' }) }); loadWaiterCalls() }
    const b2 = document.createElement('button'); b2.className = 'success'; b2.textContent = 'Atendido'; b2.onclick = async () => { await api(`/api/staff/waiter/${c.id}`, { method: 'POST', body: JSON.stringify({ status: 'atendido' }) }); loadWaiterCalls() }
    const b3 = document.createElement('button'); b3.className = 'danger'; b3.textContent = 'Cancelar'; b3.onclick = async () => { await api(`/api/staff/waiter/${c.id}`, { method: 'POST', body: JSON.stringify({ status: 'cancelado' }) }); loadWaiterCalls() }
    row.append(b1, b2, b3)
    div.append(info, row)
    container.append(div)
  }
  const bWaiter = q('badge-tab-waiter')
  if (bWaiter) { const v = list.length; bWaiter.classList.toggle('show', v > 0); bWaiter.textContent = v > 9 ? '9+' : String(v) }
  const bMenuWaiter = q('badge-menu-waiter')
  if (bMenuWaiter) { const v = list.length; bMenuWaiter.classList.toggle('show', v > 0); bMenuWaiter.textContent = v > 9 ? '9+' : String(v) }
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
  const sig = buildSig(r.reports || [], rep => [rep.fromId, rep.targetId, rep.category, rep.note].join('~'))
  S.ui = S.ui || {}
  if (S.ui.reportsSig === sig) return
  S.ui.reportsSig = sig
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
  const btnVenueRestaurant = q('btn-venue-restaurant'); if (btnVenueRestaurant) btnVenueRestaurant.onclick = () => { chooseVenueMode('restaurant') }
  const btnVenueDisco = q('btn-venue-disco'); if (btnVenueDisco) btnVenueDisco.onclick = () => { chooseVenueMode('disco') }
  
  const swAvail = q('switch-available'); if (swAvail) swAvail.onchange = setAvailable
  const receiveModeEl = q('receive-mode'); if (receiveModeEl) receiveModeEl.onchange = setAvailable
  const zoneEl = q('zone'); if (zoneEl) zoneEl.oninput = setAvailable
  const btnViewAvail = q('btn-view-available'); if (btnViewAvail) btnViewAvail.onclick = () => { setActiveNav('disponibles'); if (isRestaurantMode()) viewPromos(); else showAvailableChoice() }
  const btnViewMenu = q('btn-view-menu'); if (btnViewMenu) btnViewMenu.onclick = () => { setActiveNav('carta'); openMenu() }
  for (const b of document.querySelectorAll('.btn-invite-msg')) b.onclick = chooseMsg
  const btnInviteSend = q('btn-invite-send'); if (btnInviteSend) btnInviteSend.onclick = sendInvite
  const btnInviteAccept = q('btn-invite-accept'); if (btnInviteAccept) btnInviteAccept.onclick = () => respondInvite(true)
  const btnInvitePass = q('btn-invite-pass'); if (btnInvitePass) btnInvitePass.onclick = () => respondInvite(false)
  q('btn-meeting-cancel').onclick = cancelMeeting
  const btnMeetingHome = q('btn-meeting-home'); if (btnMeetingHome) btnMeetingHome.onclick = goHome
  const btnInviteConsumption = q('btn-invite-consumption'); if (btnInviteConsumption) btnInviteConsumption.onclick = openConsumption
  q('btn-consumption-send').onclick = sendConsumption
  const btnAddCart = q('btn-add-to-cart'); if (btnAddCart) btnAddCart.onclick = addToCart
  const btnViewOrders = q('btn-view-orders'); if (btnViewOrders) btnViewOrders.onclick = () => { setActiveNav('orders'); loadUserOrders(); loadUserInvitesHistory(); show('screen-orders-user') }
  const btnWaiterOrder = q('btn-waiter-order'); if (btnWaiterOrder) btnWaiterOrder.onclick = callWaiterOrder
  for (const b of document.querySelectorAll('.btn-waiter-reason')) b.onclick = chooseWaiterReason
  const btnBack = q('btn-back'); if (btnBack) btnBack.onclick = goBack
  const nc = q('nav-carta'), nd = q('nav-disponibles'), nm = q('nav-mesas'), no = q('nav-orders'), nf = q('nav-perfil')
  if (nc) nc.onclick = () => { setActiveNav('carta'); openMenu() }
  if (nd) nd.onclick = () => { setActiveNav('disponibles'); if (isRestaurantMode()) viewPromos(); else showAvailableChoice() }
  if (nm) nm.onclick = () => { setActiveNav('mesas'); openCallWaiter() }
  if (no) no.onclick = () => { setActiveNav('orders'); loadUserOrders(); show('screen-orders-user') }
  const btnShowAllInv = q('btn-invite-show-all'); if (btnShowAllInv) btnShowAllInv.onclick = openInvitesInbox
  const btnPassAllInv = q('btn-pass-all-invites'); if (btnPassAllInv) btnPassAllInv.onclick = passAllDanceInvites
  const btnPassAllCons = q('btn-pass-all-consumption'); if (btnPassAllCons) btnPassAllCons.onclick = passAllConsumptionInvites
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      S.appHidden = true
      try { if (S.sse) S.sse.close() } catch {}
      try { if (S.staffSSE) S.staffSSE.close() } catch {}
      try { if (S.timers.userPoll) { clearInterval(S.timers.userPoll); S.timers.userPoll = 0 } } catch {}
      try { if (S.timers.staffPoll) { clearInterval(S.timers.staffPoll); S.timers.staffPoll = 0 } } catch {}
      return
    }
    S.appHidden = false
    if (Array.isArray(S.missed) && S.missed.length) {
      const msg = S.missed.join('\n')
      showModal('Notificaciones', msg, 'info')
      S.missed = []
    }
    if (S.role === 'user' && S.user) {
      startEvents()
    } else if (S.role === 'staff' && S.sessionId) {
      startStaffEvents()
    }
  })
  if (nf) nf.onclick = () => { setActiveNav('perfil'); renderUserHeader(); show('screen-user-home') }
  const ua = q('user-alias'); if (ua) { ua.style.cursor = 'pointer'; ua.onclick = () => openEditProfileFocus('alias') }
  const ut = q('user-table'); if (ut) { ut.style.cursor = 'pointer'; ut.onclick = () => openEditProfileFocus('table') }
  const linkStaff = q('link-staff'); if (linkStaff) linkStaff.onclick = (e) => {
    e.preventDefault()
    let modeParam = ''
    try { const u = new URL(location.href); modeParam = u.searchParams.get('mode') || u.searchParams.get('restaurant') || '' } catch {}
    if (!modeParam) { show('screen-venue-type'); return }
    show('screen-staff-welcome')
  }
  const fab = q('fab-call'); if (fab) fab.onclick = openDJRequest
  const btnDjSend = q('btn-dj-send'); if (btnDjSend) btnDjSend.onclick = sendDJRequest
  const bAT = q('btn-avail-by-table'); if (bAT) bAT.onclick = exploreMesas
  const bAA = q('btn-avail-all'); if (bAA) bAA.onclick = viewAvailable
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
  const tabAnalytics = q('tab-staff-analytics'); if (tabAnalytics) tabAnalytics.onclick = () => showStaffTab('analytics')
  const tabDJ = q('tab-staff-dj'); if (tabDJ) tabDJ.onclick = () => showStaffTab('dj')
  const btnDJLoad = q('btn-staff-dj-load'); if (btnDJLoad) btnDJLoad.onclick = loadDJRequests
  const swDJ = q('staff-dj-switch'); if (swDJ) swDJ.onchange = async () => {
    const checked = swDJ.checked
    const ttlRaw = q('staff-dj-ttl')?.value.trim() || ''
    const ttl = Math.max(0, Number(ttlRaw || 0))
    const r = await api('/api/staff/dj/toggle', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, enabled: checked, ttlMinutes: checked ? ttl : 0 }) })
    renderDJStatus(r.enabled, r.until)
    loadDJRequests()
  }
  const btnDJApply = q('btn-staff-dj-apply'); if (btnDJApply) btnDJApply.onclick = async () => {
    const ttlRaw = q('staff-dj-ttl')?.value.trim() || ''
    const ttl = Math.max(0, Number(ttlRaw || 0))
    const r = await api('/api/staff/dj/toggle', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, enabled: true, ttlMinutes: ttl }) })
    renderDJStatus(r.enabled, r.until)
    loadDJRequests()
  }
  const djFilt = q('staff-dj-filter-table'); if (djFilt) { try { djFilt.value = localStorage.getItem('discos_staff_dj_filter_table') || '' } catch {} ; djFilt.oninput = () => { try { localStorage.setItem('discos_staff_dj_filter_table', djFilt.value.trim()) } catch {} } }
  const ordFilt = q('staff-orders-filter'); if (ordFilt) { try { const v = localStorage.getItem('discos_staff_orders_filter') || ''; if (v) ordFilt.value = v } catch {} ; ordFilt.onchange = () => { try { localStorage.setItem('discos_staff_orders_filter', ordFilt.value || '') } catch {}; loadOrders(ordFilt.value || '') } }
  const btnStaffAnalytics = q('btn-staff-analytics'); if (btnStaffAnalytics) btnStaffAnalytics.onclick = toggleAnalytics
  const menuPanel = q('menu-staff-panel'); if (menuPanel) menuPanel.onclick = () => showStaffTab('panel')
  const menuOrders = q('menu-staff-orders'); if (menuOrders) menuOrders.onclick = () => showStaffTab('orders')
  const menuMesas = q('menu-staff-mesas'); if (menuMesas) menuMesas.onclick = () => { showStaffTab('mesas'); const more = q('staff-menu-more'); if (more) more.style.display = 'none' }
  const menuUsers = q('menu-staff-users'); if (menuUsers) menuUsers.onclick = () => { showStaffTab('users'); const more = q('staff-menu-more'); if (more) more.style.display = 'none' }
  const menuWaiter = q('menu-staff-waiter'); if (menuWaiter) menuWaiter.onclick = () => { showStaffTab('waiter'); const more = q('staff-menu-more'); if (more) more.style.display = 'none' }
  const menuReportes = q('menu-staff-reportes'); if (menuReportes) menuReportes.onclick = () => { showStaffTab('reportes'); const more = q('staff-menu-more'); if (more) more.style.display = 'none' }
  const menuPromos = q('menu-staff-promos'); if (menuPromos) menuPromos.onclick = () => { showStaffTab('promos'); const more = q('staff-menu-more'); if (more) more.style.display = 'none' }
  const menuCatalog = q('menu-staff-catalog'); if (menuCatalog) menuCatalog.onclick = () => { showStaffTab('catalog'); const more = q('staff-menu-more'); if (more) more.style.display = 'none' }
  const btnMenuMore = q('menu-staff-more'); if (btnMenuMore) btnMenuMore.onclick = openMenuMoreModal
  q('btn-start-session-welcome').onclick = startStaffSession
  const btnScan = q('btn-scan-qr'); if (btnScan) btnScan.onclick = startScanQR
  q('btn-end-session').onclick = endStaffSession
  const btnExploreMesas = q('btn-explore-mesas'); if (btnExploreMesas) btnExploreMesas.onclick = exploreMesas
  q('btn-edit-profile').onclick = openEditProfile
  q('btn-edit-save').onclick = saveEditProfile
  // Pausa social eliminada
  const btnViewPromos = q('btn-view-promos'); if (btnViewPromos) btnViewPromos.onclick = viewPromos
  const btnCallWaiter = q('btn-call-waiter'); if (btnCallWaiter) btnCallWaiter.onclick = openCallWaiter
  const btnWaiterSend = q('btn-waiter-send'); if (btnWaiterSend) btnWaiterSend.onclick = sendWaiterCall
  q('mesa-only-available').onchange = () => loadMesaPeople(S.currentTableId)
  q('btn-select-table').onclick = openSelectTable
  const heroSelfie = q('user-selfie-hero'); if (heroSelfie) heroSelfie.onclick = () => { const url = S.user?.selfie || ''; if (url) showImageModal(url) }
  q('btn-select-table-save').onclick = saveSelectTable
  q('btn-meeting-confirm').onclick = confirmMeeting
  const endBtn = q('btn-end-dance'); if (endBtn) endBtn.onclick = finishDance
  const btnMeetCome = q('btn-meet-come'); if (btnMeetCome) btnMeetCome.onclick = () => setMeetingPlan('come')
  const btnMeetGo = q('btn-meet-go'); if (btnMeetGo) btnMeetGo.onclick = () => setMeetingPlan('go')
  const btnMeetPista = q('btn-meet-pista'); if (btnMeetPista) btnMeetPista.onclick = () => setMeetingPlan('pista')
  const btnStaffTableView = q('btn-staff-table-view'); if (btnStaffTableView) btnStaffTableView.onclick = viewStaffTableHistory
  const btnStaffTableClose = q('btn-staff-table-close'); if (btnStaffTableClose) btnStaffTableClose.onclick = closeStaffTable
  const btnStaff2TableView = q('btn-staff2-table-view'); if (btnStaff2TableView) btnStaff2TableView.onclick = viewStaffTableHistory2
  const btnStaff2TableClose = q('btn-staff2-table-close'); if (btnStaff2TableClose) btnStaff2TableClose.onclick = closeStaffTable2
  const btnStaffCatalogSave = q('btn-staff-catalog-save'); if (btnStaffCatalogSave) btnStaffCatalogSave.onclick = saveStaffCatalog
  const btnStaffCatalogUseGlobal = q('btn-staff-catalog-use-global'); if (btnStaffCatalogUseGlobal) btnStaffCatalogUseGlobal.onclick = useGlobalCatalog
  const btnStaffCatalogCopyGlobal = q('btn-staff-catalog-copy-global'); if (btnStaffCatalogCopyGlobal) btnStaffCatalogCopyGlobal.onclick = () => copyGlobalToSession(false)
  const btnStaffCatalogAdd = q('btn-staff-catalog-add'); if (btnStaffCatalogAdd) btnStaffCatalogAdd.onclick = () => {
    const name = q('staff-catalog-add-name')?.value.trim()
    const price = Number(q('staff-catalog-add-price')?.value || 0)
    const catVal = (q('staff-catalog-add-category')?.value || 'otros').toLowerCase()
    const subVal = (q('staff-catalog-add-subcategory')?.value || '').trim()
    const descVal = (q('staff-catalog-add-description')?.value || '').trim()
    const container = q('staff-catalog-list')
    if (!container || !name) return
    if (isRestaurantMode() && !descVal) { showError('Agrega una descripción'); return }
    const row = document.createElement('div')
    row.className = 'row'
    const nameInput = document.createElement('input'); nameInput.type = 'text'; nameInput.placeholder = 'Nombre'; nameInput.value = name; nameInput.className = 'catalog-name'
    const priceInput = document.createElement('input'); priceInput.type = 'number'; priceInput.min = '0'; priceInput.value = price; priceInput.className = 'catalog-price'
    const category = document.createElement('select'); category.className = 'catalog-cat'
    for (const opt of ['cervezas','botellas','cocteles','sodas','otros']) {
      const o = document.createElement('option')
      o.value = opt
      o.textContent = opt.charAt(0).toUpperCase() + opt.slice(1)
      category.append(o)
    }
    category.value = catVal
    const subInput = document.createElement('input'); subInput.type = 'text'; subInput.placeholder = 'Subcategoría'; subInput.value = subVal; subInput.className = 'catalog-sub'
    const descInput = document.createElement('input'); descInput.type = 'text'; descInput.placeholder = 'Descripción'; descInput.value = descVal; descInput.className = 'catalog-desc'
    nameInput.oninput = scheduleCatalogSave
    priceInput.oninput = scheduleCatalogSave
    category.oninput = scheduleCatalogSave
    subInput.oninput = scheduleCatalogSave
    descInput.oninput = scheduleCatalogSave
    const del = document.createElement('button'); del.className = 'danger'; del.textContent = 'Eliminar'; del.onclick = () => { try { row.remove(); scheduleCatalogSave() } catch {} }
    row.append(nameInput, priceInput, category, subInput, descInput, del)
    container.append(row)
    const inpName = q('staff-catalog-add-name'); if (inpName) inpName.value = ''
    const inpPrice = q('staff-catalog-add-price'); if (inpPrice) inpPrice.value = ''
    const inpCat = q('staff-catalog-add-category'); if (inpCat) inpCat.value = 'otros'
    const inpSub = q('staff-catalog-add-subcategory'); if (inpSub) inpSub.value = ''
    const inpDesc = q('staff-catalog-add-description'); if (inpDesc) inpDesc.value = ''
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
      let baseCandidate = ''
      try {
        const pb = await api(`/api/session/public-base?sessionId=${encodeURIComponent(S.sessionId)}`)
        baseCandidate = (pb.publicBaseUrl || '').trim()
      } catch {}
      const base = baseCandidate || location.origin
      const modeQuery = isRestaurantMode() ? '&mode=restaurant' : ''
      const href = `${base}/?venueId=${encodeURIComponent(S.venueId || 'default')}&aj=1${modeQuery}`
      await navigator.clipboard.writeText(href)
      showError('Link copiado')
      setTimeout(() => showError(''), 1000)
    } catch (e) { showError('No se pudo copiar') }
  }
  const copyStaffBtn = q('btn-copy-link-staff')
  if (copyStaffBtn) copyStaffBtn.onclick = async () => {
    try {
      let baseCandidate = ''
      try {
        const pb = await api(`/api/session/public-base?sessionId=${encodeURIComponent(S.sessionId)}`)
        baseCandidate = (pb.publicBaseUrl || '').trim()
      } catch {}
      const base = baseCandidate || location.origin
      const modeQuery = isRestaurantMode() ? '&mode=restaurant' : ''
      const href = `${base}/?venueId=${encodeURIComponent(S.venueId || 'default')}&staff=1${modeQuery}`
      await navigator.clipboard.writeText(href)
      showError('Link Staff copiado')
      setTimeout(() => showError(''), 1000)
    } catch (e) { showError('No se pudo copiar') }
  }
  const copyDJBtn = q('btn-copy-link-dj')
  if (copyDJBtn) copyDJBtn.onclick = async () => {
    try {
      let baseCandidate = ''
      try {
        const pb = await api(`/api/session/public-base?sessionId=${encodeURIComponent(S.sessionId)}`)
        baseCandidate = (pb.publicBaseUrl || '').trim()
      } catch {}
      const base = baseCandidate || location.origin
      const modeQuery = isRestaurantMode() ? '&mode=restaurant' : ''
      const href = `${base}/?venueId=${encodeURIComponent(S.venueId || 'default')}&dj=1${modeQuery}`
      await navigator.clipboard.writeText(href)
      showError('Link DJ copiado')
      setTimeout(() => showError(''), 1000)
    } catch (e) { showError('No se pudo copiar') }
  }
  const catalogSearch = q('catalog-search'); if (catalogSearch) catalogSearch.oninput = () => scheduleLater('catalog_search', applyCatalogSearch, 320)
  const btnWelcomeVenuePinSend = q('btn-welcome-venue-pin-send'); if (btnWelcomeVenuePinSend) btnWelcomeVenuePinSend.onclick = sendVenuePinAdminWelcome
  const savePB = q('btn-save-public-base')
  if (savePB) savePB.onclick = async () => {
    try {
      const val = q('public-base').value.trim()
      await api('/api/session/public-base', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, publicBaseUrl: val, pin: S.sessionPin || '' }) })
      showError('URL pública guardada')
      setTimeout(() => showError(''), 1000)
      await startStaffSession()
    } catch (e) { showError(String(e.message)) }
  }
}
function openDJRequest() {
  checkDJStatus().then(ok => {
    if (!ok) { showError('Pedidos al DJ desactivados por Staff'); setTimeout(() => showError(''), 1500); return }
    show('screen-dj-request')
    startDJUserCountdown()
  })
}
async function sendDJRequest() {
  const song = q('dj-song')?.value.trim()
  if (!song) { showError('Escribe una canción'); setTimeout(() => showError(''), 1200); return }
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  const okStatus = await checkDJStatus()
  if (!okStatus) { showError('Pedidos al DJ desactivados por Staff'); setTimeout(() => showError(''), 1500); return }
  const ok = await confirmAction(`Vas a pedir: "${song}" para tu mesa ${S.user.tableId}. ¿Confirmas?`)
  if (!ok) return
  await api('/api/dj/request', { method: 'POST', body: JSON.stringify({ userId: S.user.id, song }) })
  showError('Solicitud enviada al DJ')
  setTimeout(() => showError(''), 1200)
  show('screen-user-home')
}
async function checkDJStatus() {
  try {
    const r = await api(`/api/dj/status?sessionId=${encodeURIComponent(S.sessionId)}`)
    return !!(r && r.enabled && (!r.until || Date.now() < Number(r.until || 0)))
  } catch { return false }
}
async function startDJUserCountdown() {
  try { if (S.timers.djUserCountdown) { clearInterval(S.timers.djUserCountdown); S.timers.djUserCountdown = 0 } } catch {}
  try {
    const r = await api(`/api/dj/status?sessionId=${encodeURIComponent(S.sessionId)}`)
    renderUserDJStatus(!!r.enabled, Number(r.until || 0))
    const chip = q('dj-now-playing')
    if (chip && r.current && r.current.song) {
      const who = r.current.alias ? ` • ${r.current.alias}` : ''
      const mesa = r.current.tableId ? ` • Mesa ${r.current.tableId}` : ''
      const base = (r.current.state === 'programado') ? 'Programado' : 'Está sonando'
      chip.textContent = `${base}: ${r.current.song}${who}${mesa}`
      chip.classList.toggle('dj-programado', r.current.state === 'programado')
      chip.style.display = 'inline-block'
    }
  } catch { renderUserDJStatus(false, 0) }
  S.timers.djUserCountdown = setInterval(() => {
    renderUserDJStatus(!!S.djUserEnabled, Number(S.djUserUntil || 0))
  }, 1000)
}
function renderUserDJStatus(enabled, until) {
  S.djUserEnabled = !!enabled
  S.djUserUntil = Number(until || 0)
  const el = q('dj-request-ttl')
  const btn = q('btn-dj-send')
  if (!el || !btn) return
  const now = Date.now()
  const active = enabled && (!until || now < until)
  btn.disabled = !active
  el.style.display = active ? 'inline-block' : 'none'
  if (active && until) {
    let remSec = (((until - now) + 999) / 1000) | 0
    if (remSec < 0) remSec = 0
    const mm = ((remSec / 60) | 0)
    const ss = (remSec % 60)
    const txt = `${mm}:${ss < 10 ? ('0' + ss) : ss}`
    el.textContent = `Tiempo para pedir: ${txt}`
  }
}
async function loadDJRequests() {
  const tRaw = q('staff-dj-filter-table')?.value.trim() || ''
  const t = normalizeTableId(tRaw)
  const qs = t ? `&tableId=${encodeURIComponent(t)}` : ''
  const r = await api(`/api/staff/dj?sessionId=${encodeURIComponent(S.sessionId)}${qs}`)
  const st = await api(`/api/staff/dj/status?sessionId=${encodeURIComponent(S.sessionId)}`).catch(()=>({enabled:false,until:0}))
  renderDJStatus(!!st.enabled, Number(st.until || 0))
  const container = q('staff-dj-list')
  if (!container) return
  const listAsc = (r.requests || []).slice().sort((a, b) => Number(a.ts || 0) - Number(b.ts || 0))
  const sig = JSON.stringify(listAsc.map(it => `${it.id}:${it.status}`))
  if (S.djListSig && S.djListSig === sig) {
    { const b = q('badge-tab-dj'); if (b) { const pending = listAsc.filter(it => it.status === 'pendiente').length; b.classList.toggle('show', pending > 0); b.textContent = pending > 9 ? '9+' : String(pending) } }
    return
  }
  container.innerHTML = ''
  { const b = q('badge-tab-dj'); if (b) { const pending = listAsc.filter(it => it.status === 'pendiente').length; b.classList.toggle('show', pending > 0); b.textContent = pending > 9 ? '9+' : String(pending) } }
  for (const it of listAsc) {
    if (it.status === 'terminado') continue
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    info.textContent = `Mesa ${it.tableId || '-'} • ${it.userAlias || ''} • ${it.song}`
    const chip = document.createElement('span')
    chip.className = 'chip ' + it.status
    chip.textContent = it.status
    info.append(chip)
    const row = document.createElement('div')
    row.className = 'row'
    const b1 = document.createElement('button'); b1.className = 'info'; b1.textContent = 'Programar'; b1.onclick = async () => { await api(`/api/staff/dj/${it.id}`, { method: 'POST', body: JSON.stringify({ status: 'programado' }) }); scheduleStaffDJUpdate() }
    const b4 = document.createElement('button'); b4.className = 'success'; b4.textContent = 'Finalizar'; b4.onclick = async () => { await api(`/api/staff/dj/${it.id}`, { method: 'POST', body: JSON.stringify({ status: 'terminado' }) }); scheduleStaffDJUpdate() }
    row.append(b1, b4)
    div.append(info, row)
    container.append(div)
  }
  S.djListSig = sig
}
function renderDJStatus(enabled, until) {
  const statusEl = q('staff-dj-status')
  const sw = q('staff-dj-switch')
  const cd = q('staff-dj-countdown')
  const stChip = q('staff-dj-state-chip')
  if (sw) sw.checked = !!enabled
  if (statusEl) {
    if (enabled) {
      const untilTxt = until ? `hasta ${formatTimeShort(Number(until))}` : 'sin límite'
      statusEl.textContent = `DJ pedidos: activados (${untilTxt})`
    } else {
      statusEl.textContent = 'DJ pedidos: desactivados'
    }
  }
  if (stChip) {
    stChip.textContent = enabled ? 'Activado' : 'Desactivado'
    stChip.classList.remove('state-on', 'state-off')
    stChip.classList.add(enabled ? 'state-on' : 'state-off')
  }
  S.djUntil = Number(until || 0)
  try { if (S.timers.djCountdown) { clearInterval(S.timers.djCountdown); S.timers.djCountdown = 0 } } catch {}
  if (enabled && S.djUntil) {
    const tick = () => {
      const remMs = S.djUntil - Date.now()
      let remSec = ((remMs + 999) / 1000) | 0
      if (remSec < 0) remSec = 0
      const mm = ((remSec / 60) | 0)
      const ss = (remSec % 60)
      const txt = `${mm}:${ss < 10 ? ('0' + ss) : ss}`
      if (cd) cd.textContent = txt
      if (remSec <= 0) {
        if (cd) cd.textContent = ''
        if (sw) sw.checked = false
        if (statusEl) statusEl.textContent = 'DJ pedidos: desactivados'
        try { clearInterval(S.timers.djCountdown); S.timers.djCountdown = 0 } catch {}
      }
    }
    tick()
    S.timers.djCountdown = setInterval(tick, 1000)
  } else {
    if (cd) cd.textContent = ''
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
    div.className = 'item'
    const top = document.createElement('div')
    top.className = 'avail-top'
    const info = document.createElement('div')
    info.className = 'avail-info'
    const alias = document.createElement('div')
    alias.className = 'alias'
    alias.textContent = `Mesa ${m.tableId}`
    const sub = document.createElement('div')
    sub.className = 'avail-sub'
    sub.textContent = `Personas ${m.people} • Disponibles ${m.disponibles}`
    info.append(alias, sub)
    top.append(info)
    const tags = document.createElement('div')
    tags.className = 'tags'
    if (Array.isArray(m.tags)) {
      for (const t of m.tags) {
        const chip = document.createElement('span')
        chip.className = 'chip'
        chip.textContent = t
        tags.append(chip)
      }
    }
    const row = document.createElement('div')
    row.className = 'row compact'
    const btn = document.createElement('button')
    btn.className = 'info'
    btn.textContent = 'Ver mesa'
    btn.onclick = () => openMesaView(m.tableId)
    row.append(btn)
    div.append(top, tags, row)
    container.append(div)
  }
}

function openMesaView(tableId) {
  S.currentTableId = tableId
  q('mesa-title').textContent = `Mesa ${tableId}`
  loadMesaPeople(tableId)
  const myTable = String(S.user?.tableId || '')
  if (myTable && myTable === String(tableId)) {
    loadMesaOrders(tableId)
  } else {
    const container = q('mesa-orders')
    if (container) {
      container.innerHTML = ''
      const div = document.createElement('div')
      div.className = 'card'
      div.textContent = 'Solo puedes ver el historial de tu mesa'
      container.append(div)
    }
  }
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
    const top = document.createElement('div')
    top.className = 'avail-top'
    const img = document.createElement('img')
    img.className = 'avail-avatar'
    img.width = 44; img.height = 44
    img.src = u.selfie || ''
    if (u.selfie) img.onclick = () => showImageModal(u.selfie)
    const info = document.createElement('div')
    info.className = 'avail-info'
    const alias = document.createElement('div')
    alias.className = 'alias'
    alias.textContent = u.alias || u.id
    const sub = document.createElement('div')
    sub.className = 'avail-sub'
    const parts = []
    if (u.tableId) parts.push(`Mesa ${u.tableId}`)
    if (u.gender) parts.push(genderLabel(u.gender))
    sub.textContent = parts.join(' • ')
    info.append(alias, sub)
    top.append(img, info)
    const row = document.createElement('div')
    row.className = 'row compact'
    const bDance = document.createElement('button')
    bDance.className = 'info'
    const busy = (u.danceState && u.danceState !== 'idle')
    bDance.textContent = busy ? 'Ocupado' : 'Invitar a bailar 💃'
    bDance.disabled = !!busy
    bDance.onclick = () => openInvite(u)
    const bConsumo = document.createElement('button')
    bConsumo.className = 'success'
    bConsumo.textContent = 'Invitar una copa 🥂'
    bConsumo.onclick = () => { setReceiver(u); q('consumption-target').value = u.id; openConsumption() }
    row.append(bDance, bConsumo)
    div.append(top, row)
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
    await ensureCatalogIndex()
    const label = formatOrderProductFull(o.product)
    info.textContent = `${label} • ${o.emitterAlias || o.emitterId} → ${o.receiverAlias || o.receiverId}`
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

// Reacciones retiradas (saludo, brindis): UI y backend eliminados

function openEditProfile() {
  q('edit-alias').value = S.user.alias || ''
  q('edit-tags').value = (Array.isArray(S.user.prefs?.tags) ? S.user.prefs.tags.join(',') : '')
  q('edit-table').value = S.user.tableId || ''
  show('screen-edit-profile')
}
async function renderUserHeader() {
  const ua = q('user-alias'), us = q('user-selfie'), ut = q('user-table')
  if (ua) ua.textContent = S.user?.alias || S.user?.id || ''
  if (us) us.src = S.user?.selfie || ''
  if (ut) ut.textContent = S.user?.tableId || '-'
  const ush = q('user-selfie-hero'); if (ush) ush.src = S.user?.selfie || ''
  const hm = q('user-hero-main')
  if (hm) hm.textContent = S.user?.alias || S.user?.id || 'Tu perfil'
  const tc = q('user-table-chip'); if (tc) tc.textContent = `Mesa ${S.user?.tableId || '-'}`
  const uds = q('user-dance-status')
  if (uds) {
    const st = S.user?.danceState || 'idle'
    const p = S.user?.partnerAlias || ''
    uds.textContent = st === 'waiting' ? (`Esperando para bailar con ${p || 'pareja'}`) :
                      st === 'dancing' ? (`Bailando con ${p || 'pareja'}`) : ''
  }
  const hs = q('user-hero-sub')
  if (hs) {
    const st = S.user?.danceState || 'idle'
    const p = S.user?.partnerAlias || ''
    hs.textContent = st === 'waiting' ? (`Esperando para bailar con ${p || 'pareja'}`) :
                     st === 'dancing' ? (`Bailando con ${p || 'pareja'}`) : ''
  }
  const endBtn = q('btn-end-dance')
  if (endBtn) {
    const st = S.user?.danceState || 'idle'
    endBtn.style.display = (st === 'dancing') ? '' : 'none'
  }
  const header = q('user-header')
  const eq = q('user-equalizer')
  const heq = q('user-hero-equalizer')
  const st2 = S.user?.danceState || 'idle'
  if (header) header.classList.toggle('party', st2 === 'dancing')
  if (eq) eq.style.display = st2 === 'dancing' ? '' : 'none'
  if (heq) heq.style.display = st2 === 'dancing' ? '' : 'none'
  const vf = q('user-venue-footer')
  if (vf) {
    try {
      if (!S.venueName) {
        const sess = await api(`/api/session/active${S.venueId ? ('?venueId=' + encodeURIComponent(S.venueId)) : ''}`)
        S.venueName = sess.venueName || S.venueId || ''
      }
      vf.textContent = S.venueName || S.venueId || ''
    } catch {}
  }
  try {
    const st = await api(`/api/dj/status?sessionId=${encodeURIComponent(S.sessionId)}`)
    const chip = q('dj-now-playing')
    if (chip) {
      if (st && st.current && st.current.song) {
        const who = st.current.alias ? ` • ${st.current.alias}` : ''
        const mesa = st.current.tableId ? ` • Mesa ${st.current.tableId}` : ''
        const base = (st.current.state === 'programado') ? 'Programado' : 'Está sonando'
        chip.textContent = `${base}: ${st.current.song}${who}${mesa}`
        chip.classList.toggle('dj-programado', st.current.state === 'programado')
        chip.style.display = 'inline-block'
      } else {
        chip.style.display = 'none'
        chip.textContent = ''
        chip.classList.remove('dj-programado')
      }
    }
  } catch {}
}
function openEditProfileFocus(field) {
  openEditProfile()
  const map = { alias: 'edit-alias', tags: 'edit-tags', table: 'edit-table' }
  const id = map[field]
  const el = id ? q(id) : null
  if (el) el.focus()
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

 

function openCallWaiter() {
  S.waiterReason = ''
  S.waiterCustomReason = ''
  for (const b of document.querySelectorAll('.btn-waiter-reason')) { b.classList.remove('active') }
  show('screen-call-waiter')
}

async function sendWaiterCall() {
  let reason = ''
  if (S.waiterReason === 'hielo') reason = 'Me puedes traer hielo'
  else if (S.waiterReason === 'pasabocas') reason = 'Me puedes traer pasabocas'
  else if (S.waiterReason === 'limpieza') reason = '¿Puedes limpiar la mesa?'
  else if (S.waiterReason === 'cuenta') reason = '¿Nos traes la cuenta?'
  else if (S.waiterReason === 'agua') reason = 'Me puedes traer agua'
  else if (S.waiterReason === 'cubiertos') reason = 'Me puedes traer cubiertos'
  else if (S.waiterReason === 'servilletas') reason = 'Me puedes traer servilletas'
  else if (S.waiterReason === 'custom') reason = (S.waiterCustomReason || 'Atención')
  else { reason = 'Atención' }
  const phr = reason ? `Vas a llamar al mesero: ${reason}. ¿Confirmas?` : `Vas a llamar al mesero. ¿Confirmas?`
  const ok = await confirmAction(phr)
  if (!ok) return
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  await api('/api/waiter/call', { method: 'POST', body: JSON.stringify({ userId: S.user.id, reason: (reason || 'Atención') }) })
  showError('Mesero llamado')
  setTimeout(() => showError(''), 1000)
  show('screen-user-home')
}
async function callWaiterQuick() {
  const ok = await confirmAction('¿Confirmas llamar al mesero?')
  if (!ok) return
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  await api('/api/waiter/call', { method: 'POST', body: JSON.stringify({ userId: S.user.id, reason: 'Atención' }) })
  showError('Mesero llamado')
  setTimeout(() => showError(''), 1000)
  show('screen-user-home')
}
async function callWaiterOrder() {
  const ok = await confirmAction(`Vas a pedir tomar orden en tu mesa ${S.user.tableId || '-'}. ¿Confirmas?`)
  if (!ok) return
  if (!S.user.tableId) { showError('Debes seleccionar tu mesa'); setTimeout(() => showError(''), 1200); openSelectTable(); return }
  await api('/api/waiter/call', { method: 'POST', body: JSON.stringify({ userId: S.user.id, reason: 'Tomar orden en mesa' }) })
  showError('Mesero pedido para tomar orden')
  setTimeout(() => showError(''), 1200)
}
async function chooseWaiterReason(e) {
  const el = e.currentTarget
  const val = el && el.getAttribute('data-reason') || ''
  if (val === 'otro') {
    const txt = await promptInput('Llamar mesero', 'Escribe tu pedido…')
    const clean = String(txt || '').trim()
    if (!clean) return
    S.waiterReason = 'custom'
    S.waiterCustomReason = clean
  } else {
    S.waiterReason = val
    S.waiterCustomReason = ''
  }
  for (const b of document.querySelectorAll('.btn-waiter-reason')) b.classList.remove('active')
  el.classList.add('active')
  if (S.waiterReason) { sendWaiterCall() }
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
  const listAsc = (r.orders || []).slice().sort((a, b) => {
    const ta = Number(a.createdAt || 0), tb = Number(b.createdAt || 0)
    if (ta !== tb) return ta - tb
    const ia = String(a.id || ''), ib = String(b.id || '')
    return ia.localeCompare(ib)
  })
  for (const o of listAsc) {
    const div = document.createElement('div')
    div.className = 'card'
    const chip = document.createElement('span')
    chip.className = 'chip ' + (o.status === 'pendiente_cobro' ? 'pending' : o.status)
    chip.textContent = o.status.replace('_',' ')
    const invChip = document.createElement('span')
    if (o.isInvitation) { invChip.className = 'chip'; invChip.textContent = 'Invitación' }
    const forEmitter = o.emitterId === S.user.id
    const amountTxt = ` • $${o.total || 0}`
    const otherAlias = forEmitter ? (o.receiverAlias || o.receiverId) : (o.emitterAlias || o.emitterId)
    await ensureCatalogIndex()
    const label = formatOrderProductFull(o.product)
    const timeTxt = o.createdAt ? ` • ${formatTimeShort(o.createdAt)}` : ''
    div.textContent = `${label} x${o.quantity || 1}${amountTxt} • ${forEmitter ? 'Enviado a' : 'Recibido de'} ${otherAlias}${timeTxt}`
    div.append(chip)
    if (o.isInvitation) div.append(invChip)
    container.append(div)
  }
}
async function loadPendingInvites() {
  try {
    if (!S.user || !S.user.id) return
    const r = await api(`/api/user/invites?userId=${encodeURIComponent(S.user.id)}`)
    const list = Array.isArray(r.invites) ? r.invites : []
    for (const inv of list) {
      S.invitesQueue.push({ type: 'dance', id: inv.id, invite: { id: inv.id, from: inv.from, expiresAt: Number(inv.expiresAt || 0) } })
      S.notifications.invites = (S.notifications.invites || 0) + 1
    }
    setBadgeNav('disponibles', S.notifications.invites)
    if (list.length > 0 && !S.inInviteFlow) { showNextInvite() }
  } catch {}
}
function showNextInvite() {
  if (isRestaurantMode()) {
    S.invitesQueue = []
    S.notifications.invites = 0
    setBadgeNav('disponibles', 0)
    S.inInviteFlow = false
    return
  }
  if (S.inInviteFlow) return
  const next = S.invitesQueue.shift()
  if (!next) return
  S.inInviteFlow = true
  if (next.type === 'dance') {
    S.currentInvite = { id: next.invite.id, from: next.invite.from, expiresAt: Number(next.invite.expiresAt || 0) }
    const mesaTxt = S.currentInvite.from.tableId ? ` • Mesa ${S.currentInvite.from.tableId}` : ''
    const zoneTxt = S.currentInvite.from.zone ? ` • Zona ${S.currentInvite.from.zone}` : ''
    const g = genderLabel(S.currentInvite.from.gender)
    const gTxt = g ? ` • ${g}` : ''
    const info = q('invite-received-info'); if (info) info.textContent = `${S.currentInvite.from.alias}${gTxt} te invita${mesaTxt}${zoneTxt}`
    const img = q('invite-from-selfie'); if (img) img.src = S.currentInvite.from.selfie || ''
    show('screen-invite-received')
    openInviteModal(S.currentInvite.expiresAt)
  } else if (next.type === 'consumption') {
    S.consumptionReq = next.data
    const msg = S.consumptionReq.note ? ` • Mensaje: ${S.consumptionReq.note}` : ''
    const mesaTxt = S.consumptionReq.from.tableId ? ` • Mesa ${S.consumptionReq.from.tableId}` : ''
    const listTxt = (Array.isArray(S.consumptionReq.items) ? S.consumptionReq.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : S.consumptionReq.product)
    const g = genderLabel(S.consumptionReq.from.gender)
    const gTxt = g ? ` • ${g}` : ''
    const info = q('invite-received-info'); if (info) info.textContent = `${S.consumptionReq.from.alias}${gTxt} te invita ${listTxt}${mesaTxt}${msg}`
    stopInviteCountdown()
    show('screen-invite-received')
    S.skipConfirmInvite = true
    openInviteModal(Number(S.consumptionReq.expiresAt || 0))
  }
  S.notifications.invites = Math.max(0, (S.notifications.invites || 0) - 1)
  setBadgeNav('disponibles', S.notifications.invites)
}
function openInvitesInbox() {
  if (isRestaurantMode()) return
  const container = q('invites-inbox'); if (!container) return
  container.innerHTML = ''
  const snapshot = []
  if (S.currentInvite) snapshot.push({ type: 'dance', id: S.currentInvite.id, invite: { id: S.currentInvite.id, from: S.currentInvite.from, expiresAt: S.currentInvite.expiresAt } })
  if (S.consumptionReq) snapshot.push({ type: 'consumption', data: S.consumptionReq })
  const list = [...snapshot, ...S.invitesQueue]
  for (const item of list) {
    const div = document.createElement('div')
    div.className = 'card'
    if (item.type === 'dance') {
      const from = item.invite.from || {}
      const ttl = item.invite.expiresAt ? ` • expira ${formatTimeShort(item.invite.expiresAt)}` : ''
      const g = genderLabel(from.gender)
      const gTxt = g ? ` • ${g}` : ''
      div.textContent = `Baile de ${from.alias || from.id}${gTxt}${ttl}`
      const row = document.createElement('div'); row.className = 'row'
      const bA = document.createElement('button'); bA.className = 'success'; bA.textContent = 'Aceptar'; bA.onclick = async () => { await api('/api/invite/respond', { method: 'POST', body: JSON.stringify({ inviteId: item.id, action: 'accept', note: '' }) }); show('screen-user-home') }
      const bP = document.createElement('button'); bP.className = 'warning'; bP.textContent = 'Pasar'; bP.onclick = async () => { await api('/api/invite/respond', { method: 'POST', body: JSON.stringify({ inviteId: item.id, action: 'pass', note: '' }) }); S.invitesQueue = S.invitesQueue.filter(x => !(x.type === 'dance' && String(x.id || '') === String(item.id || ''))); openInvitesInbox() }
      row.append(bA, bP); div.append(row)
    } else if (item.type === 'consumption') {
      const data = item.data
      const listTxt = (Array.isArray(data.items) ? data.items.map(it => `${it.quantity} x ${it.product}`).join(', ') : `${data.quantity || 1} x ${data.product}`)
      const g = genderLabel(data.from.gender)
      const gTxt = g ? ` • ${g}` : ''
      div.textContent = `Consumo de ${data.from.alias}${gTxt}: ${listTxt}`
      const row = document.createElement('div'); row.className = 'row'
      const bA = document.createElement('button'); bA.className = 'success'; bA.textContent = 'Aceptar'; bA.onclick = async () => { if (Array.isArray(data.items) && data.items.length) { await api('/api/consumption/respond/bulk', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, items: data.items, action: 'accept', requestId: data.requestId || '' }) }) } else { const qty = Math.max(1, Number(data.quantity || 1)); await api('/api/consumption/respond', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, product: data.product, quantity: qty, action: 'accept', requestId: data.requestId || '' }) }) } show('screen-user-home') }
      const bP = document.createElement('button'); bP.className = 'warning'; bP.textContent = 'Pasar'; bP.onclick = async () => { if (Array.isArray(data.items) && data.items.length) { await api('/api/consumption/respond/bulk', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, items: data.items, action: 'pass', requestId: data.requestId || '' }) }) } else { await api('/api/consumption/respond', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, product: data.product, action: 'pass', requestId: data.requestId || '' }) }) } S.invitesQueue = S.invitesQueue.filter(x => !(x.type === 'consumption' && x.data && x.data.requestId === data.requestId)); openInvitesInbox() }
      row.append(bA, bP); div.append(row)
    }
    container.append(div)
  }
  show('screen-invites-inbox')
}
async function passAllDanceInvites() {
  const ids = []
  if (S.currentInvite && S.currentInvite.id) ids.push(S.currentInvite.id)
  for (const it of S.invitesQueue) if (it.type === 'dance' && it.id) ids.push(it.id)
  if (ids.length === 0) { showModal('Invitaciones', 'No hay invitaciones de baile para pasar', 'info'); return }
  for (const id of ids) { try { await api('/api/invite/respond', { method: 'POST', body: JSON.stringify({ inviteId: id, action: 'pass', note: '' }) }) } catch {} }
  S.invitesQueue = S.invitesQueue.filter(x => x.type !== 'dance')
  stopInviteCountdown()
  S.currentInvite = null
  S.inInviteFlow = false
  showNextInvite()
  openInvitesInbox()
  showSuccess('Invitaciones de baile pasadas')
}
async function passAllConsumptionInvites() {
  const items = []
  if (S.consumptionReq) items.push(S.consumptionReq)
  for (const it of S.invitesQueue) if (it.type === 'consumption' && it.data) items.push(it.data)
  if (items.length === 0) { showModal('Invitaciones', 'No hay invitaciones de consumo para pasar', 'info'); return }
  for (const data of items) {
    try {
      if (Array.isArray(data.items) && data.items.length) {
        await api('/api/consumption/respond/bulk', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, items: data.items, action: 'pass', requestId: data.requestId || '' }) })
      } else {
        await api('/api/consumption/respond', { method: 'POST', body: JSON.stringify({ fromId: data.from.id, toId: S.user.id, product: data.product, action: 'pass', requestId: data.requestId || '' }) })
      }
    } catch {}
  }
  S.invitesQueue = S.invitesQueue.filter(x => x.type !== 'consumption')
  S.consumptionReq = null
  S.inInviteFlow = false
  showNextInvite()
  openInvitesInbox()
  showSuccess('Invitaciones de consumo pasadas')
}
function startInviteCountdown(expiresAt) {
  try { if (S.timers && S.timers.inviteCountdown) { clearInterval(S.timers.inviteCountdown); S.timers.inviteCountdown = 0 } } catch {}
  const target = Number(expiresAt || 0)
  if (!target) {
    const txtRing = q('invite-ring-modal-txt') || q('invite-ring-txt')
    const elRing = q('invite-ring-modal') || q('invite-ring')
    const txtCons = q('consume-exp-text')
    if (txtRing) txtRing.textContent = ''
    if (elRing) elRing.style.setProperty('--deg','0deg')
    if (txtCons) txtCons.textContent = ''
    return
  }
  const elRing = q('invite-ring-modal') || q('invite-ring')
  const txtRing = q('invite-ring-modal-txt') || q('invite-ring-txt')
  const txtCons = q('consume-exp-text')
  if (!elRing && !txtRing && !txtCons) return
  S.inviteTTL = (((target - Date.now() + 999) / 1000) | 0); if (S.inviteTTL < 1) S.inviteTTL = 1
  const tick = () => {
    const remMs = target - Date.now()
    let remSec = ((remMs + 999) / 1000) | 0
    if (remSec < 0) remSec = 0
    if (remSec <= 0) {
      const mmss = '00:00'
      if (txtRing) txtRing.textContent = mmss
      if (txtCons) txtCons.textContent = mmss
      if (elRing) elRing.style.setProperty('--deg', '360deg')
      try { clearInterval(S.timers.inviteCountdown) } catch {}
      S.timers.inviteCountdown = 0
      return
    }
    const mm = String((remSec / 60) | 0).padStart(2, '0')
    const ss = String(remSec % 60).padStart(2, '0')
    const mmss = `${mm}:${ss}`
    if (txtRing) txtRing.textContent = mmss
    if (txtCons) txtCons.textContent = mmss
    const base = S.inviteTTL > 0 ? S.inviteTTL : 1
    const deg = ((remSec * 360) / base) | 0
    if (elRing) elRing.style.setProperty('--deg', `${deg}deg`)
  }
  tick()
  S.timers.inviteCountdown = setInterval(tick, 1000)
}
function stopInviteCountdown() {
  try { if (S.timers && S.timers.inviteCountdown) { clearInterval(S.timers.inviteCountdown); S.timers.inviteCountdown = 0 } } catch {}
  const el = q('invite-ring-modal') || q('invite-ring'); if (el) el.style.setProperty('--deg', '0deg')
  const txt = q('invite-ring-modal-txt') || q('invite-ring-txt'); if (txt) txt.textContent = ''
  const txt2 = q('consume-exp-text'); if (txt2) txt2.textContent = ''
}
async function loadUserInvitesHistory() {
  const r = await api(`/api/user/invites/history?userId=${encodeURIComponent(S.user.id)}`)
  const container = q('user-invites')
  if (!container) return
  container.innerHTML = ''
  const listAsc = (r.invites || []).slice().sort((a, b) => {
    const ta = Number(a.createdAt || 0), tb = Number(b.createdAt || 0)
    if (ta !== tb) return ta - tb
    const ia = String(a.id || ''), ib = String(b.id || '')
    return ia.localeCompare(ib)
  })
  for (const inv of listAsc) {
    const div = document.createElement('div')
    div.className = 'card'
    const chip = document.createElement('span')
    chip.className = 'chip ' + (inv.status === 'pendiente' ? 'pending' : (inv.status || ''))
    chip.textContent = inv.status || ''
    const meSender = inv.from && inv.from.id === (S.user ? S.user.id : '')
    const otherAlias = meSender ? (inv.to.alias || inv.to.id) : (inv.from.alias || inv.from.id)
    const timeTxt = inv.createdAt ? ` • ${formatTimeShort(inv.createdAt)}` : ''
    const dirTxt = meSender ? 'Enviada a' : 'Recibida de'
    const msgTxt = inv.msg === 'invitoCancion' ? '¿Te invito una canción?' : '¿Bailamos?'
    div.textContent = `${dirTxt} ${otherAlias} • ${msgTxt}${timeTxt}`
    div.append(chip)
    container.append(div)
  }
}
async function finishDance() {
  const st = S.user?.danceState || 'idle'
  if (st !== 'dancing') return
  const ok = await confirmAction('¿Terminaste de bailar? Esto cerrará tu estado de baile.')
  if (!ok) return
  await api('/api/dance/finish', { method: 'POST', body: JSON.stringify({ userId: S.user.id }) })
  showError('Marcado: baile terminado')
  setTimeout(() => showError(''), 1200)
  renderUserHeader()
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
            const vid = u.searchParams.get('venueId') || ''
            const aj = u.searchParams.get('aj') || ''
            if (sid || (vid && aj === '1')) {
              const tracks = stream.getTracks(); tracks.forEach(t => t.stop())
              location.href = data
              return
            }
          } catch {
            if (data.startsWith('sess_')) {
              const base = location.origin
              const modeQuery = isRestaurantMode() ? '&mode=restaurant' : ''
              const url = `${base}/?venueId=${encodeURIComponent(S.venueId || 'default')}&sessionId=${encodeURIComponent(data)}&aj=1${modeQuery}`
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
    if (document && document.documentElement) document.documentElement.lang = 'es'
    loadI18n().then(() => { renderGenderSelect() }).catch(() => { renderGenderSelect() })
    const modeParam = normalizeModeParam(u.searchParams.get('mode') || u.searchParams.get('restaurant') || '')
    if (modeParam) applyMode(modeParam)
    else applyMode('')
    const vid = u.searchParams.get('venueId') || ''
    if (vid) S.venueId = vid
    const sid = u.searchParams.get('sessionId') || u.searchParams.get('s')
    if (sid && q('join-code')) q('join-code').value = sid
    const aj = u.searchParams.get('aj')
    const staffParam = u.searchParams.get('staff')
    const djParam = u.searchParams.get('dj')
    S.djOnly = djParam === '1'
    if (vid && !modeParam && !staffParam && !djParam && !(aj === '1')) {
      const m = getLocalUsers()
      const modeKey = getModeKey(modeParam || '')
      const staffKey = makeLocalKey(vid, modeKey)
      if (m && m[staffKey] && m[staffKey].role === 'staff') {
        restoreLocalUser().then(ok => { if (!ok) show('screen-venue-type') })
      } else {
        show('screen-venue-type')
      }
      return
    }
    if (staffParam === '1') {
      if (!modeParam) {
        restoreLocalUser().then(ok => { if (!ok) show('screen-venue-type') })
        return
      }
      show('screen-staff-welcome')
      if (sid) {
        setTimeout(async () => {
          const pin = await promptInput('Ingresa el PIN de sesión', 'PIN de venue')
          if (pin) { await join('staff', sid, pin) }
        }, 80)
      }
    } else if (djParam === '1') {
      S.djOnly = true
      if (!modeParam) {
        restoreLocalUser().then(ok => { if (!ok) show('screen-venue-type') })
        return
      }
      show('screen-staff-welcome')
      S.autoStaffTab = 'dj'
      if (sid) {
        setTimeout(async () => {
          const pin = await promptInput('Ingresa el PIN de sesión', 'PIN de venue')
          if (pin) { await join('staff', sid, pin) }
        }, 80)
      }
    } else if (sid && aj === '1') {
      restoreLocalUser().then(ok => {
        if (ok) return
        setTimeout(() => join('user', sid), 50)
      })
    } else if (!sid && aj === '1' && vid) {
      restoreLocalUser().then(async ok => {
        if (ok) return
        try {
          const mode = getModeFromUrl() || (isRestaurantMode() ? 'restaurant' : '')
          const modeKey = mode === 'restaurant' ? 'restaurant' : 'disco'
          const r = await api(`/api/session/active?venueId=${encodeURIComponent(vid)}&mode=${encodeURIComponent(modeKey)}`)
          if (r && r.sessionId) { setTimeout(() => join('user', r.sessionId), 50); return }
        } catch {}
        show('screen-welcome')
      })
    } else {
      restoreLocalUser().then(ok => {
        if (!ok) show('screen-welcome')
      })
    }
  } catch {}
  if ('serviceWorker' in navigator) navigator.serviceWorker.register('/sw.js')
  try {
    setTheme('dark')
    try { localStorage.setItem('discos_theme', 'dark') } catch {}
  } catch { setTheme('dark') }
}

init()
function getModeKey(mode) {
  return normalizeModeParam(mode) === 'restaurant' ? 'restaurant' : 'disco'
}
function getCurrentModeKey() {
  const m = getModeFromUrl() || (isRestaurantMode() ? 'restaurant' : '')
  return getModeKey(m)
}
function makeLocalKey(venueId, mode) {
  return `${venueId || 'default'}::${getModeKey(mode)}`
}
function getLastVenueKey(modeKey) {
  try { return localStorage.getItem(`discos_last_venue_${modeKey}`) || '' } catch { return '' }
}
function setLastVenueKey(modeKey, venueId) {
  try { localStorage.setItem(`discos_last_venue_${modeKey}`, venueId || '') } catch {}
}
function getLocalUsers() {
  try {
    const rawMap = localStorage.getItem('discos_users')
    if (rawMap) {
      const obj = JSON.parse(rawMap || '{}')
      if (typeof obj === 'object' && obj) {
        const out = {}
        for (const k of Object.keys(obj)) {
          if (String(k).includes('::')) out[k] = obj[k]
          else out[makeLocalKey(k, '')] = obj[k]
        }
        return out
      }
      return {}
    }
    const raw = localStorage.getItem('discos_user')
    if (raw) {
      const d = JSON.parse(raw || '{}')
      if (d && d.venueId) {
        const m = {}
        m[makeLocalKey(d.venueId, '')] = { sessionId: d.sessionId || '', role: d.role || '', userId: d.userId || '' }
        return m
      }
    }
  } catch {}
  return {}
}
function setLocalUsers(map) {
  try { localStorage.setItem('discos_users', JSON.stringify(map || {})) } catch {}
}
function saveLocalUser() {
  const v = S.venueId || 'default'
  const modeKey = getCurrentModeKey()
  const m = getLocalUsers()
  const k = makeLocalKey(v, modeKey)
  m[k] = { sessionId: S.sessionId || '', role: S.role || (S.user ? S.user.role : ''), userId: S.user ? S.user.id : '' }
  setLocalUsers(m)
  setLastVenueKey(modeKey, v)
}
function removeLocalUser(venueId) {
  const v = venueId || (S.venueId || 'default')
  const modeKey = getCurrentModeKey()
  const k = makeLocalKey(v, modeKey)
  const m = getLocalUsers()
  if (m[k]) { delete m[k] }
  setLocalUsers(m)
}
async function restoreLocalUser() {
  try {
    let sidParam = ''
    let ajParam = ''
    let venueParam = ''
    let staffParam = ''
    let djParam = ''
    let modeParam = ''
    try {
      const u = new URL(location.href)
      sidParam = u.searchParams.get('sessionId') || u.searchParams.get('s') || ''
      ajParam = u.searchParams.get('aj') || ''
      venueParam = u.searchParams.get('venueId') || ''
      staffParam = u.searchParams.get('staff') || ''
      djParam = u.searchParams.get('dj') || ''
      modeParam = normalizeModeParam(u.searchParams.get('mode') || u.searchParams.get('restaurant') || '')
    } catch {}
    S.djOnly = djParam === '1'
    const m = getLocalUsers()
    const modeKey = getModeKey(modeParam || (isRestaurantMode() ? 'restaurant' : ''))
    const lastVenue = getLastVenueKey(modeKey)
    let key = ''
    let d = null
    if (venueParam) {
      const k = makeLocalKey(venueParam, modeKey)
      if (m[k]) {
        key = k
        d = { sessionId: m[k].sessionId, userId: m[k].userId, role: m[k].role, venueId: venueParam }
      }
    }
    if (!d && sidParam) {
      const entries = Object.entries(m)
      for (const [k, v] of entries) {
        const parts = String(k).split('::')
        const kVenue = parts[0] || ''
        const kMode = parts[1] || 'disco'
        if (kMode !== modeKey) continue
        if (v && v.sessionId === sidParam) { key = k; d = { sessionId: v.sessionId, userId: v.userId, role: v.role, venueId: kVenue }; break }
      }
    }
    if (!d && lastVenue) {
      const k = makeLocalKey(lastVenue, modeKey)
      if (m[k]) {
        key = k
        d = { sessionId: m[k].sessionId, userId: m[k].userId, role: m[k].role, venueId: lastVenue }
      }
    }
    if (!d) {
      const keys = Object.keys(m).filter(k => String(k).endsWith(`::${modeKey}`))
      if (keys.length === 1) {
        key = keys[0]
        const v = m[key]
        const parts = String(key).split('::')
        const kVenue = parts[0] || ''
        d = { sessionId: v.sessionId, userId: v.userId, role: v.role, venueId: kVenue }
      }
    }
    if (!d || !d.sessionId || !d.userId || !d.role) return false
    if (ajParam === '1' && staffParam !== '1' && djParam !== '1' && d.role === 'staff') { return false }
    if (djParam === '1' && d.role !== 'staff') { return false }
    if (sidParam && ajParam === '1' && sidParam !== d.sessionId) { return false }
    S.venueId = d.venueId || (S.venueId || 'default')
    if (d.role === 'staff') {
      S.user = { id: d.userId, role: 'staff', sessionId: d.sessionId }
      S.sessionId = d.sessionId
      S.role = 'staff'
      await syncSessionMode(S.sessionId)
    } else {
      const r = await api(`/api/user/get?userId=${encodeURIComponent(d.userId)}`).catch(() => null)
      if (!r || !r.user) return false
      S.user = r.user
      S.sessionId = r.user.sessionId
      S.role = r.user.role
      await syncSessionMode(S.sessionId)
    }
    if (S.role === 'staff') {
      const okActive = await ensureSessionActiveOffer()
      if (!okActive) { show('screen-staff-welcome'); return true }
      show('screen-staff')
      showStaffTab(S.djOnly ? 'dj' : 'panel')
      await loadSessionInfo()
      startStaffEvents()
      loadOrders(); loadUsers(); loadReports(); loadAnalytics(); loadStaffPromos()
      restoreLastView()
      return true
    } else if (S.role === 'user') {
      startEvents()
      show('screen-user-home')
      const swAvail = q('switch-available')
      if (swAvail) swAvail.checked = !!(S.user && S.user.available)
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
      renderUserHeader()
      restoreLastView()
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
  if (bOrders) {
    let v = 0
    for (const k of Object.keys(r.orders || {})) { if (k !== 'cobrado' && k !== 'cancelado') v += Number(r.orders[k] || 0) }
    bOrders.classList.toggle('show', v > 0); bOrders.textContent = v > 9 ? '9+' : String(v)
  }
  const bMenuOrders = q('badge-menu-orders')
  if (bMenuOrders) {
    let v = 0
    for (const k of Object.keys(r.orders || {})) { if (k !== 'cobrado' && k !== 'cancelado') v += Number(r.orders[k] || 0) }
    bMenuOrders.classList.toggle('show', v > 0); bMenuOrders.textContent = v > 9 ? '9+' : String(v)
  }
  const bMesas = q('badge-tab-mesas')
  if (bMesas) { const v = r.mesasActivas || 0; bMesas.classList.toggle('show', v > 0); bMesas.textContent = v > 9 ? '9+' : String(v) }
  const bUsers = q('badge-tab-usuarios')
  if (bUsers) { const v = r.usersCount || 0; bUsers.classList.toggle('show', v > 0); bUsers.textContent = v > 9 ? '9+' : String(v) }
  const top = q('an-top')
  if (top) {
    top.innerHTML = ''
    const title = document.createElement('span')
    title.className = 'chip'
    title.textContent = isRestaurantMode() ? t('top_day_foods') : t('top_night_drinks')
    top.append(title)
    for (const name of Object.keys(r.topItems || {})) {
      const chip = document.createElement('span')
      chip.className = 'chip'
      chip.textContent = `${name}: ${r.topItems[name]}`
      top.append(chip)
    }
    try {
      const vh = await api(`/api/staff/venue_health?sessionId=${encodeURIComponent(S.sessionId)}&min=10`)
      const addChip = (txt) => { const c = document.createElement('span'); c.className = 'chip'; c.textContent = txt; top.append(c) }
      if (vh.mostActiveTable) addChip(`${t('most_active_table')}: ${vh.mostActiveTable}`)
      if (vh.peakHourLabel) addChip(`${t('invite_peak_hour')}: ${vh.peakHourLabel}`)
    } catch {}
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
  const title = q('consumption-title'); if (title) title.textContent = 'Invitar consumo'
  const targetLabel = q('consumption-target-label')
  if (targetLabel) targetLabel.style.display = ''
  const noteRow = q('consumption-note-row')
  if (noteRow) noteRow.style.display = ''
  const btnSelf = q('btn-order-self'), btnTable = q('btn-order-table')
  if (btnSelf) btnSelf.style.display = 'none'
  if (btnTable) btnTable.style.display = 'none'
  const cartSection = q('cart-section'); if (cartSection) { cartSection.style.display = ''; renderCart() }
  const addCart = q('btn-add-to-cart'); if (addCart) addCart.style.display = ''
  const cats = q('catalog-cats'), grid = q('catalog-list'), back = q('btn-catalog-back')
  if (cats) cats.style.display = ''
  if (grid) grid.style.display = 'none'
  if (back) back.style.display = 'none'
  showCatalogTop()
  show('screen-consumption')
}
async function openMenu() {
  await loadCatalog()
  const target = q('consumption-target'), sendBtn = q('btn-consumption-send')
  if (target) target.style.display = 'none'
  if (sendBtn) sendBtn.style.display = 'none'
  const title = q('consumption-title'); if (title) title.textContent = isRestaurantMode() ? 'Menú' : 'Carta'
  const targetLabel = q('consumption-target-label')
  if (targetLabel) targetLabel.style.display = 'none'
  const noteRow = q('consumption-note-row')
  if (noteRow) noteRow.style.display = 'none'
  const btnSelf = q('btn-order-self'), btnTable = q('btn-order-table')
  if (btnSelf) btnSelf.style.display = ''
  if (btnTable) btnTable.style.display = ''
  const cartSection = q('cart-section'); if (cartSection) { cartSection.style.display = ''; renderCart() }
  const addCart = q('btn-add-to-cart'); if (addCart) addCart.style.display = ''
  const cats = q('catalog-cats'), grid = q('catalog-list'), back = q('btn-catalog-back')
  if (cats) cats.style.display = ''
  if (grid) grid.style.display = 'none'
  if (back) back.style.display = 'none'
  showCatalogTop()
  show('screen-consumption')
}
function getCatalogMeta() {
  if (isRestaurantMode()) {
    return {
      order: ['cervezas','botellas','cocteles','sodas','otros'],
      labels: {
        cervezas: 'Hamburguesas',
        botellas: 'Perros calientes',
        cocteles: 'Pizzas',
        sodas: 'Bebidas',
        otros: 'Acompañamientos'
      }
    }
  }
  return {
    order: ['cervezas','botellas','cocteles','sodas','otros'],
    labels: {
      cervezas: 'Cervezas',
      botellas: 'Botellas',
      cocteles: 'Cocteles',
      sodas: 'Sodas y sin alcohol',
      otros: 'Otros'
    }
  }
}
async function loadCatalog() {
  try {
    const r = await getCatalogData()
    const catsEl = q('catalog-cats')
    const itemsEl = q('catalog-list')
    if (!catsEl || !itemsEl) return
    catsEl.innerHTML = ''
    itemsEl.innerHTML = ''
    const groups = {}
    for (const it of r.items || []) {
      const cat = (it.category || 'otros').toLowerCase()
      if (!groups[cat]) groups[cat] = []
      groups[cat].push(it)
    }
    const meta = getCatalogMeta()
    S.catalogGroups = groups
    renderCatalogCats(meta.order, meta.labels)
  } catch {}
}
function renderCatalogCats(order, labels) {
  const catsEl = q('catalog-cats'), itemsEl = q('catalog-list'), back = q('btn-catalog-back')
  if (!catsEl || !itemsEl) return
  catsEl.innerHTML = ''
  itemsEl.innerHTML = ''
  if (back) { back.style.display = 'none'; back.textContent = 'Volver a categorías' }
  if (itemsEl) itemsEl.style.display = 'none'
  if (catsEl) catsEl.style.display = ''
  for (const cat of order) {
    const items = S.catalogGroups[cat]
    if (!items || !items.length) continue
    const div = document.createElement('div')
    div.className = 'card'
    const name = document.createElement('div')
    name.textContent = labels[cat] || cat
    const count = document.createElement('span')
    count.className = 'chip'
    count.textContent = `${items.length}`
    div.onclick = () => renderCatalogCategory(cat, labels)
    div.append(name, count)
    catsEl.append(div)
  }
}
function renderCatalogCategory(cat, labels) {
  S.catalogCat = cat
  const items = S.catalogGroups[cat] || []
  const subgroups = {}
  for (const it of items) {
    const sub = String(it.subcategory || '').trim()
    if (!sub) continue
    if (!subgroups[sub]) subgroups[sub] = []
    subgroups[sub].push(it)
  }
  const names = Object.keys(subgroups)
  if (names.length) {
    renderCatalogSubcats(cat, labels, names, subgroups)
  } else {
    renderCatalogItems(cat, labels, items)
  }
}
function renderCatalogSubcats(cat, labels, names, subgroups) {
  const catsEl = q('catalog-cats'), itemsEl = q('catalog-list'), back = q('btn-catalog-back')
  if (!catsEl || !itemsEl) return
  S.catalogSubcat = ''
  catsEl.style.display = 'none'
  itemsEl.style.display = ''
  itemsEl.innerHTML = ''
  if (back) {
    back.style.display = ''
    back.textContent = 'Volver a categorías'
    back.onclick = () => {
      S.catalogCat=''
      S.catalogSubcat=''
      const meta = getCatalogMeta()
      renderCatalogCats(meta.order, meta.labels)
    }
  }
  const title = document.createElement('h3')
  title.textContent = labels[cat] || cat
  itemsEl.append(title)
  for (const sub of names) {
    const div = document.createElement('div')
    div.className = 'card'
    const name = document.createElement('div')
    name.textContent = sub
    const count = document.createElement('span')
    count.className = 'chip'
    count.textContent = `${(subgroups[sub]||[]).length}`
    div.onclick = () => { S.catalogSubcat = sub; renderCatalogItems(cat, labels, subgroups[sub] || []) }
    div.append(name, count)
    itemsEl.append(div)
  }
}
function findCatalogItemByName(name) {
  const key = String(name || '').toLowerCase()
  if (S.catalogIndex && S.catalogIndex[key]) return S.catalogIndex[key]
  for (const arr of Object.values(S.catalogGroups || {})) {
    for (const it of arr || []) {
      if (String(it.name || '').toLowerCase() === key) return it
    }
  }
  return null
}
function openRestaurantItemModal(it) {
  const item = it || {}
  const title = String(item.name || '')
  const desc = String(item.description || '').trim()
  const priceLabel = formatPriceShort(Number(item.price || 0))
  showModal(title, '', 'info')
  const modalText = q('modal-text')
  const row = document.querySelector('#modal .row')
  const closeBtn = q('modal-close')
  if (modalText) {
    modalText.innerHTML = ''
    const price = document.createElement('div')
    price.className = 'chip'
    price.textContent = priceLabel
    const d = document.createElement('div')
    d.textContent = desc || 'Sin descripción'
    const qtyRow = document.createElement('div')
    qtyRow.className = 'row'
    const qtyLabel = document.createElement('div')
    qtyLabel.textContent = 'Cantidad'
    const qtyInput = document.createElement('input')
    qtyInput.type = 'number'
    qtyInput.min = '1'
    qtyInput.value = '1'
    qtyInput.style.maxWidth = '90px'
    qtyRow.append(qtyLabel, qtyInput)
    modalText.append(price, d, qtyRow)
  }
  if (!row || !closeBtn) return
  const btnAdd = document.createElement('button')
  btnAdd.className = 'success'
  btnAdd.textContent = 'Agregar al carrito'
  const cleanup = () => {
    try { btnAdd.remove() } catch {}
    try { closeBtn.onclick = () => { const m = q('modal'); if (m) m.classList.remove('show') } } catch {}
  }
  const finish = () => { const m = q('modal'); if (m) m.classList.remove('show'); cleanup() }
  btnAdd.onclick = () => {
    const qtyInput = modalText ? modalText.querySelector('input[type="number"]') : null
    const qty = qtyInput ? Number(qtyInput.value || 1) : 1
    addItemToCart(title, qty)
    finish()
  }
  closeBtn.onclick = () => finish()
  row.insertBefore(btnAdd, closeBtn)
}
function renderCatalogItems(cat, labels, items) {
  const catsEl = q('catalog-cats'), itemsEl = q('catalog-list'), back = q('btn-catalog-back')
  if (!catsEl || !itemsEl) return
  catsEl.style.display = 'none'
  itemsEl.style.display = ''
  itemsEl.innerHTML = ''
  if (back) {
    back.style.display = ''
    back.textContent = S.catalogSubcat ? 'Volver a subcategorías' : 'Volver a categorías'
    back.onclick = () => {
      if (S.catalogSubcat) { S.catalogSubcat=''; renderCatalogCategory(cat, labels) }
      else {
        S.catalogCat=''
        const meta = getCatalogMeta()
        renderCatalogCats(meta.order, meta.labels)
      }
    }
  }
  const title = document.createElement('h3')
  title.textContent = S.catalogSubcat ? `${labels[cat] || cat} • ${S.catalogSubcat}` : (labels[cat] || cat)
  itemsEl.append(title)
  for (const it of items) {
    const div = document.createElement('div')
    div.className = 'card'
    const name = document.createElement('div')
    name.textContent = it.name
    const price = document.createElement('span')
    price.className = 'chip'
    price.textContent = formatPriceShort(it.price)
    if (isRestaurantMode()) {
      div.onclick = () => openRestaurantItemModal(it)
    } else {
      div.onclick = () => { const p = q('product'); if (p) p.value = it.name }
    }
    div.append(name, price)
    itemsEl.append(div)
  }
}
async function renderCart() {
  const list = q('cart-list')
  if (!list) return
  list.innerHTML = ''
  await ensureCatalogIndex().catch(() => {})
  let total = 0
  for (let i = 0; i < S.cart.length; i++) {
    const it = S.cart[i]
    const row = document.createElement('div')
    row.className = 'row'
    const label = document.createElement('div')
    const key = String(it.product || '').toLowerCase()
    const price = S.catalogIndex && S.catalogIndex[key] ? Number(S.catalogIndex[key].price || 0) : 0
    const subtotal = price * Math.max(1, Number(it.quantity || 1))
    total += subtotal
    label.textContent = `${it.quantity} x ${it.product} • ${formatPriceShort(subtotal)}`
    const del = document.createElement('button')
    del.className = 'danger'
    del.textContent = 'Eliminar'
    del.onclick = () => { try { S.cart.splice(i, 1); renderCart() } catch {} }
    row.append(label, del)
    list.append(row)
  }
  const totalRow = document.createElement('div')
  totalRow.className = 'row'
  const ttl = document.createElement('div')
  ttl.textContent = `Total: ${formatPriceShort(total)}`
  totalRow.append(ttl)
  list.append(totalRow)
}
function addToCart() {
  const product = q('product') ? q('product').value : ''
  const qty = Math.max(1, Number(q('quantity') ? q('quantity').value : 1))
  if (!product) { showError('Selecciona un producto'); return }
  addItemToCart(product, qty)
  const inp = q('product'); if (inp) inp.value = ''
  const qn = q('quantity'); if (qn) qn.value = '1'
}
function addItemToCart(product, qty) {
  const name = String(product || '').trim()
  const qv = Math.max(1, Number(qty || 1))
  if (!name) { showError('Selecciona un producto'); return }
  S.cart.push({ product: name, quantity: qv })
  renderCart()
  maybeSuggestPairings(name)
}
function applyCatalogSearch() {
  const inp = q('catalog-search')
  const catsEl = q('catalog-cats'), itemsEl = q('catalog-list'), back = q('btn-catalog-back')
  if (!inp || !catsEl || !itemsEl) return
  const qv = String(inp.value || '').trim().toLowerCase()
  if (!qv) {
    S.catalogCat=''
    S.catalogSubcat=''
    const meta = getCatalogMeta()
    renderCatalogCats(meta.order, meta.labels)
    return
  }
  const all = []
  for (const arr of Object.values(S.catalogGroups || {})) for (const it of arr || []) all.push(it)
  const matches = all.filter(it => String(it.name || '').toLowerCase().includes(qv))
  S.catalogCat = 'search'
  S.catalogSubcat = ''
  catsEl.style.display = 'none'
  renderCatalogItems('otros', { otros: 'Resultados' }, matches)
}
async function showCatalogTop() {
  try {
    const r = await api(`/api/staff/analytics?sessionId=${encodeURIComponent(S.sessionId)}`)
    const top = q('catalog-top')
    if (!top) return
    top.innerHTML = ''
    const title = document.createElement('div')
    title.className = 'top-night-title'
    title.textContent = isRestaurantMode() ? t('top_day_foods') : t('top_night_drinks')
    top.append(title)
    const itemsRow = document.createElement('div')
    itemsRow.className = 'top-night-items'
    const items = r.topItems || {}
    const names = Object.keys(items).sort((a, b) => Number(items[b] || 0) - Number(items[a] || 0))
    for (const name of names.slice(0, 2)) {
      const chip = document.createElement('span')
      chip.className = 'top-night-item'
      chip.textContent = `${name} • ${items[name]}`
      chip.onclick = () => {
        if (isRestaurantMode()) {
          const it = findCatalogItemByName(name)
          if (it) { openRestaurantItemModal(it); return }
        }
        const p = q('product'); if (p) p.value = name
      }
      itemsRow.append(chip)
    }
    top.append(itemsRow)
  } catch {}
}
function maybeSuggestPairings(product) {
  try {
    const name = String(product || '').trim()
    if (!name) return
    const all = []
    for (const arr of Object.values(S.catalogGroups || {})) for (const it of arr || []) all.push(it)
    const combos = all.filter(it => !!it.combo && Array.isArray(it.includes) && it.includes.includes(name))
    const others = []
    for (const c of combos) for (const inc of c.includes) if (inc !== name && !others.includes(inc)) others.push(inc)
    if (others.length) {
      const pick = others.slice(0, 2)
      const msg = pick.length === 1 ? `Suele pedirse con: ${pick[0]}` : `Suele pedirse con: ${pick[0]} • ${pick[1]}`
      showSuccess(msg)
    }
  } catch {}
}
async function loadStaffCatalogEditor() {
  try {
    const qs = S.sessionId
      ? `?sessionId=${encodeURIComponent(S.sessionId)}${S.venueId ? ('&venueId=' + encodeURIComponent(S.venueId)) : ''}`
      : (S.venueId ? `?venueId=${encodeURIComponent(S.venueId)}` : '')
    const r = await api(`/api/catalog${qs}`)
    const needsBootstrap = r && r.source !== 'venue' && r.venueInitialized !== true
    if (S.sessionId && needsBootstrap && !S.catalogBootstrapPrompted) {
      S.catalogBootstrapPrompted = true
      const choice = await promptCatalogBootstrap()
      S.catalogBootstrapPrompted = false
      if (choice === 'copy') { await copyGlobalToSession(true); return }
      if (choice === 'new') { await initEmptyVenueCatalog(); return }
    }
    const srcEl = q('staff-catalog-source')
    if (srcEl) {
      const map = { venue: 'Fuente: Carta del venue', session: 'Fuente: Carta del venue (sesión)', global: 'Fuente: Carta global', file: 'Fuente: Carta global (archivo)' }
      srcEl.textContent = map[r.source] || 'Fuente: Carta'
    }
    const useGlobalBtn = q('btn-staff-catalog-use-global')
    const copyGlobalBtn = q('btn-staff-catalog-copy-global')
    const showGlobalButtons = r.source !== 'venue'
    if (useGlobalBtn) useGlobalBtn.style.display = showGlobalButtons ? '' : 'none'
    if (copyGlobalBtn) copyGlobalBtn.style.display = showGlobalButtons ? '' : 'none'
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
      name.className = 'catalog-name'
      const price = document.createElement('input')
      price.type = 'number'
      price.min = '0'
      price.value = Number(it.price || 0)
      price.className = 'catalog-price'
      const category = document.createElement('select')
      category.className = 'catalog-cat'
      for (const opt of ['cervezas','botellas','cocteles','sodas','otros']) {
        const o = document.createElement('option')
        o.value = opt
        o.textContent = opt.charAt(0).toUpperCase() + opt.slice(1)
        category.append(o)
      }
      category.value = (it.category || 'otros').toLowerCase()
      const subInput = document.createElement('input')
      subInput.type = 'text'
      subInput.placeholder = 'Subcategoría'
      subInput.value = String(it.subcategory || '')
      subInput.className = 'catalog-sub'
      const descInput = document.createElement('input')
      descInput.type = 'text'
      descInput.placeholder = 'Descripción'
      descInput.value = String(it.description || '')
      descInput.className = 'catalog-desc'
      const combo = document.createElement('input')
      combo.type = 'checkbox'
      combo.checked = !!it.combo
      combo.title = 'Combo'
      const includes = document.createElement('input')
      includes.type = 'text'
      includes.placeholder = 'Incluye (coma)'
      includes.value = Array.isArray(it.includes) ? it.includes.join(',') : ''
      includes.className = 'catalog-includes'
      const discount = document.createElement('input')
      discount.type = 'number'
      discount.min = '0'
      discount.max = '100'
      discount.placeholder = '% desc'
      discount.value = Number(it.discount || 0)
      discount.className = 'catalog-discount'
      name.oninput = scheduleCatalogSave
      price.oninput = scheduleCatalogSave
      category.oninput = scheduleCatalogSave
      subInput.oninput = scheduleCatalogSave
      descInput.oninput = scheduleCatalogSave
      combo.onchange = scheduleCatalogSave
      includes.oninput = scheduleCatalogSave
      discount.oninput = scheduleCatalogSave
      const del = document.createElement('button'); del.className = 'danger'; del.textContent = 'Eliminar'; del.onclick = () => { try { row.remove(); scheduleCatalogSave() } catch {} }
      const lblCombo = document.createElement('label'); lblCombo.textContent = 'Combo'; lblCombo.style.marginLeft = '8px'
      lblCombo.appendChild(combo)
      row.append(name, price, category, subInput, descInput, lblCombo, includes, discount, del)
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
      const nameInput = row.querySelector('.catalog-name')
      const priceInput = row.querySelector('.catalog-price')
      const catSelect = row.querySelector('.catalog-cat')
      const subInput = row.querySelector('.catalog-sub')
      const descInput = row.querySelector('.catalog-desc')
      const comboInput = row.querySelector('input[type="checkbox"]')
      const includesInput = row.querySelector('.catalog-includes')
      const discInput = row.querySelector('.catalog-discount')
      if (!nameInput || !priceInput || !catSelect) continue
      const name = nameInput.value.trim()
      const price = Number(priceInput.value || 0)
      const category = (catSelect.value || 'otros').toLowerCase()
      const subcategory = subInput ? String(subInput.value || '').trim() : ''
      const description = descInput ? String(descInput.value || '').trim() : ''
      const combo = !!(comboInput && comboInput.checked)
      const includes = includesInput ? String(includesInput.value || '').split(',').map(s => s.trim()).filter(Boolean) : []
      const discount = discInput ? Math.max(0, Math.min(100, Number(discInput.value || 0))) : 0
      if (!name) continue
      if (isRestaurantMode() && !description) { showError('Agrega una descripción'); return }
      items.push({ name, price, category, subcategory, description, combo, includes, discount })
    }
    const s = await api('/api/staff/catalog', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, items }) })
    if (s && s.ok) { showError('Carta guardada'); setTimeout(() => showError(''), 1000) }
    await loadStaffCatalogEditor()
  } catch (e) { showError(String(e.message || 'Error')) }
}
async function useGlobalCatalog() {
  try {
    await api('/api/staff/catalog', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, items: [] }) })
    await loadStaffCatalogEditor()
  } catch (e) { showError(String(e.message)) }
}
async function initEmptyVenueCatalog() {
  try {
    await api('/api/staff/catalog', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, venueId: S.venueId || '', items: [], initVenueCatalog: true }) })
    await loadStaffCatalogEditor()
  } catch (e) { showError(String(e.message)) }
}
async function copyGlobalToSession(initVenueCatalog = false) {
  try {
    const r = await api(`/api/catalog${isRestaurantMode() ? '?mode=restaurant' : ''}`)
    const items = r.items || []
    await api('/api/staff/catalog', { method: 'POST', body: JSON.stringify({ sessionId: S.sessionId, venueId: S.venueId || '', items, initVenueCatalog: !!initVenueCatalog }) })
    await loadStaffCatalogEditor()
  } catch (e) { showError(String(e.message)) }
}
async function loadStaffPromos() {
  try {
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
  } catch {}
}
async function viewStaffTableHistory() {
  const raw = q('staff-table-id')?.value.trim()
  const t = normalizeTableId(raw)
  if (!t) return
  const r = await api(`/api/staff/table/orders?sessionId=${encodeURIComponent(S.sessionId)}&tableId=${encodeURIComponent(t)}`)
  const container = q('staff-table-orders')
  if (!container) return
  container.innerHTML = ''
  const listAsc = (r.orders || []).slice().sort((a, b) => {
    const ta = Number(a.createdAt || 0), tb = Number(b.createdAt || 0)
    if (ta !== tb) return ta - tb
    const ia = String(a.id || ''), ib = String(b.id || '')
    return ia.localeCompare(ib)
  })
  for (const o of listAsc) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    await ensureCatalogIndex()
    const label = formatOrderProductFull(o.product)
    const timeTxt = o.createdAt ? ` • ${formatTimeShort(o.createdAt)}` : ''
    info.textContent = `${label} x${o.quantity || 1} • $${o.total || 0} • ${o.emitterAlias || o.emitterId}→${o.receiverAlias || o.receiverId}${timeTxt}`
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
  const listAsc = (r.orders || []).slice().sort((a, b) => {
    const ta = Number(a.createdAt || 0), tb = Number(b.createdAt || 0)
    if (ta !== tb) return ta - tb
    const ia = String(a.id || ''), ib = String(b.id || '')
    return ia.localeCompare(ib)
  })
  for (const o of listAsc) {
    const div = document.createElement('div')
    div.className = 'card'
    const info = document.createElement('div')
    await ensureCatalogIndex()
    const label = formatOrderProductFull(o.product)
    const timeTxt = o.createdAt ? ` • ${formatTimeShort(o.createdAt)}` : ''
    info.textContent = `${label} x${o.quantity || 1} • $${o.total || 0} • ${o.emitterAlias || o.emitterId}→${o.receiverAlias || o.receiverId}${timeTxt}`
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
