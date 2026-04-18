const express  = require('express');
const session  = require('express-session');
const multer   = require('multer');
const XLSX     = require('xlsx');
const path     = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Replit DB — safe init ────────────────────────────────────────────────────
let db = null;
try {
  const Database = require('@replit/database');
  db = new Database();
  console.log('✅ Replit Database connected');
} catch (e) {
  console.warn('⚠️ Replit Database not available, using in-memory fallback');
  const memStore = {};
  db = {
    get: async (k) => memStore[k] !== undefined ? memStore[k] : null,
    set: async (k, v) => { memStore[k] = v; },
    delete: async (k) => { delete memStore[k]; },
  };
}

// ── Replit DB helpers ────────────────────────────────────────────────────────
const CHUNK = 500;
async function dbGet(k)    { try { const v = await db.get(k); return v ?? null; } catch { return null; } }
async function dbSet(k, v) { try { await db.set(k, v); } catch(e) { console.error('DB set error:', e.message); } }
async function dbDel(k)    { try { await db.delete(k); } catch {} }

async function readAll(type) {
  const meta = await dbGet('meta_' + type);
  if (!meta) return [];
  const out = [];
  for (let i = 0; i < meta.chunks; i++) {
    const c = await dbGet(type + '_' + i);
    if (c) out.push(...c);
  }
  return out;
}
async function writeAll(type, arr) {
  const old = await dbGet('meta_' + type);
  if (old) for (let i = 0; i < old.chunks; i++) await dbDel(type + '_' + i);
  const nc = Math.max(1, Math.ceil(arr.length / CHUNK));
  for (let i = 0; i < nc; i++) await dbSet(type + '_' + i, arr.slice(i * CHUNK, (i + 1) * CHUNK));
  await dbSet('meta_' + type, { chunks: nc, total: arr.length, updated: new Date().toISOString() });
}
async function appendRows(type, rows) {
  const existing = await readAll(type);
  await writeAll(type, [...existing, ...rows]);
}

// ── USERS ────────────────────────────────────────────────────────────────────
const USERS = {
  owner:   { password: 'owner123',   role: 'owner',   name: 'Owner'   },
  manager: { password: 'manager123', role: 'manager', name: 'Manager' },
  staff:   { password: 'staff123',   role: 'staff',   name: 'Staff'   },
};

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));
app.set('trust proxy', 1);
app.use(session({
  secret: 'palmenswear-ims-2026',
  resave: false,
  saveUninitialized: false,
  cookie: {
    maxAge: 12 * 60 * 60 * 1000,
    secure: false,
    sameSite: 'lax'
  }
}));

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

function auth(roles) {
  return (req, res, next) => {
    if (!req.session.user) return res.status(401).json({ error: 'Not logged in' });
    if (roles && !roles.includes(req.session.user.role)) return res.status(403).json({ error: 'Access denied' });
    next();
  };
}

// ── Date parsing helper ──────────────────────────────────────────────────────
function parseDateValue(dateRaw) {
  if (!dateRaw) return null;
  if (typeof dateRaw === 'number') {
    const d = new Date((dateRaw - 25569) * 86400000);
    return d.toISOString().split('T')[0];
  }
  const s = String(dateRaw).trim();
  const parts = s.split(/[\/\-]/);
  if (parts.length === 3) {
    return `${parts[2]}-${parts[1].padStart(2,'0')}-${parts[0].padStart(2,'0')}`;
  }
  return s;
}

// ── PURCHASE EXCEL PARSER ─────────────────────────────────────────────────────
// Aryan's 51-column purchase format
// RETURNS: { purchases: [...], salesFromPurchase: [...] }
// Negative TOTAL QUANTITY → treated as a SALE (customer returned to supplier is rare;
// but Aryan says: minus in purchase file = reverse/sale side → remove from stock)
function parsePurchase(buffer) {
  const wb   = XLSX.read(buffer, { type: 'buffer', cellDates: false });
  const ws   = wb.Sheets[wb.SheetNames[0]];
  const raw  = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

  let headerRow = -1;
  for (let i = 0; i < Math.min(raw.length, 15); i++) {
    const rowStr = (raw[i] || []).join('|').toUpperCase();
    if (rowStr.includes('PARTY NAME') && (rowStr.includes('BILL DATE') || rowStr.includes('VOUCHER DATE'))) {
      headerRow = i;
      break;
    }
  }
  if (headerRow === -1) throw new Error('Could not find header row. Purchase file must have columns: BILL DATE (or VOUCHER DATE), PARTY NAME, TOTAL QUANTITY.');

  const headers = (raw[headerRow] || []).map(h => String(h || '').trim().toUpperCase());

  const findCol = (patterns) => {
    for (const p of patterns) {
      const exact = headers.indexOf(p.toUpperCase());
      if (exact !== -1) return exact;
    }
    for (const p of patterns) {
      const partial = headers.findIndex(h => h.includes(p.toUpperCase()));
      if (partial !== -1) return partial;
    }
    return -1;
  };

  const iDate    = findCol(['BILL DATE', 'VOUCHER DATE']);
  const iBillNo  = findCol(['BILL NO.', 'BILL NO', 'VOUCHER NO.', 'VOUCHER NO']);
  const iParty   = findCol(['PARTY NAME']);
  const iCompany = findCol(['COMPANY NAME']);
  const iShade   = findCol(['SHADE NAME']);
  const iItem    = findCol(['ITEM NAME']);
  const iPack    = findCol(['PACK / SIZE', 'PACK/SIZE', 'PACK / GRADE', 'PACK/GRADE', 'PACK']);
  const iLot     = findCol(['LOT NUMBER', 'LOT NO.', 'LOT NO']);
  const iQty     = findCol(['TOTAL QUANTITY', 'TOTAL QTY']);
  const iRate    = findCol(['RATE/UNIT', 'RATE / UNIT']);
  const iAmount  = findCol(['NET AMOUNT', 'GROSS AMOUNT', 'TOTAL']);

  if (iDate === -1 || iParty === -1 || iQty === -1)
    throw new Error('Missing required columns: BILL DATE (or VOUCHER DATE), PARTY NAME, TOTAL QUANTITY');

  const purchases        = [];
  const salesFromPurchase = [];

  for (let i = headerRow + 1; i < raw.length; i++) {
    const r = raw[i] || [];
    const sno = r[0];
    if (!sno && !r[iParty]) continue;
    const snoNum = parseFloat(String(sno || '').trim());
    if (isNaN(snoNum)) continue;

    const dateStr = parseDateValue(r[iDate]);
    const qty  = parseFloat(r[iQty])  || 0;
    const rate = iRate >= 0 ? (parseFloat(r[iRate]) || 0) : 0;
    const netAmount = iAmount >= 0 ? (parseFloat(r[iAmount]) || 0) : 0;
    if (qty === 0) continue;

    const absQty = Math.abs(qty);
    const rowBase = {
      date:     dateStr,
      billNo:   String(iBillNo  >= 0 ? r[iBillNo]  || '' : '').trim(),
      supplier: String(r[iParty] || '').trim(),
      brand:    String(iCompany >= 0 ? r[iCompany] || '' : '').trim(),
      item:     String(iItem    >= 0 ? r[iItem]    || '' : '').trim(),
      size:     String(iPack    >= 0 ? r[iPack]    || '' : '').trim(),
      shade:    String(iShade   >= 0 ? r[iShade]   || '' : '').trim(),
      lotNo:    String(iLot     >= 0 ? r[iLot]     || '' : '').trim(),
    };

    if (qty > 0) {
      // Normal purchase — add to stock
      purchases.push({
        ...rowBase,
        qty: absQty,
        rate,
        amount: netAmount || (absQty * rate),
      });
    } else {
      // NEGATIVE qty in purchase file → treat as SALE (remove from stock)
      salesFromPurchase.push({
        ...rowBase,
        agent:    '(From Purchase File - Reverse Entry)',
        category: '',
        qty: absQty,
        note: 'Converted from negative purchase qty',
      });
    }
  }
  return { purchases, salesFromPurchase };
}

// ── SALE EXCEL PARSER ─────────────────────────────────────────────────────────
// Aryan's 10-column sale format
// RETURNS: { sales: [...], purchasesFromSale: [...] }
// Negative SALE QTY → treated as PURCHASE / customer return (add back to stock)
function parseSale(buffer) {
  const wb   = XLSX.read(buffer, { type: 'buffer', cellDates: false });
  const ws   = wb.Sheets[wb.SheetNames[0]];
  const raw  = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

  let headerRow = -1;
  for (let i = 0; i < Math.min(raw.length, 15); i++) {
    const rowStr = (raw[i] || []).join('|').toUpperCase();
    if (rowStr.includes('BILL DATE') && rowStr.includes('COMPANY NAME')) {
      headerRow = i;
      break;
    }
  }
  if (headerRow === -1) throw new Error('Could not find header row. Sale file must have columns: BILL DATE, COMPANY NAME, SALE QTY.');

  const headers = (raw[headerRow] || []).map(h => String(h || '').trim().toUpperCase());

  const findCol = (patterns) => {
    for (const p of patterns) {
      const exact = headers.indexOf(p.toUpperCase());
      if (exact !== -1) return exact;
    }
    for (const p of patterns) {
      const partial = headers.findIndex(h => h.includes(p.toUpperCase()));
      if (partial !== -1) return partial;
    }
    return -1;
  };

  const iDate    = findCol(['BILL DATE']);
  const iAgent   = findCol(['AGENT NAME']);
  const iCompany = findCol(['COMPANY NAME']);
  const iCat     = findCol(['CATEGORY']);
  const iPack    = findCol(['PACK / SIZE', 'PACK/SIZE', 'PACK']);
  const iItem    = findCol(['ITEM NAME']);
  const iQty     = findCol(['SALE QTY', 'SALE QUANTITY']);
  const iShade   = findCol(['SHADE NAME']);
  const iLot     = findCol(['LOT NUMBER', 'LOT NO']);

  if (iDate === -1 || iCompany === -1 || iQty === -1)
    throw new Error('Missing required columns: BILL DATE, COMPANY NAME, SALE QTY');

  const sales              = [];
  const purchasesFromSale  = [];

  for (let i = headerRow + 1; i < raw.length; i++) {
    const r = raw[i] || [];
    const sno = r[0];
    if (!sno) continue;
    const snoNum = parseFloat(String(sno || '').trim());
    if (isNaN(snoNum)) continue;

    const dateStr = parseDateValue(r[iDate]);
    const qty = parseFloat(r[iQty]) || 0;
    if (qty === 0) continue;

    const absQty = Math.abs(qty);
    const rowBase = {
      date:     dateStr,
      agent:    String(iAgent   >= 0 ? r[iAgent]   || '' : '').trim(),
      brand:    String(iCompany >= 0 ? r[iCompany] || '' : '').trim(),
      category: String(iCat     >= 0 ? r[iCat]     || '' : '').trim(),
      item:     String(iItem    >= 0 ? r[iItem]    || '' : '').trim(),
      size:     String(iPack    >= 0 ? r[iPack]    || '' : '').trim(),
      shade:    String(iShade   >= 0 ? r[iShade]   || '' : '').trim(),
      lotNo:    String(iLot     >= 0 ? r[iLot]     || '' : '').trim(),
    };

    if (qty > 0) {
      // Normal sale — remove from stock
      sales.push({ ...rowBase, qty: absQty });
    } else {
      // NEGATIVE qty in sale file → treat as PURCHASE (add back to stock)
      purchasesFromSale.push({
        ...rowBase,
        supplier: '(From Sale File - Customer Return)',
        billNo: '',
        qty: absQty,
        rate: 0,
        amount: 0,
        note: 'Converted from negative sale qty (customer return)',
      });
    }
  }

  return { sales, purchasesFromSale };
}

// ── AUTH ROUTES ───────────────────────────────────────────────────────────────
app.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  const user = USERS[username?.toLowerCase()];
  if (!user || user.password !== password)
    return res.json({ success: false, error: 'Wrong username or password' });
  req.session.user = { username, role: user.role, name: user.name };
  res.json({ success: true, role: user.role, name: user.name });
});
app.post('/api/logout', (req, res) => { req.session.destroy(); res.json({ success: true }); });
app.get('/api/me', (req, res) => {
  if (!req.session.user) return res.json({ loggedIn: false });
  res.json({ loggedIn: true, ...req.session.user });
});

// ── UPLOAD ────────────────────────────────────────────────────────────────────
app.post('/api/upload/:type', auth(['owner', 'staff']), upload.single('file'), async (req, res) => {
  const type = req.params.type;
  if (!['purchase', 'sale'].includes(type))
    return res.status(400).json({ error: 'Type must be purchase or sale' });
  if (!req.file)
    return res.status(400).json({ error: 'No file uploaded' });

  try {
    let purchaseRows = 0, saleRows = 0, convertedRows = 0;

    if (type === 'purchase') {
      const result = parsePurchase(req.file.buffer);
      // Positive qty → add to purchase
      if (result.purchases.length) await appendRows('purchase', result.purchases);
      // Negative qty → add to sale (cross-conversion!)
      if (result.salesFromPurchase.length) await appendRows('sale', result.salesFromPurchase);

      purchaseRows  = result.purchases.length;
      convertedRows = result.salesFromPurchase.length;

      if (!purchaseRows && !convertedRows)
        return res.status(400).json({ error: 'No valid rows found in purchase file.' });
    } else {
      const result = parseSale(req.file.buffer);
      // Positive qty → add to sale
      if (result.sales.length) await appendRows('sale', result.sales);
      // Negative qty → add to purchase (cross-conversion!)
      if (result.purchasesFromSale.length) await appendRows('purchase', result.purchasesFromSale);

      saleRows      = result.sales.length;
      convertedRows = result.purchasesFromSale.length;

      if (!saleRows && !convertedRows)
        return res.status(400).json({ error: 'No valid rows found in sale file.' });
    }

    const logs = await dbGet('upload_logs') || [];
    logs.unshift({
      type,
      filename:     req.file.originalname,
      purchaseRows,
      saleRows,
      convertedRows, // cross-converted rows (minus qty auto-flipped)
      by:  req.session.user.name,
      role: req.session.user.role,
      at:  new Date().toISOString(),
    });
    await dbSet('upload_logs', logs.slice(0, 200));

    res.json({
      success: true,
      purchaseRows,
      saleRows,
      convertedRows,
      message: convertedRows > 0
        ? `${convertedRows} row(s) auto-converted (negative qty flipped to opposite side)`
        : undefined,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ── DATA ──────────────────────────────────────────────────────────────────────
app.get('/api/data', auth(['owner', 'manager']), async (req, res) => {
  const [purchase, sale, returns] = await Promise.all([
    readAll('purchase'), readAll('sale'), readAll('returns')
  ]);
  res.json({ purchase, sale, returns });
});

app.get('/api/uploads', auth(['owner', 'manager']), async (req, res) => {
  res.json(await dbGet('upload_logs') || []);
});

app.get('/api/meta', auth(['owner', 'manager']), async (req, res) => {
  const [pm, sm, rm] = await Promise.all([
    dbGet('meta_purchase'), dbGet('meta_sale'), dbGet('meta_returns')
  ]);
  res.json({ purchase: pm, sale: sm, returns: rm });
});

app.delete('/api/data/:type', auth(['owner']), async (req, res) => {
  const type = req.params.type;
  if (!['purchase','sale','returns'].includes(type))
    return res.status(400).json({ error: 'Invalid type' });
  await writeAll(type, []);
  res.json({ success: true });
});

// ── SPA ───────────────────────────────────────────────────────────────────────
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ── START ─────────────────────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', () => console.log(`✅ PAL MENS WEAR IMS running on port ${PORT}`));

// Prevent crashes from unhandled errors
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err.message);
});
process.on('unhandledRejection', (err) => {
  console.error('Unhandled Rejection:', err);
});
