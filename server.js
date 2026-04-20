const express  = require('express');
const session  = require('express-session');
const MongoStore = require('connect-mongo');
const multer   = require('multer');
const XLSX     = require('xlsx');
const path     = require('path');
const { MongoClient } = require('mongodb');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── MONGODB CONNECTION ───────────────────────────────────────────────────────
const MONGO_URI = process.env.MONGODB_URI || 'mongodb+srv://paladmin:JAIMATADI@cluster0.yhza2if.mongodb.net/?appName=Cluster0';
const DB_NAME   = 'palmenswear';

let db = null;

async function connectDB() {
  try {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    console.log('✅ Connected to MongoDB Atlas — data is safe forever!');

    // Create collections if they don't exist
    const collections = await db.listCollections().toArray();
    const names = collections.map(c => c.name);
    if (!names.includes('purchases'))   await db.createCollection('purchases');
    if (!names.includes('sales'))       await db.createCollection('sales');
    if (!names.includes('returns'))     await db.createCollection('returns');
    if (!names.includes('upload_logs')) await db.createCollection('upload_logs');

    console.log('✅ Collections ready: purchases, sales, returns, upload_logs');
  } catch (e) {
    console.error('❌ MongoDB connection failed:', e.message);
    process.exit(1);
  }
}

// ── STORAGE HELPERS ──────────────────────────────────────────────────────────
async function readAll(type) {
  const collName = type === 'purchase' ? 'purchases' : type === 'sale' ? 'sales' : 'returns';
  try {
    return await db.collection(collName).find({}).toArray();
  } catch (e) {
    console.error('Read error:', e.message);
    return [];
  }
}

async function appendRows(type, rows) {
  const collName = type === 'purchase' ? 'purchases' : type === 'sale' ? 'sales' : 'returns';
  try {
    if (rows.length > 0) {
      await db.collection(collName).insertMany(rows);
    }
  } catch (e) {
    console.error('Append error:', e.message);
  }
}

// ── DUPLICATE DETECTION ──────────────────────────────────────────────────────
function makePurchaseKey(r) {
  return [r.date||'', r.billNo||'', r.item||'', r.lotNo||'', r.supplier||'', r.size||'', r.shade||'', r.qty||0].join('|').toLowerCase();
}

function makeSaleKey(r) {
  return [r.date||'', r.sno||'', r.item||'', r.lotNo||'', r.brand||'', r.size||'', r.shade||'', r.qty||0].join('|').toLowerCase();
}

async function appendRowsDedup(type, rows) {
  const collName = type === 'purchase' ? 'purchases' : type === 'sale' ? 'sales' : 'returns';
  try {
    if (rows.length === 0) return { added: 0, skipped: 0 };

    // Get existing rows from DB
    const existing = await db.collection(collName).find({}).toArray();

    // Build set of existing keys
    const existingKeys = new Set();
    const keyFn = type === 'purchase' ? makePurchaseKey : makeSaleKey;
    existing.forEach(r => existingKeys.add(keyFn(r)));

    // Filter out duplicates
    const newRows = [];
    let skipped = 0;
    for (const row of rows) {
      const key = keyFn(row);
      if (existingKeys.has(key)) {
        skipped++;
      } else {
        newRows.push(row);
        existingKeys.add(key); // prevent duplicates within same file too
      }
    }

    if (newRows.length > 0) {
      await db.collection(collName).insertMany(newRows);
    }

    return { added: newRows.length, skipped };
  } catch (e) {
    console.error('Append dedup error:', e.message);
    return { added: 0, skipped: 0 };
  }
}

async function clearAll(type) {
  const collName = type === 'purchase' ? 'purchases' : type === 'sale' ? 'sales' : 'returns';
  try {
    await db.collection(collName).deleteMany({});
  } catch (e) {
    console.error('Clear error:', e.message);
  }
}

async function getUploadLogs() {
  try {
    return await db.collection('upload_logs').find({}).sort({ at: -1 }).limit(200).toArray();
  } catch (e) {
    return [];
  }
}

async function addUploadLog(log) {
  try {
    await db.collection('upload_logs').insertOne(log);
    // Keep only last 200 logs
    const count = await db.collection('upload_logs').countDocuments();
    if (count > 200) {
      const oldest = await db.collection('upload_logs').find({}).sort({ at: 1 }).limit(count - 200).toArray();
      const ids = oldest.map(o => o._id);
      await db.collection('upload_logs').deleteMany({ _id: { $in: ids } });
    }
  } catch (e) {
    console.error('Log error:', e.message);
  }
}

// ── USERS ────────────────────────────────────────────────────────────────────
const USERS = {
  owner:   { password: process.env.OWNER_PASS   || 'owner123',   role: 'owner',   name: 'Owner'   },
  manager: { password: process.env.MANAGER_PASS || 'manager123', role: 'manager', name: 'Manager' },
  staff:   { password: process.env.STAFF_PASS   || 'staff123',   role: 'staff',   name: 'Staff'   },
};

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));
app.set('trust proxy', 1);
app.use(session({
  secret: process.env.SESSION_SECRET || 'palmenswear-ims-2026',
  resave: false,
  saveUninitialized: false,
  store: MongoStore.create({
    mongoUrl: MONGO_URI,
    dbName: DB_NAME,
    collectionName: 'sessions',
    ttl: 12 * 60 * 60, // 12 hours
  }),
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

  const purchases         = [];
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
      purchases.push({
        ...rowBase,
        qty: absQty,
        rate,
        amount: netAmount || (absQty * rate),
      });
    } else {
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
  const iSno     = findCol(['SNO.', 'SNO', 'S.NO', 'S.NO.']);

  if (iDate === -1 || iCompany === -1 || iQty === -1)
    throw new Error('Missing required columns: BILL DATE, COMPANY NAME, SALE QTY');

  const sales             = [];
  const purchasesFromSale = [];

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
      sno:      String(snoNum),
      agent:    String(iAgent   >= 0 ? r[iAgent]   || '' : '').trim(),
      brand:    String(iCompany >= 0 ? r[iCompany] || '' : '').trim(),
      category: String(iCat     >= 0 ? r[iCat]     || '' : '').trim(),
      item:     String(iItem    >= 0 ? r[iItem]    || '' : '').trim(),
      size:     String(iPack    >= 0 ? r[iPack]    || '' : '').trim(),
      shade:    String(iShade   >= 0 ? r[iShade]   || '' : '').trim(),
      lotNo:    String(iLot     >= 0 ? r[iLot]     || '' : '').trim(),
    };

    if (qty > 0) {
      sales.push({ ...rowBase, qty: absQty });
    } else {
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
    let purchaseRows = 0, saleRows = 0, convertedRows = 0, skippedRows = 0;

    if (type === 'purchase') {
      const result = parsePurchase(req.file.buffer);
      if (result.purchases.length) {
        const r = await appendRowsDedup('purchase', result.purchases);
        purchaseRows = r.added;
        skippedRows += r.skipped;
      }
      if (result.salesFromPurchase.length) {
        const r = await appendRowsDedup('sale', result.salesFromPurchase);
        convertedRows = r.added;
        skippedRows += r.skipped;
      }
      if (!purchaseRows && !convertedRows && !skippedRows)
        return res.status(400).json({ error: 'No valid rows found in purchase file.' });
    } else {
      // For sale files: no row-level dedup (same item can sell multiple times)
      // Instead, check if this exact filename was already uploaded
      const logs = await getUploadLogs();
      const alreadyUploaded = logs.some(l => l.type === 'sale' && l.filename === req.file.originalname);
      if (alreadyUploaded) {
        return res.status(400).json({ error: `This file "${req.file.originalname}" was already uploaded before. If this is a new file, please rename it and try again.` });
      }

      const result = parseSale(req.file.buffer);
      if (result.sales.length) await appendRows('sale', result.sales);
      if (result.purchasesFromSale.length) await appendRows('purchase', result.purchasesFromSale);
      saleRows      = result.sales.length;
      convertedRows = result.purchasesFromSale.length;
      if (!saleRows && !convertedRows)
        return res.status(400).json({ error: 'No valid rows found in sale file.' });
    }

    await addUploadLog({
      type,
      filename:     req.file.originalname,
      purchaseRows,
      saleRows,
      convertedRows,
      skippedRows,
      by:  req.session.user.name,
      role: req.session.user.role,
      at:  new Date().toISOString(),
    });

    let message = '';
    if (skippedRows > 0) message += `${skippedRows} duplicate row(s) skipped. `;
    if (convertedRows > 0) message += `${convertedRows} row(s) auto-converted (negative qty flipped). `;

    res.json({
      success: true,
      purchaseRows,
      saleRows,
      convertedRows,
      skippedRows,
      message: message || undefined,
    });
  } catch (e) {
    console.error('Upload error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── DATA ──────────────────────────────────────────────────────────────────────
app.get('/api/data', auth(['owner', 'manager']), async (req, res) => {
  const [purchase, sale, returns] = await Promise.all([
    readAll('purchase'), readAll('sale'), readAll('returns')
  ]);
  // Remove MongoDB _id from response to keep frontend compatible
  const clean = arr => arr.map(({ _id, ...rest }) => rest);
  res.json({ purchase: clean(purchase), sale: clean(sale), returns: clean(returns) });
});

app.get('/api/uploads', auth(['owner', 'manager']), async (req, res) => {
  const logs = await getUploadLogs();
  const clean = logs.map(({ _id, ...rest }) => rest);
  res.json(clean);
});

app.get('/api/meta', auth(['owner', 'manager']), async (req, res) => {
  try {
    const [pCount, sCount, rCount] = await Promise.all([
      db.collection('purchases').countDocuments(),
      db.collection('sales').countDocuments(),
      db.collection('returns').countDocuments(),
    ]);
    res.json({
      purchase: { total: pCount, updated: new Date().toISOString() },
      sale:     { total: sCount, updated: new Date().toISOString() },
      returns:  { total: rCount, updated: new Date().toISOString() },
    });
  } catch (e) {
    res.json({ purchase: null, sale: null, returns: null });
  }
});

app.delete('/api/data/:type', auth(['owner']), async (req, res) => {
  const type = req.params.type;
  if (!['purchase','sale','returns'].includes(type))
    return res.status(400).json({ error: 'Invalid type' });
  await clearAll(type);
  res.json({ success: true });
});

// ── SPA ───────────────────────────────────────────────────────────────────────
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ── START ─────────────────────────────────────────────────────────────────────
connectDB().then(() => {
  app.listen(PORT, '0.0.0.0', () => console.log(`✅ PAL MENS WEAR IMS running on port ${PORT}`));
});

// Prevent crashes from unhandled errors
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err.message);
});
process.on('unhandledRejection', (err) => {
  console.error('Unhandled Rejection:', err);
});
