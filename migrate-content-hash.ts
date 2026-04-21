/**
 * Migration script: add contentHash to all existing newspaper pages.
 *
 * Run per-country before enabling contentHash comparison:
 *   MONGO_URL="mongodb://..." npx ts-node scripts/migrate-content-hash.ts
 *
 * What it does:
 *   1. Finds all newspapers in the collection
 *   2. For each page without contentHash: downloads image → computes hash → $set
 *   3. Skips pages that already have contentHash (idempotent)
 *   4. Can be interrupted and restarted safely
 *
 * Env vars:
 *   MONGO_URL         — full connection string including DB name (required)
 *   COLLECTION        — collection name (default: "newspapers")
 *   BRAND_NAME_SLUG   — filter by brand slug, e.g. "lidl" (optional, default: all brands)
 *   BATCH_SIZE        — newspapers per batch (default: 10)
 *   DRY_RUN           — "true" to preview without writing (default: false)
 */

import * as path from 'node:path';
import { createHash } from 'node:crypto';

import * as dotenv from 'dotenv';
dotenv.config({ path: path.join(__dirname, '.env') });

import { MongoClient } from 'mongodb';
import sharp from 'sharp';

const NORM_SIZE = 256;
const QUANT_STEP = 8;

async function computeContentHash(imageBuffer: Buffer): Promise<string | undefined> {
  try {
    const rawBuffer = await sharp(imageBuffer).resize(NORM_SIZE, NORM_SIZE, { fit: 'fill' }).greyscale().raw().toBuffer();

    const quantized = Buffer.alloc(rawBuffer.length);
    for (let i = 0; i < rawBuffer.length; i++) {
      quantized[i] = Math.floor(rawBuffer[i] / QUANT_STEP) * QUANT_STEP;
    }

    return createHash('md5').update(new Uint8Array(quantized.buffer, quantized.byteOffset, quantized.byteLength)).digest('hex');
  } catch {
    return undefined;
  }
}

async function downloadImage(url: string): Promise<Buffer | null> {
  try {
    const response = await fetch(url);
    if (!response.ok) return null;
    return Buffer.from(await response.arrayBuffer());
  } catch {
    return null;
  }
}

interface Page {
  id: string;
  imageUrl: string;
  contentHash?: string;
  position: number;
}

interface Newspaper {
  _id: unknown;
  id?: string;
  brandNameSlug?: string;
  pages: Page[];
}

async function main() {
  const mongoUrl = process.env.MONGO_URL;
  if (!mongoUrl) {
    console.error('MONGO_URL is required');
    process.exit(1);
  }

  const collectionName = process.env.COLLECTION || 'newspapers';
  const brandNameSlug = process.env.BRAND_NAME_SLUG || null;
  const batchSize = parseInt(process.env.BATCH_SIZE || '10', 10);
  const dryRun = process.env.DRY_RUN === 'true';

  console.log(`Connecting to MongoDB...`);
  console.log(`Collection: ${collectionName}, brand: ${brandNameSlug ?? 'all'}, batch: ${batchSize}, dryRun: ${dryRun}`);

  const client = new MongoClient(mongoUrl);
  await client.connect();

  const db = client.db();
  const collection = db.collection<Newspaper>(collectionName);

  // Find newspapers that have at least one page without contentHash
  const query: Record<string, unknown> = {
    'pages': { $exists: true },
    'pages.contentHash': { $exists: false },
  };
  if (brandNameSlug) query['brandNameSlug'] = brandNameSlug;

  const cursor = collection.find(query).batchSize(batchSize);

  let newspaperCount = 0;
  let pagesMigrated = 0;
  let pagesSkipped = 0;
  let pagesFailed = 0;

  for await (const newspaper of cursor) {
    newspaperCount++;
    const brand = newspaper.brandNameSlug || 'unknown';
    const id = newspaper.id || String(newspaper._id);

    console.log(`\n[${newspaperCount}] ${brand} / ${id} — ${newspaper.pages.length} pages`);

    let updated = false;
    const pages = newspaper.pages;

    for (let i = 0; i < pages.length; i++) {
      const page = pages[i];

      if (page.contentHash) {
        pagesSkipped++;
        continue;
      }

      if (!page.imageUrl || !page.imageUrl.trim()) {
        console.log(`  Page ${page.position}: no imageUrl, skipping`);
        pagesFailed++;
        continue;
      }

      const imageBuffer = await downloadImage(page.imageUrl);
      if (!imageBuffer) {
        console.log(`  Page ${page.position}: download failed (${page.imageUrl})`);
        pagesFailed++;
        continue;
      }

      const contentHash = await computeContentHash(imageBuffer);
      if (!contentHash) {
        console.log(`  Page ${page.position}: hash computation failed`);
        pagesFailed++;
        continue;
      }

      pages[i] = { ...page, contentHash };
      updated = true;
      pagesMigrated++;

      console.log(`  Page ${page.position}: ${contentHash}`);
    }

    if (updated && !dryRun) {
      await collection.updateOne(
        { _id: newspaper._id },
        { $set: { pages } },
      );
    }
  }

  console.log(`\n--- Done ---`);
  console.log(`Newspapers processed: ${newspaperCount}`);
  console.log(`Pages migrated: ${pagesMigrated}`);
  console.log(`Pages skipped (already had contentHash): ${pagesSkipped}`);
  console.log(`Pages failed: ${pagesFailed}`);

  await client.close();
}

main().catch((err) => {
  console.error('Migration failed:', err);
  process.exit(1);
});
