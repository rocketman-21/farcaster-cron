import { S3Client } from '@aws-sdk/client-s3';
import path from 'path';

require('dotenv').config();

// Configure AWS S3 Client
const s3Client = new S3Client({
  region: 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY || '',
    secretAccessKey: process.env.AWS_SECRET_KEY || '',
  },
});

// S3 bucket and prefixes
export const bucketName = 'tf-premium-parquet';
export const prefixes: Record<IngestionType, string> = {
  profiles:
    'public-postgres/farcaster/v2/incremental/farcaster-profile_with_addresses',
  casts: 'public-postgres/farcaster/v2/incremental/farcaster-casts',
  'channel-members':
    'public-postgres/farcaster/v2/incremental/farcaster-channel_members',
};

export type IngestionType = 'profiles' | 'casts' | 'channel-members';

// Function to extract the timestamp from the S3 key
export function extractTimestampFromKey(key: string): number {
  const basename = path.basename(key);
  const match = basename.match(/^farcaster-.+?-\d+-(\d+)\.parquet$/);
  return match ? parseInt(match[1], 10) * 1000 : 0;
}

// Helper function to extract the table name from the S3 key
export function getTableNameFromKey(key: string): string {
  const basename = path.basename(key);
  const match = basename.match(/^farcaster-(.+?)-\d+-\d+\.parquet$/);
  return match ? `farcaster_${match[1]}` : 'unknown_table';
}

export { s3Client };
