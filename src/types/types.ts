export interface NounishCitizen {
  fid: string;
  fname: string;
  channel_id: string;
}

export interface ChannelMember {
  id: number;
  created_at: Date;
  updated_at: Date;
  deleted_at: Date | null;
  timestamp: Date;
  fid: number;
  channel_id: string;
}

export interface FarcasterProfile {
  fname: string;
  display_name: string;
  avatar_url: string;
  bio: string;
  verified_addresses: string[];
  updated_at: Date;
  fid: number;
}

export interface FarcasterCast {
  id: number;
  created_at: Date;
  updated_at: Date;
  deleted_at: Date | null;
  timestamp: Date;
  fid: number;
  hash: Buffer;
  parent_hash: Buffer | null;
  parent_fid: number | null;
  parent_url: string | null;
  text: string;
  embeds: string | null;
  mentions: string | null;
  mentions_positions: string | null;
  root_parent_hash: Buffer | null;
  root_parent_url: string | null;
}
