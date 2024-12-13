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
  embeds_array: any | null;
  root_parent_hash: Buffer | null;
  root_parent_url: string | null;
  computed_tags: string[] | null;
  embed_summaries: string[] | null;
  mentioned_fids: number[] | null;
  mentions_positions_array: number[] | null;
  impact_verifications: {
    model: string;
    score: number;
    reason: string;
    is_grant_update: boolean;
    prompt_version: string;
    grant_id: string;
  }[];
}

export interface StagingFarcasterCast {
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
  mentions: any;
  mentions_positions: string | null;
  root_parent_hash: Buffer | null;
  root_parent_url: string | null;
}

export interface Grant {
  id: string;
  recipient: string;
  flow_id: string;
  submitter: string;
  parent_contract: string;
  is_top_level: number;
  is_flow: number;
  title: string;
  description: string;
  image: string;
  tagline?: string;
  url?: string;
  is_removed: number;
  is_active: number;
  votes_count: string;
  monthly_incoming_flow_rate: string;
  monthly_incoming_baseline_flow_rate: string;
  monthly_incoming_bonus_flow_rate: string;
  monthly_outgoing_flow_rate: string;
  monthly_reward_pool_flow_rate: string;
  monthly_baseline_pool_flow_rate: string;
  monthly_bonus_pool_flow_rate: string;
  bonus_member_units: string;
  baseline_member_units: string;
  total_earned: string;
  active_recipient_count: number;
  awaiting_recipient_count: number;
  challenged_recipient_count: number;
  tcr: string;
  erc20: string;
  arbitrator: string;
  token_emitter: string;
  status: number;
  challenge_period_ends_at: number;
  is_disputed: number;
  is_resolved: number;
  evidence_group_id: string;
  created_at: number;
  updated_at: number;
  baseline_pool: string;
  bonus_pool: string;
  manager_reward_pool: string;
  super_token: string;
  manager_reward_superfluid_pool: string;
  manager_reward_pool_flow_rate_percent: number;
  baseline_pool_flow_rate_percent: number;
}
