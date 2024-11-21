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
  flowId: string;
  submitter: string;
  parentContract: string;
  isTopLevel: number;
  isFlow: number;
  title: string;
  description: string;
  image: string;
  tagline?: string;
  url?: string;
  isRemoved: number;
  isActive: number;
  votesCount: string;
  monthlyIncomingFlowRate: string;
  monthlyIncomingBaselineFlowRate: string;
  monthlyIncomingBonusFlowRate: string;
  monthlyOutgoingFlowRate: string;
  monthlyRewardPoolFlowRate: string;
  monthlyBaselinePoolFlowRate: string;
  monthlyBonusPoolFlowRate: string;
  bonusMemberUnits: string;
  baselineMemberUnits: string;
  totalEarned: string;
  activeRecipientCount: number;
  awaitingRecipientCount: number;
  challengedRecipientCount: number;
  tcr: string;
  erc20: string;
  arbitrator: string;
  tokenEmitter: string;
  status: number;
  challengePeriodEndsAt: number;
  isDisputed: number;
  isResolved: number;
  evidenceGroupID: string;
  createdAt: number;
  updatedAt: number;
  baselinePool: string;
  bonusPool: string;
  managerRewardPool: string;
  superToken: string;
  managerRewardSuperfluidPool: string;
  managerRewardPoolFlowRatePercent: number;
  baselinePoolFlowRatePercent: number;
}
