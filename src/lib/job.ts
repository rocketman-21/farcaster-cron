export type EmbeddingType = (typeof validTypes)[number];

export interface JobBody {
  type: EmbeddingType;
  content: string;
  groups: string[];
  users: string[];
  tags: string[];
  externalId: string;
  hashSuffix?: string;
  externalUrl?: string;
  urls?: string[];
}

export const validTypes = [
  'grant',
  'cast',
  'grant-application',
  'flow',
  'dispute',
  'draft-application',
  'builder-profile',
  'story',
] as const;

export enum EmbeddingTag {
  Flows = 'flows',
  Drafts = 'drafts',
  Grants = 'grants',
}

export const validTags = [
  EmbeddingTag.Flows,
  EmbeddingTag.Drafts,
  EmbeddingTag.Grants,
] as const;

export interface IsGrantUpdateJobBody {
  castContent: string;
  castHash: string;
  builderFid: string;
  urls: string[];
}

export interface BuilderProfileJobBody {
  fid: string;
}

export interface StoryJobBody {
  newCastId: number;
  grantId: string;
}
