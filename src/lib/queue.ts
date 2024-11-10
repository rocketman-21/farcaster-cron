import { EmbeddingType, JobBody } from './job';

const validateEnvVars = () => {
  if (!process.env.EMBEDDINGS_QUEUE_URL) {
    throw new Error('EMBEDDINGS_QUEUE_URL is not defined');
  }
  if (!process.env.EMBEDDINGS_QUEUE_API_KEY) {
    throw new Error('EMBEDDINGS_QUEUE_API_KEY is not defined');
  }
};

const makeRequest = async (endpoint: string, body: any) => {
  validateEnvVars();

  const headers: HeadersInit = {
    'Content-Type': 'application/json',
    'x-api-key': process.env.EMBEDDINGS_QUEUE_API_KEY || '',
    'Cache-Control': 'no-store',
  };

  const response = await fetch(process.env.EMBEDDINGS_QUEUE_URL + endpoint, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const text = await response.text();
    console.error({ text });
    console.error(`Failed request to ${endpoint}:`, text);
    throw new Error((text as any)?.message || `Failed request to ${endpoint}`);
  }

  return response;
};

export async function postToEmbeddingsQueueRequest(payload: JobBody) {
  try {
    await makeRequest('/add-job', payload);
  } catch (error) {
    console.log('Failed to post to embeddings queue:');
    console.log({
      payload,
      EMBEDDINGS_QUEUE_URL: process.env.EMBEDDINGS_QUEUE_URL,
    });
    throw error;
  }
}

export async function postBulkToEmbeddingsQueueRequest(payloads: JobBody[]) {
  await makeRequest('/bulk-add-job', { jobs: payloads });
}

export async function deleteEmbeddingRequest(
  contentHash: string,
  type: EmbeddingType
) {
  await makeRequest('/delete-embedding', { contentHash, type });
}
