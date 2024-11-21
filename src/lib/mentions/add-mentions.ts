import { TextEncoder } from 'util';

// Helper function to map byte position to code unit position
function bytePositionToCodeUnitPosition(
  text: string,
  bytePosition: number
): number {
  const encoder = new TextEncoder();
  let codeUnitPos = 0;
  let bytePos = 0;

  while (bytePos < bytePosition && codeUnitPos < text.length) {
    const codePoint = text.codePointAt(codeUnitPos);
    if (codePoint === undefined) break;

    const codePointStr = String.fromCodePoint(codePoint);
    const codePointBytes = encoder.encode(codePointStr);
    bytePos += codePointBytes.length;

    // Advance code unit position by the length of the code point
    codeUnitPos += codePointStr.length;
  }

  return codeUnitPos;
}

// Helper function to insert mentions into the text
export function insertMentionsIntoText(
  originalText: string,
  mentionPositions: number[],
  mentionedFids: number[],
  fidToFname: Map<string, string>
): string {
  if (
    !Array.isArray(mentionPositions) ||
    !Array.isArray(mentionedFids) ||
    mentionPositions.length !== mentionedFids.length
  ) {
    console.warn('Mismatched or invalid mentions data');
    return originalText;
  }

  // Map byte positions to code unit positions
  const mentionsData = mentionPositions
    .map((bytePosition, index) => {
      const fid = mentionedFids[index];
      const codeUnitPosition = bytePositionToCodeUnitPosition(
        originalText,
        bytePosition
      );

      if (codeUnitPosition < 0 || codeUnitPosition > originalText.length) {
        console.warn(
          `Invalid byte position ${bytePosition} for fid ${fid} in cast "${originalText}"`
        );
        return null;
      }

      return {
        position: codeUnitPosition,
        fid,
      };
    })
    .filter((mention) => mention !== null)
    // Sort mentions by position
    .sort((a, b) => a!.position - b!.position);

  let modifiedText = originalText;
  let positionShift = 0;

  for (const mention of mentionsData) {
    if (!mention) throw new Error('Mention is null');

    const fidString = mention.fid.toString();
    const fname = fidToFname.get(fidString) || fidString;
    const mentionText = `@${fname}`;
    const position = mention.position + positionShift;

    if (position < 0 || position > modifiedText.length) {
      console.warn(
        `Invalid mention position ${position} for fid ${mention.fid}`
      );
      continue;
    }

    // Insert the mention into the text
    modifiedText =
      modifiedText.slice(0, position) +
      mentionText +
      modifiedText.slice(position);

    // Update the position shift
    positionShift += mentionText.length;
  }

  return modifiedText;
}
