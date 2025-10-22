/**
 * Translates a tag (all lowercase with hyphens) into a more human-readable format.
 * It replaces hyphens with spaces and capitalizes the first letter of each word.
 * @param tag tag that needs formatting
 * @returns formatted tag
 */

export function formatTag(tag: string): string {
    if (!tag) return '';
    return tag.replace(/-/g, ' ').replace(/\b\w/g, char => char.toUpperCase());
}