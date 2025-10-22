import { formatTag } from '@/lib/formatTag';

export default async function Tag(
    { params }: { params: Promise<{ tag: string }> ;
}) 
{
    const tag = (await params).tag;

    return (
    <main>
      <h1>Tag Page</h1>
      <p>This page displays details for a specific tag {formatTag(tag)}.</p>
    </main>
  );
}

