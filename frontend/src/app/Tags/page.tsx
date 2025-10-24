import { BigButton } from '@/components/bigButton';
import { formatTag } from '@/lib/formatTag';
import { PageDescription } from '@/components/pageDescription';
export default async function SourcesFiltered({ params }: { params: Promise<{ tag_name: string }> ;
}) {

  const res = await fetch("http://localhost:8000/tags/", { next: { revalidate: 0 } });
  
  if (!res.ok) {
    throw new Error("The FLASHES backend appears to be down. Please try again later.");
  }

  const data = await res.json();
  return (
    <main className="p-6">
      <h1>Available Tags</h1>
      <PageDescription>
        Choose a tag below to explore all related X-ray sources.
      </PageDescription>

      <div className="mt-10 grid grid-cols-2 md:grid-cols-4 gap-3 text-center">
        {data.map((tag : string) => (

            <BigButton
              href={`/tags/${encodeURIComponent(tag.toLowerCase().replace(/\s+/g, '-'))}`}
              name={`${formatTag(tag)}`}
              key={tag}/>
        ))}
      </div>
    </main>
  );
}