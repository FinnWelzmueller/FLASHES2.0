import { SourceTable } from "@/components/sourceTable";
import { formatTag } from "@/lib/formatTag";
import { columns } from "@/components/sourceTable-columns";
import { PageDescription } from "@/components/pageDescription";
export default async function SourcesFiltered({ params }: { params: { tag: string } ;
}) {
  const tag_name = (await params).tag;
  const res = await fetch(`http://localhost:8000/tags/${encodeURIComponent(String(tag_name))}`, { next: { revalidate: 0 } });
  
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Backend returned ${res.status}: ${body}`);
  }

  const data = await res.json();
  return (
    <main className="p-6">
      <h1>{formatTag(tag_name)}</h1>
      <PageDescription>
                 Select a source name to view more details. Click on the plot link to see all available timeseries data. Please note that the last datapoint from Swift/BAT can be due to changes, as it is continously averaged over the day. The final value for the day is only available the next day.
      </PageDescription>
      <SourceTable columns={columns} data={data} />

    </main>
  );
}