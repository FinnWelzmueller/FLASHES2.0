import { SourceTable } from "@/components/sourceTable";
import { columns, SourceRecord } from "@/components/sourceTable-columns"
import { PageDescription } from "@/components/pageDescription";
export default async function Sources() {

  const res = await fetch("http://localhost:8000/sources", { next: { revalidate: 0 } });
  
  if (!res.ok) {
    throw new Error("The FLASHES backend appears to be down. Please try again later.");
  }

  const data = await res.json();
  return (
    <main>
      <h1>Sources Page</h1>
      <PageDescription>
         Select a source name to view more details. Click on the plot link to see all available timeseries data. Please note that the last datapoint from Swift/BAT can be due to changes, as it is continously averaged over the day. The final value for the day is only available the next day.
        </PageDescription>
      <SourceTable columns={columns} data={data} />

    </main>
  );
}