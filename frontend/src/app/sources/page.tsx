import { SourceTable } from "@/components/sourceTable";
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
        This is the sources page of the application.
        </PageDescription>
      <SourceTable sources={data} />

    </main>
  );
}