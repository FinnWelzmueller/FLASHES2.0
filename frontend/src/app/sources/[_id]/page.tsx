import '@/src/app/globals.css';


export default async function SourceDetails(
    { params }: { params: Promise<{ _id: string }> ;
}) 
{
    const _id = (await params)._id;
    const res = await fetch(`http://localhost:8000/sources/${encodeURIComponent(String(_id))}`, { next: { revalidate: 0 } });
  
  if (!res.ok) {
    throw new Error("The FLASHES backend appears to be down. Please try again later.");
  }

  const data = await res.json();
    return (
    <main>
      <h1>{data.integral_name}</h1>

    </main>
  );
}