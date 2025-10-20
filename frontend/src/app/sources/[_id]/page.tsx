export default async function SourceDetails(
    { params }: { params: Promise<{ _id: string }> ;
}) 
{
    const _id = (await params)._id;
    return (
    <main>
      <h1>Source Details Page</h1>
      <p>This page displays details for a specific source {_id}.</p>
    </main>
  );
}