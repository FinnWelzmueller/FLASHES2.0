export function Tag({ label }: { label: string }) {
    return (
        <span key={label} className="inline-flex items-center rounded-xl px-2 py-0.5 text-xs border border-neutral-300 dark:border-neutral-700 hover:bg-neutral-50/50 dark:hover:bg-neutral-900/30">{ <a href={`/tags/${label.toLowerCase().replace(' ','-')}`}>{ label }</a> }</span>
    );
}