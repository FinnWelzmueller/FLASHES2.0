export function Tag({ label }: { label: string }) {
    return (
        <span key={label} className="inline-flex items-center rounded-full px-2 py-0.5 text-xs border border-neutral-300 dark:border-neutral-700 transition hover:text-neutral-900 hover:bg-gradient-to-br hover:from-neutral-400 hover:to-neutral-300">{ <a href={`/tags/${label.toLowerCase().replace(' ','-')}`}>{ label }</a> }</span>
    );
}
