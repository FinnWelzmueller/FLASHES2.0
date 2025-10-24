import React from "react";

export function BigButton({ href, name }: { href: string; name: string }) {
    return (
        <a 
            href={href}
            className="rounded-xl border border-neutral-500 text-neutral-100 py-4 font-medium transition hover:text-neutral-900 hover:bg-gradient-to-br hover:from-neutral-400 hover:to-neutral-300">
                {name}
        </a>
    );
}