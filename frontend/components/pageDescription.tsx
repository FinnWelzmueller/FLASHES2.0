import React from "react";

export function PageDescription({ children }: { children: React.ReactNode }) {
    return (
        <p className="mt-2 text-neutral-400">
            { children }
      </p>);
}