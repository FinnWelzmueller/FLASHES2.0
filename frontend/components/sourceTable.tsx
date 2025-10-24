import React from 'react';
import Link from 'next/link';
import { Tag } from './tag';

export type TelescopeRecord = {
    data_url: string;
    last_timestamp: string;
}

export type SourceRecord = {
    _id : string;
    integral_name: string;
    maxi?: TelescopeRecord;
    swift?: TelescopeRecord;
    fermi?: TelescopeRecord;
    labels_constant: string[];
}

export function SourceTable({ sources }: { sources: SourceRecord[] }) {
    return (
        <div className="rounded-2xl border border-neutral-200 dark:border-neutral-800 overflow-hidden shadow-sm">
            <table className="min-w-full divide-y divide-neutral-200 dark:divide-neutral-800">
                <thead className="bg-neutral-50 dark:bg-neutral-900/60">
                    <tr>
                        <th scope="col" className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider">Name</th>
                        <th scope="col" className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider">Last Data MAXI</th>
                        <th scope="col" className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider">Last Data Swift/BAT</th>
                        <th scope="col" className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider">Last Data Fermi/GBM</th>
                        <th scope="col" className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider">Tags</th>
                        <th scope="col" className="px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider">Plots</th>
                    </tr>
                </thead>
                <tbody className="divide-y divide-neutral-200 dark:divide-neutral-800">
                    {sources.map((source) => {
                        return (
                            <tr key={source._id} className="hover:bg-neutral-50/60 dark:hover:bg-neutral-900/50">
                                <td className="px-4 py-3 text-sm font-medium">
                                    <Link href={`/sources/${source._id}`} className="hover:underline">{source.integral_name}</Link>
                                    </td>
                                <td className="px-4 py-3 text-sm font-medium">{source.maxi ? source.maxi.last_timestamp : ""}</td>
                                <td className="px-4 py-3 text-sm font-medium">{source.swift ? source.swift.last_timestamp : ""}</td>
                                <td className="px-4 py-3 text-sm font-medium">{source.fermi ? source.fermi.last_timestamp : ""}</td>
                                <td className="px-4 py-3 text-sm font-medium">
                                    <div className="flex flex-wrap gap-1.5">
                                        {source.labels_constant.map((l) => (
                                            <Tag key={l} label={l} />))}
                                    </div>
                                </td>
                                <td className="px-4 py-3 text-sm font-medium">
                                    <a href={`http://localhost:8000/plots/${source._id}`} className="externalLink" target="_blank">here</a></td>
                            </tr>)})}
                </tbody>
            </table>
        </div>

    );
}