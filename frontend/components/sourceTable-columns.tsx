"use client"

import { ColumnDef } from "@tanstack/react-table"
import Link from 'next/link';
import { Tag } from './tag';

export type TelescopeRecord = {
    data_url: string;
    last_timestamp: string;
    last_flux: number;
    last_error: number;
}

export type SourceRecord = {
    _id : string;
    integral_name: string;
    maxi?: TelescopeRecord;
    swift?: TelescopeRecord;
    fermi?: TelescopeRecord;
    labels_constant: string[];
}


export const columns: ColumnDef<SourceRecord>[] = [
    {   accessorKey: "integral_name",
        header: () => <div className="text-right">Name</div>,
        cell: ({ row }) => {
            const _id = row.original._id;
            const name = row.original.integral_name;
            return (
                <Link href={`/sources/${encodeURIComponent(_id)}`} className="hover:underline">{name}</Link>
            )
        }
    },
    {   accessorKey: "maxi",
        header: () => <div className="text-right">Last Data MAXI</div>,
        cell: ( { row }) => { 
            const data = row.original.maxi;
            const conversionFactor = 1000 / 0.285;
            if (!data || !data.last_flux) return null;
            return (
                <div className="text-right">
                    {(data.last_flux * conversionFactor).toFixed(3)} &plusmn; {(data.last_error * conversionFactor).toFixed(3)} mCrab
                </div>
            )
       } 
    },
    {   accessorKey: "swift",
        header: () => <div className="text-right">Last Data Swift/BAT</div>,
        cell: ({ row }) => { 
            const data = row.original.swift;
            const conversionFactor = 1000 / 0.285;
            if (!data || !data.last_flux) return null;
            return (
                <div className="text-right">
                    {(data.last_flux * conversionFactor).toFixed(3)} &plusmn; {(data.last_error * conversionFactor).toFixed(3)} mCrab
                </div>
            )
       } 
    },
    {   accessorKey: "fermi",
        header: () => <div className="text-right">Last Data Fermi/GBM</div>,
        cell: ({ row }) => { 
            const data = row.original.fermi;
            const conversionFactor = 1000 / 0.285;
            if (!data || !data.last_flux) return null;
            return (
                <div className="text-right">
                    {(data.last_flux * conversionFactor).toFixed(3)} &plusmn; {(data.last_error * conversionFactor).toFixed(3)} mCrab
                </div>
            )
       } 
    },
    {   accessorKey: "tags",
        header: "Tags",
        cell: ({ row }) => {
            const tags = row.original.labels_constant;
            return (<div className="flex flex-wrap gap-1.5">
                                        {tags.map((l) => (
                                            <Tag key={l} label={l} />))}
                                    </div>
                                        )
        }
    },
    {   accessorKey: "plots",
        header: "Plots",
        cell: ({ row }) => {
            const id = row.original._id;
            return <Link href={`http://localhost:8000/plots/${encodeURIComponent(id)}`} className="externalLink" target="_blank">here</Link>
        }
    },
]