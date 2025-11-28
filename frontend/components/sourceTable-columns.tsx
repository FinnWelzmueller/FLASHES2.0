"use client"

import { ColumnDef } from "@tanstack/react-table"
import { ArrowUpDown } from "lucide-react"
import Link from 'next/link';
import { Button } from "@/components/ui/button"
import { Tag } from './tag';
import { getConversionFactor } from "@/lib/utils";

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
        header: ({ column }) => {
      return (
        <Button
          variant="ghost"
          onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
        >
          <div className="text-right">Name</div>
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      )
    },
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
            if (!data || !data.last_flux) return null;
            return (
                <div className="text-right">
                    {(data.last_flux * getConversionFactor("2-20")).toFixed(3)} &plusmn; {(data.last_error * getConversionFactor("2-20")).toFixed(3)} mCrab
                </div>
            )
       } 
    },
    {   accessorKey: "swift",
        header: () => <div className="text-right">Last Data Swift/BAT</div>,
        cell: ({ row }) => { 
            const data = row.original.swift;
            if (!data || !data.last_flux) return null;
            return (
                <div className="text-right">
                    {(data.last_flux * getConversionFactor("15-50")).toFixed(3)} &plusmn; {(data.last_error * getConversionFactor("15-50")).toFixed(3)} mCrab
                </div>
            )
       } 
    },
    {   accessorKey: "fermi",
        header: () => <div className="text-right">Last Data Fermi/GBM</div>,
        cell: ({ row }) => { 
            const data = row.original.fermi;
            if (!data || !data.last_flux) return null;
            return (
                <div className="text-right">
                    {(data.last_flux * getConversionFactor("12-50")).toFixed(3)} &plusmn; {(data.last_error * getConversionFactor("12-50")).toFixed(3)} mCrab
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