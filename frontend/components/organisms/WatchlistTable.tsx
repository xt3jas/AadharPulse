"use client"

import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"
import { ArrowUpRight, ArrowDownRight, Minus } from "lucide-react"

const data = [
    {
        region: "#IN-DL-04",
        volatility: "High (0.84)",
        trend: "up",
        lastAudit: "2 hours ago",
        status: "Pending Action",
        statusColor: "warning",
        risk: 92,
    },
    {
        region: "#IN-MH-12",
        volatility: "Moderate (0.52)",
        trend: "stable",
        lastAudit: "1 day ago",
        status: "In Review",
        statusColor: "info",
        risk: 65,
    },
    {
        region: "#IN-KA-09",
        volatility: "Low (0.12)",
        trend: "down",
        lastAudit: "4 hours ago",
        status: "Resolved",
        statusColor: "success",
        risk: 12,
    },
    {
        region: "#IN-WB-21",
        volatility: "Critical (0.95)",
        trend: "up",
        lastAudit: "12 mins ago",
        status: "Escalated",
        statusColor: "danger",
        risk: 98,
    },
]

export function WatchlistTable() {
    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <h3 className="text-lg font-serif font-medium">Operational Volatility Watchlist</h3>
                <div className="flex gap-2 text-muted-foreground">
                    {/* Mock toolbar icons */}
                </div>
            </div>

            <div className="rounded-md border border-white/5 bg-white/5 overflow-hidden">
                <Table>
                    <TableHeader className="bg-white/5">
                        <TableRow className="hover:bg-transparent border-white/5">
                            <TableHead className="w-[150px] text-xs font-semibold tracking-wider text-muted-foreground uppercase">Region Code</TableHead>
                            <TableHead className="text-xs font-semibold tracking-wider text-muted-foreground uppercase">Volatility Index</TableHead>
                            <TableHead className="text-xs font-semibold tracking-wider text-muted-foreground uppercase">Trend</TableHead>
                            <TableHead className="text-xs font-semibold tracking-wider text-muted-foreground uppercase">Last Audit</TableHead>
                            <TableHead className="text-xs font-semibold tracking-wider text-muted-foreground uppercase">Directive Status</TableHead>
                            <TableHead className="text-right text-xs font-semibold tracking-wider text-muted-foreground uppercase">Risk Score</TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {data.map((item) => (
                            <TableRow key={item.region} className="hover:bg-white/5 border-white/5">
                                <TableCell className="font-mono text-xs text-muted-foreground">{item.region}</TableCell>
                                <TableCell className="text-sm font-medium">{item.volatility}</TableCell>
                                <TableCell>
                                    {/* Mock Trend Line */}
                                    {item.trend === "up" && <ArrowUpRight className="h-4 w-4 text-red-500" />}
                                    {item.trend === "down" && <ArrowDownRight className="h-4 w-4 text-green-500" />}
                                    {item.trend === "stable" && <Minus className="h-4 w-4 text-yellow-500" />}
                                </TableCell>
                                <TableCell className="text-sm text-muted-foreground">{item.lastAudit}</TableCell>
                                <TableCell>
                                    <StatusBadge status={item.status} color={item.statusColor} />
                                </TableCell>
                                <TableCell className="text-right font-mono font-medium">
                                    <span className={cn(
                                        item.risk > 90 ? "text-red-500" : item.risk > 50 ? "text-yellow-500" : "text-green-500"
                                    )}>
                                        {item.risk}
                                    </span>
                                    <span className="text-muted-foreground">/100</span>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </div>
        </div>
    )
}

function StatusBadge({ status, color }: { status: string, color: string }) {
    const styles = {
        warning: "bg-amber-500/10 text-amber-500 border-amber-500/20",
        info: "bg-blue-500/10 text-blue-500 border-blue-500/20",
        success: "bg-emerald-500/10 text-emerald-500 border-emerald-500/20",
        danger: "bg-red-500/10 text-red-500 border-red-500/20",
    }

    return (
        <Badge variant="outline" className={cn("rounded-full px-2.5 py-0.5 text-xs font-normal border", styles[color as keyof typeof styles])}>
            <span className={cn("mr-1.5 h-1.5 w-1.5 rounded-full inline-block bg-current opacity-70")} />
            {status}
        </Badge>
    )
}
