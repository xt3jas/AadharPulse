import { LucideIcon } from "lucide-react"
import { cn } from "@/lib/utils"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"

interface MetricCardProps {
    title: string
    value: string | number
    icon?: LucideIcon
    delta?: {
        value: string
        positive?: boolean
        neutral?: boolean
    }
    progressColor?: "success" | "warning" | "danger" | "primary"
    className?: string
}

export function MetricCard({ title, value, icon: Icon, delta, progressColor = "primary", className }: MetricCardProps) {
    const colorMap = {
        success: "bg-green-500",
        warning: "bg-amber-500",
        danger: "bg-red-500",
        primary: "bg-blue-500"
    }

    // Custom badge styling based on delta intent
    const badgeClass = delta?.neutral
        ? "bg-amber-500/10 text-amber-500 hover:bg-amber-500/20"
        : delta?.positive
            ? "bg-emerald-500/10 text-emerald-500 hover:bg-emerald-500/20"
            : "bg-red-500/10 text-red-500 hover:bg-red-500/20"

    return (
        <Card className={cn("overflow-hidden relative border-none bg-card shadow-sm", className)}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">
                    {title}
                </CardTitle>
                {Icon && <Icon className="h-4 w-4 text-muted-foreground" />}
            </CardHeader>
            <CardContent>
                <div className="flex items-baseline space-x-2">
                    <div className="text-3xl font-bold font-sans">{value}</div>
                    {delta && (
                        <Badge
                            variant="outline"
                            className={cn("font-normal border-0 px-1.5 py-0.5 h-auto rounded-sm", badgeClass)}
                        >
                            {delta.value}
                        </Badge>
                    )}
                </div>
                <div className={cn("absolute bottom-0 left-0 w-full h-1", colorMap[progressColor])} />
            </CardContent>
        </Card>
    )
}
