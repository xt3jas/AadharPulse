import { Bell } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"

export function Header() {
    return (
        <div className="flex items-end justify-between pb-6 pt-2">
            <div>
                <p className="text-xs font-semibold tracking-widest text-muted-foreground uppercase mb-2">Executive Synthesis • Live Feed</p>
                <h2 className="text-3xl font-serif font-bold text-foreground">
                    Strategic Analysis &<br />Directive Overview
                </h2>
            </div>

            <div className="flex items-center gap-4">
                <Badge variant="outline" className="bg-emerald-500/10 text-emerald-500 border-emerald-500/20 px-3 py-1.5 h-9 rounded-full gap-2">
                    <span className="relative flex h-2 w-2">
                        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                        <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
                    </span>
                    System Operational
                </Badge>

                <Button variant="ghost" size="icon" className="rounded-full h-10 w-10 border border-white/10 hover:bg-white/5">
                    <Bell className="h-4 w-4" />
                </Button>

                <Button className="bg-primary text-primary-foreground hover:bg-primary/90 h-10 px-6 font-medium shadow-[0_0_15px_rgba(37,99,235,0.3)]">
                    Generate Report
                </Button>
            </div>
        </div>
    )
}
